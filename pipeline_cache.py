"""
pipeline_cache.py
=================
Implements the two-sided caching architecture shown in the system diagram:

  LEFT  (Pre-compute worker)   — runs in background / on schedule
  RIGHT (Post-compute handler) — runs at request time when dashboard opens

Architecture overview
---------------------
                         PipelineCache
  Pre-compute worker  ←──────────────────→  Post-compute handler
  ─────────────────────────────────────────────────────────────
  1. Trigger (new telemetry OR 15-min tick)
  2. cache.invalidate_device(id)             scope_resolution(tag → device_ids)
  3. run_pipeline() per device               cache.get(scope_key)
  4. Aggregate (tags / site / multi-group)   _apply_trends() if comparison needed
  5. cache.set() all payloads                serve to dashboard
                                             ↓ on cache miss → run live, cache result

Usage
-----
    from pipeline_cache import PipelineCache, pre_compute_worker, post_compute_handler

    cache  = PipelineCache()            # in-process (dict). Drop-in Redis below.
    db     = your_device_registry       # anything with .get_records(device_id, window)

    # Background worker (call every 15 min or on telemetry push)
    pre_compute_worker(cache, db, device_ids=["DEV-001", "DEV-3PH"], window=window)

    # Request-time handler (call when dashboard opens)
    payload = post_compute_handler(cache, db, tag="site-A", window=window)
"""

from __future__ import annotations

import hashlib
import json
import time
import threading
from datetime import datetime, timezone
from typing import Any, Callable

from esg_pipeline import run_pipeline, _apply_trends, m1_ingest, m2_consumption, m4_power_quality, m3_carbon


# ---------------------------------------------------------------------------
# Cache key helpers
# ---------------------------------------------------------------------------

def _make_scope_key(
    scope: str,          # e.g. device_id, tag name, "site:factory-1"
    window_from: str,    # ISO-8601
    window_to: str,      # ISO-8601
    config_hash: str,    # short hash of m3/m6 config dicts so config changes bust cache
) -> str:
    """
    Build a deterministic, human-readable cache key.

    Format:  <scope>|<window_from>|<window_to>|<config_hash>

    Examples
    --------
    "DEV-001|2024-06-01T00:00:00Z|2024-06-01T23:59:59Z|a1b2c3d4"
    "tag:site-A|2024-06-01T00:00:00Z|2024-06-01T23:59:59Z|00000000"
    """
    return f"{scope}|{window_from}|{window_to}|{config_hash}"


def _config_hash(m3_config: dict | None, m6_config: dict | None) -> str:
    """
    Produce an 8-character hex digest from the combined module configs.
    Two identical config dicts always produce the same hash; any change
    produces a different one — which automatically busts the cache.
    """
    payload = json.dumps(
        {"m3": m3_config or {}, "m6": m6_config or {}},
        sort_keys=True,
    )
    return hashlib.md5(payload.encode()).hexdigest()[:8]


# ---------------------------------------------------------------------------
# PipelineCache  — in-process dict (swap for Redis without changing callers)
# ---------------------------------------------------------------------------

class PipelineCache:
    """
    Thread-safe in-process cache backed by a plain Python dict.

    Each entry stores:
        {
            "payload":    <PipelinePayload dict>,
            "cached_at":  <unix timestamp float>,
            "ttl_s":      <seconds until stale>,
        }

    To swap in Redis:
    -----------------
        Replace get/set/invalidate_device/invalidate_scope with redis-py calls.
        The key scheme and all callers stay the same.

        import redis
        r = redis.Redis(host="localhost", port=6379, db=0)

        def get(self, key):
            raw = r.get(key)
            return json.loads(raw) if raw else None

        def set(self, key, payload, ttl_s=900):
            r.setex(key, ttl_s, json.dumps(payload))
    """

    def __init__(self, default_ttl_s: int = 900) -> None:
        self._store:   dict[str, dict] = {}
        self._lock:    threading.Lock  = threading.Lock()
        self.default_ttl_s = default_ttl_s
        self.hits   = 0      # simple telemetry counters
        self.misses = 0

    # ── Core read / write ────────────────────────────────────────────────────

    def get(self, key: str) -> dict | None:
        """
        Return the cached PipelinePayload for *key*, or None on miss/expiry.

        This is called at request time (right side of diagram: "cache.get()").
        """
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self.misses += 1
                return None

            age = time.time() - entry["cached_at"]
            if age > entry["ttl_s"]:
                # Stale — treat as miss; let caller decide whether to re-run live
                del self._store[key]
                self.misses += 1
                return None

            self.hits += 1
            return entry["payload"]

    def set(self, key: str, payload: dict, ttl_s: int | None = None) -> None:
        """
        Store *payload* under *key* with an optional TTL (default: 15 min).

        Called by the pre-compute worker after every successful run_pipeline()
        and by the cache-miss fallback on the request path.
        """
        with self._lock:
            self._store[key] = {
                "payload":   payload,
                "cached_at": time.time(),
                "ttl_s":     ttl_s if ttl_s is not None else self.default_ttl_s,
            }

    # ── Invalidation ─────────────────────────────────────────────────────────

    def invalidate_device(self, device_id: str) -> int:
        """
        Purge ALL cache entries whose scope starts with *device_id*.

        Called by the pre-compute worker (left side of diagram:
        "cache.invalidate_device(id)") before re-running the pipeline so
        the dashboard never serves a stale-but-not-expired payload.

        Returns the number of keys removed.
        """
        prefix = f"{device_id}|"
        with self._lock:
            victims = [k for k in self._store if k.startswith(prefix)]
            for k in victims:
                del self._store[k]
        return len(victims)

    def invalidate_scope(self, scope: str) -> int:
        """
        Purge all entries for an aggregated scope (tag, site, multi-group).
        Used after aggregate payloads are rebuilt so the old combined view
        is not served mid-refresh.
        """
        prefix = f"{scope}|"
        with self._lock:
            victims = [k for k in self._store if k.startswith(prefix)]
            for k in victims:
                del self._store[k]
        return len(victims)

    # ── Introspection ────────────────────────────────────────────────────────

    def stats(self) -> dict:
        """Return a snapshot of cache health metrics."""
        with self._lock:
            total  = len(self._store)
            now    = time.time()
            stale  = sum(
                1 for e in self._store.values()
                if now - e["cached_at"] > e["ttl_s"]
            )
        return {
            "keys_total":  total,
            "keys_stale":  stale,
            "keys_live":   total - stale,
            "cache_hits":  self.hits,
            "cache_misses": self.misses,
            "hit_rate_pct": round(
                self.hits / max(1, self.hits + self.misses) * 100, 1
            ),
        }

    def __repr__(self) -> str:
        s = self.stats()
        return (
            f"<PipelineCache keys={s['keys_total']} "
            f"hits={s['cache_hits']} misses={s['cache_misses']}>"
        )


# ---------------------------------------------------------------------------
# Scope resolution  — tag → list[device_id]
# ---------------------------------------------------------------------------

def resolve_scope(tag: str, device_registry: dict[str, list[str]]) -> list[str]:
    """
    Convert a dashboard tag/scope name into a list of device IDs.

    *device_registry* is a simple dict:
        {
            "site-A":    ["DEV-001", "DEV-002"],
            "site-B":    ["DEV-003"],
            "all":       ["DEV-001", "DEV-002", "DEV-003"],
        }

    In production you would replace this with a DB lookup:
        SELECT device_id FROM devices WHERE tag = %s

    This corresponds to the "Scope resolution (tag → device IDs DB lookup)"
    step on the right side of the diagram.
    """
    return device_registry.get(tag, [])


# ---------------------------------------------------------------------------
# Aggregation helper  — merge multiple single-device payloads into one
# ---------------------------------------------------------------------------

def _aggregate_payloads(payloads: list[dict], scope_label: str) -> dict:
    """
    Merge N per-device PipelinePayload dicts into one combined payload.

    Strategy (matches diagram "Aggregate all groups — tags, site, multi-group combos"):
    ─────────────────────────────────────────────────────────────────────────────────
    metric_cards  → SUM  for energy/cost/carbon  |  AVG for ratios (pf, voltage, %)
    chart_series  → SUM  values per shared timestamp bucket
    carbon        → SUM  co2e_kg; concatenate breakdowns
    cost          → SUM  cost_total; concatenate breakdowns
    anomalies     → list of per-device anomaly dicts (heterogeneous — not summed)
    """
    if not payloads:
        return {}

    # ── Metric cards ──────────────────────────────────────────────────────────
    SUM_CARD_IDS = {"total_kwh", "peak_kw", "carbon_kg", "cost_total", "demand_charge"}
    AVG_CARD_IDS = {"avg_pf", "avg_kva", "voltage_avg", "voltage_imbalance_pct"}

    card_buckets: dict[str, list[float]] = {}
    card_meta:    dict[str, dict]        = {}   # keep label/unit/precision from first seen

    for pl in payloads:
        for card in pl.get("metric_cards", []):
            cid = card["id"]
            card_buckets.setdefault(cid, []).append(card["value"])
            card_meta.setdefault(cid, card)

    agg_cards: list[dict] = []
    for cid, values in card_buckets.items():
        base = dict(card_meta[cid])
        if cid in SUM_CARD_IDS:
            agg_val = round(sum(values), 4)
        elif cid in AVG_CARD_IDS:
            agg_val = round(sum(values) / len(values), 4)
        else:
            agg_val = round(sum(values) / len(values), 4)   # default: avg
        base["value"] = agg_val
        base["trend"] = None    # trends on aggregates computed separately if needed
        agg_cards.append(base)

    # ── Chart series ──────────────────────────────────────────────────────────
    series_buckets: dict[str, dict[str, list[float]]] = {}  # series_id → ts → [values]
    series_meta:    dict[str, dict] = {}

    for pl in payloads:
        for series in pl.get("chart_series", []):
            sid = series["id"]
            series_meta.setdefault(sid, series)
            ts_map = series_buckets.setdefault(sid, {})
            for pt in series.get("points", []):
                ts_map.setdefault(pt["timestamp"], []).append(pt["value"])

    agg_series: list[dict] = []
    for sid, ts_map in series_buckets.items():
        base = dict(series_meta[sid])
        base["points"] = [
            {"timestamp": ts, "value": round(sum(vals), 4)}
            for ts, vals in sorted(ts_map.items())
        ]
        agg_series.append(base)

    # ── Carbon ────────────────────────────────────────────────────────────────
    carbon_parts = [pl["carbon"] for pl in payloads if pl.get("carbon")]
    if carbon_parts:
        total_co2e = sum(c["co2e_kg"] for c in carbon_parts if c.get("co2e_kg") is not None)
        all_breakdown: list[dict] = []
        for c in carbon_parts:
            all_breakdown.extend(c.get("breakdown", []))
        agg_carbon: dict | None = {
            "co2e_kg":      round(total_co2e, 4),
            "co2e_per_kwh": carbon_parts[0].get("co2e_per_kwh"),
            "total_kwh":    round(sum(c["total_kwh"] for c in carbon_parts), 4),
            "data_quality": carbon_parts[0].get("data_quality"),
            "ef_source":    carbon_parts[0].get("ef_source"),
            "breakdown":    all_breakdown,
        }
    else:
        agg_carbon = None

    # ── Cost ──────────────────────────────────────────────────────────────────
    cost_parts = [pl["cost"] for pl in payloads if pl.get("cost")]
    if cost_parts:
        agg_cost: dict | None = {
            "cost_total":         round(sum(c["cost_total"] for c in cost_parts), 2),
            "cost_breakdown":     [b for c in cost_parts for b in c.get("cost_breakdown", [])],
            "peak_cost_total":    None,
            "offpeak_cost_total": None,
            "demand_charge":      None,
            "off_hours_kwh":      None,
            "demand_heatmap":     [],
            "metric_cards":       [],
        }
    else:
        agg_cost = None

    # ── Anomalies ─────────────────────────────────────────────────────────────
    agg_anomalies = [
        {"device_id": pl["meta"]["device_id"], **pl["anomalies"]}
        for pl in payloads
        if pl.get("anomalies")
    ] or None

    # ── Window (union of all device windows) ──────────────────────────────────
    all_froms = [pl["meta"]["window"]["from"] for pl in payloads]
    all_tos   = [pl["meta"]["window"]["to"]   for pl in payloads]

    return {
        "meta": {
            "device_id":   scope_label,
            "device_name": scope_label,
            "phase_type":  "mixed",
            "computed_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window": {
                "from": min(all_froms),
                "to":   max(all_tos),
            },
        },
        "metric_cards": agg_cards,
        "chart_series": agg_series,
        "carbon":    agg_carbon,
        "anomalies": agg_anomalies,
        "cost":      agg_cost,
    }


# ---------------------------------------------------------------------------
# LEFT SIDE — Pre-compute worker
# ---------------------------------------------------------------------------

def pre_compute_worker(
    cache: PipelineCache,
    fetch_records: Callable[[str], list[dict]],  # fn(device_id) → list[DeviceRecord]
    device_ids:   list[str],
    window:       dict,          # {"from": ISO-str, "to": ISO-str}
    tag_map:      dict[str, list[str]] | None = None,  # tag → [device_ids]
    m3_config:    dict | None = None,
    m5_config:    dict | None = None,
    m6_config:    dict | None = None,
    ttl_s:        int = 900,
) -> dict[str, str]:
    """
    Pre-compute worker — runs in the background every 15 minutes (or on telemetry push).

    Steps (matching left side of diagram):
    ───────────────────────────────────────
    1. For each device_id:
       a. cache.invalidate_device(id)          ← purge stale entries
       b. fetch records from DB / telemetry store
       c. run_pipeline()                        ← M1→M2→M4→M3→M5→M6
       d. cache.set(device_key, payload)

    2. Aggregate all group combos defined in *tag_map*:
       a. cache.invalidate_scope(tag)
       b. build aggregate payload from per-device payloads
       c. cache.set(tag_key, aggregate_payload)

    Parameters
    ----------
    fetch_records   callable(device_id) → list[DeviceRecord dict]
                    You provide this; it wraps your DB / API call.
    tag_map         optional dict that maps group names to device lists.
                    e.g. {"site-A": ["DEV-001", "DEV-002"], "all": [...]}

    Returns
    -------
    dict mapping every cache key written → "ok" | "error: <msg>"
    """
    cfg_hash   = _config_hash(m3_config, m6_config)
    w_from     = window["from"]
    w_to       = window["to"]
    results:   dict[str, str] = {}
    device_payloads: dict[str, dict] = {}   # device_id → payload (for aggregation)

    # ── Step 1: per-device ───────────────────────────────────────────────────
    for device_id in device_ids:
        # 1a. Invalidate stale entries for this device
        purged = cache.invalidate_device(device_id)
        print(f"[pre-compute] {device_id}: invalidated {purged} cache entries")

        key = _make_scope_key(device_id, w_from, w_to, cfg_hash)

        try:
            # 1b. Fetch raw records
            records = fetch_records(device_id)

            if not records:
                results[key] = "error: no records returned"
                continue

            # 1c. Run the full pipeline (M1→M2→M4→M3→M5→M6)
            payload = run_pipeline(
                records,
                m3_config=m3_config,
                m5_config=m5_config,
                m6_config=m6_config,
            )

            # 1d. Store in cache
            cache.set(key, payload, ttl_s=ttl_s)
            device_payloads[device_id] = payload
            results[key] = "ok"
            print(f"[pre-compute] {device_id}: cached under key '{key}'")

        except Exception as exc:
            results[key] = f"error: {exc}"
            print(f"[pre-compute] {device_id}: FAILED — {exc}")

    # ── Step 2: aggregate group combos ───────────────────────────────────────
    if tag_map:
        for tag, members in tag_map.items():
            # Build the combined payload from already-computed device payloads
            group_payloads = [
                device_payloads[did]
                for did in members
                if did in device_payloads
            ]

            if not group_payloads:
                print(f"[pre-compute] tag '{tag}': no device payloads — skipping")
                continue

            tag_key = _make_scope_key(f"tag:{tag}", w_from, w_to, cfg_hash)

            # Invalidate old aggregate before writing new one
            cache.invalidate_scope(f"tag:{tag}")

            agg = _aggregate_payloads(group_payloads, scope_label=tag)
            cache.set(tag_key, agg, ttl_s=ttl_s)
            results[tag_key] = "ok"
            print(f"[pre-compute] tag '{tag}': aggregate cached ({len(group_payloads)} devices)")

    return results


# ---------------------------------------------------------------------------
# RIGHT SIDE — Post-compute handler (request time)
# ---------------------------------------------------------------------------

def post_compute_handler(
    cache: PipelineCache,
    fetch_records: Callable[[str], list[dict]],
    scope: str,                    # tag name OR single device_id
    window: dict,                  # {"from": ISO-str, "to": ISO-str}
    device_registry: dict[str, list[str]] | None = None,
    comparison_window: dict | None = None,   # for _apply_trends on user-selected period
    m3_config: dict | None = None,
    m5_config: dict | None = None,
    m6_config: dict | None = None,
    ttl_s: int = 900,
) -> dict:
    """
    Post-compute handler — runs at request time when the dashboard opens.

    Steps (matching right side of diagram):
    ────────────────────────────────────────
    1. Scope resolution  — tag → device_ids  (DB lookup via device_registry)
    2. cache.get()       — fetch pre-built payload
    3. _apply_trends()   — if user selected a comparison period
    4. Serve to dashboard

    Cache miss fallback (beige box in diagram):
    ───────────────────────────────────────────
    If cache.get() returns None (cold start / TTL expired):
      a. fetch records live
      b. run_pipeline() now
      c. cache.set() so the *next* request is a hit

    Parameters
    ----------
    scope              A tag name (looked up via device_registry) or a bare device_id.
    device_registry    dict mapping tag → [device_id].  Pass None for device-only scopes.
    comparison_window  {"from": ISO-str, "to": ISO-str} — previous period for trend arrows.
                       When provided, previous records are fetched and _apply_trends is run.

    Returns
    -------
    PipelinePayload dict ready for the dashboard.
    Latency ≈ scope-resolve (fast) + 1 Redis/dict read (fast) for cache hits.
    """
    cfg_hash = _config_hash(m3_config, m6_config)
    w_from   = window["from"]
    w_to     = window["to"]

    # ── Step 1: Scope resolution ─────────────────────────────────────────────
    # Is this scope a tag (multi-device) or a single device?
    is_tag = device_registry and scope in device_registry
    cache_scope = f"tag:{scope}" if is_tag else scope
    key = _make_scope_key(cache_scope, w_from, w_to, cfg_hash)
    print(f"[post-compute] scope='{scope}' → cache key='{key}'")

    # ── Step 2: cache.get() ──────────────────────────────────────────────────
    payload = cache.get(key)

    if payload is not None:
        print(f"[post-compute] cache HIT for '{key}'")
    else:
        # ── Cache miss fallback ───────────────────────────────────────────────
        print(f"[post-compute] cache MISS for '{key}' — running live")

        if is_tag:
            # Multi-device scope: fetch + run each device, then aggregate
            device_ids = resolve_scope(scope, device_registry)
            live_payloads = []
            for did in device_ids:
                try:
                    records = fetch_records(did)
                    dev_payload = run_pipeline(
                        records,
                        m3_config=m3_config,
                        m5_config=m5_config,
                        m6_config=m6_config,
                    )
                    live_payloads.append(dev_payload)

                    # Cache each device individually so future tag-scope hits
                    # can re-aggregate without re-running the pipeline.
                    dev_key = _make_scope_key(did, w_from, w_to, cfg_hash)
                    cache.set(dev_key, dev_payload, ttl_s=ttl_s)
                except Exception as exc:
                    print(f"[post-compute] live run for '{did}' failed: {exc}")

            payload = _aggregate_payloads(live_payloads, scope_label=scope)
        else:
            # Single device
            records = fetch_records(scope)
            payload = run_pipeline(
                records,
                m3_config=m3_config,
                m5_config=m5_config,
                m6_config=m6_config,
            )

        # Cache the result so the next request is a hit
        cache.set(key, payload, ttl_s=ttl_s)
        print(f"[post-compute] live result cached under '{key}'")

    # ── Step 3: _apply_trends() if comparison period requested ───────────────
    if comparison_window and payload:
        print(f"[post-compute] applying trends for comparison window {comparison_window}")
        try:
            prev_from = comparison_window["from"]
            prev_to   = comparison_window["to"]
            prev_key  = _make_scope_key(cache_scope, prev_from, prev_to, cfg_hash)

            # Try to get the previous period payload from cache first
            prev_payload = cache.get(prev_key)

            if prev_payload is None:
                # Not cached — fetch and compute the previous period live
                print(f"[post-compute] previous period not cached — fetching live")
                if is_tag:
                    device_ids = resolve_scope(scope, device_registry)
                    prev_pl_list = []
                    for did in device_ids:
                        try:
                            prev_records = fetch_records(did)   # caller provides windowed fetch
                            prev_dev_pl  = run_pipeline(
                                prev_records,
                                m3_config=m3_config,
                                m5_config=m5_config,
                                m6_config=m6_config,
                            )
                            prev_pl_list.append(prev_dev_pl)
                        except Exception:
                            pass
                    prev_payload = _aggregate_payloads(prev_pl_list, scope_label=scope)
                else:
                    prev_records = fetch_records(scope)
                    prev_payload = run_pipeline(
                        prev_records,
                        m3_config=m3_config,
                        m5_config=m5_config,
                        m6_config=m6_config,
                    )
                cache.set(prev_key, prev_payload, ttl_s=ttl_s)

            # Build previous-period card map and apply trends in-place
            prev_card_map = {c["id"]: c["value"] for c in prev_payload.get("metric_cards", [])}
            payload = {
                **payload,
                "metric_cards": _apply_trends(payload["metric_cards"], prev_card_map),
            }
        except Exception as exc:
            print(f"[post-compute] trend application failed: {exc}")

    # ── Step 4: serve to dashboard ───────────────────────────────────────────
    return payload


# ---------------------------------------------------------------------------
# Quick smoke-test (python pipeline_cache.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # ── Inline mock data so the smoke-test is self-contained ─────────────────

    MOCK_RECORDS: dict[str, list[dict]] = {
        "DEV-001": [
            {"device_id": "DEV-001", "device_name": "Main Panel",
             "timestamp": "2024-06-01T00:00:00Z", "interval_s": 900,
             "kwh": 1000.0, "voltage": 230.0, "pf": 0.91, "ca": 12.5},
            {"device_id": "DEV-001", "device_name": "Main Panel",
             "timestamp": "2024-06-01T00:15:00Z", "interval_s": 900,
             "kwh": 1002.3, "voltage": 228.5, "pf": 0.89, "ca": 11.8},
            {"device_id": "DEV-001", "device_name": "Main Panel",
             "timestamp": "2024-06-01T00:30:00Z", "interval_s": 900,
             "kwh": 1004.9, "voltage": 231.2, "pf": 0.92, "ca": 13.1},
            {"device_id": "DEV-001", "device_name": "Main Panel",
             "timestamp": "2024-06-01T01:00:00Z", "interval_s": 900,
             "kwh": 1010.1, "voltage": 229.0, "pf": 0.90, "ca": 12.2},
        ],
        "DEV-002": [
            {"device_id": "DEV-002", "device_name": "Sub Panel B",
             "timestamp": "2024-06-01T00:00:00Z", "interval_s": 900,
             "kwh": 500.0, "voltage": 232.0, "pf": 0.85, "ca": 9.0},
            {"device_id": "DEV-002", "device_name": "Sub Panel B",
             "timestamp": "2024-06-01T00:15:00Z", "interval_s": 900,
             "kwh": 501.8, "voltage": 230.5, "pf": 0.84, "ca": 8.7},
            {"device_id": "DEV-002", "device_name": "Sub Panel B",
             "timestamp": "2024-06-01T00:30:00Z", "interval_s": 900,
             "kwh": 503.9, "voltage": 233.1, "pf": 0.86, "ca": 9.3},
            {"device_id": "DEV-002", "device_name": "Sub Panel B",
             "timestamp": "2024-06-01T01:00:00Z", "interval_s": 900,
             "kwh": 507.5, "voltage": 231.0, "pf": 0.85, "ca": 9.1},
        ],
    }

    def fetch_records(device_id: str) -> list[dict]:
        return MOCK_RECORDS.get(device_id, [])

    WINDOW       = {"from": "2024-06-01T00:00:00Z", "to": "2024-06-01T01:00:00Z"}
    TAG_MAP      = {"site-A": ["DEV-001", "DEV-002"]}
    DEV_REGISTRY = {"site-A": ["DEV-001", "DEV-002"]}
    M3_CFG       = {"grid_region": "PK"}
    M6_CFG       = {"tariff_per_kwh": 45.0}

    cache = PipelineCache(default_ttl_s=900)

    print("\n" + "═"*60)
    print("  PRE-COMPUTE WORKER")
    print("═"*60)
    pre_results = pre_compute_worker(
        cache,
        fetch_records,
        device_ids=["DEV-001", "DEV-002"],
        window=WINDOW,
        tag_map=TAG_MAP,
        m3_config=M3_CFG,
        m6_config=M6_CFG,
    )
    print("\nKeys written:", json.dumps(pre_results, indent=2))
    print("\nCache stats:", cache.stats())

    print("\n" + "═"*60)
    print("  POST-COMPUTE HANDLER — single device (cache HIT expected)")
    print("═"*60)
    pl = post_compute_handler(
        cache, fetch_records,
        scope="DEV-001",
        window=WINDOW,
        m3_config=M3_CFG,
        m6_config=M6_CFG,
    )
    print(f"  total_kwh = {next(c['value'] for c in pl['metric_cards'] if c['id']=='total_kwh')}")
    print(f"  carbon    = {pl['carbon']['co2e_kg']} kgCO₂e")
    print("Cache stats:", cache.stats())

    print("\n" + "═"*60)
    print("  POST-COMPUTE HANDLER — tag scope (cache HIT expected)")
    print("═"*60)
    pl_tag = post_compute_handler(
        cache, fetch_records,
        scope="site-A",
        window=WINDOW,
        device_registry=DEV_REGISTRY,
        m3_config=M3_CFG,
        m6_config=M6_CFG,
    )
    total = next(c["value"] for c in pl_tag["metric_cards"] if c["id"] == "total_kwh")
    print(f"  site-A total_kwh = {total}  (DEV-001 + DEV-002 summed)")
    print("Cache stats:", cache.stats())

    print("\n" + "═"*60)
    print("  POST-COMPUTE HANDLER — cache MISS fallback (unknown device)")
    print("═"*60)
    # DEV-002 asked with a different config → config_hash differs → miss
    pl_miss = post_compute_handler(
        cache, fetch_records,
        scope="DEV-002",
        window=WINDOW,
        m3_config={"grid_region": "UK"},   # different config → different key → MISS
        m6_config=M6_CFG,
    )
    print(f"  DEV-002 total_kwh = {next(c['value'] for c in pl_miss['metric_cards'] if c['id']=='total_kwh')}")
    print("Cache stats (after live fallback):", cache.stats())
