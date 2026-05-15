"""
ESG Pipeline — Group Pipeline
==============================
Public entry point for scope-based (group / tag / site) pipeline execution.

run_pipeline() (single device) is completely unchanged — this module composes
it via three layers:

    1. ScopeResolver     → flat list of device IDs
    2. Per-device runner → run_pipeline() once per device (thread-parallel,
                           results cached by device+window+config hash)
    3. aggregate_payloads → group-level GroupPipelinePayload

Cache design
------------
Results are keyed on (device_id, window_from, window_to, config_hash).
The cache ships as an in-process PipelineCache (suitable for a single
worker / long-running server process).  Swap in a Redis-backed variant by
subclassing PipelineCache and overriding get/set/invalidate.

Usage
-----
    from esg_pipeline import run_pipeline          # unchanged
    from scope_resolver import Scope, ScopeResolver, InMemoryDeviceRegistry
    from group_pipeline import run_group_pipeline, PipelineCache

    registry = InMemoryDeviceRegistry(
        tag_map={"HVAC": ["DEV-001", "DEV-002"]},
        site_map={"SITE-01": ["DEV-001", "DEV-002", "DEV-003"]},
    )

    def fetch_records(device_id: str, from_ts: str, to_ts: str) -> list[dict]:
        # Query your telemetry DB here
        return db.query(device_id, from_ts, to_ts)

    cache    = PipelineCache()
    resolver = ScopeResolver(registry)

    payload = run_group_pipeline(
        scope          = Scope.group("HVAC"),
        record_fetcher = fetch_records,
        resolver       = resolver,
        window_from    = "2024-06-01T00:00:00Z",
        window_to      = "2024-06-30T23:59:59Z",
        m3_config      = {"grid_region": "PK"},
        m6_config      = {"tariff_per_kwh": 45.0},
        cache          = cache,
        max_workers    = 8,
    )

    # payload["meta"]["scope_type"] == "group"
    # payload["meta"]["device_count"] == 2
    # payload["metric_cards"] → aggregated cards with aggregation_method field
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Callable

from esg_pipeline import run_pipeline, ValidationError
from scope_resolver import Scope, ScopeResolver
from group_aggregator import aggregate_payloads

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline cache
# ---------------------------------------------------------------------------

class PipelineCache:
    """
    Thread-safe in-process cache for per-device pipeline results.

    Keys are (device_id, window_from, window_to, config_hash).
    Swap this out for a Redis/Memcached subclass in production.
    """

    def __init__(self) -> None:
        self._store: dict[str, dict] = {}
        self._lock  = threading.Lock()

    # ── Key construction ───────────────────────────────────────────────────

    @staticmethod
    def make_key(
        device_id: str,
        window_from: str,
        window_to: str,
        config_hash: str,
    ) -> str:
        return f"{device_id}|{window_from}|{window_to}|{config_hash}"

    # ── Read / write ───────────────────────────────────────────────────────

    def get(self, device_id: str, window_from: str, window_to: str, config_hash: str) -> dict | None:
        key = self.make_key(device_id, window_from, window_to, config_hash)
        with self._lock:
            return self._store.get(key)

    def set(self, device_id: str, window_from: str, window_to: str, config_hash: str, payload: dict) -> None:
        key = self.make_key(device_id, window_from, window_to, config_hash)
        with self._lock:
            self._store[key] = payload

    def invalidate_device(self, device_id: str) -> int:
        """
        Remove all cached entries for a device (e.g. after a meter correction).
        Returns the number of entries removed.
        """
        prefix = f"{device_id}|"
        with self._lock:
            to_remove = [k for k in self._store if k.startswith(prefix)]
            for k in to_remove:
                del self._store[k]
        return len(to_remove)

    def invalidate_all(self) -> int:
        """Clear the entire cache. Returns the number of entries removed."""
        with self._lock:
            n = len(self._store)
            self._store.clear()
        return n

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._store)


# ---------------------------------------------------------------------------
# Config hashing
# ---------------------------------------------------------------------------

def _config_hash(*configs: dict | None) -> str:
    """
    Stable 8-character hex hash of any combination of config dicts.

    None entries are ignored.  Order of configs matters so that
    (m3_config, None, m6_config) and (None, m3_config, m6_config)
    produce different hashes.
    """
    serialisable = [c if c is not None else {} for c in configs]
    payload_bytes = json.dumps(serialisable, sort_keys=True).encode()
    return hashlib.md5(payload_bytes).hexdigest()[:8]


# ---------------------------------------------------------------------------
# Per-device runner (called once per device in the thread pool)
# ---------------------------------------------------------------------------

def _compute_one(
    device_id: str,
    window_from: str,
    window_to: str,
    record_fetcher: Callable[[str, str, str], list[dict]],
    cache: PipelineCache | None,
    config_hash: str,
    m3_config: dict | None,
    m5_config: dict | None,
    m6_config: dict | None,
    previous_record_fetcher: Callable[[str, str, str], list[dict]] | None,
    prev_window_from: str | None,
    prev_window_to: str | None,
) -> dict | None:
    """
    Fetch records for one device and run run_pipeline().

    Returns the PipelinePayload dict, or None if the device has no records
    (offline / no data in window) or raises a ValidationError.

    All exceptions other than ValidationError are re-raised so the caller
    can surface unexpected failures.
    """
    # --- Cache hit ---
    if cache:
        cached = cache.get(device_id, window_from, window_to, config_hash)
        if cached is not None:
            logger.debug("Cache hit for device %s", device_id)
            return cached

    # --- Fetch current-period records ---
    try:
        records = record_fetcher(device_id, window_from, window_to)
    except Exception:
        logger.exception("record_fetcher failed for device %s", device_id)
        raise

    if not records:
        logger.debug("No records for device %s in window [%s, %s]", device_id, window_from, window_to)
        return None

    # --- Fetch previous-period records (for trend computation) ---
    previous_records: list[dict] | None = None
    if previous_record_fetcher and prev_window_from and prev_window_to:
        try:
            previous_records = previous_record_fetcher(device_id, prev_window_from, prev_window_to) or None
        except Exception:
            logger.warning(
                "previous_record_fetcher failed for device %s — trends will be null",
                device_id,
            )

    # --- Compute ---
    try:
        payload = run_pipeline(
            records,
            previous_records=previous_records,
            m3_config=m3_config,
            m5_config=m5_config,
            m6_config=m6_config,
        )
    except ValidationError as exc:
        logger.warning("ValidationError for device %s: %s", device_id, exc)
        return None   # skip bad devices rather than aborting the whole group

    # --- Cache store ---
    if cache:
        cache.set(device_id, window_from, window_to, config_hash, payload)

    return payload


# ---------------------------------------------------------------------------
# run_group_pipeline — Public entry point
# ---------------------------------------------------------------------------

def run_group_pipeline(
    scope: Scope,
    record_fetcher: Callable[[str, str, str], list[dict]],
    resolver: ScopeResolver,
    *,
    window_from: str,
    window_to: str,
    previous_record_fetcher: Callable[[str, str, str], list[dict]] | None = None,
    prev_window_from: str | None = None,
    prev_window_to: str | None = None,
    m3_config: dict | None = None,
    m5_config: dict | None = None,
    m6_config: dict | None = None,
    cache: PipelineCache | None = None,
    max_workers: int = 8,
    skip_empty_devices: bool = True,
) -> dict:
    """
    Run the ESG pipeline for a scope (single device, tag, multi-group, or site).

    Parameters
    ----------
    scope
        A Scope descriptor created via Scope.device(), .group(), .groups(),
        or .site().

    record_fetcher
        Callable(device_id, window_from, window_to) → list[DeviceRecord dicts].
        Called once per device, potentially in parallel.  Must be thread-safe.

    resolver
        ScopeResolver instance backed by your DeviceRegistry.

    window_from / window_to
        ISO-8601 UTC strings defining the analysis window, e.g.
        "2024-06-01T00:00:00Z" / "2024-06-30T23:59:59Z".

    previous_record_fetcher
        Optional callable with the same signature as record_fetcher but for
        the prior period.  When supplied together with prev_window_from/to,
        per-device trend deltas are populated in metric_cards.

    prev_window_from / prev_window_to
        ISO-8601 UTC strings for the previous window.  Required when
        previous_record_fetcher is set; ignored otherwise.

    m3_config / m5_config / m6_config
        Module configs forwarded verbatim to run_pipeline().  Same keys as
        documented in esg_pipeline.run_pipeline().

    cache
        PipelineCache instance.  Pass a shared cache across calls to avoid
        re-computing the same device+window combination.  Pass None (default)
        to disable caching.

    max_workers
        Thread-pool size for concurrent per-device computation.
        Set to 1 for sequential execution (useful for debugging).

    skip_empty_devices
        When True (default), devices with no records in the window are
        silently skipped.  When False, raises ValueError if any device
        returns no records.

    Returns
    -------
    GroupPipelinePayload dict — JSON-serialisable.

    Raises
    ------
    LookupError   if the scope resolves to zero devices.
    ValueError    if all resolved devices have no records (group is empty).
    RuntimeError  if any device computation raises an unexpected exception
                  (ValidationErrors are non-fatal and logged as warnings).
    """
    # --- Resolve scope → device IDs ---
    device_ids = resolver.resolve(scope)
    logger.info(
        "run_group_pipeline: scope=%s label=%s devices=%d window=[%s, %s]",
        scope.scope_type, scope.label, len(device_ids), window_from, window_to,
    )

    config_hash = _config_hash(m3_config, m5_config, m6_config)

    # --- Fan out: compute one payload per device, in parallel ---
    payloads: list[dict] = []
    errors:   list[tuple[str, Exception]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_device = {
            pool.submit(
                _compute_one,
                did,
                window_from,
                window_to,
                record_fetcher,
                cache,
                config_hash,
                m3_config,
                m5_config,
                m6_config,
                previous_record_fetcher,
                prev_window_from,
                prev_window_to,
            ): did
            for did in device_ids
        }

        for future in as_completed(future_to_device):
            did = future_to_device[future]
            try:
                result = future.result()
            except Exception as exc:
                errors.append((did, exc))
                logger.error("Unexpected error for device %s: %s", did, exc)
                continue

            if result is None:
                if not skip_empty_devices:
                    raise ValueError(f"Device {did} returned no records in window [{window_from}, {window_to}]")
                logger.debug("Skipping device %s (no records)", did)
                continue

            payloads.append(result)

    if errors:
        # Re-raise the first unexpected error so the caller knows something went wrong
        first_device, first_exc = errors[0]
        raise RuntimeError(
            f"{len(errors)} device(s) failed unexpectedly. "
            f"First failure was device '{first_device}': {first_exc}"
        ) from first_exc

    if not payloads:
        raise ValueError(
            f"Scope '{scope.label}' resolved to {len(device_ids)} device(s) "
            f"but none had records in the window [{window_from}, {window_to}]"
        )

    logger.info(
        "run_group_pipeline: %d/%d devices produced payloads, aggregating...",
        len(payloads), len(device_ids),
    )

    # --- Aggregate ---
    group_payload = aggregate_payloads(payloads, scope, m5_config=m5_config)

    # --- Enrich meta with run-level stats ---
    group_payload["meta"]["devices_with_data"] = len(payloads)
    group_payload["meta"]["devices_skipped"]   = len(device_ids) - len(payloads)

    return group_payload


# ---------------------------------------------------------------------------
# Convenience wrapper: single device via the group pipeline
# ---------------------------------------------------------------------------

def run_device_pipeline(
    device_id: str,
    record_fetcher: Callable[[str, str, str], list[dict]],
    *,
    window_from: str,
    window_to: str,
    **kwargs,
) -> dict:
    """
    Thin wrapper: run the group pipeline for exactly one device.

    Accepts the same keyword arguments as run_group_pipeline (m3_config,
    m6_config, cache, etc.).  Useful when you want single-device calls to
    share the same cache and config path as group calls.
    """
    registry = _SingleDeviceRegistry(device_id)
    resolver = ScopeResolver(registry)
    return run_group_pipeline(
        scope=Scope.device(device_id),
        record_fetcher=record_fetcher,
        resolver=resolver,
        window_from=window_from,
        window_to=window_to,
        **kwargs,
    )


class _SingleDeviceRegistry:
    """Minimal registry for single-device calls — avoids importing InMemoryDeviceRegistry."""
    def __init__(self, device_id: str) -> None:
        self._did = device_id
    def devices_for_tag(self, tag: str) -> list[str]: return []
    def devices_for_site(self, site_id: str) -> list[str]: return []


# ---------------------------------------------------------------------------
# Quick smoke-test (run: python group_pipeline.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json as _json
    from scope_resolver import InMemoryDeviceRegistry

    # ── Minimal in-memory fixture ──────────────────────────────────────────
    _BASE = {
        "interval_s": 900,
        "voltage":    230.0,
        "pf":         0.90,
        "ca":         12.0,
    }

    _DEVICE_RECORDS: dict[str, list[dict]] = {
        "DEV-A": [
            {**_BASE, "device_id": "DEV-A", "device_name": "Panel A",
             "timestamp": "2024-06-01T00:00:00Z", "kwh": 1000.0},
            {**_BASE, "device_id": "DEV-A", "device_name": "Panel A",
             "timestamp": "2024-06-01T00:15:00Z", "kwh": 1002.5},
            {**_BASE, "device_id": "DEV-A", "device_name": "Panel A",
             "timestamp": "2024-06-01T00:30:00Z", "kwh": 1005.0},
            {**_BASE, "device_id": "DEV-A", "device_name": "Panel A",
             "timestamp": "2024-06-01T01:00:00Z", "kwh": 1010.0},
        ],
        "DEV-B": [
            {**_BASE, "device_id": "DEV-B", "device_name": "Panel B",
             "timestamp": "2024-06-01T00:00:00Z", "kwh": 500.0},
            {**_BASE, "device_id": "DEV-B", "device_name": "Panel B",
             "timestamp": "2024-06-01T00:15:00Z", "kwh": 501.5},
            {**_BASE, "device_id": "DEV-B", "device_name": "Panel B",
             "timestamp": "2024-06-01T00:30:00Z", "kwh": 503.0},
            {**_BASE, "device_id": "DEV-B", "device_name": "Panel B",
             "timestamp": "2024-06-01T01:00:00Z", "kwh": 506.0},
        ],
        "DEV-C": [
            {**_BASE, "device_id": "DEV-C", "device_name": "Panel C",
             "timestamp": "2024-06-01T00:00:00Z", "kwh": 200.0},
            {**_BASE, "device_id": "DEV-C", "device_name": "Panel C",
             "timestamp": "2024-06-01T00:15:00Z", "kwh": 200.8},
            {**_BASE, "device_id": "DEV-C", "device_name": "Panel C",
             "timestamp": "2024-06-01T01:00:00Z", "kwh": 202.0},
        ],
    }

    def _fetch(device_id: str, _from: str, _to: str) -> list[dict]:
        return _DEVICE_RECORDS.get(device_id, [])

    registry = InMemoryDeviceRegistry(
        tag_map={
            "HVAC":    ["DEV-A", "DEV-B"],
            "Organic": ["DEV-B", "DEV-C"],
            "Floor-A": ["DEV-A", "DEV-C"],
        },
        site_map={
            "SITE-01": ["DEV-A", "DEV-B", "DEV-C"],
        },
    )
    resolver = ScopeResolver(registry)
    cache    = PipelineCache()

    WINDOW = ("2024-06-01T00:00:00Z", "2024-06-01T01:00:00Z")

    scopes = [
        Scope.device("DEV-A"),
        Scope.group("HVAC"),
        Scope.groups(["HVAC", "Organic"]),   # DEV-B should appear only once
        Scope.site("SITE-01"),
    ]

    for scope in scopes:
        print(f"\n{'='*60}")
        print(f"Scope: {scope.scope_type} — {scope.label}")
        payload = run_group_pipeline(
            scope=scope,
            record_fetcher=_fetch,
            resolver=resolver,
            window_from=WINDOW[0],
            window_to=WINDOW[1],
            m3_config={"grid_region": "PK"},
            m6_config={"tariff_per_kwh": 45.0},
            cache=cache,
            max_workers=4,
        )
        meta   = payload["meta"]
        cards  = {c["id"]: c for c in payload["metric_cards"]}

        print(f"  devices_with_data : {meta['devices_with_data']}")
        print(f"  window            : {meta['window']['from']} → {meta['window']['to']}")

        for cid in ("total_kwh", "peak_kw", "avg_pf", "cost_total"):
            if cid in cards:
                c = cards[cid]
                print(f"  {cid:25s}: {c['value']:.3f} {c['unit']}  "
                      f"[{c.get('aggregation_method','—')} / {c.get('contributing_devices','?')} devices]")

        if payload.get("carbon") and payload["carbon"].get("co2e_kg") is not None:
            print(f"  {'co2e_kg':25s}: {payload['carbon']['co2e_kg']:.4f} kgCO₂e  "
                  f"[{payload['carbon']['data_quality']}]")

    print(f"\n\nCache size after all runs: {cache.size} entries")

    # Demonstrate cache hit (second call should not re-compute)
    print("\nSecond call for HVAC (should all be cache hits):"),
    run_group_pipeline(
        scope=Scope.group("HVAC"),
        record_fetcher=_fetch,
        resolver=resolver,
        window_from=WINDOW[0],
        window_to=WINDOW[1],
        m3_config={"grid_region": "PK"},
        m6_config={"tariff_per_kwh": 45.0},
        cache=cache,
    )
    print(" done.")
    # =========================================================================
    # MultiWindowCache demo — NEW (added alongside multi_window_cache.py)
    # =========================================================================
    # Shows how to use the slab-based cache for dashboard window-switching.
    # Uses the same _DEVICE_RECORDS / _fetch fixture defined above.
    # =========================================================================

    print(f"\n{'='*60}")
    print("MultiWindowCache demo (single device, multiple windows)")
    print(f"{'='*60}")

    from datetime import date
    from multi_window_cache import MultiWindowCache, WindowPreComputer, window_preset

    # fetch_fn adapter: multi_window_cache passes (device_id, from_str, to_str)
    # which is the same signature as _fetch — no changes needed.
    def _fetch_windowed(device_id: str, w_from: str, w_to: str) -> list[dict]:
        return _fetch(device_id, w_from, w_to)

    mwc    = MultiWindowCache()
    worker = WindowPreComputer(
        slab_cache = mwc.slab_cache,
        fetch_fn   = _fetch_windowed,
        device_ids = ["DEV-A", "DEV-B", "DEV-C"],
        max_workers = 4,
    )

    # --- Step 1: back-fill slabs (first deploy / cold cache) ---
    # In production this runs once at startup.
    # Here we use days=1 because our fixture only has data for one day.
    print("\nBack-filling 1 day of slabs for all devices...")
    counts = worker.backfill(
        days      = 1,
        m3_config = {"grid_region": "PK"},
        m6_config = {"tariff_per_kwh": 45.0},
    )
    print(f"  Slabs written: {counts}")
    print(f"  Cache stats  : {mwc.stats()}")

    # --- Step 2: dashboard request — any preset, instant from cache ---
    for preset in ("1d",):          # add "7d", "30d" when you have real data
        # window_preset() returns today's date range; we override to our fixture date
        from_d = date(2024, 6, 1)
        to_d   = date(2024, 6, 1)
        print(f"\nget_window DEV-A [{from_d} → {to_d}] (preset ~'{preset}'):")
        pl = mwc.get_window(
            device_id = "DEV-A",
            from_date = from_d,
            to_date   = to_d,
            fetch_fn  = _fetch_windowed,
            m3_config = {"grid_region": "PK"},
            m6_config = {"tariff_per_kwh": 45.0},
        )
        cards = {c["id"]: c for c in pl["metric_cards"]}
        for cid in ("total_kwh", "peak_kw", "cost_total"):
            if cid in cards:
                c = cards[cid]
                print(f"  {cid:20s}: {c['value']:.3f} {c['unit']}")
        if pl.get("carbon") and pl["carbon"].get("co2e_kg") is not None:
            print(f"  {'co2e_kg':20s}: {pl['carbon']['co2e_kg']:.4f} kgCO₂e")
        print(f"  slab_count        : {pl['meta'].get('slab_count', 1)}")

    # --- Step 3: simulate scheduler tick (refresh today's partial slab) ---
    print("\nSimulating 15-min scheduler tick (refresh_today)...")
    tick_results = worker.refresh_today(
        m3_config = {"grid_region": "PK"},
        m6_config = {"tariff_per_kwh": 45.0},
    )
    print(f"  Tick results: {tick_results}")
    print(f"  Cache stats : {mwc.stats()}")

    print("\nDone. Run 'python multi_window_cache.py' for a full 7-day latency demo.")