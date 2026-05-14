"""
ESG Pipeline — Group Aggregator
================================
Combines a list of per-device PipelinePayload dicts (produced by run_pipeline)
into a single GroupPipelinePayload using metric-type-aware aggregation rules.

Aggregation rules by metric type
---------------------------------
  SUM          — total_kwh, carbon_kg / co2e_kg, cost_total, demand_charge,
                 peak_cost_total, offpeak_cost_total, off_hours_kwh
                 → meaningful group totals; just add them up.

  MAX          — peak_kw, voltage_imbalance_pct
                 → the group's worst-case / highest-demand reading.

  WEIGHTED AVG — avg_pf, avg_kva, voltage_avg
                 → weight each device's value by its contribution (kWh) so
                 a 1 kW device doesn't dilute a 100 kW device's readings.
                 Naive averaging is incorrect for electrical quantities.

  SERIES MERGE — all chart_series
                 → align on the timestamp bucket, SUM kWh / kVA series,
                 AVG voltage / PF series.  Devices absent from a bucket are
                 excluded from the denominator (not treated as zero) so an
                 offline device doesn't pull down the group average.

Anomaly aggregation
-------------------
  phase_imbalance is a per-device result.  The group view surfaces:
    - max imbalance across all devices          (worst-case signal)
    - count of *devices* (not readings) breaching the threshold
    - list of breaching device_ids              (drill-down support)

Carbon / Cost
-------------
  co2e_kg and cost_total are summed; breakdown arrays are merged and
  re-sorted by period.  data_quality is demoted to the worst tier found
  across any device.

Usage
-----
    from group_aggregator import aggregate_payloads
    from scope_resolver import Scope

    group_payload = aggregate_payloads(
        payloads,           # list[dict] — one PipelinePayload per device
        scope,              # Scope descriptor (used to populate meta)
        scope_label="HVAC", # optional display label
    )
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Aggregation rule tables
# ---------------------------------------------------------------------------

#: MetricCard id → aggregation method name
CARD_RULES: dict[str, str] = {
    # SUM
    "total_kwh":          "sum",
    "co2e_kg":            "sum",
    "cost_total":         "sum",
    "demand_charge":      "sum",
    "peak_cost_total":    "sum",
    "offpeak_cost_total": "sum",
    "off_hours_kwh":      "sum",

    # MAX
    "peak_kw":               "max",
    "voltage_imbalance_pct": "max",

    # WEIGHTED AVERAGE (weight = each device's total_kwh)
    "avg_pf":      "wavg",
    "avg_kva":     "wavg",
    "voltage_avg": "wavg",
}

#: ChartSeries id → aggregation method name
SERIES_RULES: dict[str, str] = {
    # SUM — energy and power quantities are additive across devices
    "kwh_15min":   "sum",
    "kwh_hourly":  "sum",
    "kwh_daily":   "sum",
    "kwh_monthly": "sum",
    "kva_15min":   "sum",
    # AVERAGE — quality / per-unit quantities
    "pf_15min":        "avg",
    "voltage_15min":   "avg",
    "voltage_a_15min": "avg",
    "voltage_b_15min": "avg",
    "voltage_c_15min": "avg",
}

#: Carbon data_quality tiers in ascending severity (worst = last)
_QUALITY_ORDER = ["verified", "estimated", "EF config required"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _card_value(payload: dict, card_id: str) -> float | None:
    """Extract the value of a named MetricCard from a payload, or None."""
    for card in payload.get("metric_cards", []):
        if card["id"] == card_id:
            return card["value"]
    return None


def _card_map(payload: dict) -> dict[str, dict]:
    """Return {card_id: card_dict} for fast lookup."""
    return {c["id"]: c for c in payload.get("metric_cards", [])}


def _series_points(payload: dict, series_id: str) -> list[dict]:
    """Return the points list of a named chart series, or []."""
    for s in payload.get("chart_series", []):
        if s["id"] == series_id:
            return s["points"]
    return []


def _series_meta(payload: dict, series_id: str) -> dict | None:
    """Return the series dict (without points) for metadata, or None."""
    for s in payload.get("chart_series", []):
        if s["id"] == series_id:
            return {k: v for k, v in s.items() if k != "points"}
    return None


# ---------------------------------------------------------------------------
# MetricCard aggregation
# ---------------------------------------------------------------------------

def _aggregate_cards(
    payloads: list[dict],
) -> list[dict]:
    """
    Aggregate MetricCard lists from all device payloads.

    Each card is aggregated according to CARD_RULES.  Cards not in
    CARD_RULES (e.g. future custom cards) are dropped from the group
    payload to avoid silently wrong values.

    Weighted-average cards use each device's total_kwh as the weight.
    A device with no total_kwh card (e.g. too few records) contributes
    nothing to the weighted average.

    Returns a list of MetricCard dicts with an added `aggregation_method`
    field and a `contributing_devices` count for dashboard transparency.
    """
    # Collect all card dicts keyed by id across payloads
    buckets: dict[str, list[tuple[dict, float]]] = defaultdict(list)
    # bucket value: list of (card_dict, device_kwh_weight)

    for payload in payloads:
        device_kwh = _card_value(payload, "total_kwh") or 0.0
        for card in payload.get("metric_cards", []):
            cid = card["id"]
            if cid in CARD_RULES:
                buckets[cid].append((card, device_kwh))

    result: list[dict] = []

    for cid, entries in buckets.items():
        if not entries:
            continue

        method = CARD_RULES[cid]
        reference_card = entries[0][0]  # use first card for metadata (label/unit/precision)
        values = [e[0]["value"] for e in entries]
        weights = [e[1] for e in entries]
        n = len(values)

        if method == "sum":
            agg_value = round(sum(values), 6)

        elif method == "max":
            agg_value = round(max(values), 6)

        elif method == "wavg":
            total_weight = sum(weights)
            if total_weight > 0:
                agg_value = round(
                    sum(v * w for v, w in zip(values, weights)) / total_weight,
                    6,
                )
            else:
                # All weights zero (edge case: no consumption data); fall back to simple avg
                agg_value = round(sum(values) / n, 6)

        else:
            # Unknown rule — skip rather than emit garbage
            continue

        result.append({
            "id":                  cid,
            "label":               reference_card["label"],
            "value":               agg_value,
            "unit":                reference_card["unit"],
            "precision":           reference_card["precision"],
            "trend":               None,    # trends on group payloads require group-level history
            "aggregation_method":  method,
            "contributing_devices": n,
        })

    # Sort in the canonical card order so the dashboard always sees a stable list
    _ORDER = list(CARD_RULES.keys())
    result.sort(key=lambda c: _ORDER.index(c["id"]) if c["id"] in _ORDER else 999)
    return result


# ---------------------------------------------------------------------------
# ChartSeries aggregation
# ---------------------------------------------------------------------------

def _merge_series(payloads: list[dict]) -> list[dict]:
    """
    Merge all chart_series from device payloads.

    For every known series id the points from all devices are aligned on
    their timestamp bucket and either summed (energy quantities) or averaged
    (quality quantities).

    Devices absent from a bucket are excluded from the denominator for AVG
    so an offline device does not drag down the group average.

    Unknown series IDs not present in SERIES_RULES are silently skipped.
    """
    # Gather all series IDs present in at least one payload
    all_series_ids: set[str] = set()
    for payload in payloads:
        for s in payload.get("chart_series", []):
            all_series_ids.add(s["id"])

    merged: list[dict] = []

    for sid in sorted(all_series_ids):   # stable output order
        rule = SERIES_RULES.get(sid)
        if rule is None:
            continue  # not in rules → skip

        # Collect points from all devices that have this series
        device_point_lists: list[list[dict]] = []
        meta: dict | None = None
        for payload in payloads:
            pts = _series_points(payload, sid)
            if pts:
                device_point_lists.append(pts)
                if meta is None:
                    meta = _series_meta(payload, sid)

        if not device_point_lists or meta is None:
            continue

        # Align on timestamp bucket
        # bucket → list of values (one per device that has a reading there)
        buckets: dict[str, list[float]] = defaultdict(list)
        for pts in device_point_lists:
            for pt in pts:
                buckets[pt["timestamp"]].append(pt["value"])

        points: list[dict] = []
        for ts in sorted(buckets):
            vals = buckets[ts]
            if rule == "sum":
                agg = round(sum(vals), 4)
            else:  # avg — only devices present in this bucket contribute
                agg = round(sum(vals) / len(vals), 4)
            points.append({"timestamp": ts, "value": agg})

        merged.append({**meta, "points": points})

    return merged


# ---------------------------------------------------------------------------
# Carbon result aggregation
# ---------------------------------------------------------------------------

def _aggregate_carbon(payloads: list[dict]) -> dict | None:
    """
    Sum carbon results across devices.

    - co2e_kg and total_kwh are summed.
    - data_quality is demoted to the worst tier across all devices.
    - breakdown rows with the same period are merged by summing kwh/co2e_kg.
    - Returns None if no device produced a carbon result.
    """
    carbon_results = [p["carbon"] for p in payloads if p.get("carbon")]
    if not carbon_results:
        return None

    total_co2e = 0.0
    total_kwh  = 0.0
    ef_values: list[float] = []
    ef_sources: list[str] = []
    worst_quality_idx = 0
    period_kwh:    dict[str, float] = defaultdict(float)
    period_co2e:   dict[str, float] = defaultdict(float)
    any_co2e = False

    for c in carbon_results:
        if c.get("co2e_kg") is not None:
            total_co2e += c["co2e_kg"]
            any_co2e = True
        total_kwh += c.get("total_kwh", 0.0)

        if c.get("co2e_per_kwh") is not None:
            ef_values.append(c["co2e_per_kwh"])
        if c.get("ef_source"):
            ef_sources.append(c["ef_source"])

        # Track worst data_quality
        q = c.get("data_quality", "EF config required")
        try:
            idx = _QUALITY_ORDER.index(q)
        except ValueError:
            idx = len(_QUALITY_ORDER) - 1   # unknown quality → worst
        worst_quality_idx = max(worst_quality_idx, idx)

        for row in c.get("breakdown", []):
            period_kwh[row["period"]]  += row["kwh"]
            period_co2e[row["period"]] += row["co2e_kg"]

    breakdown = [
        {"period": p, "kwh": round(period_kwh[p], 4), "co2e_kg": round(period_co2e[p], 4)}
        for p in sorted(period_kwh)
    ]

    # Use average EF for audit trail (representative value)
    avg_ef = round(sum(ef_values) / len(ef_values), 6) if ef_values else None
    unique_sources = list(dict.fromkeys(ef_sources))  # deduplicated, order preserved
    ef_source_str = "; ".join(unique_sources) if unique_sources else None

    return {
        "co2e_kg":      round(total_co2e, 4) if any_co2e else None,
        "co2e_per_kwh": avg_ef,
        "total_kwh":    round(total_kwh, 4),
        "data_quality": _QUALITY_ORDER[worst_quality_idx],
        "ef_source":    ef_source_str,
        "breakdown":    breakdown,
    }


# ---------------------------------------------------------------------------
# Cost result aggregation
# ---------------------------------------------------------------------------

def _aggregate_cost(payloads: list[dict]) -> dict | None:
    """
    Sum cost results across devices.

    All additive fields are summed.  demand_heatmap cells with matching
    (hour, dow) keys are averaged (avg_kw represents utilisation, not load).
    Returns None if no device produced a cost result.
    """
    cost_results = [p["cost"] for p in payloads if p.get("cost")]
    if not cost_results:
        return None

    cost_total        = 0.0
    peak_cost_total   = 0.0
    offpeak_cost_total = 0.0
    off_hours_kwh     = 0.0
    demand_charge     = 0.0

    has_peak_split  = any(c.get("peak_cost_total") is not None for c in cost_results)
    has_off_hours   = any(c.get("off_hours_kwh")   is not None for c in cost_results)
    has_demand      = any(c.get("demand_charge")    is not None for c in cost_results)

    period_kwh:  dict[str, float] = defaultdict(float)
    period_cost: dict[str, float] = defaultdict(float)

    # Heatmap: (hour, dow) → list of avg_kw values across devices
    heatmap_buckets: dict[tuple[int, str], list[float]] = defaultdict(list)

    for c in cost_results:
        cost_total += c.get("cost_total") or 0.0
        if c.get("peak_cost_total") is not None:
            peak_cost_total += c["peak_cost_total"]
        if c.get("offpeak_cost_total") is not None:
            offpeak_cost_total += c["offpeak_cost_total"]
        if c.get("off_hours_kwh") is not None:
            off_hours_kwh += c["off_hours_kwh"]
        if c.get("demand_charge") is not None:
            demand_charge += c["demand_charge"]

        for row in c.get("cost_breakdown", []):
            period_kwh[row["period"]]  += row["kwh"]
            period_cost[row["period"]] += row["cost_pkr"]

        for cell in c.get("demand_heatmap", []):
            heatmap_buckets[(cell["hour"], cell["dow"])].append(cell["avg_kw"])

    cost_breakdown = [
        {"period": p, "kwh": round(period_kwh[p], 4), "cost_pkr": round(period_cost[p], 2)}
        for p in sorted(period_kwh)
    ]

    demand_heatmap = [
        {
            "hour":   hr,
            "dow":    dow,
            "avg_kw": round(sum(vals) / len(vals), 4),
        }
        for (hr, dow), vals in sorted(heatmap_buckets.items())
    ]

    metric_cards: list[dict] = [
        {
            "id": "cost_total", "label": "Total Energy Cost",
            "value": round(cost_total, 2), "unit": "PKR", "precision": 0,
            "trend": None, "aggregation_method": "sum",
        }
    ]
    if has_demand:
        metric_cards.append({
            "id": "demand_charge", "label": "Demand Charge",
            "value": round(demand_charge, 2), "unit": "PKR", "precision": 0,
            "trend": None, "aggregation_method": "sum",
        })

    return {
        "cost_total":          round(cost_total, 2),
        "cost_breakdown":      cost_breakdown,
        "peak_cost_total":     round(peak_cost_total, 2)    if has_peak_split else None,
        "offpeak_cost_total":  round(offpeak_cost_total, 2) if has_peak_split else None,
        "demand_charge":       round(demand_charge, 2)      if has_demand     else None,
        "off_hours_kwh":       round(off_hours_kwh, 4)      if has_off_hours  else None,
        "demand_heatmap":      demand_heatmap,
        "metric_cards":        metric_cards,
    }


# ---------------------------------------------------------------------------
# Anomaly aggregation
# ---------------------------------------------------------------------------

def _aggregate_anomalies(payloads: list[dict], threshold_pct: float = 2.0) -> dict | None:
    """
    Aggregate M5 phase-imbalance anomaly results across devices.

    Group-level anomaly result shape:
    {
        "phase_imbalance": {
            "max_pct":            float,   # worst reading across all devices
            "avg_pct":            float,   # average of per-device avg_pct values
            "breach_device_count": int,    # devices (not readings) with any breach
            "breach_reading_count": int,   # total readings breaching threshold
            "breaching_devices":  [str],   # device_ids for drill-down
            "threshold_pct":      float,
            "device_count":       int,     # 3-phase devices with current data
        }
    }
    Returns None if no device produced an anomaly result.
    """
    anomaly_results: list[tuple[str, dict]] = []
    for payload in payloads:
        a = payload.get("anomalies")
        if a and a.get("phase_imbalance"):
            device_id = payload.get("meta", {}).get("device_id", "unknown")
            anomaly_results.append((device_id, a["phase_imbalance"]))

    if not anomaly_results:
        return None

    all_max_pcts = [pi["max_pct"] for _, pi in anomaly_results]
    all_avg_pcts = [pi["avg_pct"] for _, pi in anomaly_results]
    total_breach_readings = sum(pi["breach_count"] for _, pi in anomaly_results)

    breaching_devices = [
        did for did, pi in anomaly_results if pi["breach_count"] > 0
    ]

    return {
        "phase_imbalance": {
            "max_pct":             round(max(all_max_pcts), 4),
            "avg_pct":             round(sum(all_avg_pcts) / len(all_avg_pcts), 4),
            "breach_device_count": len(breaching_devices),
            "breach_reading_count": total_breach_readings,
            "breaching_devices":   breaching_devices,
            "threshold_pct":       threshold_pct,
            "device_count":        len(anomaly_results),
        }
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def aggregate_payloads(
    payloads: list[dict],
    scope: Any,             # Scope dataclass — avoids circular import
    *,
    m5_config: dict | None = None,
) -> dict:
    """
    Combine a list of per-device PipelinePayload dicts into a
    GroupPipelinePayload.

    Parameters
    ----------
    payloads   : list of dicts returned by run_pipeline(), one per device.
                 Payloads from failed/skipped devices should be excluded
                 before calling this function.
    scope      : The Scope descriptor that produced this device list.
                 Used only to populate the meta block.
    m5_config  : Optional M5 config forwarded from run_group_pipeline;
                 used to read imbalance_threshold_pct for anomaly grouping.

    Returns
    -------
    GroupPipelinePayload dict (JSON-serialisable).
    Extends PipelinePayload with:
        meta.scope_type       str
        meta.scope_label      str
        meta.device_count     int
        meta.device_ids       list[str]
    MetricCard gains:
        aggregation_method    str  ("sum" | "max" | "wavg")
        contributing_devices  int
    """
    if not payloads:
        raise ValueError("aggregate_payloads requires at least one payload")

    # --- Collect device IDs from payloads (preserves run_group_pipeline order) ---
    device_ids = [p["meta"]["device_id"] for p in payloads]

    # --- Window: overall span across all devices ---
    all_from = [p["meta"]["window"]["from"] for p in payloads]
    all_to   = [p["meta"]["window"]["to"]   for p in payloads]
    window_from = min(all_from)
    window_to   = max(all_to)

    # --- Aggregate metric cards ---
    agg_cards = _aggregate_cards(payloads)

    # --- Merge chart series ---
    agg_series = _merge_series(payloads)

    # --- Aggregate optional modules ---
    agg_carbon    = _aggregate_carbon(payloads)
    agg_cost      = _aggregate_cost(payloads)
    threshold_pct = float((m5_config or {}).get("imbalance_threshold_pct", 2.0))
    agg_anomalies = _aggregate_anomalies(payloads, threshold_pct=threshold_pct)

    # --- Build group payload ---
    return {
        "meta": {
            "scope_type":   scope.scope_type,
            "scope_label":  scope.label,
            "device_count": len(device_ids),
            "device_ids":   device_ids,
            "computed_at":  datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window": {
                "from": window_from,
                "to":   window_to,
            },
        },
        "metric_cards": agg_cards,
        "chart_series": agg_series,
        "carbon":    agg_carbon,
        "anomalies": agg_anomalies,
        "cost":      agg_cost,
    }
