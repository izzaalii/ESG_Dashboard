"""
ESG Pipeline — Core Path: M1 → M2 → M4 → M3
=============================================
Implements the always-runs path + M3 Carbon behind a config flag.
M5 Phase Imbalance + M6 Cost are fully implemented. M7 remains a stub.

Usage:
    from esg_pipeline import run_pipeline

    payload = run_pipeline(records)   # list[dict] matching DeviceRecord schema
    # Returns PipelinePayload dict (JSON-serialisable)
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Types (lightweight dataclasses — no Pydantic dependency required)..
# ---------------------------------------------------------------------------

class ValidationError(ValueError):
    pass


# ---------------------------------------------------------------------------
# M1 — Ingestion & Validation
# ---------------------------------------------------------------------------

VOLTAGE_BOUNDS = {
    "single": (180.0, 260.0),
    "3ph":    (340.0, 460.0),   # line-to-line
}

REQUIRED_FIELDS = {"device_id", "device_name", "timestamp", "interval_s", "kwh", "voltage"}


def _parse_ts(ts: str) -> datetime:
    """Parse ISO-8601 string → timezone-aware datetime (UTC)."""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, AttributeError) as e:
        raise ValidationError(f"Unparseable timestamp '{ts}': {e}") from e


def _detect_phase(record: dict) -> str:
    """Auto-detect single vs 3-phase from presence of per-phase voltages or currents."""
    has_phase_v = all(record.get(k) is not None for k in ("voltage_a", "voltage_b", "voltage_c"))
    has_phase_i = all(record.get(k) is not None for k in ("ca", "cb", "cc"))
    return "3ph" if (has_phase_v or has_phase_i) else "single"


def m1_ingest(records: list[dict]) -> list[dict]:
    """
    M1 — Validate, enrich, and sort records for a single device.

    Rules enforced:
    - Required fields present
    - kWh ≥ 0
    - PF in (0, 1] if present
    - Voltage within plausible range
    - Timestamps parseable and not in future
    - Current phases must be all-or-nothing (ca, cb, cc)
    - Records sorted ascending by timestamp on return
    """
    if not records:
        raise ValidationError("Empty record list")

    now_utc = datetime.now(tz=timezone.utc)
    cleaned: list[dict] = []

    for i, raw in enumerate(records):
        # --- Required fields ---
        missing = REQUIRED_FIELDS - raw.keys()
        if missing:
            raise ValidationError(f"Record {i}: missing required fields {missing}")

        r = dict(raw)  # shallow copy — don't mutate caller's data

        # --- Phase detection ---
        r["phase_type"] = _detect_phase(r)

        # --- Timestamp ---
        ts = _parse_ts(r["timestamp"])
        if ts > now_utc:
            raise ValidationError(f"Record {i}: timestamp {r['timestamp']} is in the future")
        r["_ts"] = ts  # attach parsed datetime for internal use

        # --- kWh ---
        if r["kwh"] < 0:
            raise ValidationError(f"Record {i}: kwh must be ≥ 0, got {r['kwh']}")

        # --- Power Factor ---
        if r.get("pf") is not None:
            if not (0 < r["pf"] <= 1.0):
                raise ValidationError(f"Record {i}: pf must be in (0,1], got {r['pf']}")

        # --- Voltage ---
        lo, hi = VOLTAGE_BOUNDS[r["phase_type"]]
        if not (lo <= r["voltage"] <= hi):
            raise ValidationError(
                f"Record {i}: voltage {r['voltage']} V out of range [{lo}, {hi}] "
                f"for {r['phase_type']} device"
            )

        # --- Phase currents: all-or-nothing (3-phase only) ---
        # Single-phase: ca alone is valid. 3-phase: ca/cb/cc must be all present or all absent.
        if r["phase_type"] == "3ph":
            phase_i = [r.get("ca"), r.get("cb"), r.get("cc")]
            if any(v is not None for v in phase_i) and not all(v is not None for v in phase_i):
                raise ValidationError(f"Record {i}: 3-phase device requires ca/cb/cc all present or all absent")

        cleaned.append(r)

    # Sort ascending by timestamp
    cleaned.sort(key=lambda x: x["_ts"])

    # Validate monotonic kWh within device session
    for j in range(1, len(cleaned)):
        if cleaned[j]["kwh"] < cleaned[j - 1]["kwh"]:
            # Meter rollover is legitimate; emit warning but don't crash
            # Mark the record so downstream can handle rollover
            cleaned[j]["_kwh_rollover"] = True

    return cleaned


# ---------------------------------------------------------------------------
# M2 — Consumption: kWh delta, rollups
# ---------------------------------------------------------------------------

def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _bucket_key(dt: datetime, resolution: str) -> str:
    """Return a string bucket key for a given resolution."""
    if resolution == "15min":
        # Floor to nearest 15-minute slot
        m = (dt.minute // 15) * 15
        return dt.strftime(f"%Y-%m-%dT%H:{m:02d}:00Z")
    if resolution == "1h":
        return dt.strftime("%Y-%m-%dT%H:00:00Z")
    if resolution == "1d":
        return dt.strftime("%Y-%m-%dT00:00:00Z")
    if resolution == "1mo":
        return dt.strftime("%Y-%m-01T00:00:00Z")
    raise ValueError(f"Unknown resolution: {resolution}")


def m2_consumption(records: list[dict]) -> dict:
    """
    M2 — Compute kWh deltas and roll up into hourly / daily / monthly series.

    Returns:
        {
            metric_cards: [total_kwh, peak_kw],
            chart_series: [kwh_15min, kwh_hourly, kwh_daily, kwh_monthly]
        }
    """
    if len(records) < 2:
        return {"metric_cards": [], "chart_series": []}

    # --- Compute per-interval kWh deltas ---
    deltas: list[dict] = []
    for j in range(1, len(records)):
        prev, curr = records[j - 1], records[j]
        kwh_delta = curr["kwh"] - prev["kwh"]
        if curr.get("_kwh_rollover") or kwh_delta < 0:
            kwh_delta = 0.0  # skip rollover interval
        dt_hours = (curr["_ts"] - prev["_ts"]).total_seconds() / 3600.0
        kw = (kwh_delta / dt_hours) if dt_hours > 0 else 0.0
        deltas.append({
            "timestamp": _iso(curr["_ts"]),
            "kwh": round(kwh_delta, 4),
            "kw":  round(kw, 4),
        })

    # --- Build rollup buckets ---
    def rollup(resolution: str) -> list[dict]:
        buckets: dict[str, float] = {}
        for d in deltas:
            ts = datetime.fromisoformat(d["timestamp"].replace("Z", "+00:00"))
            key = _bucket_key(ts, resolution)
            buckets[key] = round(buckets.get(key, 0.0) + d["kwh"], 4)
        return [{"timestamp": k, "value": v} for k, v in sorted(buckets.items())]

    total_kwh = round(sum(d["kwh"] for d in deltas), 4)
    peak_kw   = round(max((d["kw"] for d in deltas), default=0.0), 4)

    metric_cards = [
        {
            "id": "total_kwh", "label": "Total Consumption",
            "value": total_kwh, "unit": "kWh", "precision": 2, "trend": None,
        },
        {
            "id": "peak_kw", "label": "Peak Demand",
            "value": peak_kw, "unit": "kW", "precision": 2, "trend": None,
        },
    ]

    chart_series = [
        {
            "id": "kwh_15min",  "label": "Consumption (15 min)",
            "unit": "kWh", "resolution": "15min",
            "points": rollup("15min"),
        },
        {
            "id": "kwh_hourly", "label": "Consumption (Hourly)",
            "unit": "kWh", "resolution": "1h",
            "points": rollup("1h"),
        },
        {
            "id": "kwh_daily",  "label": "Consumption (Daily)",
            "unit": "kWh", "resolution": "1d",
            "points": rollup("1d"),
        },
        {
            "id": "kwh_monthly", "label": "Consumption (Monthly)",
            "unit": "kWh", "resolution": "1mo",
            "points": rollup("1mo"),
        },
    ]

    return {"metric_cards": metric_cards, "chart_series": chart_series}


# ---------------------------------------------------------------------------
# M4 — Power Quality: kVA, PF, voltages
# ---------------------------------------------------------------------------

def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def m4_power_quality(records: list[dict]) -> dict:
    """
    M4 — Compute apparent power (kVA), power factor, and per-phase voltages.

    - kVA = V × I / 1000  (single-phase)
    - kVA = √3 × V_line × I_avg / 1000  (3-phase, where V_line = voltage field)
    - PF  = kW / kVA  (kW from record's kw field if available, else skipped)
    - Voltage imbalance % = (max deviation from V_avg) / V_avg × 100  (3-phase only)

    Returns:
        {
            metric_cards: [avg_pf, avg_kva, voltage_avg, voltage_imbalance_pct?],
            chart_series: [kva_15min, pf_15min, voltage_15min, voltage_a/b/c_15min?]
        }
    """
    if not records:
        return {"metric_cards": [], "chart_series": []}

    is_3ph = records[0].get("phase_type") == "3ph"

    kva_points:  list[dict] = []
    pf_points:   list[dict] = []
    v_points:    list[dict] = []
    va_points:   list[dict] = []
    vb_points:   list[dict] = []
    vc_points:   list[dict] = []
    imb_vals:    list[float] = []

    for r in records:
        ts = _iso(r["_ts"])
        v  = r["voltage"]

        # --- kVA ---
        kva: float | None = r.get("kva")
        if kva is None:
            if is_3ph and all(r.get(k) is not None for k in ("ca", "cb", "cc")):
                i_avg = _avg([r["ca"], r["cb"], r["cc"]])
                kva = round(math.sqrt(3) * v * i_avg / 1000.0, 4)
            elif not is_3ph and r.get("ca") is not None:
                kva = round(v * r["ca"] / 1000.0, 4)

        if kva is not None:
            kva_points.append({"timestamp": ts, "value": kva})

        # --- PF ---
        pf: float | None = r.get("pf")
        if pf is None and kva and kva > 0 and r.get("kw") is not None:
            pf = round(min(r["kw"] / kva, 1.0), 4)
        if pf is not None:
            pf_points.append({"timestamp": ts, "value": pf})

        # --- Voltage ---
        v_points.append({"timestamp": ts, "value": round(v, 2)})

        # --- Per-phase voltage (3-phase) ---
        if is_3ph:
            va, vb, vc = r.get("voltage_a"), r.get("voltage_b"), r.get("voltage_c")
            if all(x is not None for x in (va, vb, vc)):
                va_points.append({"timestamp": ts, "value": round(va, 2)})
                vb_points.append({"timestamp": ts, "value": round(vb, 2)})
                vc_points.append({"timestamp": ts, "value": round(vc, 2)})
                v_avg = _avg([va, vb, vc])
                if v_avg > 0:
                    imb = max(abs(va - v_avg), abs(vb - v_avg), abs(vc - v_avg)) / v_avg * 100
                    imb_vals.append(imb)

    # --- Aggregate scalars ---
    avg_pf  = round(_avg([p["value"] for p in pf_points]), 4) if pf_points else None
    avg_kva = round(_avg([p["value"] for p in kva_points]), 4) if kva_points else None
    avg_v   = round(_avg([p["value"] for p in v_points]), 2)  if v_points  else None
    avg_imb = round(_avg(imb_vals), 2) if imb_vals else None

    metric_cards = []
    if avg_pf is not None:
        metric_cards.append({
            "id": "avg_pf", "label": "Average Power Factor",
            "value": avg_pf, "unit": "", "precision": 3, "trend": None,
        })
    if avg_kva is not None:
        metric_cards.append({
            "id": "avg_kva", "label": "Average Apparent Power",
            "value": avg_kva, "unit": "kVA", "precision": 2, "trend": None,
        })
    if avg_v is not None:
        metric_cards.append({
            "id": "voltage_avg", "label": "Average Voltage",
            "value": avg_v, "unit": "V", "precision": 1, "trend": None,
        })
    if avg_imb is not None:
        metric_cards.append({
            "id": "voltage_imbalance_pct", "label": "Voltage Imbalance",
            "value": avg_imb, "unit": "%", "precision": 2, "trend": None,
        })

    chart_series = [
        {"id": "voltage_15min", "label": "Voltage", "unit": "V",
         "resolution": "15min", "points": v_points},
    ]
    if kva_points:
        chart_series.append({
            "id": "kva_15min", "label": "Apparent Power", "unit": "kVA",
            "resolution": "15min", "points": kva_points,
        })
    if pf_points:
        chart_series.append({
            "id": "pf_15min", "label": "Power Factor", "unit": "",
            "resolution": "15min", "points": pf_points,
        })
    if is_3ph and va_points:
        chart_series += [
            {"id": "voltage_a_15min", "label": "Voltage Phase A", "unit": "V",
             "resolution": "15min", "points": va_points},
            {"id": "voltage_b_15min", "label": "Voltage Phase B", "unit": "V",
             "resolution": "15min", "points": vb_points},
            {"id": "voltage_c_15min", "label": "Voltage Phase C", "unit": "V",
             "resolution": "15min", "points": vc_points},
        ]

    return {"metric_cards": metric_cards, "chart_series": chart_series}


# ---------------------------------------------------------------------------
# M3 — Carbon Emissions
# ---------------------------------------------------------------------------

# Known grid emission factors (kgCO₂e per kWh) — extend as needed.
# Source: IEA 2023 / national grid averages.
KNOWN_EMISSION_FACTORS: dict[str, float] = {
    "PK":  0.402,   # Pakistan national grid
    "IN":  0.716,   # India
    "US":  0.386,   # USA average
    "UK":  0.233,   # United Kingdom
    "DE":  0.364,   # Germany
    "AU":  0.590,   # Australia
    "AE":  0.450,   # UAE
    "SG":  0.408,   # Singapore
}

# Data-quality tier labels (Watershed-inspired badge pattern)
class DataQuality:
    HIGH      = "verified"           # EF from audited/certified source
    MEDIUM    = "estimated"          # EF from known regional average
    LOW       = "EF config required" # No EF available — co2e_kg cannot be computed


def m3_carbon(m2_out: dict, config: dict | None = None) -> dict:
    """
    M3 — Carbon emissions calculation, gated on config.

    Config keys (all optional):
        emission_factor   float  kgCO₂e per kWh  (highest priority if supplied)
        grid_region       str    ISO country/region code — used to look up a
                                 built-in factor when emission_factor is absent
        ef_source         str    free-text label for the EF source (audit trail)
        ef_verified       bool   True → data_quality = "verified"

    Returns
    -------
    {
        co2e_kg        : float | None,
        co2e_per_kwh   : float | None,   # EF used (for audit trail)
        total_kwh      : float,
        data_quality   : str,            # "verified" | "estimated" | "EF config required"
        ef_source      : str | None,
        breakdown: [                     # per-period series (mirrors kwh_monthly resolution)
            { period, kwh, co2e_kg }
        ]
    }
    """
    # --- Pull total kWh and monthly series from M2 output ---
    total_kwh_card = next(
        (c for c in m2_out.get("metric_cards", []) if c["id"] == "total_kwh"), None
    )
    total_kwh = total_kwh_card["value"] if total_kwh_card else 0.0

    monthly_series = next(
        (s for s in m2_out.get("chart_series", []) if s["id"] == "kwh_monthly"), None
    )

    # --- Resolve emission factor ---
    ef: float | None = None
    ef_source: str | None = None
    data_quality: str = DataQuality.LOW

    if config:
        direct_ef = config.get("emission_factor")
        region    = config.get("grid_region", "").upper()
        verified  = config.get("ef_verified", False)
        ef_source = config.get("ef_source")

        if direct_ef is not None:
            try:
                ef = float(direct_ef)
                if ef <= 0:
                    raise ValueError("emission_factor must be > 0")
                data_quality = DataQuality.HIGH if verified else DataQuality.MEDIUM
                ef_source = ef_source or "user-supplied"
            except (TypeError, ValueError) as exc:
                return {
                    "co2e_kg":      None,
                    "co2e_per_kwh": None,
                    "total_kwh":    total_kwh,
                    "data_quality": f"invalid emission_factor: {exc}",
                    "ef_source":    None,
                    "breakdown":    [],
                }
        elif region in KNOWN_EMISSION_FACTORS:
            ef = KNOWN_EMISSION_FACTORS[region]
            data_quality = DataQuality.MEDIUM
            ef_source = ef_source or f"IEA 2023 grid average ({region})"

    # --- No EF available ---
    if ef is None:
        return {
            "co2e_kg":      None,
            "co2e_per_kwh": None,
            "total_kwh":    total_kwh,
            "data_quality": DataQuality.LOW,
            "ef_source":    None,
            "breakdown":    [],
        }

    # --- Compute totals ---
    co2e_total = round(total_kwh * ef, 4)

    # --- Per-period breakdown (monthly granularity) ---
    breakdown = []
    if monthly_series:
        for point in monthly_series["points"]:
            kwh_period   = point["value"]
            co2e_period  = round(kwh_period * ef, 4)
            breakdown.append({
                "period":   point["timestamp"][:7],   # "YYYY-MM"
                "kwh":      kwh_period,
                "co2e_kg":  co2e_period,
            })

    return {
        "co2e_kg":      co2e_total,
        "co2e_per_kwh": ef,
        "total_kwh":    total_kwh,
        "data_quality": data_quality,
        "ef_source":    ef_source,
        "breakdown":    breakdown,
    }


# ---------------------------------------------------------------------------
# M5 — Phase Current Imbalance (3-phase devices only)
# ---------------------------------------------------------------------------

# NEMA standard: imbalance > 2% is actionable
DEFAULT_IMBALANCE_THRESHOLD_PCT = 2.0


def m5_anomalies(records: list[dict], config: dict | None = None) -> dict | None:
    """
    M5 — Phase current imbalance analysis for 3-phase devices.

    Only runs when:
      - device phase_type == "3ph"  (detected by M1)
      - ca, cb, cc are all present on at least one record

    For single-phase devices or missing phase currents → returns None.

    NEMA imbalance method (per reading):
        I_avg          = (ca + cb + cc) / 3
        imbalance_pct  = max(|ca−I_avg|, |cb−I_avg|, |cc−I_avg|) / I_avg × 100

    Config keys:
        imbalance_threshold_pct  float  default 2.0  (NEMA standard)

    Returns
    -------
    {
        "phase_imbalance": {
            "series":        [{"timestamp": str, "value": float}],
            "avg_pct":       float,
            "max_pct":       float,
            "breach_count":  int,    # readings where imbalance_pct > threshold
            "threshold_pct": float,
        }
    }
    or None when module cannot run.
    """
    if not records:
        return None

    # Gate 1 — must be 3-phase
    if records[0].get("phase_type") != "3ph":
        return None

    # Gate 2 — at least one record must have all three phase currents
    has_currents = any(
        all(r.get(k) is not None for k in ("ca", "cb", "cc"))
        for r in records
    )
    if not has_currents:
        return None

    threshold = float(
        (config or {}).get("imbalance_threshold_pct", DEFAULT_IMBALANCE_THRESHOLD_PCT)
    )

    series: list[dict] = []
    breach_count = 0

    for r in records:
        ca, cb, cc = r.get("ca"), r.get("cb"), r.get("cc")
        if any(v is None for v in (ca, cb, cc)):
            continue  # skip records missing phase currents

        i_avg = (ca + cb + cc) / 3.0
        if i_avg == 0:
            continue  # avoid divide-by-zero on zero-current records

        imb_pct = round(
            max(abs(ca - i_avg), abs(cb - i_avg), abs(cc - i_avg)) / i_avg * 100,
            4,
        )
        ts = _iso(r["_ts"])
        series.append({"timestamp": ts, "value": imb_pct})

        if imb_pct > threshold:
            breach_count += 1

    if not series:
        return None  # had phase_type==3ph but no usable current readings

    values  = [pt["value"] for pt in series]
    avg_pct = round(sum(values) / len(values), 4)
    max_pct = round(max(values), 4)

    return {
        "phase_imbalance": {
            "series":        series,
            "avg_pct":       avg_pct,
            "max_pct":       max_pct,
            "breach_count":  breach_count,
            "threshold_pct": threshold,
        }
    }


# ---------------------------------------------------------------------------
# M6, M7 — remaining stubs header (M6 implemented below)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# M6 — Cost & Demand Analysis
# ---------------------------------------------------------------------------

DOW_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]  # Monday=0 per isoweekday()-1


def m6_cost(m2_out: dict, config: dict | None = None) -> dict | None:
    """
    M6 — Cost and demand analysis, gated on config.

    Config keys:
        tariff_per_kwh         float   PKR per kWh (required for any cost output)
        peak_tariff_per_kwh    float   Higher rate for peak hours (optional)
        peak_hours             [int]   Hours 0-23 considered peak (optional)
        demand_charge_per_kw   float   Monthly demand charge per kW (optional)
        operational_schedule   dict    {start_hour, end_hour, days} for off-hours calc (optional)

    Returns None when no config supplied.
    Returns CostResult dict otherwise (co2e-style null pattern for missing sub-configs).

    Output shape:
        {
            cost_total        : float | None,   # total PKR for period
            cost_breakdown    : [{period, kwh, cost_pkr}],
            peak_cost_total   : float | None,   # PKR from peak-rate hours only
            offpeak_cost_total: float | None,
            demand_charge     : float | None,   # PKR demand charge (if configured)
            off_hours_kwh     : float | None,   # kWh consumed outside operational schedule
            demand_heatmap    : [{hour, dow, avg_kw}],  # max 168 cells
            metric_cards      : [MetricCard],   # cost_total card (+ demand_charge card if set)
        }
    """
    # --- No config → null stub ---
    if not config:
        return None

    tariff = config.get("tariff_per_kwh")
    if tariff is None:
        return None   # tariff is the minimum required key

    tariff = float(tariff)

    # --- Pull M2 series we need ---
    def _get_series(sid: str) -> list[dict]:
        s = next((s for s in m2_out.get("chart_series", []) if s["id"] == sid), None)
        return s["points"] if s else []

    def _get_card(cid: str) -> float | None:
        c = next((c for c in m2_out.get("metric_cards", []) if c["id"] == cid), None)
        return c["value"] if c else None

    points_15min  = _get_series("kwh_15min")   # [{timestamp, value}]
    points_monthly = _get_series("kwh_monthly")
    peak_kw_val   = _get_card("peak_kw")

    # --- Optional config ---
    peak_tariff    = config.get("peak_tariff_per_kwh")
    peak_hours_set = set(config.get("peak_hours") or [])
    demand_rate    = config.get("demand_charge_per_kw")
    schedule       = config.get("operational_schedule")

    # ---- Per-interval cost with optional peak/off-peak split ----
    total_cost     = 0.0
    peak_cost      = 0.0
    offpeak_cost   = 0.0
    off_hours_kwh  = 0.0
    has_peak_split = peak_tariff is not None and peak_hours_set

    # Build schedule set: set of (dow_str, hour) that are operational
    schedule_slots: set[tuple[str, int]] | None = None
    if schedule:
        sched_days  = set(schedule.get("days") or [])
        sched_start = int(schedule.get("start_hour", 0))
        sched_end   = int(schedule.get("end_hour", 24))
        schedule_slots = set()
        for d in sched_days:
            for h in range(sched_start, sched_end):
                schedule_slots.add((d, h))

    # Demand heatmap: key=(hour, dow) → list of kw values
    heatmap_buckets: dict[tuple[int, str], list[float]] = {}

    for pt in points_15min:
        ts  = datetime.fromisoformat(pt["timestamp"].replace("Z", "+00:00"))
        kwh = pt["value"]
        hr  = ts.hour
        dow = DOW_NAMES[ts.isoweekday() - 1]   # Mon=0
        kw_inst = kwh * 4   # 15-min interval → kW (kwh / 0.25h)

        # Cost calculation
        if has_peak_split and hr in peak_hours_set:
            rate = float(peak_tariff)
            peak_cost += kwh * rate
        else:
            rate = tariff
            offpeak_cost += kwh * rate
        total_cost += kwh * rate

        # Off-hours kWh
        if schedule_slots is not None:
            if (dow, hr) not in schedule_slots:
                off_hours_kwh += kwh

        # Heatmap accumulation
        key = (hr, dow)
        heatmap_buckets.setdefault(key, []).append(kw_inst)

    total_cost   = round(total_cost, 2)
    peak_cost    = round(peak_cost, 2)    if has_peak_split else None
    offpeak_cost = round(offpeak_cost, 2) if has_peak_split else None
    off_hours_kwh = round(off_hours_kwh, 4) if schedule_slots is not None else None

    # --- Demand charge ---
    demand_charge: float | None = None
    if demand_rate is not None and peak_kw_val is not None:
        demand_charge = round(float(demand_rate) * peak_kw_val, 2)

    # --- Monthly cost breakdown (mirrors M3 breakdown pattern) ---
    cost_breakdown = []
    for pt in points_monthly:
        period_kwh  = pt["value"]
        period_cost = round(period_kwh * tariff, 2)
        cost_breakdown.append({
            "period":   pt["timestamp"][:7],
            "kwh":      period_kwh,
            "cost_pkr": period_cost,
        })

    # --- Demand heatmap: flatten to [{hour, dow, avg_kw}] ---
    demand_heatmap = [
        {
            "hour":   hr,
            "dow":    dow,
            "avg_kw": round(sum(vals) / len(vals), 4),
        }
        for (hr, dow), vals in sorted(heatmap_buckets.items())
    ]

    # --- Metric cards emitted by M6 ---
    metric_cards = [
        {
            "id":        "cost_total",
            "label":     "Total Energy Cost",
            "value":     total_cost,
            "unit":      "PKR",
            "precision": 0,
            "trend":     None,
        }
    ]
    if demand_charge is not None:
        metric_cards.append({
            "id":        "demand_charge",
            "label":     "Demand Charge",
            "value":     demand_charge,
            "unit":      "PKR",
            "precision": 0,
            "trend":     None,
        })

    return {
        "cost_total":         total_cost,
        "cost_breakdown":     cost_breakdown,
        "peak_cost_total":    peak_cost,
        "offpeak_cost_total": offpeak_cost,
        "demand_charge":      demand_charge,
        "off_hours_kwh":      off_hours_kwh,
        "demand_heatmap":     demand_heatmap,
        "metric_cards":       metric_cards,
    }


def m6_cost_stub(_records: list[dict], _config: dict | None = None) -> None:
    """Legacy stub signature — kept for reference. Use m6_cost(m2_out, config) instead."""
    return None


def m7_report(_payload: dict, _config: dict | None = None) -> None:
    """Stub — Report generation module not yet implemented. Returns None."""
    return None


# ---------------------------------------------------------------------------
# Trend computation — period-over-period delta helper
# ---------------------------------------------------------------------------

#: Cards eligible for trend computation, in pipeline id → label order.
TREND_CARD_IDS = {"total_kwh", "peak_kw", "avg_pf", "avg_kva", "voltage_avg", "co2e_kg"}


def _compute_trend(current_value: float, previous_value: float) -> dict | None:
    """
    Compute a MetricCard.trend object from two scalar values.

    Rules
    -----
    - If previous_value == 0 → cannot compute a meaningful percentage; return None.
    - direction "up"   if delta_pct >  1.0 %
    - direction "down" if delta_pct < -1.0 %
    - direction "flat" otherwise

    Returns
    -------
    { delta: float, delta_pct: float, direction: str } or None
    """
    if previous_value == 0:
        return None

    delta     = round(current_value - previous_value, 6)
    delta_pct = round(delta / previous_value * 100, 4)

    if delta_pct > 1.0:
        direction = "up"
    elif delta_pct < -1.0:
        direction = "down"
    else:
        direction = "flat"

    return {"delta": delta, "delta_pct": delta_pct, "direction": direction}


def _apply_trends(
    cards: list[dict],
    prev_card_map: dict[str, float],
) -> list[dict]:
    """
    Return a new list of metric cards with trend fields populated where possible.

    Parameters
    ----------
    cards         : current period metric cards (will not be mutated)
    prev_card_map : {card_id: value} from the previous period
    """
    result = []
    for card in cards:
        cid = card["id"]
        if cid in TREND_CARD_IDS and cid in prev_card_map:
            trend = _compute_trend(card["value"], prev_card_map[cid])
            result.append({**card, "trend": trend})
        else:
            result.append(card)
    return result


# ---------------------------------------------------------------------------
# run_pipeline — Public entry point
# ---------------------------------------------------------------------------

def run_pipeline(
    records: list[dict],
    *,
    previous_records: list[dict] | None = None,
    m3_config: dict | None = None,
    m5_config: dict | None = None,
    m6_config: dict | None = None,
) -> dict:
    """
    Run the full ESG pipeline for a single device's record set.

    Parameters
    ----------
    records          : list of raw device record dicts (DeviceRecord schema)
    previous_records : optional same-schema records from the prior period.
                       When provided, M1→M2→M4 (and M3 if m3_config is set) are
                       run on this set and the resulting metric-card values are
                       used to populate ``trend`` on every matching current card.
                       Cards whose previous value is zero, or cards with no prior
                       counterpart, keep ``trend: null``.
    m3_config        : optional carbon module config
                       Keys: emission_factor (float, kgCO₂e/kWh), grid_region (str, e.g. "PK"),
                             ef_source (str), ef_verified (bool)
    m5_config        : optional anomaly detection config
                       Keys: imbalance_threshold_pct (float, default 2.0)
    m6_config        : optional cost/tariff config

    Returns
    -------
    PipelinePayload dict — JSON-serialisable, matches the Data Contract v1.0.
    """
    # --- M1: Ingest & validate ---
    clean = m1_ingest(records)

    # Infer window and device identity from validated records
    first, last = clean[0], clean[-1]
    device_id   = first["device_id"]
    device_name = first["device_name"]
    phase_type  = first["phase_type"]

    # --- M2: Consumption ---
    m2_out = m2_consumption(clean)

    # --- M4: Power Quality ---
    m4_out = m4_power_quality(clean)

    # --- M3: Carbon | M5: Anomalies | M6: Cost | M7: stub ---
    carbon    = m3_carbon(m2_out, m3_config)
    anomalies = m5_anomalies(clean, m5_config)
    cost_result = m6_cost(m2_out, m6_config)
    _         = m7_report({}, None)       # No-op for now

    # --- Build previous-period card map (for trend computation) ---
    prev_card_map: dict[str, float] = {}
    if previous_records is not None:
        prev_clean  = m1_ingest(previous_records)
        prev_m2_out = m2_consumption(prev_clean)
        prev_m4_out = m4_power_quality(prev_clean)
        prev_carbon = m3_carbon(prev_m2_out, m3_config)

        # Collect all previous card values indexed by id
        for card in prev_m2_out["metric_cards"] + prev_m4_out["metric_cards"]:
            prev_card_map[card["id"]] = card["value"]

        # Add carbon card (co2e_kg) if M3 produced a result for both periods
        if prev_carbon.get("co2e_kg") is not None:
            prev_card_map["co2e_kg"] = prev_carbon["co2e_kg"]

    # --- Assemble payload ---
    # Merge M6 metric cards (cost_total, demand_charge) into top-level cards if M6 ran
    m6_cards  = cost_result["metric_cards"] if cost_result else []
    raw_cards = m2_out["metric_cards"] + m4_out["metric_cards"] + m6_cards

    # Attach co2e_kg as a virtual card so _apply_trends can match it
    if carbon.get("co2e_kg") is not None:
        raw_cards = raw_cards + [{
            "id":        "co2e_kg",
            "label":     "Total Carbon Emissions",
            "value":     carbon["co2e_kg"],
            "unit":      "kgCO₂e",
            "precision": 2,
            "trend":     None,
        }]

    # Apply trends (no-op when prev_card_map is empty)
    all_cards  = _apply_trends(raw_cards, prev_card_map) if prev_card_map else raw_cards
    all_series = m2_out["chart_series"] + m4_out["chart_series"]

    payload: dict[str, Any] = {
        "meta": {
            "device_id":   device_id,
            "device_name": device_name,
            "phase_type":  phase_type,
            "computed_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window": {
                "from": _iso(first["_ts"]),
                "to":   _iso(last["_ts"]),
            },
        },
        "metric_cards": all_cards,
        "chart_series": all_series,
        "carbon":    carbon,
        "anomalies": anomalies,
        "cost":      cost_result,
    }

    return payload


# ---------------------------------------------------------------------------
# Quick smoke-test (run: python esg_pipeline.py)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Multi-Device Site Aggregation Layer
# ---------------------------------------------------------------------------
#
# Design goals
# ─────────────
# 1. Accept a *mixed* list of raw records from any number of devices.
# 2. Classify each record as a Power Meter (PM) or Temperature Monitor (TM)
#    by field signature — no new required schema fields.
# 3. Aggregate PM records across devices into one synthetic per-bucket record
#    that M1→M6 can process unchanged.
# 4. Aggregate TM records into a parallel environmental series.
# 5. Expose a single new public entry point: run_site_pipeline().
#    The existing run_pipeline() is NOT modified.
#
# Device classification heuristic
# ────────────────────────────────
# PM  — has "kwh" field (energy meter, required by M1)
# TM  — has at least one of: "temperature", "humidity", "conductivity"
#        and does NOT have "kwh"
# Mixed records (both kwh + temperature) are treated as PM; TM fields are
# forwarded to the environmental aggregation pass.

# TM field names we recognise
_TM_FIELDS = ("temperature", "humidity", "conductivity")


def _classify_record(record: dict) -> str:
    """Return "pm" or "tm" based on field signature."""
    if "kwh" in record:
        return "pm"
    if any(record.get(f) is not None for f in _TM_FIELDS):
        return "tm"
    return "pm"  # default — will be validated by M1


def _floor_15min(dt: datetime) -> datetime:
    """Floor a datetime to the nearest 15-minute boundary."""
    return dt.replace(minute=(dt.minute // 15) * 15, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# PM aggregation
# ---------------------------------------------------------------------------

def _aggregate_pm_records(pm_records: list[dict], site_id: str, site_name: str) -> list[dict]:
    """
    Merge per-device PM records into one synthetic per-bucket record.

    Aggregation rules per 15-min bucket:
    - kwh       : sum across all devices (site total consumption)
    - voltage   : average across devices
    - pf        : average across devices (only where present)
    - kva       : sum across devices (only where present)
    - kw        : sum across devices (only where present)
    - ca/cb/cc  : sum across devices that are 3-phase (site-level phase load)
    - voltage_a/b/c : average across 3-phase devices

    The synthetic record carries:
    - device_id   = site_id
    - device_name = site_name
    - interval_s  = 900  (15-min fixed after bucketing)
    - timestamp   = bucket start ISO string
    """
    if not pm_records:
        return []

    # Parse timestamps once; floor each to 15-min bucket
    parsed: list[tuple[datetime, dict]] = []
    for r in pm_records:
        try:
            ts = _parse_ts(r["timestamp"])
        except ValidationError:
            continue
        parsed.append((_floor_15min(ts), r))

    if not parsed:
        return []

    # Group by (bucket_ts, device_id) — take last reading per device per bucket
    # to avoid double-counting cumulative meters within the same slot.
    DeviceBucket = tuple  # (bucket_dt, device_id)
    per_device: dict[DeviceBucket, dict] = {}
    for bucket_ts, r in parsed:
        key = (bucket_ts, r.get("device_id", "unknown"))
        per_device[key] = r  # last record wins within bucket

    # Now bucket by timestamp only → list of device records per slot
    by_bucket: dict[datetime, list[dict]] = {}
    for (bucket_ts, _dev_id), r in per_device.items():
        by_bucket.setdefault(bucket_ts, []).append(r)

    # --- Compute per-device kWh deltas across sorted buckets ---------------
    # Each device's raw kwh is a cumulative meter reading.  We need the
    # per-bucket delta (kWh consumed in that slot) so we can sum across
    # devices correctly without double-counting the absolute meter offset.
    sorted_buckets = sorted(by_bucket.keys())
    device_prev_kwh: dict[str, float] = {}  # last seen kwh per device

    # First pass: annotate each record in each bucket with its delta
    bucket_deltas: dict[datetime, dict[str, float]] = {}   # bucket → {dev_id: delta_kwh}
    for bucket_ts in sorted_buckets:
        bucket_deltas[bucket_ts] = {}
        for r in by_bucket[bucket_ts]:
            dev_id = r.get("device_id", "unknown")
            raw_kwh = r.get("kwh", 0.0)
            prev = device_prev_kwh.get(dev_id)
            if prev is None:
                # First reading for this device — delta is 0 (no prior bucket)
                delta = 0.0
            else:
                delta = max(raw_kwh - prev, 0.0)   # guard against rollover
            device_prev_kwh[dev_id] = raw_kwh
            bucket_deltas[bucket_ts][dev_id] = delta

    # Build one synthetic record per bucket
    synthetic: list[dict] = []
    cumulative_kwh = 0.0

    for bucket_ts in sorted_buckets:
        recs = by_bucket[bucket_ts]
        deltas = bucket_deltas[bucket_ts]

        # Sum kWh deltas across all devices for this bucket
        kwh_delta_sum = round(sum(deltas.values()), 4)
        cumulative_kwh = round(cumulative_kwh + kwh_delta_sum, 4)

        # --- Averaged / summed fields ---
        def _mean_field(field: str) -> float | None:
            vals = [r[field] for r in recs if r.get(field) is not None]
            return round(sum(vals) / len(vals), 6) if vals else None

        def _sum_field(field: str) -> float | None:
            vals = [r[field] for r in recs if r.get(field) is not None]
            return round(sum(vals), 6) if vals else None

        voltage = _mean_field("voltage")
        pf      = _mean_field("pf")
        kva     = _sum_field("kva")
        kw      = _sum_field("kw")

        # Phase currents — only sum across devices that have all three
        phase_recs = [r for r in recs if all(r.get(k) is not None for k in ("ca", "cb", "cc"))]
        ca = round(sum(r["ca"] for r in phase_recs), 6) if phase_recs else None
        cb = round(sum(r["cb"] for r in phase_recs), 6) if phase_recs else None
        cc = round(sum(r["cc"] for r in phase_recs), 6) if phase_recs else None

        # Per-phase voltages
        va = _mean_field("voltage_a")
        vb = _mean_field("voltage_b")
        vc = _mean_field("voltage_c")

        rec: dict = {
            "device_id":   site_id,
            "device_name": site_name,
            "timestamp":   _iso(bucket_ts),
            "interval_s":  900,
            "kwh":         cumulative_kwh,   # monotonically increasing — M1-compatible
            "voltage":     voltage if voltage is not None else 230.0,
        }
        if pf  is not None: rec["pf"]  = pf
        if kva is not None: rec["kva"] = kva
        if kw  is not None: rec["kw"]  = kw
        if ca  is not None:
            rec["ca"] = ca
            rec["cb"] = cb
            rec["cc"] = cc
        if va is not None:
            rec["voltage_a"] = va
            rec["voltage_b"] = vb
            rec["voltage_c"] = vc

        synthetic.append(rec)

    return synthetic


# ---------------------------------------------------------------------------
# TM aggregation
# ---------------------------------------------------------------------------

def _aggregate_tm_records(tm_records: list[dict]) -> dict:
    """
    Aggregate temperature monitor records into an environmental result dict.

    Returns
    -------
    {
        "temperature_avg":   float | None,   # °C average over window
        "temperature_min":   float | None,
        "temperature_max":   float | None,
        "humidity_avg":      float | None,   # % RH average
        "conductivity_avg":  float | None,   # µS/cm average
        "series": {
            "temperature":  [{"timestamp": str, "value": float}],
            "humidity":     [{"timestamp": str, "value": float}],
            "conductivity": [{"timestamp": str, "value": float}],
        },
        "device_count": int,
        "record_count":  int,
    }
    or None when no TM records are present.
    """
    if not tm_records:
        return None

    temp_pts:  list[dict] = []
    hum_pts:   list[dict] = []
    cond_pts:  list[dict] = []
    device_ids: set[str]  = set()

    for r in tm_records:
        try:
            ts = _parse_ts(r["timestamp"])
        except ValidationError:
            continue

        ts_str = _iso(ts)
        device_ids.add(r.get("device_id", "unknown"))

        if r.get("temperature") is not None:
            temp_pts.append({"timestamp": ts_str, "value": round(float(r["temperature"]), 3)})
        if r.get("humidity") is not None:
            hum_pts.append({"timestamp": ts_str, "value": round(float(r["humidity"]), 3)})
        if r.get("conductivity") is not None:
            cond_pts.append({"timestamp": ts_str, "value": round(float(r["conductivity"]), 3)})

    def _stats(pts: list[dict]) -> tuple:
        vals = [p["value"] for p in pts]
        if not vals:
            return None, None, None
        return (
            round(sum(vals) / len(vals), 3),
            round(min(vals), 3),
            round(max(vals), 3),
        )

    t_avg, t_min, t_max = _stats(temp_pts)
    h_avg, _, _         = _stats(hum_pts)
    c_avg, _, _         = _stats(cond_pts)

    return {
        "temperature_avg":  t_avg,
        "temperature_min":  t_min,
        "temperature_max":  t_max,
        "humidity_avg":     h_avg,
        "conductivity_avg": c_avg,
        "series": {
            "temperature":  sorted(temp_pts,  key=lambda p: p["timestamp"]),
            "humidity":     sorted(hum_pts,   key=lambda p: p["timestamp"]),
            "conductivity": sorted(cond_pts,  key=lambda p: p["timestamp"]),
        },
        "device_count": len(device_ids),
        "record_count":  len(tm_records),
    }


# ---------------------------------------------------------------------------
# run_site_pipeline — Public entry point for multi-device / site-level runs
# ---------------------------------------------------------------------------

def _build_device_inventory(
    pm_records: list[dict],
    tm_records: list[dict],
    window_from: str,
    window_to:   str,
) -> list[dict]:
    """
    Build a per-device summary for the dashboard device roster.

    Each entry describes one physical device that contributed records in this
    window.  The dashboard can use this to render a device list, flag gaps,
    or show per-device sparklines.

    Returns
    -------
    List of dicts, one per unique device_id, sorted by device_id:
    {
        device_id   : str,
        device_name : str,
        device_type : "pm" | "tm",
        zone        : str | None,      # from record["zone"] if present
        record_count: int,
        kwh_total   : float | None,    # PM only — sum of deltas over window
        last_seen   : str,             # ISO timestamp of most recent record
        online      : bool,            # True if last record is within 2× interval_s of window_to
    }
    """
    from collections import defaultdict

    # Combine all records with their type tag
    tagged: list[tuple[str, dict]] = (
        [("pm", r) for r in pm_records] +
        [("tm", r) for r in tm_records]
    )

    # Group by device_id
    by_device: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for dtype, r in tagged:
        did = r.get("device_id", "unknown")
        by_device[did].append((dtype, r))

    window_to_dt = _parse_ts(window_to)
    inventory: list[dict] = []

    for dev_id in sorted(by_device):
        entries = by_device[dev_id]
        dtype   = entries[0][0]  # type is consistent per device
        records = [r for _, r in entries]

        # Most recent record
        ts_list = []
        for r in records:
            try:
                ts_list.append(_parse_ts(r["timestamp"]))
            except ValidationError:
                pass
        last_ts = max(ts_list) if ts_list else None

        # Online heuristic: last record within 2× the device's reporting interval
        interval_s = records[0].get("interval_s", 900)
        online = False
        if last_ts is not None:
            gap_s = (window_to_dt - last_ts).total_seconds()
            online = gap_s <= interval_s * 2

        # kWh total for PMs (sum of deltas — mirrors aggregation logic)
        kwh_total: float | None = None
        if dtype == "pm":
            sorted_recs = sorted(records, key=lambda r: r.get("timestamp", ""))
            kwh_total = 0.0
            prev_kwh: float | None = None
            for r in sorted_recs:
                raw = r.get("kwh")
                if raw is None:
                    continue
                if prev_kwh is not None and raw > prev_kwh:
                    kwh_total += raw - prev_kwh
                prev_kwh = raw
            kwh_total = round(kwh_total, 4)

        inventory.append({
            "device_id":    dev_id,
            "device_name":  records[0].get("device_name", dev_id),
            "device_type":  dtype,
            "zone":         records[0].get("zone", None),
            "record_count": len(records),
            "kwh_total":    kwh_total,
            "last_seen":    _iso(last_ts) if last_ts else None,
            "online":       online,
        })

    return inventory


def _aggregate_pm_by_zone(
    pm_records: list[dict],
    site_id:    str,
) -> list[dict]:
    """
    Compute per-zone kWh totals for the zone_breakdown block.

    Records carry an optional "zone" field (string, e.g. "Floor 1", "HVAC").
    Records without a zone field are grouped under "_unassigned".

    Returns
    -------
    List of { zone, kwh_total, device_count, device_ids } sorted by zone name.
    """
    from collections import defaultdict

    # Per-zone, per-device: collect sorted kwh readings to compute deltas
    zone_device_readings: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

    for r in pm_records:
        zone   = r.get("zone") or "_unassigned"
        dev_id = r.get("device_id", "unknown")
        kwh    = r.get("kwh")
        if kwh is not None:
            zone_device_readings[zone][dev_id].append(kwh)

    result: list[dict] = []
    for zone in sorted(zone_device_readings):
        zone_kwh   = 0.0
        device_ids = set()
        for dev_id, readings in zone_device_readings[zone].items():
            readings_sorted = sorted(readings)
            if len(readings_sorted) >= 2:
                # Sum of deltas = last − first (handles monotonic cumulative meter)
                zone_kwh += max(readings_sorted[-1] - readings_sorted[0], 0.0)
            device_ids.add(dev_id)
        result.append({
            "zone":         zone,
            "kwh_total":    round(zone_kwh, 4),
            "device_count": len(device_ids),
            "device_ids":   sorted(device_ids),
        })

    return result


def run_site_pipeline(
    records: list[dict],
    *,
    site_id:          str               = "SITE-001",
    site_name:        str               = "Site",
    previous_records: list[dict] | None = None,
    m3_config:        dict | None       = None,
    m5_config:        dict | None       = None,
    m6_config:        dict | None       = None,
) -> dict:
    """
    Site / building-level ESG pipeline for mixed multi-device input.

    Designed to handle any number of devices across floors, zones, or
    subsystems in a single building — collapsing them into one unified
    dashboard payload while preserving per-device and per-zone breakdowns.

    Accepts records from any mix of:
    - Power Meter (PM) devices  — identified by presence of "kwh" field
    - Temperature Monitor (TM) devices — identified by "temperature" /
      "humidity" / "conductivity" fields

    Optional per-record field
    -------------------------
    "zone" : str  — logical grouping (e.g. "Floor 1", "HVAC", "Server Room").
                    When present, a zone_breakdown block is added to the payload.
                    Records without a zone are grouped under "_unassigned".

    Steps
    -----
    1. Classify each record as PM or TM.
    2. Aggregate PM records across all devices → one synthetic site-level stream.
    3. Aggregate TM records → environmental summary.
    4. Build device_inventory (per-device roster with online/offline status).
    5. Build zone_breakdown (per-zone kWh totals, if any zone fields present).
    6. Run existing run_pipeline() on aggregated PM stream (M1→M6 unchanged).
    7. Attach building-level blocks to payload.

    Parameters
    ----------
    records          : mixed list of raw device records (unlimited devices)
    site_id          : written into meta.device_id and meta.site_id
    site_name        : written into meta.device_name and meta.site_name
    previous_records : optional prior-period mixed records for trend deltas
    m3_config        : carbon module config (passed through)
    m5_config        : phase imbalance config (passed through)
    m6_config        : cost/tariff config (passed through)

    Returns
    -------
    PipelinePayload dict — same contract as run_pipeline(), plus:

        "environmental"  : EnvironmentalResult | None
        "device_inventory": list[DeviceInventoryEntry]
        "zone_breakdown" : list[ZoneBreakdown] | None   (None if no zone fields)
    """
    if not records:
        raise ValidationError("Empty record list")

    # --- Step 1: Classify ---
    pm_records: list[dict] = []
    tm_records: list[dict] = []
    for r in records:
        if _classify_record(r) == "tm":
            tm_records.append(r)
        else:
            pm_records.append(r)

    # --- Step 2: Aggregate PMs into one site-level stream ---
    aggregated_pm = _aggregate_pm_records(pm_records, site_id, site_name)

    if not aggregated_pm:
        raise ValidationError(
            "No Power Meter records found in input after aggregation. "
            "Ensure at least one record contains a 'kwh' field."
        )

    # --- Step 3: Aggregate TMs ---
    environmental = _aggregate_tm_records(tm_records)

    # --- Step 4: Aggregate previous period (for trends) ---
    aggregated_prev: list[dict] | None = None
    if previous_records is not None:
        prev_pm = [r for r in previous_records if _classify_record(r) == "pm"]
        aggregated_prev = _aggregate_pm_records(prev_pm, site_id, site_name) or None

    # --- Step 5: Run core pipeline on aggregated PM stream ---
    payload = run_pipeline(
        aggregated_pm,
        previous_records=aggregated_prev,
        m3_config=m3_config,
        m5_config=m5_config,
        m6_config=m6_config,
    )

    # --- Step 6: Build building-level blocks ---
    window_from = payload["meta"]["window"]["from"]
    window_to   = payload["meta"]["window"]["to"]

    device_inventory = _build_device_inventory(
        pm_records, tm_records, window_from, window_to
    )

    # Zone breakdown — only include if at least one record has a "zone" field
    has_zones = any(r.get("zone") for r in pm_records)
    zone_breakdown = _aggregate_pm_by_zone(pm_records, site_id) if has_zones else None

    # --- Step 7: Attach all blocks and enrich meta ---
    payload["environmental"]   = environmental
    payload["device_inventory"] = device_inventory
    payload["zone_breakdown"]  = zone_breakdown
    payload["meta"]["site_id"]      = site_id
    payload["meta"]["site_name"]    = site_name
    payload["meta"]["device_count"] = len({r.get("device_id") for r in pm_records})
    payload["meta"]["tm_count"]     = len({r.get("device_id") for r in tm_records})

    return payload


if __name__ == "__main__":
    import json

    SAMPLE_RECORDS = [
        {
            "device_id": "DEV-001", "device_name": "Main Panel",
            "timestamp": "2024-06-01T00:00:00Z", "interval_s": 900,
            "kwh": 1000.0, "voltage": 230.0, "pf": 0.91,
            "ca": 12.5,          # single-phase — omit cb/cc entirely
        },
        {
            "device_id": "DEV-001", "device_name": "Main Panel",
            "timestamp": "2024-06-01T00:15:00Z", "interval_s": 900,
            "kwh": 1002.3, "voltage": 228.5, "pf": 0.89,
            "ca": 11.8,
        },
        {
            "device_id": "DEV-001", "device_name": "Main Panel",
            "timestamp": "2024-06-01T00:30:00Z", "interval_s": 900,
            "kwh": 1004.9, "voltage": 231.2, "pf": 0.92,
            "ca": 13.1,
        },
        {
            "device_id": "DEV-001", "device_name": "Main Panel",
            "timestamp": "2024-06-01T01:00:00Z", "interval_s": 900,
            "kwh": 1010.1, "voltage": 229.0, "pf": 0.90,
            "ca": 12.2,
        },
    ]

    print("\n=== Without M3 config (EF required badge) ===")
    result_no_carbon = run_pipeline(SAMPLE_RECORDS)
    print(json.dumps(result_no_carbon["carbon"], indent=2))

    print("\n=== With grid_region only (estimated quality) ===")
    result_region = run_pipeline(
        SAMPLE_RECORDS,
        m3_config={"grid_region": "PK"},
    )
    print(json.dumps(result_region["carbon"], indent=2))

    print("\n=== With explicit EF + verified flag (verified quality) ===")
    result_verified = run_pipeline(
        SAMPLE_RECORDS,
        m3_config={
            "emission_factor": 0.402,
            "ef_source":       "NEPRA Grid Report 2023",
            "ef_verified":     True,
        },
    )
    print(json.dumps(result_verified["carbon"], indent=2))

    print("\n=== Full payload (verified carbon) ===")
    print(json.dumps(result_verified, indent=2))

    print("\n=== M6 Cost — basic tariff only ===")
    result_cost_basic = run_pipeline(
        SAMPLE_RECORDS,
        m3_config={"grid_region": "PK"},
        m6_config={"tariff_per_kwh": 45.0},
    )
    print(json.dumps(result_cost_basic["cost"], indent=2))

    print("\n=== M6 Cost — peak/off-peak + demand charge + schedule ===")
    result_cost_full = run_pipeline(
        SAMPLE_RECORDS,
        m3_config={"grid_region": "PK"},
        m6_config={
            "tariff_per_kwh":       35.0,
            "peak_tariff_per_kwh":  60.0,
            "peak_hours":           [17, 18, 19, 20, 21],
            "demand_charge_per_kw": 500.0,
            "operational_schedule": {
                "start_hour": 8,
                "end_hour":   18,
                "days":       ["Mon", "Tue", "Wed", "Thu", "Fri"],
            },
        },
    )
    print(json.dumps(result_cost_full["cost"], indent=2))

    print("\n=== M5 — single-phase device (anomalies should be null) ===")
    result_single = run_pipeline(SAMPLE_RECORDS)
    print(json.dumps(result_single["anomalies"], indent=2))

    print("\n=== M5 — 3-phase device with imbalanced currents ===")
    SAMPLE_3PH = [
        {
            "device_id": "DEV-3PH", "device_name": "Factory Panel",
            "timestamp": "2024-06-01T00:00:00Z", "interval_s": 900,
            "kwh": 5000.0, "voltage": 400.0,
            "ca": 100.0, "cb": 100.0, "cc": 100.0,   # balanced — 0% imbalance
        },
        {
            "device_id": "DEV-3PH", "device_name": "Factory Panel",
            "timestamp": "2024-06-01T00:15:00Z", "interval_s": 900,
            "kwh": 5020.0, "voltage": 400.0,
            "ca": 100.0, "cb": 90.0, "cc": 100.0,    # 3.33% imbalance → breaches 2.0%
        },
        {
            "device_id": "DEV-3PH", "device_name": "Factory Panel",
            "timestamp": "2024-06-01T00:30:00Z", "interval_s": 900,
            "kwh": 5040.0, "voltage": 400.0,
            "ca": 100.0, "cb": 80.0, "cc": 100.0,    # 6.67% imbalance → breaches 2.0%
        },
    ]
    result_3ph = run_pipeline(SAMPLE_3PH, m5_config={"imbalance_threshold_pct": 2.0})
    print(json.dumps(result_3ph["anomalies"], indent=2))