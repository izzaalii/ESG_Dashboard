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
# Types (lightweight dataclasses — no Pydantic dependency required)
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
# run_pipeline — Public entry point
# ---------------------------------------------------------------------------

def run_pipeline(
    records: list[dict],
    *,
    m3_config: dict | None = None,
    m5_config: dict | None = None,
    m6_config: dict | None = None,
) -> dict:
    """
    Run the full ESG pipeline for a single device's record set.

    Parameters
    ----------
    records     : list of raw device record dicts (DeviceRecord schema)
    m3_config   : optional carbon module config
                  Keys: emission_factor (float, kgCO₂e/kWh), grid_region (str, e.g. "PK"),
                        ef_source (str), ef_verified (bool)
    m5_config   : optional anomaly detection config
                  Keys: imbalance_threshold_pct (float, default 2.0)
    m6_config   : optional cost/tariff config

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

    # --- Assemble payload ---
    # Merge M6 metric cards (cost_total, demand_charge) into top-level cards if M6 ran
    m6_cards   = cost_result["metric_cards"] if cost_result else []
    all_cards  = m2_out["metric_cards"] + m4_out["metric_cards"] + m6_cards
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