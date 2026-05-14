"""
Mock Device Payloads — End-to-End Pipeline Test
================================================
Sends realistic sample data through run_pipeline() and run_site_pipeline()
for the following device types:

  PM  — Power Meters (cumulative kWh, voltage, current, PF)
         • PM-001  3-phase office panel (well-balanced phases)
         • PM-002  3-phase server room UPS (very steady load)
         • PM-003  3-phase factory panel (imbalanced ca/cb/cc)

  TM  — Temperature Monitors
         • TH-001  Temp + Humidity (server room)
         • TH-002  Temp + Humidity (warehouse)
         • TC-001  Temp + Conductivity (cooling tower)

All timestamps: 2024-06-01 00:00 → 02:00 UTC in 15-min intervals (9 readings).
"""

from __future__ import annotations
import json, sys, os

# Force UTF-8 stdout on Windows (avoids cp1252 encoding errors)
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Allow importing from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from esg_pipeline import run_pipeline, run_site_pipeline

# ───────────────────────────────────────────────────────────────────
# Helper — generate 15-min timestamps
# ───────────────────────────────────────────────────────────────────
def ts(hour: int, minute: int) -> str:
    return f"2024-06-01T{hour:02d}:{minute:02d}:00Z"

TIMES = [
    ts(0, 0), ts(0, 15), ts(0, 30), ts(0, 45),
    ts(1, 0), ts(1, 15), ts(1, 30), ts(1, 45),
    ts(2, 0),
]

# ───────────────────────────────────────────────────────────────────
# PM-001 — 3-phase office panel (≈2.5 kWh/interval, well-balanced)
# ───────────────────────────────────────────────────────────────────
PM001_KWH = [1000.0, 1002.4, 1004.9, 1007.5, 1010.2, 1012.8, 1015.1, 1017.6, 1020.3]
PM001 = [
    {
        "device_id": "PM-001", "device_name": "Office Panel A",
        "timestamp": TIMES[i], "interval_s": 900,
        "kwh": PM001_KWH[i],
        "voltage": vl, "pf": pf,
        "voltage_a": va, "voltage_b": vb, "voltage_c": vc,
        "ca": ca, "cb": cb, "cc": cc,
    }
    for i, (vl, va, vb, vc, ca, cb, cc, pf) in enumerate([
        (398.0, 230.1, 229.5, 229.8, 4.0, 3.8, 3.9, 0.92),
        (397.5, 228.8, 229.0, 229.2, 4.1, 3.9, 4.0, 0.90),
        (399.0, 231.0, 230.5, 230.8, 3.8, 3.7, 3.8, 0.93),
        (398.5, 229.5, 229.8, 230.0, 3.9, 3.8, 3.9, 0.91),
        (399.0, 230.3, 230.0, 229.7, 4.2, 4.0, 4.1, 0.89),
        (397.0, 228.2, 228.5, 228.8, 4.0, 3.9, 3.9, 0.92),
        (399.5, 231.5, 231.0, 231.2, 3.7, 3.6, 3.7, 0.90),
        (398.0, 229.8, 229.5, 230.0, 3.9, 3.8, 3.9, 0.91),
        (398.5, 230.0, 229.8, 230.2, 3.8, 3.7, 3.8, 0.93),
    ])
]

# ───────────────────────────────────────────────────────────────────
# PM-002 — 3-phase server room UPS (≈1.8 kWh/interval, very steady)
# ───────────────────────────────────────────────────────────────────
PM002_KWH = [5000.0, 5001.8, 5003.5, 5005.3, 5007.1, 5008.9, 5010.7, 5012.5, 5014.2]
PM002 = [
    {
        "device_id": "PM-002", "device_name": "Server Room UPS",
        "timestamp": TIMES[i], "interval_s": 900,
        "kwh": PM002_KWH[i],
        "voltage": vl, "pf": pf,
        "voltage_a": va, "voltage_b": vb, "voltage_c": vc,
        "ca": ca, "cb": cb, "cc": cc,
    }
    for i, (vl, va, vb, vc, ca, cb, cc, pf) in enumerate([
        (400.0, 231.0, 230.5, 230.8, 2.8, 2.7, 2.8, 0.98),
        (399.5, 230.5, 230.2, 230.6, 2.9, 2.8, 2.8, 0.97),
        (400.2, 231.2, 230.8, 231.0, 2.7, 2.7, 2.7, 0.98),
        (399.8, 230.8, 230.5, 230.7, 2.7, 2.6, 2.7, 0.99),
        (400.5, 231.5, 231.0, 231.2, 2.9, 2.8, 2.9, 0.97),
        (399.0, 230.0, 229.8, 230.2, 2.8, 2.7, 2.8, 0.98),
        (400.0, 231.0, 230.5, 230.8, 2.6, 2.6, 2.6, 0.99),
        (400.5, 231.5, 231.0, 231.3, 2.8, 2.7, 2.8, 0.97),
        (399.5, 230.5, 230.2, 230.5, 2.7, 2.7, 2.7, 0.98),
    ])
]

# ───────────────────────────────────────────────────────────────────
# PM-003 — 3-phase factory panel (≈8 kWh/interval, with imbalance)
# ───────────────────────────────────────────────────────────────────
PM003_KWH = [20000.0, 20008.1, 20016.5, 20024.7, 20033.2, 20041.0, 20049.3, 20057.8, 20066.1]
PM003 = [
    {
        "device_id": "PM-003", "device_name": "Factory Main Panel",
        "timestamp": TIMES[i], "interval_s": 900,
        "kwh": PM003_KWH[i],
        "voltage": vl,
        "voltage_a": va, "voltage_b": vb, "voltage_c": vc,
        "ca": ca, "cb": cb, "cc": cc,
        "pf": pf,
    }
    for i, (vl, va, vb, vc, ca, cb, cc, pf) in enumerate([
        (400.0, 231.0, 230.0, 229.0, 95.0, 92.0, 97.0, 0.88),
        (398.5, 230.5, 228.0, 231.2, 97.0, 88.0, 96.0, 0.87),  # cb dip → imbalance
        (401.0, 232.0, 231.0, 230.0, 94.0, 93.0, 95.0, 0.89),
        (399.0, 230.0, 229.5, 231.5, 96.0, 91.0, 98.0, 0.86),
        (400.5, 231.5, 230.5, 229.5, 100.0, 85.0, 99.0, 0.87),  # big imbalance
        (399.5, 230.8, 230.2, 230.0, 93.0, 94.0, 93.0, 0.90),
        (401.5, 232.0, 231.5, 230.5, 95.0, 90.0, 97.0, 0.88),
        (400.0, 231.0, 230.0, 231.0, 98.0, 87.0, 96.0, 0.87),
        (399.0, 230.5, 229.5, 230.0, 94.0, 93.0, 95.0, 0.89),
    ])
]

# ───────────────────────────────────────────────────────────────────
# TH-001 — Temp + Humidity (server room)
# ───────────────────────────────────────────────────────────────────
TH001 = [
    {
        "device_id": "TH-001", "device_name": "Server Room Env",
        "timestamp": TIMES[i],
        "temperature": t, "humidity": h,
    }
    for i, (t, h) in enumerate([
        (22.1, 45.2), (22.3, 45.0), (22.5, 44.8), (22.8, 44.5),
        (23.0, 44.3), (23.2, 44.1), (23.1, 44.4), (22.9, 44.6), (22.7, 44.9),
    ])
]

# ───────────────────────────────────────────────────────────────────
# TH-002 — Temp + Humidity (warehouse)
# ───────────────────────────────────────────────────────────────────
TH002 = [
    {
        "device_id": "TH-002", "device_name": "Warehouse Env",
        "timestamp": TIMES[i],
        "temperature": t, "humidity": h,
    }
    for i, (t, h) in enumerate([
        (28.5, 62.0), (28.8, 61.5), (29.1, 61.0), (29.4, 60.8),
        (29.6, 60.5), (29.3, 60.9), (29.0, 61.2), (28.7, 61.7), (28.4, 62.1),
    ])
]

# ───────────────────────────────────────────────────────────────────
# TC-001 — Temp + Conductivity (cooling tower)
# ───────────────────────────────────────────────────────────────────
TC001 = [
    {
        "device_id": "TC-001", "device_name": "Cooling Tower Monitor",
        "timestamp": TIMES[i],
        "temperature": t, "conductivity": c,
    }
    for i, (t, c) in enumerate([
        (31.2, 850.0), (31.5, 855.0), (31.8, 860.0), (32.0, 862.0),
        (32.3, 870.0), (32.1, 868.0), (31.9, 863.0), (31.6, 858.0), (31.3, 852.0),
    ])
]


# ───────────────────────────────────────────────────────────────────
# Config bundles
# ───────────────────────────────────────────────────────────────────
M3_CONFIG = {"grid_region": "PK", "ef_source": "IEA 2023"}
M5_CONFIG = {"imbalance_threshold_pct": 2.0}
M6_CONFIG = {
    "tariff_per_kwh": 45.0,
    "peak_tariff_per_kwh": 60.0,
    "peak_hours": [17, 18, 19, 20, 21],
    "demand_charge_per_kw": 500.0,
}


def _print_section(title: str):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def _summarise(payload: dict):
    """Print a compact summary of a pipeline payload."""
    meta = payload.get("meta", {})
    print(f"  Device : {meta.get('device_id')} — {meta.get('device_name')}")
    print(f"  Phase  : {meta.get('phase_type')}")
    print(f"  Window : {meta.get('window', {}).get('from')} -> {meta.get('window', {}).get('to')}")

    print("\n  Metric Cards:")
    for card in payload.get("metric_cards", []):
        trend_str = ""
        if card.get("trend"):
            t = card["trend"]
            trend_str = f"  ({t['direction']} {t['delta_pct']:+.2f}%)"
        print(f"    • {card['label']}: {card['value']} {card['unit']}{trend_str}")

    print(f"\n  Chart Series: {len(payload.get('chart_series', []))} series")
    for s in payload.get("chart_series", []):
        print(f"    • {s['id']}: {len(s['points'])} points")

    carbon = payload.get("carbon", {})
    if carbon.get("co2e_kg") is not None:
        print(f"\n  Carbon: {carbon['co2e_kg']} kgCO₂e  (quality: {carbon['data_quality']})")

    anomalies = payload.get("anomalies")
    if anomalies:
        pi = anomalies["phase_imbalance"]
        print(f"\n  Phase Imbalance: avg={pi['avg_pct']:.2f}%  max={pi['max_pct']:.2f}%  breaches={pi['breach_count']}")

    cost = payload.get("cost")
    if cost:
        print(f"\n  Cost Total: {cost['cost_total']} PKR")

    env = payload.get("environmental")
    if env:
        print(f"\n  Environmental ({env['device_count']} devices, {env['record_count']} records):")
        if env.get("temperature_avg") is not None:
            print(f"    Temp: avg={env['temperature_avg']}C  min={env['temperature_min']}C  max={env['temperature_max']}C")
        if env.get("humidity_avg") is not None:
            print(f"    Humidity: avg={env['humidity_avg']}%")
        if env.get("conductivity_avg") is not None:  # uS/cm
            print(f"    Conductivity: avg={env['conductivity_avg']} uS/cm")


# ═══════════════════════════════════════════════════════════════════
# Test 1 — Single-device run_pipeline for each PM
# ═══════════════════════════════════════════════════════════════════

def test_single_device_pipelines():
    for label, records in [("PM-001 Office", PM001), ("PM-002 Server", PM002), ("PM-003 Factory 3ph", PM003)]:
        _print_section(f"run_pipeline -- {label}")
        result = run_pipeline(
            records,
            m3_config=M3_CONFIG,
            m5_config=M5_CONFIG,
            m6_config=M6_CONFIG,
        )
        _summarise(result)


# ═══════════════════════════════════════════════════════════════════
# Test 2 — run_site_pipeline with ALL devices mixed together
# ═══════════════════════════════════════════════════════════════════

def test_site_pipeline():
    _print_section("run_site_pipeline — Full Site (3 PMs + 3 TMs)")
    all_records = PM001 + PM002 + PM003 + TH001 + TH002 + TC001

    result = run_site_pipeline(
        all_records,
        site_id="SITE-HQ",
        site_name="HQ Building",
        m3_config=M3_CONFIG,
        m5_config=M5_CONFIG,
        m6_config=M6_CONFIG,
    )
    _summarise(result)

    # Dump full JSON for inspection
    print("\n  --- Full JSON payload ---")
    print(json.dumps(result, indent=2))


# ═══════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    test_single_device_pipelines()
    test_site_pipeline()
    print("\n[OK] All mock payloads processed successfully!")
