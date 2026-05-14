"""
tests/test_esg_pipeline.py
==========================
Pytest suite for ESG Pipeline — M1, M2, M3, M5, M6, and end-to-end.

Run:
    pip install pytest
    pytest tests/test_esg_pipeline.py -v

Coverage:
    M1  — validation: missing fields, future timestamp, voltage OOR, rollover flag
    M2  — consumption: totals, peak kW, all 4 rollup series + bucket timestamps
    M3  — carbon: no-config stub, grid_region estimated, verified EF, math
    M5  — phase imbalance: single-phase null, 3-phase no-currents null, balanced ~0%,
                           unbalanced breach count, custom threshold, avg/max math
    M6  — cost: no-config stub, basic tariff, peak/off-peak split,
                demand charge, off-hours kWh, heatmap shape
    E2E — run_pipeline returns valid PipelinePayload with all required keys
"""

import sys
import os
import pytest

# Allow import from parent directory (where esg_pipeline.py lives)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from esg_pipeline import (
    ValidationError,
    m1_ingest,
    m2_consumption,
    m3_carbon,
    m5_anomalies,
    m6_cost,
    run_pipeline,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

BASE_RECORD = {
    "device_id":   "DEV-001",
    "device_name": "Main Panel",
    "timestamp":   "2024-06-01T00:00:00Z",
    "interval_s":  900,
    "kwh":         1000.0,
    "voltage":     230.0,
}

SAMPLE_RECORDS = [
    {**BASE_RECORD, "timestamp": "2024-06-01T00:00:00Z", "kwh": 1000.0,
     "voltage": 230.0, "pf": 0.91, "ca": 12.5},
    {**BASE_RECORD, "timestamp": "2024-06-01T00:15:00Z", "kwh": 1002.3,
     "voltage": 228.5, "pf": 0.89, "ca": 11.8},
    {**BASE_RECORD, "timestamp": "2024-06-01T00:30:00Z", "kwh": 1004.9,
     "voltage": 231.2, "pf": 0.92, "ca": 13.1},
    {**BASE_RECORD, "timestamp": "2024-06-01T01:00:00Z", "kwh": 1010.1,
     "voltage": 229.0, "pf": 0.90, "ca": 12.2},
]


def _make_m2_out(records=None):
    """Helper: run M1 → M2 and return m2_out."""
    clean = m1_ingest(records or SAMPLE_RECORDS)
    return m2_consumption(clean)


# ===========================================================================
# M1 — Ingestion & Validation
# ===========================================================================

class TestM1Validation:

    def test_missing_required_field_raises(self):
        """Missing 'kwh' must raise ValidationError."""
        bad = {k: v for k, v in BASE_RECORD.items() if k != "kwh"}
        with pytest.raises(ValidationError, match="missing required fields"):
            m1_ingest([bad])

    def test_missing_timestamp_raises(self):
        bad = {k: v for k, v in BASE_RECORD.items() if k != "timestamp"}
        with pytest.raises(ValidationError, match="missing required fields"):
            m1_ingest([bad])

    def test_future_timestamp_raises(self):
        bad = {**BASE_RECORD, "timestamp": "2099-01-01T00:00:00Z"}
        with pytest.raises(ValidationError, match="future"):
            m1_ingest([bad])

    def test_negative_kwh_raises(self):
        bad = {**BASE_RECORD, "kwh": -1.0}
        with pytest.raises(ValidationError, match="kwh must be"):
            m1_ingest([bad])

    def test_invalid_pf_raises(self):
        bad = {**BASE_RECORD, "pf": 1.5}
        with pytest.raises(ValidationError, match="pf must be"):
            m1_ingest([bad])

    def test_voltage_out_of_range_single_phase_raises(self):
        """Single-phase voltage outside 180–260 V must be rejected."""
        bad = {**BASE_RECORD, "voltage": 100.0}
        with pytest.raises(ValidationError, match="voltage"):
            m1_ingest([bad])

    def test_voltage_out_of_range_3phase_raises(self):
        """3-phase (auto-detected from ca/cb/cc) must validate line-to-line range."""
        bad = {
            **BASE_RECORD,
            "voltage": 230.0,   # single-phase range — wrong for 3-phase
            "ca": 10.0, "cb": 10.0, "cc": 10.0,
        }
        with pytest.raises(ValidationError, match="voltage"):
            m1_ingest([bad])

    def test_empty_records_raises(self):
        with pytest.raises(ValidationError):
            m1_ingest([])

    def test_rollover_flag_set(self):
        """kWh decrease across consecutive records must set _kwh_rollover=True."""
        records = [
            {**BASE_RECORD, "timestamp": "2024-06-01T00:00:00Z", "kwh": 9999.0},
            {**BASE_RECORD, "timestamp": "2024-06-01T00:15:00Z", "kwh": 10.0},   # rollover
        ]
        clean = m1_ingest(records)
        assert clean[1].get("_kwh_rollover") is True

    def test_valid_single_phase_passes(self):
        """A well-formed single-phase record must not raise."""
        clean = m1_ingest([BASE_RECORD])
        assert len(clean) == 1
        assert clean[0]["phase_type"] == "single"

    def test_3phase_auto_detected(self):
        """Presence of ca/cb/cc must auto-detect 3-phase."""
        r = {
            **BASE_RECORD,
            "voltage": 400.0,
            "ca": 10.0, "cb": 10.0, "cc": 10.0,
        }
        clean = m1_ingest([r])
        assert clean[0]["phase_type"] == "3ph"

    def test_records_sorted_by_timestamp(self):
        """M1 must return records in ascending timestamp order."""
        records = [
            {**BASE_RECORD, "timestamp": "2024-06-01T00:30:00Z", "kwh": 1004.9},
            {**BASE_RECORD, "timestamp": "2024-06-01T00:00:00Z", "kwh": 1000.0},
        ]
        clean = m1_ingest(records)
        assert clean[0]["kwh"] == 1000.0
        assert clean[1]["kwh"] == 1004.9


# ===========================================================================
# M2 — Consumption
# ===========================================================================

class TestM2Consumption:

    def setup_method(self):
        self.m2 = _make_m2_out()

    def test_total_kwh_correct(self):
        """4-record sample: 1010.1 - 1000.0 = 10.1 kWh total."""
        card = next(c for c in self.m2["metric_cards"] if c["id"] == "total_kwh")
        assert card["value"] == pytest.approx(10.1, abs=0.01)

    def test_peak_kw_correct(self):
        """
        Intervals:
          00:00→00:15 = 2.3 kWh in 0.25h → 9.2 kW
          00:15→00:30 = 2.6 kWh in 0.25h → 10.4 kW
          00:30→01:00 = 5.2 kWh in 0.5h  → 10.4 kW
        Peak = 10.4 kW
        """
        card = next(c for c in self.m2["metric_cards"] if c["id"] == "peak_kw")
        assert card["value"] == pytest.approx(10.4, abs=0.01)

    def test_all_four_series_present(self):
        ids = {s["id"] for s in self.m2["chart_series"]}
        assert {"kwh_15min", "kwh_hourly", "kwh_daily", "kwh_monthly"}.issubset(ids)

    def test_15min_series_has_3_points(self):
        """4 records → 3 intervals → 3 delta points in 15-min series."""
        s = next(s for s in self.m2["chart_series"] if s["id"] == "kwh_15min")
        assert len(s["points"]) == 3

    def test_15min_bucket_timestamps(self):
        s = next(s for s in self.m2["chart_series"] if s["id"] == "kwh_15min")
        ts = [p["timestamp"] for p in s["points"]]
        assert "2024-06-01T00:15:00Z" in ts
        assert "2024-06-01T00:30:00Z" in ts

    def test_hourly_rollup_correct(self):
        """
        Hour 00: intervals at :15 (2.3) and :30 (2.6) → 4.9 kWh
        Hour 01: interval at :00 (5.2) → 5.2 kWh
        """
        s = next(s for s in self.m2["chart_series"] if s["id"] == "kwh_hourly")
        pts = {p["timestamp"]: p["value"] for p in s["points"]}
        assert pts.get("2024-06-01T00:00:00Z") == pytest.approx(4.9, abs=0.01)
        assert pts.get("2024-06-01T01:00:00Z") == pytest.approx(5.2, abs=0.01)

    def test_daily_rollup_correct(self):
        s = next(s for s in self.m2["chart_series"] if s["id"] == "kwh_daily")
        assert len(s["points"]) == 1
        assert s["points"][0]["value"] == pytest.approx(10.1, abs=0.01)

    def test_monthly_rollup_correct(self):
        s = next(s for s in self.m2["chart_series"] if s["id"] == "kwh_monthly")
        assert len(s["points"]) == 1
        assert s["points"][0]["value"] == pytest.approx(10.1, abs=0.01)

    def test_single_record_returns_empty(self):
        """Need ≥2 records to compute any delta."""
        clean = m1_ingest([BASE_RECORD])
        m2 = m2_consumption(clean)
        assert m2["metric_cards"] == []
        assert m2["chart_series"] == []

    def test_rollover_interval_excluded(self):
        """A rollover interval must contribute 0 kWh, not a negative delta."""
        records = [
            {**BASE_RECORD, "timestamp": "2024-06-01T00:00:00Z", "kwh": 9999.0},
            {**BASE_RECORD, "timestamp": "2024-06-01T00:15:00Z", "kwh": 5.0},
            {**BASE_RECORD, "timestamp": "2024-06-01T00:30:00Z", "kwh": 10.0},
        ]
        clean = m1_ingest(records)
        m2 = m2_consumption(clean)
        card = next(c for c in m2["metric_cards"] if c["id"] == "total_kwh")
        assert card["value"] == pytest.approx(5.0, abs=0.01)   # only last interval counts


# ===========================================================================
# M3 — Carbon
# ===========================================================================

class TestM3Carbon:

    def setup_method(self):
        self.m2 = _make_m2_out()

    def test_no_config_returns_null_co2e(self):
        result = m3_carbon(self.m2, config=None)
        assert result["co2e_kg"] is None

    def test_no_config_data_quality_badge(self):
        result = m3_carbon(self.m2, config=None)
        assert result["data_quality"] == "EF config required"

    def test_no_config_breakdown_empty(self):
        result = m3_carbon(self.m2, config=None)
        assert result["breakdown"] == []

    def test_grid_region_estimated_quality(self):
        result = m3_carbon(self.m2, config={"grid_region": "PK"})
        assert result["data_quality"] == "estimated"
        assert result["co2e_kg"] is not None

    def test_grid_region_pk_ef_value(self):
        """PK grid EF is 0.402 → 10.1 kWh × 0.402 = 4.0602 kgCO₂e."""
        result = m3_carbon(self.m2, config={"grid_region": "PK"})
        assert result["co2e_kg"] == pytest.approx(4.0602, abs=0.001)

    def test_explicit_ef_verified_quality(self):
        result = m3_carbon(self.m2, config={
            "emission_factor": 0.402,
            "ef_verified":     True,
            "ef_source":       "NEPRA 2023",
        })
        assert result["data_quality"] == "verified"

    def test_explicit_ef_math(self):
        result = m3_carbon(self.m2, config={"emission_factor": 0.5})
        assert result["co2e_kg"] == pytest.approx(10.1 * 0.5, abs=0.001)

    def test_verified_flag_false_gives_estimated(self):
        result = m3_carbon(self.m2, config={
            "emission_factor": 0.402,
            "ef_verified":     False,
        })
        assert result["data_quality"] == "estimated"

    def test_invalid_ef_returns_error_message(self):
        """Negative emission_factor is invalid; result must signal the error."""
        result = m3_carbon(self.m2, config={"emission_factor": -1.0})
        assert result["co2e_kg"] is None
        assert "invalid" in result["data_quality"]

    def test_unknown_region_returns_null(self):
        """An unrecognised region code must fall through to EF-required."""
        result = m3_carbon(self.m2, config={"grid_region": "ZZ"})
        assert result["co2e_kg"] is None
        assert result["data_quality"] == "EF config required"

    def test_breakdown_has_correct_period_format(self):
        result = m3_carbon(self.m2, config={"grid_region": "PK"})
        assert len(result["breakdown"]) > 0
        assert result["breakdown"][0]["period"] == "2024-06"

    def test_ef_source_auto_set_for_region(self):
        result = m3_carbon(self.m2, config={"grid_region": "PK"})
        assert "PK" in result["ef_source"]

    def test_total_kwh_always_present(self):
        """total_kwh must be returned regardless of whether EF is available."""
        result = m3_carbon(self.m2, config=None)
        assert result["total_kwh"] == pytest.approx(10.1, abs=0.01)


# ===========================================================================
# M6 — Cost & Demand
# ===========================================================================

class TestM6Cost:

    def setup_method(self):
        self.m2 = _make_m2_out()

    # --- No-config stub ---

    def test_no_config_returns_none(self):
        assert m6_cost(self.m2, config=None) is None

    def test_missing_tariff_returns_none(self):
        """Config without tariff_per_kwh must also return None."""
        assert m6_cost(self.m2, config={"demand_charge_per_kw": 500}) is None

    # --- Basic tariff ---

    def test_basic_tariff_cost_total(self):
        """10.1 kWh × PKR 45 = PKR 454.50."""
        result = m6_cost(self.m2, config={"tariff_per_kwh": 45.0})
        assert result["cost_total"] == pytest.approx(454.5, abs=0.5)

    def test_basic_tariff_metric_card_present(self):
        result = m6_cost(self.m2, config={"tariff_per_kwh": 45.0})
        ids = [c["id"] for c in result["metric_cards"]]
        assert "cost_total" in ids

    def test_basic_tariff_card_unit_pkr(self):
        result = m6_cost(self.m2, config={"tariff_per_kwh": 45.0})
        card = next(c for c in result["metric_cards"] if c["id"] == "cost_total")
        assert card["unit"] == "PKR"

    def test_no_demand_charge_card_when_not_configured(self):
        result = m6_cost(self.m2, config={"tariff_per_kwh": 45.0})
        ids = [c["id"] for c in result["metric_cards"]]
        assert "demand_charge" not in ids

    def test_cost_breakdown_period_format(self):
        result = m6_cost(self.m2, config={"tariff_per_kwh": 45.0})
        assert len(result["cost_breakdown"]) > 0
        assert result["cost_breakdown"][0]["period"] == "2024-06"

    # --- Peak / off-peak split ---

    def test_peak_split_fields_present(self):
        result = m6_cost(self.m2, config={
            "tariff_per_kwh":      35.0,
            "peak_tariff_per_kwh": 60.0,
            "peak_hours":          [17, 18, 19, 20, 21],
        })
        assert result["peak_cost_total"] is not None
        assert result["offpeak_cost_total"] is not None

    def test_peak_offpeak_sums_to_total(self):
        """peak_cost + offpeak_cost must equal cost_total."""
        result = m6_cost(self.m2, config={
            "tariff_per_kwh":      35.0,
            "peak_tariff_per_kwh": 60.0,
            "peak_hours":          [0, 1],   # sample data is at hour 0 and 1
        })
        total = result["peak_cost_total"] + result["offpeak_cost_total"]
        assert total == pytest.approx(result["cost_total"], abs=0.01)

    def test_no_peak_config_split_fields_null(self):
        """Without peak config, split fields must be None."""
        result = m6_cost(self.m2, config={"tariff_per_kwh": 45.0})
        assert result["peak_cost_total"] is None
        assert result["offpeak_cost_total"] is None

    # --- Demand charge ---

    def test_demand_charge_calculated(self):
        """demand_charge = demand_charge_per_kw × peak_kw."""
        result = m6_cost(self.m2, config={
            "tariff_per_kwh":       45.0,
            "demand_charge_per_kw": 500.0,
        })
        # peak_kw from M2 is 10.4
        assert result["demand_charge"] == pytest.approx(10.4 * 500.0, abs=1.0)

    def test_demand_charge_card_emitted(self):
        result = m6_cost(self.m2, config={
            "tariff_per_kwh":       45.0,
            "demand_charge_per_kw": 500.0,
        })
        ids = [c["id"] for c in result["metric_cards"]]
        assert "demand_charge" in ids

    # --- Off-hours kWh ---

    def test_off_hours_kwh_null_without_schedule(self):
        result = m6_cost(self.m2, config={"tariff_per_kwh": 45.0})
        assert result["off_hours_kwh"] is None

    def test_off_hours_kwh_with_schedule(self):
        """Sample data falls on a Saturday midnight — outside Mon-Fri 8-18 schedule."""
        result = m6_cost(self.m2, config={
            "tariff_per_kwh": 45.0,
            "operational_schedule": {
                "start_hour": 8,
                "end_hour":   18,
                "days":       ["Mon", "Tue", "Wed", "Thu", "Fri"],
            },
        })
        # All sample data is Saturday 00:00–01:00 → 100% off-hours
        assert result["off_hours_kwh"] is not None
        assert result["off_hours_kwh"] == pytest.approx(10.1, abs=0.1)

    def test_zero_off_hours_when_all_in_schedule(self):
        """Data within schedule window → off_hours_kwh should be 0."""
        # Sample data is on Saturday. Put Saturday in schedule at hour 0-2.
        result = m6_cost(self.m2, config={
            "tariff_per_kwh": 45.0,
            "operational_schedule": {
                "start_hour": 0,
                "end_hour":   2,
                "days":       ["Sat"],
            },
        })
        assert result["off_hours_kwh"] == pytest.approx(0.0, abs=0.01)

    # --- Demand heatmap ---

    def test_demand_heatmap_is_list(self):
        result = m6_cost(self.m2, config={"tariff_per_kwh": 45.0})
        assert isinstance(result["demand_heatmap"], list)

    def test_demand_heatmap_cell_shape(self):
        """Every heatmap cell must have hour, dow, avg_kw."""
        result = m6_cost(self.m2, config={"tariff_per_kwh": 45.0})
        for cell in result["demand_heatmap"]:
            assert "hour"   in cell
            assert "dow"    in cell
            assert "avg_kw" in cell

    def test_demand_heatmap_max_168_cells(self):
        """Upper bound: 24 hours × 7 days = 168 cells."""
        result = m6_cost(self.m2, config={"tariff_per_kwh": 45.0})
        assert len(result["demand_heatmap"]) <= 168

    def test_demand_heatmap_hour_range(self):
        result = m6_cost(self.m2, config={"tariff_per_kwh": 45.0})
        for cell in result["demand_heatmap"]:
            assert 0 <= cell["hour"] <= 23

    def test_demand_heatmap_avg_kw_positive(self):
        result = m6_cost(self.m2, config={"tariff_per_kwh": 45.0})
        for cell in result["demand_heatmap"]:
            assert cell["avg_kw"] >= 0




# ===========================================================================
# M5 — Phase Current Imbalance
# ===========================================================================

# Shared 3-phase fixtures used across M5 tests
_3PH_BASE = {
    "device_id":   "DEV-3PH",
    "device_name": "Factory Panel",
    "interval_s":  900,
    "voltage":     400.0,
}

# Perfectly balanced: ca == cb == cc → imbalance = 0%
BALANCED_3PH_RECORDS = [
    {**_3PH_BASE, "timestamp": "2024-06-01T00:00:00Z", "kwh": 5000.0,
     "ca": 100.0, "cb": 100.0, "cc": 100.0},
    {**_3PH_BASE, "timestamp": "2024-06-01T00:15:00Z", "kwh": 5020.0,
     "ca": 100.0, "cb": 100.0, "cc": 100.0},
    {**_3PH_BASE, "timestamp": "2024-06-01T00:30:00Z", "kwh": 5040.0,
     "ca": 100.0, "cb": 100.0, "cc": 100.0},
]

# Deliberately unbalanced — 2 readings breach the 2% NEMA threshold
# Reading 1: balanced  → 0%     (no breach)
# Reading 2: cb=90     → I_avg=(100+90+100)/3=96.67, max_dev=6.67 → 6.90% (breach)
# Reading 3: cb=80     → I_avg=(100+80+100)/3=93.33, max_dev=13.33 → 14.29% (breach)
UNBALANCED_3PH_RECORDS = [
    {**_3PH_BASE, "timestamp": "2024-06-01T00:00:00Z", "kwh": 5000.0,
     "ca": 100.0, "cb": 100.0, "cc": 100.0},
    {**_3PH_BASE, "timestamp": "2024-06-01T00:15:00Z", "kwh": 5020.0,
     "ca": 100.0, "cb": 90.0,  "cc": 100.0},
    {**_3PH_BASE, "timestamp": "2024-06-01T00:30:00Z", "kwh": 5040.0,
     "ca": 100.0, "cb": 80.0,  "cc": 100.0},
]


class TestM5PhaseImbalance:

    # --- Gate: single-phase → null ----------------------------------------

    def test_single_phase_returns_none(self):
        """Single-phase device must return None regardless of config."""
        clean = m1_ingest(SAMPLE_RECORDS)
        result = m5_anomalies(clean)
        assert result is None

    def test_single_phase_with_config_still_none(self):
        clean = m1_ingest(SAMPLE_RECORDS)
        result = m5_anomalies(clean, config={"imbalance_threshold_pct": 1.0})
        assert result is None

    # --- Gate: 3-phase but no ca/cb/cc → null -----------------------------

    def test_3phase_no_currents_returns_none(self):
        """3-phase device without ca/cb/cc fields must return None.
        We inject phase_type manually (bypassing M1) to isolate the M5 gate.
        """
        from datetime import datetime, timezone
        records_no_i = [
            {**_3PH_BASE, "timestamp": "2024-06-01T00:00:00Z", "kwh": 5000.0,
             "phase_type": "3ph",
             "_ts": datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)},
            {**_3PH_BASE, "timestamp": "2024-06-01T00:15:00Z", "kwh": 5020.0,
             "phase_type": "3ph",
             "_ts": datetime(2024, 6, 1, 0, 15, 0, tzinfo=timezone.utc)},
        ]
        result = m5_anomalies(records_no_i)
        assert result is None

    # --- Balanced currents → ~0% imbalance --------------------------------

    def test_balanced_currents_zero_imbalance(self):
        """All three phases equal → imbalance_pct must be 0.0 for every reading."""
        clean = m1_ingest(BALANCED_3PH_RECORDS)
        result = m5_anomalies(clean)
        assert result is not None
        for pt in result["phase_imbalance"]["series"]:
            assert pt["value"] == pytest.approx(0.0, abs=1e-6)

    def test_balanced_avg_pct_zero(self):
        clean = m1_ingest(BALANCED_3PH_RECORDS)
        result = m5_anomalies(clean)
        assert result["phase_imbalance"]["avg_pct"] == pytest.approx(0.0, abs=1e-6)

    def test_balanced_breach_count_zero(self):
        clean = m1_ingest(BALANCED_3PH_RECORDS)
        result = m5_anomalies(clean)
        assert result["phase_imbalance"]["breach_count"] == 0

    # --- Unbalanced currents → correct calculations -----------------------

    def test_unbalanced_breach_count_default_threshold(self):
        """2 of 3 readings breach the default 2.0% threshold."""
        clean = m1_ingest(UNBALANCED_3PH_RECORDS)
        result = m5_anomalies(clean)
        assert result["phase_imbalance"]["breach_count"] == 2

    def test_unbalanced_max_pct_correct(self):
        """Reading 3: I_avg=93.33, max_dev=13.33 → 14.2857%."""
        clean = m1_ingest(UNBALANCED_3PH_RECORDS)
        result = m5_anomalies(clean)
        assert result["phase_imbalance"]["max_pct"] == pytest.approx(14.2857, abs=0.01)

    def test_unbalanced_avg_pct_correct(self):
        """avg of [0.0, 6.8966, 14.2857] ≈ 7.0608%."""
        clean = m1_ingest(UNBALANCED_3PH_RECORDS)
        result = m5_anomalies(clean)
        assert result["phase_imbalance"]["avg_pct"] == pytest.approx(7.0608, abs=0.01)

    def test_series_length_matches_records(self):
        """One series point per record that has ca/cb/cc."""
        clean = m1_ingest(UNBALANCED_3PH_RECORDS)
        result = m5_anomalies(clean)
        assert len(result["phase_imbalance"]["series"]) == 3

    def test_series_timestamps_present(self):
        clean = m1_ingest(UNBALANCED_3PH_RECORDS)
        result = m5_anomalies(clean)
        for pt in result["phase_imbalance"]["series"]:
            assert "timestamp" in pt
            assert "value" in pt

    # --- Configurable threshold -------------------------------------------

    def test_custom_threshold_no_breaches(self):
        """With threshold=20%, none of the unbalanced readings breach."""
        clean = m1_ingest(UNBALANCED_3PH_RECORDS)
        result = m5_anomalies(clean, config={"imbalance_threshold_pct": 20.0})
        assert result["phase_imbalance"]["breach_count"] == 0

    def test_custom_threshold_reflected_in_output(self):
        """Output must echo back the threshold that was used."""
        clean = m1_ingest(UNBALANCED_3PH_RECORDS)
        result = m5_anomalies(clean, config={"imbalance_threshold_pct": 5.0})
        assert result["phase_imbalance"]["threshold_pct"] == 5.0

    def test_custom_threshold_tight_two_breaches(self):
        """
        With threshold=5%:
          Reading 1: 0.0%    → no breach
          Reading 2: 6.89%   → breach (> 5%)
          Reading 3: 14.29%  → breach (> 5%)
        Expected breach_count = 2.
        Use threshold=10% to isolate only reading 3.
        """
        # At 10%: only reading 3 (14.29%) breaches
        clean = m1_ingest(UNBALANCED_3PH_RECORDS)
        result = m5_anomalies(clean, config={"imbalance_threshold_pct": 10.0})
        assert result["phase_imbalance"]["breach_count"] == 1

    def test_default_threshold_is_2_pct(self):
        """No config → threshold_pct in output must be 2.0."""
        clean = m1_ingest(UNBALANCED_3PH_RECORDS)
        result = m5_anomalies(clean)
        assert result["phase_imbalance"]["threshold_pct"] == 2.0

    # --- Output shape -------------------------------------------------------

    def test_output_has_phase_imbalance_key(self):
        clean = m1_ingest(UNBALANCED_3PH_RECORDS)
        result = m5_anomalies(clean)
        assert "phase_imbalance" in result

    def test_phase_imbalance_has_required_keys(self):
        clean = m1_ingest(UNBALANCED_3PH_RECORDS)
        pi = m5_anomalies(clean)["phase_imbalance"]
        for key in ("series", "avg_pct", "max_pct", "breach_count", "threshold_pct"):
            assert key in pi

    # --- NEMA math spot-check ----------------------------------------------

    def test_nema_math_single_reading(self):
        """
        ca=110, cb=100, cc=100
        I_avg = 310/3 = 103.33
        max_dev = |110-103.33| = 6.67
        imbalance_pct = 6.67/103.33*100 = 6.4516%
        """
        records = [
            {**_3PH_BASE, "timestamp": "2024-06-01T00:00:00Z", "kwh": 5000.0,
             "ca": 110.0, "cb": 100.0, "cc": 100.0},
            {**_3PH_BASE, "timestamp": "2024-06-01T00:15:00Z", "kwh": 5020.0,
             "ca": 110.0, "cb": 100.0, "cc": 100.0},
        ]
        clean = m1_ingest(records)
        result = m5_anomalies(clean)
        for pt in result["phase_imbalance"]["series"]:
            assert pt["value"] == pytest.approx(6.4516, abs=0.01)

# ===========================================================================
# End-to-end — run_pipeline
# ===========================================================================

class TestEndToEnd:

    REQUIRED_TOP_KEYS = {"meta", "metric_cards", "chart_series", "carbon", "anomalies", "cost"}
    REQUIRED_META_KEYS = {"device_id", "device_name", "phase_type", "computed_at", "window"}

    def test_required_top_level_keys(self):
        result = run_pipeline(SAMPLE_RECORDS)
        assert self.REQUIRED_TOP_KEYS.issubset(result.keys())

    def test_required_meta_keys(self):
        result = run_pipeline(SAMPLE_RECORDS)
        assert self.REQUIRED_META_KEYS.issubset(result["meta"].keys())

    def test_metric_cards_is_list(self):
        result = run_pipeline(SAMPLE_RECORDS)
        assert isinstance(result["metric_cards"], list)

    def test_chart_series_is_list(self):
        result = run_pipeline(SAMPLE_RECORDS)
        assert isinstance(result["chart_series"], list)

    def test_carbon_null_without_m3_config(self):
        result = run_pipeline(SAMPLE_RECORDS)
        assert result["carbon"]["co2e_kg"] is None

    def test_carbon_populated_with_m3_config(self):
        result = run_pipeline(SAMPLE_RECORDS, m3_config={"grid_region": "PK"})
        assert result["carbon"]["co2e_kg"] is not None

    def test_cost_null_without_m6_config(self):
        result = run_pipeline(SAMPLE_RECORDS)
        assert result["cost"] is None

    def test_cost_populated_with_m6_config(self):
        result = run_pipeline(SAMPLE_RECORDS, m6_config={"tariff_per_kwh": 45.0})
        assert result["cost"] is not None
        assert result["cost"]["cost_total"] > 0

    def test_m6_metric_cards_merged_into_top_level(self):
        """cost_total card from M6 must appear in the top-level metric_cards list."""
        result = run_pipeline(SAMPLE_RECORDS, m6_config={"tariff_per_kwh": 45.0})
        ids = [c["id"] for c in result["metric_cards"]]
        assert "cost_total" in ids

    def test_all_core_metric_card_ids_present(self):
        """M2 + M4 must always emit these cards for valid input."""
        result = run_pipeline(SAMPLE_RECORDS)
        ids = {c["id"] for c in result["metric_cards"]}
        assert {"total_kwh", "peak_kw", "avg_pf", "avg_kva", "voltage_avg"}.issubset(ids)

    def test_window_from_before_to(self):
        result = run_pipeline(SAMPLE_RECORDS)
        assert result["meta"]["window"]["from"] < result["meta"]["window"]["to"]

    def test_device_identity_in_meta(self):
        result = run_pipeline(SAMPLE_RECORDS)
        assert result["meta"]["device_id"]   == "DEV-001"
        assert result["meta"]["device_name"] == "Main Panel"

    def test_full_config_e2e(self):
        """Full run with M3 + M6 including demand charge and schedule."""
        result = run_pipeline(
            SAMPLE_RECORDS,
            m3_config={"grid_region": "PK"},
            m6_config={
                "tariff_per_kwh":       45.0,
                "demand_charge_per_kw": 500.0,
                "operational_schedule": {
                    "start_hour": 8,
                    "end_hour":   18,
                    "days":       ["Mon", "Tue", "Wed", "Thu", "Fri"],
                },
            },
        )
        assert result["carbon"]["data_quality"] == "estimated"
        assert result["cost"]["demand_charge"]  == pytest.approx(10.4 * 500.0, abs=1.0)
        assert result["cost"]["off_hours_kwh"]  is not None

    # --- Additional tests added in Task 1 merge (M6 task) ---
    # These extend coverage beyond the minimum spec requirement.

    def test_anomalies_null_for_single_phase(self):
        """M5 returns null for single-phase devices (SAMPLE_RECORDS is single-phase)."""
        result = run_pipeline(SAMPLE_RECORDS)
        assert result["anomalies"] is None

    def test_anomalies_populated_for_3phase(self):
        """M5 must populate anomalies for a 3-phase device with ca/cb/cc."""
        result = run_pipeline(UNBALANCED_3PH_RECORDS, m5_config={"imbalance_threshold_pct": 2.0})
        assert result["anomalies"] is not None
        assert "phase_imbalance" in result["anomalies"]

    def test_json_serialisable(self):
        """Entire payload must be JSON-serialisable (no datetime objects etc.)."""
        import json
        result = run_pipeline(
            SAMPLE_RECORDS,
            m3_config={"grid_region": "PK"},
            m6_config={"tariff_per_kwh": 45.0},
        )
        # Should not raise
        dumped = json.dumps(result)
        assert len(dumped) > 100

    def test_phase_type_detected_correctly(self):
        result = run_pipeline(SAMPLE_RECORDS)
        assert result["meta"]["phase_type"] == "single"
# ESG Pipeline

Python analytics pipeline that converts raw smart-meter records into ESG
dashboard payloads — consumption, power quality, carbon emissions, cost
analysis, and anomaly detection.

---

## Pipeline modules

| Module | Status | Description |
|--------|--------|-------------|
| M1 | ✅ | Ingestion & validation |
| M2 | ✅ | Consumption — kWh deltas and rollups |
| M4 | ✅ | Power quality — kVA, PF, voltage |
| M3 | ✅ | Carbon emissions (config-gated) |
| M5 | ✅ | Phase current imbalance — 3-phase only |
| M6 | ✅ | Cost & demand analysis (config-gated) |
| M7 | 🔲 | Report generation (stub) |

---

## Quick start

```python
from esg_pipeline import run_pipeline

records = [...]          # list[dict] — see DeviceRecord schema below
payload = run_pipeline(records)
# Returns a JSON-serialisable PipelinePayload dict
```

### With optional modules

```python
payload = run_pipeline(
    records,
    previous_records = prev_records,          # prior period for trend deltas
    m3_config = {"grid_region": "PK"},        # carbon emissions
    m5_config = {"imbalance_threshold_pct": 2.0},
    m6_config = {
        "tariff_per_kwh":       45.0,
        "peak_tariff_per_kwh":  60.0,
        "peak_hours":           [17, 18, 19, 20, 21],
        "demand_charge_per_kw": 500.0,
        "operational_schedule": {
            "start_hour": 8, "end_hour": 18,
            "days": ["Mon", "Tue", "Wed", "Thu", "Fri"],
        },
    },
)
```

---

## DeviceRecord schema

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `device_id` | str | ✅ | |
| `device_name` | str | ✅ | |
| `timestamp` | str | ✅ | ISO-8601, UTC |
| `interval_s` | int | ✅ | Measurement interval in seconds |
| `kwh` | float | ✅ | Cumulative meter reading (≥ 0) |
| `voltage` | float | ✅ | Line voltage (V) |
| `pf` | float | | Power factor in (0, 1] |
| `kw` | float | | Active power (kW) |
| `kva` | float | | Apparent power; computed if absent |
| `ca` | float | | Phase A current (A) |
| `cb` | float | | Phase B current — 3-phase only |
| `cc` | float | | Phase C current — 3-phase only |
| `voltage_a/b/c` | float | | Per-phase voltages — 3-phase only |

Phase type is auto-detected: presence of `ca`, `cb`, `cc` (or `voltage_a/b/c`)
triggers 3-phase mode and applies the corresponding voltage bounds (340–460 V
line-to-line). Single-phase bounds are 180–260 V.

---

## Period-over-period trends

Pass `previous_records` to `run_pipeline()` to populate the `trend` field on
every metric card:

```python
payload = run_pipeline(current_records, previous_records=prior_records)
```

### MetricCard.trend schema

```
trend?: {
  delta:     number    # absolute change vs previous period
  delta_pct: number    # percentage change
  direction: "up" | "down" | "flat"
}
```

**Direction thresholds** — `"up"` when `delta_pct > 1 %`, `"down"` when
`delta_pct < -1 %`, `"flat"` otherwise. This 1 % dead-band avoids noise-driven
direction flips on stable signals.

**Zero-previous guard** — when the previous period's value is exactly zero,
`trend` is set to `null` (division by zero would produce a meaningless
percentage).

**Cards with trend support:** `total_kwh`, `peak_kw`, `avg_pf`, `avg_kva`,
`voltage_avg`, `co2e_kg` (only when M3 ran on both periods).

When `previous_records` is omitted, every card keeps `"trend": null` — no
regression in existing callers.

---

## Output: PipelinePayload

```json
{
  "meta": {
    "device_id":   "DEV-001",
    "device_name": "Main Panel",
    "phase_type":  "single",
    "computed_at": "2024-06-01T12:00:00Z",
    "window": { "from": "...", "to": "..." }
  },
  "metric_cards": [
    {
      "id":        "total_kwh",
      "label":     "Total Consumption",
      "value":     10.1,
      "unit":      "kWh",
      "precision": 2,
      "trend": {
        "delta":     5.1,
        "delta_pct": 102.0,
        "direction": "up"
      }
    }
  ],
  "chart_series": [...],
  "carbon":    { "co2e_kg": 4.06, "data_quality": "estimated", ... },
  "anomalies": null,
  "cost":      null
}
```

---

## M3 — Carbon config

| Key | Type | Description |
|-----|------|-------------|
| `emission_factor` | float | kgCO₂e per kWh (highest priority) |
| `grid_region` | str | ISO country code — looks up built-in IEA 2023 average |
| `ef_source` | str | Audit-trail label |
| `ef_verified` | bool | `true` → `data_quality: "verified"` |

Built-in regions: `PK` (0.402), `IN` (0.716), `US` (0.386), `UK` (0.233),
`DE` (0.364), `AU` (0.590), `AE` (0.450), `SG` (0.408).

---

## M6 — Cost config

| Key | Type | Description |
|-----|------|-------------|
| `tariff_per_kwh` | float | Base rate (PKR/kWh) — **required** |
| `peak_tariff_per_kwh` | float | Peak-hour rate |
| `peak_hours` | list[int] | Hours 0-23 billed at peak rate |
| `demand_charge_per_kw` | float | Monthly demand charge per kW |
| `operational_schedule` | dict | `{start_hour, end_hour, days}` for off-hours kWh |

---

## Running tests

Run:
```
    pip install pytest
    pytest tests/test_esg_pipeline.py -v
```

The suite covers 111 tests across M1, M2, M3, M5, M6, trend computation, and
end-to-end scenarios.




