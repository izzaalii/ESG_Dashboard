# ESG Pipeline — Data Contract v1.0

> **Audience**: Pipeline engineers + dashboard component authors.  
> **Rule**: The pipeline writes this shape. The dashboard reads it. Neither side invents fields.

---

## 1. Input — Device Record

```ts
interface DeviceRecord {
  // Identity
  device_id:   string;           // unique hardware ID
  device_name: string;
  phase_type:  "single" | "3ph"; // detected by pipeline (M1)
  location?:   string;

  // Timestamps
  timestamp:   string;           // ISO-8601, UTC  e.g. "2024-06-01T14:00:00Z"
  interval_s:  number;           // seconds between readings (default 900 = 15 min)

  // Energy / Demand
  kwh:         number;           // cumulative kWh meter reading
  kva?:        number;           // apparent power (optional — computed if absent)
  pf?:         number;           // power factor 0–1 (optional — computed if absent)

  // Voltage (per-phase when 3-phase; single value when single-phase)
  voltage:     number;           // V_avg or V_rms
  voltage_a?:  number;           // Phase A (3-phase only)
  voltage_b?:  number;           // Phase B (3-phase only)
  voltage_c?:  number;           // Phase C (3-phase only)

  // Current (per-phase, optional)
  ca?:         number;           // Phase A amps
  cb?:         number;           // Phase B amps
  cc?:         number;           // Phase C amps
}
```

**Validation rules (M1)**

| Field | Rule |
|-------|------|
| `kwh` | ≥ 0; monotonically non-decreasing within a device session |
| `pf` | 0 < pf ≤ 1 if present |
| `voltage` | 180–260 V (single-phase) / 360–440 V (line-to-line, 3-phase) |
| `timestamp` | parseable ISO-8601; not in future; not > 48 h stale |
| `ca/cb/cc` | required together — all three or none |

---

## 2. Output — Pipeline Payload

```ts
interface PipelinePayload {
  meta: {
    device_id:    string;
    device_name:  string;
    phase_type:   "single" | "3ph";
    computed_at:  string;          // ISO-8601 UTC
    window: {
      from: string;                // ISO-8601 UTC
      to:   string;                // ISO-8601 UTC
    };
  };

  metric_cards: MetricCard[];
  chart_series: ChartSeries[];

  // Stubs — null when module not run / hardware missing
  carbon?:      CarbonResult | null;   // M3
  anomalies?:   AnomalyResult | null;  // M5
  cost?:        CostResult | null;     // M6
}
```

### 2a. `metric_cards`

One object per KPI scalar shown on the dashboard.

```ts
interface MetricCard {
  id:        string;   // snake_case key e.g. "total_kwh", "avg_pf", "peak_kva"
  label:     string;   // display string  e.g. "Total Consumption"
  value:     number;
  unit:      string;   // "kWh" | "kVA" | "kW" | "V" | "A" | "%" | "kg CO₂" | "PKR"
  precision: number;   // decimal places for rendering
  trend?: {
    delta:     number; // absolute change vs previous period
    delta_pct: number; // % change vs previous period
    direction: "up" | "down" | "flat";
  };
}
```

**Canonical card IDs** (pipeline must emit all that are computable):

| `id` | Source module |
|------|---------------|
| `total_kwh` | M2 |
| `peak_kw` | M2 |
| `avg_pf` | M4 |
| `avg_kva` | M4 |
| `voltage_avg` | M4 |
| `voltage_imbalance_pct` | M4 (3-phase only) |
| `carbon_kg` | M3 (stub if absent) |
| `cost_total` | M6 (stub if absent) |

### 2b. `chart_series`

Array of time-series ready for direct use in chart components (no transformation needed).

```ts
interface ChartSeries {
  id:         string;          // e.g. "kwh_hourly", "pf_15min", "voltage_a_15min"
  label:      string;          // human display name
  unit:       string;
  resolution: "15min" | "1h" | "1d" | "1mo";
  points: Array<{
    timestamp: string;         // ISO-8601 UTC
    value:     number;
  }>;
}
```

**Canonical series IDs**:

| `id` | Resolution | Source |
|------|-----------|--------|
| `kwh_15min` | 15 min | M2 |
| `kwh_hourly` | 1 h | M2 |
| `kwh_daily` | 1 d | M2 |
| `kwh_monthly` | 1 mo | M2 |
| `kva_15min` | 15 min | M4 |
| `pf_15min` | 15 min | M4 |
| `voltage_15min` | 15 min | M4 |
| `voltage_a_15min` | 15 min | M4 (3-phase) |
| `voltage_b_15min` | 15 min | M4 (3-phase) |
| `voltage_c_15min` | 15 min | M4 (3-phase) |

---


### 2c. `CarbonResult`

Returned under `payload.carbon`. Present when M3 runs; `null` when config missing.

```ts
interface CarbonResult {
  co2e_kg:      number | null;
  co2e_per_kwh: number | null;   // emission factor used (audit trail)
  total_kwh:    number;
  data_quality: "verified" | "estimated" | "EF config required";
  ef_source:    string | null;
  breakdown: Array<{
    period:   string;   // "YYYY-MM"
    kwh:      number;
    co2e_kg:  number;
  }>;
}
```

**`data_quality` badge mapping for dashboard:**

| Value | Badge colour | Meaning |
|-------|-------------|---------|
| `"verified"` | 🟢 Green | EF from audited/certified source (`ef_verified: true`) |
| `"estimated"` | 🟡 Amber | EF from IEA regional average or unverified user value |
| `"EF config required"` | 🔴 Red / locked | No EF supplied — `co2e_kg` is null; show placeholder |

### 2d. `CostResult`

Returned under `payload.cost`. Present when M6 runs with `tariff_per_kwh`; `null` otherwise.

```ts
interface CostResult {
  cost_total:          number;         // total PKR for period
  cost_breakdown: Array<{
    period:    string;                 // "YYYY-MM"
    kwh:       number;
    cost_pkr:  number;
  }>;
  peak_cost_total:     number | null;  // PKR from peak-rate hours (null if no peak config)
  offpeak_cost_total:  number | null;
  demand_charge:       number | null;  // PKR demand charge (null if not configured)
  off_hours_kwh:       number | null;  // kWh outside operational schedule (null if no schedule)
  demand_heatmap: Array<{
    hour:    number;                   // 0–23
    dow:     string;                   // "Mon" | "Tue" | ... | "Sun"
    avg_kw:  number;                   // average kW for that hour/day slot
  }>;                                  // max 168 cells (24h × 7 days)
  metric_cards: MetricCard[];          // cost_total always; demand_charge if configured
}
```

**M6 config keys (passed as `m6_config` to `run_pipeline`):**

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `tariff_per_kwh` | float | ✅ | Base PKR/kWh rate |
| `peak_tariff_per_kwh` | float | optional | Higher rate for peak hours |
| `peak_hours` | int[] | optional | Hours 0–23 treated as peak |
| `demand_charge_per_kw` | float | optional | Monthly demand charge per kW |
| `operational_schedule` | object | optional | `{start_hour, end_hour, days}` for off-hours calc |

## 3. Null / Stub Convention

When a module cannot run, its key is present but set to `null`.  
Dashboard components must handle `null` gracefully (show "—" / skeleton state).

```json
{
  "carbon": null,
  "anomalies": null,
  "cost": null
}
```

---

## 4. Versioning

The payload includes `meta.computed_at`. Breaking schema changes bump the contract version in this doc and require a dashboard migration.

---

*Last updated: 2024-06, v1.2 — Added CarbonResult (M3) and CostResult (M6) | Owner: ESG Platform Team*
