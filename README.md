# OfferLift

**An offer-incrementality and lifecycle-measurement warehouse for brick-and-mortar cashback — built on Snowflake, dbt, Dagster, and AWS.**

OfferLift answers the question a cashback business actually lives or dies on: *did this offer cause the purchase, or would it have happened anyway?* It models synthetic gas / grocery / dining transactions and lifecycle-messaging events, transforms them into a tested dbt warehouse, and produces two decision-grade marts — measured offer **incrementality** and lifecycle **campaign attribution** — with a monitoring layer that guards the metrics themselves, not just the pipelines.

The design principle throughout: catch bad data before it reaches a stakeholder dashboard. Every model has a point of view about its shape, a test that defends it, and a runbook for when it breaks.

---

## Why this exists

Correlation is cheap. A user who redeems an offer and then buys gas looks like a win in a naive attribution report — but many of those users would have bought anyway. Reporting that as "lift" quietly inflates ROI and burns marketing budget. OfferLift treats incrementality as a first-class, tested output: it builds a counterfactual, measures lift against it, and refuses to ship a number that fails its guardrails.

---

## Architecture

```
  Synthetic          AWS S3            Snowflake                dbt                     Consumers
  generators   ──►   (raw zone)  ──►   (RAW schema)  ──►   staging → core → marts  ──►  BI / metrics
   (Python)          Parquet           COPY INTO          (dimensional + OBT)          semantic layer
       │                                                        │
       └──────────────────────── Dagster (orchestration, scheduling, asset lineage) ──┘
                                            │
                              Data-quality + metric guardrails
                            (dbt tests, freshness, anomaly alerts)
```

- **Ingestion** — Python generators produce realistic transaction and messaging events (with intentional dirtiness: dupes, late arrivals, null merchant IDs) and land Parquet in an S3 raw zone.
- **Load** — Snowflake `COPY INTO` from the S3 external stage into a `RAW` schema.
- **Transform** — dbt: `staging` → `core` (dimensional) → `marts` (analytics).
- **Orchestrate** — Dagster runs the generators, the load, and `dbt build` as assets, with schedules, sensors, and lineage.
- **Serve** — two analytics marts + a small metrics/semantic layer.

---

## Tech stack

| Layer          | Tool                                    |
|----------------|-----------------------------------------|
| Storage (raw)  | AWS S3 (Parquet, partitioned by date)   |
| Warehouse      | Snowflake                               |
| Transformation | dbt (core), with dbt tests + `dbt docs` |
| Orchestration  | Dagster (assets, schedules, sensors)    |
| CI/CD          | GitHub Actions (`dbt build` on PRs)     |
| Data quality   | dbt tests + `dbt-expectations` + custom singular tests |
| Language       | SQL (window functions, DiD logic) + Python |

---

## Data model

### Sources (`RAW`)
- `raw.transactions` — user_id, merchant_id, category (gas/grocery/dining), amount, timestamp
- `raw.offers` — offer_id, merchant_id, category, cashback_pct, start/end dates
- `raw.redemptions` — user_id, offer_id, transaction_id, redeemed_at
- `raw.messaging_events` — user_id, campaign_id, channel, event_type (send/open/click), timestamp

### Core (dimensional — Kimball)
- `dim_users`, `dim_merchants`, `dim_offers`, `dim_campaigns`, `dim_date`
- `fct_transactions` — grain: one row per transaction
- `fct_messaging_events` — grain: one row per messaging event

**Modeling decision (documented in `/docs/design.md`):** the core is dimensional for reusability and clean joins; the incrementality mart below is deliberately **one big table (OBT)** — pre-joined and denormalized — because it's read repeatedly by lift computations and BI, and query performance beats normalization there. This is the dimensional-vs-OBT tradeoff stated with a reason, not a default.

### Marts
- `mart_offer_incrementality` — measured lift per offer/cohort with confidence bounds
- `mart_lifecycle_attribution` — campaign-level conversion and attributed value

---

## The incrementality model

OfferLift estimates lift with a **difference-in-differences (DiD)** design against a matched control cohort:

1. **Cohorts** — for each offer, define an exposed group (users who saw/were eligible for the offer) and a control group (comparable users who weren't), matched on pre-period spend, category mix, and frequency.
2. **Pre/post windows** — measure spend for both groups before and after the offer goes live.
3. **DiD estimate** — lift = (exposed_post − exposed_pre) − (control_post − control_pre). This nets out the underlying trend both groups share, so what's left is attributable to the offer.
4. **Outputs** — `incremental_transactions`, `incremental_spend`, and `incremental_margin` per offer and cohort, plus a naive (last-touch) number alongside so the gap between "looks like lift" and "is lift" is visible.

The DiD logic lives in SQL/dbt (window functions over the matched panel); the cohort matching runs as a Python asset in Dagster upstream of the mart.

---

## dbt project structure

```
offerlift/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── stg_transactions.sql
│   │   ├── stg_offers.sql
│   │   ├── stg_redemptions.sql
│   │   ├── stg_messaging_events.sql
│   │   └── _staging.yml            # tests: not_null, unique, accepted_values
│   ├── core/
│   │   ├── dim_users.sql
│   │   ├── dim_merchants.sql
│   │   ├── dim_offers.sql
│   │   ├── dim_campaigns.sql
│   │   ├── dim_date.sql
│   │   ├── fct_transactions.sql
│   │   ├── fct_messaging_events.sql
│   │   └── _core.yml               # relationship + grain tests
│   └── marts/
│       ├── mart_offer_incrementality.sql
│       ├── mart_lifecycle_attribution.sql
│       └── _marts.yml              # metric guardrail tests + exposures
├── tests/                          # singular tests (guardrails)
│   ├── assert_lift_within_bounds.sql
│   └── assert_no_negative_spend.sql
├── macros/
└── seeds/
```

---

## Data quality & monitoring — the part most candidates skip

This is the layer that turns "I built some dbt models" into "I own models my team trusts."

**Schema-level tests** — `not_null`, `unique`, `accepted_values`, `relationships` on every staging and core model.

**Grain tests** — every fact asserts its declared grain (no accidental fan-out from a bad join).

**Freshness & volume** — dbt source freshness plus `dbt-expectations` row-count and range checks; a Dagster sensor alerts on volume anomalies (today's transaction count outside an expected band).

**Metric guardrails (singular tests)** — the differentiator. `assert_lift_within_bounds.sql` fails the build if measured incrementality falls outside a plausible range, so an implausible lift number never silently ships. A metric can be wrong even when every column passes its column test — this catches that.

**Alerting** — failed tests and sensor trips post to a channel (Slack webhook) with the offending model and a link to the runbook.

---

## Orchestration (Dagster)

Assets, not just scheduled scripts, so lineage is explicit:

```
generate_raw_data → load_to_snowflake → match_cohorts → dbt_build → freshness_check
```

- **Schedule** — daily run.
- **Sensor** — volume-anomaly sensor that alerts before the marts refresh on bad input.
- **Lineage** — the Dagster asset graph mirrors the dbt DAG, so a broken upstream is visible at a glance.

---

## CI/CD

GitHub Actions on every pull request:
1. `dbt deps`
2. `dbt build --target ci` against a CI Snowflake schema (or DuckDB for a zero-cost local variant)
3. `dbt test`
4. Fail the PR if any test or guardrail fails.

Nothing merges without passing the same tests that guard production.

---

## Semantic / metrics layer (stretch)

Define the money metrics **once** — `incremental_transactions`, `incremental_spend`, `incremental_margin` — in the dbt Semantic Layer (or a lightweight metrics YAML), so BI, ad-hoc SQL, and any AI/agent tooling read the same definition instead of re-deriving lift five different ways. This maps directly to the "make warehouse data usable by both humans and AI" nice-to-have.

---

## Runbook (example)

**Alert:** `assert_lift_within_bounds` failed on `mart_offer_incrementality`.

1. Check the Dagster volume sensor — did today's transaction load spike or drop? A partial load skews DiD.
2. Inspect `match_cohorts` output — are exposed/control groups still balanced on pre-period spend? Imbalance inflates lift.
3. Query the naive vs DiD columns — if only DiD moved, suspect the control cohort; if both moved, suspect the input data.
4. Re-run the failed asset after fixing input; do **not** force-ship the mart.
5. Log the cause in `/docs/incidents.md` and add a test if this is a new failure mode.

Full runbooks: `/docs/runbooks/`.

---

## Local setup

```bash
git clone https://github.com/sajansshergill/offerlift.git
cd offerlift
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env          # add Snowflake + AWS creds (or use the DuckDB target)
python scripts/generate_data.py
dagster dev                   # runs generators → load → dbt build
dbt docs generate && dbt docs serve
```

A **DuckDB target** is included so the whole project runs locally at zero warehouse cost; the Snowflake target is the production path.

---

## Roadmap

- [ ] Staging + core dimensional models with full test coverage
- [ ] Cohort matching + DiD incrementality mart
- [ ] Lifecycle attribution mart
- [ ] Metric guardrails + Slack alerting
- [ ] Dagster schedules + volume-anomaly sensor
- [ ] GitHub Actions CI
- [ ] Semantic layer for the three money metrics
- [ ] Runbooks + lineage diagram in `/docs`

---

## What this demonstrates

- Owning a scoped dbt domain end to end — design, build, test, ship, monitor
- SQL fluency: window functions, matched-panel joins, DiD in-warehouse
- A defensible modeling opinion (dimensional core + OBT incrementality mart)
- Monitoring and alerting that catches problems before stakeholders do
- Marketing-measurement depth: incrementality, lift, attribution, experimentation
- Docs and runbooks that let the next person own the work

---

*Built by Sajan Shergill — sajansshergill.github.io · github.com/sajansshergill*
