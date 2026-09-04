# OfferLift

**An offer-incrementality and lifecycle-measurement warehouse for brick-and-mortar cashback — built on Snowflake, dbt, Dagster, and AWS, with a DuckDB target for zero-cost local runs.**

OfferLift answers the question a cashback business actually lives or dies on: *did this offer cause the purchase, or would it have happened anyway?* It generates synthetic gas / grocery / dining transactions and lifecycle-messaging events with a **real, seeded treatment effect**, transforms them through a tested dbt warehouse, and produces two decision-grade marts — measured offer **incrementality** and lifecycle **campaign attribution** — behind a monitoring layer that guards the metrics themselves, not just the pipelines.

The design principle throughout: catch bad data before it reaches a stakeholder dashboard. Every model has a point of view about its shape, a test that defends it, and a runbook for when it breaks.

---

## Why this exists

Correlation is cheap. A user who redeems an offer and then buys gas looks like a win in a naive attribution report — but many of those users would have bought anyway. Reporting that as "lift" quietly inflates ROI and burns marketing budget. OfferLift treats incrementality as a first-class, tested output: it builds a counterfactual with a matched control cohort, measures lift with difference-in-differences, and refuses to ship a number that fails its guardrails.

Because the synthetic generator bakes in a known true lift (`TRUE_LIFT = 0.18`), the project is also self-validating — the DiD mart should recover a number close to the seeded effect, and the guardrail test proves it does.

---

## Architecture

```
  Synthetic          AWS S3            Snowflake / DuckDB        dbt                     Consumers
  generators   -->   (raw zone)  -->   (raw schema)      -->  staging -> core ->      -->  BI / metrics
   (Python)          Parquet           COPY / read_parquet    intermediate -> marts        semantic layer
       |                                                            |
       +-------------------------- Dagster (orchestration, scheduling, asset lineage) ------+
                                              |
                                Data-quality + metric guardrails
                              (dbt tests, freshness, anomaly sensor)
```

- **Ingestion** — Python generators produce realistic transaction and messaging events (with intentional dirtiness: duplicates, null merchant IDs, negative amounts, late arrivals) and write partitioned Parquet.
- **Load** — Snowflake `COPY INTO` from an S3 external stage in production; locally, DuckDB reads the Parquet into a `raw` schema (same shape, no cost).
- **Transform** — dbt: `staging` -> `core` (dimensional) -> `intermediate` -> `marts` (analytics).
- **Orchestrate** — Dagster runs the generators, the load, cohort matching, and `dbt build` as assets, with schedules, a volume-anomaly sensor, and full lineage.
- **Serve** — two analytics marts plus a small metrics/semantic layer.

---

## Tech stack

| Layer          | Tool                                              |
|----------------|---------------------------------------------------|
| Storage (raw)  | AWS S3 (Parquet, partitioned by date)             |
| Warehouse      | Snowflake (prod) / DuckDB (local, free)           |
| Transformation | dbt 1.8 (core), dbt tests, `dbt docs`             |
| Orchestration  | Dagster 1.8 + `dagster-dbt` (assets, sensors)     |
| CI/CD          | GitHub Actions (`dbt build` on PRs, DuckDB target)|
| Data quality   | dbt tests + `dbt_expectations` + singular guardrails |
| Language       | SQL (window functions, DiD) + Python              |

---

## Project structure

```
offerlift/
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
├── Makefile
├── dbt_project.yml
├── packages.yml
├── profiles.yml
├── scripts/
│   └── generate_data.py            # synthetic data + seeded true lift
├── offerlift_dagster/
│   ├── __init__.py
│   ├── definitions.py
│   ├── resources.py
│   ├── assets.py                   # generators -> load -> cohorts -> dbt build
│   ├── schedules.py
│   └── sensors.py                  # volume-anomaly alerting
├── models/
│   ├── staging/
│   │   ├── _staging__sources.yml
│   │   ├── _staging__models.yml
│   │   ├── stg_transactions.sql
│   │   ├── stg_offers.sql
│   │   ├── stg_redemptions.sql
│   │   └── stg_messaging_events.sql
│   ├── core/
│   │   ├── _core__models.yml
│   │   ├── dim_users.sql
│   │   ├── dim_merchants.sql
│   │   ├── dim_offers.sql
│   │   ├── dim_campaigns.sql
│   │   ├── dim_date.sql
│   │   ├── fct_transactions.sql
│   │   └── fct_messaging_events.sql
│   ├── intermediate/
│   │   ├── int_user_cohorts.sql    # exposed vs matched control
│   │   └── int_offer_panel.sql     # pre/post spend panel per user-offer
│   └── marts/
│       ├── _marts__models.yml
│       ├── mart_offer_incrementality.sql   # DiD lift (OBT)
│       └── mart_lifecycle_attribution.sql
├── tests/
│   ├── assert_lift_within_bounds.sql       # metric guardrail
│   └── assert_no_negative_spend.sql
├── macros/
│   └── cents_to_dollars.sql
├── seeds/
│   └── .gitkeep
├── docs/
│   ├── design.md
│   └── runbooks/
│       └── incrementality_alert.md
└── .github/
    └── workflows/
        └── ci.yml
```

---

## Data model

### Sources (`raw`)
- `raw.transactions` — user_id, merchant_id, category, amount_cents, txn_ts
- `raw.offers` — offer_id, merchant_id, category, cashback_pct, start/end dates
- `raw.redemptions` — user_id, offer_id, transaction_id, redeemed_at
- `raw.messaging_events` — user_id, campaign_id, channel, event_type, event_ts
- `raw.users`, `raw.merchants`, `raw.campaigns` — dimension sources

### Core (dimensional — Kimball)
- `dim_users`, `dim_merchants`, `dim_offers`, `dim_campaigns`, `dim_date`
- `fct_transactions` — grain: one row per transaction
- `fct_messaging_events` — grain: one row per messaging event

### Intermediate
- `int_user_cohorts` — exposed vs. matched control per offer (matched on pre-period spend, category mix, frequency)
- `int_offer_panel` — pre/post spend per user-offer, the panel DiD runs on

### Marts
- `mart_offer_incrementality` — DiD lift per offer/cohort, naive number alongside, confidence bounds
- `mart_lifecycle_attribution` — campaign-level conversion and attributed value

**Modeling decision (see `docs/design.md`):** the core is dimensional for reuse and clean joins; `mart_offer_incrementality` is deliberately **one big table** — pre-joined and denormalized — because it's read repeatedly by lift computations and BI, and query performance beats normalization there. The dimensional-vs-OBT tradeoff, stated with a reason rather than defaulted.

---

## The incrementality model

Lift is estimated with **difference-in-differences** against a matched control cohort:

1. **Cohorts** (`int_user_cohorts`) — for each offer, an exposed group and a control group matched on pre-period spend, category mix, and frequency.
2. **Panel** (`int_offer_panel`) — spend per user for the pre-window and post-window (`pre_window_days` / `post_window_days`, configurable in `dbt_project.yml`).
3. **DiD** — `lift = (exposed_post - exposed_pre) - (control_post - control_pre)`, netting out the shared trend so what remains is attributable to the offer.
4. **Outputs** — `incremental_transactions`, `incremental_spend`, `incremental_margin` per offer/cohort, plus a naive last-touch number so the gap between *looks like lift* and *is lift* is visible.

The seeded true effect (0.18) gives the DiD mart a known target to recover — the guardrail test asserts the measured lift lands in a plausible band around it.

---

## Data quality & monitoring — the part most candidates skip

This is the layer that turns "I built some dbt models" into "I own models my team trusts."

- **Schema tests** — `not_null`, `unique`, `accepted_values`, `relationships` across staging and core.
- **Grain tests** — every fact asserts its declared grain (no silent fan-out).
- **Freshness & volume** — dbt source freshness plus `dbt_expectations` row-count and range checks; a Dagster sensor alerts when daily volume falls outside its expected band *before* the marts refresh on bad input.
- **Metric guardrails** — `assert_lift_within_bounds.sql` fails the build if measured incrementality falls outside `lift_lower_bound`/`lift_upper_bound`, so an implausible lift number never silently ships. A metric can be wrong even when every column passes — this catches that.
- **Alerting** — failed tests and sensor trips post to Slack with the offending model and a link to the runbook.

---

## Quickstart (local, DuckDB)

```bash
git clone https://github.com/sajansshergill/offerlift.git
cd offerlift

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env            # defaults to DBT_TARGET=duckdb, no creds needed

python scripts/generate_data.py # writes data/offerlift.duckdb (raw schema)
dbt deps
dbt build                       # staging -> core -> intermediate -> marts + tests
dbt docs generate && dbt docs serve
```

Then inspect the result:

```sql
select offer_id, naive_lift, did_lift, incremental_spend
from marts.mart_offer_incrementality
order by incremental_spend desc;
```

Run the full orchestrated pipeline instead:

```bash
dagster dev -m offerlift_dagster   # generators -> load -> cohorts -> dbt build
```

### Switching to Snowflake

Set `DBT_TARGET=snowflake` and the Snowflake vars in `.env`, then `dbt build`. The production path lands Parquet in S3 and loads via `COPY INTO`; the models are warehouse-agnostic.

---

## CI/CD

GitHub Actions on every pull request runs `dbt deps`, `dbt build --target ci` (DuckDB, zero warehouse cost), and `dbt test`. Nothing merges without passing the same tests that guard production. See `.github/workflows/ci.yml`.

---

## Semantic / metrics layer (stretch)

Define the money metrics once — `incremental_transactions`, `incremental_spend`, `incremental_margin` — in the dbt Semantic Layer (or a lightweight metrics YAML), so BI, ad-hoc SQL, and any AI/agent tooling read the same definition instead of re-deriving lift five different ways. This maps to the "make warehouse data usable by both humans and AI" nice-to-have.

---

## Configuration reference

Set in `dbt_project.yml` under `vars`:

| Var                | Default | Meaning                                    |
|--------------------|---------|--------------------------------------------|
| `pre_window_days`  | 28      | Pre-offer measurement window               |
| `post_window_days` | 28      | Post-offer measurement window              |
| `lift_lower_bound` | -0.10   | Guardrail floor for measured lift          |
| `lift_upper_bound` | 0.60    | Guardrail ceiling for measured lift        |

Generator constants live in `scripts/generate_data.py` (`N_USERS`, `N_OFFERS`, `TRUE_LIFT`, `SEED`).

---

## Roadmap

- [x] Root config + synthetic generator with seeded lift and intentional dirtiness
- [ ] Staging layer with full test coverage
- [ ] Core dimensional models
- [ ] Cohort matching + DiD incrementality mart
- [ ] Lifecycle attribution mart
- [ ] Metric guardrails + Slack alerting
- [ ] Dagster schedules + volume-anomaly sensor
- [ ] GitHub Actions CI
- [ ] Semantic layer for the three money metrics
- [ ] Runbooks + lineage diagram in `docs/`

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
