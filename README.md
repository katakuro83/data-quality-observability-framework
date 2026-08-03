# Data Quality & Observability Framework

Framework for validating pipeline outputs, tracking data lineage, and alerting on anomalies.

**Stack:** Great Expectations · dbt tests · Airflow

---

## What This Framework Does

Most pipelines have *some* data quality checks, but three gaps show up repeatedly: checks live in different tools with no shared history, nobody's tracking what feeds what, and a failing check doesn't reach anyone until someone happens to look. This framework addresses all three:

```
┌─────────────────┐     ┌──────────────────┐     ┌────────────────┐
│  dbt test       │     │Great Expectations│     │  Lineage events│
│  (schema tests) │      (distribution/    │     │  (source ->    │
│  results parsed │     │business rule     │     │  target per run│
│from run_results.│     │  checkpoints)    │     │                │
│  json           │     │                  │     │                │
└────────┬─────────┘    └──────────┬───────┘     └────────┬───────┘
         │                         │                      │
         └──────────────┬───────────┴────────────────────────┘
                          ▼
                ┌──────────────────┐
                │  Metrics store   │   <- SQLite/Postgres: every check result,
                │  (metrics_store) │      every metric value, every lineage edge
                └────────┬─────────┘
                          ▼
                ┌──────────────────┐
                │ Anomaly detector │   <- statistical (z score) comparison
                │                  │      against historical metric values
                └────────┬─────────┘
                          ▼
                ┌──────────────────┐
                │Alert dispatcher  │   <- Slack / email / console, pluggable
                └──────────────────┘
```

All of this is orchestrated by an Airflow DAG that runs after any pipeline completes.

---

## Components

| Component | Purpose | Location |
|---|---|---|
| **dbt test result parser** | Reads dbt's standard `run_results.json` artifact and records pass/fail per test into the metrics store no dbt Cloud API needed, works with any dbt project | `dbt_tests_integration/parse_dbt_results.py` |
| **Great Expectations suites** | Business rule and distributional checks that go beyond dbt's schema tests (e.g., "revenue should be within 3 std dev of the trailing 30 day average") | `great_expectations/expectations/` |
| **Metrics store** | Central SQLite/Postgres store for check results, metric history, and lineage events the shared history other DQ setups lack | `src/metrics_store/metrics_repository.py` |
| **Lineage tracker** | Records source → target dataset relationships per pipeline run; renders as a Mermaid graph | `src/lineage/lineage_tracker.py` |
| **Anomaly detector** | Z score based statistical check: flags a metric value as anomalous if it deviates significantly from its own history | `src/anomaly_detection/anomaly_detector.py` |
| **Alert dispatcher** | Pluggable notification interface console (for local dev/testing), Slack, email | `src/alerting/alert_dispatcher.py` |
| **Airflow DAG** | Wires all of the above into one pipeline that runs after the main data pipeline | `airflow/dags/data_quality_pipeline_dag.py` |

---

## Project Structure

```
data-quality-framework-great-expectations/
├── great_expectations/
│   ├── expectations/          # Expectation suites (JSON) per dataset
│   └── checkpoints/           # GE checkpoint definitions
├── src/
│   ├── metrics_store/         # SQLite/Postgres backed results store (stdlib only)
│   ├── lineage/               # Lineage event recording + Mermaid graph rendering
│   ├── anomaly_detection/     # Z score anomaly detection over metric history
│   └── alerting/              # Pluggable alert dispatchers
├── dbt_tests_integration/
│   └── parse_dbt_results.py   # Parses dbt's run_results.json into the metrics store
├── airflow/dags/
│   └── data_quality_pipeline_dag.py
├── tests/                     # Real, runnable pytest suite (stdlib + pytest only)
├── architecture/architecture.md
└── docs/setup_guide.md
```

---

## Design Highlights

- **One shared history, multiple check sources** dbt test results and Great Expectations results both land in the same `metrics_repository`, so "how has this dataset's quality trended over the last month" has one answer, not two disconnected ones.
- **Anomaly detection is statistical, not hardcoded thresholds** instead of a fixed "row count must be > 10,000," the detector compares against that dataset's own trailing history (mean ± N standard deviations), which adapts as the dataset naturally grows.
- **Lineage is a byproduct of running checks, not a separate manual exercise** every DQ run records which dataset it validated and what it was built from, so a lineage graph builds up automatically over time.
- **Alerting is pluggable, not hardcoded to one channel** `AlertDispatcher` is an interface; swap `ConsoleAlertDispatcher` for `SlackAlertDispatcher` without touching any calling code.

---

## Possible Extensions

- Add OpenLineage/Marquez integration for a standards-based lineage backend instead of the custom lightweight tracker
- Add a small dashboard (Streamlit/Metabase) reading directly from `metrics_repository` for a visual DQ trend view
- Add PagerDuty/Opsgenie as an `AlertDispatcher` implementation for on call escalation
- Extend anomaly detection with seasonality awareness (e.g., day of week baselines) rather than a flat trailing window

---
