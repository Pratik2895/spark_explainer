# SparkExplainer — Making Spark Execution Understandable

<div align="center">

# SparkExplainer

**A Python library that translates Spark execution into plain English**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PySpark 3.5+](https://img.shields.io/badge/PySpark-3.5+-orange.svg)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Serverless-red.svg)](https://databricks.com/)

</div>

---

## The Problem

You run a query in Spark and see this in the UI:

```
== Physical Plan ==
AdaptiveSparkPlan
  Sort [cnt DESC]
    Exchange (Shuffle) ×4
      HashAggregate [o_orderpriority]
        Project [o_orderpriority, o_totalprice]
          Filter [o_totalprice > 100000]
            Scan parquet samples.tpch.orders
```

**Most developers don't know what this means.** They can't answer:
- How many stages will this create?
- Where is the shuffle happening and why?
- How long did it actually take?
- What can I do to make it faster?

## The Solution

**SparkExplainer** translates the above into this:

```
[STATS]  Overview
   • Stages:   2
   • Shuffles: 1

[PLAN]  Step-by-step plan breakdown:
   [OK]  Step 1: Sort
         → Ordering rows by a key — may require range shuffle
   [SHUFFLE] Step 2: Exchange (Shuffle) (×4)
         → Redistributing data across executors by key — creates a new stage
   [OK]  Step 3: Filter
         → Removing rows that don't match the condition

[TIP]  Key takeaways:
   • 1 shuffle required — data moves across executors
   • Each shuffle creates a new stage boundary
```

And then tells you what **actually happened** at runtime:

```
REAL RUNTIME METRICS (from system.query.history)

Duration:     711 ms (compile: 317 ms | exec: 394 ms)
Bytes read:   5.3 KB
Shuffle bytes: 0 bytes
Spill to disk: 0 bytes
Rows:         48 read → 9 produced
Files:        1 read | 0 pruned
```

---

## How It Works

```
┌─────────────────────────────────────────────────────────────┐
│                     YOUR QUERY                              │
│   spark.sql("SELECT country, count(*) FROM table GROUP BY 1")│
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
          ┌────────────────────────────────┐
          │     explain_query(df)          │
          │     "What Spark WILL do"       │
          │                                │
          │  Parses physical plan →        │
          │  • 2 stages                    │
          │  • 1 shuffle                   │
          │  • HashJoin, Sort, Filter       │
          └────────────────┬───────────────┘
                           │
                           ▼
          ┌────────────────────────────────┐
          │  suggest_optimizations(df)     │
          │  "How to make it faster"       │
          │                                │
          │  • Use BroadcastJoin           │
          │  • Reduce shuffles             │
          │  • Enable predicate pushdown   │
          │  • Cache if reused             │
          └────────────────┬───────────────┘
                           │
                           ▼
          ┌────────────────────────────────┐
          │   get_runtime_metrics()        │
          │   "What ACTUALLY happened"     │
          │                                │
          │  Queries system.query.history  │
          │  • Duration: 711 ms            │
          │  • Bytes read: 5.3 KB         │
          │  • Spill: 0 bytes              │
          │  • Rows: 48 → 9               │
          └────────────────┬───────────────┘
                           │
                           ▼
          ┌────────────────────────────────┐
          │ educational_walkthrough(df)    │
          │ "Beginner-friendly explanation" │
          │                                │
          │ Shuffle = "moving items        │
          │  between warehouses"            │
          │ Filter = "sorting mail"        │
          │ Join = "matching socks"        │
          └────────────────────────────────┘
```

---

## Spark Concepts Visualized

### What is a Stage?

```
  QUERY: SELECT country, count(*) FROM orders GROUP BY country

  ┌─────────── STAGE 0 ───────────┐    ┌─────────── STAGE 1 ───────────┐
  │                               │    │                               │
  │  Task 1   Task 2   Task 3     │    │  Task 1   Task 2   Task 3     │
  │  ┌────┐  ┌────┐   ┌────┐     │    │  ┌────┐  ┌────┐   ┌────┐     │
  │  │Read │  │Read │   │Read │    │    │  │Group│  │Group│   │Group│    │
  │  │File1│  │File2│   │File3│    │    │  │by   │  │by   │   │by   │    │
  │  │     │  │     │   │     │    │    │  │country│country│country│   │
  │  └──┬──┘  └──┬──┘   └──┬──┘    │    │  └──┬──┘  └──┬──┘   └──┬──┘    │
  │     │        │         │       │    │     │        │         │       │
  └─────┼────────┼─────────┼───────┘    └─────┼────────┼─────────┼───────┘
        │        │         │                  │        │         │
        ▼        ▼         ▼                  ▲        ▲         ▲
        └────────┴─────────┴── SHUFFLE ────────┴────────┴─────────┘
                        (data moves across network)
```

**Stage 0:** Each task reads a file in parallel — no data movement
**Shuffle:** Data is redistributed by key (country) across the network
**Stage 1:** Each task aggregates rows for its assigned countries

### What is a Shuffle?

```
  BEFORE SHUFFLE                          AFTER SHUFFLE
  ┌────────────────────────┐            ┌────────────────────────┐
  │ Executor 1             │            │ Executor 1             │
  │  US, US, UK, JP, US    │            │  US, US, US, US        │
  │                        │            │                        │
  │ Executor 2             │   SHUFFLE  │ Executor 2             │
  │  UK, JP, US, UK, JP    │ ────────►  │  UK, UK, UK            │
  │                        │  (network) │                        │
  │ Executor 3             │            │ Executor 3             │
  │  JP, UK, US, JP, JP    │            │  JP, JP, JP, JP        │
  └────────────────────────┘            └────────────────────────┘

  Data is scattered across executors      Data is grouped by key
  (can't aggregate yet)                   (ready for aggregation)
```

**Shuffle = moving items between warehouses so similar items end up in the same building.**
This is the most expensive operation in Spark — it involves network I/O.

### Plan vs Reality

```
  ┌──── PLAN ANALYSIS (Predicted) ──────┐   ┌──── REAL METRICS (Actual) ──────┐
  │                                     │   │                                 │
  │  Stages: 2                          │   │  Duration: 711 ms               │
  │  Shuffles: 1                        │   │  Compile: 317 ms                │
  │  Join: HashJoin                     │   │  Execute: 394 ms                │
  │  Steps: Sort → Exchange → Filter    │   │  Bytes read: 5.3 KB            │
  │                                     │   │  Shuffle: 0 bytes               │
  │  "Data will move across executors"  │   │  Spill: 0 bytes                │
  │                                     │   │  Rows: 48 read → 9 produced     │
  └─────────────────────────────────────┘   │                                 │
                                            │  "Result came from cache!"      │
                                            └─────────────────────────────────┘

  THE GAP: Spark predicted a shuffle, but the result was cached.
           Zero bytes read, zero shuffle — the plan was overruled.
           This gap is invisible without comparing both.
```

---

## Capabilities

| Capability | Method | Input | Output Example |
|---|---|---|---|
| **Plan Analysis** | `explain_query(df)` | Any DataFrame | "2 stages, 1 shuffle, HashJoin" |
| **Optimization Tips** | `suggest_optimizations(df)` | Any DataFrame | "Use BroadcastJoin to eliminate 1 shuffle" |
| **Real Runtime Metrics** | `get_runtime_metrics()` | Limit, match_text | "711 ms, 5.3 KB read, 0 spill, 48 rows" |
| **Spark UI Parsing** | `parse_spark_ui()` | None | Live metrics or system table fallback |
| **Educational Walkthrough** | `educational_walkthrough(df)` | Any DataFrame | "Shuffle = moving items between warehouses" |

---

## Quick Start

### Install

```bash
pip install pyspark>=3.5.0
```

### Use

```python
from spark_explainer import SparkExplainer

explainer = SparkExplainer()

# Pass any DataFrame
df = spark.sql("SELECT country, count(*) FROM orders GROUP BY country")

# What will Spark do?
print(explainer.explain_query(df))
# → "2 stages, 1 shuffle. Sort → Exchange (Shuffle) → Filter"

# How to make it faster?
print(explainer.suggest_optimizations(df))
# → "Reduce shuffles, enable broadcast join, watch for data skew"

# What actually happened? (from system.query.history)
print(explainer.get_runtime_metrics(limit=5))
# → "711 ms, 5.3 KB read, 0 spill, 48 rows"

# Beginner-friendly explanation
print(explainer.educational_walkthrough(df))
# → "Shuffle = like moving items between warehouses..."
```

---

## Optimization Suggestions

SparkExplainer detects these patterns and suggests fixes:

```
  ┌────────────────────────────────────────────────────────────────────────┐
  │  PATTERN DETECTED          │  SUGGESTION                              │
  ├────────────────────────────┼──────────────────────────────────────────┤
  │  SortMergeJoin on small    │  Switch to BroadcastHashJoin             │
  │  table                     │  → Eliminates shuffle entirely           │
  ├────────────────────────────┼──────────────────────────────────────────┤
  │  Multiple shuffles         │  Reduce shuffles by combining operations │
  │  (3+ Exchange nodes)       │  → Each shuffle = network I/O            │
  ├────────────────────────────┼──────────────────────────────────────────┤
  │  Filter after Join         │  Push filter before join                 │
  │                            │  → Reduces data being joined              │
  ├────────────────────────────┼──────────────────────────────────────────┤
  │  Sort before aggregation   │  Remove unnecessary sort                 │
  │                            │  → Aggregation doesn't need sorted input  │
  ├────────────────────────────┼──────────────────────────────────────────┤
  │  Data skew risk            │  Add salt key for skewed partitions      │
  │  (large partitions)        │  → Balances work across executors        │
  ├────────────────────────────┼──────────────────────────────────────────┤
  │  Repeated computation      │  Cache intermediate DataFrames           │
  │  (same scan used twice)    │  → Avoids re-computing shuffles          │
  └────────────────────────────────────────────────────────────────────────┘
```

---

## Beginner Analogies

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │  SPARK CONCEPT    │  REAL-WORLD ANALOGY                            │
  ├───────────────────┼─────────────────────────────────────────────────┤
  │  Stage            │  An assembly line station                        │
  │                   │  (each station does one part of the work)        │
  ├───────────────────┼─────────────────────────────────────────────────┤
  │  Task             │  A worker at the station                        │
  │                   │  (multiple workers run in parallel)              │
  ├───────────────────┼─────────────────────────────────────────────────┤
  │  Shuffle          │  Moving items between warehouses                 │
  │                   │  (so similar items end up in the same building)  │
  ├───────────────────┼─────────────────────────────────────────────────┤
  │  Filter            │  Sorting mail                                   │
  │                   │  (keeping only letters addressed to you)         │
  ├───────────────────┼─────────────────────────────────────────────────┤
  │  HashJoin         │  Matching left socks with right socks            │
  │                   │  (by color, using a lookup table)                │
  ├───────────────────┼─────────────────────────────────────────────────┤
  │  Sort             │  Arranging books alphabetically on a shelf       │
  ├───────────────────┼─────────────────────────────────────────────────┤
  │  BroadcastJoin    │  Giving every worker a copy of the phone book    │
  │                   │  (no need to shuffle — everyone has the data)    │
  └───────────────────┴─────────────────────────────────────────────────┘
```

---

## Architecture

```
  ┌─────────────────────────────────────────────────────────────┐
  │                    INPUT                                     │
  │    SQL string  OR  PySpark DataFrame                         │
  └──────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │              SparkExplainer Class                            │
  │                                                             │
  │  ┌─────────────────────────────────────────────────────┐    │
  │  │  explain_query()                                     │    │
  │  │  ├── _get_plan_text()     → captures EXPLAIN output  │    │
  │  │  ├── _parse_plan_nodes()  → identifies node types    │    │
  │  │  ├── _count_shuffles()    → counts Exchange nodes    │    │
  │  │  ├── _count_stages()     → infers stage boundaries  │    │
  │  │  └── _node_detail()      → human-readable descriptions│   │
  │  └─────────────────────────────────────────────────────┘    │
  │                                                             │
  │  ┌─────────────────────────────────────────────────────┐    │
  │  │  suggest_optimizations()                             │    │
  │  │  ├── Detects SortMergeJoin → suggests BroadcastJoin  │    │
  │  │  ├── Counts shuffles → suggests reducing them        │    │
  │  │  ├── Detects filter placement → suggests pushdown    │    │
  │  │  └── Detects repeated scans → suggests caching       │    │
  │  └─────────────────────────────────────────────────────┘    │
  │                                                             │
  │  ┌─────────────────────────────────────────────────────┐    │
  │  │  get_runtime_metrics()                               │    │
  │  │  ├── Queries system.query.history                    │    │
  │  │  ├── Filters by current user + FINISHED status       │    │
  │  │  └── Returns: duration, bytes, spill, rows, files    │    │
  │  └─────────────────────────────────────────────────────┘    │
  │                                                             │
  │  ┌─────────────────────────────────────────────────────┐    │
  │  │  parse_spark_ui()  — two-tier fallback               │    │
  │  │  ├── Tier 1: sc.statusTracker()  (classic cluster)   │    │
  │  │  └── Tier 2: get_runtime_metrics() (serverless)      │    │
  │  └─────────────────────────────────────────────────────┘    │
  │                                                             │
  │  ┌─────────────────────────────────────────────────────┐    │
  │  │  educational_walkthrough()                           │    │
  │  │  ├── Adds analogies from ANALOGIES dict              │    │
  │  │  ├── Builds step-by-step stage walk                  │    │
  │  │  └── Marks shuffles as "warehouse moves"             │    │
  │  └─────────────────────────────────────────────────────┘    │
  │                                                             │
  │  Helpers: _format_bytes(), _get_df(), _identify_node()       │
  └─────────────────────────────────────────────────────────────┘
                             │
                             ▼
  ┌─────────────────────────────────────────────────────────────┐
  │                    OUTPUT                                    │
  │  Plain-English text with stages, shuffles, metrics,         │
  │  optimization tips, and beginner analogies                   │
  └─────────────────────────────────────────────────────────────┘
```

---

## Demo Results

Tested on 3 Databricks sample catalogs:

| Query | Catalog | Stages | Shuffles | Optimizations Suggested |
|-------|---------|--------|----------|------------------------|
| Group By + Sort | `samples.tpch.orders` | 2 | 1 | Predicate pushdown |
| Orders + Customer Join | `samples.tpch.orders` + `samples.tpch.customer` | 4 | 3 | Reduce shuffles, pushdown, skew, cache |
| Filter + Sort + Limit | `samples.nyctaxi.trips` | 1 | 0 | Predicate pushdown |

**Key finding:** The join query had 3 shuffles and triggered 4 optimization suggestions.
The NYC taxi query had 0 shuffles — the most efficient pattern.

---

## Files

| File | Description |
|------|-------------|
| `spark_explainer.py` | The full SparkExplainer class (39.7 KB) |
| `demo.py` | Demo script with TPCH, NYC Taxi, and Bakehouse queries (13.5 KB) |
| `requirements.txt` | PySpark >= 3.5.0 (only dependency) |

---

## Requirements

- Python 3.10+
- PySpark 3.5+
- Databricks workspace (for `system.query.history` runtime metrics)
- Access to sample catalogs for demo queries (optional)

---

## Built By

**Pratik Bhikadiya** — Databricks Community

## License

MIT License — free to use, modify, and share
