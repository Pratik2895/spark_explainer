# SparkExplainer — Making Spark Execution Understandable

A Python library built on Databricks that bridges the gap between **Spark UI complexity** and **developer understanding**.

## What It Does

| Capability | Method | What It Tells You |
|---|---|---|
| **Plan Analysis** | `explain_query()` | What Spark *will* do — stages, shuffles, joins |
| **Optimization Tips** | `suggest_optimizations()` | How to make it faster — broadcast, cache, repartition |
| **Real Runtime Metrics** | `get_runtime_metrics()` | What *actually* happened — duration, bytes, spill, shuffle |
| **Spark UI Parsing** | `parse_spark_ui()` | Real metrics from `sc.statusTracker()` or system tables |
| **Beginner Walkthrough** | `educational_walkthrough()` | Analogies: shuffle = "moving items between warehouses" |

## Quick Start

```python
from spark_explainer import SparkExplainer

explainer = SparkExplainer()

# Pass any DataFrame or SQL string
df = spark.sql("SELECT * FROM my_table WHERE amount > 100")

# What will Spark do?
print(explainer.explain_query(df))

# How to make it faster?
print(explainer.suggest_optimizations(df))

# What actually happened (from system.query.history)?
print(explainer.get_runtime_metrics(limit=5))

# Beginner-friendly explanation
print(explainer.educational_walkthrough(df))
```

## Key Features

- **Plan vs Reality** — Compare what Spark predicted (2 stages, 1 shuffle) with what actually happened (711 ms, 48 rows read, 0 spill)
- **Serverless Ready** — Uses `system.query.history` for runtime metrics, no classic cluster needed
- **No External Dependencies** — Pure PySpark + Python stdlib
- **Tested on 3 Sample Catalogs** — TPCH orders, NYC taxi trips, bakehouse franchises

## How It Works

### Plan Analysis (`explain_query`)
Parses the physical plan and translates it into plain English:
- Number of stages and shuffles
- Step-by-step breakdown of each plan node
- Join type detection (HashJoin, SortMergeJoin, BroadcastHashJoin)

### Real Runtime Metrics (`get_runtime_metrics`)
Queries `system.query.history` for actual execution data:
- Duration (compile vs execution time)
- Bytes read from storage
- Shuffle bytes sent over network
- Spill to disk
- Rows read vs produced
- Files read vs pruned
- Cache hits

### Spark UI Parsing (`parse_spark_ui`)
Two-tier fallback:
1. Try `sc.statusTracker()` (classic cluster) — live job/stage/task metrics
2. If serverless, fall back to `system.query.history` automatically

### Optimization Suggestions (`suggest_optimizations`)
Analyzes the plan and returns concrete tips:
- Use BroadcastJoin instead of SortMergeJoin
- Reduce the number of shuffles
- Enable broadcast join auto-threshold
- Ensure predicate pushdown
- Remove unnecessary sort before aggregation
- Optimize window function partitioning
- Watch for data skew
- Cache intermediate results

### Educational Walkthrough (`educational_walkthrough`)
Beginner-friendly analogies:
- **Shuffle** = Like moving all items between warehouses so similar items end up in the same building
- **Filter** = Like sorting mail — keeping only letters addressed to you
- **HashJoin** = Like matching left socks with right socks by color
- **Sort** = Like arranging books alphabetically on a shelf
- **BroadcastJoin** = Like giving every worker a copy of the phone directory

## Files

| File | Description |
|---|---|
| `spark_explainer.py` | The full SparkExplainer class |
| `demo.py` | Demo script with sample catalog queries (TPCH, NYC Taxi, Bakehouse) |

## Requirements

- Databricks workspace with Serverless compute (Spark Connect)
- Access to `system.query.history` (for runtime metrics)
- Access to sample catalogs (`samples.tpch`, `samples.nyctaxi.trips`) for demo queries

## Built By

Pratik Bhikadiya — Databricks Community

## License

MIT License — free to use, modify, and share
