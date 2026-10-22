# =============================================================================
# DEMO SECTION — Exercise all four capabilities of SparkExplainer
# =============================================================================
#   a. Create sample data (customers + orders, ~10K rows each)
#   b. Run a filter, join, groupBy, and window function
#   c. Call explain_query() on each
#   d. Call suggest_optimizations() on each
#   e. Call parse_spark_ui() to get real metrics
#   f. Call educational_walkthrough() on a join operation
# =============================================================================

from pyspark.sql.functions import (
    col, desc, rand, expr, row_number, sum as _sum, count, avg
)
from pyspark.sql.window import Window

# ── Instantiate the explainer ─────────────────────────────────────────
explainer = SparkExplainer()

# ── (a) Create sample data ────────────────────────────────────────────
print("━" * 70)
print("  CREATING SAMPLE DATA (10K customers, 10K orders)")
print("━" * 70)

customers = (
    spark.range(1, 10001)
    .withColumnRenamed("id", "customer_id")
    .withColumn("name", expr("concat('Customer_', cast(customer_id as string))"))
    .withColumn("region", expr("element_at(array('North','South','East','West'), cast(floor(rand()*4)+1 as int))"))
    .withColumn("tier", expr("element_at(array('Gold','Silver','Bronze'), cast(floor(rand()*3)+1 as int))"))
    .withColumn("signup_date", expr("date_add(date('2024-01-01'), cast(rand() * 365 as int))"))
)

orders = (
    spark.range(1, 10001)
    .withColumnRenamed("id", "order_id")
    .withColumn("customer_id", expr("cast(rand() * 9999 as long) + 1"))
    .withColumn("order_date", expr("date_add(date('2024-01-01'), cast(rand() * 365 as int))"))
    .withColumn("amount", expr("round(rand() * 1000, 2)"))
    .withColumn("category", expr("element_at(array('Electronics','Clothing','Food','Books'), cast(floor(rand()*4)+1 as int))"))
    .withColumn("status", expr("element_at(array('Completed','Pending','Cancelled'), cast(floor(rand()*3)+1 as int))"))
)

# Materialize by counting (cache not supported on serverless Spark Connect)
n_customers = customers.count()
n_orders = orders.count()
print(f"  Customers: {n_customers:,} rows")
print(f"  Orders:    {n_orders:,} rows")
print()

# ── (b) Define the four operations to analyze ─────────────────────────
# 1. FILTER — single stage, no shuffle
filter_result = orders.filter((col("amount") > 500) & (col("status") == "Completed"))

# 2. JOIN — two stages with shuffle
join_result = orders.join(customers, "customer_id", "inner").select(
    "order_id", "name", "region", "amount", "category"
)

# 3. GROUP BY (aggregate) — two stages with shuffle
agg_result = orders.groupBy("category").agg(
    _sum("amount").alias("total_revenue"),
    count("order_id").alias("order_count"),
    avg("amount").alias("avg_order_value"),
)

# 4. WINDOW FUNCTION — multiple stages with shuffle
w = Window.partitionBy("customer_id").orderBy(desc("amount"))
window_result = orders.withColumn("rnk", row_number().over(w)).filter(col("rnk") <= 3)

operations = [
    ("FILTER  — Completed orders > $500", filter_result),
    ("JOIN    — Orders ↔ Customers", join_result),
    ("AGGREGATE — Revenue by category", agg_result),
    ("WINDOW  — Top 3 orders per customer", window_result),
]

# ── (c) & (d) Call explain_query() and suggest_optimizations() ────────
for label, df_op in operations:
    print("\n")
    print("#" * 70)
    print(f"#  ANALYZING: {label}")
    print("#" * 70)
    print()
    print(explainer.explain_query(df_op))
    print()
    print(explainer.suggest_optimizations(df_op))

# ── (e) Call parse_spark_ui() ─────────────────────────────────────────
print("\n")
print("#" * 70)
print("#  PARSE SPARK UI — Real Execution Metrics")
print("#" * 70)
print()
ui_result = explainer.parse_spark_ui()
print(ui_result["formatted_output"])

# ── (f) Call educational_walkthrough() on the join ────────────────────
print("\n")
print("#" * 70)
print("#  EDUCATIONAL WALKTHROUGH — Join Operation")
print("#" * 70)
print()
print(explainer.educational_walkthrough(join_result))

print("\n\n✅  Demo complete — all four SparkExplainer capabilities exercised.")

# ======================================================================
# Sample Catalog Queries (No-Eemoji Mode)
# ======================================================================

# =============================================================================
# No-Emoji Mode + Sample Catalog Query 1: TPCH Orders Aggregation
# =============================================================================
# The strip_emojis() helper removes all emoji characters from the output
# and replaces them with plain ASCII markers like [STATS], [PLAN], etc.
# This makes the output suitable for terminal logging, copy-paste, or
# environments where emoji rendering is not available.
# =============================================================================

import re

def strip_emojis(text):
    """Remove emoji and box-drawing characters, replace with ASCII markers."""
    replacements = [
        ("\U0001F4CA", "[STATS]"),       # chart
        ("\U0001F4CB", "[PLAN]"),        # clipboard
        ("\U0001F4A1", "[TIP]"),         # light bulb
        ("\U0001F504", "[SHUFFLE]"),     # shuffle arrows
        ("\u2705", "[OK]"),             # check mark
        ("\u26A0\uFE0F", "[WARN]"),     # warning
        ("\U0001F393", "[LEARN]"),       # graduation cap
        ("\U0001F4DA", "[OVERVIEW]"),    # books
        ("\U0001F4D6", "[CONCEPTS]"),    # open book
        ("\U0001F6B6", "[WALK]"),        # walking person
        ("\U0001F4E6", "[STAGE]"),       # package
        ("\U0001F3AF", "[SUMMARY]"),     # target
        ("\U0001F539", "  *"),           # small diamond
        ("\u2501", "="),                # heavy horizontal
        ("\u2500", "-"),                # light horizontal
    ]
    for emoji, replacement in replacements:
        text = text.replace(emoji, replacement)
    # Strip any remaining emoji/special unicode characters
    text = re.sub(r'[\U0001F000-\U0001FFFF\u2600-\u27BF\uFE0F]', '', text)
    return text

def plain_explain(explainer, sql_or_df, label=""):
    """Run explain_query + suggest_optimizations + educational_walkthrough without emojis."""
    if label:
        print("=" * 70)
        print(f"  {label}")
        print("=" * 70)
        print()
    print(strip_emojis(explainer.explain_query(sql_or_df)))
    print()
    print(strip_emojis(explainer.suggest_optimizations(sql_or_df)))
    print()
    print(strip_emojis(explainer.educational_walkthrough(sql_or_df)))

# ── Query 1: TPCH Orders — Aggregation with Sort ────────────────────
# This query groups orders by priority, counts them, and sorts by count.
# Expect: HashAggregate + Exchange (shuffle) + Sort

df_q1 = spark.sql("""
    SELECT o_orderpriority, count(*) AS order_count, avg(o_totalprice) AS avg_price
    FROM samples.tpch.orders
    GROUP BY o_orderpriority
    ORDER BY order_count DESC
""")

plain_explain(explainer, df_q1, "Query 1: TPCH Orders — Group By Priority + Sort")

# ======================================================================

# =============================================================================
# Sample Catalog Query 2: TPCH Orders + Customer Join with Filter
# =============================================================================
# This query joins the orders table with the customer table on custkey,
# filters for high-value orders, and sorts by price.
# Expect: SortMergeJoin or BroadcastHashJoin + Exchange (shuffle) + Sort + Filter
# =============================================================================

df_q2 = spark.sql("""
    SELECT c.c_name, c.c_mktsegment, 
           o.o_orderkey, o.o_totalprice, o.o_orderdate
    FROM samples.tpch.orders o
    JOIN samples.tpch.customer c 
      ON o.o_custkey = c.c_custkey
    WHERE o.o_totalprice > 100000
    ORDER BY o.o_totalprice DESC
    LIMIT 100
""")

plain_explain(explainer, df_q2, "Query 2: TPCH Orders + Customer Join (high-value orders)")

# ======================================================================

# =============================================================================
# Sample Catalog Query 3: NYC Taxi Trips — Filter + Sort + Limit
# =============================================================================
# This query filters NYC taxi trips for long-distance rides, sorts by
# fare amount, and limits to top 1000 most expensive trips.
# Expect: Filter + Sort + Exchange (shuffle for range partitioning) + Limit
# =============================================================================

df_q3 = spark.sql("""
    SELECT pickup_zip, dropoff_zip, trip_distance, fare_amount, tpep_pickup_datetime
    FROM samples.nyctaxi.trips
    WHERE trip_distance > 10
    ORDER BY fare_amount DESC
    LIMIT 1000
""")

plain_explain(explainer, df_q3, "Query 3: NYC Taxi Trips — Long-distance top 1000 by fare")

print("\n\n" + "=" * 70)
print("  All 3 sample catalog queries analyzed with no-emoji output.")
print("=" * 70)

# ======================================================================

# =============================================================================
# Real Runtime Metrics via system.query.history
# =============================================================================
# This cell demonstrates the new get_runtime_metrics() method that fetches
# REAL execution data from Databricks system tables — not just plan analysis.
#
# Unlike explain_query() which shows what WILL happen (plan-level),
# get_runtime_metrics() shows what ACTUALLY happened (runtime-level):
#   - Actual duration (compile vs execution time)
#   - Bytes read from storage
#   - Shuffle bytes sent over network
#   - Spill to disk (if any)
#   - Rows read vs rows produced
#   - Parallelism factor (task_time / exec_time)
#   - Files read vs files pruned by optimizer
#   - Cache hits
#
# This works on serverless (Spark Connect) — no sc.statusTracker() needed.
# =============================================================================

print("=" * 70)
print("  Step 1: Running real queries to generate execution metrics")
print("=" * 70)

# Run a few queries so we have fresh metrics in system.query.history
spark.sql("""
    SELECT o_orderpriority, count(*) AS cnt, avg(o_totalprice) AS avg_price
    FROM samples.tpch.orders
    GROUP BY o_orderpriority
    ORDER BY cnt DESC
""").collect()

spark.sql("""
    SELECT c.c_name, o.o_totalprice
    FROM samples.tpch.orders o
    JOIN samples.tpch.customer c ON o.o_custkey = c.c_custkey
    WHERE o.o_totalprice > 100000
    ORDER BY o.o_totalprice DESC
    LIMIT 50
""").collect()

spark.sql("""
    SELECT pickup_zip, avg(fare_amount) AS avg_fare, count(*) AS trips
    FROM samples.nyctaxi.trips
    WHERE trip_distance > 10
    GROUP BY pickup_zip
    ORDER BY avg_fare DESC
    LIMIT 20
""").collect()

print("  3 queries executed.\n")

# ── Fetch real runtime metrics ─────────────────────────────────────
print("=" * 70)
print("  Step 2: Real runtime metrics from system.query.history")
print("=" * 70)
print()

rt = explainer.get_runtime_metrics(limit=5)
print(rt["formatted_output"])

# ── Enhanced parse_spark_ui() now falls back to system tables ────────
print("\n\n")
print("=" * 70)
print("  Step 3: parse_spark_ui() with system.query.history fallback")
print("=" * 70)
print()
print("  On serverless, sc.statusTracker() fails. Now it falls back to")
print("  system.query.history automatically:\n")

ui = explainer.parse_spark_ui()
print(strip_emojis(ui["formatted_output"]))

# ── Compare: Plan analysis vs Real metrics ─────────────────────────
print("\n\n")
print("=" * 70)
print("  Step 4: Plan analysis (predicted) vs Real metrics (actual)")
print("=" * 70)
print()

# Show plan analysis for the same query
print("PLAN ANALYSIS (what Spark predicted):")
print("-" * 40)
df_demo = spark.sql("""
    SELECT o_orderpriority, count(*) AS cnt, avg(o_totalprice) AS avg_price
    FROM samples.tpch.orders
    GROUP BY o_orderpriority
    ORDER BY cnt DESC
""")
print(strip_emojis(explainer.explain_query(df_demo)))

print("\n")
print("REAL METRICS (what actually happened):")
print("-" * 40)
rt_match = explainer.get_runtime_metrics(limit=1, match_text="o_orderpriority")
if rt_match["available"] and rt_match["metrics"]:
    m = rt_match["metrics"][0]
    print(f"  Duration:        {m['total_duration_ms']:,} ms")
    print(f"  Compile time:    {m['compilation_ms']:,} ms")
    print(f"  Execution time:  {m['execution_ms']:,} ms")
    print(f"  Task time:       {m['total_task_ms']:,} ms")
    parallelism = round(m["total_task_ms"] / m["execution_ms"], 1) if m["execution_ms"] > 0 else 0
    print(f"  Parallelism:     {parallelism}x")
    print(f"  Bytes read:      {explainer._format_bytes(m['read_bytes'])}")
    print(f"  Shuffle bytes:   {explainer._format_bytes(m['shuffle_bytes'])}")
    print(f"  Spill to disk:   {explainer._format_bytes(m['spill_bytes'])}")
    print(f"  Rows read:       {m['read_rows']:,}")
    print(f"  Rows produced:   {m['produced_rows']:,}")
    print(f"  Files read:      {m['read_files']}")
    print(f"  Files pruned:    {m['pruned_files']}")
else:
    print("  (No matching query found in recent history)")

print("\n\n" + "=" * 70)
print("  Demo complete — real runtime metrics from system tables.")
print("=" * 70)

# ======================================================================

