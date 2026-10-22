# =============================================================================
# SparkExplainer — A Python library for understanding Spark execution
# =============================================================================
# This class provides four main capabilities:
#   1. explain_query()       — explain a Spark plan in plain English
#   2. suggest_optimizations() — suggest concrete performance improvements
#   3. parse_spark_ui()      — fetch real execution metrics from the session
#   4. educational_walkthrough() — step-by-step beginner explanation w/ analogies
#
# Works on Databricks Serverless (Spark Connect) with graceful fallbacks.
# Uses only PySpark APIs and the Python standard library — no externals.
# =============================================================================

import io
import contextlib
import re
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, rand, expr, row_number
from pyspark.sql.window import Window


class SparkExplainer:
    """
    A library that explains Spark execution plans in plain English,
    suggests optimizations, parses Spark UI metrics, and provides
    educational walkthroughs using real-world analogies.
    """

    # ── Analogy table used by educational_walkthrough() ──────────────────
    ANALOGIES = {
        "Scan": (
            "Reading data from storage into memory",
            "Like opening filing cabinets to get documents",
        ),
        "Filter": (
            "Removing rows that don\'t match a condition — no data movement",
            "Like sorting mail — keeping only letters addressed to you",
        ),
        "Exchange (Shuffle)": (
            "Redistributing data across executors by key — creates a new stage",
            "Like moving all items between warehouses so similar items are in the same building",
        ),
        "BroadcastExchange": (
            "Sending a copy of the small table to all executors",
            "Like giving every worker a copy of the phone directory",
        ),
        "BroadcastHashJoin": (
            "Small table broadcast to all executors, then joined locally — no shuffle of large table",
            "Like giving every worker a copy of the phone directory instead of asking them to meet at a desk",
        ),
        "SortMergeJoin": (
            "Both tables sorted then merged — requires shuffle of both sides",
            "Like two sorted decks of cards being merged into one sorted deck",
        ),
        "HashJoin": (
            "Joining using hash tables — may require shuffle",
            "Like matching left socks with right socks by color",
        ),
        "HashAggregate": (
            "Grouping data and computing aggregates (sum, count, avg, etc.)",
            "Like counting coins by denomination into separate piles",
        ),
        "Sort": (
            "Ordering rows by a key — may require range shuffle for global sort",
            "Like arranging books alphabetically on a shelf",
        ),
        "Window": (
            "Computing window functions over partitions of data",
            "Like ranking players within each team by score",
        ),
        "Limit": (
            "Limiting the number of output rows",
            "Like taking only the top 10 exam papers from a stack",
        ),
        "Project": (
            "Selecting specific columns — no data movement",
            "Like picking only the relevant pages from each document",
        ),
        "Coalesce": (
            "Reducing the number of partitions without shuffle",
            "Like combining several small boxes into fewer larger ones",
        ),
        "Repartition": (
            "Changing the number of partitions — requires shuffle",
            "Like redistributing all items into a different number of boxes",
        ),
        "Distinct": (
            "Removing duplicate rows — requires shuffle",
            "Like removing duplicate photos from an album",
        ),
        "Range": (
            "Generating a range of numbers",
            "Like counting 1, 2, 3, ... up to N",
        ),
    }

    # ── Constructor ─────────────────────────────────────────────────────
    def __init__(self, spark_session=None):
        """Initialize with an optional SparkSession (defaults to notebook's spark)."""
        self.spark = spark_session if spark_session is not None else spark
        self._sc = None
        try:
            self._sc = self.spark.sparkContext
        except Exception:
            pass  # Not available on Spark Connect (serverless)

    # ── Helper: get DataFrame from SQL or DataFrame input ────────────────
    def _get_df(self, sql_or_df):
        """Accept either a SQL string or a PySpark DataFrame."""
        if isinstance(sql_or_df, str):
            return self.spark.sql(sql_or_df)
        if isinstance(sql_or_df, DataFrame):
            return sql_or_df
        raise TypeError(f"Expected str or DataFrame, got {type(sql_or_df)}")

    # ── Helper: capture the physical plan as a string ───────────────────
    def _get_plan_text(self, df):
        """
        Capture the extended explain output as a string.
        Works on both classic Spark and Spark Connect (serverless).
        """
        # Method 1: stdout capture of df.explain(extended=True)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            df.explain(extended=True)
        plan = buf.getvalue()
        if plan.strip():
            # Extract only the Physical Plan section to avoid duplicate
            # nodes from Parsed/Analyzed/Optimized Logical Plan sections
            if "== Physical Plan ==" in plan:
                idx = plan.index("== Physical Plan ==")
                after = plan[idx + len("== Physical Plan =="):]
                next_idx = after.find("\n==")
                if next_idx >= 0:
                    return after[:next_idx].strip()
                return after.strip()
            return plan

        # Method 2: try simple explain (without extended)
        buf2 = io.StringIO()
        with contextlib.redirect_stdout(buf2):
            df.explain()
        plan2 = buf2.getvalue()
        if plan2.strip():
            return plan2

        # Method 3: try _jdf (classic Spark only, fails on Spark Connect)
        try:
            return str(df._jdf.queryExecution().executedPlan().treeString())
        except Exception:
            return ""

    # ── Helper: parse plan text into structured nodes ───────────────────
    def _parse_plan_nodes(self, plan_text):
        """
        Parse the physical plan text into a list of node dicts.
        Each node has: raw, type, is_shuffle, indent.
        """
        nodes = []
        for line in plan_text.strip().split("\n"):
            stripped = line.strip()
            if not stripped:
                continue
            # Skip header lines that aren't plan nodes
            if stripped.startswith("==") or stripped.startswith("Adaptive"):
                continue
            node_type = self._identify_node(stripped)
            if node_type:
                # Collapse consecutive duplicate node types (e.g. multiple Projects)
                if nodes and nodes[-1]["type"] == node_type:
                    nodes[-1]["count"] = nodes[-1].get("count", 1) + 1
                    continue
                nodes.append({
                    "raw": stripped,
                    "type": node_type,
                    "is_shuffle": "Exchange" in stripped or "exchange" in stripped.lower(),
                    "indent": len(line) - len(line.lstrip()),
                    "count": 1,
                })
        return nodes

    # ── Helper: identify the type of a plan node from a line ────────────
    def _identify_node(self, line):
        """Map a plan-line to a human-readable node type."""
        ll = line.lower()
        # Order matters: more specific patterns first
        if "broadcastexchange" in ll:
            return "BroadcastExchange"
        if "broadcasthashjoin" in ll or "broadcast hash join" in ll:
            return "BroadcastHashJoin"
        if "sortmergejoin" in ll or "sort merge join" in ll:
            return "SortMergeJoin"
        if "hashjoin" in ll or ("hash join" in ll and "broadcast" not in ll):
            return "HashJoin"
        if "exchange" in ll:
            return "Exchange (Shuffle)"
        if "hashaggregate" in ll or "hash aggregate" in ll:
            return "HashAggregate"
        if "sort" in ll and "join" not in ll:
            return "Sort"
        if "filter" in ll:
            return "Filter"
        if "scan" in ll:
            return "Scan"
        if "window" in ll:
            return "Window"
        if "globallimit" in ll or "collectlimit" in ll or "global limit" in ll:
            return "Limit"
        if "project" in ll:
            return "Project"
        if "coalesce" in ll:
            return "Coalesce"
        if "repartition" in ll:
            return "Repartition"
        if "distinct" in ll:
            return "Distinct"
        if "range" in ll:
            return "Range"
        if "appendcolumns" in ll:
            return "AppendColumns"
        if "localtablescan" in ll:
            return "Scan"
        return None

    # ── Helper: count shuffles and stages ───────────────────────────────
    def _count_shuffles(self, nodes):
        return sum(1 for n in nodes if n["is_shuffle"])

    def _count_stages(self, nodes):
        return self._count_shuffles(nodes) + 1

    # ── Helper: detect join types ──────────────────────────────────────
    def _detect_join_types(self, nodes):
        return [n["type"] for n in nodes if "Join" in n["type"]]

    # ── Helper: detect broadcast presence ─────────────────────────────
    def _detect_broadcast(self, nodes):
        return any("Broadcast" in n["type"] for n in nodes)

    # ── Helper: human-readable detail per node type ─────────────────────
    def _node_detail(self, node_type):
        """Return a short human-readable description for a node type."""
        details = {
            "Scan": "Reading data from storage into memory",
            "Filter": "Removing rows that don\'t match the condition — no data movement",
            "Project": "Selecting specific columns — no data movement",
            "Exchange (Shuffle)": "Redistributing data across executors by key — creates a new stage",
            "BroadcastExchange": "Sending a copy of the small table to all executors",
            "HashAggregate": "Grouping data and computing aggregates (sum, count, avg, etc.)",
            "SortMergeJoin": "Both tables sorted and merged — requires shuffle of both sides",
            "BroadcastHashJoin": "Small table broadcast to all executors, joined locally — no shuffle of large table",
            "HashJoin": "Joining using hash tables — may require shuffle",
            "Sort": "Ordering rows by a key — may require range shuffle for global sort",
            "Window": "Computing window functions over partitions of data",
            "Limit": "Limiting the number of output rows",
            "Coalesce": "Reducing the number of partitions without shuffle",
            "Repartition": "Changing the number of partitions — requires shuffle",
            "Range": "Generating a range of numbers",
            "Distinct": "Removing duplicate rows — requires shuffle",
        }
        return details.get(node_type, "")

    # ── Helper: format bytes as human-readable string ─────────────────
    def _format_bytes(self, bytes_val):
        """Convert a byte count into a human-readable string."""
        if bytes_val is None or bytes_val == 0:
            return "0 bytes"
        units = ["bytes", "KB", "MB", "GB", "TB"]
        val = float(bytes_val)
        for unit in units:
            if abs(val) < 1024:
                return f"{val:.1f} {unit}"
            val /= 1024
        return f"{val:.1f} PB"

    # ═══════════════════════════════════════════════════════════════════
    # CAPABILITY 1: explain_query()
    # ═══════════════════════════════════════════════════════════════════
    def explain_query(self, sql_or_df):
        """
        Parse the Spark physical plan and explain in plain English:
        - What Jobs, Stages, and Tasks will be created
        - Where shuffles happen and why
        - What each node in the plan does
        """
        df = self._get_df(sql_or_df)
        plan_text = self._get_plan_text(df)
        if not plan_text.strip():
            return "⚠️  Could not retrieve the execution plan."

        nodes = self._parse_plan_nodes(plan_text)
        num_shuffles = self._count_shuffles(nodes)
        num_stages = self._count_stages(nodes)
        join_types = self._detect_join_types(nodes)
        has_broadcast = self._detect_broadcast(nodes)

        lines = []
        lines.append("=" * 70)
        lines.append("SPARK EXECUTION PLAN EXPLANATION")
        lines.append("=" * 70)
        lines.append(f"\n📊  Overview")
        lines.append(f"   • Stages:   {num_stages}")
        lines.append(f"   • Shuffles: {num_shuffles}")
        if join_types:
            lines.append(f"   • Joins:    {', '.join(join_types)}")
        if has_broadcast:
            lines.append(f"   • Broadcast: Yes")

        lines.append(f"\n📋  Step-by-step plan breakdown:")
        for i, node in enumerate(nodes, 1):
            marker = "🔄 SHUFFLE" if node["is_shuffle"] else "  ✅"
            count_str = f" (×{node.get('count', 1)})" if node.get("count", 1) > 1 else ""
            lines.append(f"   {marker}  Step {i}: {node['type']}{count_str}")
            detail = self._node_detail(node["type"])
            if detail:
                lines.append(f"           → {detail}")

        lines.append(f"\n💡  Key takeaways:")
        if num_shuffles == 0:
            lines.append("   • No shuffles — everything runs in a single stage")
            lines.append("   • This is the most efficient execution pattern")
        else:
            lines.append(f"   • {num_shuffles} shuffle(s) required — data moves across executors")
            lines.append("   • Each shuffle creates a new stage boundary")
            if num_shuffles > 2:
                lines.append("   ⚠️  Multiple shuffles may impact performance")

        for jt in join_types:
            if "SortMerge" in jt:
                lines.append("   • SortMergeJoin requires both sides sorted — expensive")
            elif "Broadcast" in jt:
                lines.append("   • BroadcastJoin avoids shuffle of the large table — efficient!")

        lines.append("\n" + "=" * 70)
        return "\n".join(lines)

    # ═══════════════════════════════════════════════════════════════════
    # CAPABILITY 2: suggest_optimizations()
    # ═══════════════════════════════════════════════════════════════════
    def suggest_optimizations(self, sql_or_df):
        """
        Analyze a Spark plan and suggest concrete optimizations:
        broadcast join, repartition, cache, predicate pushdown, avoid skew,
        reduce shuffle bytes, etc. — with explanations of why each helps.
        """
        df = self._get_df(sql_or_df)
        plan_text = self._get_plan_text(df)
        if not plan_text.strip():
            return "⚠️  Could not retrieve the execution plan."

        nodes = self._parse_plan_nodes(plan_text)
        suggestions = []
        num_shuffles = self._count_shuffles(nodes)
        join_types = self._detect_join_types(nodes)
        has_broadcast = self._detect_broadcast(nodes)

        # — Suggestion 1: SortMergeJoin → BroadcastJoin ─────────────────
        for jt in join_types:
            if "SortMerge" in jt:
                suggestions.append({
                    "optimization": "Use BroadcastJoin instead of SortMergeJoin",
                    "why": "If one side is small enough (< ~10MB by default), broadcasting it avoids shuffling the large table.",
                    "code": "from pyspark.sql.functions import broadcast\nlarge_df.join(broadcast(small_df), 'key')",
                    "impact": "Eliminates 1 shuffle of the large table",
                })

        # — Suggestion 2: Multiple shuffles ─────────────────────────────
        if num_shuffles > 1:
            suggestions.append({
                "optimization": "Reduce the number of shuffles",
                "why": f"Your plan has {num_shuffles} shuffles. Each moves data across the network. Consider combining operations or pre-partitioning.",
                "code": "df.repartition('join_key')  # Pre-partition before multiple joins",
                "impact": f"Potential savings: {num_shuffles - 1} shuffle(s) eliminated",
            })

        # — Suggestion 3: Auto-broadcast threshold ──────────────────────
        if join_types and not has_broadcast and any("SortMerge" in jt for jt in join_types):
            suggestions.append({
                "optimization": "Enable broadcast join auto-threshold",
                "why": "Spark can auto-broadcast small tables. Increase the threshold if your small table is moderately sized.",
                "code": "spark.conf.set('spark.sql.autoBroadcastJoinThreshold', 50 * 1024 * 1024)  # 50MB",
                "impact": "May convert SortMergeJoin → BroadcastHashJoin automatically",
            })

        # — Suggestion 4: Predicate pushdown ─────────────────────────────
        filter_nodes = [n for n in nodes if n["type"] == "Filter"]
        sort_nodes = [n for n in nodes if n["type"] == "Sort"]
        if filter_nodes:
            # Check if filter appears AFTER other expensive operations
            first_filter_idx = next((i for i, n in enumerate(nodes) if n["type"] == "Filter"), len(nodes))
            first_scan_idx = next((i for i, n in enumerate(nodes) if n["type"] == "Scan"), 0)
            if first_filter_idx > first_scan_idx and first_filter_idx > 1:
                suggestions.append({
                    "optimization": "Ensure predicate pushdown is working",
                    "why": "Filters should be pushed down to the data source to reduce data read. Check if your filter is applied early.",
                    "code": "result = source_df.filter(col('date') > '2024-01-01').orderBy('amount')",
                    "impact": "Reduces I/O — less data read from storage",
                })

        # — Suggestion 5: Sort before aggregate ─────────────────────────
        agg_nodes = [n for n in nodes if "Aggregate" in n["type"]]
        if agg_nodes and sort_nodes:
            suggestions.append({
                "optimization": "Remove unnecessary sort before aggregation",
                "why": "Spark\'s HashAggregate handles unordered input. Sorting before groupBy is usually redundant.",
                "code": "df.groupBy('key').agg(sum('val'))  # No .orderBy() needed before",
                "impact": "Eliminates 1 sort operation",
            })

        # — Suggestion 6: Window function optimization ──────────────────
        window_nodes = [n for n in nodes if n["type"] == "Window"]
        if window_nodes and num_shuffles > 1:
            suggestions.append({
                "optimization": "Optimize window function partitioning",
                "why": "Window functions shuffle by partition key. Reuse the same partitionBy for multiple windows to share one shuffle.",
                "code": "w = Window.partitionBy('customer_id').orderBy('date')\ndf.withColumn('rnk', row_number().over(w))\n  .withColumn('lag', lag('amount').over(w))",
                "impact": "Shares 1 shuffle across multiple window functions",
            })

        # — Suggestion 7: Data skew warning ────────────────────────────
        if join_types or agg_nodes:
            suggestions.append({
                "optimization": "Watch for data skew",
                "why": "If some keys have far more rows, some tasks run much longer. Look for 'straggler' tasks in Spark UI.",
                "code": "df.withColumn('salt', (rand() * 10).cast('int'))\n  .groupBy('key', 'salt').agg(...)\n  .drop('salt')",
                "impact": "Balances work across executors",
            })

        # — Suggestion 8: Caching opportunity ──────────────────────────
        if num_shuffles >= 2 or len(nodes) > 8:
            suggestions.append({
                "optimization": "Cache intermediate results if reused",
                "why": "If this DataFrame is used multiple times downstream, caching avoids re-computing shuffles and scans.",
                "code": "df.cache()  # or df.persist(StorageLevel.MEMORY_AND_DISK)",
                "impact": "Eliminates redundant computation on reuse",
            })

        # ── Format output ─────────────────────────────────────────────
        lines = []
        lines.append("=" * 70)
        lines.append("OPTIMIZATION SUGGESTIONS")
        lines.append("=" * 70)

        if not suggestions:
            lines.append("\n✅  No major optimizations needed — this plan looks efficient!")
        else:
            lines.append(f"\nFound {len(suggestions)} potential optimization(s):\n")
            for i, s in enumerate(suggestions, 1):
                lines.append(f"{'─' * 60}")
                lines.append(f"  {i}. {s['optimization']}")
                lines.append(f"     Why:    {s['why']}")
                lines.append(f"     Impact: {s['impact']}")
                lines.append(f"     Code:   {s['code']}")
                lines.append("")

        lines.append("=" * 70)
        return "\n".join(lines)

    # ═══════════════════════════════════════════════════════════════════
    # CAPABILITY 3: parse_spark_ui()
    # ═══════════════════════════════════════════════════════════════════
    def parse_spark_ui(self):
        """
        Fetch real execution data from the Spark session.
        On classic clusters, uses sc.statusTracker().
        On serverless (Spark Connect), gracefully falls back with an explanation.
        """
        result = {"available": False, "jobs": [], "stages": [], "note": "", "summary": {}}

        # Try sc.statusTracker() — works on classic clusters, fails on Spark Connect
        try:
            sc = self.spark.sparkContext
            tracker = sc.statusTracker()

            # Get active job IDs
            active_job_ids = (
                tracker.getActiveJobsIds()
                if hasattr(tracker, "getActiveJobsIds")
                else (tracker.getActiveJobIds() if hasattr(tracker, "getActiveJobIds") else [])
            )

            for jid in active_job_ids:
                try:
                    job_info = tracker.getJobInfo(jid)
                    if job_info:
                        result["jobs"].append({
                            "job_id": jid,
                            "status": str(getattr(job_info, "status", "unknown")),
                            "stage_ids": list(getattr(job_info, "stageIds", [])),
                        })
                except Exception:
                    pass

            # Get stage info for each job
            for job_entry in result["jobs"]:
                for sid in job_entry.get("stage_ids", []):
                    try:
                        stage_info = tracker.getStageInfo(sid)
                        if stage_info:
                            result["stages"].append({
                                "stage_id": sid,
                                "name": getattr(stage_info, "name", "unknown"),
                                "num_tasks": getattr(stage_info, "numTasks", 0),
                                "completed_tasks": getattr(stage_info, "numCompletedTasks", 0),
                                "failed_tasks": getattr(stage_info, "numFailedTasks", 0),
                                "shuffle_bytes": getattr(stage_info, "shuffleBytes", 0),
                                "duration_ms": getattr(stage_info, "submissionTime", 0),
                            })
                    except Exception:
                        pass

            result["available"] = True
            result["note"] = "Data fetched from Spark UI via sc.statusTracker()"
        except Exception:
            # Fallback: query system.query.history for real runtime metrics.
            # This works on serverless (Spark Connect) where sc.statusTracker() fails.
            return self.get_runtime_metrics(limit=5)

        # Build summary
        if result["available"]:
            result["summary"] = {
                "total_jobs": len(result["jobs"]),
                "total_stages": len(result["stages"]),
                "total_tasks": sum(s.get("num_tasks", 0) for s in result["stages"]),
                "total_completed": sum(s.get("completed_tasks", 0) for s in result["stages"]),
                "total_shuffle_bytes": sum(s.get("shuffle_bytes", 0) for s in result["stages"]),
            }
        else:
            result["summary"] = {"note": "Spark UI not available on this compute type."}

        # ── Format output ─────────────────────────────────────────────
        lines = []
        lines.append("=" * 70)
        lines.append("SPARK UI — REAL EXECUTION METRICS")
        lines.append("=" * 70)

        if result["available"]:
            s = result["summary"]
            lines.append(f"\n📊  Session Summary:")
            lines.append(f"   • Jobs:           {s['total_jobs']}")
            lines.append(f"   • Stages:         {s['total_stages']}")
            lines.append(f"   • Tasks:          {s['total_tasks']}")
            lines.append(f"   • Completed:      {s['total_completed']}")
            lines.append(f"   • Shuffle bytes:  {s['total_shuffle_bytes']:,}")
            if result["stages"]:
                lines.append(f"\n📋  Stage Details:")
                for st in result["stages"]:
                    lines.append(f"   Stage {st['stage_id']}: {st['name']}")
                    lines.append(f"     Tasks: {st['completed_tasks']}/{st['num_tasks']} done")
                    if st.get("shuffle_bytes", 0) > 0:
                        lines.append(f"     Shuffle: {st['shuffle_bytes']:,} bytes")
        else:
            lines.append(f"\n⚠️  {result['note']}")
            if "alternative" in result:
                lines.append(f"\n💡  {result['alternative']}")

        lines.append("\n" + "=" * 70)
        result["formatted_output"] = "\n".join(lines)
        return result

    # ═══════════════════════════════════════════════════════════════════
    # CAPABILITY 3b: get_runtime_metrics() — real metrics from system tables
    # ═══════════════════════════════════════════════════════════════════
    def get_runtime_metrics(self, limit=10, match_text=None):
        """
        Fetch real execution metrics from system.query.history.
        Works on Databricks Serverless (Spark Connect) — no sc.statusTracker() needed.

        Args:
            limit:       Number of recent queries to fetch (default 10).
            match_text:  Optional SQL snippet to filter by (fuzzy LIKE match).

        Returns:
            Dict with 'available', 'metrics', 'summary', 'formatted_output'.
        """
        result = {"available": False, "metrics": [], "summary": {}, "note": ""}

        try:
            # Get current user to filter
            current_user = self.spark.sql(
                "SELECT current_user() AS user"
            ).collect()[0]["user"]

            match_filter = (
                f"AND statement_text LIKE '%{match_text}%'" if match_text else ""
            )

            sql = f"""
                SELECT
                    statement_id,
                    statement_text,
                    execution_status,
                    total_duration_ms,
                    compilation_duration_ms,
                    execution_duration_ms,
                    total_task_duration_ms,
                    produced_rows,
                    read_rows,
                    read_bytes,
                    shuffle_read_bytes,
                    spilled_local_bytes,
                    read_files,
                    pruned_files,
                    from_result_cache,
                    start_time
                FROM system.query.history
                WHERE executed_by = '{current_user}'
                    AND execution_status = 'FINISHED'
                    AND statement_text IS NOT NULL
                    {match_filter}
                ORDER BY start_time DESC
                LIMIT {limit}
            """

            rows = self.spark.sql(sql).collect()

            if not rows:
                result["note"] = "No recent completed queries found."
                result["formatted_output"] = (
                    "=" * 70 + "\nREAL RUNTIME METRICS (system.query.history)\n"
                    + "=" * 70
                    + "\n\nNo recent completed queries found.\n\n" + "=" * 70
                )
                return result

            for row in rows:
                stmt = row["statement_text"] or ""
                entry = {
                    "statement_id": row["statement_id"],
                    "statement_text": (
                        stmt[:80] + "..." if len(stmt) > 80 else stmt
                    ),
                    "total_duration_ms": row["total_duration_ms"] or 0,
                    "compilation_ms": row["compilation_duration_ms"] or 0,
                    "execution_ms": row["execution_duration_ms"] or 0,
                    "total_task_ms": row["total_task_duration_ms"] or 0,
                    "produced_rows": row["produced_rows"] or 0,
                    "read_rows": row["read_rows"] or 0,
                    "read_bytes": row["read_bytes"] or 0,
                    "shuffle_bytes": row["shuffle_read_bytes"] or 0,
                    "spill_bytes": row["spilled_local_bytes"] or 0,
                    "read_files": row["read_files"] or 0,
                    "pruned_files": row["pruned_files"] or 0,
                    "from_cache": (
                        row["from_result_cache"]
                        if row["from_result_cache"] is not None
                        else False
                    ),
                    "start_time": str(row["start_time"]) if row["start_time"] else "",
                }
                result["metrics"].append(entry)

            # Build summary
            n = len(result["metrics"])
            total_dur = sum(m["total_duration_ms"] for m in result["metrics"])
            total_read = sum(m["read_bytes"] for m in result["metrics"])
            total_shuf = sum(m["shuffle_bytes"] for m in result["metrics"])
            total_spill = sum(m["spill_bytes"] for m in result["metrics"])
            cache_hits = sum(1 for m in result["metrics"] if m["from_cache"])

            result["available"] = True
            result["summary"] = {
                "total_queries": n,
                "total_duration_ms": total_dur,
                "total_read_bytes": total_read,
                "total_shuffle_bytes": total_shuf,
                "total_spill_bytes": total_spill,
                "cache_hits": cache_hits,
            }

            # Format output
            lines = []
            lines.append("=" * 70)
            lines.append("REAL RUNTIME METRICS (from system.query.history)")
            lines.append("=" * 70)
            lines.append(f"\n[STATS]  Summary (last {n} completed queries):")
            lines.append(f"   • Total duration:    {total_dur:,} ms")
            lines.append(f"   • Total bytes read:  {self._format_bytes(total_read)}")
            lines.append(f"   • Shuffle bytes:     {self._format_bytes(total_shuf)}")
            lines.append(f"   • Spill to disk:     {self._format_bytes(total_spill)}")
            lines.append(f"   • Cache hits:        {cache_hits}/{n}")

            lines.append(f"\n[PLAN]  Per-Query Breakdown:")
            for i, m in enumerate(result["metrics"], 1):
                lines.append(f"\n   Query {i}:")
                sql_short = (
                    m["statement_text"].replace("\n", " ").strip()
                    if m["statement_text"]
                    else "N/A"
                )
                lines.append(f"     SQL:       {sql_short}")
                lines.append(
                    f"     Duration:  {m['total_duration_ms']:,} ms "
                    f"(compile: {m['compilation_ms']:,} ms | exec: {m['execution_ms']:,} ms)"
                )
                lines.append(f"     Task time: {m['total_task_ms']:,} ms")
                parallelism = (
                    round(m["total_task_ms"] / m["execution_ms"], 1)
                    if m["execution_ms"] > 0
                    else 0
                )
                lines.append(f"     Parallelism: {parallelism}x (task_time / exec_time)")
                lines.append(
                    f"     Rows:      read {m['read_rows']:,} | produced {m['produced_rows']:,}"
                )
                lines.append(f"     Bytes read: {self._format_bytes(m['read_bytes'])}")
                if m["shuffle_bytes"] > 0:
                    lines.append(f"     Shuffle:   {self._format_bytes(m['shuffle_bytes'])}")
                if m["spill_bytes"] > 0:
                    lines.append(f"     [WARN] Spill to disk: {self._format_bytes(m['spill_bytes'])}")
                lines.append(
                    f"     Files:     read {m['read_files']} | pruned {m['pruned_files']}"
                )
                if m["from_cache"]:
                    lines.append(f"     [OK] Result from cache")

            lines.append("\n" + "=" * 70)
            result["formatted_output"] = "\n".join(lines)
            return result

        except Exception as e:
            result["available"] = False
            result["note"] = f"Could not query system.query.history: {str(e)[:200]}"
            result["formatted_output"] = (
                "=" * 70
                + "\nREAL RUNTIME METRICS\n"
                + "=" * 70
                + f"\n\n[WARN]  {result['note']}\n\n"
                + "=" * 70
            )
            return result

    # ═══════════════════════════════════════════════════════════════════
    # CAPABILITY 4: educational_walkthrough()
    # ═══════════════════════════════════════════════════════════════════
    def educational_walkthrough(self, sql_or_df):
        """
        Provide a beginner-friendly, step-by-step explanation of any Spark
        operation using analogies:
          Job = project, Stage = assembly line station, Task = worker,
          Shuffle = warehouse move, Partition = pizza slice.
        """
        df = self._get_df(sql_or_df)
        plan_text = self._get_plan_text(df)
        if not plan_text.strip():
            return "⚠️  Could not retrieve the execution plan."

        nodes = self._parse_plan_nodes(plan_text)
        num_shuffles = self._count_shuffles(nodes)
        num_stages = self._count_stages(nodes)
        join_types = self._detect_join_types(nodes)

        lines = []
        lines.append("=" * 70)
        lines.append("🎓  EDUCATIONAL WALKTHROUGH — Spark for Beginners")
        lines.append("=" * 70)

        # ── The Big Picture ───────────────────────────────────────────
        lines.append(f"\n📚  THE BIG PICTURE")
        lines.append(f"   When you run a Spark operation, it creates:")
        lines.append(f"   • {num_stages} Stage(s) → like {num_stages} assembly line station(s)")
        lines.append(f"   • Each stage has multiple Tasks → like workers at each station")
        if num_shuffles > 0:
            lines.append(f"   • {num_shuffles} Shuffle(s) → like moving items between warehouses")
        else:
            lines.append(f"   • No shuffles → everything stays in place, very efficient!")

        # ── Concepts used in this query ────────────────────────────────
        lines.append(f"\n📖  CONCEPTS IN THIS QUERY")
        concepts_used = set()
        for node in nodes:
            if node["type"] == "Scan":
                concepts_used.add("Scan")
            if node["type"] == "Filter":
                concepts_used.add("Filter")
            if "Join" in node["type"]:
                concepts_used.add(node["type"])
            if "Exchange" in node["type"]:
                concepts_used.add("Exchange (Shuffle)")
            if "Aggregate" in node["type"]:
                concepts_used.add("HashAggregate")
            if node["type"] == "Sort":
                concepts_used.add("Sort")
            if node["type"] == "Window":
                concepts_used.add("Window")
            if "Broadcast" in node["type"]:
                concepts_used.add("BroadcastExchange")

        concept_order = [
            "Scan", "Filter", "BroadcastExchange", "SortMergeJoin",
            "BroadcastHashJoin", "HashJoin", "Exchange (Shuffle)",
            "HashAggregate", "Sort", "Window",
        ]
        for concept in concept_order:
            if concept in concepts_used and concept in self.ANALOGIES:
                desc, analogy = self.ANALOGIES[concept]
                lines.append(f"\n   🔹  {concept}")
                lines.append(f"       What:   {desc}")
                lines.append(f"       Analogy: {analogy}")

        # ── Step-by-step execution walk ───────────────────────────────
        lines.append(f"\n🚶  STEP-BY-STEP EXECUTION WALK")
        lines.append(f"   Imagine you\'re the Spark manager. Here\'s what you\'d do:\n")

        current_stage = 0
        step_in_stage = 0
        for i, node in enumerate(nodes, 1):
            if node["is_shuffle"]:
                current_stage += 1
                step_in_stage = 0
                lines.append(f"\n   📦  STAGE {current_stage} BEGINS")
                lines.append(f"   {'─' * 50}")
                lines.append(f"   First, we reorganize the data:")
                lines.append(f"   → {self._node_detail(node['type'])}")
                lines.append(f"   🔄  This is a SHUFFLE — like moving items between warehouses")
                lines.append(f"       so similar items end up in the same building.")
                lines.append(f"       Multiple workers (tasks) do this in parallel.")
            else:
                if i == 1:
                    lines.append(f"\n   📦  STAGE 0 (Beginning)")
                    lines.append(f"   {'─' * 50}")
                step_in_stage += 1
                count_str = f" (×{node.get('count', 1)})" if node.get("count", 1) > 1 else ""
                lines.append(f"   → Step {step_in_stage}: {node['type']}{count_str}")
                detail = self._node_detail(node["type"])
                if detail:
                    lines.append(f"      {detail}")

        # ── Summary ──────────────────────────────────────────────────
        lines.append(f"\n🎯  SUMMARY")
        lines.append(f"   • This query runs in {num_stages} stage(s)")
        lines.append(f"   • Each stage processes data in parallel across many tasks")
        if num_shuffles > 0:
            lines.append(f"   • {num_shuffles} shuffle(s) move data between stages")
            lines.append(f"   • Shuffles are the most expensive part — they involve network I/O")
        else:
            lines.append(f"   • No shuffles = fastest execution (data stays in place)")

        for jt in join_types:
            if "Broadcast" in jt:
                lines.append(f"   • Broadcast Join: small table copied to all workers (smart!)")
            elif "SortMerge" in jt:
                lines.append(f"   • SortMerge Join: both tables sorted then merged (for large tables)")

        lines.append(f"\n💡  TIP: The fewer shuffles, the faster your query runs.")
        lines.append(f"       Look for ways to reduce data movement!")

        lines.append("\n" + "=" * 70)
        return "\n".join(lines)