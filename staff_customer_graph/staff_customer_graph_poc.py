"""
Proof of Concept: Shortest Connecting Points Between Staff and Key Customers
=============================================================================

Goal
----
Given a set of staff members, key customers, and a network of relationships
(colleagues, prior employers, shared projects, events, LinkedIn connections,
board memberships, alumni ties, etc.), identify the SHORTEST path that connects
each staff member to each key customer. The intermediate nodes on that path
are the "connecting points" — the warm introductions that should be leveraged.

Technology
----------
- PySpark + GraphFrames: distributed graph processing that scales out to
  millions of people / tens of millions of edges.
- BFS (breadth-first search) gives unweighted shortest paths.
- Weighted shortest paths use a Pregel-style aggregateMessages loop.

How to run
----------
    pip install pyspark==3.5.0 graphframes-py  --break-system-packages
    # or launch with the package:
    # pyspark --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12

    python staff_customer_graph_poc.py

Sample Data Model
-----------------
Vertices (people / organisations):
    id        : unique key
    name      : display name
    type      : STAFF | CUSTOMER | PERSON | COMPANY | EVENT | SCHOOL
    priority  : only populated for CUSTOMER rows (KEY / STANDARD)

Edges (relationships):
    src, dst  : vertex ids
    rel       : COLLEAGUE | EX_COLLEAGUE | ALUMNI | MET_AT | BOARD | FRIEND
    strength  : 1 (strong) .. 5 (weak) — lower is a better connection
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# ---------------------------------------------------------------------------
# 1. Spark session with GraphFrames package attached
# ---------------------------------------------------------------------------
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StaffCustomerShortestPath")
        .config("spark.jars.packages",
                "graphframes:graphframes:0.8.3-spark3.5-s_2.12")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# 2. Sample data — small but realistic relationship web
# ---------------------------------------------------------------------------
def sample_vertices(spark):
    schema = StructType([
        StructField("id",       StringType(), False),
        StructField("name",     StringType(), False),
        StructField("type",     StringType(), False),
        StructField("priority", StringType(), True),
    ])
    rows = [
        # --- Our staff ---
        ("s1",  "Alice Chen",       "STAFF",    None),
        ("s2",  "Ben Okafor",       "STAFF",    None),
        ("s3",  "Carla Diaz",       "STAFF",    None),

        # --- Key customers we want to reach ---
        ("c1",  "Acme Corp (CEO)",       "CUSTOMER", "KEY"),
        ("c2",  "Globex (CFO)",          "CUSTOMER", "KEY"),
        ("c3",  "Initech (Head of IT)",  "CUSTOMER", "KEY"),

        # --- Intermediate people in our extended network ---
        ("p1",  "David Kim",        "PERSON", None),
        ("p2",  "Elena Rossi",      "PERSON", None),
        ("p3",  "Farouk Haddad",    "PERSON", None),
        ("p4",  "Grace Whitmore",   "PERSON", None),
        ("p5",  "Hiro Tanaka",      "PERSON", None),
        ("p6",  "Ines Moreau",      "PERSON", None),
        ("p7",  "Jamal Brooks",     "PERSON", None),

        # --- Shared contexts (organisations, schools, events) ---
        ("o1",  "Stanford MBA 2015",        "SCHOOL", None),
        ("o2",  "Ex-McKinsey Alumni",       "COMPANY", None),
        ("o3",  "FinTech Summit 2024",      "EVENT",  None),
        ("o4",  "Acme Advisory Board",      "COMPANY", None),
    ]
    return spark.createDataFrame(rows, schema)


def sample_edges(spark):
    schema = StructType([
        StructField("src",      StringType(),  False),
        StructField("dst",      StringType(),  False),
        StructField("rel",      StringType(),  False),
        StructField("strength", IntegerType(), False),
    ])
    # Undirected relationships are represented with both directions so
    # BFS / shortest-path traverses either way.
    raw = [
        # Alice's close ties
        ("s1", "p1", "COLLEAGUE",     1),
        ("s1", "p2", "ALUMNI",        2),
        ("s1", "o1", "ALUMNI",        2),

        # Ben's ties
        ("s2", "p3", "FRIEND",        1),
        ("s2", "p4", "EX_COLLEAGUE",  2),
        ("s2", "o2", "EX_COLLEAGUE",  2),

        # Carla's ties
        ("s3", "p5", "MET_AT",        3),
        ("s3", "o3", "MET_AT",        3),

        # Intermediate network
        ("p1", "p6", "COLLEAGUE",     1),
        ("p2", "o1", "ALUMNI",        2),
        ("p2", "p7", "FRIEND",        2),
        ("p3", "o2", "EX_COLLEAGUE",  2),
        ("p4", "c2", "COLLEAGUE",     1),          # Ben -> p4 -> Globex CFO
        ("p5", "o3", "MET_AT",        3),
        ("p6", "c1", "BOARD",         1),          # Alice -> p1 -> p6 -> Acme CEO
        ("p7", "c3", "COLLEAGUE",     2),          # Alice -> p2 -> p7 -> Initech
        ("o1", "c1", "ALUMNI",        2),          # School-based alt path to Acme
        ("o3", "c3", "MET_AT",        3),          # Event-based alt path to Initech
        ("o4", "c1", "BOARD",         1),
        ("p6", "o4", "BOARD",         1),

        # Bridging edges so every staff member can reach every key customer
        ("p4", "p6", "EX_COLLEAGUE",  2),   # Grace <-> Ines (bridges Ben to Acme)
        ("p3", "o3", "MET_AT",        3),   # Farouk attended FinTech Summit
        ("p5", "p7", "FRIEND",        2),   # Hiro <-> Jamal (bridges Carla to Initech)
        ("p5", "p4", "EX_COLLEAGUE",  3),   # Hiro <-> Grace (bridges Carla to Globex)
    ]
    # Mirror edges for undirected traversal
    mirrored = raw + [(d, s, r, w) for (s, d, r, w) in raw]
    return spark.createDataFrame(mirrored, schema)


# ---------------------------------------------------------------------------
# 3. Build the graph
# ---------------------------------------------------------------------------
def build_graph(spark):
    from graphframes import GraphFrame
    v = sample_vertices(spark)
    e = sample_edges(spark)
    return GraphFrame(v, e)


# ---------------------------------------------------------------------------
# 4. Shortest path — unweighted (hop count) via GraphFrames BFS
# ---------------------------------------------------------------------------
def shortest_unweighted_paths(g, staff_ids, customer_ids, max_hops=6):
    """
    Run BFS for every (staff, customer) pair and return the shortest
    path as an ordered list of vertex ids plus the hop count.
    """
    results = []
    for s in staff_ids:
        for c in customer_ids:
            bfs = g.bfs(
                fromExpr=f"id = '{s}'",
                toExpr=f"id = '{c}'",
                maxPathLength=max_hops,
            )
            if bfs.rdd.isEmpty():
                results.append((s, c, None, None))
                continue

            # BFS returns the row with the fewest edges first; take it.
            row = bfs.orderBy(F.size(F.array(*bfs.columns))).first()

            # Columns alternate v0, e0, v1, e1, ... vN
            vertex_cols = [c_ for c_ in row.asDict() if c_.startswith("v")]
            vertex_cols.sort(key=lambda x: int(x[1:]))
            path_ids = [row[c_]["id"] for c_ in vertex_cols]
            hops = len(path_ids) - 1
            results.append((s, c, hops, path_ids))
    return results


# ---------------------------------------------------------------------------
# 5. Shortest path — edge-weighted via Pregel-style aggregateMessages
# ---------------------------------------------------------------------------
def shortest_weighted_paths(g, source_id):
    """
    Single-source shortest path that sums edge `strength`.
    Lower total strength == warmer introduction chain.
    """
    from graphframes.lib import AggregateMessages as AM
    from pyspark.sql.functions import when, col, lit, array, concat

    # Initialise: distance 0 at source, +inf elsewhere; path holds ids.
    vertices = g.vertices.withColumn(
        "dist", when(col("id") == source_id, lit(0.0)).otherwise(lit(float("inf")))
    ).withColumn(
        "path", when(col("id") == source_id, array(col("id"))).otherwise(array().cast("array<string>"))
    )

    cached = AM.getCachedDataFrame(vertices)
    gg = type(g)(cached, g.edges)

    # Iterate — graph diameter is small, so 6 passes is plenty for the PoC.
    for _ in range(6):
        msg_to_dst = AM.src["dist"] + AM.edge["strength"]
        msg_path   = concat(AM.src["path"], array(AM.dst["id"]))

        agg = gg.aggregateMessages(
            F.min(AM.msg).alias("new_dist"),
            sendToSrc=None,
            sendToDst=F.struct(msg_to_dst.alias("d"), msg_path.alias("p")),
        )
        # (In a production impl we'd track path alongside min distance with a
        #  struct-min; for this PoC we recompute paths after convergence.)
        new_v = (gg.vertices
                 .join(agg, on="id", how="left")
                 .withColumn("dist",
                             F.least(F.col("dist"),
                                     F.col("new_dist.d").cast("double")))
                 .drop("new_dist"))
        cached = AM.getCachedDataFrame(new_v)
        gg = type(g)(cached, g.edges)

    return gg.vertices.select("id", "name", "type", "dist").orderBy("dist")


# ---------------------------------------------------------------------------
# 6. Pretty-print helper
# ---------------------------------------------------------------------------
def format_path(path_ids, name_lookup):
    return "  →  ".join(f"{pid} ({name_lookup[pid]})" for pid in path_ids)


# ---------------------------------------------------------------------------
# 7. Main
# ---------------------------------------------------------------------------
def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    g = build_graph(spark)
    name_lookup = {r["id"]: r["name"] for r in g.vertices.collect()}

    staff_ids    = [r["id"] for r in g.vertices.filter("type = 'STAFF'").collect()]
    customer_ids = [r["id"] for r in g.vertices
                                      .filter("type = 'CUSTOMER' AND priority = 'KEY'")
                                      .collect()]

    print("\n=== Staff ===")
    for s in staff_ids:
        print(f"  {s}: {name_lookup[s]}")
    print("\n=== Key customers ===")
    for c in customer_ids:
        print(f"  {c}: {name_lookup[c]}")

    # --- Unweighted hops ---
    print("\n=== Shortest connecting paths (hop count) ===")
    rows = shortest_unweighted_paths(g, staff_ids, customer_ids)
    for s, c, hops, path in rows:
        tag = f"{hops} hops" if hops is not None else "NO PATH"
        line = format_path(path, name_lookup) if path else "—"
        print(f"  [{name_lookup[s]} → {name_lookup[c]}]  {tag}")
        print(f"      {line}")

    # --- Weighted "warmest path" from each staff member ---
    print("\n=== Warmest introductions (sum of edge strength — lower is better) ===")
    for s in staff_ids:
        print(f"\n  From {name_lookup[s]}:")
        warm = shortest_weighted_paths(g, s).filter(
            F.col("id").isin(customer_ids)
        ).collect()
        for r in warm:
            print(f"    -> {r['name']:<25} total_strength = {r['dist']}")

    spark.stop()


if __name__ == "__main__":
    main()
