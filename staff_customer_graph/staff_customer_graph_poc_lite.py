"""
Lite companion to staff_customer_graph_poc.py
=============================================
Runs the same BFS / Dijkstra logic over the same sample data using PySpark
DataFrames only (no GraphFrames JAR required). Useful for smoke-testing the
PoC in an environment where the graphframes package isn't wired up yet.

Algorithm
---------
- We iteratively JOIN a "frontier" DataFrame of (node, path, total_strength)
  against the edge DataFrame, one hop per iteration, keeping the minimum
  total_strength seen at each node.
- This is a DataFrame expression of Dijkstra on a small DAG; for large graphs
  switch back to GraphFrames / Pregel (see staff_customer_graph_poc.py).

Run
---
    pip install pyspark==3.5.0 --break-system-packages
    python staff_customer_graph_poc_lite.py
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType


def build_spark():
    return (SparkSession.builder
            .appName("StaffCustomerLite")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate())


def sample_vertices(spark):
    data = [
        ("s1", "Alice Chen",       "STAFF",    None),
        ("s2", "Ben Okafor",       "STAFF",    None),
        ("s3", "Carla Diaz",       "STAFF",    None),
        ("c1", "Acme Corp (CEO)",      "CUSTOMER", "KEY"),
        ("c2", "Globex (CFO)",         "CUSTOMER", "KEY"),
        ("c3", "Initech (Head of IT)", "CUSTOMER", "KEY"),
        ("p1", "David Kim",        "PERSON", None),
        ("p2", "Elena Rossi",      "PERSON", None),
        ("p3", "Farouk Haddad",    "PERSON", None),
        ("p4", "Grace Whitmore",   "PERSON", None),
        ("p5", "Hiro Tanaka",      "PERSON", None),
        ("p6", "Ines Moreau",      "PERSON", None),
        ("p7", "Jamal Brooks",     "PERSON", None),
        ("o1", "Stanford MBA 2015",   "SCHOOL",  None),
        ("o2", "Ex-McKinsey Alumni",  "COMPANY", None),
        ("o3", "FinTech Summit 2024", "EVENT",   None),
        ("o4", "Acme Advisory Board", "COMPANY", None),
    ]
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("type", StringType(), False),
        StructField("priority", StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


def sample_edges(spark):
    raw = [
        ("s1","p1",1),("s1","p2",2),("s1","o1",2),
        ("s2","p3",1),("s2","p4",2),("s2","o2",2),
        ("s3","p5",3),("s3","o3",3),
        ("p1","p6",1),("p2","o1",2),("p2","p7",2),("p3","o2",2),
        ("p4","c2",1),("p5","o3",3),("p6","c1",1),("p7","c3",2),
        ("o1","c1",2),("o3","c3",3),("o4","c1",1),("p6","o4",1),
        # Bridges so every staff member can reach every key customer
        ("p4","p6",2),("p3","o3",3),("p5","p7",2),("p5","p4",3),
    ]
    mirrored = raw + [(d, s, w) for (s, d, w) in raw]
    schema = StructType([
        StructField("src", StringType(), False),
        StructField("dst", StringType(), False),
        StructField("strength", IntegerType(), False),
    ])
    return spark.createDataFrame(mirrored, schema)


def shortest_paths_from(spark, vertices, edges, source_id, max_hops=6):
    """
    Iterative DataFrame Dijkstra.
    Returns a DataFrame: id, name, total_strength, path (list of ids).
    """
    # Frontier starts at the source
    frontier = spark.createDataFrame(
        [(source_id, [source_id], 0.0)],
        StructType([
            StructField("id", StringType(), False),
            StructField("path", ArrayType(StringType()), False),
            StructField("total_strength", DoubleType(), False),
        ]),
    )
    # "Best-known" distance/path per node
    best = frontier

    for _ in range(max_hops):
        # Expand: join frontier with edges on id == src
        expanded = (frontier
                    .join(edges, frontier.id == edges.src)
                    .select(
                        edges.dst.alias("id"),
                        F.concat(frontier.path, F.array(edges.dst)).alias("path"),
                        (frontier.total_strength + edges.strength.cast("double"))
                            .alias("total_strength"),
                    )
                    # Avoid cycles: don't re-enter a node already on the path
                    .filter(~F.array_contains(F.col("path"), F.col("id"))
                            | F.lit(False)))   # kept explicit for clarity

        # Candidate set = previous best + expansion
        candidates = best.unionByName(expanded)

        # Keep the minimum-strength path per node
        w = F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"]).Window
            .partitionBy("id").orderBy("total_strength")
        )
        new_best = (candidates
                    .withColumn("rn", w)
                    .filter("rn = 1")
                    .drop("rn"))

        # Converged if nothing changed
        if new_best.count() == best.count():
            best = new_best
            break
        best = new_best
        frontier = new_best

    return (best.join(vertices, "id")
                .select("id", "name", "type", "total_strength", "path"))


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    vertices = sample_vertices(spark)
    edges    = sample_edges(spark)
    names    = {r["id"]: r["name"] for r in vertices.collect()}

    staff     = [r["id"] for r in vertices.filter("type='STAFF'").collect()]
    customers = [r["id"] for r in vertices
                                    .filter("type='CUSTOMER' AND priority='KEY'")
                                    .collect()]

    print("\n=== Shortest warm introductions ===")
    for s in staff:
        sp = shortest_paths_from(spark, vertices, edges, s)
        rows = sp.filter(F.col("id").isin(customers)).orderBy("total_strength").collect()
        print(f"\nFrom {names[s]} ({s}):")
        for r in rows:
            chain = "  →  ".join(f"{pid} ({names[pid]})" for pid in r["path"])
            print(f"  to {r['name']:<25} strength={r['total_strength']}  hops={len(r['path'])-1}")
            print(f"      {chain}")

    spark.stop()


if __name__ == "__main__":
    main()
