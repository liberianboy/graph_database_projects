# PySpark + Neo4j Graph Demo (macOS)

End-to-end demo that shows:

1. Ingesting CSV files from a mounted drive (`/Volumes/DATA/input`) with PySpark.
2. Loading the data into a Neo4j graph database running in Docker.
3. Running graph analytics on **two datasets**:
   - **Social network** (`users` + `friendships`) — PageRank, connected components, triangle count.
   - **Staff / customer network** (`staff` + `customers` + `connections`) — shortest path between staff members and customers they aren't directly in contact with, using both Cypher's `shortestPath()` and GraphFrames BFS.
4. Writing every result set back out as Parquet via `DataFrame.write.parquet(...)` to `/Volumes/DATA/output`.

## Prerequisites

- macOS (Apple Silicon or Intel)
- Docker Desktop running
- Python 3.11 (PySpark 3.5.x works cleanly on 3.10/3.11; Python 3.12+ can have issues)
- Java 17 (Temurin/OpenJDK). Spark 3.5 requires Java 8/11/17.
  ```bash
  brew install --cask temurin@17
  export JAVA_HOME=$(/usr/libexec/java_home -v 17)
  ```
- A mounted drive available at `/Volumes/DATA`. If you don't have an external drive attached, create it locally and symlink, or just update the paths at the top of the notebook:
  ```bash
  sudo mkdir -p /Volumes/DATA/input /Volumes/DATA/output
  sudo chown -R "$USER" /Volumes/DATA
  ```

## One-time setup

```bash
cd pyspark-graph-demo
./setup.sh                       # creates .venv, installs pyspark + jupyter + neo4j driver
docker compose up -d             # starts Neo4j on bolt://localhost:7687 (browser: http://localhost:7474)
python generate_sample_data.py   # writes users.csv + friendships.csv to /Volumes/DATA/input
```

Default Neo4j credentials: `neo4j` / `cowork-demo-pass` (set in `docker-compose.yml`).

## Run the notebook

```bash
source .venv/bin/activate
jupyter lab pyspark_graph_demo.ipynb
```

The notebook will:
- Read `users.csv`, `friendships.csv`, `staff.csv`, `customers.csv`, and `connections.csv` from `/Volumes/DATA/input` into Spark DataFrames.
- Push `(:User)-[:FRIENDS]->(:User)` and `(:Staff|:Customer)-[:CONNECTED]->(:Staff|:Customer)` into Neo4j via the Spark connector.
- Run Cypher queries (including `shortestPath()` for staff↔customer introduction paths) and build GraphFrames for in-Spark analytics (PageRank, connected components, triangle count, BFS).
- Write `pagerank.parquet`, `components.parquet`, `triangles.parquet`, `top_influencers.parquet`, and `shortest_paths.parquet` to `/Volumes/DATA/output`.

### About `shortest_paths.parquet`

Sample output (one row per staff/customer pair with no direct touchpoint, shortest chain up to 6 hops):

| staff_id | staff_name | department | customer_id | customer_name | segment | hops | path_nodes |
|---|---|---|---|---|---|---|---|
| 7 | Alex Patel | Sales | 23 | Rossi Corp | Enterprise | 2 | `[Staff:Alex Patel, Staff:Jordan Kim, Customer:Rossi Corp]` |
| 12 | Casey Tanaka | Engineering | 41 | Kowalski LLC | SMB | 3 | `[Staff:Casey Tanaka, Staff:Blair Dubois, Customer:Silva Group, Customer:Kowalski LLC]` |

Use this to answer "who can warm-introduce me to this customer?" through the combined org + referral graph.

## Shutdown

```bash
docker compose down              # stop Neo4j (data persisted in ./neo4j_data)
deactivate                       # exit the venv
```

## Layout

```
pyspark-graph-demo/
├── README.md
├── docker-compose.yml
├── setup.sh
├── requirements.txt
├── generate_sample_data.py
└── pyspark_graph_demo.ipynb
```
