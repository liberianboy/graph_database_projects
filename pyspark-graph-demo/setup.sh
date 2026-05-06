#!/usr/bin/env bash
# One-time setup for the PySpark + Neo4j graph demo.
# Creates a Python venv, installs dependencies, and prints next steps.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# --- Python -------------------------------------------------------------------
PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "ERROR: $PYTHON_BIN not found. Install Python 3.11 (brew install python@3.11)." >&2
  exit 1
fi

PY_VERSION=$("$PYTHON_BIN" -c 'import sys; print("%d.%d" % sys.version_info[:2])')
echo "Using Python $PY_VERSION ($PYTHON_BIN)"

# --- Java ---------------------------------------------------------------------
if ! /usr/libexec/java_home -v 17 >/dev/null 2>&1; then
  echo "WARNING: Java 17 not detected. Install with: brew install --cask temurin@17" >&2
  echo "         Then: export JAVA_HOME=\$(/usr/libexec/java_home -v 17)" >&2
else
  echo "Java 17 detected at: $(/usr/libexec/java_home -v 17)"
fi

# --- venv ---------------------------------------------------------------------
if [ ! -d .venv ]; then
  "$PYTHON_BIN" -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install --upgrade pip wheel
python -m pip install -r requirements.txt

# Register the venv as a Jupyter kernel so the notebook can find it.
python -m ipykernel install --user --name pyspark-graph-demo --display-name "Python (pyspark-graph-demo)"

# --- Mount directories --------------------------------------------------------
if [ ! -d /Volumes/DATA ]; then
  echo
  echo "NOTE: /Volumes/DATA does not exist. Either attach an external drive named DATA,"
  echo "      or create a local folder and adjust paths at the top of the notebook:"
  echo "        sudo mkdir -p /Volumes/DATA/input /Volumes/DATA/output"
  echo "        sudo chown -R \"\$USER\" /Volumes/DATA"
fi

cat <<'EOF'

Setup complete.

Next steps:
  1. Start Neo4j:                 docker compose up -d
  2. Generate sample CSVs:        python generate_sample_data.py
  3. Launch the notebook:         jupyter lab pyspark_graph_demo.ipynb

Neo4j browser:  http://localhost:7474   (user: neo4j / pass: cowork-demo-pass)
EOF
