"""Generate small synthetic datasets for the graph demo.

Writes five CSVs to /Volumes/DATA/input:
  - users.csv         (id, name, age, city)                        -- social network
  - friendships.csv   (src_id, dst_id, since, weight)              -- social network

  - staff.csv         (staff_id, name, role, department, city)     -- company org
  - customers.csv     (customer_id, name, segment, industry, city) -- customer base
  - connections.csv   (src_id, src_type, dst_id, dst_type,
                       rel_type, strength)                         -- cross-network links

The connections file captures three sub-networks in one table:
  * Staff <-> Staff    (internal org network: REPORTS_TO, COLLABORATES_WITH)
  * Customer <-> Customer (referral network: REFERRED)
  * Staff <-> Customer (direct touchpoints: ACCOUNT_MANAGER, MET, EMAILED)

Override the output directory with: SAMPLE_INPUT_DIR=/tmp/input python generate_sample_data.py
"""
from __future__ import annotations

import csv
import os
import random
from pathlib import Path

INPUT_DIR = Path(os.environ.get("SAMPLE_INPUT_DIR", "/Volumes/DATA/input"))
NUM_USERS = 200
AVG_FRIENDS = 6
RNG_SEED = 42

FIRST_NAMES = [
    "Alex", "Jordan", "Sam", "Taylor", "Morgan", "Casey", "Riley", "Quinn",
    "Avery", "Harper", "Rowan", "Sage", "Blair", "Reese", "Skyler", "Drew",
    "Finley", "Hayden", "Kai", "Logan", "Parker", "Peyton", "Emerson", "Cameron",
]
LAST_NAMES = [
    "Chen", "Patel", "Garcia", "Nguyen", "Kim", "Johnson", "Singh", "Martinez",
    "Okafor", "Rossi", "Dubois", "Silva", "Müller", "Kowalski", "Tanaka", "Hansen",
]
CITIES = [
    "San Francisco", "New York", "Austin", "Seattle", "Chicago",
    "Boston", "Denver", "Atlanta", "Portland", "Los Angeles",
]


def main() -> None:
    random.seed(RNG_SEED)
    INPUT_DIR.mkdir(parents=True, exist_ok=True)

    users_path = INPUT_DIR / "users.csv"
    edges_path = INPUT_DIR / "friendships.csv"

    # ---- users ---------------------------------------------------------------
    users = []
    for uid in range(1, NUM_USERS + 1):
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        age = random.randint(18, 72)
        city = random.choice(CITIES)
        users.append((uid, name, age, city))

    with users_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "age", "city"])
        w.writerows(users)

    # ---- friendships ---------------------------------------------------------
    # Build a directed edge set with some community structure: users in the same
    # city are more likely to be connected.
    edges: set[tuple[int, int]] = set()
    target_edges = NUM_USERS * AVG_FRIENDS
    city_buckets: dict[str, list[int]] = {}
    for uid, _, _, city in users:
        city_buckets.setdefault(city, []).append(uid)

    while len(edges) < target_edges:
        if random.random() < 0.7:
            # same-city connection
            bucket = random.choice(list(city_buckets.values()))
            if len(bucket) < 2:
                continue
            a, b = random.sample(bucket, 2)
        else:
            # cross-city connection
            a, b = random.sample(range(1, NUM_USERS + 1), 2)
        if a == b:
            continue
        edges.add((a, b))

    with edges_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["src_id", "dst_id", "since", "weight"])
        for a, b in sorted(edges):
            year = random.randint(2015, 2025)
            weight = round(random.uniform(0.1, 1.0), 3)
            w.writerow([a, b, f"{year}-{random.randint(1,12):02d}-{random.randint(1,28):02d}", weight])

    print(f"Wrote {len(users)} users to {users_path}")
    print(f"Wrote {len(edges)} friendships to {edges_path}")

    # -------------------------------------------------------------------------
    # Staff / Customer network for shortest-path demo
    # -------------------------------------------------------------------------
    NUM_STAFF = 30
    NUM_CUSTOMERS = 50

    DEPARTMENTS = ["Sales", "Engineering", "Support", "Marketing", "Exec"]
    ROLES_BY_DEPT = {
        "Sales":       ["Account Executive", "SDR", "Sales Manager"],
        "Engineering": ["Engineer", "Tech Lead", "Architect"],
        "Support":     ["Support Rep", "CSM"],
        "Marketing":   ["Marketer", "Content Lead"],
        "Exec":        ["CEO", "CFO", "VP"],
    }
    SEGMENTS = ["SMB", "Mid-Market", "Enterprise"]
    INDUSTRIES = ["Healthcare", "Finance", "Retail", "Tech", "Manufacturing"]

    staff_path       = INPUT_DIR / "staff.csv"
    customers_path   = INPUT_DIR / "customers.csv"
    connections_path = INPUT_DIR / "connections.csv"

    # --- staff ---------------------------------------------------------------
    staff = []
    for sid in range(1, NUM_STAFF + 1):
        dept = random.choice(DEPARTMENTS)
        role = random.choice(ROLES_BY_DEPT[dept])
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        staff.append((sid, name, role, dept, random.choice(CITIES)))

    with staff_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["staff_id", "name", "role", "department", "city"])
        w.writerows(staff)

    # --- customers -----------------------------------------------------------
    customers = []
    for cid in range(1, NUM_CUSTOMERS + 1):
        name = f"{random.choice(LAST_NAMES)} {random.choice(['Corp', 'LLC', 'Inc', 'Group', 'Labs'])}"
        customers.append((
            cid,
            name,
            random.choice(SEGMENTS),
            random.choice(INDUSTRIES),
            random.choice(CITIES),
        ))

    with customers_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "name", "segment", "industry", "city"])
        w.writerows(customers)

    # --- connections (three overlapping sub-networks) ------------------------
    connections: list[tuple] = []

    # 1. Staff <-> Staff: each staff member connects to ~3 colleagues; same-dept preferred.
    dept_buckets: dict[str, list[int]] = {}
    for sid, _, _, dept, _ in staff:
        dept_buckets.setdefault(dept, []).append(sid)

    staff_edge_set: set[tuple[int, int]] = set()
    while len(staff_edge_set) < NUM_STAFF * 3:
        if random.random() < 0.7:
            bucket = random.choice([b for b in dept_buckets.values() if len(b) >= 2])
            a, b = random.sample(bucket, 2)
        else:
            a, b = random.sample(range(1, NUM_STAFF + 1), 2)
        if a != b:
            staff_edge_set.add((a, b))
    for a, b in staff_edge_set:
        rel = random.choice(["COLLABORATES_WITH", "COLLABORATES_WITH", "REPORTS_TO"])
        connections.append((a, "Staff", b, "Staff", rel, round(random.uniform(0.3, 1.0), 3)))

    # 2. Customer <-> Customer: sparse referral network, ~0.6 edges per customer.
    cust_edge_set: set[tuple[int, int]] = set()
    while len(cust_edge_set) < int(NUM_CUSTOMERS * 0.6):
        a, b = random.sample(range(1, NUM_CUSTOMERS + 1), 2)
        cust_edge_set.add((a, b))
    for a, b in cust_edge_set:
        connections.append((a, "Customer", b, "Customer", "REFERRED", round(random.uniform(0.2, 0.9), 3)))

    # 3. Staff <-> Customer: only ~40% of customers have a direct staff touchpoint,
    #    leaving the rest to be reached via multi-hop shortest paths.
    touch_edge_set: set[tuple[int, int]] = set()
    direct_customers = random.sample(range(1, NUM_CUSTOMERS + 1), int(NUM_CUSTOMERS * 0.4))
    for cid in direct_customers:
        # Each "covered" customer has 1-2 staff touchpoints, typically Sales/Support.
        num_links = random.choice([1, 1, 2])
        sales_support = [s[0] for s in staff if s[3] in ("Sales", "Support")] or [s[0] for s in staff]
        for sid in random.sample(sales_support, k=min(num_links, len(sales_support))):
            touch_edge_set.add((sid, cid))
    for sid, cid in touch_edge_set:
        rel = random.choice(["ACCOUNT_MANAGER", "MET", "EMAILED"])
        connections.append((sid, "Staff", cid, "Customer", rel, round(random.uniform(0.4, 1.0), 3)))

    with connections_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["src_id", "src_type", "dst_id", "dst_type", "rel_type", "strength"])
        w.writerows(connections)

    print(f"Wrote {len(staff)} staff to {staff_path}")
    print(f"Wrote {len(customers)} customers to {customers_path}")
    print(f"Wrote {len(connections)} connections to {connections_path} "
          f"(staff-staff={len(staff_edge_set)}, "
          f"cust-cust={len(cust_edge_set)}, "
          f"staff-cust={len(touch_edge_set)})")


if __name__ == "__main__":
    main()
