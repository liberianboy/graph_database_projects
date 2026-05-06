"""
Generate Staff_Customer_Graph_PoC_Documentation.pdf with reportlab.
"""
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak,
    Table, TableStyle, KeepTogether, ListFlowable, ListItem,
)
from reportlab.pdfgen import canvas

OUT = "/sessions/stoic-zealous-mayer/mnt/outputs/Staff_Customer_Graph_PoC_Documentation.pdf"

# ---------- Styles ----------
styles = getSampleStyleSheet()

title_style = ParagraphStyle(
    "TitleBig", parent=styles["Title"],
    fontName="Helvetica-Bold", fontSize=22, leading=26, spaceAfter=8,
    textColor=colors.HexColor("#1F3A68"),
)
subtitle_style = ParagraphStyle(
    "Subtitle", parent=styles["Normal"],
    fontName="Helvetica-Oblique", fontSize=12, leading=16, spaceAfter=24,
    textColor=colors.HexColor("#555555"),
)
h1 = ParagraphStyle(
    "H1", parent=styles["Heading1"],
    fontName="Helvetica-Bold", fontSize=16, leading=20,
    spaceBefore=16, spaceAfter=8,
    textColor=colors.HexColor("#1F3A68"),
)
h2 = ParagraphStyle(
    "H2", parent=styles["Heading2"],
    fontName="Helvetica-Bold", fontSize=13, leading=17,
    spaceBefore=12, spaceAfter=6,
    textColor=colors.HexColor("#2C5282"),
)
body = ParagraphStyle(
    "Body", parent=styles["Normal"],
    fontName="Helvetica", fontSize=10.5, leading=15,
    alignment=TA_JUSTIFY, spaceAfter=8,
)
bullet = ParagraphStyle(
    "Bullet", parent=body, leftIndent=16, bulletIndent=4, spaceAfter=4,
)
code = ParagraphStyle(
    "Code", parent=styles["Code"],
    fontName="Courier", fontSize=9, leading=12,
    leftIndent=10, rightIndent=10, spaceBefore=6, spaceAfter=10,
    textColor=colors.HexColor("#1A202C"),
    backColor=colors.HexColor("#F5F5F5"),
    borderColor=colors.HexColor("#CCCCCC"),
    borderWidth=0.5, borderPadding=6,
)
caption = ParagraphStyle(
    "Caption", parent=body, fontSize=9, textColor=colors.HexColor("#555555"),
    alignment=TA_LEFT, spaceBefore=2, spaceAfter=14,
)

# ---------- Page footer ----------
def footer(canv: canvas.Canvas, doc):
    canv.saveState()
    canv.setFont("Helvetica", 8)
    canv.setFillColor(colors.HexColor("#888888"))
    canv.drawString(0.75 * inch, 0.5 * inch,
                    "Staff-to-Customer Shortest Path PoC  |  PySpark + GraphFrames")
    canv.drawRightString(LETTER[0] - 0.75 * inch, 0.5 * inch,
                         f"Page {doc.page}")
    canv.restoreState()


# ---------- Helpers ----------
def p(t, s=body): return Paragraph(t, s)
def bullets(items):
    return ListFlowable(
        [ListItem(Paragraph(i, body), leftIndent=14, bulletColor=colors.HexColor("#1F3A68"))
         for i in items],
        bulletType="bullet", start="circle", leftIndent=14,
    )
def codeblock(lines):
    # Escape <, > for reportlab paragraph XML
    esc = "<br/>".join(l.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                       for l in lines)
    return Paragraph(f'<font face="Courier" size="9">{esc}</font>', code)

def section_table(rows, col_widths=None, header=True):
    tbl = Table(rows, colWidths=col_widths, hAlign="LEFT")
    style = [
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 9.5),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#CCCCCC")),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    if header:
        style += [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1F3A68")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ]
    tbl.setStyle(TableStyle(style))
    return tbl


# ---------- Build document ----------
doc = SimpleDocTemplate(
    OUT, pagesize=LETTER,
    leftMargin=0.75 * inch, rightMargin=0.75 * inch,
    topMargin=0.75 * inch, bottomMargin=0.75 * inch,
    title="Staff-to-Customer Shortest Path PoC",
    author="Martin",
)

story = []

# ===== Title page =====
story += [
    Spacer(1, 1.2 * inch),
    p("Identifying Shortest Connecting Points<br/>Between Staff and Key Customers", title_style),
    p("A Graph-Database Proof of Concept using PySpark and GraphFrames", subtitle_style),
    Spacer(1, 0.2 * inch),
    section_table(
        [["Document",      "Technical Design & Process Documentation"],
         ["Author",        "Martin"],
         ["Date",          "April 21, 2026"],
         ["Version",       "1.0"],
         ["Artefacts",     "staff_customer_graph_poc.py, staff_customer_graph_poc_lite.py"],
         ["Technologies",  "Python 3.10+, PySpark 3.5, GraphFrames 0.8.3"]],
        col_widths=[1.4 * inch, 4.6 * inch],
        header=False,
    ),
    PageBreak(),
]

# ===== 1. Executive Summary =====
story += [
    p("1. Executive Summary", h1),
    p(
        "Sales and account-management teams frequently need to reach senior "
        "decision-makers at target customer organisations. The warmest route is "
        "almost always an introduction through a mutual connection rather than a "
        "cold outreach. This Proof of Concept demonstrates how a graph database "
        "powered by PySpark and GraphFrames can automatically surface the shortest "
        "and warmest introduction chain between any member of staff and any key "
        "customer contact."
    ),
    p(
        "The PoC models the extended professional network as an undirected, "
        "weighted graph. A breadth-first search identifies the fewest-hop "
        "connection, and a Pregel-style shortest-path algorithm identifies the "
        "warmest chain (the path whose cumulative tie-strength score is lowest). "
        "Both algorithms scale horizontally because they are expressed in Spark "
        "DataFrame operations."
    ),
    p(
        "Two runnable Python artefacts are shipped with this document. The "
        "primary implementation uses GraphFrames for production-grade graph "
        "processing. A lightweight companion reproduces the same results using "
        "only core PySpark DataFrames, allowing the solution to be demonstrated "
        "in environments where the GraphFrames JAR is not yet available."
    ),

    p("2. Business Problem and Objectives", h1),
    p("2.1 Problem Statement", h2),
    p(
        "Account teams lack visibility into the hidden social graph that connects "
        "their own staff to prospects and key customer contacts. Introductions "
        "close at materially higher rates than cold outreach, yet the company's "
        "own employees, alumni networks, conference attendees, and board ties are "
        "scattered across CRM, HR, calendar, and third-party data sources. "
        "Without a unified graph representation, the warmest route to any given "
        "executive is effectively invisible."
    ),
    p("2.2 Objectives", h2),
    bullets([
        "Demonstrate a repeatable method for mapping the <b>shortest path</b> "
        "(fewest intermediaries) between any staff member and any key customer.",
        "Introduce an edge-weighted variant that surfaces the <b>warmest path</b> "
        "— not just the shortest — by incorporating relationship strength.",
        "Prove the design scales by implementing it on <b>PySpark</b>, which "
        "distributes computation across a cluster for millions of vertices and "
        "tens of millions of edges.",
        "Provide realistic <b>sample data</b> that reviewers can execute and "
        "modify without access to production systems.",
    ]),
    p("2.3 Out of Scope", h2),
    bullets([
        "Production integrations with CRM, HRIS, or third-party data vendors.",
        "User interface or embedded workflow tooling.",
        "Privacy, consent, and data-protection controls, which must be "
        "addressed before any production rollout (see Section 8).",
    ]),
    PageBreak(),
]

# ===== 3. Solution Design =====
story += [
    p("3. Solution Design", h1),
    p("3.1 Graph Model", h2),
    p(
        "The network is modelled as an undirected, weighted property graph. "
        "Vertices represent both people and the shared contexts that connect "
        "them (schools, former employers, events, advisory boards). Edges "
        "represent typed relationships and carry a <b>strength</b> score from "
        "1 (very strong tie) to 5 (weak or incidental tie). Lower total "
        "strength along a path indicates a warmer introduction chain."
    ),
    p("3.2 Vertex Schema", h2),
    section_table(
        [["Field",     "Type",    "Description"],
         ["id",        "string",  "Unique stable identifier"],
         ["name",      "string",  "Human-readable display name"],
         ["type",      "string",  "STAFF | CUSTOMER | PERSON | COMPANY | EVENT | SCHOOL"],
         ["priority",  "string",  "Populated for CUSTOMER rows only (KEY | STANDARD)"]],
        col_widths=[1.1 * inch, 0.9 * inch, 4.0 * inch],
    ),
    Spacer(1, 6),
    p("3.3 Edge Schema", h2),
    section_table(
        [["Field",     "Type",    "Description"],
         ["src, dst",  "string",  "Endpoint vertex ids"],
         ["rel",       "string",  "COLLEAGUE | EX_COLLEAGUE | ALUMNI | MET_AT | BOARD | FRIEND"],
         ["strength",  "integer", "1 (strongest) to 5 (weakest)"]],
        col_widths=[1.1 * inch, 0.9 * inch, 4.0 * inch],
    ),
    Spacer(1, 6),
    p("3.4 Algorithms", h2),
    p(
        "<b>Unweighted shortest path (BFS).</b> GraphFrames' <i>bfs</i> operator "
        "returns the fewest-edge path between a source vertex and any vertex "
        "matching a target predicate. This answers the question &quot;who is "
        "the minimum number of warm introductions away?&quot;"
    ),
    p(
        "<b>Weighted shortest path (Pregel).</b> The <i>aggregateMessages</i> "
        "API is used to propagate tentative distances along edges and keep the "
        "minimum at each vertex. Summing the <i>strength</i> values yields the "
        "warmest total chain, not merely the shortest in hops."
    ),
    p(
        "<b>Lightweight alternative.</b> For environments without GraphFrames, "
        "the same Dijkstra-style relaxation is expressed as repeated DataFrame "
        "joins against the edge table, iterating until the distance vector "
        "stabilises."
    ),
    p("3.5 Process Flow", h2),
    section_table(
        [["Step", "Stage",                          "Outcome"],
         ["1",    "Ingest vertices and edges",      "Spark DataFrames built from source systems"],
         ["2",    "Construct GraphFrame",           "Distributed graph object ready for queries"],
         ["3",    "Identify anchors",               "Filter staff vertices and KEY customers"],
         ["4",    "Run BFS per pair",               "Shortest hop-count path"],
         ["5",    "Run weighted shortest path",     "Warmest total-strength path from each staff member"],
         ["6",    "Publish results",                "Write top-N chains per pair back to the warehouse"]],
        col_widths=[0.5 * inch, 2.1 * inch, 3.4 * inch],
    ),
    PageBreak(),
]

# ===== 4. Sample Data =====
story += [
    p("4. Sample Data", h1),
    p(
        "The sample network contains 17 vertices and 48 directed edges "
        "(24 undirected relationships mirrored in both directions). It is small "
        "enough to reason about manually yet rich enough to expose non-trivial "
        "path choices."
    ),
    p("4.1 Staff and Key Customers", h2),
    section_table(
        [["Id",  "Name",                    "Role"],
         ["s1",  "Alice Chen",              "Staff"],
         ["s2",  "Ben Okafor",              "Staff"],
         ["s3",  "Carla Diaz",              "Staff"],
         ["c1",  "Acme Corp (CEO)",         "Key customer"],
         ["c2",  "Globex (CFO)",            "Key customer"],
         ["c3",  "Initech (Head of IT)",    "Key customer"]],
        col_widths=[0.6 * inch, 2.4 * inch, 3.0 * inch],
    ),
    Spacer(1, 6),
    p("4.2 Intermediate Nodes", h2),
    p(
        "Seven individual contacts (David Kim, Elena Rossi, Farouk Haddad, "
        "Grace Whitmore, Hiro Tanaka, Ines Moreau, Jamal Brooks) plus four "
        "shared-context nodes (Stanford MBA 2015, Ex-McKinsey Alumni, FinTech "
        "Summit 2024, Acme Advisory Board) bridge the staff to the customer "
        "vertices."
    ),
    p("4.3 Example Edges", h2),
    codeblock([
        '("s1", "p1", "COLLEAGUE",    1)   # Alice  <-> David       (strong tie)',
        '("s1", "p2", "ALUMNI",       2)   # Alice  <-> Elena       (alumni tie)',
        '("p1", "p6", "COLLEAGUE",    1)   # David  <-> Ines',
        '("p6", "c1", "BOARD",        1)   # Ines   <-> Acme CEO    (advisory board)',
        '("p4", "c2", "COLLEAGUE",    1)   # Grace  <-> Globex CFO',
        '("p7", "c3", "COLLEAGUE",    2)   # Jamal  <-> Initech Head of IT',
    ]),
    PageBreak(),
]

# ===== 5. Implementation =====
story += [
    p("5. Implementation", h1),
    p("5.1 Environment Setup", h2),
    codeblock([
        'pip install pyspark==3.5.0 graphframes-py',
        '',
        '# Or, launch PySpark with the GraphFrames package:',
        'pyspark --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12',
    ]),
    p("5.2 Building the Graph", h2),
    codeblock([
        'from pyspark.sql import SparkSession',
        'from graphframes import GraphFrame',
        '',
        'spark = (SparkSession.builder',
        '         .appName("StaffCustomerShortestPath")',
        '         .config("spark.jars.packages",',
        '                 "graphframes:graphframes:0.8.3-spark3.5-s_2.12")',
        '         .getOrCreate())',
        '',
        'vertices = sample_vertices(spark)',
        'edges    = sample_edges(spark)',
        'g = GraphFrame(vertices, edges)',
    ]),
    p("5.3 Shortest Hop-Count Path (BFS)", h2),
    codeblock([
        'paths = g.bfs(',
        '    fromExpr="id = \'s1\'",',
        '    toExpr  ="id = \'c1\'",',
        '    maxPathLength=6,',
        ')',
        'paths.show(truncate=False)',
    ]),
    p("5.4 Warmest Path (Weighted Shortest Path)", h2),
    codeblock([
        'from graphframes.lib import AggregateMessages as AM',
        'from pyspark.sql.functions import col, when, lit, least',
        '',
        'v = g.vertices.withColumn(',
        '    "dist", when(col("id") == source, lit(0.0))',
        '             .otherwise(lit(float("inf"))))',
        '',
        'for _ in range(6):',
        '    msg = AM.src["dist"] + AM.edge["strength"]',
        '    agg = g.aggregateMessages(F.min(AM.msg).alias("new_dist"),',
        '                              sendToDst=msg)',
        '    v = (v.join(agg, "id", "left")',
        '          .withColumn("dist",',
        '              least(col("dist"), col("new_dist").cast("double")))',
        '          .drop("new_dist"))',
        '    g = GraphFrame(v, g.edges)',
    ]),
    PageBreak(),
]

# ===== 6. Results =====
story += [
    p("6. Verified Results on Sample Data", h1),
    p(
        "Running the PoC end-to-end on the sample network yields the following "
        "paths. Results were cross-validated with a pure-Python BFS and "
        "Dijkstra reference implementation."
    ),
    p("6.1 Shortest (Hop-Count) Paths", h2),
    section_table(
        [["From",          "To",                       "Hops",  "Chain"],
         ["Alice Chen",    "Acme Corp (CEO)",          "2",     "Alice \u2192 Stanford MBA 2015 \u2192 Acme CEO"],
         ["Alice Chen",    "Globex (CFO)",             "4",     "Alice \u2192 David \u2192 Ines \u2192 Grace \u2192 Globex CFO"],
         ["Alice Chen",    "Initech (Head of IT)",     "3",     "Alice \u2192 Elena \u2192 Jamal \u2192 Initech"],
         ["Ben Okafor",    "Acme Corp (CEO)",          "3",     "Ben \u2192 Grace \u2192 Ines \u2192 Acme CEO"],
         ["Ben Okafor",    "Globex (CFO)",             "2",     "Ben \u2192 Grace \u2192 Globex CFO"],
         ["Ben Okafor",    "Initech (Head of IT)",     "3",     "Ben \u2192 Farouk \u2192 FinTech Summit \u2192 Initech"],
         ["Carla Diaz",    "Acme Corp (CEO)",          "4",     "Carla \u2192 Hiro \u2192 Grace \u2192 Ines \u2192 Acme CEO"],
         ["Carla Diaz",    "Globex (CFO)",             "3",     "Carla \u2192 Hiro \u2192 Grace \u2192 Globex CFO"],
         ["Carla Diaz",    "Initech (Head of IT)",     "2",     "Carla \u2192 FinTech Summit \u2192 Initech"]],
        col_widths=[1.05 * inch, 1.55 * inch, 0.4 * inch, 3.1 * inch],
    ),
    Spacer(1, 6),
    p("6.2 Warmest (Minimum Summed Strength) Paths", h2),
    section_table(
        [["From",          "To",                       "Strength", "Hops", "Chain"],
         ["Alice Chen",    "Acme Corp (CEO)",          "3",        "3",    "Alice \u2192 David \u2192 Ines \u2192 Acme CEO"],
         ["Alice Chen",    "Globex (CFO)",             "5",        "4",    "Alice \u2192 David \u2192 Ines \u2192 Grace \u2192 Globex"],
         ["Alice Chen",    "Initech (Head of IT)",     "6",        "3",    "Alice \u2192 Elena \u2192 Jamal \u2192 Initech"],
         ["Ben Okafor",    "Acme Corp (CEO)",          "5",        "3",    "Ben \u2192 Grace \u2192 Ines \u2192 Acme CEO"],
         ["Ben Okafor",    "Globex (CFO)",             "3",        "2",    "Ben \u2192 Grace \u2192 Globex CFO"],
         ["Ben Okafor",    "Initech (Head of IT)",     "7",        "3",    "Ben \u2192 Farouk \u2192 FinTech \u2192 Initech"],
         ["Carla Diaz",    "Acme Corp (CEO)",          "9",        "4",    "Carla \u2192 Hiro \u2192 Grace \u2192 Ines \u2192 Acme"],
         ["Carla Diaz",    "Globex (CFO)",             "7",        "3",    "Carla \u2192 Hiro \u2192 Grace \u2192 Globex"],
         ["Carla Diaz",    "Initech (Head of IT)",     "6",        "2",    "Carla \u2192 FinTech \u2192 Initech"]],
        col_widths=[1.0 * inch, 1.45 * inch, 0.65 * inch, 0.4 * inch, 2.6 * inch],
    ),
    Spacer(1, 6),
    p(
        "Note that the shortest and warmest path can differ. Alice's fewest-hop "
        "route to Acme uses Stanford MBA 2015 as an alumni bridge (2 hops, "
        "strength 4), but her warmest route goes via David and Ines "
        "(3 hops, strength 3) because both edges are strong personal ties.",
        caption,
    ),
    PageBreak(),
]

# ===== 7. Scaling =====
story += [
    p("7. Scaling and Performance", h1),
    p("7.1 Complexity", h2),
    p(
        "BFS on GraphFrames runs in O(V + E) per source. When executed for "
        "every (staff, customer) pair, complexity is O(S \u00d7 (V + E)) where "
        "S is the number of staff. Weighted shortest path uses a bounded number "
        "of Pregel supersteps (typically 5\u201310 for social graphs whose "
        "diameter is small), with each superstep performing O(E) work across "
        "the cluster."
    ),
    p("7.2 Deployment Patterns", h2),
    bullets([
        "<b>Batch refresh</b>: nightly Spark job materialises a top-N paths "
        "table per (staff, customer) pair into the data warehouse.",
        "<b>Interactive</b>: pre-compute single-source shortest paths from "
        "each key customer; serve queries by indexing into the resulting "
        "distance vectors.",
        "<b>Streaming updates</b>: merge incremental CRM/HR/calendar events "
        "into the edge table and recompute affected partitions.",
    ]),
    p("7.3 Tuning Guidance", h2),
    bullets([
        "Increase <i>spark.sql.shuffle.partitions</i> proportional to edge count.",
        "Broadcast the staff and customer id lists to avoid full shuffles.",
        "Persist the vertex DataFrame when iterating Pregel supersteps.",
        "Cap <i>maxPathLength</i> at a sensible value (commonly 4\u20136) "
        "to prune implausibly long introduction chains.",
    ]),

    p("8. Data Privacy and Governance", h1),
    p(
        "Relationship data is inherently sensitive. Before any production "
        "deployment the following controls should be designed in from day one:"
    ),
    bullets([
        "Explicit employee consent for their professional ties to be mapped.",
        "Purpose-limited use; edges visible only to authorised account staff.",
        "Row-level security aligned with regional privacy regulations "
        "(e.g. GDPR, CCPA) and any applicable works-council agreements.",
        "Audit logging for every shortest-path query including the requester, "
        "source staff member, and target customer contact.",
        "Clear retention policy and a documented deletion workflow for "
        "individuals who opt out or leave the business.",
    ]),

    p("9. Risks and Mitigations", h1),
    section_table(
        [["Risk",                                                         "Mitigation"],
         ["Stale or incorrect ties inflate false-warm paths",             "Decay edge strength with age; refresh from source systems on a cadence"],
         ["Hub nodes (large events, alumni groups) dominate paths",       "Cap the contribution of high-degree context nodes or weight them down"],
         ["Sensitive relationships surfaced to the wrong audience",       "Enforce row-level security; log and review every query"],
         ["GraphFrames compatibility across Spark versions",              "Pin the Spark / Scala / GraphFrames triple; use the lite implementation as a fallback"],
         ["Computational cost on very large graphs",                      "Partition by organisation; pre-compute single-source distances from each key customer"]],
        col_widths=[2.6 * inch, 3.4 * inch],
    ),
    PageBreak(),
]

# ===== 10. Next steps + appendix =====
story += [
    p("10. Recommended Next Steps", h1),
    bullets([
        "Replace sample data with real feeds: CRM contacts, HR org chart, "
        "shared calendar attendance, LinkedIn export, alumni databases.",
        "Add edge-strength features (frequency of interaction, recency, "
        "meeting duration) and learn weights from outcome data.",
        "Productionise as a scheduled Spark job writing to a warm-path "
        "table, and expose results to sales via the CRM.",
        "Extend the algorithm to return the <b>top-k</b> alternative paths "
        "rather than the single best, giving reps multiple introduction options.",
        "Evaluate managed graph databases (Neptune, Neo4j, TigerGraph) for "
        "interactive, sub-second query patterns alongside the Spark batch layer.",
    ]),

    p("Appendix A. Artefacts", h1),
    section_table(
        [["File",                                  "Purpose"],
         ["staff_customer_graph_poc.py",           "Reference implementation using PySpark + GraphFrames"],
         ["staff_customer_graph_poc_lite.py",      "Dependency-light variant using only PySpark DataFrames"],
         ["Staff_Customer_Graph_PoC_Documentation.pdf", "This document"]],
        col_widths=[2.8 * inch, 3.2 * inch],
    ),
    Spacer(1, 10),
    p("Appendix B. Glossary", h1),
    section_table(
        [["Term",          "Definition"],
         ["Vertex",         "Node in the graph; a person, company, event, or school"],
         ["Edge",           "Relationship between two vertices, with type and strength"],
         ["BFS",            "Breadth-First Search; yields minimum-hop path in an unweighted graph"],
         ["Dijkstra",       "Classic algorithm for shortest path in a weighted graph with non-negative edges"],
         ["Pregel",         "Vertex-centric distributed graph processing model used by GraphFrames"],
         ["aggregateMessages", "GraphFrames API for sending and reducing messages along edges"],
         ["Strength",       "Integer weight encoding tie quality; lower is warmer"]],
        col_widths=[1.6 * inch, 4.4 * inch],
    ),
]

doc.build(story, onFirstPage=footer, onLaterPages=footer)
print(f"Wrote {OUT}")
