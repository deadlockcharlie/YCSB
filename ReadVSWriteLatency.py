import os
import pandas as pd
import matplotlib.pyplot as plt

root_dir = "./BenchmarkResults/LatencyComparision"  # adjust as needed

data = []

dict_operations = {
    "GET_VERTEX_COUNT": "R1",
    "GET_EDGE_COUNT": "R2",
    "GET_EDGE_LABELS": "R3",
    "GET_VERTEX_WITH_PROPERTY": "R4",
    "GET_EDGE_WITH_PROPERTY": "R5",
    "GET_EDGES_WITH_LABEL": "R6",
    "ADD_VERTEX": "W1",
    "ADD_EDGE": "W2",
    "REMOVE_VERTEX": "W3",
    "REMOVE_EDGE": "W4",
    "SET_VERTEX_PROPERTY": "W5",
    "SET_EDGE_PROPERTY": "W6",
    "REMOVE_VERTEX_PROPERTY": "W7",
    "REMOVE_EDGE_PROPERTY": "W8"
}

for db in os.listdir(root_dir):
    db_path = os.path.join(root_dir, db)
    if not os.path.isdir(db_path):
        continue
    results_file = os.path.join(db_path, "results.txt")  # adjust if different
    if not os.path.exists(results_file):
        continue
    operations=["GET_VERTEX_COUNT","GET_EDGE_COUNT", "GET_EDGE_LABELS",
    "GET_VERTEX_WITH_PROPERTY", "GET_EDGE_WITH_PROPERTY", "GET_EDGES_WITH_LABEL", "ADD_VERTEX","ADD_EDGE","REMOVE_VERTEX", "REMOVE_EDGE", "SET_VERTEX_PROPERTY", "SET_EDGE_PROPERTY",
    "REMOVE_VERTEX_PROPERTY", "REMOVE_EDGE_PROPERTY" ]
    with open(results_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line or "," not in line:
                continue
            parts = line.split(",")
            if len(parts) != 3:
                continue
#             print(parts)
            operation, metric, value = parts
            operation = operation.strip("[]")
            metric = metric.strip()
            value = value.strip()
            if metric == "AverageLatency(us)" and operation in operations:
                data.append({
                    "DB": db,
                    "Operation": dict_operations[operation],
                    "Latency": float(value)
                })

# Convert to DataFrame
plot_df = pd.DataFrame(data)

# Pivot to have operations as index and DBs as columns
pivot_df = plot_df.pivot(index="Operation", columns="DB", values="Latency").fillna(0)

# Plot
pivot_df.plot(kind="bar", figsize=(5, 3))
plt.ylabel("Latency (µs)")
plt.yscale("log")
plt.title("")
plt.xticks(rotation=0, fontsize=8)
plt.legend(title="Database")
plt.tight_layout()
plt.savefig("GraceVsOthersLatency", dpi=500)
