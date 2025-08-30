import os
import re
import matplotlib.pyplot as plt
import sys
import numpy as np

if len(sys.argv) != 3:
    print(f"Usage: python {sys.argv[0]} <logsDirectory> <threadCountToAnalyze>")
    sys.exit(1)

top_directory = sys.argv[1]
selected_filename = sys.argv[2]


# # Top-level directory containing subdirectories (one per DB)
# top_directory = "./results"  # change this to your directory
# # Filename to analyze inside each DB directory
# selected_filename = "1"  # change this to the filename you want to analyze

latencies = {}
for db_name in os.listdir(top_directory):
    db_path = os.path.join(top_directory, db_name)
    if not os.path.isdir(db_path):
        continue

    filepath = os.path.join(db_path, selected_filename)
    if not os.path.isfile(filepath):
        continue

    with open(filepath, "r") as f:
        content = f.read()

    read_avgs, write_avgs = [], []

    # Reads = READ + SCAN
    for op in ["READ", "SCAN"]:
        avg_match = re.search(rf"\[{op}\], AverageLatency\(us\), ([0-9.]+)", content)
        if avg_match:
            read_avgs.append(float(avg_match.group(1)) / 1000.0)

    # Writes = INSERT + UPDATE (updates = deletes)
    for op in ["INSERT", "UPDATE"]:
        avg_match = re.search(rf"\[{op}\], AverageLatency\(us\), ([0-9.]+)", content)
        if avg_match:
            write_avgs.append(float(avg_match.group(1)) / 1000.0)

    if read_avgs or write_avgs:
        latencies[db_name] = {
            "read": sum(read_avgs) / len(read_avgs) if read_avgs else 0,
            "write": sum(write_avgs) / len(write_avgs) if write_avgs else 0,
        }

# Prepare data for plotting
dbs = list(latencies.keys())
n_dbs = len(dbs)
x = np.arange(2)  # 0 = Read, 1 = Write
bar_width = 0.8 / n_dbs  # split bar space among DBs

plt.figure(figsize=(10, 6))

for i, db in enumerate(dbs):
    read_val = latencies[db]["read"]
    write_val = latencies[db]["write"]
    plt.bar(x + i * bar_width, [read_val, write_val],
            width=bar_width, label=db)

plt.xticks(x + (n_dbs - 1) * bar_width / 2, ["Read", "Write"])
plt.ylabel("Latency (ms)")
plt.title(f"Average Read vs Write Latency across Databases ({selected_filename})")
plt.legend()
plt.grid(axis="y")
plt.show()