import json
from matplotlib.ticker import FormatStrFormatter, ScalarFormatter
import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
import numpy as np
import math
from pathlib import Path
from html_table import generate_comparison_table
if len(sys.argv) < 2:
    print("usage: python3 graph.py folder/file.jsonl")
    sys.exit(1)
j = pd.read_json(path_or_buf=sys.argv[1], lines=True)

j = j.reset_index()
only_needed = []
seed = 0
for _, obj in j.iterrows():
    run_name = obj["testName"]
    name_parts = run_name.split('_')
    seed = int(name_parts[3])
    only_needed.append({
            'run_name': run_name,
            'time': int(obj["resultTime"]),
            'join_algorithm': name_parts[0],
            'key_type': name_parts[1],
            'preset': name_parts[2],
            'seed': int(name_parts[3]),
            'input_data_flavour': name_parts[4],
            'left_table_size': int(name_parts[5]),
            'right_table_size': int(name_parts[6]),
        }
    )
# is_time_sampled = only_needed[0]["input_data_flavour"].startswith("Sampling")
def geo_mean_70percent_lowest(series):
    size = len(series)
    smallest = series.nsmallest(math.ceil(size * 0.7))
    positive = smallest[smallest > 0]
    if len(positive) == 0:
        return np.nan
    return np.exp(np.mean(np.log(positive)))

df = pd.DataFrame(only_needed)
df = df.drop('run_name', axis=1)
# images_root_base = str(Path.home())+"/.join_perf/images"
next_free = 0
out_root_base = str(Path.home())+"/.join_perf/graphs"
while os.path.isdir(out_root_base + '/' + str(next_free)): next_free += 1
out_root = out_root_base + '/' + str(next_free) + '/'
simple_images = out_root + "simple"
html_tables = out_root + "html_tables"

Path(simple_images).mkdir(parents=True, exist_ok=True)
Path(html_tables).mkdir(parents=True, exist_ok=True)
pd.set_option('display.max_rows', 500)
names_map = {
    "BlockHash": "BlockHashJoin",
    "ScalarMap": "MapJoin",
    "ScalarGrace": "GraceJoin",
    "BlockMap": "BlockMap(YT)",
    "ScalarHash": "unordered_map",
}
data_flavours = df['input_data_flavour'].unique()
key_types = df['key_type'].unique()
for data_flavour in data_flavours:
    for key_type in key_types:
        subset = df[(df["input_data_flavour"] == data_flavour) & 
            (df["key_type"] == key_type)]
        print(subset['preset'].iloc[0])
        file_name = str(subset['preset'].iloc[0]) + "_" + data_flavour + "_" + key_type
        graph_name = f"сравнение аналитических алгоритмов джоина с {key_type} типом ключа"
        fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(10, 8), sharex=True)
        group_values = {}
        sizes = []
        for name, group in subset.groupby('join_algorithm'):
            name = names_map[name]
            group = group.groupby('left_table_size')['time'].apply(lambda x: geo_mean_70percent_lowest(x)).sort_index()
            axes.set_xticks(group.index)
            axes.plot(
                group.index, 
                group.values, 
                label=name, 
                marker='o' 
            )
            sizes = group.index
            group_values[name] = group.values
            print(group)
        # umap_html_table = generate_comparison_table(sizes, key_type.lower(), group_values["unordered_map"], "unordered_map", group_values["GraceJoin"])
        # with open(html_tables + f"/{key_type}-umap_vs_grace.html", "w", encoding="utf-8") as f:
        #     f.write(umap_html_table)
        neumann_html_table = generate_comparison_table(sizes, key_type.lower(),  group_values["BlockHashJoin"], "BlockHashJoin", group_values["GraceJoin"])
        with open(html_tables + f"/{key_type}-neumann_vs_grace.html", "w", encoding="utf-8") as f:
            f.write(neumann_html_table)
        

        axes.set_ylabel('время(ms)')
        axes.set_xlabel('размер левой таблички(строчки)')
        axes.get_xaxis().set_major_formatter(FormatStrFormatter('%d'))
        axes.legend()

        fig.suptitle(graph_name, fontsize=16)
        plt.tight_layout()
        plt.subplots_adjust(top=0.92)
        plt.savefig(simple_images + "/" + file_name + ".jpeg")
        
print(f"images and html tables are written to {out_root}") 
os.symlink(sys.argv[1], out_root + "/src.jsonl") 
#todo: log graphs to arcadia