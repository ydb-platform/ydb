import json
import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
from pathlib import Path
if len(sys.argv) < 2:
    print("usage: python3 graph.py folder/file.jsonl")
    sys.exit(1)
j = pd.read_json(path_or_buf=sys.argv[1], lines=True)

j = j.reset_index()
only_needed = []
print(j)
for _, obj in j.iterrows():
    print(obj)
    run_name = obj["testName"]
    name_parts = run_name.split('_')
    only_needed.append({
            'run_name': run_name,
            'time': obj["resultTime"],
            'join_algorithm': name_parts[0],
            'input_data_flavour': name_parts[1],
            'left_table_size': name_parts[2],
            'key_type': name_parts[3]
        }
    )
print(len(only_needed) , " " , len(j))
print(only_needed)
df = pd.DataFrame(only_needed)
df = df.drop('run_name', axis=1)
print(df)
for data_flavour in ['SameSize', 'BigLeft']:
    for key_type in ['Integer', 'String']:
        graph_name = data_flawour + "_" + key_type
        print(graph_name)
        subset = df[(df["input_data_flavour"] == data_flawour) & 
            (df["key_type"] == key_type)]
        print(subset)
        fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(10, 8), sharex=True)
        
        for name, group in subset.groupby('join_algorithm'):
            axes.plot(
                group['left_table_size'], 
                group['time'], 
                label=name,
                marker='o'
            )
        axes.set_ylabel('time')
        axes.set_xlabel('left_rows')
        axes.legend()

        fig.suptitle(graph_name, fontsize=16)
        plt.tight_layout()
        plt.subplots_adjust(top=0.92)
        images_root_base = "~/.images"

        next_free = 0
        while(os.path.isdir('new_folder')) next_free += 1
        images_root = images_root_base + '/' + str(next_free)
        simple_images = images_root + "simple"
        Path(simple_images).mkdir(parents=True, exist_ok=True)
        plt.savefig(simple_images + "/" + graph_name + ".jpeg")
        axes.set_yscale('log')

        log_images = {images_root} + "log"
        Path(log_images).mkdir(parents=True, exist_ok=True)
        plt.savefig(log_images + "/" + graph_name + ".jpeg")

