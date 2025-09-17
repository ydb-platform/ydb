import json
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
with open("bench_results/3.json") as jfile:

    j = json.load(jfile)
    only_needed = []
    for obj in j:
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
    for data_flawour in ['SameSize', 'BigLeft']:
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
            Path("images/simple").mkdir(parents=True, exist_ok=True)
            plt.savefig(f'images/simple/{graph_name}.jpeg')
            axes.set_yscale('log')
            Path("images/log").mkdir(parents=True, exist_ok=True)
            plt.savefig(f'images/log/{graph_name}.jpeg')

