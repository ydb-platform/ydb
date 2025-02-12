
import pandas as pd
import plotly.graph_objects as go
import os
import datetime
import subprocess

timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

try:
    commit_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('utf-8').strip()[:7]
except Exception:
    commit_hash = "no-git"

output_dir = "graphs"
os.makedirs(output_dir, exist_ok=True)

df = pd.read_csv('memUsage.csv')

fig = go.Figure()

fig.add_trace(go.Scatter(x=df.index, y=df['limit'], mode='lines', name='Limit', line=dict(color='blue')))

fig.add_trace(go.Scatter(x=df.index, y=df['usage'], mode='lines', name='Usage', line=dict(color='green')))

fig.update_layout(
    title="Memory Usage and Limit Over Time",
    xaxis_title="Time",
    yaxis_title="Memory (in MB)",
    legend_title="Legend",
    hovermode="x unified"
)

filename = f"{output_dir}/memory_usage_{timestamp}_{commit_hash}.html"

fig.write_html(filename)

print(filename)

fig.show()

