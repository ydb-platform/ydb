
import pandas as pd
import plotly.graph_objects as go
import os
import datetime
import subprocess

# Получаем текущий timestamp
timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Получаем commit hash
try:
    commit_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('utf-8').strip()[:7]
except Exception:
    commit_hash = "no-git"

# Создаем папку для сохранения графиков
output_dir = "graphs"
os.makedirs(output_dir, exist_ok=True)

# Читаем CSV-файл
df = pd.read_csv('0x7fe1b239a480_memUsage.csv')

fig = go.Figure()

# Линия limit
fig.add_trace(go.Scatter(x=df.index, y=df['limit'], mode='lines', name='Limit', line=dict(color='blue')))

# Линия usage
fig.add_trace(go.Scatter(x=df.index, y=df['usage'], mode='lines', name='Usage', line=dict(color='green')))

# Добавляем пометки для reached=True
reached_points = df[df['reached'] == True]
fig.add_trace(go.Scatter(
    x=reached_points.index, 
    y=reached_points['usage'], 
    mode='markers', 
    name='Reached',
    marker=dict(color='red', size=8, symbol='cross')
))

# Настройки графика
fig.update_layout(
    title="Memory Usage and Limit Over Time",
    xaxis_title="Time",
    yaxis_title="Memory (in MB)",
    legend_title="Legend",
    hovermode="x unified"
)

# Генерация имени файла и сохранение
filename = f"{output_dir}/memory_usage_{timestamp}_{commit_hash}.html"
fig.write_html(filename)

print(filename)

# Отображение графика
fig.show()
