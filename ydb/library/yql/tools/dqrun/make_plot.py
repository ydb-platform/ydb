
import pandas as pd
import plotly.graph_objects as go

# Загружаем данные из CSV файла
df = pd.read_csv('memUsage.csv')

# Создаем интерактивный график
fig = go.Figure()

# Линия для лимита памяти
fig.add_trace(go.Scatter(x=df.index, y=df['limit'], mode='lines', name='Limit', line=dict(color='blue')))

# Линия для использования памяти
fig.add_trace(go.Scatter(x=df.index, y=df['usage'], mode='lines', name='Usage', line=dict(color='green')))

# Настройки графика
fig.update_layout(
    title="Memory Usage and Limit Over Time",
    xaxis_title="Time",
    yaxis_title="Memory (in MB)",
    legend_title="Legend",
    hovermode="x unified"
)

# Сохраняем интерактивный график в файл HTML
fig.write_html('memory_usage_plot.html')

# Отображаем график в браузере
fig.show()

