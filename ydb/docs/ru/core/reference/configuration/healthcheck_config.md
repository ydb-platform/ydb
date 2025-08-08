# healthcheck_config

Секция `healthcheck_config` настраивает пороговые значения и таймауты, используемые [сервисом Health Check](../ydb-sdk/health-check-api.md) {{ ydb-short-name }}. Эти параметры помогают настраивать обнаружение возможных [проблем](../ydb-sdk/health-check-api.md#issues), таких как чрезмерные перезапуски или расхождение по времени между динамическими узлами.

## Синтаксис

```yaml
healthcheck_config:
  thresholds:
    node_restarts_yellow: 10
    node_restarts_orange: 30
    nodes_time_difference_yellow: 5000
    nodes_time_difference_orange: 25000
    tablets_restarts_orange: 30
  timeout: 20000
```

## Параметры

| Параметр                                 | Значение по умолчанию | Описание                                                                       |
|------------------------------------------|------------------------|--------------------------------------------------------------------------------|
| `thresholds.node_restarts_yellow`         | `10`     | Количество перезапусков узлов для генерации предупреждения уровня `YELLOW`     |
| `thresholds.node_restarts_orange`         | `30`     | Количество перезапусков узлов для генерации предупреждения уровня `ORANGE`     |
| `thresholds.nodes_time_difference_yellow` | `5000`   | Максимально допустимое расхождение по времени (в µs) между динамическими узлами для уровня `YELLOW` |
| `thresholds.nodes_time_difference_orange` | `25000`  | Максимально допустимое расхождение по времени (в µs) между динамическими узлами для уровня `ORANGE` |
| `thresholds.tablets_restarts_orange`      | `30`     | Количество перезапусков таблеток для генерации предупреждения уровня `ORANGE`  |
| `timeout`                                 | `20000`  | Максимальное время ответа от healthcheck (в мс)                                |