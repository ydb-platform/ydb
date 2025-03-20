# admin cluster config fetch

С помощью команды `admin cluster config fetch` вы можете получить текущую [динамическую](../../../../../maintenance/manual/dynamic-config.md) конфигурацию кластера YDB.

Общий вид команды:

```bash
ydb [global options...] admin cluster config fetch
```

* `global options` — глобальные параметры.

Посмотрите описание команды получения динамической конфигурации:

```bash
ydb admin cluster config fetch --help
```

## Примеры {#examples}

Получите текущую динамическую конфигурацию кластера:

```bash
ydb --endpoint grpc://localhost:2135 admin cluster config fetch > config.yaml
```
