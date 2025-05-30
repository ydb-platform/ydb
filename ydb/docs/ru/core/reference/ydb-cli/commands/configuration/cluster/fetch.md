# admin cluster config fetch

С помощью команды `admin cluster config fetch` вы можете получить текущую [конфигурацию](../../../../../devops/configuration-management/index.md) кластера {{ ydb-short-name }}. В зависимости от используемой кластером [версии конфигурации](../../../../../devops/configuration-management/compare-configs.md), команда возвращает:

* V1 — только [динамическую конфигурацию](../../../../../devops/configuration-management/configuration-v1/dynamic-config.md);
* V2 — всю конфигурацию.

Общий вид команды:

```bash
ydb [global options...] admin cluster config fetch
```

* `global options` — глобальные параметры.

Посмотрите описание команды получения конфигурации:

```bash
ydb admin cluster config fetch --help
```

## Примеры {#examples}

Получите текущую конфигурацию кластера:

```bash
ydb --endpoint grpc://localhost:2135 admin cluster config fetch > config.yaml
```
