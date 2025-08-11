# Управление конфигурацией

В YDB CLI доступны команды для управления [динамической конфигурацией](../../../../devops/configuration-management/configuration-v1/dynamic-config.md) на разных уровнях системы.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] admin [scope] config [subcommands...]
```

* `global options` — [глобальные параметры](../global-options.md).
* `scope` — область применения конфигурации (cluster, node).
* `subcommands` — подкоманды для управления конфигурацией.

Посмотрите описание команды:

```bash
{{ ydb-cli }} admin --help
```

## Доступные области конфигурации {#scopes}

### Конфигурация кластера {#cluster}

Управление конфигурацией на уровне кластера:

```bash
{{ ydb-cli }} admin cluster config [subcommands...]
```

Доступные подкоманды:

* [fetch](cluster/fetch.md) - получение текущей конфигурации кластера.
* [generate](cluster/generate.md) - генерация [конфигурации V2](../../../../devops/configuration-management/configuration-v2/index.md) из [V1](../../../../devops/configuration-management/configuration-v1/index.md) для [миграции между ними](../../../../devops/configuration-management/migration/migration-to-v2.md).
* [replace](cluster/replace.md) - замена текущей конфигурации кластера.
* version - отображение версии конфигурации на узлах (V1/V2).

### Конфигурация узла {#node}

Управление конфигурацией на уровне узла:

```bash
{{ ydb-cli }} admin node config [subcommands...]
```

Доступные подкоманды:

* [init](node/init.md) - инициализация директории для конфигурации узла.
