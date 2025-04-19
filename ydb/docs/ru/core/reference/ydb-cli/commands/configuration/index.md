# Управление конфигурацией

В YDB CLI доступны команды для управления [динамической конфигурацией](../../../../maintenance/manual/dynamic-config.md) на разных уровнях системы.

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

* [fetch](cluster/fetch.md) - получение текущей динамической конфигурации кластера.
* [generate](cluster/generate.md) - генерация динамической конфигурации на основе статической на кластере.
* [replace](cluster/replace.md) - замена динамической конфигурации.

### Конфигурация узла {#node}

Управление конфигурацией на уровне узла:

```bash
{{ ydb-cli }} admin node config [subcommands...]
```

Доступные подкоманды:

* [init](node/init.md) - инициализация директории для конфигурации узла.
