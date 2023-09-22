# Нагрузочное тестирование

С помощью команды `workload` вы можете запустить различные виды нагрузки для вашей БД.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] workload [subcommands...]
```

* `global options` — [глобальные параметры](../../../commands/global-options.md).
* `subcommands` — [подкоманды](#subcomands).

Посмотрите описание команды для запуска нагрузки:

```bash
{{ ydb-cli }} workload --help
```

## Доступные подкоманды {#subcommands}

В данный момент поддерживаются следующие виды нагрузочных тестов:

* [Stock](../stock.md) - симулятор склада интернет-магазина.
* [Key-value](../../../workload-kv.md) - Key-Value нагрузка.
* [ClickBench](../../../workload-click-bench.md) - аналитический бенчмарк ClickBench (https://github.com/ClickHouse/ClickBench).
* [TPC-H](../../../workload-tpch.md) - TPC-H бенчмарк (https://www.tpc.org/tpch/).
* [Topic](../../../workload-topic.md) - Topic нагрузка.
* [Transfer](../../../workload-transfer.md) - Transfer нагрузка.
