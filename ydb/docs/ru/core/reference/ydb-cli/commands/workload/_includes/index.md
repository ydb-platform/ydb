# Нагрузочное тестирование

С помощью команды `workload` вы можете запустить различные виды нагрузки для вашей БД.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] workload [subcommands...]
```

* `global options` — [глобальные параметры](../../../commands/global-options.md).
* `subcommands` — [подкоманды](#subcommands).

Посмотрите описание команды для запуска нагрузки:

```bash
{{ ydb-cli }} workload --help
```

## Доступные подкоманды {#subcommands}

В данный момент поддерживаются следующие виды нагрузочных тестов:

* [Stock](../stock.md) - симулятор склада интернет-магазина.
* [Key-value](../../../workload-kv.md) - Key-Value нагрузка.
* [ClickBench](../../../workload-click-bench.md) - [аналитический бенчмарк ClickBench](https://github.com/ClickHouse/ClickBench).
* [TPC-C](../../../workload-tpcc.md): [TPC-C benchmark](https://www.tpc.org/tpcc/).
* [TPC-H](../../../workload-tpch.md) - [TPC-H бенчмарк](https://www.tpc.org/tpch/).
* [TPC-DS](../../../workload-tpcds.md) - [TPC-DS бенчмарк](https://www.tpc.org/tpcds/).
* [Topic](../../../workload-topic.md) - Topic нагрузка.
* [Transfer](../../../workload-transfer.md) - Transfer нагрузка.
* [Vector](../../../workload-vector.md) - нагрузка векторного поиска.

## Общие параметры для всех видов нагрузки {#global_workload_options}

Имя параметра | Короткое имя | Описание параметра
---|---|---
`--seconds <значение>` | `-s <значение>` | Продолжительность теста, в секундах. Значение по умолчанию: 10.
`--threads <значение>` | `-t <значение>` | Количество параллельных потоков, создающих нагрузку. Значение по умолчанию: 10.
`--rate <значение>` | - | Суммарная частота запросов от всех потоков, в транзакциях в секунду. Значение по умолчанию: 0 (не ограничена).
`--quiet` | - | Выводит только итоговый результат теста.
`--print-timestamp` | - | Печатает время вместе со статистикой каждого временного окна.
`--client-timeout` | - | [Транспортный таймаут в миллисекундах](../../../../../dev/timeouts.md).
`--operation-timeout` | - | [Таймаут на операцию в миллисекундах](../../../../../dev/timeouts.md).
`--cancel-after` | - | [Таймаут отмены операции в миллисекундах](../../../../../dev/timeouts.md).
`--window` | - | Длительность окна сбора статистики, в секундах. Значение по умолчанию: 1.
