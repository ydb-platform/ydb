# Проведение нагрузочного тестирования

В {{ ydb-short-name }} интегрирован инструментарий для проведения нагрузочных тестов с использованием стандартных бенчмарков:

| Бенчмарк                             | Справка                                                  |
|--------------------------------------|----------------------------------------------------------|
| [TPC-H](https://tpc.org/tpch/)       | [tpch](../../reference/ydb-cli/workload-tpch.md)|
| [TPC-DS](https://tpc.org/tpcds/)     | [tpcds](../../reference/ydb-cli/workload-tpcds.md)|
| [ClickBench](https://benchmark.clickhouse.com/) | [clickbench](../../reference/ydb-cli/workload-click-bench.md)|

Также предусмотрена возможность запуска пользовательских сценариев тестирования, которые инициируются посредством команды `ydb workload query`, см. [описание](../../reference/ydb-cli/workload-query.md). Подробности приведены в соответствующем разделе.

Все указанные методы эмулируют пользовательскую нагрузку на базу данных в рамках заданных сценариев. Детальное описание каждого метода представлено в соответствующих разделах, ссылки на которые приведены выше.

Все команды для работы с бенчмарками сгруппированы в соответствующие категории:

```bash
{{ ydb-cli }} workload clickbench ...
{{ ydb-cli }} workload tpch ...
{{ ydb-cli }} workload tpcds ...
{{ ydb-cli }} workload query ...
```

Нагрузочное тестирование состоит из трёх этапов:

  1. [Подготовка данных](#data-preparation)
  1. [Тестирование](#testing)
  1. [Очистка](#cleanup)

## Подготовка данных {#data-preparation}

Состоит из двух этапов, это инициализация таблиц и наполнение их данными.

### Инициализация

Инициализация производится командой `init`:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits init --store=row
{{ ydb-cli }} workload tpch --path tpch/s1 init --store=column
{{ ydb-cli }} workload tpcds --path tpcds/s1 init --store=external-s3
{{ ydb-cli }} workload query --path user/suite1 init --suite-path /home/user/user_suite
```

На этапе создания таблиц возможно дополнительно настроить следующие параметры:

  * Выбрать тип используемых таблиц: строковые, колоночные, внешние и тд. (параметр `--store`);
  * Выбрать типы используемых колонок: строк (параметр `--string`), дат и времени (`--datetime`) и тип вещественных чисел (`--float-mode`).

Также можно указать, что перед созданием таблицы должны быть удалены, если они уже созданы. Параметра `--clear`

Подробнее см. описание команд для каждого бенчмарка:

* [clickbench init](../../reference/ydb-cli/workload-click-bench.md#init)
* [tpch init](../../reference/ydb-cli/workload-tpch.md#init)
* [tpcds init](../../reference/ydb-cli/workload-tpcds.md#init)
* [query init](../../reference/ydb-cli/workload-query.md#init)

### Наполнение данными

Наполнение созданных таблиц данными выполняется с помощью команды `import`. Эта команда специфична для каждого бенчмарка, и её поведение зависит от дополнительных опций.

Подробное описание см. в соответствующих разделах:

* [clickbench import](../../reference/ydb-cli/workload-click-bench.md#load)
* [tpch import](../../reference/ydb-cli/workload-tpch.md#load)
* [tpcds import](../../reference/ydb-cli/workload-tpcds.md#load)
* [query import](../../reference/ydb-cli/workload-query.md#load)

Примеры:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits import files --input hits.csv.gz
{{ ydb-cli }} workload tpch --path tpch/s1 import generator --scale 1
{{ ydb-cli }} workload tpcds --path tpcds/s1 import generator --scale 1
{{ ydb-cli }} workload query --path user/suite1 import --suite-path /home/user/user_suite
```

## Тестирование {#testing}

Непосредственный запуск нагрузочного тестирования выполняется командой `run`. Её поведение практически одинаково для разных бенчмарков, хотя некоторые различия всё-таки присутствуют.

Примеры:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits run --include 1-5,8
{{ ydb-cli }} workload tpch --path tpch/s1 run --exсlude 3,4 --iterations 3
{{ ydb-cli }} workload tpcds --path tpcds/s1 run --plan ~/query_plan --include 2 --iterations 5
{{ ydb-cli }} workload query --path user/suite1 run --plan ~/query_plan --include first_query_set.1.sql,second_query_set.2.sql --iterations 5
```

Команда `run` для каждого из бенчмарков имеет ряд дополнительных параметров для настройки видов генерируемых отчётов, сбора статистики и прочих результатов нагрузочного тестирования.

Подробное описание см. в соответствующих разделах:

* [clickbench run](../../reference/ydb-cli/workload-click-bench.md#run)
* [tpch run](../../reference/ydb-cli/workload-tpch.md#run)
* [tpcds run](../../reference/ydb-cli/workload-tpcds.md#run)
* [query run](../../reference/ydb-cli/workload-query.md#run)

## Очистка {#cleanup}

При завершении работ по нагрузочному тестированию тестовые данные и таблицы можно удалить командой `clean`:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits clean
{{ ydb-cli }} workload tpch --path tpch/s1 clean
{{ ydb-cli }} workload tpcds --path tpcds/s1 clean
{{ ydb-cli }} workload query --path user/suite1 clean
```

Подробное описание см. в соответствующих разделах:

* [clickbench clean](../../reference/ydb-cli/workload-click-bench.md#cleanup)
* [tpch clean](../../reference/ydb-cli/workload-tpch.md#cleanup)
* [tpcds clean](../../reference/ydb-cli/workload-tpcds.md#cleanup)
* [query clean](../../reference/ydb-cli/workload-query.md#cleanup)
