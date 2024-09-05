# Выполнение запроса

Подкоманда `table query execute` предназначена для надежного исполнения YQL-запросов. Подкоманда обеспечивает успешное исполнение запроса при кратковременной недоступности отдельных партиций таблиц, например, связанной с [их разделением или слиянием](../../concepts/datamodel/table.md#partitioning), за счет применения встроенных политик повторных попыток (retry policies).

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] table query execute [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).

Посмотрите описание команды выполнения YQL-запроса:

```bash
{{ ydb-cli }} table query execute --help
```

## Параметры подкоманды {#options}

#|
|| **Имя** | **Описание** ||
||`--timeout` | Время, в течение которого должна быть выполнена операция на сервере.||
||`-t`, `--type` | Тип запроса.
Возможные значения:
* `data` — YQL-запрос, содержащий [DML]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Data_Manipulation_Language){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Data_Manipulation_Language){% endif %} операции, допускает как изменение данных в базе, так и получение нескольких выборок с ограничением в 1000 строк в каждой выборке.
* `scan` — YQL-запрос типа [скан](../../concepts/scan_query.md), допускает только чтение данных из базы, может вернуть только одну выборку, но без ограничения на количество записей в ней. Алгоритм исполнения запроса типа `scan` на сервере более сложный по сравнению с `data`, поэтому в отсутствие требований по возврату более 1000 строк эффективнее использовать тип запроса `data`.
* `scheme` — YQL-запрос, содержащий [DDL]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Data_Definition_Language){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Data_Definition_Language){% endif %} операции.
Значение по умолчанию — `data`.||
||`--stats` | Режим сбора статистики.
Возможные значения:
* `none` — не собирать;
* `basic` — собирать по основным событиям;
* `full` — собирать по всем событиям.
Значение по умолчанию — `none`.||
||`-s` | Включить сбор статистики в режиме `basic`.||
||`--tx-mode` | [Режим транзакций](../../concepts/transactions.md#modes) (для запросов типа `data`).
Возможные значения:<li>`serializable-rw` — результат успешно выполненных параллельных транзакций эквивалентен определенному последовательному порядку их выполнения;<li>`online-ro` — каждое из чтений в транзакции читает последние на момент своего выполнения данные;<li>`stale-ro`  — чтения данных в транзакции возвращают результаты с возможным отставанием от актуальных (доли секунды).Значение по умолчанию — `serializable-rw`.||
||`-q`, `--query` | Текст YQL-запроса для выполнения.||
||`-f,` `--file` | Путь к файлу с текстом YQL-запроса для выполнения.||
||`--format` | Формат вывода.
Возможные значения:

{% include notitle [format](./_includes/result_format_common.md) %}

{% include notitle [format](./_includes/result_format_csv_tsv.md) %}

||
|#

### Работа с параметризованными запросами {#parameterized-query}

{% include [parameterized-query](../../_includes/parameterized-query.md) %}

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

### Создание таблиц {#examples-create-tables}

```bash
{{ ydb-cli }} -p quickstart table query execute \
  --type scheme \
  -q '
  CREATE TABLE series (series_id Uint64 NOT NULL, title Utf8, series_info Utf8, release_date Date, PRIMARY KEY (series_id));
  CREATE TABLE seasons (series_id Uint64, season_id Uint64, title Utf8, first_aired Date, last_aired Date, PRIMARY KEY (series_id, season_id));
  CREATE TABLE episodes (series_id Uint64, season_id Uint64, episode_id Uint64, title Utf8, air_date Date, PRIMARY KEY (series_id, season_id, episode_id));
  '
```

### Заполнение таблиц данными {#examples-upsert}
```bash
{{ ydb-cli }} -p quickstart table query execute \
  -q '
UPSERT INTO series (series_id, title, release_date, series_info) VALUES
  (1, "IT Crowd", Date("2006-02-03"), "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by Ash Atalla and starring Chris O'"'"'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."),
  (2, "Silicon Valley", Date("2014-04-06"), "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.");

UPSERT INTO seasons (series_id, season_id, title, first_aired, last_aired) VALUES
    (1, 1, "Season 1", Date("2006-02-03"), Date("2006-03-03")),
    (1, 2, "Season 2", Date("2007-08-24"), Date("2007-09-28")),
    (2, 1, "Season 1", Date("2014-04-06"), Date("2014-06-01")),
    (2, 2, "Season 2", Date("2015-04-12"), Date("2015-06-14"));

UPSERT INTO episodes (series_id, season_id, episode_id, title, air_date) VALUES
    (1, 1, 1, "Yesterday'"'"'s Jam", Date("2006-02-03")),
    (1, 1, 2, "Calamity Jen", Date("2006-02-03")),
    (2, 1, 1, "Minimum Viable Product", Date("2014-04-06")),
    (2, 1, 2, "The Cap Table", Date("2014-04-13"));
'
```

### Простая выборка данных {#examples-simple-query}

```bash
{{ ydb-cli }} -p quickstart table query execute -q '
    SELECT season_id, episode_id, title
    FROM episodes
    WHERE series_id = 1
  '
```

Результат:

```text
┌───────────┬────────────┬───────────────────┐
| season_id | episode_id | title             |
├───────────┼────────────┼───────────────────┤
| 1         | 1          | "Yesterday's Jam" |
├───────────┼────────────┼───────────────────┤
| 1         | 2          | "Calamity Jen"    |
└───────────┴────────────┴───────────────────┘
```

### Неограниченная по размеру выборка для автоматизированной обработки {#examples-query-stream}

Выборка данных запросом, текст которого сохранен в файле, без ограничения на количество строк в выборке, с выводом в формате [Newline-delimited JSON stream](https://en.wikipedia.org/wiki/JSON_streaming).

Запишем текст запроса в файл `request1.yql`:

```bash
echo 'SELECT season_id, episode_id, title FROM episodes' > request1.yql
```

Выполним запрос:

```bash
{{ ydb-cli }} -p quickstart table query execute -f request1.yql --type scan --format json-unicode
```

Результат:

```text
{"season_id":1,"episode_id":1,"title":"Yesterday's Jam"}
{"season_id":1,"episode_id":2,"title":"Calamity Jen"}
{"season_id":1,"episode_id":1,"title":"Minimum Viable Product"}
{"season_id":1,"episode_id":2,"title":"The Cap Table"}
```

### Передача параметров {#examples-params}

Примеры исполнения параметризованных запросов, включая потоковое исполнение, приведены в статье [Передача параметров в команды исполнения YQL](parameterized-queries-cli.md).

