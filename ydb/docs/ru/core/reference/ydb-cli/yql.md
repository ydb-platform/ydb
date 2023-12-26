# Выполнение скрипта (с поддержкой стриминга)

С помощью подкоманды `yql` вы можете выполнить YQL-скрипт. Скрипт может содержать запросы разных типов. В отличие от `scripting yql`, подкоманда `yql` устанавливает стрим и получает данные через него. Выполнение запроса в стриме позволяет снять ограничение на размер читаемых данных.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] yql [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).

Посмотрите описание команды выполнения YQL-скрипта:

```bash
{{ ydb-cli }} yql --help
```

## Параметры подкоманды {#options}

Имя | Описание
---|---
`--timeout` | Время, в течение которого должна быть выполнена операция на сервере.
`--stats` | Режим сбора статистики.<br>Возможные значения:<ul><li>`none` (по умолчанию) — не собирать;</li><li>`basic` — собирать по основным событиям;</li><li>`full` — собирать по всем событиям.</li></ul>
`-s`, `--script` | Текст YQL-скрипта для выполнения.
`-f`, `--file` | Путь к файлу с текстом YQL-скрипта для выполнения.
`--format` | Формат вывода.<br>Возможные значения:<ul><li>`pretty` (по умолчанию) — человекочитаемый формат;</li><li>`json-unicode` — вывод в формате [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}, бинарные строки закодированы в [Юникод]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Юникод){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Unicode){% endif %}, каждая строка JSON выводится в отдельной строке;</li><li>`json-unicode-array` — вывод в формате JSON, бинарные строки закодированы в Юникод, результат выводится в виде массива строк JSON, каждая строка JSON выводится в отдельной строке;</li><li>`json-base64` — вывод в формате JSON, бинарные строки закодированы в [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}, каждая строка JSON выводится в отдельной строке;</li><li>`json-base64-array` — вывод в формате JSON, бинарные строки закодированы в Base64, результат выводится в виде массива строк JSON, каждая строка JSON выводится в отдельной строке;</li><li>`csv` —вывод в формате [CSV](https://ru.wikipedia.org/wiki/CSV);</li><li>`tsv` —вывод в формате [TSV](https://ru.wikipedia.org/wiki/TSV).</li></ul>

### Работа с параметризованными запросами {#parameterized-query}

{% include [parameterized-query](../../_includes/parameterized-query.md) %}

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Скрипт создания таблицы, заполнения её данными, и получения выборки из этой таблицы:

```bash
{{ ydb-cli }} -p quickstart yql -s '
    CREATE TABLE series (series_id Uint64, title Utf8, series_info Utf8, release_date Date, PRIMARY KEY (series_id));
    COMMIT;
    UPSERT INTO series (series_id, title, series_info, release_date) values (1, "Title1", "Info1", Cast("2023-04-20" as Date));
    COMMIT;
    SELECT * from series;
  '
```

Вывод команды:

```text
┌──────────────┬───────────┬─────────────┬──────────┐
| release_date | series_id | series_info | title    |
├──────────────┼───────────┼─────────────┼──────────┤
| "2023-04-20" | 1         | "Info1"     | "Title1" |
└──────────────┴───────────┴─────────────┴──────────┘
```

Выполнение скрипта из примера выше, записанного в файле `script1.yql`, с выводом результатов в формате `JSON`:

```bash
{{ ydb-cli }} -p quickstart yql -f script1.yql --format json-unicode
```

Вывод команды:

```text
{"release_date":"2023-04-20","series_id":1,"series_info":"Info1","title":"Title1"}
```


Примеры передачи параметров в скрипты приведены в [статье о передаче параметров в команды исполнения YQL](parameterized-queries-cli.md).

