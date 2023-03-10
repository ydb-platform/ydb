# Выполнение скрипта

С помощью подкоманды `scripting yql` вы можете выполнить YQL-скрипт. Скрипт может содержать запросы разных типов. В отличие от `yql`, подкоманда `scripting yql` имеет ограничение на количество возвращаемых строк и объем затрагиваемых данных.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] scripting yql [options...]
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).

Посмотрите описание команды выполнения YQL-скрипта:

```bash
ydb scripting yql --help
```

## Параметры подкоманды {#options}

Имя | Описание
---|---
`--timeout` | Время, в течение которого должна быть выполнена операция на сервере.
`--stats` | Режим сбора статистики.<br>Возможные значения:<ul><li>`none` — не собирать;</li><li>`basic` — собирать по основным событиям;</li><li>`full` — собирать по всем событиям.</li></ul>Значение по умолчанию — `none`.
`-s`, `--script` | Текст YQL-скрипта для выполнения.
`-f`, `--file` | Путь к файлу с текстом YQL-скрипта для выполнения.
`--explain` | Показать план выполнения запроса.
`--show-response-metadata` | Показать метаданные ответа.
`-p`, `--param` | [Параметры запроса](../../getting_started/yql.md#param) (для запросов над данными и скан-запросах).<br>Может быть указано несколько параметров. Для изменения формата ввода используйте параметр подкоманды `--input-format`.
`--input-format` | Формат ввода.<br>Возможные значения:<ul><li>`json-unicode` — ввод в формате [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}, бинарные строки закодированы в [Юникод]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Юникод){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Unicode){% endif %};</li><li>`json-base64` — ввод в формате JSON, бинарные строки закодированы в [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}.</li></ul>
`--format` | Формат вывода.<br>Значение по умолчанию — `pretty`.<br>Возможные значения:<ul><li>`pretty` — человекочитаемый формат;</li><li>`json-unicode` — вывод в формате [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}, бинарные строки закодированы в [Юникод]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Юникод){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Unicode){% endif %}, каждая строка JSON выводится в отдельной строке;</li><li>`json-unicode-array` — вывод в формате JSON, бинарные строки закодированы в Юникод, результат выводится в виде массива строк JSON, каждая строка JSON выводится в отдельной строке;</li><li>`json-base64` — вывод в формате JSON, бинарные строки закодированы в [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}, каждая строка JSON выводится в отдельной строке;</li><li>`json-base64-array` — вывод в формате JSON, бинарные строки закодированы в Base64, результат выводится в виде массива строк JSON, каждая строка JSON выводится в отдельной строке.</li></ul>

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

### Выполнение запроса с таймаутом {#example-timeout}

Выполните запрос с таймаутом 500 мс:

```bash
ydb yql \
  --script \
  "CREATE TABLE series ( \
  series_id Uint64, \
  title Utf8, \
  series_info Utf8, \
  release_date Uint64, \
  PRIMARY KEY (series_id) \
  );" \
  --timeout 500 
```

Если сервер не успеет выполнить запрос за 500 мс, он ответит ошибкой. Если по какой либо причине клиент не сможет получить сообщение сервера об ошибке, то операция будет прервана через 500+200 мс на стороне клиента.

### Выполнение параметризированного запроса {#example-param}

Выполните параметризированный запрос со сбором статистики:

```bash
ydb yql \
  --stats full \
  --script \
  "DECLARE \$myparam AS Uint64; \
  SELECT * FROM series WHERE series_id=\$myparam;" \
  --param '$myparam=1'
```

Результат:

```text
┌──────────────┬───────────┬──────────────────────────────────────────────────────────────────────────────┬────────────┐
| release_date | series_id | series_info                                                                  | title      |
├──────────────┼───────────┼──────────────────────────────────────────────────────────────────────────────┼────────────┤
| 13182        | 1         | "The IT Crowd is a British sitcom produced by Channel 4, written by Graham L | "IT Crowd" |
|              |           | inehan, produced by Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Ka |            |
|              |           | therine Parkinson, and Matt Berry."                                          |            |
└──────────────┴───────────┴──────────────────────────────────────────────────────────────────────────────┴────────────┘

Statistics:
query_phases {
  duration_us: 14294
  table_access {
    name: "/my-db/series"
    reads {
      rows: 1
      bytes: 209
    }
    partitions_count: 1
  }
  cpu_time_us: 783
  affected_shards: 1
}
process_cpu_time_us: 5083
total_duration_us: 81373
total_cpu_time_us: 5866
```
