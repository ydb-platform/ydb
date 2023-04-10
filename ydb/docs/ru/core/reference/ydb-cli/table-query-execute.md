# Выполнение запроса

Подкоманда `table query execute` предназначена для надежного исполнения YQL-запросов, не содержащих команд управления транзакциями или [DDL-операторов]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Data_definition_language){% else %}(https://en.wikipedia.org/wiki/Data_definition_language){% endif %}. Подкоманда обеспечивает успешное исполнение запроса при кратковременной недоступности отдельных партиций таблиц, например, связанной с [их разделением или слиянием](../../concepts/datamodel/table.md#partitioning), за счет применения встроенных политик повторных попыток (retry policies).

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

Имя | Описание
---|---
`--timeout` | Время, в течение которого должна быть выполнена операция на сервере.
`-t`, `--type` | Тип запроса.<br>Возможные значения:<ul><li>`data` — запрос над данными;</li><li>`scheme` — запрос над схемой данных;</li><li>`scan` — [скан-запрос](../../concepts/scan_query.md).</li></ul>Значение по умолчанию — `data`.
`--stats` | Режим сбора статистики.<br>Возможные значения:<ul><li>`none` — не собирать;</li><li>`basic` — собирать по основным событиям;</li><li>`full` — собирать по всем событиям.</li></ul>Значение по умолчанию — `none`.
`-s` | Включить сбор статистики в режиме `basic`.
`--tx-mode` | Указать [режим транзакций](../../concepts/transactions.md#modes) (для запросов типа `data`).<br>Возможные значения:<ul><li>`serializable-rw` — результат успешно выполненных параллельных транзакций эквивалентен определенному последовательному порядку их выполнения;</li><li>`online-ro` — каждое из чтений в транзакции читает последние на момент своего выполнения данные;</li><li>`stale-ro`  — чтения данных в транзакции возвращают результаты с возможным отставанием от актуальных (доли секунды).</li></ul>Значение по умолчанию — `serializable-rw`.
`-q`, `--query` | Текст YQL-запроса для выполнения.
`-f,` `--file` | Путь к файлу с текстом YQL-запроса для выполнения.
`--format` | Формат вывода.<br>Возможные значения:<ul><li>`pretty` (по умолчанию) — человекочитаемый формат;</li><li>`json-unicode` — вывод в формате [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}, бинарные строки закодированы в [Юникод]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Юникод){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Unicode){% endif %}, каждая строка JSON выводится в отдельной строке;</li><li>`json-unicode-array` — вывод в формате JSON, бинарные строки закодированы в Юникод, результат выводится в виде массива строк JSON, каждая строка JSON выводится в отдельной строке;</li><li>`json-base64` — вывод в формате JSON, бинарные строки закодированы в [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}, каждая строка JSON выводится в отдельной строке;</li><li>`json-base64-array` — вывод в формате JSON, бинарные строки закодированы в Base64, результат выводится в виде массива строк JSON, каждая строка JSON выводится в отдельной строке;</li><li>`csv` —вывод в формате [CSV](https://ru.wikipedia.org/wiki/CSV);</li><li>`tsv` —вывод в формате [TSV](https://ru.wikipedia.org/wiki/TSV).</li></ul>

При исполнении YQL-запроса командой `table query execute` применяются политики повторных попыток, обеспечивающие надежное исполнение запроса и продолжение обработки потока параметров на `stdin` при изменении набора партиций в таблицах, а также других стандартных ситауциях в распределенной базе данных, вызывающих кратковременную невозможность выполнения операций над частью данных.

### Параметры для работы с параметризованными запросами {#parameterized-query}

{% include [parameterized-query](../../_includes/parameterized-query.md) %}

## Примеры {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Выполните data-запрос к данным:

```bash
{{ ydb-cli }} table query execute \
  --query "SELECT season_id, episode_id, title \
  FROM episodes \
  WHERE series_id = 1 AND season_id > 1 \
  ORDER BY season_id, episode_id \
  LIMIT 3"
```

Результат:

```text
┌───────────┬────────────┬──────────────────────────────┐
| season_id | episode_id | title                        |
├───────────┼────────────┼──────────────────────────────┤
| 2         | 1          | "The Work Outing"            |
├───────────┼────────────┼──────────────────────────────┤
| 2         | 2          | "Return of the Golden Child" |
├───────────┼────────────┼──────────────────────────────┤
| 2         | 3          | "Moss and the German"        |
└───────────┴────────────┴──────────────────────────────┘
```
