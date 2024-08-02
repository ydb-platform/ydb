# Получение списка фоновых операций

С помощью подкоманды `ydb operation list` вы можете получить список фоновых операций указанного типа.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] operation list [options...] <kind>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `kind` — тип операции. Возможные значения:
  * `buildindex` — операции построения индекса;
  * `export/s3` — операции экспорта;
  * `import/s3` — операции импорта.

Посмотрите описание команды получения списка фоновых операций:

```bash
{{ ydb-cli }} operation list --help
```

## Параметры подкоманды {#options}

Имя | Описание
---|---
`-s`, `--page-size` | Количество операций на одной странице. Если список операций содержит больше строк, чем задано в параметре `--page-size`, то вывод будет разделен на несколько страниц. Для получения следующей страницы укажите параметр `--page-token`.
`-t`, `--page-token` | Токен страницы.
`--format` | Формат вывода.<br/>Значение по умолчанию — `pretty`.<br/>Возможные значения:<ul><li>`pretty` — человекочитаемый формат;</li><li>`proto-json-base64` — вывод Protobuf в формате [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}, бинарные строки закодированы в [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}.</li></ul>

## Примеры {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Получите список фоновых операций построение индекса для таблицы `series`:

```bash
ydb -p quickstart operation list \
  buildindex
```

Результат:

```text
┌───────────────────────────────────────┬───────┬─────────┬───────┬──────────┬─────────────────────┬─────────────┐
| id                                    | ready | status  | state | progress | table               | index       |
├───────────────────────────────────────┼───────┼─────────┼───────┼──────────┼─────────────────────┼─────────────┤
| ydb://buildindex/7?id=281489389055514 | true  | SUCCESS | Done  | 100.00%  | /my-database/series | idx_release |
└───────────────────────────────────────┴───────┴─────────┴───────┴──────────┴─────────────────────┴─────────────┘

Next page token: 0
```
