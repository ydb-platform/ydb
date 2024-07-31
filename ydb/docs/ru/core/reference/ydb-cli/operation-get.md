# Получение статуса фоновой операции

С помощью подкоманды `ydb operation get` вы можете получить статус указанной фоновой операции.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] operation get [options...] <id>
```

* `global options` — [глобальные параметры](commands/global-options.md).
* `options` — [параметры подкоманды](#options).
* `id` — идентификатор фоновой операции. Идентификатор содержит символы, которые могут быть интерпретированы вашей командной оболочкой. При необходимости используйте экранирование, например `'<id>'` для bash.

Посмотрите описание команды получения статуса фоновой операции:

```bash
{{ ydb-cli }} operation get --help
```

## Параметры подкоманды {#options}

Имя | Описание
---|---
`--format` | Формат вывода.<br/>Значение по умолчанию — `pretty`.<br/>Возможные значения:<ul><li>`pretty` — человекочитаемый формат;</li><li>`proto-json-base64` — вывод Protobuf в формате [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}, бинарные строки закодированы в [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}.</li></ul>

## Примеры {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Получите статус фоновой операции с идентификатором `ydb://buildindex/7?id=281489389055514`:

```bash
ydb -p quickstart operation get \
  'ydb://buildindex/7?id=281489389055514'
```

Результат:

```text
┌───────────────────────────────────────┬───────┬─────────┬───────┬──────────┬─────────────────────┬─────────────┐
| id                                    | ready | status  | state | progress | table               | index       |
├───────────────────────────────────────┼───────┼─────────┼───────┼──────────┼─────────────────────┼─────────────┤
| ydb://buildindex/7?id=281489389055514 | true  | SUCCESS | Done  | 100.00%  | /my-database/series | idx_release |
└───────────────────────────────────────┴───────┴─────────┴───────┴──────────┴─────────────────────┴─────────────┘
```
