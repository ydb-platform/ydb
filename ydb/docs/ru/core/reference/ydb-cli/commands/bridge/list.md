# admin cluster bridge list

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

С помощью команды `admin cluster bridge list` можно вывести состояния pile, поколение конфигурации, прогресс преобразования кластера и ожидающие операции с pile в [режиме bridge](../../../../concepts/bridge.md). Эта команда предназначена только для чтения и не запрашивает подтверждение.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge list [options...]
```

- `global options` — [глобальные параметры](../global-options.md) CLI;
- `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
{{ ydb-cli }} admin cluster bridge list --help
```

## Параметры подкоманды {#options}

| Имя | Описание |
| --- | --- |
| `--format <pretty, json, csv>` | Формат вывода. Допустимые значения: `pretty`, `json`, `csv`. Значение по умолчанию: `pretty`. |

## Примеры {#examples}

Вывести список pile в человекочитаемом формате:

```bash
{{ ydb-cli }} admin cluster bridge list
```

```text
Pile pile-a: PRIMARY
Pile pile-b: SYNCHRONIZED
Generation: 4
```

Во время преобразования вывод также содержит:

```text
Conversion from non-bridged cluster is in progress, piles can't be added yet
```

Во время добавления или удаления pile вывод содержит ожидающую операцию, а после неудачной попытки — её ошибку:

```text
Pending operation on pile pile-b: ADD
Last attempt has failed (being retried): <error>
```

Где `<error>` — ошибка BSC, возвращённая последней попыткой. Операция повторяется автоматически.

Вывести состояние в формате JSON:

```bash
{{ ydb-cli }} admin cluster bridge list --format json
```

```json
{
  "generation": 4,
  "piles": [
    {
      "pile_name": "pile-a",
      "state": "PRIMARY"
    },
    {
      "pile_name": "pile-b",
      "state": "SYNCHRONIZED"
    }
  ],
  "conversion_in_progress": false
}
```

Если операция с pile ожидает выполнения, JSON дополнительно содержит `pending_pile_operation` с полями `pile_name`, `kind` и `error_reason`. Используйте вывод JSON для автоматизации.

Вывести состояние в формате CSV:

```bash
{{ ydb-cli }} admin cluster bridge list --format csv
```

```csv
pile_name,state,generation
pile-a,PRIMARY,4
pile-b,SYNCHRONIZED,4
```
