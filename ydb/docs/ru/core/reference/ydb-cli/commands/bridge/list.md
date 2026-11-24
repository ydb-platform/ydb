# admin cluster bridge list

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

С помощью команды `admin cluster bridge list` можно вывести состояния pile, ход преобразования и отката кластера, выполняющиеся операции с pile, а также состояние синхронизации баз данных и групп хранения в [режиме bridge](../../../../concepts/bridge.md). Эта команда только читает состояние и не запрашивает подтверждение.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge list [piles|databases|groups] [options...]
```

- `global options` — [глобальные параметры](../global-options.md) CLI;
- `piles|databases|groups` — [представление](#views), по умолчанию `piles`;
- `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
{{ ydb-cli }} admin cluster bridge list --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `--format <pretty, json, csv>` | Формат вывода. Допустимые значения: `pretty`, `json`, `csv`. Значение по умолчанию: `pretty`. ||
|| `--detailed` | Показать дополнительные сведения: количество баз данных для `piles`, счётчики групп и последнюю ошибку для `databases`, физические идентификаторы, фазы и последнюю ошибку для `groups`. В формате JSON представления `databases` и `groups` включают доступные подробности по умолчанию. ||
|| `--pile <name>` | Показать только указанный pile. Допускается пустое имя. ||
|| `--database-path <path>` | Только для `databases` и `groups`: выбрать базу данных по абсолютному пути. Для serverless-базы данных выбирается её коммунальное хранилище. ||
|| `--not-synced` | Только для `databases` и `groups`: показать только незавершённую требуемую синхронизацию, включая неизвестные состояния. ||
|| `--mode <mode>` | Только для `databases`: фильтр по режиму всех пулов базы данных: `bridged`, `ordinary` или `mixed`. ||
|| `--pool <name>` | Только для `groups`: выбрать пул хранения с уникальным именем. Несовместимо с `--database-path` и `--pool-id`. ||
|| `--pool-id <box:pool>` | Только для `groups`: выбрать пул хранения по идентификаторам `BOX:POOL`. Несовместимо с `--database-path` и `--pool`. ||
|#

## Представления {#views}

### piles {#piles}

Состояние каждого pile, ход преобразования или отката и ожидающие операции с pile. Это представление используется по умолчанию. С `--detailed` дополнительно показывается сводка синхронизации баз данных по pile.

### databases {#databases}

Режим хранения и сводка синхронизации групп для каждой базы данных и pile. Режим `mixed` означает, что у базы данных есть как обычные пулы хранения, так и пулы в режиме bridge. С `--detailed` показываются счётчики синхронизации и последняя ошибка. Состояния вывода и критерии завершения описаны в разделе [Проверка переключения режима хранения](../../../../devops/deployment-options/manual/bridge-management.md#verify-database-mode).

### groups {#groups}

Состояние синхронизации частей групп хранения по pile. С `--detailed` показываются идентификаторы физических групп, фазы синхронизации и последняя ошибка. Состояния вывода и критерии завершения описаны в разделе [Проверка переключения режима хранения](../../../../devops/deployment-options/manual/bridge-management.md#verify-database-mode).

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

Во время преобразования вывод также содержит `Conversion from non-bridged cluster is in progress, piles can't be added yet`. Во время [отката в обычный режим](rollback.md) выводится `Rollback: storage in progress`, а после завершения отката хранилища — `Bridge configuration still in use`, пока узлы не перезапущены без `bridge_config`. Во время подготовки или удаления pile выводится выполняющаяся операция и, после неудачной попытки, её ошибка:

```text
Pending operation on pile pile-b: ADD
Last attempt has failed (being retried): <error>
```

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

Если операция с pile выполняется, вывод JSON дополнительно содержит `pending_pile_operation` с полями `pile_name`, `kind` и `error_reason`.

Вывести состояние в формате CSV:

```bash
{{ ydb-cli }} admin cluster bridge list --format csv
```

```csv
pile_name,state,generation
pile-a,PRIMARY,4
pile-b,SYNCHRONIZED,4
```

Показать подробную сводку синхронизации базы данных `/Root/db1` по pile:

```bash
{{ ydb-cli }} admin cluster bridge list databases --database-path /Root/db1 --detailed
```

Показать только части групп этой базы данных с незавершённой синхронизацией:

```bash
{{ ydb-cli }} admin cluster bridge list groups --database-path /Root/db1 --not-synced
```

Пустой результат с `--not-synced` означает, что среди выбранных данных нет незавершённой требуемой синхронизации. Для просмотра всех частей, включая уже синхронизированные и не требующие синхронизации, не указывайте этот параметр.
