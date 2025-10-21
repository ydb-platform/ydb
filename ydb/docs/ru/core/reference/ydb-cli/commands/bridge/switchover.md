# admin cluster bridge switchover

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

С помощью команды `admin cluster bridge switchover` выполняется плавное, плановое переключение указанного pile в состояние `PRIMARY` через промежуточное состояние `PROMOTED`. Подробнее см. [описание сценария](../../../../concepts/bridge.md#switchover).

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge switchover [options...]
```

* `global options` — [глобальные параметры](../global-options.md) CLI.
* `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
{{ ydb-cli }} admin cluster bridge switchover --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `--new-primary <pile>` | Имя pile, который должен стать новым PRIMARY. ||
|#

## Требования {#requirements}

- Целевой pile должен находиться в состоянии `SYNCHRONIZED`.

## Примеры {#examples}

Переключение pile `pile-b` из состояния `SYNCHRONIZED` в состояние `PRIMARY` через промежуточное состояние `PROMOTED`:

```bash
{{ ydb-cli }} admin cluster bridge switchover --new-primary pile-b
```

## Проверка результата {#verify}

Убедитесь, что спустя некоторое время (через несколько минут) состояния pile изменились корректно, с помощью команды [list](list.md):

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: SYNCHRONIZED
pile-b: PRIMARY
```
