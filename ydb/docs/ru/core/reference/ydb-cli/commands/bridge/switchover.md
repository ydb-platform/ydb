# admin cluster bridge switchover

С помощью команды `admin cluster bridge switchover` выполняется плавное, плановое переключение указанного pile в состояние `PRIMARY` через промежуточное состояние `PROMOTED`. Подробнее см. [описание сценария](../../../../concepts/bridge.md#switchover).

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
ydb [global options...] admin cluster bridge switchover [options...]
```

* `global options` — [глобальные параметры](../global-options.md) CLI.
* `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
ydb admin cluster bridge switchover --help
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
ydb admin cluster bridge switchover --new-primary pile-b
```

## Проверка результата {#verify}

Убедитесь, что спустя некоторое время (через несколько минут) состояния pile изменились корректно, с помощью команды [list](list.md):

```bash
ydb admin cluster bridge list

pile-a: SYNCHRONIZED
pile-b: PRIMARY
```
