# admin cluster bridge switchover

С помощью команды `admin cluster bridge switchover` можно выполнить плановую, плавную смену роли PRIMARY на указанный пайл. Подробнее см. [описание сценария](../../../../concepts/bridge.md#switchover).

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
|| `--new-primary <pile>` | Имя пайла, который должен стать новым PRIMARY. ||
|#

## Требования {#requirements}

- Целевой пайл должен находиться в состоянии `SYNCHRONIZED`.

## Примеры {#examples}

Переключение роли `PRIMARY` на пайл в состоянии `SYNCHRONIZED`:

```bash
ydb admin cluster bridge switchover --new-primary pile-b
```

## Проверка результата {#verify}

Убедитесь, что спустя некоторое время роли пайлов изменились корректно:

```bash
ydb admin cluster bridge list

pile-a: SYNCHRONIZED
pile-b: PRIMARY
```
