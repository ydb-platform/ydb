# admin cluster bridge switchover

С помощью команды `admin cluster bridge switchover` можно выполнить плановую, плавную смену роли PRIMARY на указанный pile. Подробнее см. [описание сценария](../../../../concepts/bridge.md#switchover).

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

Переключение роли `PRIMARY` на pile в состоянии `SYNCHRONIZED`:

```bash
ydb admin cluster bridge switchover --new-primary pile-b
```

## Проверка результата {#verify}

Убедитесь, что спустя некоторое время (в течение нескольких минут) роли pile изменились корректно, с помощью команды [list](list.md):

```bash
ydb admin cluster bridge list

pile-a: SYNCHRONIZED
pile-b: PRIMARY
```
