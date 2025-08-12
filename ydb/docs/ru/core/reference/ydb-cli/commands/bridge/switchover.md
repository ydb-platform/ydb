# admin cluster bridge switchover

С помощью команды `admin cluster bridge switchover` вы можете выполнить плановую, плавную смену PRIMARY на указанный пайл. См. [описание сценария](../../../../concepts/bridge.md#switchover).

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
ydb [global options...] admin cluster bridge switchover [options...]
```

* `global options` — глобальные параметры.
* `options` — [параметры подкоманды](#options).

Посмотрите справку по команде:

```bash
ydb admin cluster bridge switchover --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `--new-primary <pile>` | Имя пайла, который должен стать новым PRIMARY. ||
|#

## Требования {#requirements}

- Целевой пайл должен быть в состоянии `SYNCHRONIZED`.
- Текущая пара пайлов должна быть в одном из состояний: `PRIMARY/SYNCHRONIZED` или `SYNCHRONIZED/PRIMARY`.

## Примеры {#examples}

Переключить PRIMARY на SYNCHRONIZED пайл:

```bash
ydb admin cluster bridge switchover --new-primary pile-b
```

## Проверка результата {#verify}

Проверьте, что роли пайлов спустя некоторое время поменялись корректно:

```bash
ydb admin cluster bridge list

pile-a: SYNCHRONIZED
pile-b: PRIMARY
```
