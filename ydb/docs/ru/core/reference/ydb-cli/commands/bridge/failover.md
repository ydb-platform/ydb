# admin cluster bridge failover

С помощью команды `admin cluster bridge failover` вы можете выполнить [аварийное отключение](../../../../concepts/bridge.md#failover) пайла, когда он недоступен. При необходимости можно указать пайл, который станет новым PRIMARY.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
ydb [global options...] admin cluster bridge failover [options...]
```

* `global options` — глобальные параметры.
* `options` — [параметры подкоманды](#options).

Посмотрите справку по команде:

```bash
ydb admin cluster bridge failover --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `--pile <pile>` | Имя недоступного пайла. ||
|| `--new-primary <pile>` | Имя пайла, который должен стать новым PRIMARY. Укажите, если недоступный пайл был PRIMARY. ||
|#

## Требования {#requirements}

- Команда применяется, когда один из пайлов недоступен.
- Если недоступен текущий `PRIMARY`, укажите `--new-primary` и выбирайте пайл в состоянии `SYNCHRONIZED` (переключение на `DISCONNECTED`/`NOT_SYNCHRONIZED`/`SUSPENDED` невозможно).

## Примеры {#examples}

Выполните аварийное отключение для недоступного `pile-a`

```bash
ydb admin cluster bridge failover --pile pile-a
``` 

Выполните аварийное отключение для недоступного PRIMARY пайла и назначьте новым PRIMARY синхронизированный пайл:

```bash
ydb admin cluster bridge failover --pile pile-a --new-primary pile-b
``` 

## Проверка результата {#verify}

Проверьте, что недоступный пайл переведён в состояние DISCONNECTED и (если указывали `--new-primary`) выбран новый PRIMARY:

```bash
ydb admin cluster bridge list

pile-a: DISCONNECTED
pile-b: PRIMARY
```
