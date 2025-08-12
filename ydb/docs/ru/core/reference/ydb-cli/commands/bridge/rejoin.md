# admin cluster bridge rejoin

С помощью команды `admin cluster bridge rejoin` вы можете [вернуть](../../../../concepts/bridge.md#rejoin) указанный пайл в кластер после обслуживания или восстановления.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
ydb [global options...] admin cluster bridge rejoin [options...]
```

* `global options` — глобальные параметры.
* `options` — [параметры подкоманды](#options).

Посмотрите справку по команде:

```bash
ydb admin cluster bridge rejoin --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `--pile <pile>` | Имя пайла, который нужно вернуть в кластер. ||
|#

## Требования {#requirements}

- Пайл перед возвращением должен находиться в состоянии `DISCONNECTED`.
- После выполнения команды ожидается переход в `NOT_SYNCHRONIZED` и последующая автоматическая синхронизация до `SYNCHRONIZED`.

## Примеры {#examples}

Вернуть DISCONNECTED пайл в кластер:

```bash
ydb admin cluster bridge rejoin --pile pile-a
```

## Проверка результата {#verify}

Сразу после выполнения команды ожидается переход в состояние `NOT_SYNCHRONIZED`:

```bash
ydb admin cluster bridge list

pile-a: NOT_SYNCHRONIZED
pile-b: PRIMARY
```

Спустя некоторое время, после завершения синхронизации, пайл станет `SYNCHRONIZED`:

```bash
ydb admin cluster bridge list

pile-a: SYNCHRONIZED
pile-b: PRIMARY
```
