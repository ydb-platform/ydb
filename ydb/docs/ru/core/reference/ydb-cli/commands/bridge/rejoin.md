# admin cluster bridge rejoin

С помощью команды `admin cluster bridge rejoin` можно [вернуть](../../../../concepts/bridge.md#rejoin) указанный пайл в кластер после обслуживания или восстановления.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
ydb [global options...] admin cluster bridge rejoin [options...]
```

* `global options` — глобальные параметры.
* `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
ydb admin cluster bridge rejoin --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `--pile <pile>` | Имя пайла, который нужно вернуть в кластер. ||
|#

## Требования {#requirements}

- Пайл перед возвращением должен находиться в состоянии `DISCONNECTED` перед возвращением.
- После выполнения команды ожидается переход в состояние `NOT_SYNCHRONIZED` и последующая автоматическая синхронизация до состояния `SYNCHRONIZED`.

## Примеры {#examples}

Возврат пайла в состоянии `DISCONNECTED` в кластер:

```bash
ydb admin cluster bridge rejoin --pile pile-a
```

## Проверка результата {#verify}

Сразу после выполнения команды ожидается переход пайла в состояние `NOT_SYNCHRONIZED`:

```bash
ydb admin cluster bridge list

pile-a: NOT_SYNCHRONIZED
pile-b: PRIMARY
```

После завершения синхронизации пайл переходит в состояние `SYNCHRONIZED`:

```bash
ydb admin cluster bridge list

pile-a: SYNCHRONIZED
pile-b: PRIMARY
```
