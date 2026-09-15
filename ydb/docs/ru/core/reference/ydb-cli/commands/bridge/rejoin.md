# admin cluster bridge rejoin

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

С помощью команды `admin cluster bridge rejoin` можно [вернуть](../../../../concepts/bridge.md#rejoin) указанный pile в кластер после обслуживания или восстановления. После выполнения команды ожидается переход pile из состояния `DISCONNECTED` в состояние `NOT_SYNCHRONIZED`, последующая автоматическая синхронизация и переход в состояние `SYNCHRONIZED`.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge rejoin [options...]
```

* `global options` — глобальные параметры.
* `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
{{ ydb-cli }} admin cluster bridge rejoin --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `--pile <pile>` | Имя pile, который нужно вернуть в кластер. ||
|#

## Требования {#requirements}

- Pile перед возвращением должен быть в состоянии `DISCONNECTED`.

## Примеры {#examples}

Возврат pile `pile-a` из состояния `DISCONNECTED`:

```bash
{{ ydb-cli }} admin cluster bridge rejoin --pile pile-a
```

## Проверка результата {#verify}

Сразу после выполнения команды ожидается переход pile в состояние `NOT_SYNCHRONIZED`. Проверьте результат с помощью команды [list](list.md):

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: NOT_SYNCHRONIZED
pile-b: PRIMARY
```

После завершения синхронизации pile переходит в состояние `SYNCHRONIZED`:

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: SYNCHRONIZED
pile-b: PRIMARY
```
