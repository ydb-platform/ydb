# admin cluster bridge failover

С помощью команды `admin cluster bridge failover` можно выполнить [аварийное отключение](../../../../concepts/bridge.md#failover) pile, когда он недоступен. При необходимости можно указать pile, который станет новым `PRIMARY`.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
ydb [global options...] admin cluster bridge failover [options...]
```

* `global options` — [глобальные параметры](../global-options.md) CLI.
* `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
ydb admin cluster bridge failover --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `--pile <pile>` | Имя недоступного pile. ||
|| `--new-primary <pile>` | Имя pile, который должен стать новым `PRIMARY` pile. Укажите, если недоступный pile был `PRIMARY`. ||
|#

## Требования {#requirements}

- Если недоступен текущий `PRIMARY`, обязательно укажите `--new-primary` и выберите pile в состоянии `SYNCHRONIZED`. При отсутствии `--new-primary` или выборе pile в состоянии, отличном от `SYNCHRONIZED`, команда вернёт ошибку без каких‑либо изменений.
- Кластер не перейдёт в невалидное состояние: при нарушении требований команда ничего не изменяет и сообщает об ошибке.
- Если pile не вышел из строя, но его нужно отключить, используйте [плановое отключение](../../../../concepts/bridge.md#takedown) — команду [`takedown`](takedown.md).

## Примеры {#examples}

Выполнение аварийного отключения для недоступного pile под названием `pile-a`:

```bash
ydb admin cluster bridge failover --pile pile-a
```

Выполнение аварийного отключения для недоступного `PRIMARY`-pile и назначение новым `PRIMARY` синхронизированного pile:

```bash
ydb admin cluster bridge failover --pile pile-a --new-primary pile-b
```

### Проверка результата {#verify}

Проверьте, что недоступный pile переведён в состояние `DISCONNECTED` и (если указан `--new-primary`) выбран новый `PRIMARY` pile с помощью команды [list](list.md):

```bash
ydb admin cluster bridge list

pile-a: DISCONNECTED
pile-b: PRIMARY
```
