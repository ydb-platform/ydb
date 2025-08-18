# admin cluster bridge failover

С помощью команды `admin cluster bridge failover` можно выполнить [аварийное отключение](../../../../concepts/bridge.md#failover) пайла, когда он недоступен. При необходимости можно указать пайл, который станет новым `PRIMARY`.

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
|| `--pile <pile>` | Имя недоступного пайла. ||
|| `--new-primary <pile>` | Имя пайла, который должен стать новым `PRIMARY` пайлом. Укажите, если недоступный пайл был `PRIMARY`. ||
|#

## Требования {#requirements}

- Команда применяется, когда один из пайлов недоступен.
- Если недоступен текущий `PRIMARY`, укажите `--new-primary` и выбирайте пайл в состоянии `SYNCHRONIZED` (переключение на `DISCONNECTED`, `NOT_SYNCHRONIZED` или `SUSPENDED` невозможно).

## Примеры {#examples}

Выполнение аварийного отключения для недоступного пайла под названием `pile-a`:

```bash
ydb admin cluster bridge failover --pile pile-a
``` 

Выполнение аварийного отключения для недоступного `PRIMARY`-пайла и назначение новым `PRIMARY` синхронизированного пайла:

```bash
ydb admin cluster bridge failover --pile pile-a --new-primary pile-b
``` 

### Проверка результата {#verify}

Проверьте, что недоступный пайл переведён в состояние `DISCONNECTED` и (если указан `--new-primary`) выбран новый `PRIMARY` пайл:

```bash
ydb admin cluster bridge list

pile-a: DISCONNECTED
pile-b: PRIMARY
```
