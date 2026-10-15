# admin cluster bridge failover

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

С помощью команды `admin cluster bridge failover` можно выполнить [аварийное отключение](../../../../concepts/bridge.md#failover) одного или нескольких недоступных pile. При необходимости можно указать pile, который станет новым `PRIMARY`.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge failover [options...]
```

* `global options` — [глобальные параметры](../global-options.md) CLI.
* `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
{{ ydb-cli }} admin cluster bridge failover --help
```

## Параметры подкоманды {#options}

#|
|| Имя | Описание ||
|| `--pile <pile>` | Имя недоступного pile. Повторите параметр, чтобы отключить несколько pile одним обновлением конфигурации. ||
|| `--new-primary <pile>` | Имя pile, который должен стать новым `PRIMARY` pile. Укажите, если недоступный pile был `PRIMARY`. ||
|#

## Требования {#requirements}

- Перед failover убедитесь, что недоступные pile остановлены или изолированы и не смогут самостоятельно возобновить обработку запросов. Не выполняйте failover независимо по разные стороны сетевого разделения; см. [восстановление после split-brain](../../../../concepts/bridge.md#split-brain).
- Оставшиеся pile должны удовлетворять требованиям к кворумам узлов и хранилища для новой конфигурации. Команда не отменяет проверок кворума.
- Если недоступен текущий `PRIMARY`, обязательно укажите `--new-primary` и выберите pile в состоянии `SYNCHRONIZED`. При отсутствии `--new-primary` или выборе pile в состоянии, отличном от `SYNCHRONIZED`, команда вернёт ошибку без каких‑либо изменений.
- Кластер не перейдёт в невалидное состояние: при нарушении требований команда ничего не изменяет и сообщает об ошибке.
- Если pile не вышел из строя, но его нужно отключить, используйте [плановое отключение](../../../../concepts/bridge.md#takedown) — команду [`takedown`](takedown.md).
- Команда требует прав администратора кластера. Если включена обязательная аутентификация, то в аварийном состоянии кластера аутентификация по логину и паролю не может быть использована — используйте аутентификацию по клиентским сертификатам. Подробнее см. [{#T}](../../../../devops/deployment-options/manual/bridge-management.md#emergency-auth).

## Примеры {#examples}

Выполнение аварийного отключения для недоступного pile под названием `pile-a`:

```bash
{{ ydb-cli }} admin cluster bridge failover --pile pile-a
```

Выполнение аварийного отключения для недоступного `PRIMARY` pile и назначение новым `PRIMARY` синхронизированного pile:

```bash
{{ ydb-cli }} admin cluster bridge failover --pile pile-a --new-primary pile-b
```

Аварийное отключение двух недоступных pile в кластере из трёх pile с продолжением работы на оставшемся синхронизированном pile:

```bash
{{ ydb-cli }} admin cluster bridge failover --pile pile-a --pile pile-b --new-primary pile-c
```

Оба pile переводятся в `DISCONNECTED` одним обновлением конфигурации. Отдельные команды могут завершиться ошибкой, поскольку первое обновление всё ещё требует кворума в другом недоступном pile.

### Проверка результата {#verify}

С помощью команды [list](list.md) проверьте, что недоступный pile переведён в состояние `DISCONNECTED` и (если был указан `--new-primary`) выбран новый `PRIMARY` pile :

```bash
{{ ydb-cli }} admin cluster bridge list

pile-a: DISCONNECTED
pile-b: PRIMARY
```
