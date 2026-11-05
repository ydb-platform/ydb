# admin cluster bridge destroy-pile

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

С помощью команды `admin cluster bridge destroy-pile` можно удалить части отключённого pile в группах хранения вместе со всеми их данными и перевести pile в состояние `INACTIVE`. Операция необратима.

{% include [danger-warning](../_includes/danger-warning.md) %}

{% note warning %}

Перед выполнением команды убедитесь, что ни одна обычная группа не содержит VDisk в этом pile и ни одна группа в режиме bridge не зависит только от своей части в нём.

{% endnote %}

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge destroy-pile [options...]
```

- `global options` — [глобальные параметры](../global-options.md) CLI;
- `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
{{ ydb-cli }} admin cluster bridge destroy-pile --help
```

## Параметры подкоманды {#options}

| Имя | Описание |
| --- | --- |
| `--pile <pile>` | Имя удаляемого pile в состоянии `DISCONNECTED`. |

## Требования {#requirements}

- Pile должен быть `DISCONNECTED`, а его узлы — остановлены. Если он `PRIMARY`, сначала выполните switchover, затем takedown.
- Ни одна обычная группа не должна содержать VDisk в этом pile.
- Ни одна группа в режиме bridge не должна зависеть только от своей части в этом pile.
- Другая операция добавления или удаления pile не должна выполняться, за исключением ожидающей операции `ADD` для того же pile, которую эта команда отменяет.

Полная процедура приведена в разделе [Удаление pile](../../../../devops/deployment-options/manual/bridge-management.md#remove-pile).

## Примеры {#examples}

Удалить отключённый pile `pile-b`:

```bash
{{ ydb-cli }} -y admin cluster bridge destroy-pile --pile pile-b
```

Без глобального параметра `-y` команда запрашивает подтверждение перед удалением данных.

### Проверка результата {#verify}

Команда запускает ожидающую операцию `REMOVE`. С помощью [list](list.md) дождитесь завершения этой операции и перехода pile в состояние `INACTIVE`, после чего pile и его хосты можно удалить из конфигурации. Удалённый pile можно позже зарегистрировать и [подготовить](provision-pile.md) снова на новых или тех же хостах.
