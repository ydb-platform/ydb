# admin cluster bridge remove-pile

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

С помощью команды `admin cluster bridge remove-pile` можно удалить части отключённого pile во всех группах хранения вместе со всеми их данными и перевести pile в состояние `INACTIVE`. Операция необратима.

{% include [danger-warning](../_includes/danger-warning.md) %}

{% note warning %}

Удаление pile удаляет все данные в его частях групп хранения. Перед выполнением команды убедитесь, что ни одна обычная группа не содержит VDisk в этом pile и ни одна группа в режиме bridge не зависит исключительно от своей части в нём.

{% endnote %}

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge remove-pile [options...]
```

- `global options` — [глобальные параметры](../global-options.md) CLI;
- `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
{{ ydb-cli }} admin cluster bridge remove-pile --help
```

## Параметры подкоманды {#options}

| Имя | Описание |
| --- | --- |
| `--pile <pile>` | Имя удаляемого pile в состоянии `DISCONNECTED`. |

## Требования {#requirements}

- Pile должен быть `DISCONNECTED`, а его узлы — остановлены. Если он `PRIMARY`, сначала выполните switchover, затем takedown.
- Ни одна обычная группа не должна содержать VDisk в этом pile.
- Ни одна группа не должна зависеть исключительно от своей части в этом pile. Дождитесь синхронизации преобразования пула или сохраните другой pile при переводе пула в обычный режим.
- Не должно быть другой ожидающей операции добавления или удаления pile.

Команда также отменяет застрявшую ожидающую операцию `ADD` для указанного pile. Проверки безопасности данных и полная процедура описаны в разделе [Удаление pile](../../../../devops/deployment-options/manual/bridge-management.md#remove-pile).

## Примеры {#examples}

Удалить отключённый pile `pile-b`:

```bash
{{ ydb-cli }} -y admin cluster bridge remove-pile --pile pile-b
```

Без TTY пропущенный глобальный параметр `-y` приводит к автоматическому отклонению подтверждения без сообщения и завершению с кодом 1.

### Проверка результата {#verify}

Проверьте ожидающую операцию:

```bash
{{ ydb-cli }} admin cluster bridge list
```

BSC удаляет части и данные pile во время ожидающей операции `REMOVE`. Дождитесь её завершения и перехода pile в `INACTIVE`, прежде чем удалять pile и его хосты из `bridge_config`, `hosts` и `system_tablets`.

Удалённый pile можно позже зарегистрировать и [добавить](add-pile.md) снова на новых или тех же хостах.
