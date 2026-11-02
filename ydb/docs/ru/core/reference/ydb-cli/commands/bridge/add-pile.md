# admin cluster bridge add-pile

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

С помощью команды `admin cluster bridge add-pile` можно выделить ресурсы хранения для pile, уже зарегистрированного в `bridge_config`. Команда создаёт части этого pile в группах хранения в режиме bridge и оставляет pile в состоянии `DISCONNECTED`, чтобы затем вернуть его командой [rejoin](rejoin.md).

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge add-pile [options...]
```

- `global options` — [глобальные параметры](../global-options.md) CLI;
- `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
{{ ydb-cli }} admin cluster bridge add-pile --help
```

## Параметры подкоманды {#options}

| Имя | Описание |
| --- | --- |
| `--pile <pile>` | Имя добавляемого pile в состоянии `INACTIVE`. |

## Требования {#requirements}

- Преобразование кластера должно быть завершено.
- Pile и его хосты должны присутствовать в конфигурации, статические узлы должны быть запущены, а [list](list.md) должен показывать pile в состоянии `INACTIVE`.
- Списки узлов `system_tablets` должны включать узлы нового pile, чтобы системные таблетки могли запускаться там после failover.
- Не должно быть другой ожидающей операции добавления или удаления pile.
- В pile должно быть достаточно дисков и доменов отказа для статических групп, State Storage и всех пулов в режиме bridge.

Полная процедура приведена в разделе [Регистрация и добавление pile](../../../../devops/deployment-options/manual/bridge-management.md#add-pile).

## Примеры {#examples}

Добавить зарегистрированный pile `pile-b`:

```bash
{{ ydb-cli }} -y admin cluster bridge add-pile --pile pile-b
```

Команда запускает ожидающую операцию `ADD`. При недостатке места она завершается ошибкой без изменения конфигурации. Временная ошибка BSC сохраняется в ожидающей операции и повторяется. Сразу после регистрации хостов `group allocation failed` также может означать, что BSC ещё не обнаружил их; повторите команду через несколько секунд.

### Проверка результата {#verify}

Проверьте операцию и состояние pile:

```bash
{{ ydb-cli }} admin cluster bridge list
```

Дождитесь завершения операции `ADD` и перехода pile в `DISCONNECTED`, затем верните его:

```bash
{{ ydb-cli }} -y admin cluster bridge rejoin --pile pile-b
```

Pile синхронизируется в фоновом режиме и переходит в `SYNCHRONIZED`. Застрявшую операцию `ADD` можно отменить командой [remove-pile](remove-pile.md).
