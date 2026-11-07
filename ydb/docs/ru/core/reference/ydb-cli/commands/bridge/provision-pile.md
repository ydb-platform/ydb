# admin cluster bridge provision-pile

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

С помощью команды `admin cluster bridge provision-pile` можно подготовить хранилище для pile, уже указанного в `bridge_config`. Команда создаёт части этого pile в статических группах и группах хранения в режиме bridge и оставляет pile в состоянии `DISCONNECTED`, чтобы затем подключить его командой [join-pile](rejoin.md). Обычные группы хранения не изменяются.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge provision-pile [options...]
```

- `global options` — [глобальные параметры](../global-options.md) CLI;
- `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
{{ ydb-cli }} admin cluster bridge provision-pile --help
```

## Параметры подкоманды {#options}

| Имя | Описание |
| --- | --- |
| `--pile <pile>` | Имя подготавливаемого pile в состоянии `INACTIVE`. |

## Требования {#requirements}

- Преобразование кластера должно быть завершено.
- Pile и его хосты должны присутствовать в конфигурации, статические узлы должны быть запущены, а [list](list.md) должен показывать pile в состоянии `INACTIVE`.
- В pile должно быть достаточно дисков и доменов отказа для статических групп, State Storage и всех пулов в режиме bridge.
- Другая операция добавления или удаления pile не должна выполняться.

Полная процедура приведена в разделе [Добавление pile](../../../../devops/deployment-options/manual/bridge-management.md#add-pile).

## Примеры {#examples}

Подготовить зарегистрированный pile `pile-b`:

```bash
{{ ydb-cli }} -y admin cluster bridge provision-pile --pile pile-b
```

### Проверка результата {#verify}

Команда запускает ожидающую операцию `ADD`. С помощью [list](list.md) дождитесь завершения этой операции и перехода pile в состояние `DISCONNECTED`, затем подключите его:

```bash
{{ ydb-cli }} -y admin cluster bridge join-pile --pile pile-b
```

Pile синхронизируется в фоновом режиме и переходит в `SYNCHRONIZED`.
