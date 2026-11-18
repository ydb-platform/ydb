# admin cluster bridge join-pile (rejoin)

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

С помощью команды `admin cluster bridge join-pile` можно подключить подготовленный pile или [вернуть](../../../../concepts/bridge.md#rejoin) pile в кластер после обслуживания или восстановления. `rejoin` — псевдоним `join-pile`. Pile переходит из состояния `DISCONNECTED` в `NOT_SYNCHRONIZED`, синхронизируется в фоновом режиме, а затем автоматически становится `SYNCHRONIZED`.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge join-pile [options...]
```

- `global options` — [глобальные параметры](../global-options.md) CLI;
- `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
{{ ydb-cli }} admin cluster bridge join-pile --help
```

## Параметры подкоманды {#options}

| Имя | Описание |
| --- | --- |
| `--pile <pile>` | Имя pile, который нужно подключить или вернуть в кластер. |

## Требования {#requirements}

- Pile должен находиться в состоянии `DISCONNECTED`. Новый pile переходит в это состояние после завершения [provision-pile](provision-pile.md).

## Примеры {#examples}

Подключить подготовленный pile `pile-b`:

```bash
{{ ydb-cli }} -y admin cluster bridge join-pile --pile pile-b
```

Вернуть pile `pile-a` после обслуживания с помощью псевдонима:

```bash
{{ ydb-cli }} -y admin cluster bridge rejoin --pile pile-a
```

### Проверка результата {#verify}

Сразу после выполнения команды ожидается переход pile в состояние `NOT_SYNCHRONIZED`. Проверьте результат с помощью команды [list](list.md). После завершения синхронизации pile переходит в состояние `SYNCHRONIZED`.
