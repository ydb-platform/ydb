# admin cluster bridge convert

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

С помощью команды `admin cluster bridge convert` можно преобразовать обычный кластер в [кластер bridge](../../../../concepts/bridge.md) с одним pile. Существующие данные сохраняются. Преобразование необратимо.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge convert [options...]
```

- `global options` — [глобальные параметры](../global-options.md) CLI;
- `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
{{ ydb-cli }} admin cluster bridge convert --help
```

## Параметры подкоманды {#options}

| Имя | Описание |
| --- | --- |
| `--skip-storage-pool-conversion` | Оставить существующие пулы и группы хранения баз данных обычными. Статическое и системное хранилище всё равно преобразуется. Позже режим хранения каждой базы данных можно изменить отдельно командой [`set-database-mode`](set-database-mode.md). Этот параметр требует `bridge_config.enable_non_uniform_storage: true`. |

## Требования {#requirements}

- Должны использоваться конфигурация V2 и автоматическое управление.
- На каждом статическом и динамическом узле должна работать поддерживающая сборка, а узлы должны быть перезапущены с `bridge_config`, содержащим один pile.
- Конфигурация должна содержать ровно один pile.
- С `--skip-storage-pool-conversion` параметр `bridge_config.enable_non_uniform_storage` должен быть `true`. При преобразовании по умолчанию он не должен быть задан до завершения преобразования.

Полная процедура приведена в разделе [Преобразование кластера](../../../../devops/deployment-options/manual/bridge-management.md#convert).

## Примеры {#examples}

Преобразовать кластер и перевести все существующие пулы баз данных в режим bridge:

```bash
{{ ydb-cli }} -y admin cluster bridge convert
```

Преобразовать системное хранилище и оставить существующие пулы баз данных обычными:

```bash
{{ ydb-cli }} -y admin cluster bridge convert --skip-storage-pool-conversion
```

### Проверка результата {#verify}

С помощью [list](list.md) дождитесь завершения преобразования. Пока вывод содержит `Conversion from non-bridged cluster is in progress, piles can't be added yet`, список pile изменять нельзя. После завершения единственный pile отображается как `PRIMARY`.
