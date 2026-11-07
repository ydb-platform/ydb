# admin cluster bridge set-database-mode

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

С помощью команды `admin cluster bridge set-database-mode` можно переключить все пулы хранения базы данных между режимом bridge и обычным режимом. Существующие данные сохраняются. Репликация или удаление избыточных копий продолжается в фоновом режиме после завершения команды.

{% include [danger-warning](../_includes/danger-warning.md) %}

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge set-database-mode <mode> --target-database <path> [options...]
```

- `global options` — [глобальные параметры](../global-options.md) CLI;
- `<mode>` — `bridged` для включения репликации bridge или `ordinary` для её отключения;
- `<path>` — абсолютный путь к изменяемой базе данных;
- `options` — [параметры подкоманды](#options).

Просмотр справки по команде:

```bash
{{ ydb-cli }} admin cluster bridge set-database-mode --help
```

## Параметры подкоманды {#options}

| Имя | Описание |
| --- | --- |
| `--target-database <path>` | Обязательный абсолютный путь к изменяемой базе данных. Не изменяет базу данных подключения и аутентификации, заданную глобальным параметром `--database`. |
| `--retain-pile <pile>` | В режиме `ordinary` сохранить данные в указанном pile, в котором уже должны быть синхронизированные копии. Если параметр не указан, сохраняемые копии выбираются автоматически и могут находиться в разных pile для разных групп. Этот параметр не перемещает данные в пулах, которые уже используют обычный режим. |

## Требования {#requirements}

- Кластер должен быть преобразован в режим bridge, и преобразование должно быть завершено.
- Должно быть установлено `bridge_config.enable_non_uniform_storage: true`.
- Добавление или удаление pile не должно выполняться.
- При переключении в `bridged` каждая существующая группа должна находиться внутри одного pile. Группы, охватывающие несколько pile, нельзя преобразовать напрямую.
- Параметр `--retain-pile` можно использовать только с `ordinary`.

Для serverless-базы данных укажите её общую базу данных в `--target-database`. Изменение применяется к общей базе и всем её serverless-базам.

Полная процедура приведена в разделе [Переключение режима хранения базы данных](../../../../devops/deployment-options/manual/bridge-management.md#database-mode).

## Примеры {#examples}

Переключить базу данных `/Root/db1` в режим bridge:

```bash
{{ ydb-cli }} -y admin cluster bridge set-database-mode bridged --target-database /Root/db1
```

Переключить базу данных `/Root/db1` в обычный режим и сохранить данные в `pile-a`:

```bash
{{ ydb-cli }} -y admin cluster bridge set-database-mode ordinary --target-database /Root/db1 --retain-pile pile-a
```
