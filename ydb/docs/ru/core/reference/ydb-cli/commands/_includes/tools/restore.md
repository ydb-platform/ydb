# Восстановление данных из дампа

С помощью подкоманды `tools restore` вы можете восстановить данные из созданного ранее дампа.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] tools restore [options...]
```

* `global options` — [глобальные параметры](../../../commands/global-options.md).
* `options` — [параметры подкоманды](#options).

Посмотрите описание команды для восстановления данных из дампа:

```bash
{{ ydb-cli }} tools restore --help
```

## Параметры подкоманды {#options}

Имя параметра | Описание параметра
---|---
`-p`<br/>`--path` | Путь к директории или таблице, дамп которых нужно сделать.<br/>Значение по умолчанию — `.`, будет сделан полный дамп БД.
`-o`<br/>`--output` | Обязательный параметр.<br/>Путь на локальной файловой системе, по которому будут размещены объекты дампа. Директория для дампа не должна существовать, либо быть пустой.
`--scheme-only` | Сделать дамп только схемы БД. Возможные значения:<br/><ul><li>`0` — нет;</li><li>`1` — да.</li>Значение по умолчанию — `0`.
`--avoid-copy` | Избегать копирования. Возможные значения:<br/><ul><li>`0` — нет;</li><li>`1` — да.</li>Значение по умолчанию — `0`.
`--save-partial-result` | Сохранять результаты неполного дампа.  Возможные значения:<br/><ul><li>`0` — нет;</li><li>`1` — да.</li>Значение по умолчанию — `0`.
`--consistency-level` | Уровень консистентности. Возможные значения:<br/><ul><li>`database` — консистентность на уровне базы данных;</li><li>`table` — консистентность на уровне таблицы.</li>Значение по умолчанию — `database`.

`-p <значение>`<br/>`--path <значение>` | Обязательный параметр.<br/>Путь в базе данных, по которому будет восстановлена директория или таблица.
`-o <значение>`<br/>`--output <значение>` | Обязательный параметр.<br/>Путь на локальной файловой системе, по которому размещены объекты дампа.
`--dry-run` | Не восстанавливать таблицы. Проверить, что:<br/><ul><li>— все таблицы дампа имеются в базе данных;</li><li>— схемы всех таблиц дампа соответствую схемам таблиц БД.
  --restore-data VAL       Whether to restore data or not (default: 1)
  --restore-indexes VAL    Whether to restore indexes or not (default: 1)
  --skip-document-tables VAL
                           Document API tables cannot be restored for now. Specify this option to skip such tables
                           (default: 0)
  --save-partial-result    Do not remove partial restore result.
                           Default: 0.
  --bandwidth VAL          Limit data upload bandwidth, bytes per second (example: 2MiB) (default: 0)
  --rps VAL                Limit requests per second (example: 100) (default: 30)
  --upload-batch-rows VAL  Limit upload batch size in rows (example: 1K) (default: 0)
  --upload-batch-bytes VAL Limit upload batch size in bytes (example: 1MiB) (default: "512KiB")
  --upload-batch-rus VAL   Limit upload batch size in request units (example: 100) (default: 30)
  --bulk-upsert            Use BulkUpsert to upload data in more efficient way (default: 0)
  --import-data            Use ImportData to upload data (default: 0)

## Примеры {#examples}

### Полный дамп БД

Сделайте дамп БД:

```bash
{{ ydb-cli }} tools dump -o ~/dump
```

Посмотрите список объектов директории `dump`:

```bash
ls -R ~/dump
```

Результат:

```text
/home/user/dump:
episodes  my-directory  seasons  series

/home/user/dump/episodes:
data_00.csv  scheme.pb

/home/user/dump/my-directory:
sub-directory1  sub-directory2

/home/user/dump/my-directory/sub-directory1:
sub-directory1-1

/home/user/dump/my-directory/sub-directory1/sub-directory1-1:

/home/user/dump/my-directory/sub-directory2:

/home/user/dump/seasons:
data_00.csv  scheme.pb

/home/user/dump/series:
data_00.csv  scheme.pb
```
