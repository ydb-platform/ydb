# Резервное копирование и восстановление

Резервное копирование применяется для защиты от потери данных, позволяя восстановить потерянные данные из резервной копии. 

YDB предоставляет несколько решений для выполнения резервного копирования и восстановления:

- Копирование в файлы и восстановление по команде администратора с помощью YDB CLI
- Копирование в S3-совместимое хранилище и восстановление по команде администратора с помошью YDB CLI

{% include [_includes/backup_and_recovery/options_overlay.md](_includes/backup_and_recovery/options_overlay.md) %}

## YDB CLI {#cli}

### Файлы {#files}

Для выполнения резервного копирования в файлы администратором применяется [команда `tools dump`](../reference/ydb-cli/export_import/tools_dump.md) YDB CLI. Перейдите [по ссылке](../reference/ydb-cli/export_import/tools_dump.md) в справочник по YDB CLI для получения информации о данной команде.

Для выполнения восстановления из файловой резервной копии, созданной при помощи команды `tools dump` YDB CLI, применяется [команда `tools restore`](../reference/ydb-cli/export_import/tools_restore.md) YDB CLI. Перейдите [по ссылке](../reference/ydb-cli/export_import/tools_restore.md) в справочник по YDB CLI для получения информации о данной команде.

### S3-совместимое хранилище {#s3}

Для выполнения резервного копирования в S3-совместимое хранилище (например, [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html)) администратором применяется [команда `export s3`](../reference/ydb-cli/export_import/s3_export.md) YDB CLI. Перейдите [по ссылке](../reference/ydb-cli/export_import/s3_export.md) в справочник по YDB CLI для получения информации о данной команде.

Для выполнения восстановления из резервной копии, созданной в S3-совместимом хранилище при помощи команды `export s3` YDB CLI, применяется [команда `import s3`](../reference/ydb-cli/export_import/s3_import.md) YDB CLI. Перейдите [по ссылке](../reference/ydb-cli/export_import/s3_import.md) в справочник по YDB CLI для получения информации о данной команде.

{% include [_includes/backup_and_recovery/cli_overlay.md](_includes/backup_and_recovery/cli_overlay.md) %}

{% include [_includes/backup_and_recovery/others_overlay.md](_includes/backup_and_recovery/others_overlay.md) %}


