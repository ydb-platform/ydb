# Резервное копирование и восстановление

Резервное копирование применяется для защиты от потери данных, позволяя восстановить их из резервной копии.

{{ ydb-short-name }} предоставляет несколько решений для выполнения резервного копирования и восстановления:

{% include [_includes/backup_and_recovery/options_overlay.md](_includes/backup_and_recovery/options_overlay.md) %}

## {{ ydb-short-name }} CLI {#cli}

### Файлы {#files}

Для выполнения резервного копирования в файлы применяется команда `{{ ydb-cli }} tools dump`. Перейдите [по ссылке](../../reference/ydb-cli/export-import/tools-dump.md) в справочник по {{ ydb-short-name }} CLI для получения информации о данной команде.

Для выполнения восстановления из файловой резервной копии применяется команда `{{ ydb-cli }} tools restore`. Перейдите [по ссылке](../../reference/ydb-cli/export-import/tools-restore.md) в справочник по {{ ydb-short-name }} CLI для получения информации о данной команде.

### S3-совместимое хранилище {#s3}

Для выполнения резервного копирования в S3-совместимое хранилище (например, [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html))  применяется команда `{{ ydb-cli }} export s3`. Перейдите [по ссылке](../../reference/ydb-cli/export-import/export-s3.md) в справочник по {{ ydb-short-name }} CLI для получения информации о данной команде.

Для выполнения восстановления из резервной копии, созданной в S3-совместимом хранилище, применяется команда `{{ ydb-cli }} import s3`. Перейдите [по ссылке](../../reference/ydb-cli/export-import/import-s3.md) в справочник по {{ ydb-short-name }} CLI для получения информации о данной команде.

{% include [_includes/backup_and_recovery/cli_overlay.md](_includes/backup_and_recovery/cli_overlay.md) %}

{% include [_includes/backup_and_recovery/others_overlay.md](_includes/backup_and_recovery/others_overlay.md) %}
