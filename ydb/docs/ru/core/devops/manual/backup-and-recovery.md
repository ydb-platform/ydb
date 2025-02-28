# Резервное копирование и восстановление

Резервное копирование применяется для защиты от потери данных, позволяя восстановить их из резервной копии.

{{ ydb-short-name }} предоставляет несколько решений для выполнения резервного копирования и восстановления:

{% include [_includes/backup_and_recovery/options_overlay.md](_includes/backup_and_recovery/options_overlay.md) %}

## {{ ydb-short-name }} CLI {#cli}

### Файлы {#files}

Для выполнения резервного копирования в файлы применяются команды:
- `{{ ydb-cli }} admin cluster dump` - для резервного копирования кластера
- `{{ ydb-cli }} admin database dump` - для резервного копирования базы данных
- `{{ ydb-cli }} tools dump` - для резервного копирования отдельных схемных объектов или директорий

Перейдите [по ссылке](../../reference/ydb-cli/export-import/tools-dump.md) в справочник по {{ ydb-short-name }} CLI для получения информации о команднах резервного копирования в файлы.

Для выполнения восстановления из файловой резервной копии применяется команды:
- `{{ ydb-cli }} admin cluster restore` - для восстановления кластера из резервной копии
- `{{ ydb-cli }} admin database restore` - для восстановления базы данных из резервной копии
- `{{ ydb-cli }} tools restore` - для восстановления отдельных схемных объектов или директорий из резервной копии

Перейдите [по ссылке](../../reference/ydb-cli/export-import/tools-restore.md) в справочник по {{ ydb-short-name }} CLI для получения информации о командах восстановления из файловой резервной копии.

### S3-совместимое хранилище {#s3}

Для выполнения резервного копирования в S3-совместимое хранилище (например, [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html))  применяется команда `{{ ydb-cli }} export s3`. Перейдите [по ссылке](../../reference/ydb-cli/export-import/export-s3.md) в справочник по {{ ydb-short-name }} CLI для получения информации о данной команде.

Для выполнения восстановления из резервной копии, созданной в S3-совместимом хранилище, применяется команда `{{ ydb-cli }} import s3`. Перейдите [по ссылке](../../reference/ydb-cli/export-import/import-s3.md) в справочник по {{ ydb-short-name }} CLI для получения информации о данной команде.

{% note info %}

Скорость операций резервного копирования и восстановления в/из S3-совместимого хранилища подобрана таким образом, чтобы минимизировать влияние на пользовательскую нагрузку. Для управления скоростью операций настройте лимиты для соответствующей очереди [брокера ресурсов](../../reference/configuration/index.md#resource-broker-config).

{% endnote %}

{% include [_includes/backup_and_recovery/cli_overlay.md](_includes/backup_and_recovery/cli_overlay.md) %}

{% include [_includes/backup_and_recovery/others_overlay.md](_includes/backup_and_recovery/others_overlay.md) %}
