# Backup and Recovery

Backup is used to protect against data loss, allowing you to restore data from a backup copy.

{{ ydb-short-name }} provides several solutions for performing backup and recovery:

{% include [_includes/backup_and_recovery/options_overlay.md](_includes/backup_and_recovery/options_overlay.md) %}

## {{ ydb-short-name }} CLI {#cli}

### Files {#files}

The following commands are used to back up files:

- `{{ ydb-cli }} admin cluster dump` — for backing up cluster metadata
- `{{ ydb-cli }} admin database dump` — for backing up a database
- `{{ ydb-cli }} tools dump` — for backing up individual schema objects or directories

You can learn more about these commands in [{#T}](../reference/ydb-cli/export-import/tools-dump.md).

The following commands are used to perform recovery from a file backup:

- `{{ ydb-cli }} admin cluster restore` — for restoring cluster metadata from a backup
- `{{ ydb-cli }} admin database restore` — for restoring a database from a backup
- `{{ ydb-cli }} tools restore` — for restoring individual schema objects or directories from a backup

You can learn more about these commands in [{#T}](../reference/ydb-cli/export-import/tools-restore.md).

### S3-Compatible Storage {#s3}

The `{{ ydb-cli }} export s3` command is used to back up data to S3-compatible storage (for example, [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html)). Follow [this link](../reference/ydb-cli/export-import/export-s3.md) to the {{ ydb-short-name }} CLI reference for information about this command.

The `{{ ydb-cli }} import s3` command is used to recover data from a backup created in S3-compatible storage. Follow [this link](../reference/ydb-cli/export-import/import-s3.md) to the {{ ydb-short-name }} CLI reference for information about this command.

{% note info %}

The speed of backup and recovery operations to/from S3-compatible storage is configured to minimize impact on user workload. To control the speed of operations, configure limits for the corresponding queue in the [resource broker](../reference/configuration/resource_broker_config.md#resource-broker-config).

{% endnote %}

{% include [_includes/backup_and_recovery/cli_overlay.md](_includes/backup_and_recovery/cli_overlay.md) %}

{% include [_includes/backup_and_recovery/others_overlay.md](_includes/backup_and_recovery/others_overlay.md) %}