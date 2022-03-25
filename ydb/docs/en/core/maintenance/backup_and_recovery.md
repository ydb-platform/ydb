# Backup and recovery

Backup protects against data loss and lets you restore data from a backup copy in the event of a loss.

YDB provides multiple solutions for backup and recovery:

- Backing up data to files and restoring it with a command run by an admin in the YDB CLI
- Backing up data to S3-compatible storage with a command run by an admin in the YDB CLI

{% include [_includes/backup_and_recovery/options_overlay.md](_includes/backup_and_recovery/options_overlay.md) %}

## YDB CLI {#cli}

### Files {#files}

To back up data to a file, run the [`tools dump`](../reference/ydb-cli/export_import/tools_dump.md) command in the YDB CLI as an admin. To learn more about this command, follow the [link](../reference/ydb-cli/export_import/tools_dump.md) in the YDB CLI reference.

To restore data from a file backup created with the `tools dump` command in the YDB CLI, run the [`tools restore`](../reference/ydb-cli/export_import/tools_restore.md) command in the YDB CLI. To learn more about this command, follow the [link](../reference/ydb-cli/export_import/tools_restore.md) in the YDB CLI reference.

### S3-compatible storage {#s3}

To back up data to S3-compatible storage (such as [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html)), run the [`export s3`](../reference/ydb-cli/export_import/s3_export.md) command in the YDB CLI as an admin. To learn more about this command, follow the [link](../reference/ydb-cli/export_import/s3_export.md) in the YDB CLI reference.

To restore data from a backup created in S3-compatible storage with the`export s3` command in the YDB CLI, run the [`import s3`](../reference/ydb-cli/export_import/s3_import.md) command in the YDB CLI. To learn more about this command, follow the [link](../reference/ydb-cli/export_import/s3_import.md) in the YDB CLI reference.

{% include [_includes/backup_and_recovery/cli_overlay.md](_includes/backup_and_recovery/cli_overlay.md) %}

{% include [_includes/backup_and_recovery/others_overlay.md](_includes/backup_and_recovery/others_overlay.md) %}

