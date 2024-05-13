# Backup and recovery

Backups protect against data loss by letting you restore data.

{{ ydb-short-name }} provides multiple solutions for backup and recovery:

* Backing up data to files and restoring it using the {{ ydb-short-name }} CLI.
* Backing up data to S3-compatible storage and restoring it using the {{ ydb-short-name }} CLI.

{% include [_includes/backup_and_recovery/options_overlay.md](_includes/backup_and_recovery/options_overlay.md) %}

## {{ ydb-short-name }} CLI {#cli}

### Files {#files}

To back up data to a file, run the `ydb tools dump` command. To learn more about this command, follow the [link](../../reference/ydb-cli/export-import/tools-dump.md) to the {{ ydb-short-name }} CLI reference.

To restore data from a backup, run the `ydb tools restore` command. To learn more about this command, follow the [link](../../reference/ydb-cli/export-import/tools-restore.md) to the {{ ydb-short-name }} CLI reference.

### S3-compatible storage {#s3}

To back up data to S3-compatible storage (such as [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html)), run the `ydb export s3` command. To learn more about this command, follow the [link](../../reference/ydb-cli/export-import/export-s3.md) to the {{ ydb-short-name }} CLI reference.

To restore data from a backup created in S3-compatible storage, run the `ydb import s3` command. To learn more about this command, follow the [link](../../reference/ydb-cli/export-import/import-s3.md) to the {{ ydb-short-name }} CLI reference.

{% include [_includes/backup_and_recovery/cli_overlay.md](_includes/backup_and_recovery/cli_overlay.md) %}

{% include [_includes/backup_and_recovery/others_overlay.md](_includes/backup_and_recovery/others_overlay.md) %}

