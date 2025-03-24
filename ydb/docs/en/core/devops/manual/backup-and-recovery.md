# Backup and recovery

Backups protect against data loss by letting you restore data.

{{ ydb-short-name }} provides multiple solutions for backup and recovery:

* Backing up data to files and restoring it using the {{ ydb-short-name }} CLI.
* Backing up data to S3-compatible storage and restoring it using the {{ ydb-short-name }} CLI.

{% include [_includes/backup_and_recovery/options_overlay.md](_includes/backup_and_recovery/options_overlay.md) %}

## {{ ydb-short-name }} CLI {#cli}

### Files {#files}

To back up data to a file, the following commands are used:

- `{{ ydb-cli }} admin cluster dump` — to back up a cluster' metadata
- `{{ ydb-cli }} admin database dump` — to back up a database
- `{{ ydb-cli }} tools dump` — to back up individual schema objects or directories

To learn more about these commands, see [{#T}](../../reference/ydb-cli/export-import/tools-dump.md).

To restore data from a backup, the following commands are used:

- `{{ ydb-cli }} admin cluster restore` — to restore a cluster' metadata from a backup
- `{{ ydb-cli }} admin database restore` — to restore a database from a backup
- `{{ ydb-cli }} tools restore` — to restore individual schema objects or directories from a backup

To learn more about these command, see [{#T}](../../reference/ydb-cli/export-import/tools-restore.md).

### S3-compatible storage {#s3}

To back up data to S3-compatible storage (such as [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html)), run the `ydb export s3` command. To learn more about this command, follow the [link](../../reference/ydb-cli/export-import/export-s3.md) to the {{ ydb-short-name }} CLI reference.

To restore data from a backup created in S3-compatible storage, run the `ydb import s3` command. To learn more about this command, follow the [link](../../reference/ydb-cli/export-import/import-s3.md) to the {{ ydb-short-name }} CLI reference.

{% include [_includes/backup_and_recovery/cli_overlay.md](_includes/backup_and_recovery/cli_overlay.md) %}

{% include [_includes/backup_and_recovery/others_overlay.md](_includes/backup_and_recovery/others_overlay.md) %}

