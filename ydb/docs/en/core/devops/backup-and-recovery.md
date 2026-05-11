# Backup and Recovery

{{ ydb-short-name }} is designed to preserve data against hardware failures: redundancy options are available for various numbers of availability zones, racks, hosts, disks, and other components (see [cluster operating modes](../concepts/topology.md#cluster-config)). This reduces the risk of data loss due to hardware failures.

However, data can be lost or corrupted **logically**: errors or malicious actions can cause mass deletion or distortion of data through operations that are legitimate from the DBMS perspective. In such cases, cluster fault tolerance does not replace a **separate copy** of data outside the cluster.

Backup is used to protect against such scenarios and allows you to restore data from a backup copy. It is recommended to keep copies on separate storage media or in cloud object storage (for example, via [dumping to files](#files) or [export to S3](#s3)).

{{ ydb-short-name }} provides several solutions for performing backup and recovery. For conceptual information and comparison of backup methods, see [Backup concepts](../concepts/backup.md).

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

{% note info %}

When running the export operation, a directory named `export_*` is created in the root directory, where `*` is the numeric part of the export ID. This directory stores tables with a consistent snapshot of exported data as of the export start time. After a successful backup, the `export_*` directory and its contents are removed.

{% endnote %}

## Backup Collections {#backup-collections}

Backup collections enable incremental backups and recovery to any saved backup point in the chain for production workloads. For conceptual information and architecture details, see [Backup collections](../concepts/datamodel/backup-collection.md).

Backup collections are recommended for production environments with regular backup schedules and large datasets where incremental changes are much smaller than total data size. For simpler scenarios (one-time migrations, development environments, small databases), consider using [export/import](#s3) or [dump/restore](#files) instead.

For step-by-step instructions on configuring and using backup collections, see:

- [Backup collections](../concepts/datamodel/backup-collection.md) — architecture, concepts, and limitations
- [Backup collection recipes](../recipes/backup-collections/index.md) — common scenarios and examples

{% include [_includes/backup_and_recovery/cli_overlay.md](_includes/backup_and_recovery/cli_overlay.md) %}

{% include [_includes/backup_and_recovery/others_overlay.md](_includes/backup_and_recovery/others_overlay.md) %}
