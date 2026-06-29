# Backup and recovery

{{ ydb-short-name }} is originally designed for data safety in case of hardware failures: redundancy options are available for different numbers of availability zones, racks, hosts, disks, and other components (see [cluster operating modes](../../concepts/topology.md#cluster-config)). This reduces the risk of data loss due to hardware failures.

However, data can be lost or corrupted **logically**: errors or malicious actions can lead to mass deletion/corruption of data by operations legitimate for the DBMS. In such cases, cluster fault tolerance does not replace a **separate copy** of data outside the cluster.

Backup is used to protect against such scenarios and allows restoring data from a backup. It is recommended to store copies on a separate medium or in cloud object storage (for example, via [export to files](#files) or [export to S3](#s3)).

{{ ydb-short-name }} provides several solutions for performing backup and recovery. For conceptual information and a comparison of backup methods, see [backup concepts](../../concepts/backup.md).

{% include [_includes/backup_and_recovery/options_overlay.md](../_includes/backup_and_recovery/options_overlay.md) %}

## {{ ydb-short-name }} CLI {#cli}

### Files {#files}

The following commands are used to perform backup to files:

- `{{ ydb-cli }} admin cluster dump` — to back up cluster metadata
- `{{ ydb-cli }} admin database dump` — to back up the database
- `{{ ydb-cli }} tools dump` — to back up individual schema objects or directories

Learn more about these commands in [{#T}](../../reference/ydb-cli/export-import/tools-dump.md).

The following commands are used to restore from a file backup:

- `{{ ydb-cli }} admin cluster restore` — to restore cluster metadata from a backup
- `{{ ydb-cli }} admin database restore` — to restore the database from a backup
- `{{ ydb-cli }} tools restore` — to restore individual schema objects or directories from a backup

Learn more about these commands in [{#T}](../../reference/ydb-cli/export-import/tools-restore.md).

### S3-compatible storage {#s3}

To perform backup to an S3-compatible storage (for example, [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html)), use the `{{ ydb-cli }} export s3` command. Follow [the link](../../reference/ydb-cli/export-import/export-s3.md) to the {{ ydb-short-name }} CLI reference for information about this command.

To restore from a backup created in an S3-compatible storage, use the `{{ ydb-cli }} import s3` command. Follow [the link](../../reference/ydb-cli/export-import/import-s3.md) to the {{ ydb-short-name }} CLI reference for information about this command.

### NFS {#nfs}

To perform backup to [NFS](https://en.wikipedia.org/wiki/Network_File_System) on hosts where {{ ydb-short-name }} is running, use the `{{ ydb-cli }} export nfs` command. Follow [the link](../../reference/ydb-cli/export-import/export-nfs.md) to the {{ ydb-short-name }} CLI reference for information about this command.

To restore from a backup created in [NFS](https://en.wikipedia.org/wiki/Network_File_System) on hosts where {{ ydb-short-name }} is running, use the `{{ ydb-cli }} import nfs` command. Follow [the link](../../reference/ydb-cli/export-import/import-nfs.md) to the {{ ydb-short-name }} CLI reference for information about this command.

For more details on configuring NFS for backup and recovery, see the recipe [Backup and recovery via NFS](../../recipes/nfs-backup/nfs-backup.md).

{% note info %}

The speed of backup and restore operations to/from S3-compatible storage or NFS is tuned to minimize the impact on user load. To control the operation speed, configure limits for the corresponding queue of the [resource broker](../../reference/configuration/resource_broker_config.md#resource-broker-config).

{% endnote %}

{% note info %}

When performing an export, a directory named `export_*` is created in the root directory of the database, where `*` is the numeric part of the export identifier. This directory contains tables that hold a consistent snapshot of the exported data at the time the export started. After the backup operation completes successfully, the `export_*` directory along with its contents is deleted.

{% endnote %}

{% include [_includes/backup_and_recovery/cli_overlay.md](../_includes/backup_and_recovery/cli_overlay.md) %}

{% include [_includes/backup_and_recovery/others_overlay.md](../_includes/backup_and_recovery/others_overlay.md) %}

## Backup of system tablets {#system-tablet-backup}

The backup mechanism for system tablets, such as [Hive](../../concepts/glossary.md#hive), [BSController](../../concepts/glossary.md#ds-controller), and [SchemeShard](../../concepts/glossary.md#scheme-shard), provides incremental copying of cluster metadata to the cluster's local file system.

This mechanism is designed to restore cluster metadata when other recovery methods are unavailable — for example, when a database backup is missing or the database size is too large to restore from a backup in an acceptable time. The mechanism allows restoring metadata in an existing cluster without needing to recreate the cluster and restore databases from backups.

If you can restore the cluster using the [export/import](#s3) or [dump/restore](#files) commands, it is recommended to use these methods.

For conceptual information about system tablet backup, see [Backup concepts](../../concepts/backup.md#system-tablet-backup).
