# Backup and recovery

{{ ydb-short-name }} is originally designed for data safety in case of hardware failures: redundancy options are available for different numbers of availability zones, racks, hosts, disks, and other components (see [cluster operation modes](../../concepts/topology.md#cluster-config)). This reduces the risk of data loss due to hardware failures.

However, data can be lost or corrupted **logically**: errors or malicious actions can lead to mass deletion/corruption of data by operations legitimate for the DBMS. In such cases, cluster fault tolerance does not replace a **separate copy** of data outside the cluster.

Backup is used to protect against such scenarios and allows restoring data from a backup. It is recommended to store copies on a separate medium or in cloud object storage (for example, via [export to files](#files) or [export to S3](#s3)).

{{ ydb-short-name }} provides several solutions for performing backup and recovery. For conceptual information and a comparison of backup methods, see [backup concepts](../../concepts/backup.md).

{% include [_includes/backup_and_recovery/options_overlay.md](../_includes/backup_and_recovery/options_overlay.md) %}

## {{ ydb-short-name }} CLI {#cli}

### Files {#files}

The following commands are used for backup to files:

- `{{ ydb-cli }} admin cluster dump` — for backing up cluster metadata
- `{{ ydb-cli }} admin database dump` — for backing up a database
- `{{ ydb-cli }} tools dump` — for backing up individual schema objects or directories

Learn more about these commands in [{#T}](../../reference/ydb-cli/export-import/tools-dump.md).

The following commands are used for recovery from a file backup:

- `{{ ydb-cli }} admin cluster restore` — for restoring cluster metadata from a backup
- `{{ ydb-cli }} admin database restore` — for restoring a database from a backup
- `{{ ydb-cli }} tools restore` — for restoring individual schema objects or directories from a backup

Learn more about these commands in [{#T}](../../reference/ydb-cli/export-import/tools-restore.md).

### S3-compatible storage {#s3}

To perform backup to an S3-compatible storage (for example, [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html)), the `{{ ydb-cli }} export s3` command is used. Follow [the link](../../reference/ydb-cli/export-import/export-s3.md) to the {{ ydb-short-name }} CLI reference for information about this command.

To perform recovery from a backup created in an S3-compatible storage, the `{{ ydb-cli }} import s3` command is used. Follow [the link](../../reference/ydb-cli/export-import/import-s3.md) to the {{ ydb-short-name }} CLI reference for information about this command.

{% note info %}

The speed of backup and recovery operations to/from an S3-compatible storage is tuned to minimize the impact on user load. To control the operation speed, configure limits for the corresponding queue of the [resource broker](../../reference/configuration/resource_broker_config.md#resource-broker-config).

{% endnote %}

{% note info %}

When performing an export, a directory named `export_*` is created in the root directory of the database, where `*` is the numeric part of the export identifier. This directory contains tables that hold a consistent snapshot of the exported data as of the start of the export. After the backup operation completes successfully, the `export_*` directory along with its contents is deleted.

{% endnote %}

## Backup collections {#backup-collections}

Backup collections provide incremental backup and point-in-time recovery for production workloads. For conceptual information and architecture details, see [Backup collections](../../concepts/datamodel/backup-collection.md).

Backup collections are recommended for production environments with a regular backup schedule and large datasets where incremental changes are significantly smaller than the total volume. For simpler scenarios (one-time migrations, development environments, small databases), consider using [export/import](#s3) or [dump/restore](#files).

For step-by-step instructions on configuring and using backup collections, see:

- [Backup collections](../../concepts/datamodel/backup-collection.md) — architecture, concepts, and limitations
- [Backup collection recipes](../../recipes/backup-collections/index.md) — typical scenarios and examples

{% include [_includes/backup_and_recovery/cli_overlay.md](../_includes/backup_and_recovery/cli_overlay.md) %}

{% include [_includes/backup_and_recovery/others_overlay.md](../_includes/backup_and_recovery/others_overlay.md) %}

## Backup of system tablets {#system-tablet-backup}

The system tablet backup mechanism provides incremental copying of cluster metadata to the local file system of cluster hosts.

For conceptual information and how it works, see [Backup concepts](../../concepts/backup.md#system-tablet-backup).

See step-by-step instructions for enabling and recovery in [Recipes for backing up system tablets](../../recipes/system-tablet-backup/index.md).
