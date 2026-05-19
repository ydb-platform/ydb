# System tablet backup

{% note info %}

Currently, only cluster system tablets are supported for backup. Database system tablet backup is not supported.

{% endnote %}

The system tablet backup mechanism provides incremental copying of cluster metadata — such as [Hive](../../concepts/glossary.md#hive), [BSController](../../concepts/glossary.md#ds-controller), and [SchemeShard](../../concepts/glossary.md#scheme-shard) — to the local file system of cluster hosts.

This mechanism is intended to restore cluster metadata when other recovery methods are unavailable — for example, when a database backup is missing or the database is too large to restore from a backup within an acceptable time. It lets you restore metadata in an existing cluster without recreating the cluster and restoring databases from backups.

If you can restore the cluster using [export/import](./index.md#s3) or [dump/restore](./index.md#files), prefer those methods.

{% note warning %}

Backups of different system tablets are created independently and are not consistent with each other. After recovery, tablet state may be inconsistent, which can negatively affect cluster operation.

{% endnote %}

## How it works {#how-it-works}

Backup consists of two components:

- **Snapshot** — on each run, the tablet scans all its tables and writes its full state to the backup, including the data schema. Scanning is based on a snapshot and does not block tablet operation.
- **Changelog** — on each data or schema change, the tablet asynchronously writes the change to the log in parallel with writing to distributed storage. When the log size exceeds the snapshot size, the tablet automatically creates a new snapshot.

{% note warning %}

Because writes are asynchronous, the most recent changes that did not reach the backup before a failure may be lost.

{% endnote %}

Backups are created **locally on the host where the tablet is currently running**. Therefore, the most up-to-date copy is on the host where the tablet was running immediately before the failure.

The number of backups stored on a host is limited in the [configuration](../../reference/configuration/system_tablet_backup_config.md). After a snapshot is taken successfully, the oldest backup is automatically removed when the limit is exceeded. Incomplete backups (without a fully written snapshot) are removed when a new backup is created.

## Enabling and configuring backup {#enabling-backup}

{% note warning %}

Only local file systems are supported as backup storage. Network file systems such as NFS are not supported and may lead to backup corruption or tablet failures.

{% endnote %}

By default, system tablet backup is **disabled** because it requires storage configuration. {{ ydb-short-name }} nodes usually do not use the local file system, so enabling backup requires a deliberate administrator decision.

To enable backup, add the `system_tablet_backup_config` section to the [cluster configuration](../../reference/configuration/index.md):

```yaml
system_tablet_backup_config:
    filesystem:
        path: "/path/to/backup/directory"
```

The `path` parameter is the absolute path to a directory on the host's local file system for storing backups.

{% note warning %}

The backup directory must be writable by the {{ ydb-short-name }} process on every host where system tablets run. Make sure each host has enough free disk space to store several backups of all system tablets. For large deployments with dozens of storage nodes, at least 5 GB is recommended.

{% endnote %}

After changing the configuration, restart cluster nodes where system tablets may run. Backup will start automatically.

{% note warning %}

By default, system tablets can run on any static cluster nodes, including those whose disks are part of the [static group](../../concepts/glossary.md#static-group). Backups are stored locally on hosts where system tablets run. This means that if static group hosts are lost, backups needed to restore the static group may be lost as well.

It is recommended to configure the `bootstrap_config` section in the [cluster configuration](../../reference/configuration/index.md) so that system tablets start on nodes that are not part of the static group. In this case, backups are stored separately from static group data.

{% endnote %}

## Recovery guide {#recovery-guide}

{% note warning %}

System tablet recovery is a critical operation that may result in data loss. Perform it only when you clearly understand the problem and after consulting the operations team. Before you begin, make sure you have reviewed all steps.

{% endnote %}

### Step 1. Put the tablet in Recovery mode {#enable-recovery-mode}

The tablet to be recovered must be put in Recovery mode. In this mode, the tablet starts and is available through [Embedded UI](../../reference/embedded-ui/index.md), but **does not operate normally** and **does not read data from distributed storage**, which allows recovery operations. Other tablets continue to operate normally, so the cluster can keep running, but some control-plane operations may be unavailable.

{% note warning %}

If recovery is performed after a complete loss of the [static group](../../concepts/glossary.md#static-group), all system tablets must be put in Recovery mode at the same time as the static group is recreated on new hosts. If you recreate the static group without putting tablets in Recovery mode, they will start automatically on top of an empty static group, which will cause incorrect cluster behavior.

{% endnote %}

1. Determine the ID of the system tablet to recover. You can find the tablet ID in the Tablets section of [Embedded UI](../../reference/embedded-ui/index.md).
2. Determine the list of nodes where the system tablet to be recovered may run. This list is in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../configuration-management/index.md). If `bootstrap_config` is missing from the configuration, use the list of all cluster [static nodes](../../concepts/glossary.md#static-node) specified in the `hosts` section of the cluster configuration.
3. Change the configuration by adding `boot_mode: RECOVERY` to the `bootstrap_config` section of the tablet being recovered.
    - When using V1 configuration, change the [static configuration](../configuration-management/configuration-v1/static-config.md) on all nodes where the tablet being recovered may run.
    - When using V2 configuration, follow the [instructions](../configuration-management/configuration-v2/update-config.md).
    - Example for the `Hive` tablet with ID `72057594037968897`:

    ```yaml
        bootstrap_config:
            tablet:
            - type: FLAT_HIVE
              node:
              - 1
              - 2
              - 3
              info:
                  tablet_id: '72057594037968897'
                  channels:
                  - channel: 0
                  history:
                  - from_generation: 0
                      group_id: 0
                  channel_erasure_name: mirror-3-dc
                  - channel: 1
                  history:
                  - from_generation: 0
                      group_id: 0
                  channel_erasure_name: mirror-3-dc
                  - channel: 2
                  history:
                  - from_generation: 0
                      group_id: 0
                  channel_erasure_name: mirror-3-dc
              boot_mode: RECOVERY
    ```

4. Restart all nodes where the tablet being recovered may run. If any node is unavailable and cannot be restarted, isolate it from the cluster over the network — for example, using a firewall.

    {% note warning %}

    Make sure all nodes where the tablet being recovered may run are restarted with the updated configuration or isolated from the cluster over the network before recovery begins. During recovery, old data is erased and replaced with data from the backup. If the tablet starts in normal mode on a node with the old configuration during this process, it may begin operating with partially recovered data.

    {% endnote %}

5. Verify that:
    - There are no issues with the tablet in [HealthCheck](../../reference/ydb-sdk/health-check-api.md).
    - The tablet is not restarting.
    - The recovery form is available in the tablet App in [Embedded UI](../../reference/embedded-ui/index.md).

### Step 2. Find backup files {#find-backup-files}

1. Determine which hosts to search. First, check hosts where the tablet was running before the failure. You can identify these hosts using logs or the monitoring system.

    If you cannot identify specific hosts, check all hosts where the tablet could have run. This list is in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../configuration-management/index.md). If `bootstrap_config` is missing from the configuration, use the list of all cluster [static nodes](../../concepts/glossary.md#static-node) specified in the `hosts` section of the cluster configuration.

2. Find the backup directory. On each candidate host, check for backups. The backup path is determined by the `path` parameter in the `system_tablet_backup_config` configuration:

    ```bash
    ls /path/to/backup/directory/<tablet_type>/<tablet_id>/
    ```

    Example for the `Hive` tablet with ID `72057594037968897`:

    ```bash
    ls /tablet/hive/72057594037968897/
    ```

    ```text
    backup_20251007T181003_g213_s1001
    backup_20251007T191002_g214_s1040
    backup_20251007T193502_g214_s1222
    ```

3. Select the most up-to-date backup. Each backup name contains key information: `backup_<timestamp>_g<generation>_s<step>`, where:

    - `timestamp` — backup creation time;
    - `generation` — [tablet generation](../../concepts/glossary.md#tablet-generation), incremented on each tablet restart;
    - `step` — tablet step within the generation, incremented on each tablet state change.

    Select the backup with the **maximum generation**, and if generations are equal, with the **maximum step**. If backups are found on multiple hosts, compare them and choose the most up-to-date one.

4. Make sure the backup is fully written. The selected backup must contain a `snapshot` directory, **not** `snapshot.tmp`. The presence of `snapshot.tmp` means snapshot writing did not complete and the backup is not suitable for recovery. In this case, choose the previous most up-to-date backup.

    ```bash
    ls /tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222/snapshot/
    ```

    ```text
    manifest.json
    schema.json
    Tablet.json
    TabletFollowerGroup.json
    ...
    ```

### Step 3. Transfer backup files {#transfer-backup-files}

1. Determine which host is running the tablet in Recovery mode. Open [Embedded UI](../../reference/embedded-ui/index.md) and find the node where the tablet is running.
2. If backup files are on another host, copy them to the host with the tablet in Recovery mode using `scp`, `rsync`, or any other available tool:

    ```bash
    scp -r /tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222 \
        user@target-host:~/backup_20251007T193502_g214_s1222
    ```

3. Make sure the files are readable by the {{ ydb-short-name }} process on the target host.

### Step 4. Perform recovery {#perform-recovery}

1. Open the App of the tablet being recovered in [Embedded UI](../../reference/embedded-ui/index.md).

2. In the recovery form, specify the full path to the backup directory, for example:

    ```text
    /tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222
    ```

3. If needed, set the flags:

    - **Dry Run** — performs a trial recovery without making changes to storage. Lets you verify that the backup is correct.
    - **Skip Checksum Validation** — skips checksum validation of backup files.

    {% note warning %}

    Always run Dry Run before the first recovery to verify that the backup is correct.

    {% endnote %}

    {% note warning %}

    Skip checksum validation only if you manually edited backup files. In other cases, keep validation enabled to ensure integrity of recovered data.

    {% endnote %}

4. Click **Start Restore**. Before recovery begins, the system asks for confirmation because the operation overwrites existing tablet data. Confirm to start. After recovery starts, the form becomes unavailable — you cannot start recovery again until the tablet is restarted.

    {% note info %}

    You can also start recovery using `curl` by sending a POST request to the tablet App page with the `restoreBackup` parameter:

    ```bash
    curl -X POST "http://<host>:<mon_port>/tablets/app?TabletID=72057594037968897&restoreBackup=/tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222"
    ```

    Where `<host>` is the cluster node address and `<mon_port>` is that node's monitoring port.

    {% endnote %}

5. Monitor recovery progress. The page **does not refresh automatically** — refresh the page manually to get the current status. Recovery duration depends on backup size.

    {% note info %}

    Recovery runs on the server side and does not depend on the browser. You can close the page or browser — this **does not interrupt** recovery. When you open the page again, the current status is displayed.

    {% endnote %}

    The current operation status is shown below the form. Possible statuses:

    - `Restoring from '<path>'` — recovery is in progress; data from the backup is being read and written to storage. A **progress bar** with the completion percentage is also shown.
    - `Restore from '<path>' completed successfully` — recovery completed successfully. You can proceed to the next step.
    - `Restore from '<path>' completed, but changelog is not fully restored` — main tablet data was recovered, but the changelog tail in the backup is corrupted and some recent changes were lost. Review what data was lost and restore it manually if needed, or proceed to the next step.
    - `Restore from '<path>' failed: <error description>` — recovery failed with an error. Review the error description and retry if needed.

    To force recovery to stop, **restart the tablet**.

6. Wait for recovery to complete successfully. If the recovery form becomes available again (**Start Restore** is active, status is reset), the tablet was restarted and recovery was interrupted. In this case, start recovery again from step 2.

### Step 5. Return the tablet to normal operation {#return-to-normal}

After successful recovery:

1. Determine the list of nodes where the recovered system tablet may run. This list is in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../configuration-management/index.md). If `bootstrap_config` is missing from the configuration, use the list of all cluster [static nodes](../../concepts/glossary.md#static-node) specified in the `hosts` section of the cluster configuration.
2. Change the configuration by removing `boot_mode: RECOVERY` from the `bootstrap_config` section of the recovered tablet.
    - When using V1 configuration, change the [static configuration](../configuration-management/configuration-v1/static-config.md) on all nodes where the tablet may run.
    - When using V2 configuration, follow the [instructions](../configuration-management/configuration-v2/update-config.md).
3. Restart all nodes where the tablet may run. If any nodes were isolated from the cluster over the network in previous steps, remove the network isolation.
4. Verify that:
    - There are no issues with the tablet in [HealthCheck](../../reference/ydb-sdk/health-check-api.md).
    - The tablet is not restarting.
    - The recovery form is not shown in the tablet App in [Embedded UI](../../reference/embedded-ui/index.md).
