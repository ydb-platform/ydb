# System tablet backup

{% note info %}

Currently, only cluster-level system tablets are supported for backup. Backup of database system tablets is not supported.

{% endnote %}

The system tablet backup mechanism provides incremental copying of cluster metadata — such as [Hive](../../concepts/glossary.md#hive), [BSController](../../concepts/glossary.md#ds-controller), and [SchemeShard](../../concepts/glossary.md#scheme-shard) — to the local file system of cluster hosts.

This mechanism is intended for recovering cluster metadata when other recovery methods are unavailable — for example, when a database backup is missing or the database size is too large to restore from a backup within an acceptable time. The mechanism allows you to recover metadata in an existing cluster without recreating the cluster and restoring databases from backups.

If you can recover the cluster using [export/import](./index.md#s3) or [dump/restore](./index.md#files), it is recommended to use those methods.

{% note warning %}

Backups of different system tablets are created independently of each other and are not consistent with each other. After recovery, tablet state may be inconsistent, which can negatively affect cluster operation.

{% endnote %}

## How it works {#how-it-works}

Backup consists of two components:

- **Snapshot** — on each run, the tablet scans all its tables and writes its full state to the backup, including the data schema. Scanning is performed based on a snapshot and does not block tablet operation.
- **Changelog** — on each data or schema change, the tablet asynchronously writes the change to the changelog in parallel with writing to distributed storage. When the changelog size exceeds the snapshot size, the tablet automatically creates a new snapshot.

{% note warning %}

Due to asynchronous writes, the most recent changes that did not make it into the backup before a failure may be lost.

{% endnote %}

Backups are created **locally on the host where the tablet is currently running**. Therefore, the most up-to-date copy is on the host where the tablet was running immediately before the failure.

The number of backups stored on a host is limited in the [configuration](../../reference/configuration/system_tablet_backup_config.md). After a snapshot is successfully taken, the oldest copy is automatically deleted when the limit is exceeded. Incomplete backups (without a fully written snapshot) are deleted when a new backup is created.

## Enabling and configuring backup {#enabling-backup}

{% note warning %}

Only local file systems are supported as backup storage. Network file systems, such as NFS, are not supported and may lead to backup corruption or tablet failures.

{% endnote %}

By default, system tablet backup is **disabled** because it requires storage configuration. {{ ydb-short-name }} nodes typically do not use the local file system, so enabling backup requires a deliberate administrator decision.

To enable backup, add the `system_tablet_backup_config` section to the [cluster configuration](../../reference/configuration/index.md):

```yaml
system_tablet_backup_config:
    filesystem:
        path: "/path/to/backup/directory"
```

The `path` parameter is the absolute path to a directory on the host's local file system for storing backups.

{% note warning %}

The backup directory must be writable by the {{ ydb-short-name }} process on every host where system tablets run. Make sure each host has enough free disk space to store several backups of all system tablets. For large production environments with dozens of storage nodes, at least 5 GB is recommended.

{% endnote %}

After changing the configuration, restart the cluster nodes where system tablets may run. Backup will start automatically.

{% note warning %}

By default, system tablets can run on any static nodes in the cluster, including those whose disks are part of the [static group](../../concepts/glossary.md#static-group). Backups are stored locally on the hosts where system tablets run. This means that if static group hosts are lost, the backups that may be needed to recover the static group may be lost as well.

It is recommended to configure the `bootstrap_config` section in the [cluster configuration](../../reference/configuration/index.md) so that system tablets start on nodes that are not part of the static group. In this case, backups will be stored separately from static group data.

{% endnote %}

## Recovery guide {#recovery-guide}

{% note warning %}

System tablet recovery is a critical operation that may result in data loss. Perform it only when you have a clear understanding of the problem and after consulting with the operations team. Before you begin, make sure you are familiar with all steps.

{% endnote %}

### Step 1. Put the tablet into Recovery mode {#enable-recovery-mode}

The tablet to be recovered must be put into Recovery mode. In this mode, the tablet starts and is available through [Embedded UI](../../reference/embedded-ui/index.md), but **does not operate normally** and **does not read data from distributed storage**, which allows recovery operations to be performed. Other tablets continue to operate normally, allowing the cluster to keep functioning, but some control-plane operations may be unavailable.

{% note warning %}

If recovery is performed after a complete loss of the [static group](../../concepts/glossary.md#static-group), all system tablets must be put into Recovery mode at the same time as the static group is recreated on new hosts. If you recreate the static group without putting tablets into Recovery mode, they will automatically start on top of an empty static group, which will lead to incorrect cluster operation.

{% endnote %}

1. Determine the ID of the system tablet to be recovered. You can find the tablet ID in the Tablets section of [Embedded UI](../../reference/embedded-ui/index.md).
2. Determine the list of nodes where the system tablet to be recovered can run. This list is in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../configuration-management/index.md). If the `bootstrap_config` section is missing from the configuration, use the list of all [static nodes](../../concepts/glossary.md#static-node) in the cluster specified in the `hosts` section of the cluster configuration.
3. Change the configuration by adding `boot_mode: RECOVERY` to the `bootstrap_config` section of the tablet being recovered.
    - When using configuration V1, change the [static configuration](../configuration-management/configuration-v1/static-config.md) on all nodes where the tablet being recovered can run.
    - When using configuration V2, follow the [instructions](../configuration-management/configuration-v2/update-config.md).
    - Example for a `Hive` tablet with ID `72057594037968897`:

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

4. Restart all nodes where the tablet being recovered can run. If any node is unavailable and cannot be restarted, isolate it from the cluster over the network — for example, using a firewall.

    {% note warning %}

    Make sure all nodes where the tablet being recovered can run are restarted with the updated configuration or isolated from the cluster over the network before recovery begins. During recovery, old data is erased and replaced with data from the backup. If the tablet starts in normal mode on a node with the old configuration at this time, it may start operating with partially recovered data.

    {% endnote %}

5. Verify that:
    - There are no issues with the tablet in [HealthCheck](../../reference/ydb-sdk/health-check-api.md).
    - The tablet is not restarting.
    - The recovery form is available in the tablet App in [Embedded UI](../../reference/embedded-ui/index.md).

### Step 2. Find backup files {#find-backup-files}

1. Determine which hosts to check. First, check the hosts where the tablet was running before the failure. You can determine these hosts using logs or a monitoring system.

    If you cannot determine specific hosts, check all hosts where the tablet could have run. This list is in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../configuration-management/index.md). If the `bootstrap_config` section is missing from the configuration, use the list of all [static nodes](../../concepts/glossary.md#static-node) in the cluster specified in the `hosts` section of the cluster configuration.

2. Find the backup directory. On each candidate host, check for backups. The backup path is determined by the `path` parameter in the `system_tablet_backup_config` configuration:

    ```bash
    ls /path/to/backup/directory/<tablet_type>/<tablet_id>/
    ```

    Example for a `Hive` tablet with ID `72057594037968897`:

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
    - `step` — tablet step within a generation, incremented on each tablet state change.

    Select the backup with the **maximum generation**, and if generations are equal — with the **maximum step**. If backups are found on multiple hosts, compare them and select the most up-to-date one.

4. Make sure the backup is fully written. The selected backup must contain a `snapshot` directory, **not** `snapshot.tmp`. The presence of `snapshot.tmp` means snapshot writing was not completed and the copy is not suitable for recovery. In this case, select the previous most up-to-date copy.

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

1. Determine the host where the tablet is running in Recovery mode. To do this, open [Embedded UI](../../reference/embedded-ui/index.md) and find the node where the tablet is running.
2. If the backup files are on another host, copy them to the host with the tablet in Recovery mode using `scp`, `rsync`, or any other available tool:

    ```bash
    scp -r /tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222 \
        user@target-host:~/backup_20251007T193502_g214_s1222
    ```

3. Make sure the files are readable by the {{ ydb-short-name }} process on the target host.

### Step 4. Perform recovery {#perform-recovery}

1. Open the App of the tablet being recovered in [Embedded UI](../../reference/embedded-ui/index.md).

2. In the recovery form, specify the full path to the directory with backup files, for example:

    ```text
    /tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222
    ```

3. If necessary, set the flags:

    - **Dry Run** — performs a trial recovery without making changes to storage. Allows you to verify backup correctness.
    - **Skip Checksum Validation** — skips checksum validation of backup files.

    {% note warning %}

    Always perform a Dry Run before the first recovery to verify backup correctness.

    {% endnote %}

    {% note warning %}

    Skip checksum validation only when manually editing backup files. In other cases, it is recommended to keep validation enabled to ensure integrity of recovered data.

    {% endnote %}

4. Click **Start Restore**. Before recovery begins, the system will ask for confirmation, as the operation overwrites existing tablet data. Confirm the action to start. After recovery starts, the form becomes unavailable — you cannot start recovery again until the tablet is restarted.

    {% note info %}

    Recovery can also be started using `curl` by sending a POST request to the tablet App page with the `restoreBackup` parameter:

    ```bash
    curl -X POST "http://<host>:<mon_port>/tablets/app?TabletID=72057594037968897&restoreBackup=/tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222"
    ```

    Where `<host>` is the cluster node address and `<mon_port>` is that node's monitoring port.

    {% endnote %}

5. Monitor recovery progress. The page **does not refresh automatically** — refresh the page manually to get the current status. Recovery duration depends on backup size.

    {% note info %}

    Recovery runs on the server side and does not depend on the browser. You can close the page or browser — this will **not interrupt** the recovery process. When you open the page again, the current status will be displayed.

    {% endnote %}

    The current operation status is displayed below the form. Possible statuses:

    - `Restoring from '<path>'` — recovery is in progress; data from the backup is being read and written to storage. A **progress bar** with the operation completion percentage is also displayed.
    - `Restore from '<path>' completed successfully` — recovery completed successfully. You can proceed to the next step.
    - `Restore from '<path>' completed, but changelog is not fully restored` — main tablet data has been recovered, but the changelog tail in the backup is corrupted and some recent changes are lost. Review what data was lost and recover it manually if necessary, or proceed to the next step.
    - `Restore from '<path>' failed: <error description>` — recovery finished with an error. Review the error description and retry if necessary.

    To forcibly interrupt recovery, **restart the tablet**.

6. Wait for recovery to complete successfully. If the recovery form becomes available again (**Start Restore** button is active, status is reset), the tablet was restarted and recovery was interrupted. In this case, start recovery again from step 2.

### Step 5. Return the tablet to normal operation {#return-to-normal}

After successful recovery:

1. Determine the list of nodes where the system tablet being recovered can run. This list is in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../configuration-management/index.md). If the `bootstrap_config` section is missing from the configuration, use the list of all [static nodes](../../concepts/glossary.md#static-node) in the cluster specified in the `hosts` section of the cluster configuration.
2. Change the configuration by removing `boot_mode: RECOVERY` from the `bootstrap_config` section of the tablet being recovered.
    - When using configuration V1, change the [static configuration](../configuration-management/configuration-v1/static-config.md) on all nodes where the tablet being recovered can run.
    - When using configuration V2, follow the [instructions](../configuration-management/configuration-v2/update-config.md).
3. Restart all nodes where the tablet being recovered can run. If any nodes were isolated from the cluster over the network in previous steps, remove the network isolation.
4. Verify that:
    - There are no issues with the tablet in [HealthCheck](../../reference/ydb-sdk/health-check-api.md).
    - The tablet is not restarting.
    - The recovery form is not present in the tablet App in [Embedded UI](../../reference/embedded-ui/index.md).
