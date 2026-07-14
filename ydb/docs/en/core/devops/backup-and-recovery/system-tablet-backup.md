# System tablet backup

{% note info %}

Currently, only cluster system tablets are supported for backup. Backup of database system tablets is not supported.

{% endnote %}

The system tablet backup mechanism provides incremental copying of cluster metadata — such as [Hive](../../concepts/glossary.md#hive), [BSController](../../concepts/glossary.md#ds-controller), and [SchemeShard](../../concepts/glossary.md#scheme-shard) — to the local file system of cluster hosts.

This mechanism is designed to restore cluster metadata when other recovery methods are unavailable — for example, when a database backup is missing or the database size is too large to restore it from a backup within an acceptable time. The mechanism allows restoring metadata in an existing cluster without needing to recreate the cluster and restore databases from backups.

If it is possible to restore the cluster using the [export/import](./index.md#s3) or [dump/restore](./index.md#files) commands, it is recommended to use these methods.

{% note warning %}

Backups of different system tablets are created independently of each other and are not consistent with each other. After restoration, the state of the tablets may become inconsistent, which may negatively affect cluster operation.

{% endnote %}

## How it works {#how-it-works}

Backup consists of two components:

- **State snapshot** — on each run, the tablet scans all its tables and writes its full state to the backup, including the data schema. The scan is performed based on a state snapshot and does not block the tablet's operation.
- **Change log (changelog)** — on each change of data or schema, the tablet asynchronously writes the change to the log in parallel with writing to the distributed storage. When the log size exceeds the state snapshot size, the tablet automatically creates a new snapshot.

{% note warning %}

Due to asynchronous writing, the latest changes that have not been written to the backup before a failure may be lost.

{% endnote %}

Backups are created **locally on the host where the tablet is currently running**. Therefore, the most recent copy is on the host where the tablet was running immediately before the failure.

The number of stored backups on a host is limited in the [configuration](../../reference/configuration/system_tablet_backup_config.md). After a successful snapshot, the oldest copy is automatically deleted when the limit is exceeded. Incomplete copies (without a fully written snapshot) are deleted when a new backup is created.

## Enabling and configuring backup {#enabling-backup}

{% note warning %}

Only local file systems are supported as backup storage. Network file systems, such as NFS, are not supported and may lead to backup corruption or tablet failures.

{% endnote %}

By default, backup of system tablets is **disabled** because it requires storage configuration. {{ ydb-short-name }} nodes typically do not use a local file system, so enabling it requires an informed decision by the administrator.

To enable it, add the `system_tablet_backup_config` section to the [cluster configuration](../../reference/configuration/index.md):


```yaml
system_tablet_backup_config:
    filesystem:
        path: "/path/to/backup/directory"
```


The `path` parameter is an absolute path to a directory on the host's local file system for storing backups.

{% note warning %}

The backup directory must be writable by the {{ ydb-short-name }} process on each host where system tablets run. Ensure that each host's disk has enough free space to store several backups of all system tablets. For large production environments consisting of dozens of storage nodes, at least 5 GB is recommended.

{% endnote %}

After changing the configuration, restart the cluster nodes where system tablets may run. Backup will start automatically.

{% note warning %}

By default, system tablets can run on any static nodes of the cluster, including those whose disks are part of the [static group](../../concepts/glossary.md#static-group). Backups are stored locally on the hosts where system tablets run. This means that if static group hosts are lost, the backups that may be needed to restore the static group may also be lost.

It is recommended to configure the `bootstrap_config` section in the [cluster configuration](../../reference/configuration/index.md) so that system tablets run on nodes that are not part of the static group. In this case, backups will be stored separately from the static group data.

{% endnote %}

## Recovery guide {#recovery-guide}

{% note warning %}

Restoring system tablets is a critical operation that may result in data loss. Perform it only if you have a clear understanding of the problem and after consulting with the operations team. Before starting, make sure you have reviewed all the steps.

{% endnote %}

### Step 1. Put the tablet into Recovery mode {#enable-recovery-mode}

The tablet to be restored must be put into Recovery mode. In this mode, the tablet starts and is accessible via the [Embedded UI](../../reference/embedded-ui/index.md), but **does not work normally** and **does not read data from the distributed storage**, which allows recovery operations to be performed. Other tablets will continue to work normally, allowing the cluster to keep functioning, but some control-plane operations may be unavailable.

{% note warning %}

If recovery is performed after a complete loss of the [static group](../../concepts/glossary.md#static-group), all system tablets must be put into Recovery mode simultaneously with recreating the static group on new hosts. If you recreate the static group without putting the tablets into Recovery mode, they will automatically start on top of an empty static group, leading to incorrect cluster operation.

{% endnote %}

1. Determine the ID of the system tablet to be restored. The tablet ID can be found in the Tablets section of the [Embedded UI](../../reference/embedded-ui/index.md).
2. Determine the list of nodes on which the system tablet being restored can run. This list is located in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../configuration-management/index.md). If the `bootstrap_config` section is missing from the configuration, use the list of all [static nodes](../../concepts/glossary.md#static-node) of the cluster specified in the `hosts` section of the cluster configuration.
3. Modify the configuration by adding `boot_mode: RECOVERY` to the `bootstrap_config` section of the tablet being restored.

   - When using V1 configuration, you need to modify the [static configuration](../configuration-management/configuration-v1/static-config.md) on all nodes where the tablet being restored can run.
   - When using V2 configuration, follow the [instructions](../configuration-management/configuration-v2/update-config.md).
   - Example for tablet `Hive` with ID `72057594037968897`:


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

4. Restart all nodes on which the tablet being restored can run. If any node is unavailable and cannot be restarted, isolate it from the cluster over the network — for example, using a firewall.

   {% note warning %}

   Make sure that all nodes on which the tablet being restored can run are restarted with the updated configuration or isolated from the cluster over the network before starting recovery. During recovery, old data is erased and replaced with data from the backup. If at that moment the tablet starts in normal mode on a node with the old configuration, it may start working with partially restored data.

   {% endnote %}
5. Make sure that:

   - There are no issues with the tablet in [HealthCheck](../../reference/ydb-sdk/health-check-api.md).
   - The tablet is not restarting.
   - The recovery form is available in the tablet's App in the [Embedded UI](../../reference/embedded-ui/index.md).

### Step 2. Find the backup files {#find-backup-files}

1. Determine which hosts to search. First, check the hosts where the tablet was running before the failure. You can identify these hosts using logs or a monitoring system.

   If you cannot determine specific hosts, check all hosts where the tablet could have been running. This list is located in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../configuration-management/index.md). If the `bootstrap_config` section is missing from the configuration, use the list of all [static nodes](../../concepts/glossary.md#static-node) of the cluster specified in the `hosts` section of the cluster configuration.
2. Find the directory with backups. On each candidate host, check for the presence of backups. The path to backups is determined by the `path` parameter in the `system_tablet_backup_config` configuration:


   ```bash
   ls /path/to/backup/directory/<tablet_type>/<tablet_id>/
   ```


   Example for tablet `Hive` with ID `72057594037968897`:


   ```bash
   ls /tablet/hive/72057594037968897/
   ```


   ```text
   backup_20251007T181003_g213_s1001
   backup_20251007T191002_g214_s1040
   backup_20251007T193502_g214_s1222
   ```

3. Select the most recent backup. The name of each backup contains key information: `backup_<timestamp>_g<generation>_s<step>`, where:

   - `timestamp` — the time the backup was created;
   - `generation` — [tablet generation](../../concepts/glossary.md#tablet-generation), increases with each tablet restart;
   - `step` — tablet step within a generation, increases with each tablet state change.

   Select a backup with the **maximum generation**, and if generations are equal, with the **maximum step**. If backups are found on multiple hosts, compare them and select the most recent one.
4. Make sure the backup is fully written. The selected backup must contain the directory `snapshot`, **not** `snapshot.tmp`. The presence of `snapshot.tmp` means that the snapshot write was not completed and the copy is unsuitable for recovery. In this case, select the previous most recent copy.


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

### Step 3. Transfer the backup files {#transfer-backup-files}

1. Determine which host the tablet is running on in Recovery mode. To do this, open the [Embedded UI](../../reference/embedded-ui/index.md) and find the node where the tablet is running.
2. If the backup files are on a different host, copy them to the host with the tablet in Recovery mode using `scp`, `rsync`, or any other available tool:


   ```bash
   scp -r /tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222 \
       user@target-host:~/backup_20251007T193502_g214_s1222
   ```

3. Make sure the files are readable by the {{ ydb-short-name }} process on the target host.

### Step 4. Perform the restore {#perform-recovery}

1. Open the App of the tablet being restored in the [Embedded UI](../../reference/embedded-ui/index.md).
2. In the restore form, specify the full path to the directory with the backup files, for example:


   ```text
   /tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222
   ```

3. If necessary, set the flags:

   - **Dry Run** — performs a trial restore without making changes to the storage. Allows you to verify the backup is correct.
   - **Skip Checksum Validation** — skips checksum verification of the backup files.

   {% note warning %}

   Always perform a Dry Run before the first restore to verify the backup is correct.

   {% endnote %}

   {% note warning %}

   Skip checksum verification only if you manually edited the backup files. In other cases, it is recommended to keep verification enabled to ensure the integrity of the restored data.

   {% endnote %}
4. Click the **Start Restore** button. Before starting the restore, the system will ask for confirmation because the operation overwrites existing tablet data. Confirm the action to start. After the restore starts, the form becomes unavailable — a restart is not possible until the tablet is restarted.

   {% note info %}

   You can also start the restore using `curl` by sending a POST request to the tablet's App page with the parameter `restoreBackup`:


   ```bash
   curl -X POST "http://<host>:<mon_port>/tablets/app?TabletID=72057594037968897&restoreBackup=/tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222"
   ```


   Where `<host>` is the cluster node address, and `<mon_port>` is the monitoring port of that node.

   {% endnote %}
5. Monitor the restore progress. The page **does not update automatically** — to get the current status, refresh the page manually. The restore duration depends on the backup size.

   {% note info %}

   The restore runs on the server side and does not depend on the browser. You can close the page or browser — this will **not interrupt** the restore process. When you reopen the page, the current status will be displayed.

   {% endnote %}

   The current operation status is displayed below the form. Possible statuses:

   - `Restoring from '<path>'` — restore in progress, data from the backup is being read and written to storage. Additionally, a **progress bar** with the operation completion percentage is displayed.
   - `Restore from '<path>' completed successfully` — restore completed successfully. You can proceed to the next step.
   - `Restore from '<path>' completed, but changelog is not fully restored` — the tablet's main data has been restored, but the tail of the change log in the backup is damaged, and some recent changes are lost. Review what data was lost and, if necessary, restore it manually, or proceed to the next step.
   - `Restore from '<path>' failed: <error description>` — restore completed with an error. Review the error description and retry if necessary.

   To forcibly interrupt the restore, **restart the tablet**.
6. Wait for the restore to complete successfully. If the restore form becomes available again (the **Start Restore** button is active, status reset), it means the tablet was restarted and the restore was interrupted. In this case, start the restore again from step 2.

### Step 5. Return the tablet to normal operation mode {#return-to-normal}

After a successful restore:

1. Determine the list of nodes where the system tablet being restored can run. This list is located in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../configuration-management/index.md). If the `bootstrap_config` section is missing from the configuration, use the list of all [static nodes](../../concepts/glossary.md#static-node) of the cluster specified in the `hosts` section of the cluster configuration.
2. Change the configuration by removing `boot_mode: RECOVERY` from the `bootstrap_config` section of the tablet being restored.

   - When using configuration V1, you must change the [static configuration](../configuration-management/configuration-v1/static-config.md) on all nodes where the tablet being restored can run.
   - When using configuration V2, follow the [instructions](../configuration-management/configuration-v2/update-config.md).
3. Restart all nodes where the tablet being restored can run. If any nodes were isolated from the cluster over the network in previous steps, remove the network isolation.
4. Make sure that:

   - There are no issues with the tablet in [HealthCheck](../../reference/ydb-sdk/health-check-api.md).
   - The tablet is not restarting.
   - The App of the tablet in [Embedded UI](../../reference/embedded-ui/index.md) does not have a recovery form.
