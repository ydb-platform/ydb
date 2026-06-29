# Recovering system tablets {#recovery-guide}

{% note info %}

For conceptual information about the mechanism, see the [System tablet backup](../../concepts/backup.md#system-tablet-backup) section.

{% endnote %}

{% note warning %}

Recovering system tablets is a critical operation that may result in data loss. Perform it only if you have a clear understanding of the problem and after consulting with the operations team. Before starting, make sure you have reviewed all steps.

{% endnote %}

## Step 1. Put the tablet into Recovery mode {#enable-recovery-mode}

The tablet to be recovered must be put into Recovery mode. In this mode, the tablet starts and is accessible via the [Embedded UI](../../reference/embedded-ui/index.md), but **does not work normally** and **does not read data from the distributed storage**, allowing recovery operations to be performed. Other tablets will continue to operate normally, allowing the cluster to keep functioning, but some control-plane operations may be unavailable.

{% note warning %}

If recovery is performed after a complete loss of the [static group](../../concepts/glossary.md#static-group), all system tablets must be put into Recovery mode simultaneously with recreating the static group on new hosts. If you recreate the static group without putting the tablets into Recovery mode, they will automatically start on top of an empty static group, leading to incorrect cluster operation.

{% endnote %}

1. Determine the ID of the system tablet to be recovered. The tablet ID can be found in the Tablets section of the [Embedded UI](../../reference/embedded-ui/index.md).
2. Determine the list of nodes where the system tablet to be recovered can run. This list is located in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../../devops/configuration-management/index.md). If the `bootstrap_config` section is missing from the configuration, use the list of all [static nodes](../../concepts/glossary.md#static-node) of the cluster specified in the `hosts` section of the cluster configuration.
3. Modify the configuration by adding `boot_mode: RECOVERY` to the `bootstrap_config` section of the tablet being recovered.

   - When using configuration V1, you need to modify the [static configuration](../../devops/configuration-management/configuration-v1/static-config.md) on all nodes where the tablet being recovered can run.
   - When using configuration V2, follow the [instructions](../../devops/configuration-management/configuration-v2/update-config.md).
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

4. Restart all nodes where the tablet being recovered can run. If any node is unavailable and cannot be restarted, isolate it from the cluster over the network — for example, using a firewall.

   {% note warning %}

   Make sure that all nodes where the tablet being recovered can run are restarted with the updated configuration or isolated from the cluster over the network before starting recovery. During recovery, old data is erased and replaced with data from the backup. If at that moment the tablet starts in normal mode on a node with the old configuration, it may start working with partially recovered data.

   {% endnote %}
5. Make sure that:

   - There are no issues with the tablet in [HealthCheck](../../reference/ydb-sdk/health-check-api.md).
   - The tablet is not restarting.
   - The recovery form is available in the tablet's App in the [Embedded UI](../../reference/embedded-ui/index.md).

## Step 2. Find the backup files {#find-backup-files}

1. Determine which hosts to search. First, check the hosts where the tablet was running before the failure. These hosts can be identified using logs or a monitoring system.

   If you cannot determine specific hosts, check all hosts where the tablet could have been running. This list is located in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../../devops/configuration-management/index.md). If the `bootstrap_config` section is missing from the configuration, use the list of all [static nodes](../../concepts/glossary.md#static-node) of the cluster specified in the `hosts` section of the cluster configuration.
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

   - `timestamp` — backup creation time.
   - `generation` — [tablet generation](../../concepts/glossary.md#tablet-generation), increases with each tablet restart.
   - `step` — tablet step within a generation, increases with each change of tablet state.

   Select the backup with the **maximum generation**, and if generations are equal, with the **maximum step**. If backups are found on multiple hosts, compare them and select the most recent one.
4. Make sure the backup is fully written. The selected backup must contain the `snapshot` directory, **not** `snapshot.tmp`. The presence of `snapshot.tmp` means that the snapshot write was not completed and the backup is not suitable for recovery. In this case, select the previous most recent backup.


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

## Step 3. Transfer the backup files {#transfer-backup-files}

1. Determine which host the tablet is running on in Recovery mode. To do this, open the [Embedded UI](../../reference/embedded-ui/index.md) and find the node where the tablet is running.
2. If the backup files are on a different host, copy them to the host with the tablet in Recovery mode using `scp`, `rsync`, or any other available tool:


   ```bash
   scp -r /tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222 \
       user@target-host:~/backup_20251007T193502_g214_s1222
   ```

3. Make sure the files are readable by the {{ ydb-short-name }} process on the target host.

## Step 4. Perform the restore {#perform-recovery}

1. Open the App of the tablet being restored in the [Embedded UI](../../reference/embedded-ui/index.md).
2. In the restore form, specify the full path to the directory with the backup files, for example:


   ```text
   /tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222
   ```

3. If necessary, set the flags:

   - **Dry Run** — performs a trial restore without making changes to the storage. Allows you to verify the correctness of the backup.
   - **Skip Checksum Validation** — skips checksum validation of the backup files.

   {% note warning %}

   Always perform a Dry Run before the first restore to verify the correctness of the backup.

   {% endnote %}

   {% note warning %}

   Skip checksum validation only if you have manually edited the backup files. In other cases, it is recommended to keep validation enabled to ensure the integrity of the restored data.

   {% endnote %}
4. Click the **Start Restore** button. Before starting the restore, the system will ask for confirmation, as the operation overwrites the existing tablet data. Confirm the action to start. After the restore starts, the form becomes unavailable — a restart is not possible until the tablet is restarted.

   {% note info %}

   You can also start the restore using `curl` by sending a POST request to the tablet's App page with the `restoreBackup` parameter:


   ```bash
   curl -X POST "http://<host>:<mon_port>/tablets/app?TabletID=72057594037968897&restoreBackup=/tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222"
   ```


   Where `<host>` is the address of the cluster node, `<mon_port>` is the monitoring port of this node.

   {% endnote %}
5. Monitor the restore progress. The page **does not update automatically** — to get the current status, refresh the page manually. The duration of the restore depends on the size of the backup.

   {% note info %}

   The restore is performed server-side and does not depend on the browser. You can close the page or browser — this will **not interrupt** the restore process. When you reopen the page, the current status will be displayed.

   {% endnote %}

   The current operation status is displayed below the form. Possible statuses:

   - `Restoring from '<путь>'` — restore in progress, data from the backup is being read and written to storage. Additionally, a **progress bar** with the operation completion percentage is displayed.
   - `Restore from '<путь>' completed successfully` — restore completed successfully. You can proceed to the next step.
   - `Restore from '<путь>' completed, but changelog is not fully restored` — the tablet's main data has been restored, but the tail of the change log in the backup is damaged, and some recent changes are lost. Review which data was lost and, if necessary, restore it manually, or proceed to the next step.
   - `Restore from '<путь>' failed: <описание ошибки>` — restore completed with an error. Review the error description and retry if necessary.

   To forcefully interrupt the restore, **restart the tablet**.
6. Wait for the restore to complete successfully. If the restore form becomes available again (the **Start Restore** button is active, status reset), it means the tablet was restarted and the restore was interrupted. In this case, start the restore again from step 2.

## Step 5. Return the tablet to normal operation {#return-to-normal}

After successful restore:

1. Determine the list of nodes on which the system tablet being restored can run. This list is located in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../../devops/configuration-management/index.md). If the `bootstrap_config` section is missing from the configuration, use the list of all [static nodes](../../concepts/glossary.md#static-node) of the cluster specified in the `hosts` section of the cluster configuration.
2. Change the configuration by removing `boot_mode: RECOVERY` from the `bootstrap_config` section of the tablet being restored.

   - When using configuration V1, you need to change the [static configuration](../../devops/configuration-management/configuration-v1/static-config.md) on all nodes where the tablet being restored can run.
   - When using configuration V2, follow the [instructions](../../devops/configuration-management/configuration-v2/update-config.md).
3. Restart all nodes where the tablet being restored can run. If any nodes were isolated from the cluster over the network in the previous steps, remove the network isolation.
4. Make sure that:

   - There are no issues with the tablet in [HealthCheck](../../reference/ydb-sdk/health-check-api.md).
   - The tablet is not restarting.
   - The tablet's App in [Embedded UI](../../reference/embedded-ui/index.md) does not have a recovery form.
