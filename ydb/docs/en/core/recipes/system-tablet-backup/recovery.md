# Recovering system tablets {#recovery-guide}

{% note info %}

For conceptual information about the mechanism, see [Backup of system tablets](../../concepts/backup.md#system-tablet-backup).

{% endnote %}

{% note warning %}

Recovering system tablets is a critical operation that may result in data loss. Perform it only when you clearly understand the problem and after consulting the operations team. Before you start, make sure you have read all steps.

{% endnote %}

## Step 1. Switch the tablet to Recovery mode {#enable-recovery-mode}

The tablet to recover must be switched to Recovery mode. In this mode, the tablet starts and is available through [Embedded UI](../../reference/embedded-ui/index.md), but **does not operate normally** and **does not read data from distributed storage**, which allows recovery operations. Other tablets continue to operate normally so the cluster keeps running, but some control-plane operations may be unavailable.

{% note warning %}

If recovery is performed after a complete loss of a [static group](../../concepts/glossary.md#static-group), all system tablets must be switched to Recovery mode at the same time as the static group is recreated on new hosts. If you recreate the static group without switching tablets to Recovery mode, they will automatically start on top of an empty static group, which leads to incorrect cluster operation.

{% endnote %}

1. Determine the ID of the system tablet to recover. You can find the tablet ID in the Tablets section of [Embedded UI](../../reference/embedded-ui/index.md).
2. Determine the list of nodes where the system tablet to recover may run. This list is in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../../devops/configuration-management/index.md). If `bootstrap_config` is missing from the configuration, use the list of all cluster [static nodes](../../concepts/glossary.md#static-node) specified in the `hosts` section of the cluster configuration.
3. Change the configuration by adding `boot_mode: RECOVERY` to the `bootstrap_config` section of the tablet to recover.
    - If you use configuration V1, change the [static configuration](../../devops/configuration-management/configuration-v1/static-config.md) on all nodes where the tablet to recover may run.
    - If you use configuration V2, follow the [instructions](../../devops/configuration-management/configuration-v2/update-config.md).
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

4. Restart all nodes where the tablet to recover may run. If any node is unavailable and cannot be restarted, isolate it from the cluster at the network level — for example, using a firewall.

    {% note warning %}

    Make sure all nodes where the tablet to recover may run are restarted with the updated configuration or isolated from the cluster at the network level before recovery starts. During recovery, old data is erased and replaced with data from the backup copy. If the tablet starts in normal mode on a node with the old configuration during recovery, it may begin operating with partially recovered data.

    {% endnote %}

5. Make sure that:
    - The tablet has no issues in [HealthCheck](../../reference/ydb-sdk/health-check-api.md).
    - The tablet is not restarting.
    - The recovery form is available in the tablet App in [Embedded UI](../../reference/embedded-ui/index.md).

## Step 2. Find backup files {#find-backup-files}

1. Determine which hosts to check. First, check the hosts where the tablet ran before the failure. You can identify these hosts using logs or the monitoring system.

    If you cannot identify specific hosts, check all hosts where the tablet could have run. This list is in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../../devops/configuration-management/index.md). If `bootstrap_config` is missing from the configuration, use the list of all cluster [static nodes](../../concepts/glossary.md#static-node) specified in the `hosts` section of the cluster configuration.

2. Find the backup directory. On each candidate host, check for backups. The backup path is defined by the `path` parameter in `system_tablet_backup_config`:

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

3. Choose the most up-to-date backup copy. Each backup name contains key information: `backup_<timestamp>_g<generation>_s<step>`, where:

    - `timestamp` — backup creation time;
    - `generation` — [tablet generation](../../concepts/glossary.md#tablet-generation), incremented on each tablet restart;
    - `step` — tablet step within the generation, incremented on each tablet state change.

    Choose the backup with the **maximum generation**, and if generations are equal — with the **maximum step**. If backups are found on several hosts, compare them and choose the most up-to-date one.

4. Make sure the backup copy is fully written. The chosen backup must contain a `snapshot` directory, **not** `snapshot.tmp`. A `snapshot.tmp` directory means snapshot writing did not finish and the copy cannot be used for recovery. In that case, choose the previous most up-to-date copy.

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

## Step 3. Transfer backup files {#transfer-backup-files}

1. Determine which host is running the tablet in Recovery mode. Open [Embedded UI](../../reference/embedded-ui/index.md) and find the node where the tablet is running.
2. If backup files are on another host, copy them to the host with the tablet in Recovery mode using `scp`, `rsync`, or any other available tool:

    ```bash
    scp -r /tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222 \
        user@target-host:~/backup_20251007T193502_g214_s1222
    ```

3. Make sure the files are readable by the {{ ydb-short-name }} process on the target host.

## Step 4. Perform recovery {#perform-recovery}

1. Open the App of the tablet to recover in [Embedded UI](../../reference/embedded-ui/index.md).

2. In the recovery form, specify the full path to the backup file directory, for example:

    ```text
    /tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222
    ```

3. If needed, set the flags:

    - **Dry Run** — performs a trial recovery without changing storage. Lets you verify that the backup copy is valid.
    - **Skip Checksum Validation** — skips checksum validation of backup files.

    {% note warning %}

    Always run Dry Run before the first recovery to verify that the backup copy is valid.

    {% endnote %}

    {% note warning %}

    Skip checksum validation only if you manually edited backup files. Otherwise, keep validation enabled to ensure integrity of recovered data.

    {% endnote %}

4. Click **Start Restore**. Before recovery starts, the system asks for confirmation because the operation overwrites existing tablet data. Confirm to start. After recovery starts, the form becomes unavailable — you cannot start again until the tablet is restarted.

    {% note info %}

    You can also start recovery with `curl` by sending a POST request to the tablet App page with the `restoreBackup` parameter:

    ```bash
    curl -X POST "http://<host>:<mon_port>/tablets/app?TabletID=72057594037968897&restoreBackup=/tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222"
    ```

    Where `<host>` is the cluster node address and `<mon_port>` is that node's monitoring port.

    {% endnote %}

5. Monitor recovery progress. The page **does not refresh automatically** — refresh it manually for the current status. Recovery duration depends on backup size.

    {% note info %}

    Recovery runs on the server side and does not depend on the browser. You can close the page or browser — this **does not interrupt** recovery. When you open the page again, the current status is shown.

    {% endnote %}

    The current operation status is shown below the form. Possible statuses:

    - `Restoring from '<path>'` — recovery is in progress; data from the backup copy is read and written to storage. A **progress bar** with the completion percentage is also shown.
    - `Restore from '<path>' completed successfully` — recovery completed successfully. You can proceed to the next step.
    - `Restore from '<path>' completed, but changelog is not fully restored` — main tablet data is recovered, but the changelog tail in the backup copy is damaged and some recent changes are lost. Review what data was lost and recover it manually if needed, or proceed to the next step.
    - `Restore from '<path>' failed: <error description>` — recovery failed with an error. Review the error description and retry if needed.

    To force-stop recovery, **restart the tablet**.

6. Wait for recovery to complete successfully. If the recovery form becomes available again (**Start Restore** is active, status is reset), the tablet was restarted and recovery was interrupted. In that case, start recovery again from step 2.

## Step 5. Return the tablet to normal operation {#return-to-normal}

After successful recovery:

1. Determine the list of nodes where the recovered system tablet may run. This list is in the `bootstrap_config` section of the corresponding tablet in the [cluster configuration](../../devops/configuration-management/index.md). If `bootstrap_config` is missing from the configuration, use the list of all cluster [static nodes](../../concepts/glossary.md#static-node) specified in the `hosts` section of the cluster configuration.
2. Change the configuration by removing `boot_mode: RECOVERY` from the `bootstrap_config` section of the recovered tablet.
    - If you use configuration V1, change the [static configuration](../../devops/configuration-management/configuration-v1/static-config.md) on all nodes where the tablet may run.
    - If you use configuration V2, follow the [instructions](../../devops/configuration-management/configuration-v2/update-config.md).
3. Restart all nodes where the tablet may run. If any nodes were isolated from the cluster at the network level in previous steps, remove the isolation.
4. Make sure that:
    - The tablet has no issues in [HealthCheck](../../reference/ydb-sdk/health-check-api.md).
    - The tablet is not restarting.
    - The recovery form is absent in the tablet App in [Embedded UI](../../reference/embedded-ui/index.md).
