# Recovering system tablets {#recovery-guide}

{% note info %}

For conceptual information about the mechanism, see the [System tablet backup](../../../concepts/backup.md#system-tablet-backup) section.

{% endnote %}

{% note warning %}

Recovering system tablets is a critical operation that may result in data loss. Perform it only if you have a clear understanding of the problem and after consulting with the operations team. Before starting, make sure you have reviewed all the steps.

{% endnote %}

## Step 1. Put the tablet into Recovery mode {#enable-recovery-mode}

The tablet to be recovered must be put into [Recovery mode](../../../concepts/glossary.md#tablet-recovery-mode).

{% note warning %}

If recovery is performed after a complete loss of the [static group](../../../concepts/glossary.md#static-group), all system tablets must be put into Recovery mode simultaneously with recreating the static group on new hosts. If you recreate the static group without putting the tablets into Recovery mode, they will automatically start on top of an empty static group, leading to incorrect cluster operation.

{% endnote %}

1. Determine the ID of the system tablet to be recovered. The tablet ID can be found in the Tablets section of the [{{ ydb-ui-name }}](../../../reference/ydb-ui/index.md).
2. Save the current [cluster configuration](../../../devops/configuration-management/index.md) to a file `config.yaml`.
    - When using configuration V1, save the [static configuration](../../../devops/configuration-management/configuration-v1/static-config.md).
    - When using configuration V2, follow the [instructions](../../../devops/configuration-management/configuration-v2/update-config.md).
3. Determine the list of hosts where the system tablet to be recovered can run based on the cluster configuration. This list will be used in subsequent steps for restarting nodes and updating the configuration.

    For this, it is convenient to use a script that takes the tablet ID and the cluster configuration file as input, which stores the list of nodes where system tablets can run and the mapping from nodes to hosts. The script combines this information and outputs the list of hosts where the given system tablet can run, in a format suitable for pssh.

    Create an isolated environment with PyYAML:

    ```bash
    python3 -m venv ./recovery && ./recovery/bin/pip install pyyaml
    ```

    Create the script file:

    ```bash
    cat > find-tablet-hosts.py <<'EOF'
    #!/usr/bin/env python3
    import argparse
    import yaml


    def find_nodes(cfg, tid):
        bootstrap = (cfg.get("bootstrap_config") or {}).get("tablet") or []
        for tablet in bootstrap:
            if str(tablet.get("info", {}).get("tablet_id")) == tid:
                return tablet.get("node") or []

        for tablets_by_type in (cfg.get("system_tablets") or {}).values():
            for tablet in tablets_by_type or []:
                if str(tablet.get("info", {}).get("tablet_id")) == tid:
                    return tablet.get("node") or []

        return []


    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", required=True, help="Path to cluster configuration file")
    parser.add_argument("--tablet-id", required=True, help="System tablet id")
    args = parser.parse_args()

    with open(args.config_path) as f:
        cfg = yaml.safe_load(f) or {}

    cfg = cfg.get("config", cfg)

    hosts_by_id = {h["node_id"]: h["host"] for h in cfg.get("hosts") or []}

    node_ids = find_nodes(cfg, args.tablet_id)
    if not node_ids:
        node_ids = list(hosts_by_id)

    for node_id in sorted(set(node_ids)):
        host = hosts_by_id.get(node_id)
        if host is not None:
            print(host)
    EOF
    ```

    Run the script, substituting the path to your configuration and the tablet ID:

    Example:

    ```bash
    ./recovery/bin/python find-tablet-hosts.py --config-path config.yaml --tablet-id <tablet-id> > hosts.txt
    ```

4. Modify the saved configuration by adding `boot_type: RECOVERY` to the startup configuration section of the tablet being recovered and reduce the list of nodes where the tablet being recovered can run (the `node` array) to a single node.
    Example for tablet `Hive` with ID `72057594037968897`:
    - When using `bootstrap_config`:

        ```yaml
            bootstrap_config:
                tablet:
                - type: FLAT_HIVE
                    node:
                    - 1
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
                    boot_type: RECOVERY
        ```

    - When using `system_tablets`:

        ```yaml
        flat_hive:
        - info:
            tablet_id: 72057594037968897
          node:
          - 9
          boot_type: RECOVERY
        ```

5. Update the configuration in the cluster.
    - When using configuration V1, update the static configuration on all hosts obtained in step 3 using the command:

        {% include [pssh-config-update](_includes/pssh-config-update.md) %}

    - When using configuration V2, follow the [instructions](../../../devops/configuration-management/configuration-v2/update-config.md).

6. Restart all nodes where the tablet being recovered can run. If any node is unavailable and cannot be restarted, isolate it from the cluster over the network — for example, using a firewall.

    {% include [pssh-restart-nodes](_includes/pssh-restart-nodes.md) %}

    {% note warning %}

    Make sure that all nodes where the tablet being recovered can run are restarted with the updated configuration or isolated from the cluster over the network before starting recovery. During recovery, old data is erased and replaced with data from the backup. If at that moment the tablet starts in normal mode on a node with the old configuration, it may start working with partially recovered data.

    {% endnote %}

7. Make sure that:
    - There are no issues with the tablet in [HealthCheck](../../../reference/ydb-sdk/health-check-api.md).
    - The tablet is not restarting.
    - The recovery form is available in the tablet's App in the [{{ ydb-ui-name }}](../../../reference/ydb-ui/index.md).

## Step 2. Find the backup files {#find-backup-files}

1. On each host obtained in step 3, check for the presence of backups. The path to backups is determined by the `path` parameter in the [`system_tablet_backup_config`](../../../reference/configuration/system_tablet_backup_config.md) configuration section.

    The name of each backup contains key information: `backup_<timestamp>_g<generation>_s<step>`, where:

    - `timestamp` — backup creation time;
    - `generation` — [tablet generation](../../../concepts/glossary.md#tablet-generation), increases with each tablet restart;
    - `step` — tablet step within a generation, increases with each change in tablet state.

    {% include [pssh-find-backups](_includes/pssh-find-backups.md) %}

2. Select the most recent backup suitable for recovery.

   Select the backup with the **maximum generation**, and if generations are equal, with the **maximum step**.

   Make sure the backup is fully written. The selected backup must contain the `snapshot` directory, **not** `snapshot.tmp`. The presence of `snapshot.tmp` means that the snapshot write was not completed and the backup is not suitable for recovery. In this case, select the previous most recent backup.

   {% include [check-backup](_includes/check-backup.md) %}

   If the checksums differ, two situations are possible:
   - The last entry in `changelog.json` was not fully written. In this case, the checksum stored in `changelog.json.sha256` is found in one of the entries in `changelog.json` in the `prev_sha256` field. To recover, you need to edit the files: remove/complete the incompletely written entries in `changelog.json` and update the checksum in `changelog.json.sha256`.
   - Data corruption has occurred, recovery is impossible.

## Step 3. Transfer the backup files {#transfer-backup-files}

1. Determine which host the tablet is running on in Recovery mode. To do this, open the [{{ ydb-ui-name }}](../../../reference/ydb-ui/index.md) and find the host where the tablet is running.
2. If the backup files are on a different host, copy them to the host with the tablet in Recovery mode using `scp`, `rsync`, or any other available tool:

    Copy the backup from the backup directory to your home directory:

   {% include [copy-backup](_includes/copy-backup.md) %}

    Copy the backup to the target host:

    ```bash
    scp -r ~/backup_20251007T193502_g214_s1222 target-host:~/backup_20251007T193502_g214_s1222
    ```

    If direct copying between hosts is not available, copy through an intermediate host:

    ```bash

    scp -r backup-host:~/backup_20251007T193502_g214_s1222 backup_20251007T193502_g214_s1222

    scp -r backup_20251007T193502_g214_s1222 target-host:~/backup_20251007T193502_g214_s1222
    ```

    {% include [chown-backup](_includes/chown-backup.md) %}

## Step 4. Perform the recovery {#perform-recovery}

1. Open the App of the tablet being restored in the [{{ ydb-ui-name }}](../../../reference/ydb-ui/index.md).

2. In the recovery form, specify the full path to the directory with the backup files, for example:

    ```text
    /path/to/backup/directory/hive/72057594037968897/backup_20251007T193502_g214_s1222
    ```

3. If necessary, set the flags:

    - **Dry Run** — performs a trial recovery without making changes to the storage. Allows you to verify the correctness of the backup. After the trial recovery completes, you must restart the tablet.
    - **Skip Checksum Validation** — skips the checksum verification of the backup files.

    {% note warning %}

    Always perform a Dry Run before recovery to verify the correctness of the backup.

    {% endnote %}

    {% note warning %}

    Skip checksum verification only if you have manually edited the backup files. In other cases, it is recommended to keep verification enabled to ensure the integrity of the restored data.

    {% endnote %}

4. Click the **Start Restore** button. Before starting the recovery, the system will ask for confirmation, as the operation overwrites the existing tablet data. Confirm the action to start. After the recovery starts, the form becomes unavailable — a restart is not possible until the tablet is restarted.

    {% note info %}

    You can also start the recovery using `curl` by sending a POST request to the tablet's App page with the `restoreBackup` parameter:

    ```bash
    curl -X POST "http://<host>:<mon_port>/tablets/app?TabletID=72057594037968897&restoreBackup=/tablet/hive/72057594037968897/backup_20251007T193502_g214_s1222"
    ```

    Where `<host>` is the cluster node address, `<mon_port>` is the monitoring port of this node.

    {% endnote %}

5. Monitor the recovery progress. The page **does not update automatically** — to get the current status, refresh the page manually. The recovery duration depends on the backup size.

    {% note info %}

    The recovery is performed server-side and does not depend on the browser. You can close the page or browser — this **will not interrupt** the recovery process. When you reopen the page, the current status will be displayed.

    {% endnote %}

    The current operation status is displayed below the form. Possible statuses:

    - `Restoring from '<path>'` — recovery is in progress, data from the backup is being read and written to the storage. Additionally, a **progress bar** with the operation completion percentage is displayed.
    - `Restore from '<path>' completed successfully` — recovery completed successfully. You can proceed to the next step.
    - `Restore from '<path>' completed, but changelog is not fully restored` — the main tablet data has been restored, but the tail of the change log in the backup is damaged, and some recent changes are lost. Examine which data was lost and, if necessary, restore it manually, or proceed to the next step.
    - `Restore from '<path>' failed: <error description>` — recovery completed with an error. Examine the error description and retry if necessary.

    To forcibly interrupt the recovery, **restart the tablet**.

6. Wait for the recovery to complete successfully. If the recovery form becomes available again (the **Start Restore** button is active, the status is reset), this means the tablet was restarted and the recovery was interrupted. In this case, start the recovery again from step 2.

## Step 5. Return the tablet to normal operation mode {#return-to-normal}

After successful recovery:

1. Restore the configuration to its original state by removing `boot_type: RECOVERY` from the startup configuration section of the tablet being recovered and restoring the original list of nodes where the tablet being recovered can run.
    - When using configuration V1, update the static configuration on all hosts obtained in step 3 using the command:

        {% include [pssh-config-rollback](_includes/pssh-config-rollback.md) %}

    - When using configuration V2, follow the [instructions](../../../devops/configuration-management/configuration-v2/update-config.md).
2. Restart all nodes where the tablet being recovered can run. If any nodes were isolated from the cluster over the network in previous steps, remove the network isolation.

    {% include [pssh-restart-nodes](_includes/pssh-restart-nodes.md) %}

3. Make sure that:
    - There are no issues with the tablet in [HealthCheck](../../../reference/ydb-sdk/health-check-api.md).
    - The tablet does not restart.
    - The recovery form is absent in the tablet's App in [{{ ydb-ui-name }}](../../../reference/ydb-ui/index.md).

4. Restart all cluster nodes one by one to synchronize the state of internal in-memory caches with the tablet state. After restarting each node, wait for it to return to a healthy state and make sure there are no issues in [HealthCheck](../../../reference/ydb-sdk/health-check-api.md); only then proceed to the next node.
