# Recovering System Tablets {#recovery-guide}

{% note info %}

For conceptual information about the mechanism, see the [System tablet backup](../../../concepts/backup.md#system-tablet-backup) section.

{% endnote %}

{% note warning %}

Recovering system tablets is a critical operation that may result in data loss. Perform it only if you have a clear understanding of the problem and after consulting with the operations team. Before starting, make sure you have reviewed all the steps.

{% endnote %}

## Step 1. Put the Tablet into Recovery Mode {#enable-recovery-mode}

The tablet to be recovered must be put into [Recovery mode](../../../concepts/glossary.md#tablet-recovery-mode).

{% note warning %}

If recovery is performed after a complete loss of the [static group](../../../concepts/glossary.md#static-group), all system tablets must be put into Recovery mode simultaneously with recreating the static group on new hosts. If you recreate the static group without putting the tablets into Recovery mode, they will automatically start on top of an empty static group, leading to incorrect cluster operation.

{% endnote %}

1. Determine the ID of the system tablet to be recovered. The tablet ID can be found in the Tablets section of the [{{ ydb-ui-name }}](../../../reference/ydb-ui/index.md).
2. Save the current [cluster configuration](../../../devops/configuration-management/index.md) to the file `config.yaml`.
    - При использовании конфигурации V1, необходимо сохранить [статическую конфигурацию](../../../devops/configuration-management/configuration-v1/static-config.md).
    - При использовании конфигурации V2, воспользуйтесь [инструкцией](../../../devops/configuration-management/configuration-v2/update-config.md).
3. Determine the list of hosts where the system tablet to be recovered can run based on the cluster configuration. This list will be used in further steps for restarting nodes and updating the configuration.

    Для этого удобно воспользоваться скриптом, который принимает на вход идентификатор таблетки и файл конфигурации кластера, в котором хранится список узлов, где могут работать системные таблетки, и отображение из узлов в хосты. Скрипт объединяет эту информацию, и в результате выдает список хостов, на которых может работать переданная системная таблетка, в пригодном для pssh формате.

    Создайте изолированное окружение с PyYAML:

    ```bash
    python3 -m venv ./recovery && ./recovery/bin/pip install pyyaml
    ```

    Создайте файл скрипта:

    ```bash
    cat > find-tablet-hosts.py <<'EOF'
    #!/usr/bin/env python3
    import argparse
    import yaml


    def find_nodes(cfg, tid):
        bootstrap = (cfg.get("`bootstrap_config`") or {}).get("tablet") or []
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

    Запустите скрипт, подставив путь к своей конфигурации и идентификатор таблетки:

    Пример:

    ```bash
    ./recovery/bin/python find-tablet-hosts.py --config-path config.yaml --tablet-id <tablet-id> > hosts.txt
    ```

4. Modify the saved configuration by adding `boot_type: `RECOVERY`` to the launch configuration section of the tablet being recovered and reduce the list of nodes where the tablet being recovered can run (the `node` array) to a single node.
    Пример для таблетки `Hive` с идентификатором `72057594037968897`:
    - При использовании `bootstrap_config`:

        ```yaml
            `bootstrap_config`:
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
                    boot_type: `RECOVERY`
        ```

    - При использовании `system_tablets`:

        ```yaml
        flat_hive:
        - info:
            tablet_id: 72057594037968897
          node:
          - 9
          boot_type: `RECOVERY`
        ```

5. Update the configuration in the cluster.
    - При использовании конфигурации V1 необходимо обновить статическую конфигурацию на всех хостах, полученных на шаге 3, с помощью команды:

        {% include [pssh-config-update](_includes/pssh-config-update.md) %}

    - При использовании конфигурации V2, воспользуйтесь [инструкцией](../../../devops/configuration-management/configuration-v2/update-config.md).

6. Restart all nodes where the tablet being recovered can run. If any node is unavailable and cannot be restarted, isolate it from the cluster over the network — for example, using a firewall.

    {% include [pssh-restart-nodes](_includes/pssh-restart-nodes.md) %}

    {% note warning %}

    Убедитесь, что все узлы, на которых может работать восстанавливаемая таблетка, перезапущены с обновлённой конфигурацией или изолированы от кластера по сети до начала восстановления. В процессе восстановления старые данные стираются и заменяются данными из резервной копии. Если в этот момент таблетка запустится в обычном режиме на узле со старой конфигурацией, она может начать работу с частично восстановленными данными.

    {% endnote %}

7. Make sure that:
    - С таблеткой нет проблем в [HealthCheck](../../../reference/ydb-sdk/health-check-api.md).
    - Таблетка не перезапускается.
    - В App таблетки в [{{ ydb-ui-name }}](../../../reference/ydb-ui/index.md) доступна форма восстановления.

## Step 2. Find the Backup Files {#find-backup-files}

1. On each host obtained in step 3, check for the presence of backups. The path to backups is determined by the `path` parameter in the [`system_tablet_`backup_`config`](../../../reference/configuration/`system_tablet_`backup_`config`.md) configuration section.

    Имя каждой резервной копии содержит ключевую информацию: `backup_<timestamp>_g<generation>_s<step>`, где:

    - `timestamp` — время создания резервной копии;
    - `generation` — [поколение таблетки](../../../concepts/glossary.md#tablet-generation), увеличивается при каждом перезапуске таблетки;
    - `step` — шаг таблетки в рамках поколения, увеличивается при каждом изменении состояния таблетки.

    {% include [pssh-find-backups](_includes/pssh-find-backups.md) %}

2. Select the most recent backup suitable for recovery.

   Select the backup with the **maximum generation**, and if generations are equal, with the **maximum step**.

   Make sure the backup is fully written. The selected backup must contain the `snapshot` directory, **not** `snapshot.tmp`. The presence of `snapshot.tmp` means that the snapshot write was not completed and the backup is not suitable for recovery. In this case, select the previous most recent backup.

   {% include [check-backup](_includes/check-backup.md) %}

   If the checksums differ, two situations are possible:
   - The last record in `changelog.json` was not fully written. In this case, the checksum stored in `changelog.json.sha256` is located in one of the records in `changelog.json` in the `prev_sha256` field. For recovery, you need to edit the files: delete/complete the incompletely written records in `changelog.json` and update the checksum in `changelog.json.sha256`.
   - Data corruption has occurred; recovery is impossible.

## Step 3. Transfer the Backup Files {#transfer-backup-files}

1. Determine which host the tablet is running on in Recovery mode. To do this, open the [{{ ydb-ui-name }}](../../../reference/ydb-ui/index.md) and find the host where the tablet is running.
2. If the backup files are on a different host, copy them to the host with the tablet in Recovery mode using `scp`, `rsync`, or any other available tool:

    Скопируйте бекап из директории с бекапами в свою домашнюю директорию:

   {% include [copy-backup](_includes/copy-backup.md) %}

    Скопируйте бекап на целевой хост:

    ```bash
    scp -r ~/`backup_`20251007T193502_g214_s1222 target-host:~/`backup_`20251007T193502_g214_s1222
    ```

    В случае, если операция копирования между хостами напрямую недоступна, то необходимо копировать через промежуточный хост:

    ```bash

    scp -r backup-host:~/`backup_`20251007T193502_g214_s1222 `backup_`20251007T193502_g214_s1222

    scp -r `backup_`20251007T193502_g214_s1222 target-host:~/`backup_`20251007T193502_g214_s1222
    ```

    {% include [chown-backup](_includes/chown-backup.md) %}

## Step 4. Perform the Recovery {#perform-recovery}

1. Open the App of the tablet being restored in the [{{ ydb-ui-name }}](../../../reference/ydb-ui/index.md).

2. In the recovery form, specify the full path to the directory with the backup files, for example:

    ```text
    /path/to/backup/directory/hive/72057594037968897/`backup_`20251007T193502_g214_s1222
    ```

3. If necessary, set the flags:

    - **Dry Run** — выполняет пробное восстановление без внесения изменений в хранилище. Позволяет убедиться в корректности резервной копии. После завершения пробного восстановления необходимо перезапустить таблетку.
    - **Skip Checksum Validation** — пропускает проверку контрольных сумм файлов резервной копии.

    {% note warning %}

    Всегда выполняйте Dry Run перед восстановлением, чтобы убедиться в корректности резервной копии.

    {% endnote %}

    {% note warning %}

    Пропускайте проверку контрольных сумм только в случае ручного редактирования файлов резервной копии. В остальных случаях рекомендуется оставлять проверку включённой для обеспечения целостности восстанавливаемых данных.

    {% endnote %}

4. Click the **Start Restore** button. Before starting the recovery, the system will ask for confirmation, as the operation overwrites the existing tablet data. Confirm the action to start. After the recovery starts, the form becomes unavailable — a restart is not possible until the tablet is restarted.

    {% note info %}

    Восстановление также можно запустить с помощью `curl`, отправив POST-запрос на страницу App таблетки с параметром `restoreBackup`:

    ```bash
    curl -X POST "http://<host>:<`mon_port`>/tablets/app?TabletID=72057594037968897&restoreBackup=/tablet/hive/72057594037968897/`backup_`20251007T193502_g214_s1222"
    ```

    Где `<host>` — адрес узла кластера, `<`mon_port`>` — порт мониторинга этого узла.

    {% endnote %}

5. Monitor the recovery progress. The page **does not update automatically** — to get the current status, refresh the page manually. The recovery duration depends on the backup size.

    {% note info %}

    Восстановление выполняется на стороне сервера и не зависит от браузера. Вы можете закрыть страницу или браузер — это **не прервёт** процесс восстановления. При повторном открытии страницы отобразится актуальный статус.

    {% endnote %}

    Под формой отображается текущий статус операции. Возможные статусы:

    - `Restoring from '<путь>'` — восстановление выполняется, данные из резервной копии считываются и записываются в хранилище. Дополнительно отображается **прогресс-бар** с процентом выполнения операции.
    - `Restore from '<путь>' completed successfully` — восстановление завершено успешно. Можно переходить к следующему шагу.
    - `Restore from '<путь>' completed, but changelog is not fully restored` — основные данные таблетки восстановлены, но хвост журнала изменений в резервной копии повреждён, и часть последних изменений потеряна. Изучите, какие данные были потеряны, и при необходимости восстановите их вручную, либо переходите к следующему шагу.
    - `Restore from '<путь>' failed: <описание ошибки>` — восстановление завершилось с ошибкой. Изучите описание ошибки и при необходимости повторите попытку.

    Для принудительного прерывания восстановления **перезапустите таблетку**.

6. Wait for the recovery to complete successfully. If the recovery form becomes available again (the **Start Restore** button is active, the status is reset), this means the tablet was restarted and the recovery was interrupted. In this case, start the recovery again from step 2.

## Step 5. Return the Tablet to Normal Operation Mode {#return-to-normal}

After successful recovery:

1. Return the configuration to its original state by removing `boot_type: `RECOVERY`` from the launch configuration section of the tablet being recovered and restoring the original list of nodes where the tablet being recovered can run.
    - При использовании конфигурации V1 необходимо обновить статическую конфигурацию на всех хостах, полученных на шаге 3, с помощью команды:

        {% include [pssh-config-rollback](_includes/pssh-config-rollback.md) %}

    - При использовании конфигурации V2, воспользуйтесь [инструкцией](../../../devops/configuration-management/configuration-v2/update-config.md).
2. Restart all nodes where the tablet being recovered can run. If any nodes were isolated from the cluster over the network in previous steps, remove the network isolation.

    {% include [pssh-restart-nodes](_includes/pssh-restart-nodes.md) %}

3. Make sure that:
    - С таблеткой нет проблем в [HealthCheck](../../../reference/ydb-sdk/health-check-api.md).
    - Таблетка не перезапускается.
    - В App таблетки в [{{ ydb-ui-name }}](../../../reference/ydb-ui/index.md) отсутствует форма восстановления.

4. Restart all cluster nodes one by one to synchronize the state of internal in-memory caches with the tablet state. After restarting each node, wait for it to return to a working state and make sure there are no issues in the [HealthCheck](../../../reference/ydb-sdk/health-check-api.md); only then proceed to the next node.
