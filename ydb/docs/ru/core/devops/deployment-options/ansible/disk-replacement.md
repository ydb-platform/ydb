# Replace hard drive

## Requirements
- preinstalled cluster with `3-nodes-mirror-3-dc` configuration
- a failed drive on one or more servers (in this example `/dev/vdb` labelled as `ydb_disk_1` on `static-node-1.ydb-cluster.com`)

## Steps
1. Check that the hard drive is not working in the monitoring UI
![Step 1](./_assets/ydb-disk-replace-step-1.png)
2. Manually replace the hard drive
3. Prepare the replaced hard drive: `ansible-playbook ydb_platform.ydb.prepare_drives -l static-node-1.ydb-cluster.com --extra-vars "ydb_disk_prepare=ydb_disk_1"`
4. Restart the broken drive through the monitoring UI:
![Step 2](./_assets/ydb-disk-replace-step-4.png)
or using the command: `ansible-playbook ydb_platform.ydb.rolling_restart_static -l static-node-1.ydb-cluster.com`
5. Check that the drive is working in the monitoring UI:
![Step 3](./_assets/ydb-disk-replace-step-5.png)