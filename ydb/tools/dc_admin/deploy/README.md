# Single-host mirror-3-dc stand

Nine storage processes represent three data centers on one Linux machine. A separate dynamic node serves `/Root/tpcc`. The database receives nine mirror-3-dc NVMe groups. Group 0 serves the cluster's system tablets. StateStorage, SchemeBoard and StateStorageBoard each start with one ring per storage node.

## Render the stand

Install Python 3.9+, PyYAML and systemd on the target host. Use nine **empty, exclusively assigned** block devices and a dedicated system user and group named `ydb`. Each storage process needs read/write access to its PDisk after every boot. On a three-NVMe host, assign three pre-provisioned partitions or logical volumes from the first NVMe to dc1, three from the second to dc2, and three from the third to dc3. On a one-NVMe host supply nine pre-provisioned volumes in the same order. Verify the parent disks and mountpoints with `lsblk -o PATH,PKNAME,SIZE,FSTYPE,MOUNTPOINTS`.

Run from `ydb/tools/dc_admin`. By default the device names are `/dev/mapper/ydb_dc1_node1_pdisk` through `/dev/mapper/ydb_dc3_node3_pdisk`. For other names, put their full paths in `disks.txt`, one per line in dc1/node1 … dc3/node3 order, and add `--disks disks.txt`.

```sh
python3 deploy/generate.py stand --check-devices --pin-l3
```

The generator writes a new directory with the seed YAML, node inventory and systemd units. `--check-devices` checks all nine paths resolve to distinct block devices. Omit it to render files offline. `--pin-l3` allocates a different L3 cache to each YDB process, including compute. Omit it for normal OS scheduling. `--dynamic-nodes 2` or `3` adds a compute process in each additional DC. Each compute process has its own ports and can register through any storage node.

## Install on the test host

Install `ydb` and `ydbd` from this branch in `/opt/ydb/bin/`. Ensure `ip6-localhost` resolves to `::1`. Run the following on a fresh stand, in Bash, from the same directory. Keep the application, units and inventory root-owned. The per-node directories hold mutable YDB configs.

```bash
sudo install -d /opt/ydb/dc-admin /etc/ydb-dc /var/lib/ydb-dc
sudo install -m 0644 *.py index.html /opt/ydb/dc-admin/
sudo install -m 0644 stand/nodes.json stand/config.yaml /etc/ydb-dc/
sudo install -m 0644 stand/systemd/* /etc/systemd/system/
sudo install -d -o ydb -g ydb -m 0700 /var/lib/ydb-dc/admin
for service in stand/systemd/ydb_dc?_node?.service stand/systemd/ydb_tpcc_dynamic*.service; do
    dir=$(sed -n 's/^WorkingDirectory=//p' "$service")
    sudo install -d -o ydb -g ydb -m 0700 "$dir"
    sudo -u ydb test ! -e "$dir/config.yaml" || exit 1
    sudo -u ydb /opt/ydb/bin/ydb admin node config init --config-dir "$dir" --from-config /etc/ydb-dc/config.yaml || exit 1
done
sudo systemctl daemon-reload
sudo systemctl enable --now ydb_dc{1..3}_node{1..3}.service
```

Bootstrap the cluster with a fresh UUID. Keep that UUID for retries. Once the static nodes complete startup, create the database, then start its compute services.

```bash
cluster_uuid=$(cat /proc/sys/kernel/random/uuid)
/opt/ydb/bin/ydb -e grpc://ip6-localhost:2135 admin cluster bootstrap --uuid "$cluster_uuid"
/opt/ydb/bin/ydbd -s grpc://ip6-localhost:2135 admin database /Root/tpcc create nvme:9
for service in stand/systemd/ydb_tpcc_dynamic*.service; do sudo systemctl enable --now "${service##*/}"; done
/opt/ydb/bin/ydb -e grpc://ip6-localhost:2145 -d /Root/tpcc sql -s 'SELECT 1;'
sudo systemctl enable --now ydb_dc_monitor.socket ydb_dc_admin.service
```

Open the admin at `http://[::1]:8765` and YDB UI at `http://[::1]:31001/monitoring/`. From your workstation, forward both loopback ports with `ssh -N -L '[::1]:8765:[::1]:8765' -L '[::1]:31001:[::1]:31001' HOST`. The **Groups** page should show group 0 and nine groups in `/Root/tpcc:nvme`. **Start TPC-C** creates the workload tables and initializes 100 warehouses under `/Root/tpcc`.

## Host controls and offline recovery

For the host/DC start and stop buttons install this rule with `visudo -f /etc/sudoers.d/ydb-dc` (sudo 1.9.10+). The command-argument regexp restricts systemctl to the nine storage units.

```sudoers
ydb ALL=(root) NOPASSWD: /usr/bin/systemctl ^(start|stop) (ydb_dc[1-3]_node[1-3][.]service)( ydb_dc[1-3]_node[1-3][.]service)*$
```

For offline config editing, install the `dc-admin-prepare-solo` executable from `prepare_solo/` and a symlink named `ydbd-recovery` pointing to this branch's `ydbd` under `/opt/ydb/bin/`. Create `/opt/ydb/bin/recovery-archive` with mode 0700, owned by `ydb`. Set `DC_ADMIN_STATIC_RECOVERY_DIR=/opt/ydb/bin/recovery-archive` in `/etc/ydb-dc/admin.env` and restart `ydb_dc_admin.service`. The tool saves each operation's original PDisk metadata in this directory.
