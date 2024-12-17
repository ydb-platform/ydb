import os
import tempfile
import argparse

from ydb.tools.cfg.walle import NopHostsInformationProvider
from ydb.tools.ydbd_slice import nodes, cluster_description


def create_systemd_units(nodes_):
    slice_kikimr_path = '/Berkanavt/kikimr/bin/kikimr'
    slice_lib_dir = '/Berkanavt/kikimr/lib'
    slice_cfg_path = '/Berkanavt/kikimr/cfg'
    user = "yc-user"

    # TODO: configs need proper review
    systemd_unit_storage = """[Unit]
Description=YDB storage node
After=network-online.target rc-local.service
Wants=network-online.target
StartLimitInterval=10
StartLimitBurst=15
[Service]
Restart=always
RestartSec=1
User={user}
PermissionsStartOnly=true
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=ydbd
SyslogFacility=daemon
SyslogLevel=err
Environment=LD_LIBRARY_PATH={lib_dir}
ExecStart={kikimr_path} server --log-level 3 --syslog --tcp --yaml-config  {cfg_path}/config.yaml \
--grpc-port 2135 --ic-port 19001 --mon-port 8765 --node static
LimitNOFILE=65536
LimitCORE=0
LimitMEMLOCK=3221225472
[Install]
WantedBy=multi-user.target
""".format(
        user=user,
        lib_dir=slice_lib_dir,
        kikimr_path=slice_kikimr_path,
        cfg_path=slice_cfg_path,
    )

    systemd_unit_compute_all = """[Unit]
Description=YDB Compute Node all instances
After=network-online.target remote-fs.target time-sync.target
[Service]
Type=oneshot
ExecStart=/bin/bash -c 'for i in `ls /Berkanavt/|awk \'/^kikimr_/ {gsub("kikimr_","",$1); print $1}\'`;do systemctl start kikimr-multi@$i; done || true'
ExecReload=/bin/true
ExecStop=/bin/true
RemainAfterExit=yes
[Install]
WantedBy=multi-user.target
"""

    systemd_unit_compute = """[Unit]
Description=YDB Compute Node
StartLimitInterval=10
StartLimitBurst=15
ConditionPathExists=/Berkanavt/kikimr_%i/env.txt
After=network-online.target remote-fs.target time-sync.target
PartOf=kikimr-multi-all.service
[Service]
Type=simple
MemoryAccounting=yes
MemoryMax=50G
User={user}
RuntimeDirectory=kikimr_slot
RuntimeDirectoryPreserve=yes
PermissionsStartOnly=true
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=kikimr_%i
SyslogFacility=daemon
SyslogLevel=err
Environment=kikimr_main_dir=/Berkanavt/kikimr
Environment=tenant_main_dir=/Berkanavt/tenants
Environment=user=kikimr_slot
Environment=slot=%i
EnvironmentFile=/Berkanavt/kikimr_%i/env.txt
# ExecStartPre=/bin/bash /usr/local/bin/kikimr-multi-scripts/kikimr-start-pre.sh
ExecStart=/bin/bash /etc/systemd/system/kikimr-run.sh
# ExecStartPost=/bin/bash /usr/local/bin/kikimr-multi-scripts/start_post.sh
LimitNOFILE=131072
LimitCORE=0
LimitMEMLOCK=32212254720
KillMode=mixed
TimeoutStopSec=300
Restart=always
RestartSec=1
[Install]
WantedBy=multi-user.target
""".format(
        user=user
    )

    kikimr_run_compute_node_sh = """set -e
[ ! -d /Berkanavt/kikimr/cfg ] && logger -p daemon.err -t kikimr_$slot "No conf dir" && exit 1
if [ -z $tenant ] || [ -z $grpc ] || [ -z $mbus ] || [ -z $ic ] || [ -z $mon ]; then
if [ -s /Berkanavt/kikimr_$slot/slot_cfg ]; then
. /Berkanavt/kikimr_$slot/slot_cfg
else
echo no slot
exit 1
fi
fi
[ ! -s "$kikimr_main_dir/cfg/dynamic_server.cfg" ] && logger -p daemon.err -t kikimr_$slot "No dynamic server config" && exit 1
[ -s ${tenant_main_dir}/location ] && location="--data-center $(cat ${tenant_main_dir}/location)"
[ -s /Berkanavt/kikimr_$slot/sys.txt ] && kikimr_system_file="/Berkanavt/kikimr_$slot/sys.txt"
# Build kikimr_arg from cfg
kikimr_tenant=${tenant} \
kikimr_grpc_port=${grpc} \
kikimr_grpcs_port=${grpcs} \
kikimr_mbus_port=${mbus} \
kikimr_ic_port=${ic} \
kikimr_mon_port=${mon} \
kikimr_home=${kikimr_main_dir} \
kikimr_syslog_service_tag=kikimr_$slot \
. "$kikimr_main_dir/cfg/dynamic_server.cfg"
export kikimr_user="${user}"
export kikimr_bin_path="${kikimr_main_dir}/bin"
#export kikimr_coregen="--core"
export kikimr_log="daemon.err"
kikimr_symlinks_path=/Berkanavt/kikimr/bin/pinned_versions
kikimr_binary_path=/Berkanavt/kikimr/bin/kikimr
kikimr_binary_symlink_path="$kikimr_symlinks_path/${tenant}/kikimr"
if [ -f "$kikimr_binary_symlink_path" ]; then
kikimr_binary_path=$kikimr_binary_symlink_path
fi
if [ -f "/usr/lib/libbreakpad_init.so" ]; then
export LD_PRELOAD=libbreakpad_init.so
export BREAKPAD_MINIDUMPS_PATH=/Berkanavt/minidumps/
fi
taskset=""
if [ -n "$tset" ]; then
taskset="taskset -c $tset"
fi
exec $taskset $kikimr_binary_path $kikimr_arg $location --node-type slot
"""
    mapping = [
        (systemd_unit_storage, "/etc/systemd/system/kikimr.service"),
        (systemd_unit_compute_all, "/etc/systemd/system/kikimr-multi-all.service"),
        (systemd_unit_compute, "/etc/systemd/system/kikimr-multi@.service"),
        (kikimr_run_compute_node_sh, "/etc/systemd/system/kikimr-run.sh"),
    ]

    for content, location in mapping:
        fd, temp_path = tempfile.mkstemp()
        try:
            with os.fdopen(fd, "w") as tmp:
                tmp.write(content)
            os.chmod(temp_path, 0o644)
            nodes_.copy(temp_path, location)
        finally:
            os.remove(temp_path)


def create_file_drives(nodes_, cluster_details):
    tasks = []
    for host in cluster_details.hosts:
        for drive in host.drives:
            host_name = host.hostname
            drive_path = drive.path
            # TODO: allow customize disk size
            cmd = "sudo fallocate -l 32GB {path} && sudo chmod 0666 {path}".format(path=drive_path)
            tasks.extend(nodes_.execute_async_ret(cmd, nodes=[host_name]))
    nodes_._check_async_execution(tasks)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-c",
        "--cluster",
        required=True,
        help="Path to cluster yaml"
    )

    parser.add_argument(
        "-u",
        "--ssh-user",
        default="yc-user",
        help="User for ssh interaction"
    )

    args = parser.parse_args()
    cluster_yaml = args.cluster
    ssh_user = args.ssh_user
    walle_provider = NopHostsInformationProvider()

    cluster_details = cluster_description.ClusterDetails(cluster_yaml, walle_provider)
    cluster_hosts = cluster_details.hosts_names

    nodes_ = nodes.Nodes(cluster_hosts, False, ssh_user=ssh_user)

    create_systemd_units(nodes_)
    create_file_drives(nodes_, cluster_details)


if __name__ == "__main__":
    main()
