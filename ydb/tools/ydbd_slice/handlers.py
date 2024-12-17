import os
import time
import logging
import subprocess
from collections import deque, defaultdict

from ydb.tools.cfg.walle import NopHostsInformationProvider


logger = logging.getLogger(__name__)


class CalledProcessError(subprocess.CalledProcessError):
    def __init__(self, base):
        super(CalledProcessError, self).__init__(base.returncode, base.cmd, base.output)

    def __str__(self):
        return "Command '%s' returned non-zero exit status %d and output was '%s'" % (
            self.cmd,
            self.returncode,
            self.output
        )


def format_drivers(nodes):
    cmd = "sudo find /dev/disk/by-partlabel/ -maxdepth 1 -name 'kikimr_*' " \
          "-exec dd if=/dev/zero of={} bs=1M count=1 status=none \;"  # noqa: W605
    nodes.execute_async(cmd)


def ctr_image_import(nodes, tar_path):
    cmd = f"sudo ctr image import {tar_path} && rm -rf {tar_path}"
    nodes.execute_async(cmd)


def clear_registered_slots(nodes):
    nodes.execute_async("sudo find /Berkanavt/ -maxdepth 1 -type d  -name 'kikimr_*' -exec  rm -rf -- {} \;")  # noqa: W605


def clear_slot(nodes, slot):
    cmd = "sudo find /Berkanavt/ -maxdepth 1 -type d  -name kikimr_{slot} -exec  rm -rf -- {{}} \;".format(slot=slot.slot)  # noqa: W605
    nodes.execute_async(cmd)


def clear_logs(nodes):
    cmd = "sudo service rsyslog stop; " \
        "find /Berkanavt/ -mindepth 2 -maxdepth 2 -name logs | egrep '^/Berkanavt/kikimr' | sudo xargs -I% find % -mindepth 1 -delete; " \
        "sudo service rsyslog start;"
    nodes.execute_async(cmd)


def slice_format(components, nodes, cluster_details):
    slice_stop(components, nodes, cluster_details)
    format_drivers(nodes)
    slice_start(components, nodes, cluster_details)


def slice_clear(components, nodes, cluster_details):
    slice_stop(components, nodes, cluster_details)

    if 'dynamic_slots' in components:
        for slot in cluster_details.dynamic_slots.values():
            clear_slot(nodes, slot)

    if 'kikimr' in components:
        format_drivers(nodes)


def invoke_scripts(dynamic_cfg_path, scripts):
    for script_name in scripts:
        script_path = os.path.join(dynamic_cfg_path, script_name)
        if os.path.isfile(script_path):
            cmd = ["bash", script_path]
            logger.info("run cmd '%s'", cmd)
            try:
                subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as er:
                raise CalledProcessError(er)


def dynamic_configure(configurations):
    dynamic_cfg_path = configurations.create_dynamic_cfg()
    # wait for bs to configure
    time_remaining = 60
    while True:
        try:
            invoke_scripts(dynamic_cfg_path, ['init_storage.bash'])
            break
        except CalledProcessError:
            time_to_wait = min(time_remaining, 5)
            if not time_to_wait:
                raise
            time_remaining -= time_to_wait
            time.sleep(time_to_wait)
    invoke_scripts(
        dynamic_cfg_path, (
            "init_cms.bash",
            "init_compute.bash",
            "init_root_storage.bash",
            "init_databases.bash"
        )
    )


def slice_install(components, nodes, cluster_details, configurator, do_clear_logs, args):
    slice_stop(components, nodes, cluster_details)

    if 'dynamic_slots' in components or 'kikimr' in components:
        stop_all_slots(nodes)
        clear_registered_slots(nodes)

    if do_clear_logs:
        clear_logs(nodes)

    if 'kikimr' in components:
        format_drivers(nodes)

        if 'bin' in components.get('kikimr', []):
            update_kikimr(nodes, configurator.kikimr_bin, configurator.kikimr_compressed_bin)

        if 'cfg' in components.get('kikimr', []):
            static_cfg_path = configurator.create_static_cfg()
            update_cfg(nodes, static_cfg_path)
            deploy_secrets(nodes, args.yav_version)

        start_static(nodes)
        dynamic_configure(configurator)

    deploy_slot_configs(components, nodes, cluster_details)
    start_dynamic(components, nodes, cluster_details)


def get_available_slots(components, nodes, cluster_details):
    if 'dynamic_slots' not in components:
        return {}

    walle = NopHostsInformationProvider()
    slots_per_domain = {}

    for domain in cluster_details.domains:
        available_slots_per_zone = defaultdict(deque)
        all_available_slots_count = 0

        for slot in cluster_details.dynamic_slots.values():
            if slot.domain == domain.domain_name:
                for node in nodes.nodes_list:
                    item = (slot, node)
                    available_slots_per_zone[walle.get_datacenter(node).lower()].append(item)
                    available_slots_per_zone['any'].append(item)
                    all_available_slots_count += 1
        slots_per_domain[domain.domain_name] = available_slots_per_zone

    return (slots_per_domain, all_available_slots_count, )


def deploy_slot_config_for_tenant(nodes, slot, tenant, node):
    slot_dir = "/Berkanavt/kikimr_{slot}".format(slot=slot.slot)
    logs_dir = slot_dir + "/logs"
    slot_cfg = slot_dir + "/slot_cfg"
    env_txt = slot_dir + "/env.txt"
    cfg = """\
tenant=/{domain}/{tenant}
grpc={grpc}
mbus={mbus}
ic={ic}
mon={mon}""".format(
        domain=slot.domain,
        tenant=tenant.name,
        mbus=slot.mbus,
        grpc=slot.grpc,
        mon=slot.mon,
        ic=slot.ic,
    )

    escaped_cmd = cfg.encode('unicode_escape').decode()

    cmd = "sudo sh -c 'mkdir -p {logs_dir}; sudo chown syslog {logs_dir}; touch {env_txt}; /bin/echo -e \"{cfg}\" > {slot_cfg};'".format(
        logs_dir=logs_dir,
        env_txt=env_txt,
        cfg=escaped_cmd,
        slot_cfg=slot_cfg,
    )

    nodes.execute_async(cmd, check_retcode=False, nodes=[node])


def deploy_slot_configs(components, nodes, cluster_details):
    if 'dynamic_slots' not in components:
        return

    slots_per_domain = get_available_slots(components, nodes, cluster_details)[0]
    for domain in cluster_details.domains:
        slots_taken = set()
        available_slots_per_zone = slots_per_domain[domain.domain_name]
        for tenant in domain.tenants:
            for compute_unit in tenant.compute_units:
                zone = compute_unit.zone.lower()
                for _ in range(compute_unit.count):
                    try:
                        while True:
                            slot, node = available_slots_per_zone[zone].popleft()
                            if (slot, node) in slots_taken:
                                continue
                            slots_taken.add((slot, node))
                            deploy_slot_config_for_tenant(nodes, slot, tenant, node)
                            break
                    except IndexError:
                        logger.critical('insufficient slots allocated')
                        return


def start_slot(nodes, slot):
    cmd = "sudo sh -c \"if [ -x /sbin/start ]; "\
          "    then start kikimr-multi slot={slot} tenant=dynamic mbus={mbus} grpc={grpc} mon={mon} ic={ic}; "\
          "    else systemctl start kikimr-multi@{slot}; fi\"".format(
              slot=slot.slot,
              mbus=slot.mbus,
              grpc=slot.grpc,
              mon=slot.mon,
              ic=slot.ic
          )
    nodes.execute_async(cmd, check_retcode=False)


def start_slot_for_tenant(nodes, slot, tenant, host, node_bind=None):
    cmd = "sudo sh -c \"if [ -x /sbin/start ]; "\
          "    then start kikimr-multi slot={slot} tenant=/{domain}/{name} mbus={mbus} grpc={grpc} mon={mon} ic={ic}; "\
          "    else systemctl start kikimr-multi@{slot}; fi\"".format(
              slot=slot.slot,
              domain=slot.domain,
              name=tenant.name,
              mbus=slot.mbus,
              grpc=slot.grpc,
              mon=slot.mon,
              ic=slot.ic
          )
    if node_bind is not None:
        cmd += " bindnumanode={bind}".format(bind=node_bind)
    nodes.execute_async(cmd, check_retcode=False, nodes=[host])


def start_all_slots(nodes):
    cmd = "find /Berkanavt/ -maxdepth 1 -type d  -name kikimr_* " \
          " | while read x; do " \
          "      sudo sh -c \"if [ -x /sbin/start ]; "\
          "          then start kikimr-multi slot=${x#/Berkanavt/kikimr_}; "\
          "          else systemctl start kikimr-multi@${x#/Berkanavt/kikimr_}; fi\"; " \
          "   done"
    nodes.execute_async(cmd, check_retcode=False)


def start_static(nodes):
    nodes.execute_async("sudo service kikimr start", check_retcode=False)


def start_dynamic(components, nodes, cluster_details):
    if 'dynamic_slots' in components:

        def get_numa_nodes(nodes):
            results = dict()
            nodes.execute_async("numactl --hardware | head -n 1 | awk '{print $2}'", check_retcode=False,
                                results=results)
            return {
                host: int(result['stdout']) if result['retcode'] == 0 else 0
                for host, result in results.items()
            }

        numa_nodes = None  # get_numa_nodes(nodes)
        numa_nodes_counters = {node: 0 for node in nodes.nodes_list}

        (slots_per_domain, all_available_slots_count,) = get_available_slots(components, nodes, cluster_details)

        for domain in cluster_details.domains:

            slots_taken = set()
            available_slots_per_zone = slots_per_domain[domain.domain_name]

            if domain.bind_slots_to_numa_nodes and numa_nodes is None:
                numa_nodes = get_numa_nodes(nodes)

            for tenant in domain.tenants:
                for compute_unit in tenant.compute_units:
                    zone = compute_unit.zone.lower()
                    for _ in range(compute_unit.count):
                        try:
                            while True:
                                slot, node = available_slots_per_zone[zone].popleft()
                                if (slot, node) in slots_taken:
                                    continue
                                slots_taken.add((slot, node))
                                if domain.bind_slots_to_numa_nodes and numa_nodes[node] > 0:
                                    start_slot_for_tenant(nodes, slot, tenant, host=node,
                                                          node_bind=numa_nodes_counters[node])
                                    numa_nodes_counters[node] += 1
                                    numa_nodes_counters[node] %= numa_nodes[node]
                                else:
                                    start_slot_for_tenant(nodes, slot, tenant, host=node)
                                break
                        except IndexError:
                            logger.critical('insufficient slots allocated')
                            return

            logger.warning('{count} unused slots'.format(count=all_available_slots_count - len(slots_taken)))


def slice_start(components, nodes, cluster_details):
    if 'kikimr' in components:
        start_static(nodes)

    start_dynamic(components, nodes, cluster_details)


def stop_all_slots(nodes):
    cmd = "find /Berkanavt/ -maxdepth 1 -type d  -name kikimr_* " \
          " | while read x; do " \
          "      sudo sh -c \"if [ -x /sbin/stop ]; "\
          "          then stop kikimr-multi slot=${x#/Berkanavt/kikimr_}; "\
          "          else systemctl stop kikimr-multi@${x#/Berkanavt/kikimr_}; fi\"; " \
          "   done"
    nodes.execute_async(cmd, check_retcode=False)


def stop_slot_ret(nodes, slot):
    cmd = "sudo sh -c \"if [ -x /sbin/stop ]; "\
          "    then stop kikimr-multi slot={slot}; "\
          "    else systemctl stop kikimr-multi@{slot}; fi\"".format(
              slot=slot.slot,
          )
    return nodes.execute_async_ret(cmd, check_retcode=False)


def stop_slot(nodes, slot):
    tasks = stop_slot_ret(nodes, slot)
    nodes._check_async_execution(tasks, False)


def stop_static(nodes):
    nodes.execute_async("sudo service kikimr stop", check_retcode=False)


def stop_dynamic(components, nodes, cluster_details):
    if 'dynamic_slots' in components:
        tasks = []
        for slot in cluster_details.dynamic_slots.values():
            tasks_slot = stop_slot_ret(nodes, slot)
            for task in tasks_slot:
                tasks.append(task)
        nodes._check_async_execution(tasks, False)


def slice_stop(components, nodes, cluster_details):
    stop_dynamic(components, nodes, cluster_details)

    if 'kikimr' in components:
        stop_static(nodes)


slice_kikimr_path = '/Berkanavt/kikimr/bin/kikimr'
slice_cfg_path = '/Berkanavt/kikimr/cfg'
slice_secrets_path = '/Berkanavt/kikimr/token'


def update_kikimr(nodes, bin_path, compressed_path):
    bin_directory = os.path.dirname(bin_path)
    nodes.copy(bin_path, slice_kikimr_path, compressed_path=compressed_path)
    for lib in ['libiconv.so', 'liblibaio-dynamic.so', 'liblibidn-dynamic.so']:
        lib_path = os.path.join(bin_directory, lib)
        if os.path.exists(lib_path):
            remote_lib_path = os.path.join('/lib', lib)
            nodes.copy(lib_path, remote_lib_path)


def update_cfg(nodes, cfg_path):
    nodes.copy(cfg_path, slice_cfg_path, directory=True)


def deploy_secrets(nodes, yav_version):
    if not yav_version:
        return

    nodes.execute_async(
        "sudo bash -c 'set -o pipefail && sudo mkdir -p {secrets} && "
        "yav get version {yav_version} -o auth_file | sudo tee {auth}'".format(
            yav_version=yav_version,
            secrets=slice_secrets_path,
            auth=os.path.join(slice_secrets_path, 'kikimr.token')
        )
    )

    # creating symlinks, to attach auth.txt to node
    nodes.execute_async(
        "sudo ln -f {secrets_auth} {cfg_auth}".format(
            secrets_auth=os.path.join(slice_secrets_path, 'kikimr.token'),
            cfg_auth=os.path.join(slice_cfg_path, 'auth.txt')
        )
    )

    nodes.execute_async(
        "sudo bash -c 'set -o pipefail && yav get version {yav_version} -o tvm_secret |  sudo tee {tvm_secret}'".format(
            yav_version=yav_version,
            tvm_secret=os.path.join(slice_secrets_path, 'tvm_secret')
        )
    )


def slice_update(components, nodes, cluster_details, configurator, do_clear_logs, args):
    if do_clear_logs:
        clear_logs(nodes)

    if 'kikimr' in components:
        if 'bin' in components.get('kikimr', []):
            update_kikimr(nodes, configurator.kikimr_bin, configurator.kikimr_compressed_bin)

    slice_stop(components, nodes, cluster_details)
    if 'kikimr' in components:
        if 'cfg' in components.get('kikimr', []):
            static = configurator.create_static_cfg()
            update_cfg(nodes, static)
            deploy_secrets(nodes, args.yav_version)

    deploy_slot_configs(components, nodes, cluster_details)
    slice_start(components, nodes, cluster_details)


def slice_update_raw_configs(components, nodes, cluster_details, config_path):
    slice_stop(components, nodes, cluster_details)
    if 'kikimr' in components:
        if 'cfg' in components.get('kikimr', []):
            kikimr_cfg = os.path.join(config_path, 'kikimr-static')
            update_cfg(nodes, kikimr_cfg)

    slice_start(components, nodes, cluster_details)
