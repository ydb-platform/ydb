import os
import time
import logging
import subprocess
from collections import deque, defaultdict


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


class Slice:
    def __init__(self, components, nodes, cluster_details, configurator, do_clear_logs, yav_version, walle_provider):
        self.slice_kikimr_path = '/Berkanavt/kikimr/bin/kikimr'
        self.slice_cfg_path = '/Berkanavt/kikimr/cfg'
        self.slice_secrets_path = '/Berkanavt/kikimr/token'
        self.components = components
        self.nodes = nodes
        self.cluster_details = cluster_details
        self.configurator = configurator
        self.do_clear_logs = do_clear_logs
        self.yav_version = yav_version
        self.walle_provider = walle_provider

    def _ensure_berkanavt_exists(self):
        cmd = r"sudo mkdir -p /Berkanavt"
        self.nodes.execute_async(cmd)

    def _clear_registered_slots(self):
        self.nodes.execute_async(r"sudo find /Berkanavt/ -maxdepth 1 -type d  -name 'kikimr_*' -exec  rm -rf -- {} \;")

    def _clear_slot(self, slot):
        cmd = r"sudo find /Berkanavt/ -maxdepth 1 -type d  -name kikimr_{slot} -exec  rm -rf -- {{}} \;".format(slot=slot.slot)
        self.nodes.execute_async(cmd)

    def _clear_logs(self):
        cmd = "sudo service rsyslog stop; " \
            "find /Berkanavt/ -mindepth 2 -maxdepth 2 -name logs | egrep '^/Berkanavt/kikimr' | sudo xargs -I% find % -mindepth 1 -delete; " \
            "sudo service rsyslog start;"
        self.nodes.execute_async(cmd)

    def _get_all_drives(self):
        result = []
        for host in self.cluster_details.hosts:
            for drive in host.drives:
                result.append((host.hostname, drive.path))
        return result

    def _format_drives(self):
        tasks = []
        for (host_name, drive_path) in self._get_all_drives():
            cmd = "sudo dd if=/dev/zero of={} bs=1M count=1 status=none conv=notrunc".format(drive_path)
            tasks.extend(self.nodes.execute_async_ret(cmd, nodes=[host_name]))
        self.nodes._check_async_execution(tasks)

    def slice_format(self):
        self.slice_stop()
        self._format_drives()
        self.slice_start()

    def slice_clear(self):
        self.slice_stop()

        if 'dynamic_slots' in self.components:
            for slot in self.cluster_details.dynamic_slots.values():
                self._clear_slot(slot)

        if 'kikimr' in self.components:
            self._format_drives()

    def _invoke_scripts(self, dynamic_cfg_path, scripts):
        for script_name in scripts:
            script_path = os.path.join(dynamic_cfg_path, script_name)
            if os.path.isfile(script_path):
                cmd = ["bash", script_path]
                logger.info("run cmd '%s'", cmd)
                try:
                    subprocess.check_output(cmd, stderr=subprocess.STDOUT)
                except subprocess.CalledProcessError as er:
                    raise CalledProcessError(er)

    def _dynamic_configure(self):
        dynamic_cfg_path = self.configurator.create_dynamic_cfg()
        # wait for bs to configure
        time_remaining = 120
        while True:
            try:
                self._invoke_scripts(dynamic_cfg_path, ['init_storage.bash'])
                break
            except CalledProcessError:
                time_to_wait = min(time_remaining, 5)
                if not time_to_wait:
                    raise
                time_remaining -= time_to_wait
                time.sleep(time_to_wait)
        self._invoke_scripts(
            dynamic_cfg_path, (
                "init_cms.bash",
                "init_compute.bash",
                "init_root_storage.bash",
                "init_databases.bash"
            )
        )

    def slice_install(self):
        self._ensure_berkanavt_exists()
        self.slice_stop()

        if 'dynamic_slots' in self.components or 'kikimr' in self.components:
            self._stop_all_slots()
            self._clear_registered_slots()

        if self.do_clear_logs:
            self._clear_logs()

        if 'kikimr' in self.components:
            self._format_drives()

            if 'bin' in self.components.get('kikimr', []):
                self._update_kikimr()

            if 'cfg' in self.components.get('kikimr', []):
                static_cfg_path = self.configurator.create_static_cfg()
                self._update_cfg(static_cfg_path)
                self._deploy_secrets()

            self._start_static()
            self._dynamic_configure()

        self._deploy_slot_configs()
        self._start_dynamic()

    def _get_available_slots(self):
        if 'dynamic_slots' not in self.components:
            return {}

        slots_per_domain = {}

        for domain in self.cluster_details.domains:
            available_slots_per_zone = defaultdict(deque)
            all_available_slots_count = 0

            for slot in self.cluster_details.dynamic_slots.values():
                if slot.domain == domain.domain_name:
                    for node in self.nodes.nodes_list:
                        item = (slot, node)
                        available_slots_per_zone[self.walle_provider.get_datacenter(node).lower()].append(item)
                        available_slots_per_zone['any'].append(item)
                        all_available_slots_count += 1
            slots_per_domain[domain.domain_name] = available_slots_per_zone

        return (slots_per_domain, all_available_slots_count, )

    def _deploy_slot_config_for_tenant(self, slot, tenant, node):
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

        self.nodes.execute_async(cmd, check_retcode=False, nodes=[node])

    def _deploy_slot_configs(self):
        if 'dynamic_slots' not in self.components:
            return

        slots_per_domain = self._get_available_slots()[0]
        for domain in self.cluster_details.domains:
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
                                self._deploy_slot_config_for_tenant(slot, tenant, node)
                                break
                        except IndexError:
                            logger.critical('insufficient slots allocated')
                            return

    def _start_slot(self, slot):
        cmd = "sudo sh -c \"if [ -x /sbin/start ]; "\
            "    then start kikimr-multi slot={slot} tenant=dynamic mbus={mbus} grpc={grpc} mon={mon} ic={ic}; "\
            "    else systemctl start kikimr-multi@{slot}; fi\"".format(
                slot=slot.slot,
                mbus=slot.mbus,
                grpc=slot.grpc,
                mon=slot.mon,
                ic=slot.ic
            )
        self.nodes.execute_async(self, cmd, check_retcode=False)

    def _start_slot_for_tenant(self, slot, tenant, host, node_bind=None):
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
        self.nodes.execute_async(cmd, check_retcode=False, nodes=[host])

    def _start_all_slots(self):
        cmd = "find /Berkanavt/ -maxdepth 1 -type d  -name kikimr_* " \
            " | while read x; do " \
            "      sudo sh -c \"if [ -x /sbin/start ]; "\
            "          then start kikimr-multi slot=${x#/Berkanavt/kikimr_}; "\
            "          else systemctl start kikimr-multi@${x#/Berkanavt/kikimr_}; fi\"; " \
            "   done"
        self.nodes.execute_async(cmd, check_retcode=False)

    def _start_static(self):
        self.nodes.execute_async("sudo service kikimr start", check_retcode=True)

    def _start_dynamic(self):
        if 'dynamic_slots' in self.components:

            def get_numa_nodes(nodes):
                results = dict()
                nodes.execute_async("numactl --hardware | head -n 1 | awk '{print $2}'", check_retcode=False,
                                    results=results)
                return {
                    host: int(result['stdout']) if result['retcode'] == 0 else 0
                    for host, result in results.items()
                }

            numa_nodes = None  # get_numa_nodes(nodes)
            numa_nodes_counters = {node: 0 for node in self.nodes.nodes_list}

            (slots_per_domain, all_available_slots_count,) = self._get_available_slots()

            for domain in self.cluster_details.domains:

                slots_taken = set()
                available_slots_per_zone = slots_per_domain[domain.domain_name]

                if domain.bind_slots_to_numa_nodes and numa_nodes is None:
                    numa_nodes = get_numa_nodes(self.nodes)

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
                                        self._start_slot_for_tenant(
                                            slot,
                                            tenant,
                                            host=node,
                                            node_bind=numa_nodes_counters[node]
                                        )
                                        numa_nodes_counters[node] += 1
                                        numa_nodes_counters[node] %= numa_nodes[node]
                                    else:
                                        self._start_slot_for_tenant(slot, tenant, host=node)
                                    break
                            except IndexError:
                                logger.critical('insufficient slots allocated')
                                return

                logger.warning('{count} unused slots'.format(count=all_available_slots_count - len(slots_taken)))

    def slice_start(self):
        if 'kikimr' in self.components:
            self._start_static()

        self._start_dynamic()

    def _stop_all_slots(self):
        cmd = "find /Berkanavt/ -maxdepth 1 -type d  -name kikimr_* " \
            " | while read x; do " \
            "      sudo sh -c \"if [ -x /sbin/stop ]; "\
            "          then stop kikimr-multi slot=${x#/Berkanavt/kikimr_}; "\
            "          else systemctl stop kikimr-multi@${x#/Berkanavt/kikimr_}; fi\"; " \
            "   done"
        self.nodes.execute_async(cmd, check_retcode=False)

    def _stop_slot_ret(self, slot):
        cmd = "sudo sh -c \"if [ -x /sbin/stop ]; "\
            "    then stop kikimr-multi slot={slot}; "\
            "    else systemctl stop kikimr-multi@{slot}; fi\"".format(
                slot=slot.slot,
            )
        return self.nodes.execute_async_ret(cmd, check_retcode=False)

    def _stop_slot(self, slot):
        tasks = self._stop_slot_ret(slot)
        self.nodes._check_async_execution(tasks, False)

    def _stop_static(self):
        self.nodes.execute_async("sudo service kikimr stop", check_retcode=False)

    def _stop_dynamic(self):
        if 'dynamic_slots' in self.components:
            tasks = []
            for slot in self.cluster_details.dynamic_slots.values():
                tasks_slot = self._stop_slot_ret(slot)
                for task in tasks_slot:
                    tasks.append(task)
            self.nodes._check_async_execution(tasks, False)

    def slice_stop(self):
        self._stop_dynamic()

        if 'kikimr' in self.components:
            self._stop_static()

    def _update_kikimr(self):
        bin_directory = os.path.dirname(self.configurator.kikimr_bin)
        self.nodes.copy(self.configurator.kikimr_bin, self.slice_kikimr_path, compressed_path=self.configurator.kikimr_compressed_bin)
        for lib in ['libiconv.so', 'liblibaio-dynamic.so', 'liblibidn-dynamic.so']:
            lib_path = os.path.join(bin_directory, lib)
            if os.path.exists(lib_path):
                remote_lib_path = os.path.join('/lib', lib)
                self.nodes.copy(lib_path, remote_lib_path)

    def _update_cfg(self, cfg_path):
        self.nodes.copy(cfg_path, self.slice_cfg_path, directory=True)

    def _deploy_secrets(self):
        if not self.yav_version:
            return

        self.nodes.execute_async(
            "sudo bash -c 'set -o pipefail && sudo mkdir -p {secrets} && "
            "yav get version {yav_version} -o auth_file | sudo tee {auth}'".format(
                yav_version=self.yav_version,
                secrets=self.slice_secrets_path,
                auth=os.path.join(self.slice_secrets_path, 'kikimr.token')
            )
        )

        # creating symlinks, to attach auth.txt to node
        self.nodes.execute_async(
            "sudo ln -f {secrets_auth} {cfg_auth}".format(
                secrets_auth=os.path.join(self.slice_secrets_path, 'kikimr.token'),
                cfg_auth=os.path.join(self.slice_cfg_path, 'auth.txt')
            )
        )

        self.nodes.execute_async(
            "sudo bash -c 'set -o pipefail && yav get version {yav_version} -o tvm_secret |  sudo tee {tvm_secret}'".format(
                yav_version=self.yav_version,
                tvm_secret=os.path.join(self.slice_secrets_path, 'tvm_secret')
            )
        )

    def slice_update(self):
        if self.do_clear_logs:
            self._clear_logs()

        if 'kikimr' in self.components:
            if 'bin' in self.components.get('kikimr', []):
                self._update_kikimr()

        self.slice_stop()
        if 'kikimr' in self.components:
            if 'cfg' in self.components.get('kikimr', []):
                static = self.configurator.create_static_cfg()
                self._update_cfg(static)
                self._deploy_secrets()

        self._deploy_slot_configs()
        self.slice_start()

    def slice_update_raw_configs(self, raw_config_path):
        self.slice_stop()
        if 'kikimr' in self.components:
            if 'cfg' in self.components.get('kikimr', []):
                kikimr_cfg = os.path.join(raw_config_path, 'kikimr-static')
                self._update_cfg(kikimr_cfg)

        self.slice_start()
