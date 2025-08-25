import os
import time
import logging
import subprocess
from collections import deque, defaultdict
from uuid import uuid4
from ydb.tools.ydbd_slice import config_client

logger = logging.getLogger(__name__)


class CalledProcessError(subprocess.CalledProcessError):
    def __init__(self, base):
        super(CalledProcessError, self).__init__(base.returncode, base.cmd, base.output)

    def __str__(self):
        return "Command '%s' returned non-zero exit status %d and output was '%s'" % (
            self.cmd,
            self.returncode,
            self.output,
        )


class Slice:
    def __init__(
        self,
        components,
        nodes,
        cluster_details,
        bin=None,
        compressed_bin=None,
        do_clear_logs=False,
        yav_version=None,
        walle_provider=None,
        configurator=None,
    ):
        self.slice_kikimr_path = '/Berkanavt/kikimr/bin/kikimr'
        self.__slice_cfg_path = '/Berkanavt/kikimr/cfg'
        self.__slice_tenants_path = '/Berkanavt/tenants'
        self.slice_secrets_path = '/Berkanavt/kikimr/token'
        self.components = components
        self.nodes = nodes
        self.cluster_details = cluster_details
        self.bin = bin
        self.compressed_bin = compressed_bin
        self.configurator = configurator
        self.do_clear_logs = do_clear_logs
        self.yav_version = yav_version
        self.walle_provider = walle_provider
        self.__config_client = config_client.ConfigClient(
            self.cluster_details.hosts[0].hostname,
            self.cluster_details.grpc_config.get('port'),
            retry_count=10,
        )

    @property
    def slice_cfg_path(self) -> str:
        return self.__slice_cfg_path

    @property
    def slice_tenants_path(self) -> str:
        return self.__slice_tenants_path

    @property
    def v2(self):
        return self.configurator.v2 if hasattr(self.configurator, 'v2') else False

    def _ensure_berkanavt_exists(self):
        self.nodes.execute_async(r"sudo mkdir -p /Berkanavt")

    def _clear_config(self):
        self.nodes.execute_async(r"sudo rm -f {cfg_dir}/config.yaml".format(cfg_dir=self.slice_cfg_path))

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
            cmd = "sudo {} admin bs disk obliterate {}".format(self.slice_kikimr_path, drive_path)
            tasks.extend(self.nodes.execute_async_ret(cmd, nodes=[host_name]))
        self.nodes._check_async_execution(tasks)

    def _set_locations(self):
        # Set location for each group of hosts sharing the same datacenter
        for datacenter, hosts in self.configurator.group_hosts_by_datacenter.items():
            cmd = "sudo sh -c 'echo {} > {}/location'".format(datacenter, self.slice_tenants_path)
            self.nodes.execute_async(cmd, nodes=hosts)

        # Set bridge-pile for each group of hosts sharing the same pile
        for pile, hosts in self.configurator.group_hosts_by_bridge_pile.items():
            cmd = "sudo sh -c 'echo {} > {}/bridge-pile'".format(pile, self.slice_tenants_path)
            self.nodes.execute_async(cmd, nodes=hosts)

    def _clear_locations(self):
        self.nodes.execute_async("sudo rm -f {}/{{location,bridge-pile}}".format(self.slice_tenants_path))

    def slice_format(self):
        self.slice_stop()
        self._format_drives()
        self.slice_start()

    def slice_clear(self):
        self.slice_stop()
        self._clear_config()
        self._clear_locations()

        if 'dynamic_slots' in self.components:
            for slot in self.cluster_details.dynamic_slots:
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

    # Not realy needed function
    def _dynamic_configure(self):
        if self.configurator is None:
            raise ValueError("Configurator is required for dynamic configuration")

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

    def __create_databases(self):
        create_db_template = f"{self.slice_kikimr_path} admin database /{{}}/{{}} create {{}}:{{}}"
        for domain in self.cluster_details.domains:
            for tenant in domain.tenants:
                for storage in tenant.storage_units:
                    self.nodes.execute_async(
                        create_db_template.format(domain.domain_name, tenant.name, storage.kind, storage.count),
                        nodes=self.nodes.nodes_list[:1]
                    )

    def __init_blobstorage_kikimr(self):
        self.nodes.execute_async(
            f"{self.slice_kikimr_path} admin blobstorage config init --yaml-file {self.slice_cfg_path}/config.yaml",
            nodes=self.nodes.nodes_list[:1],
            check_retcode=True,
            retry_attempts=5,
        )

    def __cluster_bootstrap(self):
        try:
            self.__config_client.bootstrap_cluster(str(uuid4()))
        except config_client.ConfigClientError as e:
            raise RuntimeError(f"Failed to bootstrap cluster: {e}")

    def _dynamic_provision(self):
        self.__init_blobstorage_kikimr()
        self.__create_databases()

        return

    def slice_install(self):
        self._ensure_berkanavt_exists()
        self.slice_stop()

        if self.configurator is None:
            raise ValueError("Configurator is required for static configuration")

        if 'dynamic_slots' in self.components or 'kikimr' in self.components:
            self._stop_all_slots()
            self._clear_registered_slots()

        if self.do_clear_logs:
            self._clear_logs()

        if 'kikimr' in self.components:
            if 'bin' in self.components.get('kikimr', []):
                self._update_kikimr()

            self._format_drives()
            self._set_locations()
            if 'cfg' in self.components.get('kikimr', []):
                static_cfg_path = self.configurator.create_static_cfg()
                self._upload_cfg(static_cfg_path)
                self._deploy_secrets()

            self._start_static()
            if self.v2:
                self.__cluster_bootstrap()
            else:
                self.__init_blobstorage_kikimr()
                # Old bootstrap via proto files
                # self._dynamic_configure()

            self.__create_databases()

        self._deploy_slot_configs()
        self._start_dynamic()

    def _get_available_slots(self):
        if 'dynamic_slots' not in self.components:
            return {}

        slots_per_domain = {}

        all_available_slots_count = 0
        for domain in self.cluster_details.domains:
            available_slots_per_zone = defaultdict(deque)

            for slot in self.cluster_details.dynamic_slots:
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
kafka_port={kafka_port}
mbus={mbus}
ic={ic}
mon={mon}""".format(
            domain=slot.domain,
            tenant=tenant.name,
            mbus=slot.mbus,
            grpc=slot.grpc,
            mon=slot.mon,
            ic=slot.ic,
            kafka_port=slot.kafka_port,
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
            "    then start kikimr-multi slot={slot} tenant=dynamic mbus={mbus} grpc={grpc} mon={mon} ic={ic} kafka_port={kafka_port}; "\
            "    else systemctl start kikimr-multi@{slot}; fi\"".format(
                slot=slot.slot,
                mbus=slot.mbus,
                grpc=slot.grpc,
                mon=slot.mon,
                ic=slot.ic,
                kafka_port=slot.kafka_port,
            )
        self.nodes.execute_async(self, cmd, check_retcode=False)

    def _start_slot_for_tenant(self, slot, tenant, host, node_bind=None):
        cmd = "sudo sh -c \"if [ -x /sbin/start ]; "\
            "    then start kikimr-multi slot={slot} tenant=/{domain}/{name} mbus={mbus} grpc={grpc} mon={mon} ic={ic} kafka_port={kafka_port}; "\
            "    else systemctl start kikimr-multi@{slot}; fi\"".format(
                slot=slot.slot,
                domain=slot.domain,
                name=tenant.name,
                mbus=slot.mbus,
                grpc=slot.grpc,
                mon=slot.mon,
                ic=slot.ic,
                kafka_port=slot.kafka_port,
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
                                    if domain.bind_slots_to_numa_nodes and numa_nodes and numa_nodes[node] > 0:
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
            for slot in self.cluster_details.dynamic_slots:
                tasks_slot = self._stop_slot_ret(slot)
                for task in tasks_slot:
                    tasks.append(task)
            self.nodes._check_async_execution(tasks, False)

    def slice_stop(self):
        self._stop_dynamic()

        if 'kikimr' in self.components:
            self._stop_static()

    def _update_kikimr(self):
        bin_directory = os.path.dirname(self.bin)
        self.nodes.copy(self.bin, self.slice_kikimr_path, compressed_path=self.compressed_bin)
        for lib in ['libiconv.so', 'liblibaio-dynamic.so', 'liblibidn-dynamic.so']:
            lib_path = os.path.join(bin_directory, lib)
            if os.path.exists(lib_path):
                remote_lib_path = os.path.join('/lib', lib)
                self.nodes.copy(lib_path, remote_lib_path)

    def _upload_cfg(self, cfg_path):
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
        if self.configurator is None:
            raise ValueError("Configurator is required for static configuration")

        if self.do_clear_logs:
            self._clear_logs()

        if 'kikimr' in self.components:
            if 'bin' in self.components.get('kikimr', []):
                self._update_kikimr()

        if 'kikimr' in self.components and 'cfg' in self.components.get('kikimr', []):
            if self.v2:
                try:
                    self.__config_client.replace_config(self.configurator.static)
                except config_client.ConfigClientError as e:
                    raise RuntimeError(f"Failed to config replace cluster: {e}")
            else:
                static = self.configurator.create_static_cfg()
                self._upload_cfg(static)

            self._deploy_secrets()

        self.slice_stop()
        self._deploy_slot_configs()
        self.slice_start()

    def slice_update_raw_configs(self, raw_config_path):
        self.slice_stop()
        if 'kikimr' in self.components:
            if 'cfg' in self.components.get('kikimr', []):
                kikimr_cfg = os.path.join(raw_config_path, 'kikimr-static')
                self._upload_cfg(kikimr_cfg)

        self.slice_start()
