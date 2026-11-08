import os
import time
import logging
import subprocess
from collections import deque, defaultdict
from uuid import uuid4
from ydb.tools.ydbd_slice import config_client
from ydb.tools.ydbd_slice import blobstorage_init
from ydb.tools.ydbd_slice import process_profiles

logger = logging.getLogger(__name__)

# Max time to wait for kikimr processes to come back after a rolling-restart stop/start.
ROLLING_RESTART_TIMEOUT_SEC = 1800
# Require the new PID set to stay visible across several polls before treating the host as back.
ROLLING_RESTART_STABLE_POLLS = 3


class CalledProcessError(subprocess.CalledProcessError):
    def __init__(self, base):
        super(CalledProcessError, self).__init__(base.returncode, base.cmd, base.output)

    def __str__(self):
        return "Command '%s' returned non-zero exit status %d and output was '%s'" % (
            self.cmd,
            self.returncode,
            self.output,
        )


def _list_kikimr_pids_command(include_regex=None, exclude_regex=None):
    """Remote awk: pids of /Berkanavt/kikimr/bin/kikimr, optionally filtered by args."""
    clauses = [r'$0 ~ /[k]ikimr\/bin\/kikimr/']
    if include_regex:
        clauses.append('$0 ~ /%s/' % include_regex.replace('/', r'\/'))
    if exclude_regex:
        clauses.append('$0 !~ /%s/' % exclude_regex.replace('/', r'\/'))
    clauses.append('$0 !~ /awk/')
    return "ps -eo pid=,args= | awk '%s {print $1}' | sort -n" % ' && '.join(clauses)


def _parse_pids(stdout):
    return [part for part in (stdout or '').split() if part]


def _pid_count(stdout):
    return len(_parse_pids(stdout))


def _static_pids_command():
    return _list_kikimr_pids_command(include_regex='--node static')


def _dynamic_pids_command():
    # New-style static also has --ic-port; exclude --node static so only slots match.
    return _list_kikimr_pids_command(include_regex='--ic-port', exclude_regex='--node static')


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
        rolling_restart_sec=None,
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
        self.rolling_restart_sec = rolling_restart_sec
        self.walle_provider = walle_provider
        self._host_dynamic_slot_counts = getattr(cluster_details, 'host_dynamic_slot_counts', None) or {}
        self._host_storage_enabled_map = getattr(cluster_details, 'host_storage_enabled', None) or {}
        self._host_storage_profile = getattr(cluster_details, 'host_storage_profile', None) or {}
        self._host_dynamic_profiles = getattr(cluster_details, 'host_dynamic_profiles', None) or {}
        self.__config_client = config_client.ConfigClient(
            self.nodes.nodes_list[0],
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

    def __create_databases(self, serverless=False):
        create_db_template = f"{self.slice_kikimr_path} admin database /{{}}/{{}} create {{}}"
        for domain in self.cluster_details.domains:
            for tenant in domain.tenants:
                opts = []
                for storage in tenant.storage_units:
                    opts.append(':'.join([storage.kind, str(storage.count)]))
                if tenant.shared_database_path:
                    if serverless:
                        opts.append("--serverless")
                        opts.append(tenant.shared_database_path)
                    else:
                        continue
                elif serverless:
                    continue
                elif tenant.shared:
                    opts.append("--shared")
                self.nodes.execute_async(
                    create_db_template.format(domain.domain_name, tenant.name, ' '.join(opts)),
                    nodes=self.nodes.nodes_list[:1]
                )

    def __init_blobstorage_kikimr(self):
        host = self.nodes.nodes_list[0]
        cmd = (
            f"{self.slice_kikimr_path} admin blobstorage config init "
            f"--yaml-file {self.slice_cfg_path}/config.yaml"
        )
        blobstorage_init.run_blobstorage_config_init_with_retry(self.nodes, host, cmd)

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

        if 'kikimr' in self.components:
            self.__create_databases(serverless=True)  # create serverless databases if any

    def _host_dynamic_slot_limit(self, node):
        return self._host_dynamic_slot_counts.get(node)

    def _host_storage_enabled(self, node):
        return self._host_storage_enabled_map.get(node, True)

    def _profile_id_for_slot(self, node, slot):
        profiles = self._host_dynamic_profiles.get(node) or []
        domain_slots = [
            domain_slot
            for domain_slot in self.cluster_details.dynamic_slots
            if domain_slot.domain == slot.domain
        ]
        for index, domain_slot in enumerate(domain_slots):
            if domain_slot.slot == slot.slot:
                if index < len(profiles):
                    return profiles[index]
                return None
        return None

    def _storage_hosts(self):
        return [node for node in self.nodes.nodes_list if self._host_storage_enabled(node)]

    def _get_available_slots(self):
        if 'dynamic_slots' not in self.components:
            return ({}, 0)

        slots_per_domain = {}

        all_available_slots_count = 0
        for domain in self.cluster_details.domains:
            available_slots_per_zone = defaultdict(deque)
            domain_slots = [slot for slot in self.cluster_details.dynamic_slots if slot.domain == domain.domain_name]

            for slot_index, slot in enumerate(domain_slots, 1):
                for node in self.nodes.nodes_list:
                    host_limit = self._host_dynamic_slot_limit(node)
                    if host_limit is not None and slot_index > host_limit:
                        continue
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

        profile_id = self._profile_id_for_slot(node, slot)
        if profile_id:
            src = os.path.join(self.slice_cfg_path, process_profiles.profile_yaml_filename(profile_id))
            dst = slot_dir + "/config.yaml"
            copy_cmd = "sudo cp {src} {dst}".format(src=src, dst=dst)
            self.nodes.execute_async(copy_cmd, check_retcode=False, nodes=[node])

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
        storage_hosts = self._storage_hosts()
        storage_host_set = set(storage_hosts)
        skip_hosts = [node for node in self.nodes.nodes_list if node not in storage_host_set]
        if skip_hosts:
            # Static unit only (`kikimr`). Dynnodes are kikimr-multi@<slot> and stay running.
            # check_retcode=False: stop is idempotent; the unit may already be inactive.
            self._stop_static_on_hosts(skip_hosts)
        if storage_hosts:
            self.nodes.execute_async("sudo service kikimr start", check_retcode=True, nodes=storage_hosts)

    def _iter_dynamic_assignments(self):
        """Return [(slot, tenant, host, node_bind), ...] for the current cluster config."""
        if 'dynamic_slots' not in self.components:
            return []

        def get_numa_nodes(nodes):
            results = dict()
            nodes.execute_async(
                "numactl --hardware | head -n 1 | awk '{print $2}'",
                check_retcode=False,
                results=results,
            )
            return {
                host: int(result['stdout']) if result['retcode'] == 0 else 0
                for host, result in results.items()
            }

        numa_nodes = None  # get_numa_nodes(nodes)
        numa_nodes_counters = {node: 0 for node in self.nodes.nodes_list}
        (slots_per_domain, all_available_slots_count,) = self._get_available_slots()
        assignments = []

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
                                node_bind = None
                                if domain.bind_slots_to_numa_nodes and numa_nodes and numa_nodes[node] > 0:
                                    node_bind = numa_nodes_counters[node]
                                    numa_nodes_counters[node] += 1
                                    numa_nodes_counters[node] %= numa_nodes[node]
                                assignments.append((slot, tenant, node, node_bind))
                                break
                        except IndexError:
                            logger.critical('insufficient slots allocated')
                            return assignments

            logger.warning('{count} unused slots'.format(count=all_available_slots_count - len(slots_taken)))

        return assignments

    def _start_dynamic(self):
        for slot, tenant, host, node_bind in self._iter_dynamic_assignments():
            self._start_slot_for_tenant(slot, tenant, host=host, node_bind=node_bind)

    def slice_start(self):
        if 'kikimr' in self.components:
            self._start_static()

        self._start_dynamic()

    def _stop_slot_command(self, slot):
        return (
            "sudo sh -c \"if [ -x /sbin/stop ]; "
            "    then stop kikimr-multi slot={slot}; "
            "    else systemctl stop kikimr-multi@{slot}; fi\"".format(
                slot=slot.slot,
            )
        )

    def _stop_all_slots(self, hosts=None):
        cmd = "find /Berkanavt/ -maxdepth 1 -type d  -name kikimr_* " \
            " | while read x; do " \
            "      sudo sh -c \"if [ -x /sbin/stop ]; "\
            "          then stop kikimr-multi slot=${x#/Berkanavt/kikimr_}; "\
            "          else systemctl stop kikimr-multi@${x#/Berkanavt/kikimr_}; fi\"; " \
            "   done"
        self.nodes.execute_async(cmd, check_retcode=False, nodes=hosts)

    def _stop_slot_ret(self, slot, nodes=None):
        return self.nodes.execute_async_ret(
            self._stop_slot_command(slot),
            check_retcode=False,
            nodes=nodes,
        )

    def _stop_slot(self, slot):
        tasks = self._stop_slot_ret(slot)
        self.nodes._check_async_execution(tasks, False)

    def _stop_static_on_hosts(self, hosts):
        self.nodes.execute_async("sudo service kikimr stop", check_retcode=False, nodes=hosts)

    def _stop_static(self):
        self._stop_static_on_hosts(self.nodes.nodes_list)

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
        self._apply_storage_process_profiles()

    def _apply_storage_process_profiles(self):
        if not getattr(self.configurator, 'enable_process_profiles', False):
            return
        if self.v2:
            return
        for host, profile_id in self._host_storage_profile.items():
            filename = process_profiles.profile_yaml_filename(profile_id)
            cfg_file = os.path.join(self.slice_cfg_path, 'kikimr.cfg')
            cmd = (
                "sudo sed -i 's|--yaml-config ${kikimr_config}/config.yaml"
                "|--yaml-config ${kikimr_config}/%s|' %s"
                % (filename, cfg_file)
            )
            self.nodes.execute_async(cmd, nodes=[host])

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

        self._restart_after_update()

    def _restart_after_update(self):
        if self.rolling_restart_sec is not None:
            self._deploy_slot_configs()
            self._rolling_restart(self.rolling_restart_sec)
            return

        self.slice_stop()
        self._deploy_slot_configs()
        self.slice_start()

    def _host_probe(self, host, command):
        results = {}
        self.nodes.execute_async(
            command,
            check_retcode=False,
            nodes=[host],
            results=results,
            retry_attempts=0,
        )
        result = results.get(host) or {}
        return (
            (result.get('stdout') or '').strip(),
            result.get('retcode'),
            (result.get('stderr') or '').strip(),
        )

    def _wait_kikimr_back(self, host, list_cmd, old_pids, min_count, phase, index, total):
        old_set = set(old_pids)
        deadline = time.time() + ROLLING_RESTART_TIMEOUT_SEC
        next_report = time.time() + 30
        stable_polls = 0
        stable_pid_set = None
        last_stdout = ''
        last_retcode = None
        last_stderr = ''
        while True:
            if getattr(self.nodes, '_dry_run', False):
                return
            stdout, retcode, stderr = self._host_probe(host, list_cmd)
            last_stdout, last_retcode, last_stderr = stdout, retcode, stderr
            pids = _parse_pids(stdout)
            pid_set = set(pids)
            probe_ok = retcode in (0, None)
            # min_count == 0: wait only until old pids are gone (drained host).
            have_replacements = min_count == 0 or (len(pids) >= min_count and len(pids) > 0)
            restarted = probe_ok and not (old_set & pid_set) and have_replacements
            if restarted:
                if pid_set != stable_pid_set:
                    # Crash-looping services can show a fresh PID each poll; require
                    # the same replacement set across consecutive successful polls.
                    stable_pid_set = pid_set
                    stable_polls = 1
                else:
                    stable_polls += 1
                if stable_polls >= ROLLING_RESTART_STABLE_POLLS:
                    logger.info(
                        "rolling restart %s %d/%d %s: new pids: %s",
                        phase,
                        index,
                        total,
                        host,
                        stdout,
                    )
                    return
            else:
                stable_polls = 0
                stable_pid_set = None
            now = time.time()
            if now >= next_report:
                logger.info(
                    "rolling restart %s %d/%d %s: still waiting (%ss), current: %s "
                    "retcode=%s stderr=%s",
                    phase,
                    index,
                    total,
                    host,
                    int(ROLLING_RESTART_TIMEOUT_SEC - (deadline - now)),
                    stdout,
                    retcode,
                    stderr,
                )
                next_report = now + 30
            if now >= deadline:
                raise RuntimeError(
                    "timeout waiting for kikimr restart on %s (%s %d/%d); "
                    "hosts 1..%d in this phase may already be on the new binary/config; "
                    "current: %s retcode=%s stderr=%s"
                    % (
                        host,
                        phase,
                        index,
                        total,
                        index - 1,
                        last_stdout,
                        last_retcode,
                        last_stderr,
                    )
                )
            time.sleep(2)

    def _rolling_pause(self, interval_sec, message, *args):
        if interval_sec <= 0:
            return
        if getattr(self.nodes, '_dry_run', False):
            logger.info("dry-run: skip " + message, *args)
            return
        logger.info(message, *args)
        time.sleep(interval_sec)

    def _rolling_probe_old_pids(self, host, list_cmd, phase, index, total):
        """List kikimr pids before stop. Fail if the probe itself did not succeed."""
        if getattr(self.nodes, '_dry_run', False):
            return []
        stdout, retcode, stderr = self._host_probe(host, list_cmd)
        if retcode != 0:
            raise RuntimeError(
                "failed to list kikimr pids on %s before %s restart (%d/%d): "
                "retcode=%s stderr=%s stdout=%s"
                % (host, phase, index, total, retcode, stderr, stdout)
            )
        return _parse_pids(stdout)

    def _rolling_restart_static_host(self, host, index, total):
        list_cmd = _static_pids_command()
        old_pids = self._rolling_probe_old_pids(host, list_cmd, "storage", index, total)
        logger.info(
            "rolling restart storage %d/%d %s: stop/start kikimr (old pids: %s)",
            index,
            total,
            host,
            ' '.join(old_pids) or "none",
        )
        self.nodes.execute_async(
            "sudo service kikimr stop",
            check_retcode=False,
            nodes=[host],
            retry_attempts=0,
        )
        # Same default retries as _start_static: covers transient SSH/start failures.
        # Persistent start failure still ends in sys.exit from Nodes after retries.
        self.nodes.execute_async(
            "sudo service kikimr start",
            check_retcode=True,
            nodes=[host],
        )
        self._wait_kikimr_back(
            host,
            list_cmd,
            old_pids=old_pids,
            min_count=1,
            phase="storage",
            index=index,
            total=total,
        )

    def _rolling_restart_dynamic_host(self, host, index, total, host_assignments):
        list_cmd = _dynamic_pids_command()
        old_pids = self._rolling_probe_old_pids(host, list_cmd, "dynamic", index, total)
        logger.info(
            "rolling restart dynamic %d/%d %s: stop all local slots, start %d assigned "
            "(old pids: %s)",
            index,
            total,
            host,
            len(host_assignments),
            ' '.join(old_pids) or "none",
        )
        # One SSH call stops every kikimr_* dir, including slots removed from the
        # current config. Then start only the assignments from this update.
        self._stop_all_slots(hosts=[host])
        for slot, tenant, _, node_bind in host_assignments:
            self._start_slot_for_tenant(slot, tenant, host=host, node_bind=node_bind)

        if not host_assignments:
            if old_pids:
                # Drained host: still confirm previously running slots actually died.
                logger.info(
                    "rolling restart dynamic %d/%d %s: no assigned slots, "
                    "waiting until old pids are gone",
                    index,
                    total,
                    host,
                )
                self._wait_kikimr_back(
                    host,
                    list_cmd,
                    old_pids=old_pids,
                    min_count=0,
                    phase="dynamic",
                    index=index,
                    total=total,
                )
            else:
                logger.info(
                    "rolling restart dynamic %d/%d %s: no assigned slots, skip wait",
                    index,
                    total,
                    host,
                )
            return

        self._wait_kikimr_back(
            host,
            list_cmd,
            old_pids=old_pids,
            min_count=len(host_assignments),
            phase="dynamic",
            index=index,
            total=total,
        )

    def _rolling_restart_hosts(self, hosts, interval_sec, phase, assignments_by_host=None):
        hosts = list(hosts)
        total = len(hosts)
        if total == 0:
            return
        logger.info(
            "rolling restart %s: %d hosts, pause %ss after processes are up and stable",
            phase,
            total,
            interval_sec,
        )
        for index, host in enumerate(hosts, 1):
            if phase == "storage":
                self._rolling_restart_static_host(host, index, total)
            else:
                host_assignments = (assignments_by_host or {}).get(host, [])
                self._rolling_restart_dynamic_host(host, index, total, host_assignments)
            if index < total and interval_sec > 0:
                self._rolling_pause(
                    interval_sec,
                    "waiting %ss before the next %s host",
                    interval_sec,
                    phase,
                )

    def _rolling_restart(self, interval_sec):
        # Same order as a full update restart: storage first across hosts, then dynamic.
        # Intentional two-phase (not per-host combined): storage should be back before dynnodes.
        # "Back" here is PID liveness/stability only; SECONDS must cover cluster rejoin.
        did_storage = False
        if "kikimr" in self.components:
            storage_hosts = self._storage_hosts()
            storage_host_set = set(storage_hosts)
            skip_hosts = [node for node in self.nodes.nodes_list if node not in storage_host_set]
            # Roll remaining storage first so storage:false hosts stay up for quorum,
            # then stop them (mirrors _start_static's skip_hosts stop, but after the roll).
            self._rolling_restart_hosts(
                storage_hosts,
                interval_sec,
                phase="storage",
            )
            if skip_hosts:
                self._stop_static_on_hosts(skip_hosts)
            did_storage = bool(storage_hosts)
        if "dynamic_slots" in self.components:
            if did_storage and interval_sec > 0:
                self._rolling_pause(
                    interval_sec,
                    "waiting %ss between storage and dynamic rolling-restart phases",
                    interval_sec,
                )
            assignments_by_host = defaultdict(list)
            for slot, tenant, host, node_bind in self._iter_dynamic_assignments():
                assignments_by_host[host].append((slot, tenant, host, node_bind))
            self._rolling_restart_hosts(
                self.nodes.nodes_list,
                interval_sec,
                phase="dynamic",
                assignments_by_host=assignments_by_host,
            )

    def slice_update_raw_configs(self, raw_config_path):
        self.slice_stop()
        if 'kikimr' in self.components:
            if 'cfg' in self.components.get('kikimr', []):
                kikimr_cfg = os.path.join(raw_config_path, 'kikimr-static')
                self._upload_cfg(kikimr_cfg)

        self.slice_start()
