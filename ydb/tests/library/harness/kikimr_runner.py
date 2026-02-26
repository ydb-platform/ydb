# -*- coding: utf-8 -*-
import logging
import os
import shutil
import tempfile
import time
import itertools
import threading
from importlib_resources import read_binary
from google.protobuf import text_format
import yaml
import subprocess
import requests

from six.moves.queue import Queue

import yatest

from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.common.types import FailDomainType
from . import daemon
from . import kikimr_config
from . import kikimr_node_interface
from . import kikimr_cluster_interface
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
import ydb.core.protos.blobstorage_config_pb2 as bs
from ydb.tests.library.predicates.blobstorage import blobstorage_controller_has_started_on_some_node
from ydb.tests.library.clients.kikimr_config_client import config_client_factory
from ydb.tests.library.clients.kikimr_monitoring import KikimrMonitor


logger = logging.getLogger(__name__)


def get_unique_path_for_current_test(output_path, sub_folder):
    # TODO: remove current function, don't rely on test environment, use explicit paths
    # we can't remove it now, because it is used in Arcadia
    import yatest.common
    import os
    try:
        test_name = yatest.common.context.test_name or ""
    except Exception:
        test_name = ""

    test_name = test_name.replace(':', '_')
    return os.path.join(output_path, test_name, sub_folder)


def ensure_path_exists(path):
    # NOTE: can't switch to os.makedirs(path, exist_ok=True) as some tests
    # are still running under python2 (exist_ok was added in py3.2)
    if not os.path.isdir(path):
        os.makedirs(path)
    return path


class KiKiMRNode(daemon.Daemon, kikimr_node_interface.NodeInterface):
    def __init__(self, node_id, config_path, port_allocator, cluster_name, configurator,
                 udfs_dir=None, role='node', node_broker_port=None, tenant_affiliation=None, encryption_key=None,
                 binary_path=None, data_center=None, use_config_store=False, seed_nodes_file=None):

        super(kikimr_node_interface.NodeInterface, self).__init__()
        self.node_id = node_id
        self.data_center = data_center
        self.__config_path = config_path
        self.__cluster_name = cluster_name
        self.__configurator = configurator
        self.__common_udfs_dir = udfs_dir
        self.__binary_path = binary_path
        self.__use_config_store = use_config_store or self.__configurator.use_config_store

        self.__encryption_key = encryption_key
        self._tenant_affiliation = tenant_affiliation
        self.grpc_port = port_allocator.grpc_port
        self.mon_port = port_allocator.mon_port
        self.mon_uses_https = self.__configurator.monitoring_tls_cert_path is not None
        self.ic_port = port_allocator.ic_port
        self.grpc_ssl_port = port_allocator.grpc_ssl_port
        self.pgwire_port = port_allocator.pgwire_port
        self.sqs_port = None
        if not configurator.simple_config and configurator.sqs_service_enabled:
            self.sqs_port = port_allocator.sqs_port

        self.__role = role
        self.__node_broker_port = node_broker_port
        self.__seed_nodes_file = seed_nodes_file

        self.__working_dir = ensure_path_exists(
            os.path.join(
                self.__configurator.working_dir,
                self.__cluster_name,
                "{}_{}".format(
                    self.__role,
                    self.node_id
                )
            )
        )

        if configurator.use_log_files:
            # use NamedTemporaryFile only as a unique name generator
            log_file = tempfile.NamedTemporaryFile(dir=self.__working_dir, prefix="logfile_", suffix=".log", delete=False)
            self.__log_file_name = log_file.name
            log_file.close()
            kwargs = {
                "stdout_file": os.path.join(self.__working_dir, "stdout"),
                "stderr_file": os.path.join(self.__working_dir, "stderr"),
                "aux_file": self.__log_file_name,
                }
        else:
            self.__log_file_name = None
            kwargs = {
                "stdout_file": "/dev/stdout",
                "stderr_file": "/dev/stderr"
                }

        daemon.Daemon.__init__(self, self.command, cwd=self.__working_dir, timeout=180, stderr_on_error_lines=240, **kwargs)

    def is_port_listening(self, port):
        """Check if the port is listening after node startup"""
        try:
            cmd = ["netstat", "-tuln"]
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output = result.stdout.decode()
            port_lines = [line for line in output.split('\n') if str(port) in line]
            if port_lines:
                for line in port_lines:
                    logger.info("Port %s status: %s" % (str(port), line.strip()))
                is_listening = True
            else:
                logger.info("Port %s is not found in netstat output" % str(port))
                is_listening = False
            return is_listening
        except Exception as e:
            logger.error("Error checking port %s: %s" % (str(port), str(e)))
            return False

    def check_ports(self):
        """Check if all allocated ports are listening"""
        ports_status = {
            "grpc_port": self.is_port_listening(self.grpc_port),
            "mon_port": self.is_port_listening(self.mon_port),
            "ic_port": self.is_port_listening(self.ic_port)
        }

        if hasattr(self, 'grpc_ssl_port') and self.grpc_ssl_port:
            ports_status["grpc_ssl_port"] = self.is_port_listening(self.grpc_ssl_port)

        if hasattr(self, 'pgwire_port') and self.pgwire_port:
            ports_status["pgwire_port"] = self.is_port_listening(self.pgwire_port)

        if hasattr(self, 'sqs_port') and self.sqs_port:
            ports_status["sqs_port"] = self.is_port_listening(self.sqs_port)

        return ports_status

    @property
    def cwd(self):
        return self.__working_dir

    @property
    def binary_path(self):
        return self.__binary_path

    @binary_path.setter
    def binary_path(self, value):
        self.__binary_path = value

    @property
    def command(self):
        return self.__make_run_command()

    def format_pdisk(self, pdisk_path, disk_size, **kwargs):
        logger.debug("Formatting pdisk %s on node %s, disk_size %s" % (pdisk_path, self, disk_size))
        if pdisk_path.startswith('SectorMap'):
            return

        with open(pdisk_path, "wb") as out:
            out.seek(disk_size - 1)
            out.write(b'\0')

    def __make_run_command(self):
        command = [self.binary_path, "server"]

        if self.__common_udfs_dir is not None:
            command.append("--udfs-dir={}".format(self.__common_udfs_dir))

        if self.__configurator.suppress_version_check:
            command.append("--suppress-version-check")

        if self.__use_config_store:
            command.append("--config-dir=%s" % self.__config_path)
        else:
            command.append("--yaml-config=%s" % os.path.join(self.__config_path, "config.yaml"))

        if self.__seed_nodes_file:
            command.append("--seed-nodes=%s" % self.__seed_nodes_file)

        if self.__node_broker_port is not None:
            command.append("--node-broker=%s%s:%d" % (
                "grpcs://" if self.__configurator.grpc_ssl_enable else "",
                self.host,
                self.__node_broker_port))
        else:
            command.append("--node=%d" % self.node_id)

        if self.__configurator.grpc_ssl_enable:
            command.append(
                "--ca=%s" % self.__configurator.grpc_tls_ca_path
            )

        if self.__configurator.protected_mode:
            command.append(
                "--cert=%s" % self.__configurator.grpc_tls_cert_path
            )
            command.append(
                "--key=%s" % self.__configurator.grpc_tls_key_path
            )

        if self.__role == 'slot' or (self.__configurator.node_kind == "yq" and self._tenant_affiliation is not None):
            command.append(
                "--tenant=%s" % self._tenant_affiliation
            )

        if self.__configurator.grpc_ssl_enable:
            command.append(
                "--grpcs-port={}".format(
                    self.grpc_ssl_port
                )
            )

        if self.__configurator.node_kind is not None:
            command.append(
                "--node-kind=%s" % self.__configurator.node_kind
            )

        if self.__log_file_name is not None:
            command.append(
                "--log-file-name=%s" % self.__log_file_name,
            )

        command.extend(
            [
                "--grpc-port=%s" % self.grpc_port,
                "--mon-port=%d" % self.mon_port,
                "--ic-port=%d" % self.ic_port,
            ]
        )

        if self.__configurator.monitoring_tls_cert_path is not None:
            command.append(
                "--mon-cert=%s" % self.__configurator.monitoring_tls_cert_path
            )

        if self.__configurator.monitoring_tls_key_path is not None:
            command.append(
                "--mon-key=%s" % self.__configurator.monitoring_tls_key_path
            )

        if self.__configurator.monitoring_tls_ca_path is not None:
            command.append(
                "--mon-ca=%s" % self.__configurator.monitoring_tls_ca_path
            )

        if os.environ.get("YDB_ALLOCATE_PGWIRE_PORT", "") == "true":
            command.append("--pgwire-port=%d" % self.pgwire_port)

        if self.__encryption_key is not None:
            command.extend(["--key-file", self.__encryption_key])

        if self.sqs_port is not None:
            command.extend(["--sqs-port=%d" % self.sqs_port])

        if self.data_center is not None:
            command.append(
                "--data-center=%s" % self.data_center
            )

        if self.__configurator.module is not None:
            command.append(
                "--module=%s" % self.__configurator.module
            )

        if self.__configurator.breakpad_minidumps_path:
            command.extend(["--breakpad-minidumps-path", self.__configurator.breakpad_minidumps_path])
        if self.__configurator.breakpad_minidumps_script:
            command.extend(["--breakpad-minidumps-script", self.__configurator.breakpad_minidumps_script])

        if getattr(self.__configurator, "tiny_mode", False):
            command.append("--tiny-mode")

        logger.info('CFG_DIR_PATH="%s"', self.__config_path)
        logger.info("Final command: %s", ' '.join(command).replace(self.__config_path, '$CFG_DIR_PATH'))
        return command

    def stop(self):
        try:
            super(KiKiMRNode, self).stop()
        finally:
            logger.info("Stopped node %s", self)

    def kill(self):
        try:
            super(KiKiMRNode, self).kill()
            self.start()
        finally:
            logger.info("Killed node %s", self)

    def send_signal(self, signal):
        self.daemon.process.send_signal(signal)

    @property
    def host(self):
        return 'localhost'

    @property
    def port(self):
        return self.grpc_port

    @property
    def endpoint(self):
        return "{}:{}".format(self.host, self.port)

    @property
    def pid(self):
        return self.daemon.process.pid

    def start(self):
        try:
            self.update_command(self.__make_run_command())
            super(KiKiMRNode, self).start()
        finally:
            logger.info("Started node %s", self)
            logger.info("Node %s version:\n%s", self.node_id, self.get_node_binary_version())

    def read_node_config(self):
        config_file = os.path.join(self.__config_path, "config.yaml")
        with open(config_file) as f:
            return yaml.safe_load(f)

    def get_config_version(self):
        config = self.read_node_config()
        return config.get('metadata', {}).get('version', 0)

    def get_node_binary_version(self):
        version_output = yatest.common.execute([self.binary_path, '-V']).std_out.decode('utf-8')
        version_info = []
        for line in version_output.splitlines():
            if not line.strip():
                break
            version_info.append(line)
        return '\n'.join(version_info)

    def enable_config_dir(self):
        self.__use_config_store = True
        self.update_command(self.__make_run_command())

    def disable_config_dir(self, cleanup=True):
        self.__use_config_store = False
        self.update_command(self.__make_run_command())
        if cleanup:
            if self.__configurator.separate_node_configs:
                node_config_dir = os.path.join(
                    self.__config_path,
                )
                if os.path.exists(node_config_dir):
                    shutil.rmtree(node_config_dir)
            else:
                config_file = os.path.join(self.__config_path, "config.yaml")
                if os.path.exists(config_file):
                    os.remove(config_file)

    def set_seed_nodes_file(self, seed_nodes_file):
        self.__seed_nodes_file = seed_nodes_file
        self.update_command(self.__make_run_command())

    def make_config_dir(self, source_config_yaml_path, target_config_dir_path):
        if not os.path.exists(source_config_yaml_path):
            raise RuntimeError("Source config file not found: %s" % source_config_yaml_path)

        try:
            os.makedirs(target_config_dir_path, exist_ok=True)
        except Exception as e:
            raise RuntimeError("Unexpected error initializing config for node %s: %s" % (str(self.node_id), str(e)))


class KiKiMR(kikimr_cluster_interface.KiKiMRClusterInterface):
    def __init__(self, configurator=None, cluster_name='cluster'):
        super(KiKiMR, self).__init__()

        self.__tmpdir = tempfile.mkdtemp(prefix="kikimr_" + cluster_name + "_")
        self.__common_udfs_dir = None
        self.__cluster_name = cluster_name
        self.__configurator = kikimr_config.KikimrConfigGenerator() if configurator is None else configurator
        self.__port_allocator = self.__configurator.port_allocator
        self._nodes = {}
        self._slots = {}
        self.__server = 'localhost'
        self.__storage_pool_id_allocator = itertools.count(1)
        if self.__configurator.separate_node_configs:
            self.__config_base_path = ensure_path_exists(
                os.path.join(self.__configurator.working_dir, self.__cluster_name, "kikimr_configs")
            )
        else:
            self.__config_path = ensure_path_exists(
                os.path.join(self.__configurator.working_dir, self.__cluster_name, "kikimr_configs")
            )
        self._slot_index_allocator = itertools.count(1)
        self._node_index_allocator = itertools.count(1)
        self.default_channel_bindings = None
        self.__initialy_prepared = False
        self.root_token = None

    @property
    def config(self):
        return self.__configurator

    @property
    def nodes(self):
        return self._nodes

    @property
    def slots(self):
        return self._slots

    @property
    def domain_name(self):
        return self.__configurator.domain_name

    @property
    def server(self):
        return self.__server

    def _get_token(self, timeout=30, interval=2):
        start_time = time.time()
        last_exception = None
        while time.time() - start_time < timeout:
            try:
                result = self.__call_ydb_cli(['--user', 'root', '--no-password', 'auth', 'get-token', '--force'], use_database=True)
                token = result.std_out.decode('utf-8').strip()
                if token:
                    logger.info("Successfully got token")
                    return token
                else:
                    raise Exception("Got empty token")

            except Exception as e:
                last_exception = e
                time.sleep(interval)
        raise last_exception

    def __call_kikimr_new_cli(self, cmd, connect_to_server=True, token=None):
        if self.__configurator.protected_mode:
            server = 'grpcs://{server}:{port}'.format(server=self.server, port=self.nodes[1].grpc_ssl_port)
        else:
            server = 'grpc://{server}:{port}'.format(server=self.server, port=self.nodes[1].port)

        binary_path = self.__configurator.get_binary_path(0)
        full_command = [binary_path]
        if connect_to_server:
            full_command += ["--server", server]
            if self.__configurator.protected_mode:
                full_command += ['--ca-file', self.__configurator.grpc_tls_ca_path]
        if self.root_token is not None:
            token_file = tempfile.NamedTemporaryFile(dir=self.__configurator.working_dir, delete=False)
            token_file.write(self.root_token.encode('utf-8'))
            token_file.close()
            full_command += ["--token-file", token_file.name]
        full_command += cmd

        env = None
        token = token or self.__configurator.default_clusteradmin
        if token is not None:
            env = os.environ.copy()
            env['YDB_TOKEN'] = token
        elif self.__configurator.enable_static_auth:
            # If no token is provided, use the default user from the configuration
            default_user = next(iter(self.__configurator.yaml_config["domains_config"]["security_config"]["default_users"]))
            env = os.environ.copy()
            env['YDB_USER'] = default_user["name"]
            env['YDB_PASSWORD'] = default_user["password"]

        logger.debug("Executing command = {}".format(full_command))
        try:
            return yatest.common.execute(full_command, env=env)
        except yatest.common.ExecutionError as e:
            logger.exception("KiKiMR command '{cmd}' failed with error: {e}\n\tstdout: {out}\n\tstderr: {err}".format(
                cmd=" ".join(str(x) for x in full_command),
                e=str(e),
                out=e.execution_result.std_out,
                err=e.execution_result.std_err
            ))
            raise

    def __call_ydb_cli(self, cmd, token=None, check_exit_code=True, use_database=False):
        if self.__configurator.protected_mode:
            endpoint = 'grpcs://{server}:{port}'.format(server=self.server, port=self.nodes[1].grpc_ssl_port)
        else:
            endpoint = 'grpc://{server}:{port}'.format(server=self.server, port=self.nodes[1].port)

        full_command = [self.__configurator.get_ydb_cli_path(), '--endpoint', endpoint]

        if use_database:
            full_command += ['--database', '/{}'.format(self.domain_name)]
        if self.__configurator.protected_mode:
            full_command += ['--ca-file', self.__configurator.grpc_tls_ca_path]

        full_command += ['-y'] + cmd

        env = None
        token = token or self.__configurator.default_clusteradmin
        if token is not None:
            env = os.environ.copy()
            env['YDB_TOKEN'] = token

        logger.debug("Executing command = {}".format(full_command))
        try:
            return yatest.common.execute(full_command, env=env, check_exit_code=check_exit_code)
        except yatest.common.ExecutionError as e:
            logger.exception("KiKiMR command '{cmd}' failed with error: {e}\n\tstdout: {out}\n\tstderr: {err}".format(
                cmd=" ".join(str(x) for x in full_command),
                e=str(e),
                out=e.execution_result.std_out,
                err=e.execution_result.std_err
            ))
            raise

    def start(self, timeout_seconds=240):
        """
        Safely starts kikimr instance.
        Do not override this method.
        """
        try:
            logger.debug("Working directory: " + self.__tmpdir)
            self.__run(timeout_seconds=timeout_seconds)
            return self

        except Exception:
            logger.exception("KiKiMR start failed")
            self.stop()
            raise

    def prepare(self):
        if self.__initialy_prepared:
            return

        self.__initialy_prepared = True
        self.__instantiate_udfs_dir()
        self.__write_configs()
        for _ in self.__configurator.all_node_ids():
            self.__register_node()

    def _bootstrap_cluster(self, self_assembly_uuid="test-cluster", timeout=30, interval=2):
        start_time = time.time()
        last_exception = None
        while time.time() - start_time < timeout:
            try:
                result = self.config_client.bootstrap_cluster(self_assembly_uuid=self_assembly_uuid)
                if result.operation.status == StatusIds.SUCCESS:
                    logger.info("Successfully bootstrapped cluster")
                    return result
                else:
                    error_msg = "Bootstrap cluster failed with status: %s" % (result.operation.status, )
                    for issue in result.operation.issues:
                        error_msg += "\nIssue: %s" % (issue, )
                    raise Exception(error_msg)

            except Exception as e:
                last_exception = e
                time.sleep(interval)
        raise last_exception

    def __run(self, timeout_seconds=240):
        self.prepare()

        for node_id in self.__configurator.all_node_ids():
            self.__run_node(node_id)

        if self.__configurator.use_self_management:
            self._bootstrap_cluster(self_assembly_uuid="test-cluster")

        if self.__configurator.protected_mode:
            time.sleep(5)
            self.root_token = self._get_token()

        bs_needed = ('blob_storage_config' in self.__configurator.yaml_config) or self.__configurator.use_self_management

        if bs_needed:
            token = self.root_token or self.__configurator.default_clusteradmin
            self.__wait_for_bs_controller_to_start(timeout_seconds=timeout_seconds, token=token)
            if not self.__configurator.use_self_management:
                self.__add_bs_box()

        pools = {}

        for p in self.__configurator.dynamic_storage_pools:
            self.add_storage_pool(
                name=p['name'],
                kind=p['kind'],
                pdisk_user_kind=p['pdisk_user_kind'],
                num_groups=p.get('num_groups'),
            )
            pools[p['name']] = p['kind']

        root_token = self.root_token or self.__configurator.default_clusteradmin

        if not root_token and self.__configurator.enable_static_auth:
            root_token = requests.post("http://localhost:%s/login" % self.nodes[1].mon_port, json={
                "user": self.__configurator.yaml_config["domains_config"]["security_config"]["default_users"][0]["name"],
                "password": self.__configurator.yaml_config["domains_config"]["security_config"]["default_users"][0]["password"]
            }).cookies.get('ydb_session_id')
            logger.info("Obtained root token: %s" % root_token)

        if len(pools) > 0:
            logger.info("Binding storage pools to domain %s: %s", self.domain_name, pools)
            self.client.bind_storage_pools(self.domain_name, pools, token=root_token)
            default_pool_name = list(pools.keys())[0]
        else:
            default_pool_name = ""

        self.default_channel_bindings = {idx: default_pool_name for idx in range(3)}
        logger.info("Cluster started and initialized")

        if bs_needed:
            self.client.add_config_item(read_binary(__name__, "resources/default_profile.txt"), token=root_token)

    def __run_node(self, node_id):
        """
        :returns started KiKiMRNode instance
        Can be overriden.
        """
        self.__format_disks(node_id)
        self._nodes[node_id].start()
        return self._nodes[node_id]

    def __register_node(self, configurator=None, seed_nodes_file=None):
        configurator = configurator or self.__configurator
        node_index = next(self._node_index_allocator)

        if configurator.separate_node_configs:
            node_config_path = ensure_path_exists(
                os.path.join(self.__config_base_path, "node_{}".format(node_index))
            )
        else:
            node_config_path = self.__config_path

        data_center = None
        if isinstance(configurator.dc_mapping, dict):
            if node_index in configurator.dc_mapping:
                data_center = configurator.dc_mapping[node_index]
        self._nodes[node_index] = KiKiMRNode(
            node_id=node_index,
            config_path=node_config_path,
            port_allocator=self.__port_allocator.get_node_port_allocator(node_index),
            cluster_name=self.__cluster_name,
            configurator=configurator,
            udfs_dir=self.__common_udfs_dir,
            tenant_affiliation=configurator.yq_tenant,
            binary_path=configurator.get_binary_path(node_index),
            data_center=data_center,
            seed_nodes_file=seed_nodes_file,
        )
        return self._nodes[node_index]

    def __register_slots(self, database, count=1, encryption_key=None, seed_nodes_file=None):
        return [self.__register_slot(database, encryption_key, seed_nodes_file=seed_nodes_file) for _ in range(count)]

    def register_and_start_slots(self, database, count=1, encryption_key=None, seed_nodes_file=None):
        slots = self.__register_slots(database, count, encryption_key, seed_nodes_file=seed_nodes_file)
        for slot in slots:
            slot.start()
        return slots

    def write_encryption_key(self, slug):
        workdir = os.path.join(self.__configurator.working_dir, self.__cluster_name)
        secret_path = os.path.join(workdir, slug + "_secret.txt")
        with open(secret_path, "w") as writer:
            writer.write("fake_secret_data_for_%s" % slug)
        keyfile_path = os.path.join(workdir, slug + "_key.txt")
        with open(keyfile_path, "w") as writer:
            writer.write('Keys { ContainerPath: "%s" Pin: "" Id: "%s" Version: 1 } ' % (secret_path, slug))
        return keyfile_path

    def __register_slot(self, tenant_affiliation=None, encryption_key=None, seed_nodes_file=None):
        slot_index = next(self._slot_index_allocator)
        node_broker_port = (
            self.nodes[1].grpc_ssl_port if self.__configurator.grpc_ssl_enable
            else self.nodes[1].grpc_port
        )

        if tenant_affiliation is None:
            tenant_affiliation = "dynamic"

        if encryption_key is None and self.__configurator.enable_pool_encryption:
            slug = tenant_affiliation.replace('/', '_')
            encryption_key = self.write_encryption_key(slug)

        self._slots[slot_index] = KiKiMRNode(
            node_id=slot_index,
            config_path=self.config_path,
            port_allocator=self.__port_allocator.get_slot_port_allocator(slot_index),
            cluster_name=self.__cluster_name,
            configurator=self.__configurator,
            udfs_dir=self.__common_udfs_dir,
            role='slot',
            node_broker_port=node_broker_port,
            tenant_affiliation=tenant_affiliation,
            encryption_key=encryption_key,
            binary_path=self.__configurator.get_binary_path(slot_index),
            seed_nodes_file=seed_nodes_file,
        )
        return self._slots[slot_index]

    def __unregister_slots(self, slots):
        for i in slots:
            del self._slots[i.node_id]

    def unregister_and_stop_slots(self, slots):
        self.__unregister_slots(slots)
        for i in slots:
            i.stop()

    def __stop_node(self, node, kill=False):
        ret = None
        try:
            if kill:
                node.kill()
            else:
                node.stop()
        except daemon.DaemonError as exceptions:
            ret = exceptions
        else:
            if self.__tmpdir is not None:
                shutil.rmtree(self.__tmpdir, ignore_errors=True)
            if self.__common_udfs_dir is not None:
                shutil.rmtree(self.__common_udfs_dir, ignore_errors=True)
        return ret

    def stop(self, kill=False):
        saved_exceptions_queue = Queue()

        def stop_node(node, kill):
            exception = self.__stop_node(node, kill)
            if exception is not None:
                saved_exceptions_queue.put(exception)

        # do in parallel to faster stopping (important for tests)
        threads = []
        for node in list(self.slots.values()) + list(self.nodes.values()):
            thread = threading.Thread(target=stop_node, args=(node, kill))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()

        saved_exceptions = list(saved_exceptions_queue.queue)

        self.__port_allocator.release_ports()

        if saved_exceptions:
            raise daemon.SeveralDaemonErrors(saved_exceptions)

    def restart_nodes(self):
        for node in self.nodes.values():
            node.stop()
            node.start()

    def prepare_node(self, configurator=None, seed_nodes_file=None):
        try:
            new_node_object = self.__register_node(configurator, seed_nodes_file)
            # Skip writing protocol configuration files if seeding nodes from a file
            skip_proto_write = seed_nodes_file is not None
            self.__write_node_config(new_node_object.node_id, configurator, skip_proto_write)
            logger.info("Successfully registered new node object with ID: %s" % str(new_node_object.node_id))
            return new_node_object
        except Exception as e:
            logger.error("Failed to register new node: %s" % str(e), exc_info=True)
            raise RuntimeError("Failed to register new node: %s" % str(e))

    def start_node(self, node_id):
        if node_id not in self._nodes:
            logger.error("Cannot start node: Node ID %s not found in registered nodes." % str(node_id))
            raise KeyError("Node ID %s not found." % str(node_id))

        logger.info("Starting registered node %s." % str(node_id))
        try:
            self._KiKiMR__run_node(node_id)
            logger.info("Successfully started node %s." % str(node_id))
        except Exception as e:
            raise RuntimeError("Failed to start node %s: %s" % (str(node_id), str(e)))

    def update_configurator_and_restart(self, configurator):
        for node in itertools.chain(self.nodes.values(), self.slots.values()):
            node.stop()

        self.__configurator = configurator
        self.__initialy_prepared = False
        # re-register nodes
        self._node_index_allocator = itertools.count(1)
        self.prepare()
        # re-register slots
        tenants = [s._tenant_affiliation for s in self.slots.values()]
        self._slot_index_allocator = itertools.count(1)
        for tenant in tenants:
            self.__register_slot(tenant)

        for node in itertools.chain(self.nodes.values(), self.slots.values()):
            node.start()

    def enable_config_dir(self, node_ids=None):
        if node_ids is None:
            node_ids = self.__configurator.all_node_ids()
        self.__configurator.use_config_store = True
        for node_id in node_ids:
            self.nodes[node_id].enable_config_dir()

    def disable_config_dir(self, node_ids=None):
        if node_ids is None:
            node_ids = self.__configurator.all_node_ids()
        self.__configurator.use_config_store = False
        for node_id in node_ids:
            self.nodes[node_id].disable_config_dir()

    @property
    def config_path(self):
        if self.__configurator.separate_node_configs:
            return self.__config_base_path
        return self.__config_path

    def __write_node_config(self, node_id, configurator=None, config_dir_only=False):
        configurator = configurator or self.__configurator
        node_config_path = ensure_path_exists(
            os.path.join(self.__config_base_path, "node_{}".format(node_id))
        )
        if config_dir_only:
            return
        configurator.write_proto_configs(node_config_path)

    def __write_configs(self):
        if self.__configurator.separate_node_configs:
            for node_id in self.__configurator.all_node_ids():
                self.__write_node_config(node_id)
        else:
            self.__configurator.write_proto_configs(self.__config_path)

    def overwrite_configs(self, config, node_ids=None):
        self.__configurator.full_config = config
        if node_ids is None:
            self.__write_configs()
        else:
            if not self.__configurator.separate_node_configs:
                raise ValueError(
                    "overwrite_configs(node_ids=...) is only supported when "
                    "separate_node_configs is enabled"
                )
            for node_id in node_ids:
                self.__write_node_config(node_id)

    def __instantiate_udfs_dir(self):
        to_load = self.__configurator.get_yql_udfs_to_load()
        if len(to_load) == 0:
            return
        self.__common_udfs_dir = tempfile.mkdtemp(prefix="common_udfs")
        for udf_path in to_load:
            link_name = os.path.join(self.__common_udfs_dir, os.path.basename(udf_path))
            os.symlink(udf_path, link_name)
        return self.__common_udfs_dir

    def __format_disks(self, node_id):
        for pdisk in self.__configurator.pdisks_info:
            if pdisk['node_id'] != node_id:
                continue

            self.nodes[node_id].format_pdisk(**pdisk)

    def __add_bs_box(self):
        request = bs.TConfigRequest()

        for node_id in self.__configurator.all_node_ids():
            cmd = request.Command.add()
            cmd.DefineHostConfig.HostConfigId = node_id
            for drive in self.__configurator.pdisks_info:
                if drive['node_id'] != node_id:
                    continue

                drive_proto = cmd.DefineHostConfig.Drive.add()
                drive_proto.Path = drive['pdisk_path']
                drive_proto.Kind = drive['pdisk_user_kind']
                drive_proto.Type = drive.get('pdisk_type', 0)

                for key, value in drive.get('pdisk_config', {}).items():
                    if key == 'expected_slot_count':
                        drive_proto.PDiskConfig.ExpectedSlotCount = value
                    else:
                        raise KeyError("unknown pdisk_config option %s" % key)

        cmd = request.Command.add()
        cmd.DefineBox.BoxId = 1
        for node_id, node in self.nodes.items():
            host = cmd.DefineBox.Host.add()
            host.Key.Fqdn = node.host
            host.Key.IcPort = node.ic_port
            host.HostConfigId = node_id

        self._bs_config_invoke(request)

    def _bs_config_invoke(self, request):
        timeout = 240
        sleep = 5
        retries, success = timeout / sleep, False
        while retries > 0 and not success:
            try:
                self.__call_kikimr_new_cli(
                    [
                        "admin",
                        "blobstorage",
                        "config",
                        "invoke",
                        "--proto=%s" % text_format.MessageToString(request)
                    ]
                )
                success = True

            except Exception as e:
                logger.error("Failed to execute, %s", str(e))
                retries -= 1
                time.sleep(sleep)

                if retries == 0:
                    raise

    def add_storage_pool(self, name=None, kind="rot", pdisk_user_kind=0, erasure=None, num_groups=None):
        if erasure is None:
            erasure = self.__configurator.static_erasure
        if num_groups is None:
            num_groups = 2
        request = bs.TConfigRequest()
        cmd = request.Command.add()
        cmd.DefineStoragePool.BoxId = 1

        pool_id = cmd.DefineStoragePool.StoragePoolId = next(self.__storage_pool_id_allocator)
        if name is None:
            name = "dynamic_storage_pool:%s" % pool_id
        cmd.DefineStoragePool.StoragePoolId = pool_id
        cmd.DefineStoragePool.Name = name
        cmd.DefineStoragePool.Kind = kind
        cmd.DefineStoragePool.ErasureSpecies = str(erasure)
        cmd.DefineStoragePool.VDiskKind = "Default"
        cmd.DefineStoragePool.NumGroups = num_groups

        if str(erasure) == "mirror-3-dc" and len(self.__configurator.all_node_ids()) == 3:
            cmd.DefineStoragePool.Geometry.RealmLevelBegin = int(FailDomainType.DC)
            cmd.DefineStoragePool.Geometry.RealmLevelEnd = int(FailDomainType.Room)
            cmd.DefineStoragePool.Geometry.DomainLevelBegin = int(FailDomainType.DC)
            cmd.DefineStoragePool.Geometry.DomainLevelEnd = int(FailDomainType.Disk) + 1

        pdisk_filter = cmd.DefineStoragePool.PDiskFilter.add()
        pdisk_filter.Property.add().Type = 0
        pdisk_filter.Property.add().Kind = pdisk_user_kind
        self._bs_config_invoke(request)
        return name

    def __wait_for_bs_controller_to_start(self, timeout_seconds=240, token=None):
        monitors = [
            KikimrMonitor(
                node.host,
                node.mon_port,
                use_https=getattr(node, 'mon_uses_https', False),
                token=token
            )
            for node in self.nodes.values()
        ]

        def predicate():
            return blobstorage_controller_has_started_on_some_node(monitors)

        bs_controller_started = wait_for(
            predicate=predicate, timeout_seconds=timeout_seconds, step_seconds=1.0, multiply=1.3
        )
        assert bs_controller_started

    def replace_config(self, config):
        timeout = 10
        sleep = 2
        retries, success = timeout / sleep, False
        while retries > 0 and not success:
            try:
                self.__call_ydb_cli(
                    [
                        "admin",
                        "cluster",
                        "config",
                        "replace",
                        "-f", config
                    ]
                )
                success = True
            except Exception as e:
                logger.error("Failed to execute, %s", str(e))
                retries -= 1
                time.sleep(sleep)
        if not success:
            logger.error("Failed to replace config")
            raise RuntimeError("Failed to replace config")

    def fetch_config(self):
        try:
            result = self.__call_ydb_cli(
                [
                    "admin",
                    "cluster",
                    "config",
                    "fetch"
                ],
                check_exit_code=False
            )
            if result.exit_code != 0:
                return None
            return result.std_out.decode('utf-8')
        except Exception as e:
            logger.error("Error fetching config: %s", str(e), exc_info=True)
            return None

    def generate_config(self):
        result = self.__call_ydb_cli(
            [
                "admin",
                "cluster",
                "config",
                "generate"
            ]
        )
        return result.std_out.decode('utf-8')

    def _create_config_client(self):
        first_node = self.nodes[list(self.nodes.keys())[0]]
        if self.__configurator.protected_mode:
            port = first_node.grpc_ssl_port
            client = config_client_factory(
                first_node.host, port, retry_count=20,
                ca_path=self.__configurator.grpc_tls_ca_path,
                cert_path=self.__configurator.grpc_tls_cert_path,
                key_path=self.__configurator.grpc_tls_key_path
            )
            return client
        else:
            port = first_node.port
            return config_client_factory(first_node.host, port, retry_count=20)


class KikimrExternalNode(daemon.ExternalNodeDaemon, kikimr_node_interface.NodeInterface):
    kikimr_binary_deploy_path = '/Berkanavt/kikimr/bin/kikimr'
    kikimr_configure_binary_deploy_path = '/Berkanavt/kikimr/bin/kikimr_configure'
    kikimr_configuration_deploy_path = '/Berkanavt/kikimr/cfg'
    kikimr_cluster_yaml_deploy_path = '/Berkanavt/kikimr/cfg/cluster.yaml'

    def __init__(
            self,
            kikimr_configure_binary_path,
            kikimr_path,
            kikimr_next_path,
            node_id,
            host,
            datacenter,
            rack,
            bridge_pile_name,
            bridge_pile_id,
            ssh_username,
            port,
            mon_port,
            ic_port,
            mbus_port,
            configurator=None,
            slot_id=None,
            ):
        super(KikimrExternalNode, self).__init__(host=host, ssh_username=ssh_username)
        self.__node_id = node_id
        self.__host = host
        self.__port = port
        self.__grpc_port = port
        self.__mon_port = mon_port
        self.__ic_port = ic_port
        self.__datacenter = datacenter
        self.__rack = rack
        self.__bridge_pile_name = bridge_pile_name
        self.__bridge_pile_id = bridge_pile_id
        self.__configurator = configurator
        self.__mbus_port = mbus_port
        self.logger = logger.getChild(self.__class__.__name__)
        if slot_id is not None:
            self.__slot_id = "%s" % str(self.__ic_port)
        else:
            self.__slot_id = None

        self._can_update = None
        self.current_version_idx = 0
        self.__kikimr_configure_binary_path = kikimr_configure_binary_path
        self.versions = [
            self.kikimr_binary_deploy_path + "_last",
            self.kikimr_binary_deploy_path + "_next",
        ]

        self.local_drivers_path = [
            kikimr_path,
            kikimr_next_path,
        ]

    @property
    def can_update(self):
        if self._can_update is None:
            choices = self.ssh_command('ls %s*' % self.kikimr_binary_deploy_path, raise_on_error=True)
            choices = choices.split()
            choices = [path.decode("utf-8", errors="replace") for path in choices]
            self.logger.error("Current available choices are: %s" % choices)
            self._can_update = True
            for version in self.versions:
                if version not in choices:
                    self._can_update &= False
        return self._can_update

    def start(self):
        if self.__slot_id is None:
            return self.ssh_command("sudo service kikimr start")

        slot_dir = "/Berkanavt/kikimr_{slot}".format(slot=self.__slot_id)
        slot_cfg = slot_dir + "/slot_cfg"
        env_txt = slot_dir + "/env.txt"

        cfg = """\
tenant=/Root/db1
grpc={grpc}
mbus={mbus}
ic={ic}
mon={mon}""".format(
            mbus=self.__mbus_port,
            grpc=self.__grpc_port,
            mon=self.__mon_port,
            ic=self.__ic_port,
        )

        self.ssh_command(["sudo", "mkdir", slot_dir])
        self.ssh_command(["sudo", "touch", env_txt])
        self.ssh_command(["/bin/echo", "-e", "\"{}\"".format(cfg),  "|", "sudo", "tee", slot_cfg])

        return self.ssh_command(["sudo", "systemctl", "start", "kikimr-multi@{}".format(self.__slot_id)])

    def stop(self):
        if self.__slot_id is None:
            return self.ssh_command("sudo service kikimr stop")
        return self.ssh_command(
            [
                "sudo", "systemctl", "stop", "kikimr-multi@{}".format(self.__slot_id),
            ]
        )

    @property
    def cwd(self):
        assert False, "not supported"

    @property
    def mon_port(self):
        return self.__mon_port

    @property
    def pid(self):
        return None

    def is_alive(self):
        # TODO implement check
        return True

    @property
    def host(self):
        return self.__host

    @property
    def datacenter(self):
        return self.__datacenter

    @property
    def rack(self):
        return self.__rack

    @property
    def bridge_pile_name(self):
        return self.__bridge_pile_name

    @property
    def bridge_pile_id(self):
        return self.__bridge_pile_id

    @property
    def port(self):
        return self.__port

    @property
    def grpc_ssl_port(self):
        # TODO(gvit): support in clusters
        return None

    @property
    def grpc_port(self):
        return self.__port

    @property
    def mbus_port(self):
        return self.__mbus_port

    @property
    def ic_port(self):
        return self.__ic_port

    @property
    def node_id(self):
        return self.__node_id

    @property
    def logs_directory(self):
        folder = 'kikimr_%s' % self.__slot_id if self.__slot_id else 'kikimr'
        return "/Berkanavt/{}/logs".format(folder)

    def update_binary_links(self):
        self.ssh_command("sudo rm -rf %s" % self.kikimr_binary_deploy_path)
        self.ssh_command(
            "sudo cp -l %s %s" % (
                self.versions[self.current_version_idx],
                self.kikimr_binary_deploy_path,
            )
        )

    def __generate_configs_cmd(self, configs_type="", deploy_path=None):
        deploy_path = self.kikimr_configuration_deploy_path if deploy_path is None else deploy_path
        return "sudo {} cfg {} {} {} --enable-cores {}".format(
            self.kikimr_configure_binary_deploy_path,
            self.kikimr_cluster_yaml_deploy_path, self.kikimr_binary_deploy_path, deploy_path, configs_type)

    def switch_version(self):
        if not self.can_update:
            self.logger.info("Next version is not available. Cannot change versions.")
            return None

        self.current_version_idx ^= 1
        self.update_binary_links()

    def prepare_artifacts(self, cluster_yml):
        if self.__kikimr_configure_binary_path is not None:
            self.copy_file_or_dir(
                self.__kikimr_configure_binary_path, self.kikimr_configure_binary_deploy_path)

        for version, local_driver in zip(self.versions, self.local_drivers_path):
            self.ssh_command("sudo rm -rf %s" % version)
            if local_driver is not None:
                self.copy_file_or_dir(
                    local_driver, version)
                self.ssh_command("sudo /sbin/setcap 'CAP_SYS_RAWIO,CAP_SYS_NICE=ep' %s" % version)

        self.update_binary_links()
        if self.__kikimr_configure_binary_path is not None:
            self.ssh_command("sudo mkdir -p %s" % self.kikimr_configuration_deploy_path)
            self.copy_file_or_dir(cluster_yml, self.kikimr_cluster_yaml_deploy_path)
            self.ssh_command(self.__generate_configs_cmd())
            self.ssh_command(
                self.__generate_configs_cmd(
                    "--dynamic"
                )
            )

    def format_pdisk(self, pdisk_id):
        pass

    def cleanup_disk(self, path):
        self.ssh_command(
            'sudo /Berkanavt/kikimr/bin/kikimr admin bs disk obliterate {};'.format(path),
            raise_on_error=True)

    def cleanup_disks(self):
        self.ssh_command(
            "for X in /dev/disk/by-partlabel/kikimr_*; "
            "do sudo /Berkanavt/kikimr/bin/kikimr admin bs disk obliterate $X; done",
            raise_on_error=True)
