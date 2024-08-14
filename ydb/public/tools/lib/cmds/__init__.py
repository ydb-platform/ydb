# -*- coding: utf-8 -*-
import argparse
import shutil
import signal
import os
import json
import random
import string
import typing  # noqa: F401
import sys
import yaml
from mergedeep import merge
from six.moves.urllib.parse import urlparse

from ydb.library.yql.providers.common.proto.gateways_config_pb2 import TGenericConnectorConfig
from ydb.tests.library.common import yatest_common
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.daemon import Daemon
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.harness.kikimr_port_allocator import KikimrFixedPortAllocator, KikimrFixedNodePortAllocator
from library.python.testing.recipe import set_env


class EmptyArguments(object):
    def __init__(self):
        self.ydb_working_dir = None
        self.erasure = None
        self.ydb_binary_path = None
        self.use_packages = None
        self.suppress_version_check = False
        self.ydb_udfs_dir = None
        self.fq_config_path = None
        self.auth_config_path = None
        self.debug_logging = []
        self.fixed_ports = False
        self.base_port_offset = 0
        self.public_http_config_path = None
        self.dont_use_log_files = False
        self.enabled_feature_flags = []
        self.enabled_grpc_services = []


def ensure_path_exists(path):
    if not os.path.isdir(path):
        os.makedirs(path)
    return path


def parse_erasure(args):
    erasure = os.getenv("YDB_ERASURE")
    if erasure is not None:
        return Erasure.from_string(erasure)
    if args.erasure is None:
        return None
    return Erasure.from_string(args.erasure)


def driver_path_packages(package_path):
    return yatest_common.build_path(
        "{}/Berkanavt/kikimr/bin/kikimr".format(
            package_path
        )
    )


def udfs_path_packages(package_path):
    return yatest_common.build_path(
        "{}/Berkanavt/kikimr/libs".format(
            package_path
        )
    )


def wrap_path(path):
    if path is None:
        return path
    return os.path.abspath(
        os.path.expanduser(
            path
        )
    )


def write_file_flushed(filename, content):
    with open(filename, 'w') as fd:
        fd.write(content)
        fd.flush()
        os.fsync(fd.fileno())


def write_file(args, suffix, content):
    if args.ydb_working_dir:
        write_file_flushed(os.path.join(args.ydb_working_dir, suffix), content)
        return

    write_file_flushed(os.path.join(yatest_common.output_path(suffix)), content)

    try:
        write_file_flushed(suffix, content)
    except IOError:
        return


def read_file(args, suffix):
    if args.ydb_working_dir:
        with open(os.path.join(args.ydb_working_dir, suffix), 'r') as fd:
            return fd.read()

    with open(os.path.join(yatest_common.output_path(suffix)), 'r') as fd:
        return fd.read()


def read_recipe_meta_file(args):
    return json.loads(read_file(args, 'ydb_recipe.json'))


def write_recipe_meta_file(args, content):
    write_file(args, 'ydb_recipe.json', json.dumps(content))


def write_ydb_database_file(args, content):
    write_file(args, 'ydb_database.txt', content)


def write_endpoints_file(args, content):
    write_file(args, 'ydb_endpoint.txt', '\n'.join(map(str, content)))


def initialize_working_dir(args):
    if args.ydb_working_dir:
        try:
            os.mkdir(args.ydb_working_dir)
        except OSError:
            pass


def random_string():
    return ''.join([random.choice(string.ascii_lowercase) for _ in range(6)])


class Recipe(object):
    def __init__(self, arguments):
        self.recipe_metafile = 'ydb_recipe.json'
        self.endpoint_file = 'ydb_endpoint.txt'
        self.database_file = 'ydb_database.txt'
        self.arguments = arguments
        self.data_path = None
        self.recipe_metafile_var = 'YDB_RECIPE_METAFILE'
        self.data_path_template = 'ydb_data_%s'

    def metafile_path(self):
        if self.arguments.ydb_working_dir:
            return os.path.join(self.arguments.ydb_working_dir, self.recipe_metafile)
        if os.getenv(self.recipe_metafile_var) is not None:
            return os.getenv(self.recipe_metafile_var)
        return os.path.join(self.generate_data_path(), self.recipe_metafile)

    def database_file_path(self):
        if self.arguments.ydb_working_dir:
            return os.path.join(self.arguments.ydb_working_dir, self.database_file)

    def endpoint_file_path(self):
        if self.arguments.ydb_working_dir:
            return os.path.join(self.arguments.ydb_working_dir, self.endpoint_file)

    def write(self, path, content):
        if path is None:
            return

        with open(path, 'w') as fd:
            fd.write(content)
            fd.flush()
            os.fsync(fd.fileno())

    def read(self, path):
        with open(path, 'r') as r:
            data = r.read()
        return data

    def setenv(self, varname, value):
        os.environ[varname] = value
        try:
            set_env(varname, value)
        except Exception:
            # NOTE(gvit): that is possible in case when yatest infra is not available
            pass

    def write_metafile(self, content):
        self.setenv(self.recipe_metafile_var, self.metafile_path())
        return self.write(self.metafile_path(), json.dumps(content))

    def write_endpoint(self, endpoint):
        self.setenv('YDB_ENDPOINT', endpoint)
        self.write(self.endpoint_file_path(), endpoint)
        # write for compatibility
        try:
            self.write(self.endpoint_file, endpoint)
        except Exception:
            pass

    def write_database(self, database):
        self.setenv('YDB_DATABASE', database)
        self.write(self.database_file_path(), database)
        # write for compatibility
        try:
            self.write(self.database_file, database)
        except Exception:
            pass

    def write_connection_string(self, connection_string):
        self.setenv('YDB_CONNECTION_STRING', connection_string)

    def write_certificates_path(self, certificates_path):
        self.setenv('YDB_SSL_ROOT_CERTIFICATES_FILE', certificates_path)

    def read_metafile(self):
        return json.loads(self.read(self.metafile_path()))

    def generate_data_path(self):
        if self.data_path is not None:
            return self.data_path
        if self.arguments.ydb_working_dir:
            self.data_path = self.arguments.ydb_working_dir
            return self.data_path
        self.data_path = yatest_common.output_path(self.data_path_template % random_string())
        return ensure_path_exists(self.data_path)


def use_in_memory_pdisks_flag(ydb_working_dir=None):
    if os.getenv('YDB_USE_IN_MEMORY_PDISKS') is not None:
        return os.getenv('YDB_USE_IN_MEMORY_PDISKS') == "true"

    if ydb_working_dir:
        return False

    return True


def default_users():
    if not os.getenv("POSTGRES_USER") and not os.getenv("POSTGRES_PASSWORD"):
        return None

    user = os.getenv("POSTGRES_USER")
    if not user:
        user = "postgres"

    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        password = ""
    return {user: password}


def enable_survive_restart():
    return os.getenv('YDB_LOCAL_SURVIVE_RESTART') == 'true'


def enable_tls():
    return os.getenv('YDB_GRPC_ENABLE_TLS') == 'true'


def generic_connector_config():
    endpoint = os.getenv("FQ_CONNECTOR_ENDPOINT")
    if not endpoint:
        return None

    parsed = urlparse(endpoint)
    if not parsed.hostname:
        raise ValueError("Invalid host '{}' in FQ_CONNECTOR_ENDPOINT".format(parsed.hostname))

    if not (1024 <= parsed.port <= 65535):
        raise ValueError("Invalid port '{}' in FQ_CONNECTOR_ENDPOINT".format(parsed.port))

    valid_schemes = ['grpc', 'grpcs']
    if parsed.scheme not in valid_schemes:
        raise ValueError("Invalid schema '{}' in FQ_CONNECTOR_ENDPOINT (possible: {})".format(parsed.scheme, valid_schemes))

    cfg = TGenericConnectorConfig()
    cfg.Endpoint.host = parsed.hostname
    cfg.Endpoint.port = parsed.port

    if parsed.scheme == 'grpc':
        cfg.UseSsl = False
    elif parsed.scheme == 'grpcs':
        cfg.UseSsl = True

    return cfg


def grpc_tls_data_path(arguments):
    default_store = arguments.ydb_working_dir if arguments.ydb_working_dir else None
    return os.getenv('YDB_GRPC_TLS_DATA_PATH', default_store)


def pq_client_service_types(arguments):
    items = getattr(arguments, 'pq_client_service_types', None)
    if not items:
        types_str = os.getenv('YDB_PQ_CLIENT_SERVICE_TYPES')
        if types_str:
            items = types_str.split(',')
    service_types = []
    if items:
        for item in items:
            item = item.strip()
            if item:
                service_types.append(item)
    return service_types


def enable_pqcd(arguments):
    return (getattr(arguments, 'enable_pqcd', False) or os.getenv('YDB_ENABLE_PQCD') == 'true')

def merge_two_yaml_configs(main_yaml_config, additioanal_yaml_config):
    return merge(main_yaml_config, additioanal_yaml_config)

def get_additional_yaml_config(arguments, path):
    if arguments.ydb_working_dir:
        with open(os.path.join(arguments.ydb_working_dir, path)) as fh:
            additional_yaml_config = yaml.load(fh, Loader=yaml.FullLoader)
    else:
        raise Exception("No working directory")

    return additional_yaml_config


def deploy(arguments):
    initialize_working_dir(arguments)
    recipe = Recipe(arguments)

    if os.path.exists(recipe.metafile_path()) and enable_survive_restart():
        return start(arguments)

    if getattr(arguments, 'use_packages', None) is not None:
        arguments.ydb_binary_path = driver_path_packages(arguments.use_packages)
        arguments.ydb_udfs_dir = None

    additional_log_configs = {}
    if getattr(arguments, 'debug_logging', []):
        debug_logging = getattr(arguments, 'debug_logging', [])
        additional_log_configs = {
            k: LogLevels.DEBUG
            for k in debug_logging
        }

    port_allocator = None
    if getattr(arguments, 'fixed_ports', False):
        base_port_offset = getattr(arguments, 'base_port_offset', 0)
        port_allocator = KikimrFixedPortAllocator(base_port_offset, [KikimrFixedNodePortAllocator(base_port_offset=base_port_offset)])

    optionals = {}
    if enable_tls():
        optionals.update({'grpc_tls_data_path': grpc_tls_data_path(arguments)})
        optionals.update({'grpc_ssl_enable': enable_tls()})
    pdisk_store_path = arguments.ydb_working_dir if arguments.ydb_working_dir else None

    enable_feature_flags = arguments.enabled_feature_flags.copy()  # type: typing.List[str]
    if 'YDB_FEATURE_FLAGS' in os.environ:
        flags = os.environ['YDB_FEATURE_FLAGS'].split(",")
        for flag_name in flags:
            enable_feature_flags.append(flag_name)

    if 'YDB_EXPERIMENTAL_PG' in os.environ:
        optionals['pg_compatible_expirement'] = True

    configuration = KikimrConfigGenerator(
        parse_erasure(arguments),
        arguments.ydb_binary_path,
        output_path=recipe.generate_data_path(),
        pdisk_store_path=pdisk_store_path,
        domain_name='local',
        pq_client_service_types=pq_client_service_types(arguments),
        enable_pqcd=enable_pqcd(arguments),
        load_udfs=True,
        suppress_version_check=arguments.suppress_version_check,
        udfs_path=arguments.ydb_udfs_dir,
        additional_log_configs=additional_log_configs,
        port_allocator=port_allocator,
        use_in_memory_pdisks=use_in_memory_pdisks_flag(arguments.ydb_working_dir),
        fq_config_path=arguments.fq_config_path,
        public_http_config_path=arguments.public_http_config_path,
        auth_config_path=arguments.auth_config_path,
        use_log_files=not arguments.dont_use_log_files,
        default_users=default_users(),
        extra_feature_flags=enable_feature_flags,
        extra_grpc_services=arguments.enabled_grpc_services,
        generic_connector_config=generic_connector_config(),
        **optionals
    )

    if os.getenv("YDB_CONFIG_PATCH") is not None:      
        additional_yaml_config = get_additional_yaml_config(arguments, os.getenv("YDB_CONFIG_PATCH"))
        configuration.yaml_config = merge_two_yaml_configs(configuration.yaml_config, additional_yaml_config)
    
    cluster = kikimr_cluster_factory(configuration)
    cluster.start()

    info = {'nodes': {}}
    endpoints = []
    for node_id, node in cluster.nodes.items():
        info['nodes'][node_id] = {
            'pid': node.pid,
            'host': node.host,
            'sqs_port': node.sqs_port,
            'grpc_port': node.port,
            'mon_port': node.mon_port,
            'command': node.command,
            'cwd': node.cwd,
            'stdin_file': node.stdin_file_name,
            'stderr_file': node.stderr_file_name,
            'stdout_file': node.stdout_file_name,
            'pdisks': [
                drive.get('pdisk_path')
                for drive in cluster.config.pdisks_info
            ]
        }

        endpoints.append("localhost:%d" % node.grpc_port)

    endpoint = endpoints[0]
    database = cluster.domain_name
    recipe.write_metafile(info)
    recipe.write_endpoint(endpoint)
    recipe.write_database(cluster.domain_name)
    recipe.write_connection_string(("grpcs://" if enable_tls() else "grpc://") + endpoint + "?database=/" + cluster.domain_name)
    if enable_tls():
        recipe.write_certificates_path(configuration.grpc_tls_ca.decode("utf-8"))
    return endpoint, database


def _stop_instances(arguments):
    recipe = Recipe(arguments)
    if not os.path.exists(recipe.metafile_path()):
        return
    try:
        info = recipe.read_metafile()
    except Exception:
        sys.stderr.write("Metafile not found for ydb recipe ...")
        return

    for node_id, node_meta in info['nodes'].items():
        pid = node_meta['pid']
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass

        try:
            with open(node_meta['stderr_file'], "r") as r:
                sys.stderr.write(r.read())

        except Exception as e:
            sys.stderr.write(str(e))


def cleanup_working_dir(arguments):
    if arguments.ydb_working_dir:
        try:
            shutil.rmtree(arguments.ydb_working_dir)
        except Exception:
            pass


def _cleanup_working_dir(arguments):
    recipe = Recipe(arguments)
    if not os.path.exists(recipe.metafile_path()):
        return
    info = recipe.read_metafile()
    for node_id, node_meta in info['nodes'].items():
        pdisks = node_meta['pdisks']
        for pdisk in pdisks:
            try:
                if not use_in_memory_pdisks_flag():
                    os.remove(pdisk)
            except IOError:
                pass

    if arguments.ydb_working_dir:
        try:
            shutil.rmtree(arguments.ydb_working_dir)
        except Exception:
            pass


def start(arguments):
    recipe = Recipe(arguments)
    info = recipe.read_metafile()
    for node_id, node_meta in info['nodes'].items():
        files = {}
        if node_meta['stderr_file'] is not None and os.path.exists(node_meta['stderr_file']):
            files = {
                'stdin_file': node_meta['stdin_file'],
                'stderr_file': node_meta['stderr_file'],
                'stdout_file': node_meta['stdout_file'],
            }

        daemon = Daemon(
            node_meta['command'],
            node_meta['cwd'],
            timeout=5,
            **files
        )

        daemon.start()
        info['nodes'][node_id]['pid'] = daemon.daemon.process.pid

    recipe.write_metafile(info)


def stop(arguments):
    _stop_instances(arguments)


def update(arguments):
    stop(arguments)
    start(arguments)


def cleanup(arguments):
    for func in (_stop_instances, _cleanup_working_dir):
        func(
            arguments
        )


def produce_arguments(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--use-packages", action="store", default=None)
    parser.add_argument("--suppress-version-check", action='store_true', default=False)
    parser.add_argument("--ydb-working-dir", action="store")
    parser.add_argument("--debug-logging", nargs='*')
    parser.add_argument("--enable-pq", action='store_true', default=False)
    parser.add_argument("--fixed-ports", action='store_true', default=False)
    parser.add_argument("--base-port-offset", action="store", type=int, default=0)
    parser.add_argument("--pq-client-service-type", action='append', default=[])
    parser.add_argument("--enable-datastreams", action='store_true', default=False)
    parser.add_argument("--enable-pqcd", action='store_true', default=False)
    parsed, _ = parser.parse_known_args(args)
    arguments = EmptyArguments()
    arguments.suppress_version_check = parsed.suppress_version_check
    arguments.ydb_working_dir = parsed.ydb_working_dir
    arguments.fixed_ports = parsed.fixed_ports
    arguments.base_port_offset = parsed.base_port_offset
    if parsed.use_packages is not None:
        arguments.use_packages = parsed.use_packages
    if parsed.debug_logging:
        arguments.debug_logging = parsed.debug_logging
    arguments.enable_pq = parsed.enable_pq
    arguments.pq_client_service_types = parsed.pq_client_service_type
    arguments.enable_datastreams = parsed.enable_datastreams
    arguments.enable_pqcd = parsed.enable_pqcd
    return arguments


def start_recipe(args):
    arguments = produce_arguments(args)
    return deploy(arguments)


def stop_recipe(args):
    arguments = produce_arguments(args)
    cleanup(arguments)
