# -*- coding: utf-8 -*-
import copy
import itertools
import os
import socket
import sys
import tempfile

import six
import yaml
from google.protobuf.text_format import Parse
from importlib_resources import read_binary

import yatest

from ydb.core.protos import config_pb2
from ydb.tests.library.common.types import Erasure, FailDomainType

from . import tls_tools
from .kikimr_port_allocator import KikimrPortManagerPortAllocator
from .param_constants import kikimr_driver_path, ydb_cli_path
from .util import LogLevels

import logging

logger = logging.getLogger(__name__)

PDISK_SIZE_STR = os.getenv("YDB_PDISK_SIZE", str(64 * 1024 * 1024 * 1024))
if PDISK_SIZE_STR.endswith("KB"):
    PDISK_SIZE = int(PDISK_SIZE_STR[:-2]) * 1024
elif PDISK_SIZE_STR.endswith("MB"):
    PDISK_SIZE = int(PDISK_SIZE_STR[:-2]) * 1024 * 1024
elif PDISK_SIZE_STR.endswith("GB"):
    PDISK_SIZE = int(PDISK_SIZE_STR[:-2]) * 1024 * 1024 * 1024
else:
    PDISK_SIZE = int(PDISK_SIZE_STR)

KNOWN_STATIC_YQL_UDFS = set([
    "yql/udfs/common/unicode",
    "yql/udfs/common/url",
    "yql/udfs/common/compress",
])


def _get_fqdn():
    hostname = socket.gethostname()
    addrinfo = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, 0, 0, socket.AI_CANONNAME)
    for ai in addrinfo:
        canonname_candidate = ai[3]
        if canonname_candidate:
            return six.text_type(canonname_candidate)
    assert False, 'Failed to get FQDN'


# GRPC_SERVER:DEBUG,TICKET_PARSER:WARN,KQP_COMPILE_ACTOR:DEBUG
def _get_additional_log_configs():
    log_configs = os.getenv('YDB_ADDITIONAL_LOG_CONFIGS', '')
    rt = {}

    for c_and_l in log_configs.split(','):
        if c_and_l:
            c, log_level = c_and_l.split(':')
            rt[c] = LogLevels.from_string(log_level)
    return rt


def _get_grpc_host():
    if sys.platform == "darwin":
        return "localhost"
    return "[::]"


def _load_default_yaml(default_tablet_node_ids, ydb_domain_name, static_erasure, log_configs):
    data = read_binary(__name__, "resources/default_yaml.yml")
    if isinstance(data, bytes):
        data = data.decode('utf-8')
    data = data.format(
        ydb_result_rows_limit=os.getenv("YDB_KQP_RESULT_ROWS_LIMIT", 1000),
        ydb_yql_syntax_version=os.getenv("YDB_YQL_SYNTAX_VERSION", "1"),
        ydb_defaut_tablet_node_ids=str(default_tablet_node_ids),
        ydb_default_log_level=int(LogLevels.from_string(os.getenv("YDB_DEFAULT_LOG_LEVEL", "NOTICE"))),
        ydb_domain_name=ydb_domain_name,
        ydb_static_erasure=static_erasure,
        ydb_grpc_host=_get_grpc_host(),
        ydb_pq_topics_are_first_class_citizen=bool(os.getenv("YDB_PQ_TOPICS_ARE_FIRST_CLASS_CITIZEN", "true")),
        ydb_pq_cluster_table_path=str(os.getenv("YDB_PQ_CLUSTER_TABLE_PATH", "")),
        ydb_pq_version_table_path=str(os.getenv("YDB_PQ_VERSION_TABLE_PATH", "")),
        ydb_pq_root=str(os.getenv("YDB_PQ_ROOT", "")),
    )
    yaml_dict = yaml.safe_load(data)
    yaml_dict["log_config"]["entry"] = []
    for log, level in six.iteritems(log_configs):
        yaml_dict["log_config"]["entry"].append({"component": log, "level": int(level)})
    if os.getenv("YDB_ENABLE_COLUMN_TABLES", "") == "true":
        yaml_dict |= {"column_shard_config": {"disabled_on_scheme_shard": False}}
        yaml_dict["table_service_config"]["enable_htap_tx"] = True
        yaml_dict["table_service_config"]["enable_olap_sink"] = True
        yaml_dict["table_service_config"]["enable_create_table_as"] = True
    return yaml_dict


def _load_yaml_config(filename):
    with open(filename, "r") as f:
        return yaml.safe_load(f)


def _use_in_memory_pdisks_var(pdisk_store_path, use_in_memory_pdisks):
    if os.getenv('YDB_USE_IN_MEMORY_PDISKS') is not None:
        return os.getenv('YDB_USE_IN_MEMORY_PDISKS') == "true"

    if pdisk_store_path:
        return False

    return use_in_memory_pdisks


class KikimrConfigGenerator(object):
    def __init__(
            self,
            erasure=None,
            binary_paths=None,
            nodes=None,
            additional_log_configs=None,
            port_allocator=None,
            udfs_path=None,
            output_path=None,
            enable_pq=True,
            pq_client_service_types=None,
            pdisk_store_path=None,
            enable_sqs=False,
            domain_name='Root',
            suppress_version_check=True,
            static_pdisk_size=PDISK_SIZE,
            static_pdisk_config=None,
            dynamic_pdisk_size=PDISK_SIZE,
            dynamic_pdisks=[],
            dynamic_pdisks_config=None,
            dynamic_storage_pools=[dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0)],
            state_storage_rings=None,
            n_to_select=None,
            use_log_files=True,
            grpc_ssl_enable=False,
            use_in_memory_pdisks=True,
            enable_pqcd=False,
            enable_metering=False,
            enable_audit_log=False,
            audit_log_config=None,
            grpc_tls_data_path=None,
            fq_config_path=None,
            public_http_config_path=None,
            public_http_config=None,
            auth_config_path=None,
            enable_public_api_external_blobs=False,
            node_kind=None,
            bs_cache_file_path=None,
            yq_tenant=None,
            use_legacy_pq=False,
            dc_mapping={},
            enable_alter_database_create_hive_first=False,
            overrided_actor_system_config=None,
            default_users=None,  # dict[user]=password
            extra_feature_flags=None,     # list[str]
            disabled_feature_flags=None,  # list[str]
            extra_grpc_services=None,     # list[str]
            disabled_grpc_services=None,  # list[str]
            hive_config=None,
            datashard_config=None,
            enforce_user_token_requirement=False,
            default_user_sid=None,
            pg_compatible_expirement=False,
            generic_connector_config=None,  # typing.Optional[TGenericConnectorConfig]
            kafka_api_port=None,
            metadata_section=None,
            column_shard_config=None,
            use_config_store=False,
            separate_node_configs=False,
            default_clusteradmin=None,
            enable_resource_pools=None,
            scan_grouped_memory_limiter_config=None,
            comp_grouped_memory_limiter_config=None,
            deduplication_grouped_memory_limiter_config=None,
            query_service_config=None,
            domain_login_only=None,
            use_self_management=False,
            simple_config=False,
            breakpad_minidumps_path=None,
            breakpad_minidumps_script=None,
            explicit_hosts_and_host_configs=False,
            table_service_config=None,  # dict[name]=value
            bridge_config=None,
            memory_controller_config=None,
            verbose_memory_limit_exception=False,
            enable_static_auth=False,
            cms_config=None,
            explicit_statestorage_config=None,
            system_tablets=None,
            protected_mode=False,  # Authentication
            enable_pool_encryption=False,
            tiny_mode=False,
            module=None,
    ):
        if extra_feature_flags is None:
            extra_feature_flags = []
        if disabled_feature_flags is None:
            disabled_feature_flags = []
        if extra_grpc_services is None:
            extra_grpc_services = []
        if disabled_grpc_services is None:
            disabled_grpc_services = []

        self.explicit_statestorage_config = explicit_statestorage_config
        self.cms_config = cms_config
        self.use_log_files = use_log_files
        self.use_self_management = use_self_management
        self.simple_config = simple_config
        self.bridge_config = bridge_config
        self.suppress_version_check = suppress_version_check
        self.explicit_hosts_and_host_configs = explicit_hosts_and_host_configs
        if use_self_management:
            self.suppress_version_check = False
            self.explicit_hosts_and_host_configs = True
        self._pdisk_store_path = pdisk_store_path
        self.static_pdisk_size = static_pdisk_size
        self.static_pdisk_config = static_pdisk_config
        self.app_config = config_pb2.TAppConfig()
        self.port_allocator = KikimrPortManagerPortAllocator() if port_allocator is None else port_allocator
        erasure = Erasure.NONE if erasure is None else erasure
        self.system_tablets = system_tablets
        self.protected_mode = protected_mode
        self.enable_pool_encryption = enable_pool_encryption
        self.module = module
        self.__grpc_ssl_enable = grpc_ssl_enable or protected_mode
        self.__grpc_tls_data_path = None
        self.__grpc_tls_ca = None
        self.__grpc_tls_key = None
        self.__grpc_tls_cert = None
        self._pdisks_info = []
        if self.__grpc_ssl_enable:
            self.__grpc_tls_data_path = grpc_tls_data_path or yatest.common.output_path()
            cert_pem, key_pem = tls_tools.generate_selfsigned_cert(_get_fqdn())
            self.__grpc_tls_ca = cert_pem
            self.__grpc_tls_key = key_pem
            self.__grpc_tls_cert = cert_pem

        self.monitoring_tls_cert_path = None
        self.monitoring_tls_key_path = None
        self.monitoring_tls_ca_path = None

        self.__binary_paths = binary_paths
        rings_count = 3 if erasure == Erasure.MIRROR_3_DC else 1
        if nodes is None:
            nodes = rings_count * erasure.min_fail_domains
        self._rings_count = rings_count
        self.__node_ids = list(range(1, nodes + 1))
        self.n_to_select = n_to_select
        self.state_storage_rings = state_storage_rings
        self.__use_in_memory_pdisks = _use_in_memory_pdisks_var(pdisk_store_path, use_in_memory_pdisks)
        self.__pdisks_directory = os.getenv('YDB_PDISKS_DIRECTORY')
        self.static_erasure = erasure
        self.domain_name = domain_name
        self.__num_static_pdisks = 1
        if erasure == Erasure.MIRROR_3_DC and nodes == 3:
            self.__num_static_pdisks = 3

        self.__number_of_pdisks_per_node = self.__num_static_pdisks + len(dynamic_pdisks)
        self.__udfs_path = udfs_path
        self._dcs = [1]
        if erasure == Erasure.MIRROR_3_DC:
            self._dcs = [1, 2, 3]

        self.__additional_log_configs = {} if additional_log_configs is None else additional_log_configs
        self.__additional_log_configs.update(_get_additional_log_configs())
        if pg_compatible_expirement:
            self.__additional_log_configs.update({
                'PGWIRE': LogLevels.from_string('DEBUG'),
                'LOCAL_PGWIRE': LogLevels.from_string('DEBUG'),
            })

        self.dynamic_pdisk_size = dynamic_pdisk_size
        self.dynamic_storage_pools = dynamic_storage_pools
        self.dynamic_pdisks_config = dynamic_pdisks_config

        self.__dynamic_pdisks = dynamic_pdisks

        try:
            test_path = yatest.common.test_output_path()
        except Exception:
            test_path = os.path.abspath("kikimr_working_dir")

        self.__working_dir = output_path or test_path

        if not os.path.isdir(self.__working_dir):
            os.makedirs(self.__working_dir)

        self.node_kind = node_kind
        self.yq_tenant = yq_tenant
        self.dc_mapping = dc_mapping

        self.tiny_mode = tiny_mode

        self.__bs_cache_file_path = bs_cache_file_path

        self.yaml_config = _load_default_yaml(self.__node_ids, self.domain_name, self.static_erasure, self.__additional_log_configs)

        security_config_root = self.yaml_config["domains_config"]
        if self.use_self_management:
            self.yaml_config["self_management_config"] = dict()
            self.yaml_config["self_management_config"]["enabled"] = True

        if self.cms_config:
            self.yaml_config["cms_config"] = self.cms_config

        if overrided_actor_system_config:
            self.yaml_config["actor_system_config"] = overrided_actor_system_config

        if "table_service_config" not in self.yaml_config:
            self.yaml_config["table_service_config"] = {}

        if verbose_memory_limit_exception:
            self.yaml_config["table_service_config"]["resource_manager"]["verbose_memory_limit_exception"] = True

        if table_service_config:
            self.yaml_config["table_service_config"].update(table_service_config)

        if os.getenv('YDB_KQP_ENABLE_IMMEDIATE_EFFECTS', 'false').lower() == 'true':
            self.yaml_config["table_service_config"]["enable_kqp_immediate_effects"] = True

        # disable kqp pattern cache on darwin platform to avoid using llvm versions of computational
        # nodes. These compute nodes are not properly tested and maintained on darwin platform.
        if sys.platform == "darwin":
            self.yaml_config["table_service_config"]["resource_manager"]["kqp_pattern_cache_compiled_capacity_bytes"] = 0

        if os.getenv('PGWIRE_LISTENING_PORT', ''):
            self.yaml_config["local_pg_wire_config"] = {}
            self.yaml_config["local_pg_wire_config"]["listening_port"] = os.getenv('PGWIRE_LISTENING_PORT')

        # dirty hack for internal ydbd flavour
        if "cert" in self.get_binary_path(0):
            # Hardcoded feature flags. Should be hardcoded in binary itself
            self.yaml_config["feature_flags"]["enable_strict_acl_check"] = True
            self.yaml_config["feature_flags"]["enable_strict_user_management"] = True
            self.yaml_config["feature_flags"]["enable_database_admin"] = True
            self.yaml_config["feature_flags"]["database_yaml_config_allowed"] = True
            self.yaml_config["feature_flags"]["enable_resource_pools"] = False
            self.yaml_config["feature_flags"]["check_database_access_permission"] = True

        self.yaml_config["feature_flags"]["enable_public_api_external_blobs"] = enable_public_api_external_blobs

        # for faster shutdown: there is no reason to wait while tablets are drained before whole cluster is stopping
        self.yaml_config["feature_flags"]["enable_drain_on_shutdown"] = False
        if enable_resource_pools is not None:
            self.yaml_config["feature_flags"]["enable_resource_pools"] = enable_resource_pools
        for extra_feature_flag in extra_feature_flags:
            self.yaml_config["feature_flags"][extra_feature_flag] = True
        for disabled_feature_flag in disabled_feature_flags:
            self.yaml_config["feature_flags"][disabled_feature_flag] = False
        if enable_alter_database_create_hive_first:
            self.yaml_config["feature_flags"]["enable_alter_database_create_hive_first"] = enable_alter_database_create_hive_first
        self.yaml_config['pqconfig']['enabled'] = enable_pq
        self.yaml_config['pqconfig']['enable_proto_source_id_info'] = True
        self.yaml_config['pqconfig']['max_storage_node_port'] = 65535
        # NOTE(shmel1k@): KIKIMR-14221
        if use_legacy_pq:
            self.yaml_config['pqconfig']['topics_are_first_class_citizen'] = False
            self.yaml_config['pqconfig']['cluster_table_path'] = '/Root/PQ/Config/V2/Cluster'
            self.yaml_config['pqconfig']['version_table_path'] = '/Root/PQ/Config/V2/Versions'
            self.yaml_config['pqconfig']['check_acl'] = False
            self.yaml_config['pqconfig']['require_credentials_in_new_protocol'] = False
            self.yaml_config['pqconfig']['root'] = '/Root/PQ'
            self.yaml_config['pqconfig']['quoting_config']['enable_quoting'] = False
        if pq_client_service_types:
            self.yaml_config['pqconfig']['client_service_type'] = []
            for service_type in pq_client_service_types:
                self.yaml_config['pqconfig']['client_service_type'].append({'name': service_type})

        self.yaml_config['grpc_config']['services'] = [
            item for item in (self.yaml_config['grpc_config']['services'] + extra_grpc_services)
            if item not in disabled_grpc_services
        ]

        # NOTE(shmel1k@): change to 'true' after migration to YDS scheme
        self.yaml_config['sqs_config']['enable_sqs'] = enable_sqs
        self.yaml_config['pqcluster_discovery_config']['enabled'] = enable_pqcd
        self.yaml_config["net_classifier_config"]["net_data_file_path"] = os.path.join(
            self.__working_dir,
            'netData.tsv',
        )
        with open(self.yaml_config["net_classifier_config"]["net_data_file_path"], "w") as net_data_file:
            net_data_file.write("")

        if enable_metering:
            self.__set_enable_metering()

        if enable_audit_log:
            self.__set_audit_log(audit_log_config)

        self.naming_config = config_pb2.TAppConfig()
        dc_it = itertools.cycle(self._dcs)
        rack_it = itertools.count(start=1)
        body_it = itertools.count(start=1)
        self.yaml_config["nameservice_config"] = {"node": []}
        self.breakpad_minidumps_path = breakpad_minidumps_path
        self.breakpad_minidumps_script = breakpad_minidumps_script
        for node_id in self.__node_ids:
            dc, rack, body = next(dc_it), next(rack_it), next(body_it)
            ic_port = self.port_allocator.get_node_port_allocator(node_id).ic_port
            node = self.naming_config.NameserviceConfig.Node.add(
                NodeId=node_id,
                Address="::1",
                Port=ic_port,
                Host="localhost",
            )

            node.WalleLocation.DataCenter = str(dc)
            node.WalleLocation.Rack = str(rack)
            node.WalleLocation.Body = body
            self.yaml_config["nameservice_config"]["node"].append(
                dict(
                    node_id=node_id,
                    address="::1", port=ic_port,
                    host='localhost',
                    walle_location=dict(
                        data_center=str(dc),
                        rack=str(rack),
                        body=body,
                    )
                )
            )

        if auth_config_path:
            self.yaml_config["auth_config"] = _load_yaml_config(auth_config_path)

        if domain_login_only is not None:
            if "auth_config" not in self.yaml_config:
                self.yaml_config["auth_config"] = dict()
            self.yaml_config["auth_config"]["domain_login_only"] = domain_login_only

        if fq_config_path:
            self.yaml_config["federated_query_config"] = _load_yaml_config(fq_config_path)

        if public_http_config:
            self.yaml_config["public_http_config"] = public_http_config
        elif public_http_config_path:
            self.yaml_config["public_http_config"] = _load_yaml_config(public_http_config_path)

        if hive_config:
            self.yaml_config["hive_config"] = hive_config

        if datashard_config:
            self.yaml_config["data_shard_config"] = datashard_config

        if column_shard_config:
            self.yaml_config["column_shard_config"] = column_shard_config

        if query_service_config:
            self.yaml_config["query_service_config"] = query_service_config

        if scan_grouped_memory_limiter_config:
            self.yaml_config["scan_grouped_memory_limiter_config"] = scan_grouped_memory_limiter_config
        if comp_grouped_memory_limiter_config:
            self.yaml_config["comp_grouped_memory_limiter_config"] = comp_grouped_memory_limiter_config
        if deduplication_grouped_memory_limiter_config:
            self.yaml_config["deduplication_grouped_memory_limiter_config"] = deduplication_grouped_memory_limiter_config

        self.__build()
        if self.grpc_ssl_enable:
            self.yaml_config["grpc_config"]["ca"] = self.grpc_tls_ca_path
            self.yaml_config["grpc_config"]["cert"] = self.grpc_tls_cert_path
            self.yaml_config["grpc_config"]["key"] = self.grpc_tls_key_path

        if default_users is not None:
            # check for None for remove default users for empty dict
            if "security_config" not in security_config_root:
                security_config_root["security_config"] = dict()

            # remove existed default users
            security_config_root["security_config"]["default_users"] = []

            for user, password in default_users.items():
                security_config_root["security_config"]["default_users"].append({
                    "name": user,
                    "password": password,
                })

        if os.getenv("YDB_ALLOW_ORIGIN") is not None:
            self.yaml_config["monitoring_config"] = {"allow_origin": str(os.getenv("YDB_ALLOW_ORIGIN"))}

        if enforce_user_token_requirement or protected_mode:
            security_config_root["security_config"]["enforce_user_token_requirement"] = True

        if default_user_sid:
            security_config_root["security_config"]["default_user_sids"] = [default_user_sid]

        # protected mode is described in the YDB documentation for cluster deployment: it uses both certificate and token authentication.
        # see https://ydb.tech/docs/en/devops/deployment-options/manual/initial-deployment?version=main
        if protected_mode:
            security_config = security_config_root.setdefault("security_config", {})
            if "default_users" in security_config:
                del security_config["default_users"]

            base_sids = ["root", "root@builtin", "ADMINS", "DATABASE-ADMINS", "clusteradmins@cert"]
            security_config["monitoring_allowed_sids"] = base_sids
            security_config["viewer_allowed_sids"] = base_sids
            security_config["bootstrap_allowed_sids"] = base_sids
            security_config["administration_allowed_sids"] = base_sids

            self.yaml_config["interconnect_config"] = {
                "start_tcp": True,
                "encryption_mode": "OPTIONAL",
                "path_to_certificate_file": self.grpc_tls_cert_path,
                "path_to_private_key_file": self.grpc_tls_key_path,
                "path_to_ca_file": self.grpc_tls_ca_path,
            }

            self.yaml_config['grpc_config']['services_enabled'] = ['legacy']
            if 'services' in self.yaml_config['grpc_config']:
                del self.yaml_config['grpc_config']['services']

            self.yaml_config['client_certificate_authorization'] = {
                "request_client_certificate": True,
                "client_certificate_definitions": [
                    {
                        "member_groups": ["clusteradmins@cert"],
                        "subject_terms": [
                            {
                                "short_name": "O",
                                "values": ["YDB"]
                            }
                        ]
                    }
                ]
            }

        if memory_controller_config:
            self.yaml_config["memory_controller_config"] = memory_controller_config

        if os.getenv("YDB_HARD_MEMORY_LIMIT_BYTES"):
            if "memory_controller_config" not in self.yaml_config:
                self.yaml_config["memory_controller_config"] = {}
            self.yaml_config["memory_controller_config"]["hard_limit_bytes"] = int(os.getenv("YDB_HARD_MEMORY_LIMIT_BYTES"))

        if os.getenv("YDB_CHANNEL_BUFFER_SIZE"):
            self.yaml_config["table_service_config"]["resource_manager"]["channel_buffer_size"] = int(os.getenv("YDB_CHANNEL_BUFFER_SIZE"))

        if pg_compatible_expirement:
            self.yaml_config["table_service_config"]["enable_ast_cache"] = True
            self.yaml_config["feature_flags"]['enable_temp_tables'] = True
            self.yaml_config["feature_flags"]['enable_table_pg_types'] = True
            self.yaml_config['feature_flags']['enable_pg_syntax'] = True
            self.yaml_config['feature_flags']['enable_uniq_constraint'] = True
            if "local_pg_wire_config" not in self.yaml_config:
                self.yaml_config["local_pg_wire_config"] = {}

            ydb_pgwire_port = self.port_allocator.get_node_port_allocator(node_id).pgwire_port
            self.yaml_config['local_pg_wire_config']['listening_port'] = ydb_pgwire_port

            # https://github.com/ydb-platform/ydb/issues/5152
            # self.yaml_config["table_service_config"]["enable_pg_consts_to_params"] = True

        if generic_connector_config:
            if "query_service_config" not in self.yaml_config:
                self.yaml_config["query_service_config"] = {}

            self.yaml_config["query_service_config"]["generic"] = {
                "connector": {
                    "endpoint": {
                        "host": generic_connector_config.Endpoint.host,
                        "port": generic_connector_config.Endpoint.port,
                    },
                    "use_ssl": generic_connector_config.UseSsl
                },
                "default_settings": [
                    {
                        "name": "DateTimeFormat",
                        "value": "string"
                    },
                    {
                        "name": "UsePredicatePushdown",
                        "value": "true"
                    }
                ]
            }

            self.yaml_config["feature_flags"]["enable_external_data_sources"] = True
            self.yaml_config["feature_flags"]["enable_script_execution_operations"] = True

        if kafka_api_port is not None:
            kafka_proxy_config = dict()
            kafka_proxy_config['enable_kafka_proxy'] = True
            kafka_proxy_config["listening_port"] = kafka_api_port

            self.yaml_config["kafka_proxy_config"] = kafka_proxy_config

        if bridge_config is not None:
            self.yaml_config["bridge_config"] = bridge_config

        self.full_config = dict()
        if self.explicit_hosts_and_host_configs:
            self._add_host_config_and_hosts()
            self.yaml_config.pop("nameservice_config")
        if self.use_self_management:

            if "security_config" in self.yaml_config["domains_config"]:
                self.yaml_config["security_config"] = self.yaml_config["domains_config"].pop("security_config")

            if "services" in self.yaml_config["grpc_config"] and "config" not in self.yaml_config["grpc_config"]["services"]:
                self.yaml_config["grpc_config"]["services"].append("config")

            self.yaml_config["default_disk_type"] = "ROT"

            if self.static_erasure == Erasure.MIRROR_3_DC and len(self.__node_ids) == 3:
                self.yaml_config["fail_domain_type"] = "disk"
            else:
                self.yaml_config["fail_domain_type"] = "rack"

            self.yaml_config["erasure"] = self.yaml_config.pop("static_erasure")

            for name in ['blob_storage_config', 'domains_config', 'system_tablets',
                         'channel_profile_config']:
                del self.yaml_config[name]
        if self.simple_config:
            self.yaml_config.pop("feature_flags")
            self.yaml_config.pop("federated_query_config")
            self.yaml_config.pop("pqconfig")
            self.yaml_config.pop("pqcluster_discovery_config")
            self.yaml_config.pop("net_classifier_config")
            self.yaml_config.pop("sqs_config")
            self.yaml_config.pop("table_service_config")
            self.yaml_config.pop("kqpconfig")

        if self.explicit_statestorage_config:
            if "domains_config" not in self.yaml_config:
                self.yaml_config["domains_config"] = dict()
            if "state_storage" in self.yaml_config["domains_config"]:
                del self.yaml_config["domains_config"]["state_storage"]
            self.yaml_config["domains_config"]["explicit_state_storage_config"] = self.explicit_statestorage_config["explicit_state_storage_config"]
            self.yaml_config["domains_config"]["explicit_state_storage_board_config"] = self.explicit_statestorage_config["explicit_state_storage_board_config"]
            self.yaml_config["domains_config"]["explicit_scheme_board_config"] = self.explicit_statestorage_config["explicit_scheme_board_config"]

        if self.system_tablets:
            self.yaml_config["system_tablets"] = self.system_tablets

        if metadata_section:
            self.full_config["metadata"] = metadata_section
            self.full_config["config"] = self.yaml_config
        elif self.use_self_management:
            self.full_config["metadata"] = {
                "kind": "MainConfig",
                "version": 0,
                "cluster": "",
            }
            self.full_config["config"] = self.yaml_config
        else:
            self.full_config = self.yaml_config

        self.use_config_store = use_config_store
        self.separate_node_configs = separate_node_configs

        self.__default_clusteradmin = default_clusteradmin
        if self.__default_clusteradmin is not None:
            security_config = self.yaml_config["domains_config"]["security_config"]
            security_config.setdefault("administration_allowed_sids", []).append(self.__default_clusteradmin)
            security_config.setdefault("default_access", []).append('+F:{}'.format(self.__default_clusteradmin))
        self.__enable_static_auth = enable_static_auth

    @property
    def enable_static_auth(self):
        return self.__enable_static_auth

    @property
    def default_clusteradmin(self):
        return self.__default_clusteradmin

    @property
    def pdisks_info(self):
        return self._pdisks_info

    @property
    def grpc_tls_data_path(self):
        return self.__grpc_tls_data_path

    @property
    def grpc_tls_key_path(self):
        return os.path.join(self.grpc_tls_data_path, 'key.pem')

    @property
    def grpc_tls_cert_path(self):
        return os.path.join(self.grpc_tls_data_path, 'cert.pem')

    @property
    def grpc_tls_ca_path(self):
        return os.path.join(self.grpc_tls_data_path, 'ca.pem')

    @property
    def grpc_ssl_enable(self):
        return self.__grpc_ssl_enable

    @property
    def grpc_tls_cert(self):
        return self.__grpc_tls_cert

    @property
    def grpc_tls_key(self):
        return self.__grpc_tls_key

    @property
    def grpc_tls_ca(self):
        return self.__grpc_tls_ca

    @property
    def domains_txt(self):
        app_config = config_pb2.TAppConfig()
        assert not self.enable_pool_encryption, "pool encryption is not addressed in domains.txt"
        Parse(read_binary(__name__, "resources/default_domains.txt"), app_config.DomainsConfig)
        return app_config.DomainsConfig

    @property
    def names_txt(self):
        return self.naming_config.NameserviceConfig

    def __set_enable_metering(self):
        metering_file_path = os.path.join(self.__working_dir, 'metering.txt')
        with open(metering_file_path, "w") as metering_file:
            metering_file.write('')
        self.yaml_config['metering_config'] = {'metering_file_path': metering_file_path}

    def __set_audit_log(self, audit_log_config):
        if audit_log_config is None:
            cfg = dict(file_backend=dict())
        else:
            cfg = audit_log_config.copy()
        file_backend_cfg = cfg.get('file_backend')

        # Generate path for audit file
        if file_backend_cfg is not None:
            file_path = file_backend_cfg.get('file_path')
            if file_path is None:
                audit_file = tempfile.NamedTemporaryFile('w', prefix="audit_log.", suffix=".txt",
                                                         dir=self.__working_dir)
                file_backend_cfg['file_path'] = audit_file.name
                with audit_file:
                    audit_file.write('')
        self.yaml_config['audit_config'] = cfg

    @property
    def metering_file_path(self):
        return self.yaml_config.get('metering_config', {}).get('metering_file_path')

    @property
    def audit_file_path(self):
        return self.yaml_config.get('audit_config', {}).get('file_backend', {}).get('file_path')

    @property
    def sqs_service_enabled(self):
        return self.yaml_config['sqs_config']['enable_sqs']

    @property
    def working_dir(self):
        return self.__working_dir

    def get_binary_path(self, node_id):
        binary_paths = self.__binary_paths
        if not binary_paths:
            binary_paths = [kikimr_driver_path()]
        return binary_paths[node_id % len(binary_paths)]

    def get_ydb_cli_path(self):
        return ydb_cli_path()

    def get_binary_paths(self):
        binary_paths = self.__binary_paths
        if not binary_paths:
            binary_paths = [kikimr_driver_path()]
        return binary_paths

    def set_binary_paths(self, binary_paths):
        self.__binary_paths = binary_paths

    def write_tls_data(self):
        if self.__grpc_ssl_enable:
            for fpath, data in (
                (self.grpc_tls_ca_path, self.grpc_tls_ca), (self.grpc_tls_cert_path, self.grpc_tls_cert),
                (self.grpc_tls_key_path, self.grpc_tls_key)
            ):
                with open(fpath, 'wb') as f:
                    f.write(data)

    def write_proto_configs(self, configs_path):
        self.write_tls_data()
        with open(os.path.join(configs_path, "config.yaml"), "w") as writer:
            writer.write(yaml.safe_dump(self.full_config))

    def clone_grpc_as_ext_endpoint(self, port, endpoint_id=None):
        cur_grpc_config = copy.deepcopy(self.yaml_config['grpc_config'])
        if 'ext_endpoints' in cur_grpc_config:
            del cur_grpc_config['ext_endpoints']

        cur_grpc_config['port'] = port

        if endpoint_id is not None:
            cur_grpc_config['endpoint_id'] = endpoint_id

        if 'ext_endpoints' not in self.yaml_config['grpc_config']:
            self.yaml_config['grpc_config']['ext_endpoints'] = []

        self.yaml_config['grpc_config']['ext_endpoints'].append(cur_grpc_config)

    def get_yql_udfs_to_load(self):
        if self.__udfs_path is None:
            return []
        udfs_path = self.__udfs_path
        result = []
        for dirpath, dnames, fnames in os.walk(udfs_path):
            is_loaded = False
            for already_loaded_static_udf in KNOWN_STATIC_YQL_UDFS:
                dirpath = dirpath.rstrip("/")

                if dirpath.endswith(already_loaded_static_udf):
                    is_loaded = True

            if is_loaded:
                continue

            for f in fnames:
                if f.endswith(".so"):
                    full = os.path.join(dirpath, f)
                    result.append(full)
                elif f.endswith('.dylib'):
                    full = os.path.join(dirpath, f)
                    result.append(full)
        return result

    def all_node_ids(self):
        return self.__node_ids

    def _add_state_storage_config(self):
        if self.use_self_management and self.n_to_select is None and self.state_storage_rings is None:
            return
        if self.n_to_select is None:
            if self.static_erasure == Erasure.MIRROR_3_DC:
                if len(self.__node_ids) >= 9:
                    self.n_to_select = 9
                else:
                    self.n_to_select = len(self.__node_ids)
            else:
                self.n_to_select = min(5, len(self.__node_ids))
        if self.state_storage_rings is None:
            self.state_storage_rings = copy.deepcopy(self.__node_ids[: 9 if self.static_erasure == Erasure.MIRROR_3_DC else 8])
        self.yaml_config["domains_config"]["state_storage"] = []
        self.yaml_config["domains_config"]["state_storage"].append({"ssid" : 1, "ring" : {"nto_select" : self.n_to_select, "ring" : []}})
        for ring in self.state_storage_rings:
            self.yaml_config["domains_config"]["state_storage"][0]["ring"]["ring"].append({"node" : ring if isinstance(ring, list) else [ring], "use_ring_specific_node_selection" : True})

    def _add_pdisk_to_static_group(self, pdisk_id, path, node_id, pdisk_category, ring, pdisk_config=None):
        domain_id = len(
            self.yaml_config['blob_storage_config']["service_set"]["groups"][0]["rings"][ring]["fail_domains"])
        pdisk_entry = {
            "node_id": node_id,
            "pdisk_id": pdisk_id,
            "path": path,
            "pdisk_guid": pdisk_id,
            "pdisk_category": pdisk_category
        }
        if pdisk_config:
            pdisk_entry["pdisk_config"] = pdisk_config
        self.yaml_config['blob_storage_config']["service_set"]["pdisks"].append(pdisk_entry)
        self.yaml_config['blob_storage_config']["service_set"]["vdisks"].append(
            {
                "vdisk_id": {"group_id": 0, "group_generation": 1, "ring": ring, "domain": domain_id, "vdisk": 0},
                "vdisk_location": {"node_id": node_id, "pdisk_id": pdisk_id, "pdisk_guid": pdisk_id,
                                   'vdisk_slot_id': 0},
            }
        )
        self.yaml_config['blob_storage_config']["service_set"]["groups"][0]["rings"][ring]["fail_domains"].append(
            {"vdisk_locations": [
                {"node_id": node_id, "pdisk_id": pdisk_id, "pdisk_guid": pdisk_id, 'vdisk_slot_id': 0}]}
        )

    def _initialize_pdisks_info(self):
        datacenter_id_generator = itertools.cycle(self._dcs)
        for node_id in self.__node_ids:
            datacenter_id = next(datacenter_id_generator)

            for pdisk_id in range(1, self.__number_of_pdisks_per_node + 1):
                is_static_pdisk = pdisk_id <= self.__num_static_pdisks
                if is_static_pdisk:
                    disk_size = self.static_pdisk_size
                    pdisk_user_kind = 0
                    pdisk_config = self.static_pdisk_config
                else:
                    dynamic_pdisk_idx = pdisk_id - 1 - self.__num_static_pdisks
                    disk_size = self.__dynamic_pdisks[dynamic_pdisk_idx].get('disk_size', self.dynamic_pdisk_size)
                    pdisk_user_kind = self.__dynamic_pdisks[dynamic_pdisk_idx].get('user_kind', 0)
                    pdisk_config = self.dynamic_pdisks_config

                if self.__use_in_memory_pdisks:
                    pdisk_size_gb = disk_size / (1024 * 1024 * 1024)
                    pdisk_path = "SectorMap:%d:%d" % (pdisk_id, pdisk_size_gb)
                elif self.__pdisks_directory:
                    pdisk_path = os.path.join(self.__pdisks_directory, str(pdisk_id))
                else:
                    tmp_file = tempfile.NamedTemporaryFile(prefix="pdisk{}".format(pdisk_id), suffix=".data",
                                                           dir=self._pdisk_store_path)
                    pdisk_path = tmp_file.name

                pdisk_info = {
                    'pdisk_path': pdisk_path,
                    'node_id': node_id,
                    'disk_size': disk_size,
                    'pdisk_user_kind': pdisk_user_kind,
                    'pdisk_id': pdisk_id
                }
                if pdisk_config:
                    pdisk_info['pdisk_config'] = pdisk_config

                self._pdisks_info.append(pdisk_info)
                if not self.use_self_management and is_static_pdisk and node_id <= self.static_erasure.min_fail_domains * self._rings_count:
                    self._add_pdisk_to_static_group(
                        pdisk_id,
                        pdisk_path,
                        node_id,
                        pdisk_user_kind,
                        datacenter_id - 1,
                        pdisk_config=pdisk_config,
                    )

    def _add_host_config_and_hosts(self):
        self._initialize_pdisks_info()
        host_configs = []
        hosts = []
        host_config_id_counter = itertools.count(1)

        for node_id in self.__node_ids:
            host_config_id = next(host_config_id_counter)
            drive = []
            for pdisk_info in self._pdisks_info:
                if pdisk_info['node_id'] == node_id:
                    drive.append(
                        {
                            "path": pdisk_info['pdisk_path'],
                            "type": pdisk_info.get('pdisk_type', 'ROT').upper(),
                        }
                    )

            host_configs.append(
                {
                    "host_config_id": host_config_id,
                    "drive": drive,
                }
            )
            host_dict = {
                "host": "localhost",
                "port": self.port_allocator.get_node_port_allocator(node_id).ic_port,
                "host_config_id": host_config_id,
            }
            if self.bridge_config:
                host_dict["location"] = {"bridge_pile_name": self.bridge_config.get("piles", [])[(node_id - 1) % len(self.bridge_config.get("piles", []))].get("name")}
            elif self.static_erasure == Erasure.MIRROR_3_DC:
                host_dict["location"] = {"data_center": "zone-%d" % (node_id % 3)}
            hosts.append(host_dict)

        self.yaml_config["host_configs"] = host_configs
        self.yaml_config["hosts"] = hosts

    def __build(self):
        self.yaml_config["blob_storage_config"] = {}
        if self.__bs_cache_file_path:
            self.yaml_config["blob_storage_config"]["cache_file_path"] = \
                self.__bs_cache_file_path
        self.yaml_config["blob_storage_config"]["service_set"] = {}
        self.yaml_config["blob_storage_config"]["service_set"]["availability_domains"] = 1
        self.yaml_config["blob_storage_config"]["service_set"]["pdisks"] = []
        self.yaml_config["blob_storage_config"]["service_set"]["vdisks"] = []
        self.yaml_config["blob_storage_config"]["service_set"]["groups"] = [
            {"group_id": 0, 'group_generation': 1, 'erasure_species': int(self.static_erasure)}]
        self.yaml_config["blob_storage_config"]["service_set"]["groups"][0]["rings"] = []

        for dc in self._dcs:
            self.yaml_config["blob_storage_config"]["service_set"]["groups"][0]["rings"].append({"fail_domains": []})

        self._add_state_storage_config()
        if not self.use_self_management and not self.explicit_hosts_and_host_configs:
            self._initialize_pdisks_info()

        if self.enable_pool_encryption:
            for domain in self.yaml_config['domains_config']['domain']:
                for pool_type in domain['storage_pool_types']:
                    pool_type['pool_config']['encryption_mode'] = 1

        if self.static_erasure == Erasure.MIRROR_3_DC and len(self.__node_ids) == 3:
            for domain in self.yaml_config['domains_config']['domain']:
                for pool_type in domain['storage_pool_types']:
                    pool_type['pool_config']['geometry'] = {
                        "realm_level_begin": int(FailDomainType.DC),
                        "realm_level_end": int(FailDomainType.Room),
                        "domain_level_begin": int(FailDomainType.DC),
                        "domain_level_end": int(FailDomainType.Disk) + 1
                    }
