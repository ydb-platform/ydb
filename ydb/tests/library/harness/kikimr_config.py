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

import ydb.tests.library.common.yatest_common as yatest_common
from ydb.core.protos import config_pb2
from ydb.tests.library.common.types import Erasure

from . import tls_tools
from .kikimr_port_allocator import KikimrPortManagerPortAllocator
from .param_constants import kikimr_driver_path
from .util import LogLevels

PDISK_SIZE_STR = os.getenv("YDB_PDISK_SIZE", str(64 * 1024 * 1024 * 1024))
if PDISK_SIZE_STR.endswith("GB"):
    PDISK_SIZE = int(PDISK_SIZE_STR[:-2]) * 1024 * 1024 * 1024
else:
    PDISK_SIZE = int(PDISK_SIZE_STR)

KNOWN_STATIC_YQL_UDFS = set([
    "yql/udfs/common/unicode",
    "yql/udfs/common/url",
    "yql/udfs/common/compress",
])


def get_fqdn():
    hostname = socket.gethostname()
    addrinfo = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, 0, 0, socket.AI_CANONNAME)
    for ai in addrinfo:
        canonname_candidate = ai[3]
        if canonname_candidate:
            return six.text_type(canonname_candidate)
    assert False, 'Failed to get FQDN'


# GRPC_SERVER:DEBUG,TICKET_PARSER:WARN,KQP_COMPILE_ACTOR:DEBUG
def get_additional_log_configs():
    log_configs = os.getenv('YDB_ADDITIONAL_LOG_CONFIGS', '')
    rt = {}

    for c_and_l in log_configs.split(','):
        if c_and_l:
            c, log_level = c_and_l.split(':')
            rt[c] = LogLevels.from_string(log_level)
    return rt


def get_grpc_host():
    if sys.platform == "darwin":
        return "localhost"
    return "[::]"


def load_default_yaml(default_tablet_node_ids, ydb_domain_name, static_erasure, log_configs):
    data = read_binary(__name__, "resources/default_yaml.yml")
    if isinstance(data, bytes):
        data = data.decode('utf-8')
    data = data.format(
        ydb_result_rows_limit=os.getenv("YDB_KQP_RESULT_ROWS_LIMIT", 1000),
        ydb_yql_syntax_version=os.getenv("YDB_YQL_SYNTAX_VERSION", "1"),
        ydb_force_new_engine=os.getenv("YDB_KQP_FORCE_NEW_ENGINE", "true"),
        ydb_defaut_tablet_node_ids=str(default_tablet_node_ids),
        ydb_default_log_level=int(LogLevels.from_string(os.getenv("YDB_DEFAULT_LOG_LEVEL", "NOTICE"))),
        ydb_domain_name=ydb_domain_name,
        ydb_static_erasure=static_erasure,
        ydb_grpc_host=get_grpc_host(),
        ydb_pq_topics_are_first_class_citizen=bool(os.getenv("YDB_PQ_TOPICS_ARE_FIRST_CLASS_CITIZEN", "true")),
        ydb_pq_cluster_table_path=str(os.getenv("YDB_PQ_CLUSTER_TABLE_PATH", "")),
        ydb_pq_version_table_path=str(os.getenv("YDB_PQ_VERSION_TABLE_PATH", "")),
        ydb_pq_root=str(os.getenv("YDB_PQ_ROOT", "")),
    )
    yaml_dict = yaml.safe_load(data)
    yaml_dict["log_config"]["entry"] = []
    for log, level in six.iteritems(log_configs):
        yaml_dict["log_config"]["entry"].append({"component": log, "level": int(level)})
    return yaml_dict


def _read_file(filename):
    with open(filename, "r") as f:
        return f.read()


def _load_yaml_config(filename):
    return yaml.safe_load(_read_file(filename))


def use_in_memory_pdisks_var(pdisk_store_path, use_in_memory_pdisks):
    if os.getenv('YDB_USE_IN_MEMORY_PDISKS') is not None:
        return os.getenv('YDB_USE_IN_MEMORY_PDISKS') == "true"

    if pdisk_store_path:
        return False

    return use_in_memory_pdisks


class KikimrConfigGenerator(object):
    def __init__(
            self,
            erasure=None,
            binary_path=None,
            nodes=None,
            additional_log_configs=None,
            port_allocator=None,
            has_cluster_uuid=True,
            load_udfs=False,
            udfs_path=None,
            output_path=None,
            enable_pq=True,
            pq_client_service_types=None,
            slot_count=0,
            pdisk_store_path=None,
            version=None,
            enable_nbs=False,
            enable_sqs=False,
            domain_name='Root',
            suppress_version_check=True,
            static_pdisk_size=PDISK_SIZE,
            dynamic_pdisk_size=PDISK_SIZE,
            dynamic_pdisks=[],
            dynamic_storage_pools=[dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0)],
            state_storage_rings=None,
            n_to_select=None,
            use_log_files=True,
            grpc_ssl_enable=False,
            use_in_memory_pdisks=True,
            enable_pqcd=True,
            enable_metering=False,
            enable_audit_log=False,
            grpc_tls_data_path=None,
            fq_config_path=None,
            public_http_config_path=None,
            public_http_config=None,
            enable_datastreams=False,
            auth_config_path=None,
            enable_public_api_external_blobs=False,
            node_kind=None,
            bs_cache_file_path=None,
            yq_tenant=None,
            use_legacy_pq=False,
            dc_mapping={},
            enable_alter_database_create_hive_first=False,
            disable_iterator_reads=False,
            disable_iterator_lookups=False,
            overrided_actor_system_config=None,
            default_users=None,  # dict[user]=password
            extra_feature_flags=None,  # list[str]
            extra_grpc_services=None,  # list[str]
            hive_config=None,
            datashard_config=None,
            enforce_user_token_requirement=False,
            default_user_sid=None,
            pg_compatible_expirement=False,
            generic_connector_config=None,  # typing.Optional[TGenericConnectorConfig]
            pgwire_port=None,
    ):
        if extra_feature_flags is None:
            extra_feature_flags = []
        if extra_grpc_services is None:
            extra_grpc_services = []

        self._version = version
        self.use_log_files = use_log_files
        self.suppress_version_check = suppress_version_check
        self._pdisk_store_path = pdisk_store_path
        self.static_pdisk_size = static_pdisk_size
        self.app_config = config_pb2.TAppConfig()
        self.port_allocator = KikimrPortManagerPortAllocator() if port_allocator is None else port_allocator
        erasure = Erasure.NONE if erasure is None else erasure
        self.__grpc_ssl_enable = grpc_ssl_enable
        self.__grpc_tls_data_path = None
        self.__grpc_ca_file = None
        self.__grpc_cert_file = None
        self.__grpc_key_file = None
        self.__grpc_tls_ca = None
        self.__grpc_tls_key = None
        self.__grpc_tls_cert = None
        self._pdisks_info = []
        if self.__grpc_ssl_enable:
            self.__grpc_tls_data_path = grpc_tls_data_path or yatest_common.output_path()
            cert_pem, key_pem = tls_tools.generate_selfsigned_cert(get_fqdn())
            self.__grpc_tls_ca = cert_pem
            self.__grpc_tls_key = key_pem
            self.__grpc_tls_cert = cert_pem

        self.__binary_path = binary_path
        rings_count = 3 if erasure == Erasure.MIRROR_3_DC else 1
        if nodes is None:
            nodes = rings_count * erasure.min_fail_domains
        self._rings_count = rings_count
        self._enable_nbs = enable_nbs
        self.__node_ids = list(range(1, nodes + 1))
        self.n_to_select = n_to_select
        if self.n_to_select is None:
            if erasure == Erasure.MIRROR_3_DC:
                self.n_to_select = 9
            else:
                self.n_to_select = min(5, nodes)
        self.state_storage_rings = state_storage_rings
        if self.state_storage_rings is None:
            self.state_storage_rings = copy.deepcopy(self.__node_ids[: 9 if erasure == Erasure.MIRROR_3_DC else 8])
        self.__use_in_memory_pdisks = use_in_memory_pdisks_var(pdisk_store_path, use_in_memory_pdisks)
        self.__pdisks_directory = os.getenv('YDB_PDISKS_DIRECTORY')
        self.static_erasure = erasure
        self.domain_name = domain_name
        self.__number_of_pdisks_per_node = 1 + len(dynamic_pdisks)
        self.__load_udfs = load_udfs
        self.__udfs_path = udfs_path
        self.__slot_count = slot_count
        self._dcs = [1]
        if erasure == Erasure.MIRROR_3_DC:
            self._dcs = [1, 2, 3]

        self.__additional_log_configs = {} if additional_log_configs is None else additional_log_configs
        self.__additional_log_configs.update(get_additional_log_configs())
        if pg_compatible_expirement:
            self.__additional_log_configs.update({
                'PGWIRE': LogLevels.from_string('DEBUG'),
                'LOCAL_PGWIRE': LogLevels.from_string('DEBUG'),
            })

        self.dynamic_pdisk_size = dynamic_pdisk_size
        self.dynamic_storage_pools = dynamic_storage_pools

        self.__dynamic_pdisks = dynamic_pdisks

        self.__output_path = output_path or yatest_common.output_path()
        self.node_kind = node_kind
        self.yq_tenant = yq_tenant
        self.dc_mapping = dc_mapping

        self.__bs_cache_file_path = bs_cache_file_path

        self.yaml_config = load_default_yaml(self.__node_ids, self.domain_name, self.static_erasure, self.__additional_log_configs)

        if overrided_actor_system_config:
            self.yaml_config["actor_system_config"] = overrided_actor_system_config

        if "table_service_config" not in self.yaml_config:
            self.yaml_config["table_service_config"] = {}

        if os.getenv('YDB_KQP_ENABLE_IMMEDIATE_EFFECTS', 'false').lower() == 'true':
            self.yaml_config["table_service_config"]["enable_kqp_immediate_effects"] = True

        if os.getenv('YDB_TABLE_ENABLE_PREPARED_DDL', 'false').lower() == 'true':
            self.yaml_config["table_service_config"]["enable_prepared_ddl"] = True

        if os.getenv('PGWIRE_LISTENING_PORT', ''):
            self.yaml_config["local_pg_wire_config"] = {}
            self.yaml_config["local_pg_wire_config"]["listening_port"] = os.getenv('PGWIRE_LISTENING_PORT')

        if pgwire_port:
            self.yaml_config["local_pg_wire_config"] = {}
            self.yaml_config["local_pg_wire_config"]["listening_port"] = pgwire_port

        if disable_iterator_reads:
            self.yaml_config["table_service_config"]["enable_kqp_scan_query_source_read"] = False

        if disable_iterator_lookups:
            self.yaml_config["table_service_config"]["enable_kqp_scan_query_stream_lookup"] = False
            self.yaml_config["table_service_config"]["enable_kqp_data_query_stream_lookup"] = False

        self.yaml_config["feature_flags"]["enable_public_api_external_blobs"] = enable_public_api_external_blobs
        for extra_feature_flag in extra_feature_flags:
            self.yaml_config["feature_flags"][extra_feature_flag] = True
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

        self.yaml_config['grpc_config']['services'].extend(extra_grpc_services)

        # NOTE(shmel1k@): change to 'true' after migration to YDS scheme
        self.yaml_config['sqs_config']['enable_sqs'] = enable_sqs
        self.yaml_config['pqcluster_discovery_config']['enabled'] = enable_pqcd
        self.yaml_config["net_classifier_config"]["net_data_file_path"] = os.path.join(self.__output_path,
                                                                                       'netData.tsv')
        with open(self.yaml_config["net_classifier_config"]["net_data_file_path"], "w") as net_data_file:
            net_data_file.write("")

        if enable_metering:
            self.__set_enable_metering()

        if enable_audit_log:
            self.__set_enable_audit_log()

        self.naming_config = config_pb2.TAppConfig()
        dc_it = itertools.cycle(self._dcs)
        rack_it = itertools.count(start=1)
        body_it = itertools.count(start=1)
        self.yaml_config["nameservice_config"] = {"node": []}
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

        self.__build()

        if self.grpc_ssl_enable:
            self.yaml_config["grpc_config"]["ca"] = self.grpc_tls_ca_path
            self.yaml_config["grpc_config"]["cert"] = self.grpc_tls_cert_path
            self.yaml_config["grpc_config"]["key"] = self.grpc_tls_key_path

        if default_users is not None:
            # check for None for remove default users for empty dict
            if "security_config" not in self.yaml_config["domains_config"]:
                self.yaml_config["domains_config"]["security_config"] = dict()

            # remove existed default users
            self.yaml_config["domains_config"]["security_config"]["default_users"] = []

            for user, password in default_users.items():
                self.yaml_config["domains_config"]["security_config"]["default_users"].append({
                    "name": user,
                    "password": password,
                })

        if os.getenv("YDB_ALLOW_ORIGIN") is not None:
            self.yaml_config["monitoring_config"] = {"allow_origin": str(os.getenv("YDB_ALLOW_ORIGIN"))}

        if enforce_user_token_requirement:
            self.yaml_config["domains_config"]["security_config"]["enforce_user_token_requirement"] = True

        if default_user_sid:
            self.yaml_config["domains_config"]["security_config"]["default_user_sids"] = [default_user_sid]

        if pg_compatible_expirement:
            self.yaml_config["table_service_config"]["enable_prepared_ddl"] = True
            self.yaml_config["table_service_config"]["enable_ast_cache"] = True
            self.yaml_config["table_service_config"]["index_auto_choose_mode"] = 'max_used_prefix'
            self.yaml_config["feature_flags"]['enable_temp_tables'] = True
            self.yaml_config["feature_flags"]['enable_table_pg_types'] = True
            self.yaml_config['feature_flags']['enable_uniq_constraint'] = True
            if not "local_pg_wire_config" in self.yaml_config:
                self.yaml_config["local_pg_wire_config"] = {}

            ydb_pg_port=5432
            if 'YDB_PG_PORT' in os.environ:
                ydb_pg_port = os.environ['YDB_PG_PORT']
            self.yaml_config['local_pg_wire_config']['listening_port'] = ydb_pg_port

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
        Parse(read_binary(__name__, "resources/default_domains.txt"), app_config.DomainsConfig)
        return app_config.DomainsConfig

    @property
    def names_txt(self):
        return self.naming_config.NameserviceConfig

    def __set_enable_metering(self):
        def ensure_path_exists(path):
            if not os.path.isdir(path):
                os.makedirs(path)
            return path

        def get_cwd_for_test(output_path):
            test_name = yatest_common.context.test_name or ""
            test_name = test_name.replace(':', '_')
            return os.path.join(output_path, test_name)

        cwd = get_cwd_for_test(self.__output_path)
        ensure_path_exists(cwd)
        metering_file_path = os.path.join(cwd, 'metering.txt')
        with open(metering_file_path, "w") as metering_file:
            metering_file.write('')
        self.yaml_config['metering_config'] = {'metering_file_path': metering_file_path}

    def __set_enable_audit_log(self):
        def ensure_path_exists(path):
            if not os.path.isdir(path):
                os.makedirs(path)
            return path

        def get_cwd_for_test(output_path):
            test_name = yatest_common.context.test_name or ""
            test_name = test_name.replace(':', '_')
            return os.path.join(output_path, test_name)

        cwd = get_cwd_for_test(self.__output_path)
        ensure_path_exists(cwd)
        audit_file_path = os.path.join(cwd, 'audit.txt')
        with open(audit_file_path, "w") as audit_file:
            audit_file.write('')
        self.yaml_config['audit_config'] = dict(
            file_backend=dict(
                file_path=audit_file_path,
            )
        )

    @property
    def metering_file_path(self):
        return self.yaml_config.get('metering_config', {}).get('metering_file_path')

    @property
    def audit_file_path(self):
        return self.yaml_config.get('audit_config', {}).get('file_backend', {}).get('file_path')

    @property
    def nbs_enable(self):
        return self._enable_nbs

    @property
    def sqs_service_enabled(self):
        return self.yaml_config['sqs_config']['enable_sqs']

    @property
    def output_path(self):
        return self.__output_path

    def set_binary_path(self, binary_path):
        self.__binary_path = binary_path
        return self

    @property
    def binary_path(self):
        if self.__binary_path is not None:
            return self.__binary_path
        return kikimr_driver_path()

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
            writer.write(yaml.safe_dump(self.yaml_config))

    def get_yql_udfs_to_load(self):
        if not self.__load_udfs:
            return []
        udfs_path = self.__udfs_path or yatest_common.build_path("yql/udfs")
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
        self.yaml_config["domains_config"]["state_storage"] = []
        self.yaml_config["domains_config"]["state_storage"].append({"ssid" : 1, "ring" : {"nto_select" : self.n_to_select, "ring" : []}})

        for ring in self.state_storage_rings:
            self.yaml_config["domains_config"]["state_storage"][0]["ring"]["ring"].append({"node" : ring if isinstance(ring, list) else [ring], "use_ring_specific_node_selection" : True})

    def _add_pdisk_to_static_group(self, pdisk_id, path, node_id, pdisk_category, ring):
        domain_id = len(
            self.yaml_config['blob_storage_config']["service_set"]["groups"][0]["rings"][ring]["fail_domains"])
        self.yaml_config['blob_storage_config']["service_set"]["pdisks"].append(
            {"node_id": node_id, "pdisk_id": pdisk_id, "path": path, "pdisk_guid": pdisk_id,
             "pdisk_category": pdisk_category})
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

    def __build(self):
        datacenter_id_generator = itertools.cycle(self._dcs)
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

        for node_id in self.__node_ids:
            datacenter_id = next(datacenter_id_generator)

            for pdisk_id in range(1, self.__number_of_pdisks_per_node + 1):
                disk_size = self.static_pdisk_size if pdisk_id <= 1 else self.__dynamic_pdisks[pdisk_id - 2].get(
                    'disk_size', self.dynamic_pdisk_size)
                pdisk_user_kind = 0 if pdisk_id <= 1 else self.__dynamic_pdisks[pdisk_id - 2].get('user_kind', 0)

                if self.__use_in_memory_pdisks:
                    pdisk_size_gb = disk_size / (1024 * 1024 * 1024)
                    pdisk_path = "SectorMap:%d:%d" % (pdisk_id, pdisk_size_gb)
                elif self.__pdisks_directory:
                    pdisk_path = os.path.join(self.__pdisks_directory, str(pdisk_id))
                else:
                    tmp_file = tempfile.NamedTemporaryFile(prefix="pdisk{}".format(pdisk_id), suffix=".data",
                                                           dir=self._pdisk_store_path)
                    pdisk_path = tmp_file.name

                self._pdisks_info.append({'pdisk_path': pdisk_path, 'node_id': node_id, 'disk_size': disk_size,
                                          'pdisk_user_kind': pdisk_user_kind})
                if pdisk_id == 1 and node_id <= self.static_erasure.min_fail_domains * self._rings_count:
                    self._add_pdisk_to_static_group(
                        pdisk_id,
                        pdisk_path,
                        node_id,
                        pdisk_user_kind,
                        datacenter_id - 1,
                    )
