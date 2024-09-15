#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
import base64
import collections
import itertools
import logging
import subprocess
import tempfile

import yaml
from ydb.core.fq.libs.config.protos.fq_config_pb2 import TConfig as TFederatedQueryConfig
from google.protobuf import json_format

from ydb.core.protos import (
    auth_pb2,
    blobstorage_vdisk_config_pb2,
    bootstrap_pb2,
    cms_pb2,
    config_pb2,
    feature_flags_pb2,
    key_pb2,
    netclassifier_pb2,
    pqconfig_pb2,
    resource_broker_pb2,
)
from ydb.tools.cfg import base, types, utils
from ydb.tools.cfg.templates import (
    dynamic_cfg_new_style,
    kikimr_cfg_for_dynamic_node,
    kikimr_cfg_for_dynamic_slot,
    kikimr_cfg_for_static_node,
    kikimr_cfg_for_static_node_new_style,
)

logger = logging.getLogger(__name__)


class StaticConfigGenerator(object):
    def __init__(
        self,
        template,
        binary_path,
        output_dir,
        database=None,
        node_broker_port=2135,
        ic_port=19001,
        walle_provider=None,
        grpc_port=2135,
        mon_port=8765,
        cfg_home="/Berkanavt/kikimr",
        sqs_port=8771,
        enable_cores=False,
        local_binary_path=None,
        skip_location=False,
        schema_validator=None,
        **kwargs
    ):
        self.__proto_configs = {}
        self.__binary_path = binary_path
        self.__local_binary_path = local_binary_path or binary_path
        self.__output_dir = output_dir
        # collects and provides information about cluster hosts
        self.__cluster_details = base.ClusterDetailsProvider(template, walle_provider, validator=schema_validator, database=database)
        self._enable_cores = template.get("enable_cores", enable_cores)
        self._yaml_config_enabled = template.get("yaml_config_enabled", False)
        self.__is_dynamic_node = True if database is not None else False
        self._database = database
        self._walle_provider = walle_provider
        self._skip_location = skip_location
        self.__node_broker_port = node_broker_port
        self.__grpc_port = grpc_port
        self.__ic_port = ic_port
        self.__mon_port = mon_port
        self.__kikimr_home = cfg_home
        self.__sqs_port = sqs_port
        self._mon_address = None
        self.__config_file_to_generate_callable = {
            "boot.txt": self.__generate_boot_txt,
            "bs.txt": self.__generate_bs_txt,
            "channels.txt": self.__generate_channels_txt,
            "domains.txt": self.__generate_domains_txt,
            "log.txt": self.__generate_log_txt,
            "kqp.txt": self.__generate_kqp_txt,
            "names.txt": self.__generate_names_txt,
            "sys.txt": self.__generate_sys_txt,
            "tracing.txt": self.__generate_tracing_txt,
            # files with default implementation
            "sqs.txt": None,
            "vdisks.txt": None,
            "ic.txt": None,
            "grpc.txt": None,
            "feature_flags.txt": None,
            "auth.txt": None,
            "pq.txt": None,
            "cms.txt": None,
            "rb.txt": None,
            "metering.txt": None,
            "audit.txt": None,
            "fq.txt": None,
            "dyn_ns.txt": None,
            "netclassifier.txt": None,
            "pqcd.txt": None,
            "failure_injection.txt": None,
            "pdisk_key.txt": None,
        }
        self.__optional_config_files = set(
            (
                "rb.txt",
                "metering.txt",
                "audit.txt",
                "fq.txt",
                "failure_injection.txt",
                "pdisk_key.txt",
            )
        )
        tracing = template.get("tracing_config")
        if tracing is not None:
            self.__tracing = (
                tracing["backend"],
                tracing.get("uploader"),
                tracing.get("sampling", []),
                tracing.get("external_throttling", []),
            )
        else:
            self.__tracing = None
        self.__write_mbus_settings_to_kikimr_cfg = False

    @property
    def auth_txt(self):
        return self.__proto_config("auth.txt", auth_pb2.TAuthConfig, self.__cluster_details.use_auth)

    @property
    def sys_txt(self):
        return self.__proto_config("sys.txt")

    @property
    def tracing_txt(self):
        return self.__proto_config("tracing.txt")

    @property
    def names_txt(self):
        return self.__proto_config("names.txt")

    @property
    def kqp_txt(self):
        return self.__proto_config("kqp.txt")

    @property
    def pq_txt(self):
        return self.__proto_config("pq.txt", pqconfig_pb2.TPQConfig, self.__cluster_details.pq_config)

    @property
    def boot_txt(self):
        return self.__proto_config("boot.txt")

    @property
    def domains_txt(self):
        return self.__proto_config("domains.txt")

    @property
    def feature_flags_txt(self):
        return self.__proto_config("feature_flags.txt", feature_flags_pb2.TFeatureFlags, self.__cluster_details.get_service("features"))

    @property
    def failure_injection_txt(self):
        return self.__proto_config(
            "failure_injection.txt",
            config_pb2.TFailureInjectionConfig,
            self.__cluster_details.get_service("failure_injection_config"),
        )

    @property
    def failure_injection_txt_enabled(self):
        return self.__proto_config("failure_injection.txt").ByteSize() > 0

    @property
    def bs_txt(self):
        return self.__proto_config("bs.txt")

    @property
    def netclassifier_txt(self):
        return self.__proto_config(
            "netclassifier.txt",
            netclassifier_pb2.TNetClassifierConfig,
            self.__cluster_details.get_service("netclassifier"),
        )

    @property
    def pqcd_txt(self):
        return self.__proto_config(
            "pqcd.txt", pqconfig_pb2.TPQClusterDiscoveryConfig, self.__cluster_details.get_service("pqclusterdiscovery")
        )

    @property
    def ic_txt(self):
        return self.__proto_config("ic.txt", config_pb2.TInterconnectConfig, self.__cluster_details.ic_config)

    @property
    def grpc_txt(self):
        return self.__proto_config("grpc.txt", config_pb2.TGRpcConfig, self.__cluster_details.grpc_config)

    @property
    def dyn_ns_txt(self):
        return self.__proto_config("dyn_ns.txt", config_pb2.TDynamicNameserviceConfig, self.__cluster_details.dynamicnameservice_config)

    @property
    def log_txt(self):
        return self.__proto_config("log.txt")

    @property
    def channels_txt(self):
        return self.__proto_config("channels.txt")

    @property
    def vdisks_txt(self):
        return self.__proto_config("vdisks.txt", blobstorage_vdisk_config_pb2.TAllVDiskKinds, self.__cluster_details.vdisk_config)

    @property
    def sqs_txt(self):
        return self.__proto_config("sqs.txt", config_pb2.TSqsConfig, self.__cluster_details.get_service("sqs"))

    @property
    def cms_txt(self):
        return self.__proto_config("cms.txt", cms_pb2.TCmsConfig, self.__cluster_details.get_service("cms"))

    @property
    def rb_txt(self):
        return self.__proto_config(
            "rb.txt", resource_broker_pb2.TResourceBrokerConfig, self.__cluster_details.get_service("resource_broker")
        )

    @property
    def rb_txt_enabled(self):
        return self.__proto_config("rb.txt").ByteSize() > 0

    @property
    def metering_txt(self):
        return self.__proto_config("metering.txt", config_pb2.TMeteringConfig, self.__cluster_details.get_service("metering"))

    @property
    def metering_txt_enabled(self):
        return self.__proto_config("metering.txt").ByteSize() > 0

    @property
    def audit_txt(self):
        return self.__proto_config("audit.txt", config_pb2.TAuditConfig, self.__cluster_details.get_service("audit"))

    @property
    def audit_txt_enabled(self):
        return self.__proto_config("audit.txt").ByteSize() > 0

    @property
    def fq_txt(self):
        return self.__proto_config("fq.txt", TFederatedQueryConfig, self.__cluster_details.get_service("yq"))

    @property
    def fq_txt_enabled(self):
        return self.__proto_config("fq.txt").ByteSize() > 0

    @property
    def pdisk_key_txt(self):
        return self.__proto_config("pdisk_key.txt", key_pb2.TKeyConfig, self.__cluster_details.pdisk_key_config)

    @property
    def pdisk_key_txt_enabled(self):
        return self.__proto_config("pdisk_key.txt").ByteSize() > 0

    @property
    def mbus_enabled(self):
        mbus_config = self.__cluster_details.get_service("message_bus_config")
        return mbus_config is not None and len(mbus_config) > 0

    @property
    def table_service_config(self):
        return self.__cluster_details.get_service("table_service_config")

    @property
    def column_shard_config(self):
        return self.__cluster_details.get_service("column_shard_config")

    @property
    def hive_config(self):
        return self.__proto_config("hive", config_pb2.THiveConfig, self.__cluster_details.get_service("hive_config"))

    @property
    def kikimr_cfg(self):
        if self.__is_dynamic_node:
            return kikimr_cfg_for_dynamic_node(
                self.__node_broker_port,
                self._database,
                self.__ic_port,
                self.__mon_port,
                self.__kikimr_home,
                self.__sqs_port,
                self.sqs_txt.EnableSqs,
                self._enable_cores,
                self.__cluster_details.default_log_level,
                mon_address=self.__cluster_details.monitor_address,
                cert_params=self.__cluster_details.ic_cert_params,
                rb_txt_enabled=self.rb_txt_enabled,
                metering_txt_enabled=self.metering_txt_enabled,
                audit_txt_enabled=self.audit_txt_enabled,
                fq_txt_enabled=self.fq_txt_enabled,
            )

        if self.__cluster_details.use_new_style_kikimr_cfg:
            return kikimr_cfg_for_static_node_new_style(
                ic_port=self.__ic_port,
                mon_port=self.__mon_port,
                mon_address=self.__cluster_details.monitor_address,
                grpc_port=self.__grpc_port,
                enable_cores=self._enable_cores,
                kikimr_home=self.__kikimr_home,
                cert_params=self.__cluster_details.ic_cert_params,
                mbus_enabled=self.mbus_enabled,
            )

        return kikimr_cfg_for_static_node(
            self._database,
            self.__ic_port,
            self.__mon_port,
            self.__kikimr_home,
            self.pq_txt.Enabled,
            self._enable_cores,
            self.__cluster_details.default_log_level,
            mon_address=self.__cluster_details.monitor_address,
            cert_params=self.__cluster_details.ic_cert_params,
            rb_txt_enabled=self.rb_txt_enabled,
            metering_txt_enabled=self.metering_txt_enabled,
            audit_txt_enabled=self.audit_txt_enabled,
            fq_txt_enabled=self.fq_txt_enabled,
            mbus_enabled=self.mbus_enabled,
        )

    def get_all_configs(self):
        all_configs = {}
        for file_name in self.__config_file_to_generate_callable.keys():
            field_name = file_name.replace(".", "_")
            config_proto = getattr(self, field_name)
            if file_name in self.__optional_config_files and not getattr(self, field_name + "_enabled"):
                continue  # skip optional files that are not enabled
            if self.__cluster_details.need_txt_files:
                all_configs[file_name] = utils.message_to_string(config_proto)

        if self.__cluster_details.need_generate_app_config:
            all_configs["app_config.proto"] = utils.message_to_string(self.get_app_config())
        all_configs["kikimr.cfg"] = self.kikimr_cfg
        all_configs["dynamic_server.cfg"] = self.dynamic_server_common_args
        normalized_config = self.get_normalized_config()
        all_configs["config.yaml"] = self.get_yaml_format_config(normalized_config)
        all_configs["dynconfig.yaml"] = self.get_yaml_format_dynconfig(normalized_config)
        return all_configs

    def get_yaml_format_string(self, key):
        result = []
        prev = None
        for c in key:
            if prev is not None and c.isupper() and prev.islower():
                result.append("_")
                result.append(c.lower())
            elif prev is not None and prev.isdigit() and c.isupper():
                result.append("_")
                result.append(c.lower())
            else:
                result.append(c.lower())
            prev = c
        return "".join(result)

    def normalize_dictionary(self, yaml_config):
        result = {}
        if isinstance(yaml_config, list):
            result = []
            for item in yaml_config:
                result.append(self.normalize_dictionary(item))
            return result
        elif isinstance(yaml_config, dict):
            result = {}
            for key, value in yaml_config.items():
                result[self.get_yaml_format_string(key)] = self.normalize_dictionary(value)
        else:
            return yaml_config
        return result

    def get_normalized_config(self):
        app_config = self.get_app_config()
        dictionary = json_format.MessageToDict(app_config, preserving_proto_field_name=True)
        normalized_config = self.normalize_dictionary(dictionary)

        if self.table_service_config:
            normalized_config["table_service_config"] = self.table_service_config

        if self.column_shard_config:
            normalized_config["column_shard_config"] = self.column_shard_config

        if self.__cluster_details.blob_storage_config is not None:
            normalized_config["blob_storage_config"] = self.__cluster_details.blob_storage_config
        else:
            blobstorage_config_service_set = normalized_config["blob_storage_config"]["service_set"]
            del blobstorage_config_service_set["vdisks"]

            pdisks_info = {}
            pdisk_config = {}

            for pdisk in blobstorage_config_service_set["pdisks"]:
                pdisk_lookup_id = (pdisk["node_id"], pdisk["pdisk_id"])
                pdisks_info[pdisk_lookup_id] = (pdisk["path"], pdisk["pdisk_category"])

                if "pdisk_config" in pdisk:
                    pdisk_config[pdisk_lookup_id] = pdisk["pdisk_config"]

            del blobstorage_config_service_set["pdisks"]

            for group in blobstorage_config_service_set["groups"]:
                for ring in group["rings"]:
                    for fd in ring["fail_domains"]:
                        for vl in fd["vdisk_locations"]:
                            if vl["vdisk_slot_id"] == 0:
                                del vl["vdisk_slot_id"]

                            pdisk_lookup_id = (vl["node_id"], vl["pdisk_id"])
                            vl["path"], vl["pdisk_category"] = pdisks_info[pdisk_lookup_id]

                            if pdisk_lookup_id in pdisk_config:
                                vl["pdisk_config"] = pdisk_config[pdisk_lookup_id]

        for entry in normalized_config.get("log_config", {}).get("entry", []):
            entry["component"] = types.py3_ensure_str(base64.b64decode(entry["component"]))

        normalized_config["system_tablets"] = {}
        system_tablets_info = normalized_config["system_tablets"]
        for tablet in normalized_config["bootstrap_config"]["tablet"]:
            tablet_type = tablet["type"].lower()
            if tablet_type not in system_tablets_info:
                system_tablets_info[tablet_type] = []

            system_tablets_info[tablet_type].append({"info": {"tablet_id": tablet["info"]["tablet_id"]}, "node": tablet["node"]})

        del normalized_config["bootstrap_config"]["tablet"]

        normalized_config["domains_config"]["disable_builtin_security"] = True
        for domain in normalized_config["domains_config"]["domain"]:
            if "coordinator" in domain:
                del domain["coordinator"]

            if "proxy" in domain:
                del domain["proxy"]

            if "allocators" in domain:
                del domain["allocators"]

            if "mediator" in domain:
                del domain["mediator"]

            if "hive_uid" in domain:
                del domain["hive_uid"]

            for field in ["explicit_coordinators", "explicit_allocators", "explicit_mediators"]:
                if field in domain:
                    del domain[field]

        if "hive_config" in normalized_config["domains_config"]:
            del normalized_config["domains_config"]["hive_config"]

        normalized_config["hosts"] = []
        for node in normalized_config["nameservice_config"]["node"]:
            if "port" in node and int(node.get("port")) == 19001:
                del node["port"]

            if "interconnect_host" in node and node["interconnect_host"] == node["host"]:
                del node["interconnect_host"]

            normalized_config["hosts"].append(node)

        del normalized_config["nameservice_config"]["node"]

        normalized_config["static_erasure"] = str(self.__cluster_details.static_erasure)

        if 'blob_storage_config' in normalized_config:
            for group in normalized_config['blob_storage_config']['service_set']['groups']:
                for ring in group['rings']:
                    for fail_domain in ring['fail_domains']:
                        for vdisk_location in fail_domain['vdisk_locations']:
                            vdisk_location['pdisk_guid'] = int(vdisk_location['pdisk_guid'])
                            vdisk_location['pdisk_category'] = int(vdisk_location['pdisk_category'])
                            if 'pdisk_config' in vdisk_location:
                                if 'expected_slot_count' in vdisk_location['pdisk_config']:
                                    vdisk_location['pdisk_config']['expected_slot_count'] = int(vdisk_location['pdisk_config']['expected_slot_count'])
        if 'channel_profile_config' in normalized_config:
            for profile in normalized_config['channel_profile_config']['profile']:
                for channel in profile['channel']:
                    channel['pdisk_category'] = int(channel['pdisk_category'])
        if 'system_tablets' in normalized_config:
            for tablets in normalized_config['system_tablets'].values():
                for tablet in tablets:
                    tablet['info']['tablet_id'] = int(tablet['info']['tablet_id'])

        if self._yaml_config_enabled:
            normalized_config['yaml_config_enabled'] = True

        return normalized_config

    def get_yaml_format_config(self, normalized_config):
        return yaml.safe_dump(normalized_config, sort_keys=True, default_flow_style=False, indent=2)

    def get_yaml_format_dynconfig(self, normalized_config):
        cluster_uuid = normalized_config.get('nameservice_config', {}).get('cluster_uuid', '')
        dynconfig = {
            'metadata': {
                'kind': 'MainConfig',
                'cluster': cluster_uuid,
                'version': 0,
            },
            'config': copy.deepcopy(normalized_config),
            'allowed_labels': {
                'node_id': {'type': 'string'},
                'host': {'type': 'string'},
                'tenant': {'type': 'string'},
            },
            'selector_config': [],
        }

        if self.__cluster_details.use_auto_config:
            dynconfig['selector_config'].append({
                'description': 'actor system config for dynnodes',
                'selector': {
                    'dynamic': True,
                },
                'config': {
                    'actor_system_config': {
                        'cpu_count': self.__cluster_details.dynamic_cpu_count,
                        'node_type': 'COMPUTE',
                        'use_auto_config': True,
                    }
                }
            })
        # emulate dumping ordered dict to yaml
        lines = []
        for key in ['metadata', 'config', 'allowed_labels', 'selector_config']:
            lines.append(key + ':')
            substr = yaml.safe_dump(dynconfig[key], sort_keys=True, default_flow_style=False, indent=2)
            for line in substr.split('\n'):
                lines.append('  ' + line)
        return '\n'.join(lines)

    def get_app_config(self):
        app_config = config_pb2.TAppConfig()
        app_config.BootstrapConfig.CopyFrom(self.boot_txt)
        app_config.BlobStorageConfig.CopyFrom(self.bs_txt)
        app_config.ChannelProfileConfig.CopyFrom(self.channels_txt)
        app_config.DomainsConfig.CopyFrom(self.domains_txt)
        if self.feature_flags_txt.ByteSize() > 0:
            app_config.FeatureFlags.CopyFrom(self.feature_flags_txt)
        app_config.LogConfig.CopyFrom(self.log_txt)
        if self.auth_txt.ByteSize() > 0:
            app_config.AuthConfig.CopyFrom(self.auth_txt)
        app_config.KQPConfig.CopyFrom(self.kqp_txt)
        app_config.NameserviceConfig.CopyFrom(self.names_txt)
        app_config.ActorSystemConfig.CopyFrom(self.sys_txt)
        app_config.GRpcConfig.CopyFrom(self.grpc_txt)
        app_config.InterconnectConfig.CopyFrom(self.ic_txt)
        app_config.VDiskConfig.CopyFrom(self.vdisks_txt)
        app_config.PQConfig.CopyFrom(self.pq_txt)

        if self.cms_txt.ByteSize() > 0:
            app_config.CmsConfig.CopyFrom(self.cms_txt)
        if self.dyn_ns_txt.ByteSize() > 0:
            app_config.DynamicNameserviceConfig.CopyFrom(self.dyn_ns_txt)
        if self.pqcd_txt.ByteSize() > 0:
            app_config.PQClusterDiscoveryConfig.CopyFrom(self.pqcd_txt)
        if self.netclassifier_txt.ByteSize() > 0:
            app_config.NetClassifierConfig.CopyFrom(self.netclassifier_txt)
        if self.rb_txt_enabled:
            app_config.ResourceBrokerConfig.CopyFrom(self.rb_txt)
        if self.metering_txt_enabled:
            app_config.MeteringConfig.CopyFrom(self.metering_txt)
        if self.audit_txt_enabled:
            app_config.AuditConfig.CopyFrom(self.audit_txt)
        if self.fq_txt_enabled:
            app_config.FederatedQueryConfig.CopyFrom(self.fq_txt)
        if self.failure_injection_txt_enabled:
            app_config.FailureInjectionConfig.CopyFrom(self.failure_injection_txt)
        if self.sqs_txt.ByteSize() > 0:
            app_config.SqsConfig.CopyFrom(self.sqs_txt)
        if self.hive_config.ByteSize() > 0:
            app_config.HiveConfig.CopyFrom(self.hive_config)
        app_config.MergeFrom(self.tracing_txt)
        if self.pdisk_key_txt_enabled:
            app_config.PDiskKeyConfig.CopyFrom(self.pdisk_key_txt)
        return app_config

    def __proto_config(self, config_file, config_class=None, cluster_details_for_field=None):
        if config_file not in self.__proto_configs:
            if config_class is not None:
                self.__proto_configs[config_file] = config_class()

            config_file_factory = self.__config_file_to_generate_callable.get(config_file)
            if config_file_factory is not None:
                config_file_factory()

            if cluster_details_for_field is not None:
                utils.apply_config_changes(
                    self.__proto_configs[config_file],
                    cluster_details_for_field,
                )

        return self.__proto_configs[config_file]

    def _tablet_config(self, tablet_name, idx):
        tablet_config_id = tablet_name.lower() + "-" + str(idx)
        if tablet_config_id in self.__cluster_details.system_tablets_config:
            return self.__cluster_details.system_tablets_config.get(tablet_config_id, {})
        return self.__cluster_details.system_tablets_config.get(tablet_name.lower(), {})

    def __add_tablet(self, tablet_type, index, node_ids, number_of_channels=3):
        boot_config = self.__proto_configs["boot.txt"]
        tablet_name = tablet_type.name
        tablet_config = self._tablet_config(tablet_name, index)
        if not tablet_config.get("enabled", True):
            return

        tablet_id = tablet_type.tablet_id_for(index)
        if tablet_config.get("tablet_id", None):
            tablet_id = tablet_config.get("tablet_id", None)

        tablet = boot_config.Tablet.add()
        tablet.Type = boot_config.ETabletType.Value(tablet_name)
        tablet.Info.TabletID = tablet_id

        allow_dynamic_configuration = tablet_config.get("allow_dynamic_configuration", False)
        explicit_node_ids = tablet_config.get("explicit_node_ids", [])

        if allow_dynamic_configuration:
            tablet.AllowDynamicConfiguration = True

        if explicit_node_ids:
            node_ids = explicit_node_ids
        tablet.Node.extend(node_ids)

        for channel_id in range(int(number_of_channels)):
            channel = tablet.Info.Channels.add(Channel=channel_id, ChannelErasureName=str(self.__cluster_details.static_erasure))
            channel.History.add(FromGeneration=0, GroupID=0)

    @property
    def __tablet_types(self):
        if self.__cluster_details.use_fixed_tablet_types:
            return types.TabletTypesFixed
        return types.TabletTypes

    @property
    def __system_tablets(self):
        all_tablets = []
        tablet_types = self.__tablet_types
        for domain in self.__cluster_details.domains:
            all_tablets += [
                (tablet_types.FLAT_HIVE, 1),
                (tablet_types.FLAT_BS_CONTROLLER, 1),
                (tablet_types.FLAT_SCHEMESHARD, 1),
                (tablet_types.FLAT_TX_COORDINATOR, domain.coordinators),
                (tablet_types.TX_MEDIATOR, domain.mediators),
                (tablet_types.TX_ALLOCATOR, domain.allocators),
                (tablet_types.CMS, 1),
                (tablet_types.NODE_BROKER, 1),
                (tablet_types.TENANT_SLOT_BROKER, 1),
                (tablet_types.CONSOLE, 1),
            ]
        return all_tablets

    def __generate_boot_txt(self):
        self.__proto_configs["boot.txt"] = bootstrap_pb2.TBootstrap()

        for tablet_type, tablet_count in self.__system_tablets:
            for index in range(int(tablet_count)):
                self.__add_tablet(tablet_type, index, self.__cluster_details.system_tablets_node_ids)

        if self.__cluster_details.shared_cache_memory_limit is not None:
            boot_txt = self.__proto_configs["boot.txt"]
            boot_txt.SharedCacheConfig.MemoryLimit = self.__cluster_details.shared_cache_memory_limit
        shared_cache_size = self.__cluster_details.pq_shared_cache_size
        if shared_cache_size is not None:
            boot_txt = self.__proto_configs["boot.txt"]
            boot_txt.NodeLimits.PersQueueNodeConfig.SharedCacheSizeMb = shared_cache_size

    def __generate_bs_txt(self):
        self.__proto_configs["bs.txt"] = config_pb2.TBlobStorageConfig()
        bs_format_config = config_pb2.TBlobStorageFormatConfig()

        all_guids = set()
        rack_enumeration = {}
        dc_enumeration = {}
        for body_id, host in enumerate(self.__cluster_details.static_bs_group_hosts):
            pdisk_id = 1
            if host.rack not in rack_enumeration:
                rack_enumeration[host.rack] = 1 + len(rack_enumeration)

            if host.datacenter not in dc_enumeration:
                dc_enumeration[host.datacenter] = 1 + len(dc_enumeration)

            for drive in host.drives:
                drive_pb = bs_format_config.Drive.add(
                    RackId=rack_enumeration[host.rack],
                    NodeId=host.node_id,
                    Hostname=host.hostname,
                    Type=drive.type,
                    Path=drive.path,
                    Guid=utils.random_int(2**60, 2**64 - 1, host.hostname, drive.path, drive.type),
                    PDiskId=pdisk_id,
                    DataCenterId=dc_enumeration[host.datacenter],
                    BodyId=body_id,
                )

                if drive.expected_slot_count is not None:
                    drive_pb.PDiskConfig.ExpectedSlotCount = drive.expected_slot_count

                assert drive_pb.Guid not in all_guids, "All Guids must be unique!"
                all_guids.add(drive_pb.Guid)

                pdisk_id += 1

        rack_enumeration = {}
        dc_enumeration = {}

        if not self.__cluster_details.get_service("static_groups"):
            self.__proto_configs["bs.txt"] = self._read_generated_bs_config(
                str(self.__cluster_details.static_erasure),
                str(self.__cluster_details.min_fail_domains),
                str(self.__cluster_details.static_pdisk_type),
                str(self.__cluster_details.fail_domain_type),
                bs_format_config,
            )
            if self.__cluster_details.nw_cache_file_path is not None:
                self.__proto_configs["bs.txt"].CacheFilePath = self.__cluster_details.nw_cache_file_path
            return

        hosts_map = {host.node_id: host for host in self.__cluster_details.hosts}
        groups = self.__cluster_details.get_service("static_groups")["groups"]
        dc_migration = self.__cluster_details.static_group_hosts_migration
        for group in groups:
            group_id = group["group_id"]

            bs_format_config = config_pb2.TBlobStorageFormatConfig()

            for drive_json in group.get("drives"):
                host = hosts_map.get(drive_json.get("node_id"))

                assert host is not None

                drive = None
                for can in host.drives:
                    if can.path == drive_json["path"]:
                        drive = can

                if host.rack not in rack_enumeration:
                    rack_enumeration[host.rack] = 1 + len(rack_enumeration)

                static_group_host_dc = host.datacenter
                if dc_migration:
                    for node_to_migrate in dc_migration:
                        if host.node_id == node_to_migrate["node_id"]:
                            static_group_host_dc = node_to_migrate["from_dc"]
                if static_group_host_dc not in dc_enumeration:
                    dc_enumeration[static_group_host_dc] = 1 + len(dc_enumeration)

                default_pdisk_guid = utils.random_int(2**60, 2**64 - 1, host.hostname, drive.path, drive.type)

                drive_pb = bs_format_config.Drive.add(
                    RackId=rack_enumeration[host.rack],
                    NodeId=host.node_id,
                    Hostname=host.hostname,
                    Type=drive.type,
                    Path=drive.path,
                    Guid=drive_json.get("pdisk_guid", default_pdisk_guid),
                    PDiskId=drive_json.get("pdisk_id", 1),  # default is 1
                    DataCenterId=dc_enumeration[static_group_host_dc],
                    BodyId=host.node_id,
                )

                if drive.expected_slot_count is not None:
                    drive_pb.PDiskConfig.ExpectedSlotCount = drive.expected_slot_count

            my_group = self._read_generated_bs_config(
                group.get("erasure"),
                str(self.__cluster_details.min_fail_domains),
                group.get("static_pdisk_type"),
                group.get("fail_domain_type"),
                bs_format_config,
            )

            if len(self.__proto_configs["bs.txt"].ServiceSet.Groups) == 0:
                self.__proto_configs["bs.txt"] = my_group
                continue

            self.__proto_configs["bs.txt"].ServiceSet.PDisks.extend(my_group.ServiceSet.PDisks)
            for vdisk in my_group.ServiceSet.VDisks:
                vdisk.VDiskID.GroupID = group_id
                self.__proto_configs["bs.txt"].ServiceSet.VDisks.append(vdisk)

            for gr in my_group.ServiceSet.Groups:
                gr.GroupID = group_id
                self.__proto_configs["bs.txt"].ServiceSet.Groups.append(gr)

        if self.__cluster_details.nw_cache_file_path is not None:
            self.__proto_configs["bs.txt"].CacheFilePath = self.__cluster_details.nw_cache_file_path

    def _read_generated_bs_config(self, static_erasure, min_fail_domains, static_pdisk_type, fail_domain_type, bs_format_config):
        result = config_pb2.TBlobStorageConfig()

        with tempfile.NamedTemporaryFile(delete=True) as t_file:
            utils.write_proto_to_file(t_file.name, bs_format_config)

            rx_begin, rx_end, dx_begin, dx_end = types.DistinctionLevels[types.FailDomainType.from_string(fail_domain_type)]

            cmd_base = [
                self.__local_binary_path,
                "admin",
                "bs",
                "genconfig",
                "static",
                "--bs-format-file",
                t_file.name,
                "--erasure",
                static_erasure,
                "--avdomain",
                "1",
                "--faildomains",
                min_fail_domains,
                "--vdisks",
                "1",
                "--pdisktype",
                static_pdisk_type,
            ]

            try:
                output = subprocess.check_output(
                    cmd_base
                    + [
                        "--ring-level-begin",
                        str(rx_begin),
                        "--ring-level-end",
                        str(rx_end),
                        "--domain-level-begin",
                        str(dx_begin),
                        "--domain-level-end",
                        str(dx_end),
                    ]
                )
            except subprocess.CalledProcessError:
                output = subprocess.check_output(
                    cmd_base
                    + [
                        "--dx",
                        fail_domain_type,
                    ]
                )

        utils.read_message_from_string(output, result)

        return result

    def __generate_channels_txt(self):
        self.__proto_configs["channels.txt"] = config_pb2.TChannelProfileConfig()
        channels_config = self.__proto_configs["channels.txt"]
        profile_id = itertools.count(start=0)

        if len(self.__cluster_details.tablet_profiles) < 1:
            # Corner case
            chosen_category = None
            for category in types.PDiskCategory.all_categories():
                if chosen_category is not None:
                    break

                for host in self.__cluster_details.hosts:
                    for drive in host.drives:
                        if str(drive.type).lower() == str(category).lower():
                            chosen_category = category
                            break

            profile = channels_config.Profile.add()
            profile.ProfileId = next(profile_id)
            for _ in range(3):
                profile.Channel.add(
                    ErasureSpecies=str(self.__cluster_details.static_erasure),
                    PDiskCategory=chosen_category,
                    VDiskCategory=str(types.VDiskCategory.Default),
                )

        for user_profile in self.__cluster_details.tablet_profiles:
            profile = channels_config.Profile.add()
            profile.ProfileId = next(profile_id)
            for user_profile_channel in user_profile.channels:
                params = {
                    "ErasureSpecies": str(user_profile_channel.erasure),
                    "PDiskCategory": user_profile_channel.pdisk_type,
                    "VDiskCategory": user_profile_channel.vdisk_kind,
                }

                if user_profile_channel.storage_pool_kind is not None:
                    params["StoragePoolKind"] = user_profile_channel.storage_pool_kind

                profile.Channel.add(**params)

    @property
    def __n_to_select(self):
        nodes_count = len(self.__cluster_details.state_storage_node_ids)
        if self.__cluster_details.static_erasure == types.Erasure.MIRROR_3_DC:
            n_to_select_candidate = 9
            if nodes_count < n_to_select_candidate:
                if types.FailDomainType.is_body_fail_domain(self.__cluster_details.fail_domain_type):
                    n_to_select_candidate = nodes_count
                else:
                    raise RuntimeError(
                        "Unable to configure state storage, n to select %d > length of hosts %d" % (n_to_select_candidate, nodes_count)
                    )
            return n_to_select_candidate

        n_to_select_candidate = nodes_count
        if n_to_select_candidate % 2 == 0:
            n_to_select_candidate -= 1

        return min(5, n_to_select_candidate)

    def __configure_security_settings(self, domains_config):
        utils.apply_config_changes(
            domains_config.SecurityConfig,
            self.__cluster_details.security_settings,
        )

    def __generate_domains_txt(self):
        self.__proto_configs["domains.txt"] = config_pb2.TDomainsConfig()

        domains_config = self.__proto_configs["domains.txt"]

        if self.__cluster_details.forbid_implicit_storage_pools:
            domains_config.ForbidImplicitStoragePools = True

        self.__configure_security_settings(domains_config)

        tablet_types = self.__tablet_types

        for domain_description in self.__cluster_details.domains:
            domain_id = domain_description.domain_id
            domain_name = domain_description.domain_name
            domain = domains_config.Domain.add(Name=domain_name, DomainId=domain_id, PlanResolution=domain_description.plan_resolution)
            domain.SSId.append(domain_id)
            domain.HiveUid.append(domain_id)

            schemeshard_config = self._tablet_config(tablet_types.FLAT_SCHEMESHARD.name, 0)
            schemeroot = schemeshard_config.get("tablet_id")
            if schemeroot is None:
                schemeroot = tablet_types.FLAT_SCHEMESHARD.tablet_id_for(0)

            domain.SchemeRoot = schemeroot

            domain.ExplicitCoordinators.extend(
                [tablet_types.FLAT_TX_COORDINATOR.tablet_id_for(i) for i in range(int(domain_description.coordinators))]
            )
            domain.ExplicitMediators.extend([tablet_types.TX_MEDIATOR.tablet_id_for(i) for i in range(int(domain_description.mediators))])
            domain.ExplicitAllocators.extend(
                [tablet_types.TX_ALLOCATOR.tablet_id_for(i) for i in range(int(domain_description.allocators))]
            )

            self._configure_statestorages(domains_config, domain_id)

            domains_config.HiveConfig.add(HiveUid=domain_id, Hive=tablet_types.FLAT_HIVE.tablet_id_for(0))

            for pool_kind in domain_description.storage_pool_kinds.values():
                pool_type = domain.StoragePoolTypes.add(Kind=pool_kind.kind)
                pool_type.PoolConfig.BoxId = pool_kind.box_id
                pool_type.PoolConfig.Kind = pool_kind.kind
                pool_type.PoolConfig.ErasureSpecies = pool_kind.erasure
                pool_type.PoolConfig.VDiskKind = pool_kind.vdisk_kind
                pool_type.PoolConfig.EncryptionMode = pool_kind.encryption_mode

                fail_domain_type = types.FailDomainType.from_string(pool_kind.fail_domain_type)
                if fail_domain_type != types.FailDomainType.Rack:  # noqa
                    erasure = types.Erasure.from_string(pool_kind.erasure)
                    rx_begin, rx_end, dx_begin, dx_end = types.DistinctionLevels[fail_domain_type]
                    pool_type.PoolConfig.Geometry.RealmLevelBegin = rx_begin
                    pool_type.PoolConfig.Geometry.RealmLevelEnd = rx_end
                    pool_type.PoolConfig.Geometry.DomainLevelBegin = dx_begin
                    pool_type.PoolConfig.Geometry.DomainLevelEnd = dx_end
                    pool_type.PoolConfig.Geometry.NumVDisksPerFailDomain = 1
                    pool_type.PoolConfig.Geometry.NumFailDomainsPerFailRealm = erasure.min_fail_domains
                    num_fail_realms = 3 if erasure == types.Erasure.MIRROR_3_DC else 1
                    pool_type.PoolConfig.Geometry.NumFailRealms = num_fail_realms

                pdisk_filter = pool_type.PoolConfig.PDiskFilter.add()
                if "type" in pool_kind.filter_properties:
                    pdisk_type = pool_kind.filter_properties["type"]
                    pdisk_category = int(types.PDiskCategory.from_string(pdisk_type))
                    pdisk_filter.Property.add(Type=pdisk_category)

                    if "SharedWithOs" in pool_kind.filter_properties:
                        pdisk_filter.Property.add(SharedWithOs=pool_kind.filter_properties["SharedWithOs"])

    def _get_base_statestorage(self, domains_cfg, ss):
        ssid = ss.get("ssid", None)
        if ssid is None and ssid not in (1, 33):
            raise RuntimeError("SSId should be specified for state storage. Possible values are 1 or 33.")
        ss_cfg = domains_cfg.StateStorage.add(SSId=ssid)
        n_to_select = ss.get("n_to_select", self.__n_to_select)
        ss_cfg.Ring.NToSelect = n_to_select
        if n_to_select % 2 != 1:
            raise RuntimeError("Invalid n to select %d, should be odd!" % n_to_select)
        return ss_cfg

    def _configure_state_storage_rings_explicit(self, domains_cfg, ss):
        ss_cfg = self._get_base_statestorage(domains_cfg, ss)
        n_to_select = ss_cfg.Ring.NToSelect
        if len(ss.get("rings", [])) < n_to_select:
            raise RuntimeError("Invalid state storage, expected at least %d rings" % n_to_select)
        by_node_id_index = {node.node_id: node for node in self.__cluster_details.hosts}
        already_appear = set()
        for ring in ss.get("rings", []):
            ring_cfg = ss_cfg.Ring.Ring.add()
            this_ring_racks = set()
            node_ids = ring.get("node_ids", [])
            for node_id in node_ids:
                node = by_node_id_index.get(node_id)
                this_ring_racks.add(node.rack)

            if len(already_appear & this_ring_racks) >= 1 and not self.__cluster_details.allow_incorrect_state_storage:
                raise RuntimeError("Some racks appears in at least 2 rings")

            already_appear = already_appear | this_ring_racks
            ring_cfg.Node.extend(node_ids)

            use_ring_specific_node_selection = ring.get("use_ring_specific_node_selection", False)
            if use_ring_specific_node_selection:
                ring_cfg.UseRingSpecificNodeSelection = use_ring_specific_node_selection

            use_single_node_actor_id = ring.get("use_single_node_actor_id", False)
            if use_single_node_actor_id:
                if len(ring_cfg.Node) > 1:
                    raise RuntimeError("use_single_node_actor_id can be True only for one node rings")
                ring_cfg.UseSingleNodeActorId = use_single_node_actor_id

    def _validate_rings_count(self, n_to_select, rings_count):
        if n_to_select == 9 and rings_count != 9:
            raise RuntimeError("Invalid case: n to select is 9, but is rings_count is %d" % rings_count)

    def _configure_state_storage_rings_select(self, domains_cfg, ss):
        ss_cfg = self._get_base_statestorage(domains_cfg, ss)
        n_to_select = ss_cfg.Ring.NToSelect
        rings_count = ss.get("rings_count", n_to_select)
        host_count_per_ring = ss.get("host_count_per_ring", 1)
        if rings_count < n_to_select:
            raise RuntimeError("Invalid rings count %d is less than n to select" % rings_count)
        racks = collections.defaultdict(list)
        rack_sizes = collections.Counter()
        for node in self.__cluster_details.hosts:
            rack_sizes[node.rack] += 1
            racks[node.rack].append(node)

        chosen_racks = []
        dc_limit = n_to_select == 9
        rack_sizes = reversed(sorted(rack_sizes.items(), key=lambda x: x[1]))
        it = iter(rack_sizes)
        by_dc = collections.Counter()
        while len(chosen_racks) < rings_count:
            try:
                rack_id, _ = next(it)
                rack = racks[rack_id]
            except StopIteration:
                raise RuntimeError("Failed to collect %d rings" % rings_count)

            dc_id = rack[0].datacenter
            by_dc[dc_id] += 1

            if by_dc[dc_id] >= 3 and dc_limit:
                continue

            chosen_racks.append(rack)

        for rack in chosen_racks:
            if len(rack) < host_count_per_ring:
                raise RuntimeError("Some racks size is less than host_count_per_ring")
            ring_cfg = ss_cfg.Ring.Ring.add()
            node_ids = sorted(list(map(lambda x: x.node_id, rack)))[:host_count_per_ring]
            ring_cfg.Node.extend(node_ids)

    def _configure_statestorages(self, domains_cfg, domain_id):
        if not self.__cluster_details.state_storages:
            return self._configure_default_state_storage(domains_cfg, domain_id)

        for ss in self.__cluster_details.state_storages:
            use_explicit_ss = ss.get("use_explicit_ss", False)

            if use_explicit_ss:
                # using rings feature
                self._configure_state_storage_rings_explicit(domains_cfg, ss)
            else:
                self._configure_state_storage_rings_select(domains_cfg, ss)

    def _configure_default_state_storage(self, domains_config, domain_id):
        state_storage_cfg = domains_config.StateStorage.add(SSId=domain_id)
        if self.__n_to_select < 5 or types.FailDomainType.is_body_fail_domain(self.__cluster_details.fail_domain_type):
            state_storage_cfg.Ring.NToSelect = self.__n_to_select
            state_storage_cfg.Ring.Node.extend(self.__cluster_details.state_storage_node_ids)
            return

        if self.__cluster_details.allow_incorrect_state_storage:
            logger.warning("Using unsafe option: " "state storage in the cluster is probably broken")
            state_storage_cfg.Ring.NToSelect = self.__n_to_select
            state_storage_cfg.Ring.Node.extend(self.__cluster_details.state_storage_node_ids)
            return

        rack_limit = 1
        dc_limit = None
        if self.__n_to_select == 9:
            dc_limit = 3

        occupied_dcs = collections.Counter()
        occupied_racks = collections.Counter()
        selected_ids = []
        hosts_by_node_id = {node.node_id: node for node in self.__cluster_details.hosts}
        for node_id in self.__cluster_details.state_storage_node_ids:
            node = hosts_by_node_id.get(node_id)
            assert node is not None

            if occupied_racks[node.rack] == rack_limit:
                continue

            if occupied_dcs[node.datacenter] == dc_limit:
                continue

            occupied_racks[node.rack] += 1
            occupied_dcs[node.datacenter] += 1
            selected_ids.append(node.node_id)

        if len(selected_ids) < self.__n_to_select:
            raise RuntimeError("Unable to build valid quorum in state storage")

        state_storage_cfg.Ring.NToSelect = self.__n_to_select
        state_storage_cfg.Ring.Node.extend(selected_ids)

    def __generate_log_txt(self):
        self.__proto_configs["log.txt"] = config_pb2.TLogConfig()
        utils.apply_config_changes(
            self.__proto_configs["log.txt"],
            self.__cluster_details.log_config,
        )

    def __generate_names_txt(self):
        self.__proto_configs["names.txt"] = config_pb2.TStaticNameserviceConfig()

        for host in self.__cluster_details.hosts:
            node = self.names_txt.Node.add(
                NodeId=host.node_id,
                Port=host.ic_port,
                Host=host.hostname,
                InterconnectHost=host.hostname,
            )

            if not self._skip_location:
                if self.__cluster_details.use_walle:
                    node.WalleLocation.DataCenter = host.datacenter
                    node.WalleLocation.Rack = host.rack
                    node.WalleLocation.Body = int(host.body)
                else:
                    node.Location.DataCenter = host.datacenter
                    node.Location.Rack = host.rack
                    node.Location.Body = int(host.body)

        if self.__cluster_details.use_cluster_uuid:
            accepted_uuids = self.__cluster_details.accepted_cluster_uuids
            cluster_uuid = self.__cluster_details.cluster_uuid
            cluster_uuid = "ydb:{}".format(utils.uuid()) if cluster_uuid is None else cluster_uuid
            self.names_txt.ClusterUUID = cluster_uuid
            self.names_txt.AcceptUUID.append(cluster_uuid)
            self.names_txt.AcceptUUID.extend(accepted_uuids)

    def __generate_sys_txt(self):
        self.__proto_configs["sys.txt"] = config_pb2.TActorSystemConfig()
        if self.__cluster_details.sys_preset_name is not None:
            utils.read_from_resource(
                self.__proto_configs["sys.txt"],
                "sys",
                self.__cluster_details.sys_preset_name,
            )
        elif self.__cluster_details.use_auto_config:
            sys_config = self.__proto_configs["sys.txt"]
            sys_config.UseAutoConfig = True
            sys_config.NodeType = sys_config.ENodeType.Value("STORAGE")
            sys_config.CpuCount = self.__cluster_details.static_cpu_count
        elif self.__cluster_details.sys.get("use_auto_config", False):
            sys_config = self.__proto_configs["sys.txt"]
            sys_config.UseAutoConfig = True
            if "node_type" in self.__cluster_details.sys:
                sys_config.NodeType = types.NodeType.from_string(self.__cluster_details.sys["node_type"])
            if "cpu_count" in self.__cluster_details.sys:
                sys_config.CpuCount = self.__cluster_details.sys["cpu_count"]
        else:
            self.__generate_sys_txt_advanced()

    def __generate_tracing_txt(self):
        def get_selectors(selectors):
            selectors_pb = config_pb2.TTracingConfig.TSelectors()

            request_type = selectors["request_type"]
            if request_type is not None:
                selectors_pb.RequestType = request_type

            return selectors_pb

        def get_sampling_scope(sampling):
            sampling_scope_pb = config_pb2.TTracingConfig.TSamplingRule()
            selectors = sampling.get("scope")
            if selectors is not None:
                sampling_scope_pb.Scope.CopyFrom(get_selectors(selectors))
            sampling_scope_pb.Fraction = sampling['fraction']
            sampling_scope_pb.Level = sampling['level']
            sampling_scope_pb.MaxTracesPerMinute = sampling['max_traces_per_minute']
            sampling_scope_pb.MaxTracesBurst = sampling.get('max_traces_burst', 0)
            return sampling_scope_pb

        def get_external_throttling(throttling):
            throttling_scope_pb = config_pb2.TTracingConfig.TExternalThrottlingRule()
            selectors = throttling.get("scope")
            if selectors is not None:
                throttling_scope_pb.Scope.CopyFrom(get_selectors(selectors))
            throttling_scope_pb.MaxTracesPerMinute = throttling['max_traces_per_minute']
            throttling_scope_pb.MaxTracesBurst = throttling.get('max_traces_burst', 0)
            return throttling_scope_pb

        def get_auth_config(auth):
            auth_pb = config_pb2.TTracingConfig.TBackendConfig.TAuthConfig()
            tvm = auth.get("tvm")
            if tvm is not None:
                tvm_pb = auth_pb.Tvm

                if "host" in tvm:
                    tvm_pb.Host = tvm["host"]
                if "port" in tvm:
                    tvm_pb.Port = tvm["port"]
                tvm_pb.SelfTvmId = tvm["self_tvm_id"]
                tvm_pb.TracingTvmId = tvm["tracing_tvm_id"]
                if "disk_cache_dir" in tvm:
                    tvm_pb.DiskCacheDir = tvm["disk_cache_dir"]

                if "plain_text_secret" in tvm:
                    tvm_pb.PlainTextSecret = tvm["plain_text_secret"]
                elif "secret_file" in tvm:
                    tvm_pb.SecretFile = tvm["secret_file"]
                elif "secret_environment_variable" in tvm:
                    tvm_pb.SecretEnvironmentVariable = tvm["secret_environment_variable"]
            return auth_pb

        def get_opentelemetry(opentelemetry):
            opentelemetry_pb = config_pb2.TTracingConfig.TBackendConfig.TOpentelemetryBackend()

            opentelemetry_pb.CollectorUrl = opentelemetry["collector_url"]
            opentelemetry_pb.ServiceName = opentelemetry["service_name"]

            return opentelemetry_pb

        def get_backend(backend):
            backend_pb = config_pb2.TTracingConfig.TBackendConfig()

            auth = backend.get("auth_config")
            if auth is not None:
                backend_pb.AuthConfig.CopyFrom(get_auth_config(auth))

            opentelemetry = backend["opentelemetry"]
            if opentelemetry is not None:
                backend_pb.Opentelemetry.CopyFrom(get_opentelemetry(opentelemetry))

            return backend_pb

        def get_uploader(uploader):
            uploader_pb = config_pb2.TTracingConfig.TUploaderConfig()

            max_exported_spans_per_second = uploader.get("max_exported_spans_per_second")
            if max_exported_spans_per_second is not None:
                uploader_pb.MaxExportedSpansPerSecond = max_exported_spans_per_second

            max_spans_in_batch = uploader.get("max_spans_in_batch")
            if max_spans_in_batch is not None:
                uploader_pb.MaxSpansInBatch = max_spans_in_batch

            max_bytes_in_batch = uploader.get("max_bytes_in_batch")
            if max_bytes_in_batch is not None:
                uploader_pb.MaxBytesInBatch = max_bytes_in_batch

            max_batch_accumulation_milliseconds = uploader.get("max_batch_accumulation_milliseconds")
            if max_batch_accumulation_milliseconds is not None:
                uploader_pb.MaxBatchAccumulationMilliseconds = max_batch_accumulation_milliseconds

            span_export_timeout_seconds = uploader.get("span_export_timeout_seconds")
            if span_export_timeout_seconds is not None:
                uploader_pb.SpanExportTimeoutSeconds = span_export_timeout_seconds

            max_export_requests_inflight = uploader.get("max_export_requests_inflight")
            if max_export_requests_inflight is not None:
                uploader_pb.MaxExportRequestsInflight = max_export_requests_inflight

            return uploader_pb

        pb = config_pb2.TAppConfig()
        if self.__tracing:
            tracing_pb = pb.TracingConfig
            (
                backend,
                uploader,
                sampling,
                external_throttling
            ) = self.__tracing

            assert isinstance(sampling, list)
            assert isinstance(external_throttling, list)

            tracing_pb.Backend.CopyFrom(get_backend(backend))

            if uploader is not None:
                tracing_pb.Uploader.CopyFrom(get_uploader(uploader))

            for sampling_scope in sampling:
                tracing_pb.Sampling.append(get_sampling_scope(sampling_scope))

            for throttling_scope in external_throttling:
                tracing_pb.ExternalThrottling.append(get_external_throttling(throttling_scope))

        self.__proto_configs["tracing.txt"] = pb

    def __generate_sys_txt_advanced(self):
        sys_config = self.__proto_configs["sys.txt"]
        well_known_users = ("SysExecutor", "UserExecutor", "BatchExecutor", "IoExecutor")
        executors = [
            {
                "Type": sys_config.TExecutor.EType.Value("BASIC"),
                "Threads": 9,
                "SpinThreshold": 1,
                "Name": "System",
                "ExecutorUser": "SysExecutor",
            },
            {
                "Type": sys_config.TExecutor.EType.Value("BASIC"),
                "Threads": 16,
                "SpinThreshold": 1,
                "Name": "User",
                "ExecutorUser": "UserExecutor",
            },
            {
                "Type": sys_config.TExecutor.EType.Value("BASIC"),
                "Threads": 7,
                "SpinThreshold": 1,
                "Name": "Batch",
                "ExecutorUser": "BatchExecutor",
            },
            {
                "Type": sys_config.TExecutor.EType.Value("IO"),
                "Threads": 1,
                "Name": "IO",
                "ExecutorUser": "IoExecutor",
            },
            {
                "Type": sys_config.TExecutor.EType.Value("BASIC"),
                "Threads": 3,
                "SpinThreshold": 10,
                "Name": "IC",
                "TimePerMailboxMicroSecs": 100,
                "ExecutorUser": "Interconnect",
            },
        ]
        scheduler = {
            "Resolution": 64,
            "SpinThreshold": 0,
            "ProgressThreshold": 10000,
        }
        for executor_id, executor in enumerate(executors):
            short_name, executor_user = executor["Name"], executor["ExecutorUser"]
            del executor["ExecutorUser"]
            if executor_user in well_known_users:
                setattr(sys_config, executor_user, executor_id)
            else:
                sys_config.ServiceExecutor.add(ServiceName=executor_user, ExecutorId=executor_id)

            override_values = self.__cluster_details.executors.get(short_name.lower(), {})
            for key, override_value in override_values.items():
                actual = utils.capitalize_name(key)
                executor[actual] = override_value
            sys_config.Executor.add(**executor)

        for opt, default_value in scheduler.items():
            setattr(sys_config.Scheduler, opt, default_value)

        utils.apply_config_changes(
            sys_config.Scheduler,
            self.__cluster_details.schedulers,
        )

        self.__proto_configs["sys.txt"] = sys_config

    # KQP Stuff

    def __generate_kqp_txt(self):
        self.__proto_configs["kqp.txt"] = config_pb2.TKQPConfig()
        kqp_txt = self.__proto_configs["kqp.txt"]
        kqp_txt.Enable = self.__cluster_details.kqp_enable
        settings = self.__cluster_details.kqp_settings
        for name, value in settings.items():
            name = "_%s" % utils.capitalize_name(name)
            str_value = str(value)
            if str_value.lower() in ["true", "false"]:
                str_value = str_value.lower()
            kqp_txt.Settings.add(
                Name=name,
                Value=str_value,
            )

    @property
    def dynamic_server_common_args(self):
        if self.__cluster_details.use_new_style_kikimr_cfg:
            return dynamic_cfg_new_style(self._enable_cores)
        return kikimr_cfg_for_dynamic_slot(
            self._enable_cores, cert_params=self.__cluster_details.ic_cert_params
        )
