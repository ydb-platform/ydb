#!/usr/bin/env python
# -*- coding: utf-8 -*-
import collections
import copy
import os
from concurrent.futures import ThreadPoolExecutor

from ydb.tools.cfg import types, validation, walle

DEFAULT_LOG_LEVEL = types.LogLevels.NOTICE

DEFAULT_INTERCONNECT_PORT = 19001

DEFAULT_MONITOR_PORT = 8765

DEFAULT_KQP_ENABLE = True
DEFAULT_KQP_RESULT_ROWS_LIMIT = None

DEFAULT_FAIL_DOMAIN_TYPE = types.FailDomainType.Rack

DEFAULT_PDISK_TYPE = types.PDiskCategory.ROT

DEFAULT_BOX_ID = 1

# MOVE THESE DEFAULTS TO YDB CODE
VDISKS_DEFAULT_CONFIG = {
    "vdisk_kinds": [
        {"kind": "Log", "base_kind": "Default", "config": {"fresh_use_dreg": True, "allow_keep_flags": False}},
        {
            "kind": "LocalMode",
            "base_kind": "Default",
            "config": {"hull_comp_level0_max_ssts_at_once": 2, "hull_comp_sorted_parts_num": 1},
        },
    ]
}

GRPC_DEFAULT_CONFIG = {
    "port": 2135,
    "host": "[::]",
    "start_grpc_proxy": True,
    "grpc_memory_quota_bytes": 1073741824,
    "streaming_config": {"enable_output_streams": True},
    "keep_alive_enable": True,
    "keep_alive_idle_timeout_trigger_sec": 90,
    "keep_alive_max_probe_count": 3,
    "keep_alive_probe_interval_sec": 10,
}

DYNAMIC_NAME_SERVICE = {"max_static_node_id": 50000}

IC_DEFAULT_CONFIG = {
    "start_tcp": True,
}


def merge_with_default(dft, override):
    result = copy.deepcopy(dft)
    result.update(override)
    return result


class KiKiMRDrive(object):
    def __init__(self, type, path, shared_with_os=False, expected_slot_count=None, kind=None):
        self.type = type
        self.path = path
        self.shared_with_os = shared_with_os
        self.expected_slot_count = expected_slot_count
        self.kind = kind

    def __eq__(self, other):
        return (
            self.type == other.type
            and self.path == other.path
            and self.shared_with_os == other.shared_with_os
            and self.expected_slot_count == other.expected_slot_count
            and self.kind == other.kind
        )

    def __hash__(self):
        return hash("\0".join(map(str, (self.type, self.path, self.shared_with_os, self.expected_slot_count, self.kind))))


Domain = collections.namedtuple(
    "_Domain",
    [
        "domain_name",
        "domain_id",
        "plan_resolution",
        "mediators",
        "coordinators",
        "allocators",
        "dynamic_slots",
        "storage_pool_kinds",
        "storage_pools",
        "tenants",
        "console_initializers",
        "bind_slots_to_numa_nodes",
        "config_cookie",
    ],
)

KiKiMRHost = collections.namedtuple(
    "_KiKiMRHost", ["hostname", "node_id", "drives", "ic_port", "body", "datacenter", "rack", "host_config_id"]
)

DEFAULT_PLAN_RESOLUTION = 10
DEFAULT_DOMAIN = {"domain_name": "Root", "domain_id": 1, "plan_resolution": DEFAULT_PLAN_RESOLUTION}

DEFAULT_RESOURCE_BROKER_CPU_LIMIT = 20
DEFAULT_RESOURCE_BROKER_MEMORY_LIMIT = 16 * (1024**3)


class StoragePool(object):
    def __init__(
        self,
        box_id,
        erasure,
        num_groups,
        filter_properties,
        fail_domain_type,
        kind,
        name,
        vdisk_kind,
        encryption_mode,
        generation=0,
    ):
        self._dict_items = {}
        self.box_id = box_id
        self.erasure = erasure
        self.num_groups = num_groups
        self.filter_properties = filter_properties
        self.fail_domain_type = fail_domain_type
        self.kind = kind
        self.vdisk_kind = vdisk_kind
        self.name = name
        self.encryption_mode = encryption_mode
        self.generation = generation

    def __setattr__(self, key, value):
        super(StoragePool, self).__setattr__(key, value)

        if key.startswith("_"):
            return

        if key not in self._dict_items or self._dict_items[key] != value:
            self._dict_items[key] = value

    def to_dict(self):
        return self._dict_items


class StoragePoolKind(object):
    def __init__(self, box_id, erasure, filter_properties, fail_domain_type, kind, vdisk_kind, encryption_mode):
        self._dict_items = {}
        self.box_id = box_id
        self.erasure = erasure
        self.filter_properties = filter_properties
        self.fail_domain_type = fail_domain_type
        self.kind = kind
        self.vdisk_kind = vdisk_kind
        self.encryption_mode = encryption_mode

    def __setattr__(self, key, value):
        super(StoragePoolKind, self).__setattr__(key, value)

        if key.startswith("_"):
            return

        if key not in self._dict_items or self._dict_items[key] != value:
            self._dict_items[key] = value

    def to_dict(self):
        return self._dict_items


Subdomain = collections.namedtuple(
    "_Subdomain",
    [
        "domain",
        "name",
        "plan_resolution",
        "mediators",
        "coordinators",
        "storage_pools",
    ],
)

DynamicSlot = collections.namedtuple(
    "_DynamicSlot",
    [
        "domain",
        "id",
    ],
)

Profile = collections.namedtuple(
    "_Profile",
    [
        "channels",
    ],
)
Channel = collections.namedtuple("_Channel", ["erasure", "pdisk_type", "vdisk_kind", "storage_pool_kind"])


class StorageUnit(object):
    def __init__(self, kind, count):
        self.kind = kind
        self.count = count


class ComputeUnit(object):
    def __init__(self, count, kind, zone):
        self.kind = kind
        self.zone = zone
        self.count = count


class Tenant(object):
    def __init__(self, name, storage_units, compute_units=None, overridden_configs=None, shared=False, plan_resolution=None):
        self.name = name
        self.overridden_configs = overridden_configs
        self.storage_units = tuple(StorageUnit(**storage_unit_template) for storage_unit_template in storage_units)
        self.compute_units = tuple()
        if compute_units:
            self.compute_units = tuple(ComputeUnit(**compute_unit_template) for compute_unit_template in compute_units)
        self.shared = shared
        self.plan_resolution = plan_resolution


class HostConfig(object):
    def __init__(self, host_config_id, drives, generation=0):
        self.drives = drives
        self.host_config_id = host_config_id
        self.generation = generation


class _ClusterDescription(dict):
    @classmethod
    def from_template(cls, template):
        return cls(**template)

    def set_validator(self, validator):
        self.__validator = validator

    def validate(self):
        if not self.__validator:
            return
        return self.__validator.validate(self)


def get_full_tenant_name(domain, tenant_name):
    return os.path.join("/", domain, tenant_name)


def get_tenant_domain(tenant):
    for part in tenant.split("/"):
        if part:
            return part
    raise RuntimeError()


def normalize_domain(domain_name):
    return get_tenant_domain(domain_name)


class ClusterDetailsProvider(object):
    def __init__(self, template, walle_provider, validator=None, database=None):
        if not validator:
            validator = validation.default_validator()

        self.__validator = validator

        self.__cluster_description = _ClusterDescription.from_template(template)
        self.__cluster_description.set_validator(self.__validator)
        self.__cluster_description.validate()

        if database is not None:
            self.__cluster_description = self.get_subjective_description(self.__cluster_description, database, self.__validator)

        self._use_walle = self.__cluster_description.get("use_walle", True)
        if not walle_provider:
            walle_provider = walle.NopHostsInformationProvider()
        self._walle = walle_provider
        self.__translated_storage_pools_deprecated = None
        self.__translated_hosts = None
        self.__racks = {}
        self.__bodies = {}
        self.__dcs = {}
        self.use_new_style_kikimr_cfg = self.__cluster_description.get("use_new_style_kikimr_cfg", False)
        self.need_generate_app_config = self.__cluster_description.get("need_generate_app_config", False)
        self.need_txt_files = self.__cluster_description.get("need_txt_files", True)
        self.use_auto_config = self.__cluster_description.get("use_auto_config", False)
        self.static_cpu_count = self.__cluster_description.get("static_cpu_count", 20)
        self.dynamic_cpu_count = self.__cluster_description.get("dynamic_cpu_count", 8)
        self.force_io_pool_threads = self.__cluster_description.get("force_io_pool_threads", None)
        self.blob_storage_config = self.__cluster_description.get("blob_storage_config")
        self.pdisk_key_config = self.__cluster_description.get("pdisk_key_config", {})
        if not self.need_txt_files and not self.use_new_style_kikimr_cfg:
            assert "cannot remove txt files without new style kikimr cfg!"

        self._hosts = None
        self._thread_pool = ThreadPoolExecutor(max_workers=8)

    @staticmethod
    def get_subjective_description(objective_description, tenant, validator):
        overridden_configurations = {}
        domain_of_tenant = get_tenant_domain(tenant)
        for domain_template in objective_description.get("domains", []):
            if normalize_domain(domain_template.get("domain_name")) != domain_of_tenant:
                continue

            all_domain_tenants = domain_template.get("databases", [])
            for current_tenant in all_domain_tenants:
                if get_full_tenant_name(domain_of_tenant, current_tenant.get("name")) != tenant:
                    continue

                overridden_configurations = current_tenant.get("overridden_configs", {})
                for service in (
                    "nfs",
                    "nfs_control",
                    "nbs",
                    "nbs_control",
                ):
                    if service in overridden_configurations:
                        overridden_configurations[service]["domain"] = domain_template.get("domain_name")
                        overridden_configurations[service]["subdomain"] = current_tenant.get("name")

        subjective_description = _ClusterDescription(dict(objective_description))
        subjective_description.update(overridden_configurations)
        subjective_description.set_validator(validator)
        subjective_description.validate()
        return subjective_description

    @property
    def storage_config_generation(self):
        return self.__cluster_description.get("storage_config_generation", 0)

    @property
    def use_walle(self):
        return self._use_walle

    @property
    def security_settings(self):
        return self.__cluster_description.get("security_settings", {})

    @property
    def forbid_implicit_storage_pools(self):
        return self.__cluster_description.get("forbid_implicit_storage_pools", False)

    def _get_datacenter(self, host_description):
        if host_description.get("datacenter") is not None:
            return str(host_description.get("datacenter"))
        return str(self._walle.get_datacenter(host_description["name"]))

    def _get_rack(self, host_description):
        if host_description.get("rack") is not None:
            return str(host_description.get("rack"))
        return str(self._walle.get_rack(host_description["name"]))

    def _get_body(self, host_description):
        if host_description.get("body") is not None:
            return str(host_description.get("body"))
        return str(self._walle.get_body(host_description["name"]))

    def _collect_drives_info(self, host_description):
        host_config_id = host_description.get("host_config_id", None)
        drives = host_description.get("drives", [])
        drives = tuple(KiKiMRDrive(**drive) for drive in drives)
        if host_config_id is not None:
            host_config = None
            for candidate in self.host_configs:
                if candidate.host_config_id == host_config_id:
                    host_config = candidate

            assert host_config is not None, "Unknown host config %d" % host_config_id
            drives = host_config.drives
        return drives

    def __collect_host_info(self, node_id, host_description):
        return KiKiMRHost(
            hostname=host_description["name"],
            node_id=host_description.get("node_id", node_id),
            drives=self._collect_drives_info(host_description),
            ic_port=host_description.get("ic_port", DEFAULT_INTERCONNECT_PORT),
            body=self._get_body(host_description),
            datacenter=self._get_datacenter(host_description),
            rack=self._get_rack(host_description),
            host_config_id=host_description.get("host_config_id", None),
        )

    @property
    def use_fixed_tablet_types(self):
        return self.__cluster_description.get("use_fixed_tablet_types", False)

    @property
    def hosts(self):
        if self._hosts is not None:
            return self._hosts
        futures = []
        for node_id, host_description in enumerate(self.__cluster_description.get("hosts"), 1):
            futures.append(self._thread_pool.submit(self.__collect_host_info, node_id, host_description))

        r = []
        for f in futures:
            r.append(f.result())

        if self._hosts is None:
            self._hosts = r
        return self._hosts

    @property
    def system_tablets_node_ids(self):
        return [host.node_id for host in self.static_bs_group_hosts]

    @property
    def state_storages(self):
        return self.__cluster_description.get("state_storages", [])

    @property
    def state_storage_node_ids(self):
        node_ids = self.__cluster_description.get("state_storage", {}).get("node_ids", [])
        if node_ids:
            return node_ids

        state_storage_node_set = set(self.__cluster_description.get("state_storage", {}).get("node_set", []))
        if len(state_storage_node_set) < 1:
            return [host.node_id for host in self.static_bs_group_hosts]
        node_ids = []
        for host in self.hosts:
            if host.hostname in state_storage_node_set:
                node_ids.append(host.node_id)
        return node_ids

    @property
    def allow_incorrect_state_storage(self):
        return self.__cluster_description.get("state_storage", {}).get("allow_incorrect", False)

    @property
    def static_bs_group_hosts(self):
        static_bs_group_filter = self.__cluster_description.get("static_bs_group_hosts", [])
        matched = []
        for host in self.hosts:
            if host.hostname in static_bs_group_filter or len(static_bs_group_filter) == 0:
                matched.append(host)
        return matched

    @property
    def node_ids(self):
        return [host.node_id for host in self.hosts]

    @property
    def monitor_port(self):
        return DEFAULT_MONITOR_PORT

    @property
    def monitor_address(self):
        return self.__cluster_description.get("monitoring_address", "")

    @property
    def use_cluster_uuid(self):
        return self.__cluster_description.get("use_cluster_uuid", True)

    @property
    def cluster_uuid(self):
        return self.__cluster_description.get("cluster_uuid", None)

    @property
    def accepted_cluster_uuids(self):
        return self.__cluster_description.get("accepted_cluster_uuids", [])

    @property
    def host_configs(self):
        converted_host_configs = []
        for host_config in self.__cluster_description.get("host_configs", []):
            host_config_drives = host_config.get("drives", [])
            converted_host_configs.append(
                HostConfig(
                    host_config_id=host_config["host_config_id"],
                    generation=host_config.get("generation", 0),
                    drives=tuple(KiKiMRDrive(**drive) for drive in host_config_drives),
                )
            )
        return converted_host_configs

    @property
    def tablet_profiles(self):
        all_profiles = []
        storage_pool_kinds = []
        for domain in self.domains:
            for storage_pool_kind in domain.storage_pool_kinds.values():
                storage_pool_kinds.append(storage_pool_kind)

        for profile in self.__cluster_description.get("profiles", []):
            profile_channels = []

            for channel in profile.get("channels", []):
                erasure = channel.get("erasure", str(self.static_erasure))
                pdisk_type = channel.get("pdisk_type")
                pdisk_type = None if pdisk_type is None else types.PDiskCategory.from_string(pdisk_type)
                channel_storage_pool_kind = channel.get("storage_pool_kind", None)
                if channel_storage_pool_kind is not None and pdisk_type is None:
                    pdisk_type = None
                    for storage_pool_kind in storage_pool_kinds:
                        if channel_storage_pool_kind == storage_pool_kind.kind:
                            f_properties = storage_pool_kind.filter_properties
                            pdisk_type = types.PDiskCategory.from_string(f_properties["type"])

                    assert pdisk_type is not None

                profile_channels.append(
                    Channel(
                        erasure=types.Erasure.from_string(erasure),
                        pdisk_type=pdisk_type,
                        vdisk_kind=channel.get("vdisk_kind", "Default"),
                        storage_pool_kind=channel_storage_pool_kind,
                    )
                )
            all_profiles.append(Profile(channels=tuple(profile_channels)))
        return tuple(all_profiles)

    # Domains Stuff

    @staticmethod
    def __storage_pool_kind(storage_description):
        return StoragePoolKind(
            kind=storage_description.get("kind"),
            box_id=storage_description.get("box_id", DEFAULT_BOX_ID),
            erasure=storage_description.get("erasure"),
            filter_properties=storage_description.get("filter_properties", {}),
            fail_domain_type=storage_description.get("fail_domain_type", DEFAULT_FAIL_DOMAIN_TYPE),
            vdisk_kind=storage_description.get("vdisk_kind", "Default"),
            encryption_mode=storage_description.get("encryption_mode", 0),
        )

    @property
    def __coordinators_count_optimal(self):
        return min(5, max(1, len(self.hosts) / 4 + 1))

    @property
    def __mediators_count_optimal(self):
        return min(5, max(1, len(self.hosts) / 4 + 1))

    @property
    def __allocators_count_optimal(self):
        return min(5, max(1, len(self.hosts) / 4 + 1))

    @property
    def domains(self):
        cl_desc_domains = self.__cluster_description.get("domains", [DEFAULT_DOMAIN])
        domains = []
        for domain_id, domain in enumerate(cl_desc_domains, 1):
            domain_name = domain.get("domain_name")

            storage_pool_kinds = {
                pool_kind.get("kind"): self.__storage_pool_kind(pool_kind) for pool_kind in domain.get("storage_pool_kinds", [])
            }
            assert len(set(storage_pool_kinds.keys())) == len(
                storage_pool_kinds.keys()
            ), "required unique kind value in storage_pool_kinds items"

            storage_pools = [
                self.__storage_pool(storage_pool_kinds, pool_instance, domain_name) for pool_instance in domain.get("storage_pools", [])
            ]

            domains.append(
                Domain(
                    domain_name=domain_name,
                    domain_id=domain.get("domain_id", domain_id),
                    mediators=domain.get("mediators", self.__coordinators_count_optimal),
                    coordinators=domain.get("coordinators", self.__mediators_count_optimal),
                    allocators=domain.get("allocators", self.__allocators_count_optimal),
                    plan_resolution=domain.get("plan_resolution", DEFAULT_PLAN_RESOLUTION),
                    dynamic_slots=domain.get("dynamic_slots", 0),
                    storage_pool_kinds=storage_pool_kinds,
                    storage_pools=storage_pools,
                    tenants=self._get_domain_tenants(domain),
                    console_initializers=domain.get("console_initializers", []),
                    bind_slots_to_numa_nodes=domain.get("bind_slots_to_numa_nodes", False),
                    config_cookie=domain.get("config_cookie", ""),
                )
            )
        return domains

    @staticmethod
    def _get_domain_tenants(domain_description):
        tenants = domain_description.get("databases", [])
        prepared_tenants = []
        for tenant in tenants:
            prepared_tenants.append(Tenant(**tenant))
        return prepared_tenants

    # Static BlobStorage Group Stuff

    @property
    def static_pdisk_type(self):
        return self.__cluster_description.get("static_pdisk_type", DEFAULT_PDISK_TYPE)

    @property
    def nw_cache_file_path(self):
        return self.__cluster_description.get("nw_cache_file_path", None)

    @property
    def fail_domain_type(self):
        return types.FailDomainType.from_string(str(self.__cluster_description.get("fail_domain_type", DEFAULT_FAIL_DOMAIN_TYPE)))

    @property
    def min_fail_domains(self):
        return self.static_erasure.min_fail_domains

    @property
    def static_erasure(self):
        return types.Erasure.from_string(self.__cluster_description["static_erasure"])

    # Auth txt
    @property
    def use_auth(self):
        return self.__cluster_description.get("auth", {})

    # Log Stuff
    @property
    def log_config(self):
        log_config = copy.deepcopy(self.__cluster_description.get("log", {}))

        if "entries" in log_config:
            services = log_config.get("entries", [])
            entry_list = []
            for service in list(sorted(services, key=lambda x: x["name"])):
                service["component"] = service["name"]
                del service["name"]
                entry_list.append(service)

            log_config["entry"] = entry_list
            del log_config["entries"]

        default_level = self.default_log_level
        if "default" in log_config:
            del log_config["default"]

        log_config["default_level"] = default_level

        return log_config

    @property
    def default_log_level(self):
        return self.__cluster_description.get("log", {}).get("default", int(DEFAULT_LOG_LEVEL))

    @property
    def grpc_config(self):
        return merge_with_default(GRPC_DEFAULT_CONFIG, self.__cluster_description.get("grpc", {}))

    @property
    def dynamicnameservice_config(self):
        return merge_with_default(DYNAMIC_NAME_SERVICE, self.__cluster_description.get("dynamicnameservice", {}))

    @property
    def grpc_port(self):
        return self.grpc_config.get("port")

    @property
    def vdisk_config(self):
        return merge_with_default(VDISKS_DEFAULT_CONFIG, self.__cluster_description.get("vdisks", {}))

    # KQP Stuff

    @property
    def kqp_settings(self):
        settings = copy.deepcopy(self.__cluster_description.get("kqp", {}))
        if "enable" in settings:
            del settings["enable"]
        return settings

    @property
    def kqp_enable(self):
        return self.__cluster_description.get("kqp", {}).get("enable", DEFAULT_KQP_ENABLE)

    @property
    def kqp_result_rows_limit(self):
        return self.__cluster_description.get("kqp", {}).get("result_rows_limit", DEFAULT_KQP_RESULT_ROWS_LIMIT)

    @property
    def ic_config(self):
        ic_cfg = merge_with_default(IC_DEFAULT_CONFIG, self.__cluster_description.get("ic", {}))
        # drop if not empty
        ic_cfg.pop("ca_path", "")
        ic_cfg.pop("cert_path", "")
        ic_cfg.pop("key_path", "")
        return ic_cfg

    @property
    def ic_cert_params(self):
        return tuple(self.__cluster_description.get("ic", {}).get(k) for k in ["ca_path", "cert_path", "key_path"])

    # PQ stuff
    @property
    def pq_config(self):
        cfg = copy.deepcopy(self.__cluster_description.get("pq", {"enabled": True}))
        cfg.pop("shared_cache_size_mb", "")
        return cfg

    # Shared Cache
    @property
    def shared_cache_memory_limit(self):
        return self.__cluster_description.get("shared_cache", {}).get("memory_limit", None)

    @property
    def pq_shared_cache_size(self):
        return self.__cluster_description.get("pq", {}).get("shared_cache_size_mb", None)

    # Sys Stuff
    @property
    def sys(self):
        return self.__cluster_description.get("sys", {})

    @property
    def sys_preset_name(self):
        return self.sys.get("preset_name", None)

    @property
    def executors(self):
        return self.__cluster_description.get("sys", {}).get("executors", {})

    @property
    def schedulers(self):
        return self.__cluster_description.get("sys", {}).get("scheduler", {})

    # DynamicBlobStorage Stuff

    def __storage_pool_kinds(self, domain_name):
        for domain in self.domains:
            if domain.domain_name == domain_name:
                return domain.storage_pool_kinds
        assert False, "domain with name {0} is not found".format(domain_name)

    def __storage_pools_deprecated(self):
        storage_pools = []
        for storage_pool in self.__cluster_description.get("storage_pools", []):
            storage_pools.append(
                StoragePool(
                    name=storage_pool.get("name", None),
                    box_id=DEFAULT_BOX_ID,
                    erasure=storage_pool.get("erasure"),
                    num_groups=storage_pool.get("num_groups"),
                    filter_properties=storage_pool.get("filter_properties", {}),
                    fail_domain_type=storage_pool.get("fail_domain_type", DEFAULT_FAIL_DOMAIN_TYPE),
                    kind=storage_pool.get("kind", None),
                    vdisk_kind=storage_pool.get("vdisk_kind", "Default"),
                    encryption_mode=storage_pool.get("encryption_mode", 0),
                    generation=storage_pool.get("generation", 0),
                )
            )

        return storage_pools

    @property
    def storage_pools_deprecated(self):
        if self.__translated_storage_pools_deprecated is None:
            self.__translated_storage_pools_deprecated = self.__storage_pools_deprecated()
        return self.__translated_storage_pools_deprecated

    def __storage_pool(self, storage_pool_kinds, storage_pool_instance, domain_name):
        kind_name = storage_pool_instance.get("kind")

        assert kind_name in storage_pool_kinds, "databse storage pools should use pool kinds from storage_pool_kinds section"

        storage_pool_name = "/{domain}:{kind}".format(domain=domain_name, kind=kind_name)

        return StoragePool(
            name=storage_pool_name,
            num_groups=storage_pool_instance.get("num_groups"),
            generation=storage_pool_instance.get("generation", 0),
            **storage_pool_kinds[kind_name].to_dict()
        )

    @property
    def dynamic_slots(self):
        slots = []
        for domain_description in self.__cluster_description.get("domains", []):
            for slot_id in range(1, int(domain_description.get("dynamic_slots", 0) + 1)):
                slots.append(DynamicSlot(domain=domain_description.get("domain_name"), id=slot_id))
        return slots

    @property
    def system_tablets_config(self):
        return self.__cluster_description.get("system_tablets", {})

    # NBS/NFS Options
    @property
    def nbs_enable(self):
        return self.__cluster_description.get("nbs", {}).get("enable", False)

    @property
    def nfs_enable(self):
        return self.__cluster_description.get("nfs", {}).get("enable", False)

    def get_service(self, service_name):
        return self.__cluster_description.get(service_name, {})

    @property
    def static_group_hosts_migration(self):
        return self.__cluster_description.get("static_group_hosts_migration", [])
