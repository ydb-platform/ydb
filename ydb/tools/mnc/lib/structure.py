import yaml


class Drive:
    def __init__(self, path: str, type: str, expected_slot_count: int):
        self.path = path
        self.type = type
        self.expected_slot_count = expected_slot_count

    def convert_to_dict(self):
        return {
            'path': self.path,
            'type': self.type,
            'expected_slot_count': self.expected_slot_count,
        }


class Host:
    def __init__(
        self, fqdn: str, datacenter: str, rack_id: str, body_id: str, ic_port: int = None, host_config_id: int = None
    ):
        self.fqdn = fqdn
        self.name = fqdn
        self.rack = rack_id
        self.body = body_id
        self.datacenter = datacenter
        self.port = ic_port
        self.ic_port = ic_port
        self.host_config_id = host_config_id

    def convert_to_dict(self):
        names = ['name', 'datacenter', 'rack', 'body', 'port', 'ic_port', 'host_config_id']
        dt = {key: self.__dict__[key] for key in names if self.__dict__[key] is not None}
        return dt


class HostConfig:
    def __init__(self, host_config_id: int, drives: list[str] = None):
        self.host_config_id = host_config_id
        self.drives = drives

    def add_drive(self, path: str, type: str, expected_slot_count: str):
        self.drives.append(Drive(path, type, expected_slot_count))

    def convert_to_dict(self):
        return {
            'host_config_id': self.host_config_id,
            'drive': [drive.convert_to_dict() for drive in self.drives],
        }


class Rack:
    BODY_NEXT_ID = 0

    def __init__(self, structure, datacenter: str, rack_id: str):
        self.datacenter = datacenter
        self.rack_id = rack_id
        self.structure = structure

    def new_host(self, fqdn: str, ic_port: int = None, drives: list = None):
        host = Host(fqdn, self.datacenter, self.rack_id, Rack.BODY_NEXT_ID, ic_port, drives)
        Rack.BODY_NEXT_ID += 1
        self.structure.hosts.append(host)
        return host


class DataCenter:
    def __init__(self, structure, datacenter: str, idx: int):
        self.datacenter = datacenter
        self.rack_id = 0
        self.structure = structure
        self.idx = idx

    def new_rack(self):
        rack = Rack(self.structure, self.datacenter, self.rack_id + 1000000 * self.idx)
        self.rack_id += 1
        return rack


class StoragePool:
    def __init__(self, storage_pool_id: int, num_groups: int, type: str, erasure: str, name: str = None):
        self.storage_pool_id = storage_pool_id
        self.num_groups = num_groups
        self.type = type
        self.erasure = erasure
        self.name = name

    def convert_to_dict(self):
        return {
            'storage_pool_id': self.storage_pool_id,
            'num_groups': self.num_groups,
            'erasure': self.erasure,
            'name': self.name,
            'filter_properties': {
                'type': self.type,
            }
        }


class StoragePoolKind:
    def __init__(self, kind: str, erasure: str, type: str):
        self.kind = kind
        self.erasure = erasure
        self.type = type

    def convert_to_dict(self):
        return {
            'kind': self.kind,
            'erasure': self.erasure,
            'filter_properties': {
                'type': self.type,
            }
        }


class StorageUnit:
    def __init__(self, kind: str, count: int):
        self.kind = kind
        self.count = count

    def convert_to_dict(self):
        return {
            'kind': self.kind,
            'count': self.count,
        }


class ComputeUnit:
    def __init__(self, kind: str, zone: str, count: int):
        self.kind = kind
        self.zone = zone
        self.count = count

    def convert_to_dict(self):
        return {
            'kind': self.kind,
            'zone': self.zone,
            'count': self.count,
        }


class Database:
    def __init__(self, name: str):
        self.name = name
        self.storage_units = []
        self.compute_units = []
        self.overridden_configs = dict()

    def add_storage_unit(self, kind: str, count: int):
        for su in self.storage_units:
            if su.kind == kind:
                su.count += count
                return
        self.storage_units.append(StorageUnit(kind, count))

    def add_compute_unit(self, kind: str, count: int, zone: str = "any"):
        for cu in self.compute_units:
            if cu.kind == kind and cu.zone == zone:
                cu.count += count
                return
        self.compute_units.append(ComputeUnit(kind, zone, count))

    def set_overridden_configs(self, overridden_configs):
        self.overridden_configs = overridden_configs

    def get_overridden_configs_dict(self):
        if self.name == 'NBS':
            self.overridden_configs['nbs'] = {
                'enable': True,
                'new_config_generator_enabled': True,
            }
            self.overridden_configs['nbs_control'] = {
                'enable': True,
                'new_config_generator_enabled': True,
            }
        if self.overridden_configs:
            return {'overridden_configs': self.overridden_configs}
        else:
            return {}

    def convert_to_dict(self):
        return {
            'name': self.name,
            'storage_units': [su.convert_to_dict() for su in self.storage_units],
            'compute_units': [cu.convert_to_dict() for cu in self.compute_units],
            **self.get_overridden_configs_dict(),
        }


class Domain:
    def __init__(self, domain_id: int, name: str):
        self.domain_id = domain_id
        self.name = name
        self.storage_pool_kinds = []
        self.databases = []

    def add_storage_pool_kind(self, kind: str, erasure: str, type: str):
        for spk in self.storage_pool_kinds:
            assert spk.kind != kind
        self.storage_pool_kinds.append(StoragePoolKind(kind, erasure, type))

    def new_database(self, database_name: str):
        for db in self.databases:
            assert db.name != database_name
        self.databases.append(Database(database_name))
        return self.databases[-1]

    def convert_to_dict(self):
        dynamic_slot_count = 0
        for db in self.databases:
            for cu in db.compute_units:
                if cu.kind == "slot":
                    dynamic_slot_count += cu.count
        return {
            'domain_name': self.name,
            'domain_id': self.domain_id,
            'dynamic_slots': dynamic_slot_count,
            'storage_pool_kinds': [spk.convert_to_dict() for spk in self.storage_pool_kinds],
            'databases': [db.convert_to_dict() for db in self.databases],
        }


log_levels_map = {
    'trace': 8,
    'debug': 7,
    'info': 6,
    'notice': 5,
    'warn': 4,
    'error': 3,
    'crit': 2,
    'alert': 1,
    'emerg': 0,
}


class Cluster:
    def __init__(self, static_erasure: str, static_pdisk_type: str):
        self.static_erasure = static_erasure
        self.static_pdisk_type = static_pdisk_type
        self.hosts = []
        self.storage_pools = []
        self.domains = []
        self.storage_pool_id = 0
        self.explicit_node_ids = []
        self.dc_idx = 0
        self.domain_idx = 0
        self.global_log_level = 5
        self.log_entries = {}
        self.node_warden_cache_file_path = None
        self.fake_secret_path = None
        self.overridden_configs = dict()
        self.host_configs = []

    def new_datacenter(self, name: str):
        self.dc_idx += 1
        return DataCenter(self, name, self.dc_idx)

    def new_domain(self, name: str):
        self.domain_idx += 1
        self.domains.append(Domain(self.domain_idx, name))
        return self.domains[-1]

    def add_host_config(self, host_config_id: int, drives: list[str] = None):
        self.host_configs.append(HostConfig(host_config_id, drives))

    def add_storage_pool(self, name: str, num_groups: int, type: str, erasure: str):
        self.storage_pool_id += 1
        self.storage_pools.append(StoragePool(self.storage_pool_id, num_groups, type, erasure, name))

    def set_nodes_for_system_tablets(self, nodes: list[int]):
        self.explicit_node_ids = nodes

    def set_global_log_level(self, level: str):
        self.global_log_level = log_levels_map[level]

    def set_entry_log_level(self, entry: str, level: str):
        self.log_entries[entry] = log_levels_map[level]

    def set_node_warden_cache_file_path(self, path: str):
        self.node_warden_cache_file_path = path

    def set_fake_secret_path(self, path: str):
        self.fake_secret_path = path

    def set_overridden_configs(self, overridden_configs):
        self.overridden_configs = overridden_configs

    def get_overridden_configs_dict(self):
        return self.overridden_configs

    def get_log_dict(self):
        return {
            'log': {
                'default_level': self.global_log_level,
                'entry': [{'component': name, 'level': level} for name, level in self.log_entries.items()]
            }
        }

    def get_host_configs_dict(self):
        return {
            'host_configs': [host_config.convert_to_dict() for host_config in self.host_configs]
        }

    def get_system_tables_dict(self):
        if not self.explicit_node_ids:
            return {}
        return {
            'system_tablets': {
                'flat_hive': {'explicit_node_ids': self.explicit_node_ids},
                'flat_bs_controller': {'explicit_node_ids': self.explicit_node_ids},
                'flat_tx_coordinator': {'explicit_node_ids': self.explicit_node_ids},
                'flat_schemeshard': {'explicit_node_ids': self.explicit_node_ids},
                'tx_mediator': {'explicit_node_ids': self.explicit_node_ids},
                'tx_allocator': {'explicit_node_ids': self.explicit_node_ids},
                'cms': {'explicit_node_ids': self.explicit_node_ids},
                'node_broker': {'explicit_node_ids': self.explicit_node_ids},
                'tenant_slot_broker': {'explicit_node_ids': self.explicit_node_ids},
                'console': {'explicit_node_ids': self.explicit_node_ids},
            }
        }

    def get_static_group_dict(self):
        dt = {
            'use_new_style_ydb_cfg': True,
            'static_erasure': self.static_erasure,
            'static_pdisk_type': self.static_pdisk_type,
            'state_storage': {
                'node_ids': self.explicit_node_ids,
            }
        }
        if self.node_warden_cache_file_path:
            dt['nw_cache_file_path'] = {self.node_warden_cache_file_path}
        return dt

    def get_drives_dict(self):
        if not self.hosts:
            return {}
        return {
            'hosts': [host.convert_to_dict() for host in self.hosts]
        }

    def get_storage_pools_dict(self):
        if not self.storage_pools:
            return {}
        return {
            'storage_pools': [sp.convert_to_dict() for sp in self.storage_pools]
        }

    def get_domains_dict(self):
        if not self.domains:
            return {}
        return {
            'domains': [domain.convert_to_dict() for domain in self.domains]
        }

    def get_pdisk_key_dict(self):
        if self.fake_secret_path:
            return {
                'pdisk_key': {
                    'keys': [
                        {
                            'container_path': '',
                            'pin': '',
                            'id': '0',
                            'version': 0,
                        },
                        {
                            'container_path': self.fake_secret_path,
                            'pin': '',
                            'id': 'fake-secret',
                            'version': 1
                        },
                    ]
                }
            }
        else:
            return {}

    def convert_to_dict(self):
        d = {
            **self.get_static_group_dict(),
            **self.get_drives_dict(),
            **self.get_storage_pools_dict(),
            **self.get_domains_dict(),
            **self.get_host_configs_dict(),
            **self.get_system_tables_dict(),
            **self.get_log_dict(),
            **self.get_pdisk_key_dict(),
        }
        d.update(self.get_overridden_configs_dict())
        return d

    def generate_config_for_ydb_configure(self):
        return yaml.safe_dump(self.convert_to_dict())
