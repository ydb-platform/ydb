import yaml
from dataclasses import dataclass


config_header = '''
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
'''

allowed_labels_header = '''
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
'''


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


@dataclass
class YdbBaseConfig:
    erasure: str = "block-4-2"
    default_disk_type: str = "NVME"
    fail_domain_type: str = "disk"


@dataclass
class Pile:
    name: str


@dataclass
class Host:
    host: str
    pile: str
    data_center: str
    rack: str
    body: str
    port: int
    node_id: int
    devices: list[str]

    def add_device(self, path: str):
        self.devices.append(path)


class Subbuilder:
    def __init__(self, builder: "YdbConfigBuilder", additional_kwargs: dict):
        self.builder = builder
        self.additional_kwargs = additional_kwargs

    def add_pile(self):
        name = self.builder.get_next_pile_name()
        self.builder.piles.append(name)
        return Subbuilder(self.builder, {"pile": name, **self.additional_kwargs})

    def add_data_center(self):
        name = self.builder.get_next_data_center_name()
        self.builder.data_centers.append(name)
        return Subbuilder(self.builder, {"data_center": name, **self.additional_kwargs})

    def add_rack(self):
        name = self.builder.get_next_rack_name()
        self.builder.racks.append(name)
        return Subbuilder(self.builder, {"rack": name, **self.additional_kwargs})

    def add_body(self):
        name = self.builder.get_next_body_name()
        self.builder.bodies.append(name)
        return Subbuilder(self.builder, {"body": name, **self.additional_kwargs})

    def add_host(self, **kwargs):
        kwargs.update(self.additional_kwargs)
        return self.builder.add_host(**kwargs)


class YdbConfigBuilder(Subbuilder):
    def __init__(self, ydb_config: YdbBaseConfig):
        super().__init__(self, {})
        self.ydb_config = ydb_config
        self.piles = []
        self.data_centers = []
        self.racks = []
        self.bodies = []
        self.hosts = []
        self.manual_config_fields = {}
        self.selectors = []
        self.domain_name = None
        self.system_tablets_node_ids = None

    def get_next_pile_name(self):
        return f"pile_{len(self.piles)}"

    def get_next_data_center_name(self):
        return f"data_center_{len(self.data_centers)}"

    def get_next_rack_name(self):
        return f"rack_{len(self.racks)}"

    def get_next_body_name(self):
        return f"body_{len(self.bodies)}"

    def add_host(self, pile=None, data_center=None, rack=None, body=None, fqdn=None, port=None, node_id=None):
        if body is None:
            body = self.get_next_body_name()
            self.bodies.append(body)
        node_id = node_id or len(self.hosts) + 1
        host = Host(fqdn, pile, data_center, rack, body, port, node_id, [])
        self.hosts.append(host)
        return host

    def get_piles(self):
        return list(sorted(set(host.pile for host in self.hosts)))

    def get_host_configs(self):
        host_configs = {}
        for host in self.hosts:
            devices = tuple(sorted(host.devices))
            if devices not in host_configs:
                new_host_config_id = len(host_configs) + 1
                host_configs[devices] = {
                    "host_config_id": new_host_config_id,
                    "devices": devices,
                    "hosts": set()
                }
            host_configs[devices]["hosts"].add(host.node_id)
        return host_configs

    def get_domain_config(self):
        if self.domain_name is None:
            return None
        return {
            "domain": [{
                "domain_id": 1,
                "name": self.domain_name,
            }]
        }

    def get_system_tablets(self):
        if not self.system_tablets_node_ids:
            return None
        tablets = {
            'flat_hive': [72057594037968897],
            'flat_bs_controller': [72057594037932033],
            'flat_schemeshard': [72075186232723360],
            'flat_tx_coordinator': [72075186232723361],
            'tx_mediator': [72075186232426497],
            'tx_allocator': [72075186232492033],
            'cms': [72057594037936128],
            'node_broker': [72057594037936129],
            'tenant_slot_broker': [72057594037936130],
            'console': [72057594037936131],
        }
        return {
            name: [
                {
                    'info': {
                        'tablet_id': tablet_id
                    },
                    'node': self.system_tablets_node_ids
                }
                for tablet_id in tablet_ids
            ]
            for name, tablet_ids in tablets.items()
        }

    def get_dict(self):
        config = {
            "erasure": self.ydb_config.erasure,
            "default_disk_type": self.ydb_config.default_disk_type,
            "fail_domain_type": self.ydb_config.fail_domain_type,
            "yaml_config_enabled": True,
            "self_management_config": {
                "enabled": True
            }
        }

        piles = self.get_piles()
        has_piles = len(piles) > 1
        if has_piles:
            config["bridge_config"] = {
                "piles": [{"name": pile} for pile in piles]
            }

        host_configs = self.get_host_configs()
        config["host_configs"] = []
        for host_config in host_configs.values():
            config["host_configs"].append({
                "host_config_id": host_config["host_config_id"],
                self.ydb_config.default_disk_type.lower(): host_config["devices"]
            })

        host_to_host_config_id = {}
        for host_config in host_configs.values():
            for host in host_config["hosts"]:
                host_to_host_config_id[host] = host_config["host_config_id"]

        config["hosts"] = []
        for host in self.hosts:
            host_desc = {
                "host": host.host,
                "port": host.port,
                "node_id": host.node_id,
                "host_config_id": host_to_host_config_id[host.node_id],
                "location": {
                    "body": host.body,
                    "data_center": host.data_center,
                    "rack": host.rack,
                }
            }
            if has_piles:
                host_desc["location"]["bridge_pile_name"] = host.pile
            config["hosts"].append(host_desc)

        domain_config = self.get_domain_config()
        if domain_config is not None:
            config["domains_config"] = domain_config

        system_tablets = self.get_system_tablets()
        if system_tablets is not None:
            config["system_tablets"] = system_tablets

        config.update(self.manual_config_fields)
        return config

    def add_manual_config_field(self, key: str, value: any):
        self.manual_config_fields[key] = value

    def add_tenant_selector(self, tenant: str, config: dict):
        if config is None:
            return
        self.selectors.append({
            "description": f'config for tenant {tenant}',
            "selector": {
                "tenant": tenant,
            },
            "config": config
        })

    def generate_yaml_config(self):
        config = {
            'config': self.get_dict(),
        }
        if self.selectors:
            config['selector_config'] = self.selectors

        return config_header + allowed_labels_header + yaml.safe_dump(config)
