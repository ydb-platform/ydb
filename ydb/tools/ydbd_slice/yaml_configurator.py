import os
import yaml

from ydb.tools.ydbd_slice import cluster_description
import copy
from ydb.tools.cfg.utils import write_to_file

from ydb.tools.cfg.templates import (
    dynamic_cfg_new_style,
    dynamic_cfg_new_style_v2,
    kikimr_cfg_for_static_node_new_style,
    kikimr_cfg_for_static_node_new_style_v2,
)

# Remove specified keys
STORAGE_ONLY_KEYS = [
    'static_erasure',
    'host_configs',
    'nameservice_config',
    'blob_storage_config',
    'hosts'
]


class YDBDArgsBuilder:
    def __init__(self):
        self.__args = {}

    def add_argument(self, key: str, value: str):
        if key in self.__args:
            raise ValueError(f"Argument '{key}' already exists with value '{self.__args[key]}'")

        self.__args[key] = value

    def remove_argument(self, key: str):
        if key not in self.__args:
            raise ValueError(f"Argument '{key}' does not exist")

        del self.__args[key]

    def clear_arguments(self):
        self.__args.clear()

    def build_command_line(self) -> str:
        if not self.__args:
            return ""

        return " ".join([f"{key} {value}" for key, value in self.__args.items()])


class YamlConfig(object):
    def __init__(self, yaml_config_path: str):
        try:
            with open(yaml_config_path, 'r') as f:
                self.__yaml_config = yaml.safe_load(f)
        except (yaml.YAMLError, yaml.scanner.ScannerError, yaml.parser.ParserError) as e:  # type: ignore
            raise ValueError(f"Invalid yaml config: {e}")

    @property
    def dynamic_simple(self):
        subconfig = copy.deepcopy(self.__yaml_config)

        for key in STORAGE_ONLY_KEYS:
            subconfig.pop(key, None)

        cluster_uuid = self.__yaml_config.get('nameservice_config', {}).get('cluster_uuid', '')
        dynconfig = {
            'metadata': {
                'kind': 'MainConfig',
                'cluster': cluster_uuid,
                'version': 0,
            },
            'config': subconfig,
            'allowed_labels': {
                'node_id': {'type': 'string'},
                'host': {'type': 'string'},
                'tenant': {'type': 'string'},
            },
            'selector_config': [],
        }

        return yaml.dump(dynconfig, sort_keys=True, default_flow_style=False, indent=2)


class YamlConfigurator(object):
    def __init__(
            self,
            cluster_path: os.PathLike,
            out_dir: os.PathLike,
            config_path: os.PathLike):
        # walle provider is not used
        # use config_path instad of cluster_path

        self.__static_cfg = str(out_dir)
        with open(config_path, 'r') as f:
            self.static = f.read()

        self.cluster_description = cluster_description.ClusterDetails(config_path)

        with open(cluster_path, 'r') as f:
            _domains = cluster_description.safe_load_no_duplicates(f.read())
            self.cluster_description.domains = _domains.get('domains', [])

        self._slot_args = YDBDArgsBuilder()

    @property
    def v2(self):
        return 'metadata' in self.static_dict

    @property
    def static(self):
        return self.__static

    @property
    def static_dict(self):
        return self.__static_dict

    @property
    def static_yaml(self):
        return yaml.dump(self.__static_dict)

    @static.setter
    def static(self, value):
        try:
            self.__static_dict = yaml.safe_load(value)
        except (yaml.scanner.ScannerError, yaml.parser.ParserError) as e:  # type: ignore
            raise ValueError(f"Invalid yaml config: {e}")

        self.__static = value

    @property
    def dynamic(self):
        return self.__dynamic

    @property
    def dynamic_dict(self):
        return self.__dynamic_dict

    @dynamic.setter
    def dynamic(self, value):
        try:
            self.__dynamic_dict = yaml.safe_load(value)
        except (yaml.scanner.ScannerError, yaml.parser.ParserError) as e:  # type: ignore
            raise ValueError(f"Invalid yaml config: {e}")

        self.__dynamic = value

    @staticmethod
    def _generate_fake_keys():
        return '''Keys {
  ContainerPath: "/Berkanavt/kikimr/cfg/fake-secret.txt"
  Pin: ""
  Id: "fake-secret"
  Version: 1
}'''

    @staticmethod
    def _generate_fake_secret():
        return 'not a secret at all, only for more similar behavior with cloud'

    @property
    def hosts_names(self):
        return [host['host'] for host in self.static_config_dict.get('hosts', [])]

    @property
    def static_config_dict(self):
        if self.v2:
            return self.static_dict.get('config', {})
        return self.static_dict

    @property
    def hosts_datacenters(self):
        return {host.get('host'): host.get('location', {}).get('data_center') for host in self.static_config_dict.get('hosts', [])}

    @property
    def hosts_bridge_piles(self):
        return {host.get('host'): host.get('location', {}).get('bridge_pile_name') for host in self.static_config_dict.get('hosts', [])}

    @property
    def group_hosts_by_datacenter(self):
        """
        Groups hosts by datacenter.

        Returns:
            dict: A dictionary mapping datacenter names to lists of host names
        """
        datacenter_to_hosts = {}
        for host, datacenter in self.hosts_datacenters.items():
            if not datacenter:
                continue
            if datacenter not in datacenter_to_hosts:
                datacenter_to_hosts[datacenter] = []
            datacenter_to_hosts[datacenter].append(host)
        return datacenter_to_hosts

    @property
    def group_hosts_by_bridge_pile(self):
        """
        Groups hosts by bridge pile name.

        Returns:
            dict: A dictionary mapping bridge pile names to lists of host names
        """
        pile_to_hosts = {}
        for host, pile in self.hosts_bridge_piles.items():
            if not pile:
                continue
            if pile not in pile_to_hosts:
                pile_to_hosts[pile] = []
            pile_to_hosts[pile].append(host)
        return pile_to_hosts

    @property
    def kikimr_cfg(self):
        if self.v2:
            return kikimr_cfg_for_static_node_new_style_v2()
        return kikimr_cfg_for_static_node_new_style()

    @property
    def dynamic_cfg(self) -> str:
        if self.v2:
            return dynamic_cfg_new_style_v2(extra_args=self._slot_args.build_command_line())
        return dynamic_cfg_new_style(extra_args=self._slot_args.build_command_line())

    def add_slot_arg(self, key: str, value: str):
        self._slot_args.add_argument(key, value)

    def create_static_cfg(self) -> str:
        """
        Creates static configuration files in the output directory.
        For v2 configs, uses v2-specific templates and file format.
        For traditional configs, uses the classic file format.

        Returns:
            str: Path to the static configuration directory
        """

        write_to_file(
            os.path.join(self.__static_cfg, 'config.yaml'),
            self.static
        )
        write_to_file(
            os.path.join(self.__static_cfg, 'kikimr.cfg'),
            self.kikimr_cfg
        )
        write_to_file(
            os.path.join(self.__static_cfg, 'dynamic_server.cfg'),
            self.dynamic_cfg
        )

        if not self.v2:
            write_to_file(
                os.path.join(self.__static_cfg, 'key.txt'),
                self._generate_fake_keys()
            )
            write_to_file(
                os.path.join(self.__static_cfg, 'fake-secret.txt'),
                self._generate_fake_secret()
            )

        return self.__static_cfg
