import os
import yaml

from ydb.tools.ydbd_slice import cluster_description
import copy
from ydb.tools.cfg.utils import write_to_file

from ydb.tools.cfg.templates import (
    dynamic_cfg_new_style,
    kikimr_cfg_for_static_node_new_style,
)

# Remove specified keys
STORAGE_ONLY_KEYS = [
        'static_erasure',
        'host_configs',
        'nameservice_config',
        'blob_storage_config',
        'hosts'
]


class YamlConfig(object):
    def __init__(self, yaml_config_path: str):
        try:
            with open(yaml_config_path, 'r') as f:
                self.__yaml_config = yaml.safe_load(f)
        except (yaml.scanner.ScannerError, yaml.parser.ParserError) as e:
            raise "Invalid yaml config: {}".format(e)

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
                bin_path: os.PathLike,
                compressed_bin_path: os.PathLike,
                config_path: os.PathLike,
            ):
        # walle provider is not used
        # use config_path instad of cluster_path
        self.cluster_description = cluster_description.ClusterDetails(config_path, None)

        with open(cluster_path, 'r') as f:
            _domains = yaml.safe_load(f)
            self.cluster_description.domains = _domains.get('domains', [])

        self.__static_cfg = out_dir
        # might be needed in the future to do something locally using the ydbd binary
        self.__kikimr_bin_file = bin_path
        self.__kikimr_compressed_bin_file = compressed_bin_path
        with open(config_path, 'r') as f:
            self.static = f.read()

    @property
    def kikimr_bin(self):
        return self.bin_path

    @property
    def kikimr_compressed_bin(self):
        return self.compressed_bin_path

    @property
    def static(self):
        return self.__static

    @property
    def static_dict(self):
        return self.__static_dict

    @static.setter
    def static(self, value):
        try:
            self.__static_dict = yaml.safe_load(value)
        except (yaml.scanner.ScannerError, yaml.parser.ParserError) as e:
            raise "Invalid yaml config: {}".format(e)

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
        except (yaml.scanner.ScannerError, yaml.parser.ParserError) as e:
            raise "Invalid yaml config: {}".format(e)

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
        return [host['host'] for host in self.static_dict.get('hosts', [])]

    @property
    def kikimr_cfg(self):
        return kikimr_cfg_for_static_node_new_style()

    @property
    def dynamic_cfg(self):
        return dynamic_cfg_new_style()

    def create_static_cfg(self) -> str:
        write_to_file(
            os.path.join(self.__static_cfg, 'key.txt'),
            self._generate_fake_keys()
        )
        write_to_file(
            os.path.join(self.__static_cfg, 'fake-secret.txt'),
            self._generate_fake_secret()
        )
        write_to_file(
            os.path.join(self.__static_cfg, 'config.yaml'),
            self.__static
        )
        write_to_file(
            os.path.join(self.__static_cfg, 'kikimr.cfg'),
            self.kikimr_cfg
        )
        write_to_file(
            os.path.join(self.__static_cfg, 'dynamic_server.cfg'),
            self.dynamic_cfg
        )

        return self.__static_cfg
