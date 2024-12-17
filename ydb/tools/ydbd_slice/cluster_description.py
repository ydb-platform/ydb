import os
import yaml
import random
from collections import namedtuple
from functools import reduce

from ydb.tools.cfg.base import ClusterDetailsProvider
from ydb.tools.cfg.dynamic import DynamicConfigGenerator
from ydb.tools.cfg.static import StaticConfigGenerator
from ydb.tools.cfg.utils import write_to_file


DynamicSlot = namedtuple(
    '_DynamicSlot', [
        'slot',
        'domain',
        'mbus',
        'grpc',
        'mon',
        'ic',
    ]
)


class ClusterDetails(ClusterDetailsProvider):
    TENANTS_PORTS_START = 30000
    SLOTS_PORTS_START = 31000
    PORTS_SHIFT = 10

    def __init__(self, cluster_description_path, walle_provider):
        self.__template = None
        self.__details = None
        self.__databases = None
        self.__dynamic_slots = None
        self._cluster_description_file = cluster_description_path
        self._walle_provider = walle_provider

        super(ClusterDetails, self).__init__(self.template, self._walle_provider)

    @property
    def template(self):
        if self.__template is None:
            with open(self._cluster_description_file, 'r') as file:
                content = file.read()
            self.__template = yaml.safe_load(content)
        return self.__template

    @property
    def hosts_names(self):
        return sorted(list(set(node.hostname for node in self.hosts)))

    @staticmethod
    def __is_oneof_in(oneof, container):
        facts = [x in container for x in oneof]
        return reduce(lambda x, y: x or y, facts)

    @property
    def databases(self):
        if self.__databases is None:
            self.__databases = {}

            for domain in self.domains:
                self.__databases[domain.domain_name] = domain.tenants

        return self.__databases

    @property
    def dynamic_slots(self):
        if self.__dynamic_slots is None:
            self.__dynamic_slots = {}
            for slot in super(ClusterDetails, self).dynamic_slots:
                mbus_port = self.SLOTS_PORTS_START + 0
                grpc_port = self.SLOTS_PORTS_START + 1
                mon_port = self.SLOTS_PORTS_START + 2
                ic_port = self.SLOTS_PORTS_START + 3
                full_name = str(ic_port)

                self.__dynamic_slots[full_name] = DynamicSlot(
                    slot=full_name,
                    domain=slot.domain,
                    mbus=mbus_port,
                    grpc=grpc_port,
                    mon=mon_port,
                    ic=ic_port,
                )
                self.SLOTS_PORTS_START += self.PORTS_SHIFT
        return self.__dynamic_slots


def make_dir(dir):
    permits = 0o755
    if not os.path.exists(dir):
        os.mkdir(dir, permits)
    assert os.path.isdir(dir)


class Configurator(object):
    RANDOM_SEED = 100

    def __init__(
            self,
            cluster_details,
            out_dir,
            kikimr_bin,
            kikimr_compressed_bin,
            walle_provider
    ):
        self.__cluster_details = cluster_details
        self.__kikimr_bin_file = kikimr_bin
        self.__kikimr_compressed_bin_file = kikimr_compressed_bin
        self.__static = None
        self.__static_cfg = os.path.join(out_dir, 'kikimr-static')

        self.__dynamic = None
        self.__dynamic_cfg = os.path.join(out_dir, 'kikimr-dynamic')
        self.__subdomains = None
        self.__walle_provider = walle_provider

    @property
    def kikimr_bin(self):
        return self.__kikimr_bin_file

    @property
    def kikimr_compressed_bin(self):
        return self.__kikimr_compressed_bin_file

    @property
    def template(self):
        return self.detail.template

    @property
    def detail(self):
        return self.__cluster_details

    @staticmethod
    def _generate_fake_keys():
        content = 'Keys {\n'
        content += '  ContainerPath: "/Berkanavt/kikimr/cfg/fake-secret.txt"\n'
        content += '  Pin: ""\n'
        content += '  Id: "fake-secret"\n'
        content += '  Version: 1\n'
        content += '}\n'
        return content

    @staticmethod
    def _generate_fake_secret():
        return 'not a secret at all, only for more similar behavior with cloud'

    def _make_cfg(self, generator, dir):
        random.seed(self.RANDOM_SEED)
        all_configs = generator.get_all_configs()
        for cfg_name, cfg_value in all_configs.items():
            write_to_file(
                os.path.join(dir, cfg_name),
                cfg_value
            )

        write_to_file(
            os.path.join(dir, "key.txt"),
            self._generate_fake_keys()
        )

        write_to_file(
            os.path.join(dir, "fake-secret.txt"),
            self._generate_fake_secret()
        )

    @property
    def static(self):
        assert self.__kikimr_bin_file

        if self.__static is None:
            self.__static = StaticConfigGenerator(self.template, self.__kikimr_bin_file, self.__static_cfg, walle_provider=self.__walle_provider)
        return self.__static

    def create_static_cfg(self):
        make_dir(self.__static_cfg)
        self._make_cfg(self.static, self.__static_cfg)
        return self.__static_cfg

    @property
    def dynamic(self):
        assert self.__kikimr_bin_file

        if self.__dynamic is None:
            self.__dynamic = DynamicConfigGenerator(
                self.__cluster_details.template, self.__kikimr_bin_file, self.__dynamic_cfg, walle_provider=self.__walle_provider
            )
        return self.__dynamic

    def create_dynamic_cfg(self):
        make_dir(self.__dynamic_cfg)
        self._make_cfg(self.dynamic, self.__dynamic_cfg)
        return self.__dynamic_cfg
