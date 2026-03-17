from typing import Callable

from annet.connectors import Connector
from annet.vendors.base import AbstractVendor

from .library import (
    arista,
    aruba,
    b4com,
    cisco,
    h3c,
    huawei,
    iosxr,
    juniper,
    nexus,
    nokia,
    optixtrans,
    pc,
    ribbon,
    routeros,
)
from .registry import Registry, registry


class _RegistryConnector(Connector[Registry]):
    name = "Registry"
    ep_name = "vendors"

    def _get_default(self) -> Callable[[], Registry]:
        return lambda: registry


registry_connector = _RegistryConnector()
