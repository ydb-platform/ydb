from typing import List

from annet.adapters.netbox.common.models import NetboxDevice
from annet.generators import BaseGenerator, PartialGenerator
from annet.mesh import MeshExecutor
from annet.storage import Storage

from .mesh_logic import registry


class Bgp(PartialGenerator):
    TAGS = ["mgmt", "bgp"]

    def acl_huawei(self, device):
        return """
        bgp
            peer
        """

    def run_huawei(self, device: NetboxDevice):
        executor = MeshExecutor(registry, device.storage)
        res = executor.execute_for(device)
        yield f"bgp {res.global_options.local_as}"
        for peer in res.peers:
            yield f"   peer {peer.addr} interface {peer.interface}"
            if peer.group_name:
                yield f"   peer {peer.addr} {peer.group_name}"
            if peer.remote_as is not None:
                yield f"   peer {peer.addr} remote-as {peer.remote_as}"

        for group in res.global_options.groups:
            yield f"   peer {group.name} remote-as {group.remote_as}"
        for interface in device.interfaces:
            print(
                interface.name,
                interface.lag_min_links,
                interface.lag.name if interface.lag else None,
                interface.ip_addresses,
            )


def get_generators(store: Storage) -> List[BaseGenerator]:
    return [
        Bgp(store),
    ]
