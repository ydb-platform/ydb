from typing import Any

from annet.generators import BaseGenerator, Entire
from annet.mesh import MeshExecutor
from annet.rpl import RouteMap, RoutingPolicy
from annet.rpl_generators import (
    AsPathFilter,
    AsPathFilterGenerator,
    CommunityList,
    CommunityListGenerator,
    CumulusPolicyGenerator,
    IpPrefixList,
    PrefixListFilterGenerator,
    RDFilter,
    RDFilterFilterGenerator,
    RoutingPolicyGenerator,
    get_policies,
)
from annet.storage import Storage

from .items import AS_PATH_FILTERS, COMMUNITIES, PREFIX_LISTS, RD_FILTERS
from .mesh import registry
from .route_policy import routemap


class CommunityGenerator(CommunityListGenerator):
    def get_community_lists(self, device: Any) -> list[CommunityList]:
        return COMMUNITIES

    def get_routemap(self) -> RouteMap:
        return routemap

    def get_policies(self, device: Any) -> list[RoutingPolicy]:
        return get_policies(
            routemap=routemap,
            device=device,
            mesh_executor=MeshExecutor(registry, self.storage),
        )


class PolicyGenerator(RoutingPolicyGenerator):
    def get_prefix_lists(self, device: Any) -> list[IpPrefixList]:
        return PREFIX_LISTS

    def get_policies(self, device: Any) -> list[RoutingPolicy]:
        return get_policies(
            routemap=routemap,
            device=device,
            mesh_executor=MeshExecutor(registry, self.storage),
        )

    def get_community_lists(self, device: Any) -> list[CommunityList]:
        return COMMUNITIES

    def get_rd_filters(self, device: Any) -> list[RDFilter]:
        return RD_FILTERS


class AsPathGenerator(AsPathFilterGenerator):
    def get_policies(self, device: Any) -> list[RoutingPolicy]:
        return get_policies(
            routemap=routemap,
            device=device,
            mesh_executor=MeshExecutor(registry, self.storage),
        )

    def get_as_path_filters(self, device: Any) -> list[AsPathFilter]:
        return AS_PATH_FILTERS


class RDGenerator(RDFilterFilterGenerator):
    def get_policies(self, device: Any) -> list[RoutingPolicy]:
        return get_policies(
            routemap=routemap,
            device=device,
            mesh_executor=MeshExecutor(registry, self.storage),
        )

    def get_rd_filters(self, device: Any) -> list[RDFilter]:
        return RD_FILTERS


class PrefixListGenerator(PrefixListFilterGenerator):
    def get_policies(self, device: Any) -> list[RoutingPolicy]:
        return get_policies(
            routemap=routemap,
            device=device,
            mesh_executor=MeshExecutor(registry, self.storage),
        )

    def get_prefix_lists(self, device: Any) -> list[IpPrefixList]:
        return PREFIX_LISTS


FRR_HEADER = """\
!!! This file is auto-generated
!
frr defaults datacenter
log syslog informational
log timestamp precision 6
service integrated-vtysh-config"""


class FrrGenerator(Entire, CumulusPolicyGenerator):
    def get_policies(self, device: Any) -> list[RoutingPolicy]:
        return get_policies(
            routemap=routemap,
            device=device,
            mesh_executor=MeshExecutor(registry, self.storage),
        )

    def get_community_lists(self, device: Any) -> list[CommunityList]:
        return COMMUNITIES

    def get_prefix_lists(self, device: Any) -> list[IpPrefixList]:
        return PREFIX_LISTS

    def get_as_path_filters(self, device: Any) -> list[AsPathFilter]:
        return AS_PATH_FILTERS

    def path(self, device):
        if device.hw.PC.Mellanox or device.hw.PC.NVIDIA:
            return "/etc/frr/frr.conf"

    def run(self, device):
        yield FRR_HEADER
        yield from self.generate_cumulus_rpl(device)
        yield "line vty"


def get_generators(store: Storage) -> list[BaseGenerator]:
    return [
        AsPathGenerator(store),
        PolicyGenerator(store),
        CommunityGenerator(store),
        RDGenerator(store),
        PrefixListGenerator(store),
        FrrGenerator(store),
    ]
