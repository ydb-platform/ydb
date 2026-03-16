from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

from annet.generators import PartialGenerator
from annet.rpl import MatchField, RouteMap, RoutingPolicy

from .entities import RDFilter


class RDFilterFilterGenerator(PartialGenerator, ABC):
    TAGS = ["policy", "rpl", "routing"]

    @abstractmethod
    def get_policies(self, device: Any) -> list[RoutingPolicy]:
        raise NotImplementedError()

    @abstractmethod
    def get_rd_filters(self, device: Any) -> Sequence[RDFilter]:
        raise NotImplementedError()

    def get_used_rd_filters(self, device: Any) -> Sequence[RDFilter]:
        filters = {c.name: c for c in self.get_rd_filters(device)}
        policies = self.get_policies(device)
        used_filters = set()
        for policy in policies:
            for statement in policy.statements:
                for condition in statement.match.find_all(MatchField.rd):
                    used_filters.update(condition.value)
        return [filters[name] for name in sorted(used_filters)]

    def acl_huawei(self, _):
        return r"""
        ip rd-filter
        """

    def run_huawei(self, device: Any):
        for rd_filter in self.get_used_rd_filters(device):
            for i, route_distinguisher in enumerate(rd_filter.members):
                rd_id = (i + 1) * 10 + 5
                yield "ip rd-filter", rd_filter.number, f"index {rd_id}", "permit", route_distinguisher

    def acl_iosxr(self, _):
        return r"""
        rd-set *
            ~ %global=1
        """

    def run_iosxr(self, device: Any):
        for rd_filter in self.get_used_rd_filters(device):
            with self.block("rd-set", rd_filter.name):
                for i, route_distinguisher in enumerate(rd_filter.members):
                    if i + 1 < len(rd_filter.members):
                        comma = ","
                    else:
                        comma = ""
                    yield (f"{route_distinguisher}{comma}",)

    def acl_juniper(self, _):
        return r"""
        policy-options             %cant_delete
            route-distinguisher
        """

    def run_juniper(self, device: Any):
        for rd_filter in self.get_used_rd_filters(device):
            with self.block("policy-options"):
                if len(rd_filter.members) == 1:
                    yield "route-distinguisher", rd_filter.name, "members", rd_filter.members[0]
                elif len(rd_filter.members) > 1:
                    joined = " ".join(rd_filter.members)
                    yield "route-distinguisher", rd_filter.name, "members", f"[ {joined} ]"
