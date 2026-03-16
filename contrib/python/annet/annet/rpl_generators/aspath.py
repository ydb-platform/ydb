from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

from annet.generators import PartialGenerator
from annet.rpl import MatchField, RouteMap, RoutingPolicy

from .entities import AsPathFilter


def get_used_as_path_filters(
    as_path_filters: Sequence[AsPathFilter],
    policies: list[RoutingPolicy],
) -> Sequence[AsPathFilter]:
    filters = {c.name: c for c in as_path_filters}

    used_filters = set()
    for policy in policies:
        for statement in policy.statements:
            for condition in statement.match.find_all(MatchField.as_path_filter):
                used_filters.add(condition.value)
    return [filters[name] for name in sorted(used_filters)]


class AsPathFilterGenerator(PartialGenerator, ABC):
    TAGS = ["policy", "rpl", "routing"]

    @abstractmethod
    def get_policies(self, device: Any) -> list[RoutingPolicy]:
        raise NotImplementedError()

    @abstractmethod
    def get_as_path_filters(self, device: Any) -> Sequence[AsPathFilter]:
        raise NotImplementedError

    def get_used_as_path_filters(self, device: Any) -> Sequence[AsPathFilter]:
        filters = self.get_as_path_filters(device)
        policies = self.get_policies(device)
        return get_used_as_path_filters(filters, policies)

    def acl_huawei(self, _):
        return r"""
        ip as-path-filter
        """

    def run_huawei(self, device: Any):
        for as_path_filter in self.get_used_as_path_filters(device):
            values = "_".join((x for x in as_path_filter.filters if x != ".*"))
            yield "ip as-path-filter", as_path_filter.name, "index 10 permit", f"_{values}_"

    def acl_arista(self, _):
        return r"""
        ip as-path access-list
        """

    def run_arista(self, device: Any):
        for as_path_filter in self.get_used_as_path_filters(device):
            values = "_".join((x for x in as_path_filter.filters if x != ".*"))
            yield "ip as-path access-list", as_path_filter.name, "permit", f"_{values}_"

    def acl_iosxr(self, _):
        return r"""
        as-path-set *
            ~ %global=1
        """

    def run_iosxr(self, device: Any):
        for as_path_filter in self.get_used_as_path_filters(device):
            with self.block("as-path-set", as_path_filter.name):
                for n, filter_item in enumerate(as_path_filter.filters):
                    if n + 1 < len(as_path_filter.filters):
                        comma = ","
                    else:
                        comma = ""
                    yield "ios-regex", f"'{filter_item}'{comma}"

    def acl_juniper(self, _):
        return r"""
        policy-options  %cant_delete
            as-path ~
        """

    def _juniper_as_path(self, name: str, as_path_member: str):
        if not as_path_member.isnumeric():
            as_path_member = f'"{as_path_member}"'
        yield "as-path", name, as_path_member

    def run_juniper(self, device: Any):
        for as_path_filter in self.get_used_as_path_filters(device):
            # TODO could be implemented via as-path-groups
            # But we need to provide as_path_filters to policy generator
            # To select between regular as-path and as-path-groups
            if len(as_path_filter.filters) > 1:
                raise NotImplementedError(
                    f"Multiple elements in as_path_filter {as_path_filter.name} is not supported for Juniper"
                )

            with self.block("policy-options"):
                yield from self._juniper_as_path(as_path_filter.name, as_path_filter.filters[0])
