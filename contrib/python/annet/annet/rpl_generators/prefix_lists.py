from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from itertools import chain
from typing import Any, Literal

from annet.generators import PartialGenerator
from annet.rpl import MatchField, PrefixMatchValue, RoutingPolicy, SingleCondition

from .entities import IpPrefixList, JuniperPrefixListNameGenerator, PrefixListNameGenerator


class PrefixListFilterGenerator(PartialGenerator, ABC):
    TAGS = ["policy", "rpl", "routing"]

    @abstractmethod
    def get_policies(self, device: Any) -> list[RoutingPolicy]:
        raise NotImplementedError()

    @abstractmethod
    def get_prefix_lists(self, device: Any) -> Sequence[IpPrefixList]:
        raise NotImplementedError()

    # huawei
    def acl_huawei(self, _):
        return r"""
        ip ip-prefix
        ip ipv6-prefix
        """

    def _huawei_prefix_list(
        self,
        prefix_type: Literal["ipv6-prefix", "ip-prefix"],
        plist: IpPrefixList,
    ) -> Iterable[Sequence[str]]:
        for i, m in enumerate(plist.members):
            ge, le = m.or_longer
            yield (
                (
                    "ip",
                    prefix_type,
                    plist.name,
                    f"index {i * 5 + 5}",
                    "permit",
                    str(m.prefix.network_address).upper(),
                    str(m.prefix.prefixlen),
                )
                + (("greater-equal", str(ge)) if ge is not None else ())
                + (("less-equal", str(le)) if le is not None else ())
            )

    def run_huawei(self, device: Any):
        prefix_lists = self.get_prefix_lists(device)
        policies = self.get_policies(device)

        name_generator = PrefixListNameGenerator(prefix_lists, policies)
        processed_names = set()
        for policy in policies:
            for statement in policy.statements:
                cond: SingleCondition[PrefixMatchValue]
                for cond in statement.match.find_all(MatchField.ip_prefix):
                    for name in cond.value.names:
                        plist = name_generator.get_prefix(name, cond.value)
                        if plist.name in processed_names:
                            continue
                        yield from self._huawei_prefix_list("ip-prefix", plist)
                        processed_names.add(plist.name)
                for cond in statement.match.find_all(MatchField.ipv6_prefix):
                    for name in cond.value.names:
                        plist = name_generator.get_prefix(name, cond.value)
                        if plist.name in processed_names:
                            continue
                        yield from self._huawei_prefix_list("ipv6-prefix", plist)
                        processed_names.add(plist.name)

    # arista
    def acl_arista(self, _):
        return r"""
        ip prefix-list
            seq
        ipv6 prefix-list
            seq
        """

    def _arista_prefix_list(
        self,
        prefix_type: Literal["ipv6", "ip"],
        plist: IpPrefixList,
    ) -> Iterable[Sequence[str]]:
        with self.block(prefix_type, "prefix-list", plist.name):
            for i, m in enumerate(plist.members):
                ge, le = m.or_longer
                yield (
                    (
                        f"seq {i * 10 + 10}",
                        "permit",
                        str(m.prefix),
                    )
                    + (("ge", str(ge)) if ge is not None else ())
                    + (("le", str(le)) if le is not None else ())
                )

    def run_arista(self, device: Any):
        prefix_lists = self.get_prefix_lists(device)
        policies = self.get_policies(device)
        name_generator = PrefixListNameGenerator(prefix_lists, policies)
        processed_names = set()
        for policy in policies:
            for statement in policy.statements:
                cond: SingleCondition[PrefixMatchValue]
                for cond in statement.match.find_all(MatchField.ip_prefix):
                    for name in cond.value.names:
                        plist = name_generator.get_prefix(name, cond.value)
                        if plist.name in processed_names:
                            continue
                        yield from self._arista_prefix_list("ip", plist)
                        processed_names.add(plist.name)
                for cond in statement.match.find_all(MatchField.ipv6_prefix):
                    for name in cond.value.names:
                        plist = name_generator.get_prefix(name, cond.value)
                        if plist.name in processed_names:
                            continue
                        yield from self._arista_prefix_list("ipv6", plist)
                        processed_names.add(plist.name)

    # Cisco IOS XR
    def acl_iosxr(self, device: Any):
        return r"""
        prefix-set
            ~ %global=1
        """

    def _iosxr_prefixlist(self, prefixlist: IpPrefixList):
        with self.block("prefix-set", prefixlist.name):
            for n, member in enumerate(prefixlist.members):
                if n + 1 < len(prefixlist.members):
                    comma = ","
                else:
                    comma = ""

                ge, le = member.or_longer
                if ge is le is None:
                    yield (f"{member.prefix}{comma}",)
                elif ge is None:
                    yield (f"{member.prefix} le {le}{comma}",)
                elif le is None:
                    yield (f"{member.prefix} ge {ge}{comma}",)
                elif ge == le:
                    yield (f"{member.prefix} eq {ge}{comma}",)
                else:
                    yield (f"{member.prefix} ge {ge} le {le}{comma}",)

    def run_iosxr(self, device: Any):
        prefix_lists = self.get_prefix_lists(device)
        policies = self.get_policies(device)

        name_generator = PrefixListNameGenerator(prefix_lists, policies)
        processed_names = set()
        for policy in policies:
            for statement in policy.statements:
                cond: SingleCondition[PrefixMatchValue]
                for cond in statement.match.find_all(MatchField.ip_prefix):
                    for name in cond.value.names:
                        plist = name_generator.get_prefix(name, cond.value)
                        if plist.name in processed_names:
                            continue
                        yield from self._iosxr_prefixlist(plist)
                        processed_names.add(plist.name)
                for cond in statement.match.find_all(MatchField.ipv6_prefix):
                    for name in cond.value.names:
                        plist = name_generator.get_prefix(name, cond.value)
                        if plist.name in processed_names:
                            continue
                        yield from self._iosxr_prefixlist(plist)
                        processed_names.add(plist.name)

    def acl_juniper(self, _):
        return r"""
        policy-options     %cant_delete
            prefix-list *
                ~
            route-filter-list *
                ~
        """

    def _juniper_prefixlist(self, name: str, prefixlist: IpPrefixList):
        with self.block("policy-options"):
            with self.block("prefix-list", name):
                for member in prefixlist.members:
                    yield f"{member.prefix}"

    def _juniper_router_filter_list(self, name: str, prefixlist: IpPrefixList):
        with self.block("policy-options"):
            with self.block("route-filter-list", name):
                for member in prefixlist.members:
                    ge, le = member.or_longer
                    if ge is None and le is None:
                        yield f"{member.prefix} exact"
                        continue
                    if ge is None:
                        ge = member.prefix.prefixlen
                    if le is None:
                        le = member.prefix.max_prefixlen
                    # may produce config that is not accepted by commit
                    # since juniper enforces that n <= ge <= le
                    # where n is prefix len: .../n

                    # this is done specifically to match other generators behaviour
                    # can be revised in two ways: exeption or enforce via max/min
                    # but need to be consistent across vendors so will leave it for now
                    yield f"{member.prefix}", "prefix-length-range", f"/{ge}-/{le}"

    def run_juniper(self, device: Any):
        prefix_lists = self.get_prefix_lists(device)
        policies = self.get_policies(device)

        name_generator = JuniperPrefixListNameGenerator(prefix_lists, policies)
        processed_names: set[str] = set()
        for policy in policies:
            for statement in policy.statements:
                conds = chain(
                    statement.match.find_all(MatchField.ip_prefix),
                    statement.match.find_all(MatchField.ipv6_prefix),
                )
                cond: SingleCondition[PrefixMatchValue]
                for cond in conds:
                    for cond_name in cond.value.names:
                        plist = name_generator.get_prefix(cond_name, cond.value)
                        plist_type = name_generator.get_type(cond_name, cond.value)

                        if plist.name not in processed_names:
                            processed_names.add(plist.name)
                            if plist_type == "prefix-list":
                                yield from self._juniper_prefixlist(plist.name, plist)
                            elif plist_type == "route-filter":
                                yield from self._juniper_router_filter_list(plist.name, plist)
                            else:
                                raise NotImplementedError(
                                    f"Prefix list {cond_name} type {plist_type} is not supported for Juniper",
                                )
