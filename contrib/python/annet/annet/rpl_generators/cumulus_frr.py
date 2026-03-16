from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Sequence
from ipaddress import ip_interface
from typing import Any, Iterable, Iterator, Literal, Optional, cast

from annet.rpl import (
    ActionType,
    ConditionOperator,
    MatchField,
    PrefixMatchValue,
    ResultType,
    RoutingPolicy,
    RoutingPolicyStatement,
    SingleAction,
    SingleCondition,
    ThenField,
)
from annet.rpl.statement_builder import AsPathActionValue, CommunityActionValue, NextHopActionValue

from .aspath import get_used_as_path_filters
from .community import get_used_united_community_lists
from .entities import (
    AsPathFilter,
    CommunityList,
    CommunityLogic,
    CommunityType,
    IpPrefixList,
    PrefixListNameGenerator,
    group_community_members,
    mangle_united_community_list_name,
)


FRR_RESULT_MAP = {
    ResultType.ALLOW: "permit",
    ResultType.DENY: "deny",
    ResultType.NEXT: "permit",
}
FRR_MATCH_COMMAND_MAP: dict[str, str] = {
    MatchField.as_path_filter: "as-path {option_value}",
    MatchField.metric: "metric {option_value}",
    MatchField.protocol: "source-protocol {option_value}",
    MatchField.interface: "interface {option_value}",
    # unsupported: as_path_length
    # unsupported: rd
}
FRR_THEN_COMMAND_MAP: dict[str, str] = {
    ThenField.local_pref: "local-preference {option_value}",
    ThenField.metric_type: "metric-type {option_value}",
    ThenField.origin: "origin {option_value}",
    ThenField.tag: "tag {option_value}",
    # unsupported: resolution
    # unsupported: rpki_valid_state
    # unsupported: mpls-label
}

FRR_INDENT = " "


class CumulusPolicyGenerator(ABC):
    @abstractmethod
    def get_policies(self, device: Any) -> list[RoutingPolicy]:
        raise NotImplementedError()

    @abstractmethod
    def get_prefix_lists(self, device: Any) -> Sequence[IpPrefixList]:
        raise NotImplementedError()

    @abstractmethod
    def get_community_lists(self, device: Any) -> list[CommunityList]:
        raise NotImplementedError()

    @abstractmethod
    def get_as_path_filters(self, device: Any) -> Sequence[AsPathFilter]:
        raise NotImplementedError

    def generate_cumulus_rpl(self, device: Any) -> Iterator[Sequence[str]]:
        prefix_lists = self.get_prefix_lists(device)
        policies = self.get_policies(device)
        prefix_list_name_generator = PrefixListNameGenerator(prefix_lists, policies)

        communities = {c.name: c for c in self.get_community_lists(device)}
        yield from self._cumulus_as_path_filters(device, policies)
        yield from self._cumulus_communities(device, communities, policies)
        yield from self._cumulus_prefix_lists(device, policies, prefix_list_name_generator)
        yield from self._cumulus_policy_config(device, communities, policies, prefix_list_name_generator)

    def _cumulus_as_path_filters(
        self,
        device: Any,
        policies: list[RoutingPolicy],
    ):
        as_path_filters = get_used_as_path_filters(self.get_as_path_filters(device), policies)
        if not as_path_filters:
            return
        for as_path_filter in as_path_filters:
            values = "_".join(x for x in as_path_filter.filters if x != ".*")
            yield "ip as-path access-list", as_path_filter.name, "permit", f"_{values}_"

    def _cumulus_prefix_list(
        self,
        ip_type: Literal["ipv6", "ip"],
        plist: IpPrefixList,
    ) -> Iterable[Sequence[str]]:
        for i, m in enumerate(plist.members):
            ge, le = m.or_longer
            yield (
                (
                    ip_type,
                    "prefix-list",
                    plist.name,
                    f"seq {i * 5 + 5}",
                    "permit",
                    str(m.prefix),
                )
                + (("ge", str(ge)) if ge is not None else ())
                + (("le", str(le)) if le is not None else ())
            )

    def _cumulus_prefix_lists(
        self,
        device: Any,
        policies: list[RoutingPolicy],
        name_generator: PrefixListNameGenerator,
    ) -> Iterable[Sequence[str]]:
        processed_names = set()
        for policy in policies:
            for statement in policy.statements:
                cond: SingleCondition[PrefixMatchValue]
                for cond in statement.match.find_all(MatchField.ip_prefix):
                    for name in cond.value.names:
                        plist = name_generator.get_prefix(name, cond.value)
                        if plist.name in processed_names:
                            continue
                        yield from self._cumulus_prefix_list("ip", plist)
                        processed_names.add(plist.name)
                for cond in statement.match.find_all(MatchField.ipv6_prefix):
                    for name in cond.value.names:
                        plist = name_generator.get_prefix(name, cond.value)
                        if plist.name in processed_names:
                            continue
                        yield from self._cumulus_prefix_list("ipv6", plist)
                        processed_names.add(plist.name)
        yield "!"

    def get_used_united_community_lists(
        self,
        communities: dict[str, CommunityList],
        policies: list[RoutingPolicy],
    ) -> list[list[CommunityList]]:
        return get_used_united_community_lists(communities=communities.values(), policies=policies)

    def _cumulus_community(
        self,
        name: str,
        cmd: str,
        member: str,
        use_regex: bool,
        seq: int,
    ) -> Iterable[Sequence[str]]:
        if use_regex:
            yield (
                cmd,
                "expanded",
                name,
                f"seq {seq}",
                "permit",
                member,
            )
        else:
            yield (
                cmd,
                "standard",
                name,
                f"seq {seq}",
                "permit",
                member,
            )

    def _cumulus_communities(
        self,
        device: Any,
        communities: dict[str, CommunityList],
        policies: list[RoutingPolicy],
    ) -> Iterable[Sequence[str]]:
        """BGP community-lists section configuration"""
        community_unions = self.get_used_united_community_lists(communities, policies)
        if not community_unions:
            return
        for community_list_union in community_unions:
            name = mangle_united_community_list_name([c.name for c in community_list_union])
            comm_number = 0

            for clist in community_list_union:
                if clist.type is CommunityType.BASIC:
                    member_prefix = ""
                    cmd = "bgp community-list"
                elif clist.type is CommunityType.RT:
                    member_prefix = "rt "
                    cmd = "bgp extcommunity"
                elif clist.type is CommunityType.SOO:
                    member_prefix = "soo "
                    cmd = "bgp extcommunity"
                elif clist.type is CommunityType.LARGE:
                    member_prefix = ""
                    cmd = "bgp large-community-list"
                else:
                    raise NotImplementedError(f"Community type {clist.type} is not supported on Cumulus")

                if clist.logic == CommunityLogic.AND:
                    if clist.use_regex:
                        if len(clist.members) > 1:
                            raise NotImplementedError("Multiple regexes with AND logic are not supported on Cumulus")
                        member = member_prefix + clist.members[0]
                    else:
                        member = " ".join(f"{member_prefix}{m}" for m in clist.members)
                    yield from self._cumulus_community(
                        name=name,
                        cmd=cmd,
                        member=member,
                        use_regex=clist.use_regex,
                        seq=(comm_number + 1) * 10,
                    )
                else:
                    for comm_number, member_value in enumerate(clist.members, start=comm_number):
                        yield from self._cumulus_community(
                            name=name,
                            cmd=cmd,
                            member=member_prefix + member_value,
                            use_regex=clist.use_regex,
                            seq=(comm_number + 1) * 10,
                        )
                comm_number += 1
        yield "!"

    def _get_match_community_names(self, condition: SingleCondition[Sequence[str]]) -> Sequence[str]:
        if condition.operator is ConditionOperator.HAS_ANY:
            return [mangle_united_community_list_name(condition.value)]
        else:
            return condition.value

    def _cumulus_policy_match(
        self,
        device: Any,
        condition: SingleCondition[Any],
        name_generator: PrefixListNameGenerator,
    ) -> Iterator[Sequence[str]]:
        if condition.field == MatchField.community:
            for comm_name in self._get_match_community_names(condition):
                yield "match community", comm_name
            return
        if condition.field == MatchField.large_community:
            for comm_name in self._get_match_community_names(condition):
                yield "match large-community-list", comm_name
            return
        if condition.field == MatchField.extcommunity_rt:
            for comm_name in self._get_match_community_names(condition):
                yield "match extcommunity", comm_name
            return
        if condition.field == MatchField.extcommunity_soo:
            for comm_name in self._get_match_community_names(condition):
                yield "match extcommunity", comm_name
            return
        if condition.field == MatchField.ip_prefix:
            for name in condition.value.names:
                plist = name_generator.get_prefix(name, condition.value)
                yield "match", "ip address prefix-list", plist.name
            return
        if condition.field == MatchField.ipv6_prefix:
            for name in condition.value.names:
                plist = name_generator.get_prefix(name, condition.value)
                yield "match", "ipv6 address prefix-list", plist.name
            return
        if condition.operator is not ConditionOperator.EQ:
            raise NotImplementedError(
                f"`{condition.field}` with operator {condition.operator} is not supported for Cumulus",
            )
        if condition.field not in FRR_MATCH_COMMAND_MAP:
            raise NotImplementedError(f"Match using `{condition.field}` is not supported for Cumulus")
        cmd = FRR_MATCH_COMMAND_MAP[condition.field]
        yield "match", cmd.format(option_value=condition.value)

    def _cumulus_then_community(
        self,
        communities: dict[str, CommunityList],
        device: Any,
        action: SingleAction[CommunityActionValue],
    ) -> Iterator[Sequence[str]]:
        if action.value.replaced is not None:
            if action.value.added or action.value.removed:
                raise NotImplementedError(
                    "Cannot set community together with add/replace on cumulus",
                )
            members = [m for name in action.value.replaced for m in communities[name].members]
            if members:
                yield "set", "community", *members
            else:
                yield "set", "community", "none"
        if action.value.added:
            members = [m for name in action.value.added for m in communities[name].members]
            yield "set", "community", *members, "additive"
        for community_name in action.value.removed:
            yield "set comm-list", community_name, "delete"

    def _cumulus_then_large_community(
        self,
        communities: dict[str, CommunityList],
        device: Any,
        action: SingleAction[CommunityActionValue],
    ) -> Iterator[Sequence[str]]:
        if action.value.replaced is not None:
            raise NotImplementedError("Replacing Large community is not supported for Cumulus")
        for community_name in action.value.added:
            yield "set", "large-community", community_name, "additive"
        for community_name in action.value.removed:
            raise NotImplementedError("Large-community remove is not supported for Cumulus")

    def _cumulus_then_rt_community(
        self,
        communities: dict[str, CommunityList],
        device: Any,
        action: SingleAction[CommunityActionValue],
    ) -> Iterator[Sequence[str]]:
        if action.value.replaced is not None:
            raise NotImplementedError("Replacing RT extcommunity is not supported for Cumulus")
        for community_name in action.value.added:
            yield "set", "extcommunity rt", community_name, "additive"
        for community_name in action.value.removed:
            raise NotImplementedError("RT extcommunity remove is not supported for Cumulus")

    def _cumulus_then_soo_community(
        self,
        communities: dict[str, CommunityList],
        device: Any,
        action: SingleAction[CommunityActionValue],
    ) -> Iterator[Sequence[str]]:
        if action.value.replaced is not None:
            raise NotImplementedError("Replacing SOO extcommunity is not supported for Cumulus")
        for community_name in action.value.added:
            yield "set", "extcommunity soo", community_name, "additive"
        if action.value.removed:
            raise NotImplementedError("SOO extcommunity remove is not supported for Cumulus")

    def _cumulus_extcommunity_type_str(self, comm_type: CommunityType) -> str:
        if comm_type is CommunityType.SOO:
            return "soo"
        elif comm_type is CommunityType.RT:
            return "rt"
        elif comm_type is CommunityType.LARGE:
            raise ValueError("Large community is not subtype of extcommunity")
        elif comm_type is CommunityType.BASIC:
            raise ValueError("Basic community is not subtype of extcommunity")
        else:
            raise NotImplementedError(f"Community type {comm_type} is not supported on cumulus")

    def _cumulus_then_extcommunity(
        self,
        communities: dict[str, CommunityList],
        device: Any,
        action: SingleAction[CommunityActionValue],
    ):
        if action.value.replaced is not None:
            if action.value.added or action.value.removed:
                raise NotImplementedError(
                    "Cannot set extcommunity together with add/delete on cumulus",
                )
            if not action.value.replaced:
                yield "set", "extcommunity", "none"
                return
            members = group_community_members(communities, action.value.replaced)
            for community_type, replaced_members in members.items():
                type_str = self._cumulus_extcommunity_type_str(community_type)
                yield "set", "extcommunity", type_str, *replaced_members
        if action.value.added:
            raise NotImplementedError("extcommunity add is not supported for Cumulus")
        if action.value.removed:
            raise NotImplementedError("extcommunity remove is not supported for Cumulus")

    def _cumulus_then_as_path(
        self,
        device: Any,
        action: SingleAction[AsPathActionValue],
    ) -> Iterator[Sequence[str]]:
        if action.value.prepend:
            for path_item in action.value.prepend:
                yield "set", "as-path prepend", path_item
        if action.value.expand:
            raise NotImplementedError("asp_path.expand is not supported for Cumulus")
        if action.value.delete:
            for path_item in action.value.delete:
                yield "set", "as-path exclude", path_item
        if action.value.set is not None:
            yield "set", "as-path exclude", "all"
            for path_item in action.value.set:
                yield "set", "as-path prepend", path_item
        if action.value.expand_last_as:
            yield "set", "as-path prepend last-as", action.value.expand_last_as

    def _cumulus_policy_then(
        self,
        communities: dict[str, CommunityList],
        device: Any,
        action: SingleAction[Any],
    ) -> Iterator[Sequence[str]]:
        if action.field == ThenField.community:
            yield from self._cumulus_then_community(
                communities,
                device,
                cast(SingleAction[CommunityActionValue], action),
            )
            return
        if action.field == ThenField.large_community:
            yield from self._cumulus_then_large_community(
                communities,
                device,
                cast(SingleAction[CommunityActionValue], action),
            )
            return
        if action.field == ThenField.extcommunity:
            yield from self._cumulus_then_extcommunity(communities, device, action)
            return
        if action.field == ThenField.extcommunity_rt:
            yield from self._cumulus_then_rt_community(
                communities,
                device,
                cast(SingleAction[CommunityActionValue], action),
            )
            return
        if action.field == ThenField.extcommunity_soo:
            yield from self._cumulus_then_soo_community(
                communities,
                device,
                cast(SingleAction[CommunityActionValue], action),
            )
            return
        if action.field == ThenField.metric:
            if action.type is ActionType.ADD:
                yield "set", f"metric +{action.value}"
            elif action.type is ActionType.REMOVE:
                yield "set", f"metric -{action.value}"
            elif action.type is ActionType.SET:
                yield "set", f"metric {action.value}"
            else:
                raise NotImplementedError(f"Action type {action.type} for metric is not supported for Cumulus")
            return
        if action.field == ThenField.as_path:
            yield from self._cumulus_then_as_path(device, action)
            return
        if action.field == ThenField.next_hop:
            next_hop_action_value = cast(NextHopActionValue, action.value)
            if next_hop_action_value.target == "self":
                yield "set", "metric 1"
            elif next_hop_action_value.target == "discard":
                pass
            elif next_hop_action_value.target == "peer":
                pass
            elif next_hop_action_value.target == "ipv4_addr":
                yield "set", f"ip next-hop {next_hop_action_value.addr}"
            elif next_hop_action_value.target == "ipv6_addr":
                yield "set", f"ipv6 next-hop {next_hop_action_value.addr}"
            elif next_hop_action_value.target == "mapped_ipv4":
                yield "set", "ipv6 next-hop ::FFFF:{next_hop_action_value.addr}"
            else:
                raise NotImplementedError(
                    f"Next_hop target {next_hop_action_value.target} is not supported for Cumulus"
                )
            return

        if action.type is not ActionType.SET:
            raise NotImplementedError(f"Action type {action.type} for `{action.field}` is not supported for Cumulus")
        if action.field not in FRR_THEN_COMMAND_MAP:
            raise NotImplementedError(f"Then action using `{action.field}` is not supported for Cumulus")
        cmd = FRR_THEN_COMMAND_MAP[action.field]
        yield "set", cmd.format(option_value=action.value)

    def _cumulus_policy_statement(
        self,
        communities: dict[str, CommunityList],
        device: Any,
        policy: RoutingPolicy,
        statement: RoutingPolicyStatement,
        prefix_list_name_generator: PrefixListNameGenerator,
    ) -> Iterable[Sequence[str]]:
        yield "route-map", policy.name, FRR_RESULT_MAP[statement.result], str(statement.number)

        for condition in statement.match:
            for row in self._cumulus_policy_match(device, condition, prefix_list_name_generator):
                yield FRR_INDENT, *row
        for action in statement.then:
            for row in self._cumulus_policy_then(communities, device, action):
                yield FRR_INDENT, *row
        if statement.result is ResultType.NEXT:
            yield FRR_INDENT, "on-match next"
        yield "!"

    def _cumulus_policy_config(
        self,
        device: Any,
        communities: dict[str, CommunityList],
        policies: list[RoutingPolicy],
        prefix_list_name_generator: PrefixListNameGenerator,
    ) -> Iterable[Sequence[str]]:
        """Route maps configuration"""

        for policy in policies:
            applied_stmts: dict[int, Optional[str]] = {}
            for statement in policy.statements:
                if statement.number is None:
                    raise RuntimeError(
                        f"Statement number should not be empty on Cumulus (found for policy: {policy.name})"
                    )

                if statement.number in applied_stmts:
                    raise RuntimeError(
                        f"Multiple statements have same number {statement.number} for policy `{policy.name}`: "
                        f"`{statement.name}` and `{applied_stmts[statement.number]}`"
                    )
                yield from self._cumulus_policy_statement(
                    communities,
                    device,
                    policy,
                    statement,
                    prefix_list_name_generator,
                )
                applied_stmts[statement.number] = statement.name
