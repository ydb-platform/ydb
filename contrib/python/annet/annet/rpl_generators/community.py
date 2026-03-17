from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Collection, Iterator, Mapping, Sequence
from typing import Any

from annet.generators import PartialGenerator
from annet.rpl import ConditionOperator, MatchField, RoutingPolicy, SingleCondition, ThenField

from .entities import (
    CommunityList,
    CommunityLogic,
    CommunityType,
    arista_well_known_community,
    mangle_united_community_list_name,
)


def get_used_community_lists(
    communities: Collection[CommunityList],
    policies: Collection[RoutingPolicy],
) -> list[CommunityList]:
    assert_unique_names(communities)
    communities_dict = {c.name: c for c in communities}
    used_communities: set[str] = set()
    for policy in policies:
        for statement in policy.statements:
            condition: SingleCondition[Sequence[str]]
            for match_field in (
                MatchField.community,
                MatchField.large_community,
                MatchField.extcommunity_rt,
                MatchField.extcommunity_soo,
            ):
                for condition in statement.match.find_all(match_field):
                    used_communities.update(condition.value)
            for then_field in (
                ThenField.community,
                ThenField.large_community,
                ThenField.extcommunity_rt,
                ThenField.extcommunity_soo,
                ThenField.extcommunity,
            ):
                for action in statement.then.find_all(then_field):
                    if action.value.replaced is not None:
                        used_communities.update(action.value.replaced)
                    used_communities.update(action.value.added)
                    used_communities.update(action.value.removed)
    return [communities_dict[name] for name in sorted(used_communities)]


def get_used_united_community_lists(
    communities: Collection[CommunityList],
    policies: Collection[RoutingPolicy],
) -> list[list[CommunityList]]:
    """
    Return communities united into groups according to HAS_ANY policy
    """
    assert_unique_names(communities)
    communities_dict = {c.name: c for c in communities}
    used_communities: dict[str, list[CommunityList]] = {}
    for policy in policies:
        for statement in policy.statements:
            condition: SingleCondition[Sequence[str]]
            for match_field in (
                MatchField.community,
                MatchField.large_community,
                MatchField.extcommunity_rt,
                MatchField.extcommunity_soo,
            ):
                for condition in statement.match.find_all(match_field):
                    if condition.operator == ConditionOperator.HAS_ANY and len(condition.value) > 1:
                        united_name = mangle_united_community_list_name(condition.value)
                        united_communities: list[CommunityList] = [communities_dict[name] for name in condition.value]
                        if not all(united_communities[0].type == c.type for c in united_communities):
                            raise ValueError(
                                f"Cannot apply HAS_ANY to communities of different types, "
                                f"found for policy: `{policy.name}`, statement: {statement.name}"
                            )
                        if not all(united_communities[0].use_regex == c.use_regex for c in united_communities):
                            raise ValueError(
                                f"Cannot apply HAS_ANY to communities with different use_regex flag, "
                                f"found for policy: `{policy.name}`, statement: {statement.name}"
                            )
                        used_communities[united_name] = united_communities
                    else:
                        for name in condition.value:
                            used_communities[name] = [communities_dict[name]]
            for then_field in (
                ThenField.community,
                ThenField.large_community,
                ThenField.extcommunity_rt,
                ThenField.extcommunity_soo,
            ):
                for action in statement.then.find_all(then_field):
                    if action.value.replaced is not None:
                        for name in action.value.replaced:
                            used_communities[name] = [communities_dict[name]]
                    for name in action.value.added:
                        used_communities[name] = [communities_dict[name]]
                    for name in action.value.removed:
                        used_communities[name] = [communities_dict[name]]
    return [used_communities[name] for name in sorted(used_communities)]


def assert_unique_names(communities: Collection[CommunityList]) -> None:
    duplicated: list[str] = []
    seen_names: set[str] = set()
    for c in communities:
        if c.name in seen_names:
            duplicated.append(c.name)
        seen_names.add(c.name)
    if duplicated:
        raise NotImplementedError(f"Non-unique community-list names are not supported: {duplicated}")


class CommunityListGenerator(PartialGenerator, ABC):
    TAGS = ["policy", "rpl", "routing"]

    @abstractmethod
    def get_policies(self, device: Any) -> list[RoutingPolicy]:
        raise NotImplementedError()

    @abstractmethod
    def get_community_lists(self, device: Any) -> list[CommunityList]:
        raise NotImplementedError()

    def get_used_community_lists(self, device: Any) -> list[CommunityList]:
        return get_used_community_lists(
            communities=self.get_community_lists(device),
            policies=self.get_policies(device),
        )

    def get_used_united_community_lists(self, device: Any) -> list[list[CommunityList]]:
        return get_used_united_community_lists(
            communities=self.get_community_lists(device),
            policies=self.get_policies(device),
        )

    def acl_huawei(self, _):
        return r"""
        ip community-filter
        ip extcommunity-filter
        ip extcommunity-list
        ip large-community-filter
        """

    def ref_huawei(self, _):
        return """
        route-policy
            if-match community-filter <name>
            if-match extcommunity-filter <name>
            if-match extcommunity-list soo <name>
            if-match large-community-filter <name>
            apply comm-filter <name>
        """

    def _huawei_community_filter(self, index: int, community_list: CommunityList, members: str) -> Sequence[str]:
        if community_list.use_regex:
            match_type = "advanced"
        else:
            match_type = "basic"
        if community_list.type is CommunityType.BASIC:
            return "ip community-filter", match_type, community_list.name, f"index {index}", "permit", members
        elif community_list.type is CommunityType.RT:
            return "ip extcommunity-filter", match_type, community_list.name, f"index {index}", "permit", members
        elif community_list.type is CommunityType.SOO:
            return "ip extcommunity-list soo", match_type, community_list.name, f"index {index}", "permit", members
        elif community_list.type is CommunityType.LARGE:
            return "ip large-community-filter", match_type, community_list.name, f"index {index}", "permit", members
        else:
            raise NotImplementedError(f"CommunityList type {community_list.type} not implemented for huawei")

    def run_huawei(self, device: Any):
        for community_list in self.get_used_community_lists(device):
            if community_list.use_regex and len(community_list.members) > 1:
                raise NotImplementedError("Multiple regex is not supported for huawei")
            if community_list.type is CommunityType.RT:
                # RT communities used with prefix rt
                members: Sequence[str] = [f"rt {m}" for m in community_list.members]
            else:
                members = community_list.members

            if community_list.logic == CommunityLogic.AND:
                # to get AND logic the communities should be in one sting
                yield self._huawei_community_filter(10, community_list, " ".join(members))
            elif community_list.logic == CommunityLogic.OR:
                for i, member in enumerate(members):
                    member_id = (i + 1) * 10
                    yield self._huawei_community_filter(member_id, community_list, member)
            else:
                raise NotImplementedError(f"Community logic {community_list.logic} is not implemented for huawei")

    def acl_arista(self, _):
        return r"""
        ip community-list
        ip extcommunity-list
        """

    def _arista_community_list(
        self,
        name: str,
        use_regex: bool,
        comm_type: CommunityType,
        members: str,
    ) -> Sequence[str]:
        if use_regex:
            match_type = "regexp"
        else:
            match_type = ""
        if comm_type is CommunityType.BASIC:
            return "ip community-list", match_type, name, "permit", members
        elif comm_type is CommunityType.RT:
            return "ip extcommunity-list", match_type, name, "permit", members
        elif comm_type is CommunityType.SOO:
            return "ip extcommunity-list", match_type, name, "permit", members
        elif comm_type is CommunityType.LARGE:
            return "ip large-community-list", match_type, name, "permit", members
        else:
            raise NotImplementedError(f"CommunityList type {comm_type} not implemented for arista")

    def _arista_community_prefix(self, community_list: CommunityList) -> str:
        if community_list.type is CommunityType.BASIC:
            return ""
        elif community_list.type is CommunityType.RT:
            if community_list.use_regex:
                return "RT:"
            return "rt "
        elif community_list.type is CommunityType.SOO:
            if community_list.use_regex:
                return "SoO:"
            return "soo "
        elif community_list.type is CommunityType.LARGE:
            return ""
        else:
            raise NotImplementedError(f"CommunityList type {community_list.type} not implemented for arista")

    def run_arista(self, device):
        for community_list_union in self.get_used_united_community_lists(device):
            name = mangle_united_community_list_name([c.name for c in community_list_union])
            for community_list in community_list_union:
                if community_list.use_regex and len(community_list.members) > 1:
                    raise NotImplementedError("Multiple regex is not supported for arista")

                member_prefix = self._arista_community_prefix(community_list)

                if community_list.logic == CommunityLogic.AND:
                    # to get AND logic the communities should be in one sting
                    member_str = " ".join(
                        member_prefix + arista_well_known_community(m) for m in community_list.members
                    )
                    yield self._arista_community_list(
                        name=name,
                        use_regex=community_list.use_regex,
                        comm_type=community_list.type,
                        members=member_str,
                    )
                elif community_list.logic == CommunityLogic.OR:
                    for member in community_list.members:
                        yield self._arista_community_list(
                            name=name,
                            use_regex=community_list.use_regex,
                            comm_type=community_list.type,
                            members=member_prefix + member,
                        )
                else:
                    raise NotImplementedError(f"Community logic {community_list.logic} is not implemented for arista")

    def acl_iosxr(self, _) -> str:
        return r"""
        community-set *
            ~ %global=1
        extcommunity-set *
            ~ %global=1
        """

    def _iosxr_community_list(self, community_list: CommunityList) -> Iterator[Sequence[str]]:
        if community_list.type is CommunityType.BASIC:
            name = "community-set"
        elif community_list.type is CommunityType.RT:
            name = "extcommunity-set rt"
        elif community_list.type is CommunityType.SOO:
            name = "extcommunity-set soo"
        else:
            raise NotImplementedError(f"CommunityList type {community_list.type} not implemented for Cisco IOS XR")

        with self.block(name, community_list.name):
            for n, community in enumerate(community_list.members):
                if n + 1 < len(community_list.members):
                    comma = ","
                else:
                    comma = ""
                if community_list.use_regex:
                    yield "ios-regex", f"'{community}'"
                else:
                    yield (f"{community}{comma}",)

    def run_iosxr(self, device):
        for community_list in self.get_used_community_lists(device):
            yield from self._iosxr_community_list(community_list)

    def acl_juniper(self, _) -> str:
        return r"""
        policy-options  %cant_delete
            community ~
        """

    def _juniper_community_list(self, name: str, community_lists: list[CommunityList]) -> Iterator[Sequence[str]]:
        members: list[str] = []
        logic: set[CommunityLogic] = set()
        for community_list in community_lists:
            prefix: str
            if community_list.type is CommunityType.BASIC:
                prefix = ""
            elif community_list.type is CommunityType.RT:
                prefix = "target:"
            elif community_list.type is CommunityType.SOO:
                prefix = "origin:"
            elif community_list.type is CommunityType.LARGE:
                prefix = "large:"
            else:
                raise NotImplementedError(
                    f"CommunityList {name}: type {community_list.type} not implemented for Juniper"
                )

            logic.add(community_list.logic)
            for community in community_list.members:
                members.append(prefix + community)

        if len(members) > 1 and logic != {CommunityLogic.AND}:
            raise NotImplementedError(f"CommunityList {name}: only AND logic between members is implemeted for Juniper")

        definition = ["community", name, "members"]
        with self.block("policy-options"):
            if len(members) == 1:
                yield *definition, *members
            if len(members) > 1:
                yield *definition, "[", *members, "]"

    def run_juniper(self, device):
        # Juniper allows different community types
        # so we write generator in a generic way to reflect that.
        #
        # But get_used_community_lists DOES NOT allow multiple names
        # This is in part because juniper does not have a type-aware match
        # It would mean that there is no way to describe a following config via rpl.py:
        #
        #    CommunityList("COMM_LIST", ["65000:4000"], CommunityType.BASIC),
        #    CommunityList("COMM_LIST", ["65000:4000"], CommunityType.RT),
        #
        #    # match only route-target but not basic one
        #    with route(R.extcommunity_rt.has("COMM_LIST")) as rule:
        #       ...
        used = self.get_used_community_lists(device)
        by_name: Mapping[str, list[CommunityList]] = defaultdict(list)
        for community_list in used:
            by_name[community_list.name].append(community_list)

        for name, community_lists in by_name.items():
            yield from self._juniper_community_list(name, community_lists)
