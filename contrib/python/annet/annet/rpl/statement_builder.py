import warnings
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal, Optional, TypeVar, Union

from .action import ActionType, SingleAction
from .policy import RoutingPolicyStatement
from .result import ResultType


class ThenField(str, Enum):
    community = "community"
    large_community = "large_community"
    extcommunity_rt = "extcommunity_rt"
    extcommunity_soo = "extcommunity_soo"
    extcommunity = "extcommunity"
    as_path = "as_path"
    local_pref = "local_pref"
    metric = "metric"
    rpki_valid_state = "rpki_valid_state"
    resolution = "resolution"
    mpls_label = "mpls_label"
    metric_type = "metric_type"
    origin = "origin"
    tag = "tag"

    next_hop = "next_hop"


ValueT = TypeVar("ValueT")
_Setter = Callable[[ValueT], SingleAction[ValueT]]


@dataclass
class CommunityActionValue:
    replaced: Optional[list[str]] = None  # None means no replacement is done
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:  # check if any action required
        return bool(self.replaced is not None or self.added or self.removed)


class CommunityActionBuilder:
    def __init__(self, community: CommunityActionValue):
        self._community = community

    def add(self, *community: str) -> None:
        for c in community:
            self._community.added.append(c)

    def remove(self, *community: str) -> None:
        for c in community:
            self._community.removed.append(c)

    def set(self, *community: str) -> None:
        self._community.added.clear()
        self._community.removed.clear()
        self._community.replaced = list(community)


@dataclass
class AsPathActionValue:
    set: Optional[list[str]] = None  # None means no replacement is done
    prepend: list[str] = field(default_factory=list)
    expand: list[str] = field(default_factory=list)
    expand_last_as: str = ""
    delete: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:  # check if any action required
        return bool(self.set is not None or self.prepend or self.expand or self.expand_last_as or self.delete)


RawAsNum = Union[str, int]


class AsPathActionBuilder:
    def __init__(self, as_path_value: AsPathActionValue):
        self._as_path_value = as_path_value

    def prepend(self, *values: RawAsNum) -> None:
        self._as_path_value.prepend = list(map(str, values))

    def delete(self, *values: RawAsNum) -> None:
        self._as_path_value.delete = list(map(str, values))

    def expand(self, *values: RawAsNum) -> None:
        self._as_path_value.expand = list(map(str, values))

    def expand_last_as(self, value: RawAsNum) -> None:
        self._as_path_value.expand_last_as = str(value)

    def set(self, *values: RawAsNum) -> None:
        self._as_path_value.expand.clear()
        self._as_path_value.expand_last_as = ""
        self._as_path_value.delete.clear()
        self._as_path_value.prepend.clear()
        self._as_path_value.set = list(map(str, values))


@dataclass
class NextHopActionValue:
    target: Optional[Literal["self", "discard", "peer", "ipv4_addr", "ipv6_addr", "mapped_ipv4"]] = None
    addr: str = ""

    def __bool__(self) -> bool:
        return bool(self.target)


class NextHopActionBuilder:
    def __init__(self, next_hop_value: NextHopActionValue):
        self._next_hop_value = next_hop_value

    def ipv4_addr(self, value: str) -> None:
        self._next_hop_value.target = "ipv4_addr"
        self._next_hop_value.addr = value

    def ipv6_addr(self, value: str) -> None:
        self._next_hop_value.target = "ipv6_addr"
        self._next_hop_value.addr = value

    def mapped_ipv4(self, value: str) -> None:
        self._next_hop_value.target = "mapped_ipv4"
        self._next_hop_value.addr = value

    def self(self) -> None:
        self._next_hop_value.target = "self"
        self._next_hop_value.addr = ""

    def peer(self) -> None:
        self._next_hop_value.target = "peer"
        self._next_hop_value.addr = ""

    def discard(self) -> None:
        self._next_hop_value.target = "discard"
        self._next_hop_value.addr = ""


class StatementBuilder:
    def __init__(self, statement: RoutingPolicyStatement) -> None:
        self._statement = statement
        self._added_as_path: list[int] = []
        self._community = CommunityActionValue()
        self._large_community = CommunityActionValue()
        self._extcommunity = CommunityActionValue()
        self._extcommunity_rt = CommunityActionValue()
        self._extcommunity_soo = CommunityActionValue()
        self._as_path = AsPathActionValue()
        self._next_hop = NextHopActionValue()

    @property
    def next_hop(self) -> NextHopActionBuilder:
        return NextHopActionBuilder(self._next_hop)

    @property
    def as_path(self) -> AsPathActionBuilder:
        return AsPathActionBuilder(self._as_path)

    @property
    def community(self) -> CommunityActionBuilder:
        return CommunityActionBuilder(self._community)

    @property
    def large_community(self) -> CommunityActionBuilder:
        return CommunityActionBuilder(self._large_community)

    @property
    def extcommunity(self) -> CommunityActionBuilder:
        return CommunityActionBuilder(self._extcommunity)

    @property
    def extcommunity_rt(self) -> CommunityActionBuilder:
        warnings.warn("extcommunity_rt is deprecated, use extcommunity", DeprecationWarning, stacklevel=2)
        return CommunityActionBuilder(self._extcommunity_rt)

    @property
    def extcommunity_soo(self) -> CommunityActionBuilder:
        warnings.warn("extcommunity_soo is deprecated, use extcommunity", DeprecationWarning, stacklevel=2)
        return CommunityActionBuilder(self._extcommunity_soo)

    def _set(self, field: str, value: ValueT) -> None:
        action = self._statement.then
        if field in action:
            action[field].type = ActionType.SET
            action[field].value = value
        else:
            action.append(
                SingleAction(
                    field=field,
                    type=ActionType.SET,
                    value=value,
                )
            )

    def set_local_pref(self, value: int) -> None:
        self._set(ThenField.local_pref, value)

    def set_metric_type(self, value: str) -> None:
        self._set(ThenField.metric_type, value)

    def set_metric(self, value: int) -> None:
        self._set(ThenField.metric, value)

    def add_metric(self, value: int) -> None:
        action = self._statement.then
        field = ThenField.metric
        if field in action:
            old_action = action[field]
            if old_action.type == ActionType.SET:
                action[field].value += value
            elif old_action.type == ActionType.ADD:
                action[field].value = value
            else:
                raise RuntimeError(f"Unknown action type {old_action.type} for metric")
        else:
            action.append(
                SingleAction(
                    field=field,
                    type=ActionType.ADD,
                    value=value,
                )
            )

    def set_rpki_valid_state(self, value: str) -> None:
        self._set(ThenField.rpki_valid_state, value)

    def set_resolution(self, value: str) -> None:
        self._set(ThenField.resolution, value)

    def set_mpls_label(self) -> None:
        self._set(ThenField.mpls_label, True)

    def set_origin(self, value: str) -> None:
        self._set(ThenField.origin, value)

    def set_tag(self, value: int) -> None:
        self._set(ThenField.tag, value)

    def set_next_hop(self, value: Literal["self", "peer"]) -> None:  # ???
        self._set(ThenField.next_hop, value)

    def __enter__(self) -> "StatementBuilder":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._community:
            self._statement.then.append(
                SingleAction(
                    field=ThenField.community,
                    type=ActionType.CUSTOM,
                    value=self._community,
                )
            )
        if self._large_community:
            self._statement.then.append(
                SingleAction(
                    field=ThenField.large_community,
                    type=ActionType.CUSTOM,
                    value=self._large_community,
                )
            )
        if self._extcommunity:
            self._statement.then.append(
                SingleAction(
                    field=ThenField.extcommunity,
                    type=ActionType.CUSTOM,
                    value=self._extcommunity,
                )
            )
        if self._extcommunity_rt:
            self._statement.then.append(
                SingleAction(
                    field=ThenField.extcommunity_rt,
                    type=ActionType.CUSTOM,
                    value=self._extcommunity_rt,
                )
            )
        if self._extcommunity_soo:
            self._statement.then.append(
                SingleAction(
                    field=ThenField.extcommunity_soo,
                    type=ActionType.CUSTOM,
                    value=self._extcommunity_soo,
                )
            )
        if self._as_path:
            self._statement.then.append(
                SingleAction(
                    field=ThenField.as_path,
                    type=ActionType.CUSTOM,
                    value=self._as_path,
                )
            )
        if self._next_hop:
            self._statement.then.append(
                SingleAction(
                    field=ThenField.next_hop,
                    type=ActionType.CUSTOM,
                    value=self._next_hop,
                )
            )
        return None

    def allow(self) -> None:
        self._statement.result = ResultType.ALLOW

    def deny(self) -> None:
        self._statement.result = ResultType.DENY

    def next(self) -> None:
        self._statement.result = ResultType.NEXT

    def next_policy(self) -> None:
        self._statement.result = ResultType.NEXT_POLICY

    def add_as_path(self, *as_path: int) -> None:
        self._added_as_path.extend(as_path)

    def custom_action(self, action: SingleAction[Any]) -> None:
        self._statement.then.append(action)
