from dataclasses import dataclass
from typing import Any, Callable, Sequence

from .basemodel import is_dataclass_empty
from .device_models import GlobalOptionsDTO
from .match_args import MatchedArgs, MatchExpr, PairMatcher, SingleMatcher
from .peer_models import DirectPeerDTO, IndirectPeerDTO, MeshSession, VirtualLocalDTO, VirtualPeerDTO
from .port_processor import PortProcessor, united_ports


class DirectPeer(DirectPeerDTO):
    match: MatchedArgs
    device: Any
    ports: list[str]
    all_connected_ports: list[str]

    def __init__(self, match: MatchedArgs, device: Any, ports: list[str], all_connected_ports: list[str]) -> None:
        super().__init__()
        self.match = match
        self.device = device
        self.ports = ports
        self.all_connected_ports = all_connected_ports

    def is_empty(self):
        if not is_dataclass_empty(self.family_options):
            return False
        return self.__dict__.keys() == {"match", "device", "ports", "all_connected_ports", "family_options"}


class IndirectPeer(IndirectPeerDTO):
    match: MatchedArgs
    device: Any

    def __init__(self, match: MatchedArgs, device: Any) -> None:
        super().__init__()
        self.match = match
        self.device = device

    def is_empty(self):
        if not is_dataclass_empty(self.family_options):
            return False
        return self.__dict__.keys() == {"match", "device", "family_options"}


class VirtualLocal(VirtualLocalDTO):
    match: MatchedArgs
    device: Any

    def __init__(self, match: MatchedArgs, device: Any) -> None:
        super().__init__()
        self.match = match
        self.device = device

    def is_empty(self):
        if not is_dataclass_empty(self.family_options):
            return False
        return self.__dict__.keys() == {"match", "device", "family_options"}


class VirtualPeer(VirtualPeerDTO):
    num: int

    def is_empty(self):
        return self.__dict__.keys() == {"num"}


class GlobalOptions(GlobalOptionsDTO):
    match: MatchedArgs
    device: Any

    def __init__(self, match: MatchedArgs, device: Any) -> None:
        super().__init__()
        self.match = match
        self.device = device

    def is_empty(self):
        return self.__dict__.keys() == {"match", "device"}


GlobalHandler = Callable[[GlobalOptions], None]


@dataclass
class GlobalRule:
    matcher: SingleMatcher
    handler: GlobalHandler


DirectHandler = Callable[[DirectPeer, DirectPeer, MeshSession], None]
IndirectHandler = Callable[[IndirectPeer, IndirectPeer, MeshSession], None]
VirtualHandler = Callable[[VirtualLocal, VirtualPeer, MeshSession], None]


@dataclass
class DirectRule:
    __slots__ = ("matcher", "handler", "port_processor")
    matcher: PairMatcher
    handler: DirectHandler
    port_processor: PortProcessor


@dataclass
class IndirectRule:
    __slots__ = ("matcher", "handler")
    matcher: PairMatcher
    handler: IndirectHandler


@dataclass
class VirtualRule:
    __slots__ = ("matcher", "num", "handler")
    matcher: SingleMatcher
    num: Sequence[int]
    handler: VirtualHandler


@dataclass
class MatchedGlobal:
    __slots__ = ("match", "handler")
    match: MatchedArgs
    handler: GlobalHandler


@dataclass
class MatchedDirectPair:
    __slots__ = ("handler", "port_processor", "direct_order", "name_left", "name_right", "match_left", "match_right")
    handler: DirectHandler
    port_processor: PortProcessor
    direct_order: bool
    name_left: str
    name_right: str
    match_left: MatchedArgs
    match_right: MatchedArgs


@dataclass
class MatchedIndirectPair:
    __slots__ = ("handler", "direct_order", "name_left", "name_right", "match_left", "match_right")
    handler: IndirectHandler
    direct_order: bool
    name_left: str
    name_right: str
    match_left: MatchedArgs
    match_right: MatchedArgs


@dataclass
class MatchedVirtualPair:
    __slots__ = ("match", "num", "handler")
    match: MatchedArgs
    num: Sequence[int]
    handler: VirtualHandler


class MeshRulesRegistry:
    def __init__(self, match_short_name: bool = False):
        self.direct_rules: list[DirectRule] = []
        self.indirect_rules: list[IndirectRule] = []
        self.global_rules: list[GlobalRule] = []
        self.virtual_rules: list[VirtualRule] = []
        self.nested: list[MeshRulesRegistry] = []
        self.match_short_name = match_short_name

    def _normalize_host(self, host: str) -> str:
        if self.match_short_name:
            return host.split(".", maxsplit=1)[0]
        return host

    def include(self, nested_registry: "MeshRulesRegistry") -> None:
        self.nested.append(nested_registry)

    def device(self, peer_mask: str, *match: MatchExpr) -> Callable[[GlobalHandler], GlobalHandler]:
        matcher = SingleMatcher(peer_mask, match)

        def register(handler: GlobalHandler) -> GlobalHandler:
            self.global_rules.append(GlobalRule(matcher, handler))
            return handler

        return register

    def direct(
        self,
        left_mask: str,
        right_mask: str,
        *match: MatchExpr,
        port_processor: PortProcessor = united_ports,
    ) -> Callable[[DirectHandler], DirectHandler]:
        matcher = PairMatcher(left_mask, right_mask, match)

        def register(handler: DirectHandler) -> DirectHandler:
            self.direct_rules.append(DirectRule(matcher, handler, port_processor))
            return handler

        return register

    def indirect(
        self,
        left_mask: str,
        right_mask: str,
        *match: MatchExpr,
    ) -> Callable[[IndirectHandler], IndirectHandler]:
        matcher = PairMatcher(left_mask, right_mask, match)

        def register(handler: IndirectHandler) -> IndirectHandler:
            self.indirect_rules.append(IndirectRule(matcher, handler))
            return handler

        return register

    def virtual(
        self,
        peer_mask: str,
        num: Sequence[int],
        *match: MatchExpr,
    ) -> Callable[[VirtualHandler], VirtualHandler]:
        matcher = SingleMatcher(peer_mask, match)

        def register(handler: VirtualHandler) -> VirtualHandler:
            self.virtual_rules.append(VirtualRule(matcher, num, handler))
            return handler

        return register

    def lookup_direct(self, device: str, neighbors: list[str]) -> list[MatchedDirectPair]:
        found = []
        device_norm = self._normalize_host(device)
        for neighbor in neighbors:
            neighbor_norm = self._normalize_host(neighbor)
            for rule in self.direct_rules:
                if args := rule.matcher.match_pair(device_norm, neighbor_norm):
                    found.append(
                        MatchedDirectPair(
                            handler=rule.handler,
                            port_processor=rule.port_processor,
                            direct_order=True,
                            name_left=device,
                            name_right=neighbor,
                            match_left=args[0],
                            match_right=args[1],
                        )
                    )
                if args := rule.matcher.match_pair(neighbor_norm, device_norm):
                    found.append(
                        MatchedDirectPair(
                            handler=rule.handler,
                            port_processor=rule.port_processor,
                            direct_order=False,
                            name_left=neighbor,
                            name_right=device,
                            match_left=args[0],
                            match_right=args[1],
                        )
                    )
        for registry in self.nested:
            found.extend(registry.lookup_direct(device, neighbors))
        return found

    def lookup_indirect(self, device: str, devices: list[str]) -> list[MatchedIndirectPair]:
        found = []
        device_norm = self._normalize_host(device)
        for other_device in devices:
            other_device_norm = self._normalize_host(other_device)
            for rule in self.indirect_rules:
                if args := rule.matcher.match_pair(device_norm, other_device_norm):
                    found.append(
                        MatchedIndirectPair(
                            handler=rule.handler,
                            direct_order=True,
                            name_left=device,
                            name_right=other_device,
                            match_left=args[0],
                            match_right=args[1],
                        )
                    )
                if args := rule.matcher.match_pair(other_device_norm, device_norm):
                    found.append(
                        MatchedIndirectPair(
                            handler=rule.handler,
                            direct_order=False,
                            name_left=other_device,
                            name_right=device,
                            match_left=args[0],
                            match_right=args[1],
                        )
                    )
        for registry in self.nested:
            found.extend(registry.lookup_indirect(device, devices))
        return found

    def lookup_virtual(self, device: str) -> list[MatchedVirtualPair]:
        found = []
        device_norm = self._normalize_host(device)
        for rule in self.virtual_rules:
            if args := rule.matcher.match_one(device_norm):
                found.append(
                    MatchedVirtualPair(
                        handler=rule.handler,
                        match=args,
                        num=rule.num,
                    )
                )
        for registry in self.nested:
            found.extend(registry.lookup_virtual(device))
        return found

    def lookup_global(self, device: str) -> list[MatchedGlobal]:
        found = []
        device_norm = self._normalize_host(device)
        for rule in self.global_rules:
            if args := rule.matcher.match_one(device_norm):
                found.append(
                    MatchedGlobal(
                        handler=rule.handler,
                        match=args,
                    )
                )
        for registry in self.nested:
            found.extend(registry.lookup_global(device))
        return found
