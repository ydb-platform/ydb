"""ChaosTarget: unified descriptor of what a chaos inject/extract acts on.

Dispatch still goes to an agent ``host``; ``kind`` + ids say which entity on that host
(or cluster-wide tablet) is the real target. See CHAOS_TARGET_IMPLEMENTATION_PLAN.md.
"""

from __future__ import annotations

import enum
from dataclasses import asdict, dataclass
from typing import Any


class TargetKind(enum.Enum):
    HOST = "host"
    NODE = "node"
    SLOT = "slot"
    DISK = "disk"
    TABLET = "tablet"
    DATACENTER = "datacenter"
    PILE = "pile"


@dataclass(frozen=True)
class ChaosTarget:
    kind: TargetKind
    host: str
    node_id: int | None = None
    ic_port: int | None = None
    slot_idx: int | None = None
    disk_path: str | None = None
    tablet_id: int | None = None
    group_id: str | None = None  # datacenter name or pile id

    # -- factories ----------------------------------------------------------

    @classmethod
    def for_host(cls, host: str) -> "ChaosTarget":
        return cls(kind=TargetKind.HOST, host=host)

    @classmethod
    def for_node(
        cls,
        host: str,
        *,
        node_id: int | None = None,
        ic_port: int | None = None,
    ) -> "ChaosTarget":
        return cls(kind=TargetKind.NODE, host=host, node_id=node_id, ic_port=ic_port)

    @classmethod
    def for_slot(
        cls,
        host: str,
        *,
        slot_idx: int | None = None,
        ic_port: int | None = None,
        node_id: int | None = None,
    ) -> "ChaosTarget":
        return cls(
            kind=TargetKind.SLOT,
            host=host,
            slot_idx=slot_idx,
            ic_port=ic_port,
            node_id=node_id,
        )

    @classmethod
    def for_disk(
        cls,
        host: str,
        *,
        node_id: int | None = None,
        disk_path: str | None = None,
        ic_port: int | None = None,
    ) -> "ChaosTarget":
        return cls(
            kind=TargetKind.DISK,
            host=host,
            node_id=node_id,
            disk_path=disk_path,
            ic_port=ic_port,
        )

    @classmethod
    def for_tablet(cls, host: str, *, tablet_id: int | None = None) -> "ChaosTarget":
        return cls(kind=TargetKind.TABLET, host=host, tablet_id=tablet_id)

    @classmethod
    def for_datacenter(cls, host: str, datacenter: str) -> "ChaosTarget":
        return cls(kind=TargetKind.DATACENTER, host=host, group_id=datacenter)

    @classmethod
    def for_pile(cls, host: str, pile_id: str | int) -> "ChaosTarget":
        return cls(kind=TargetKind.PILE, host=host, group_id=str(pile_id))

    # -- identity / serde ---------------------------------------------------

    def identity_key(self) -> str:
        """Stable key for touched-set bookkeeping."""
        if self.kind is TargetKind.NODE:
            return f"node:{self.node_id if self.node_id is not None else self.ic_port}:{self.host}"
        if self.kind is TargetKind.SLOT:
            return f"slot:{self.slot_idx if self.slot_idx is not None else self.ic_port}:{self.host}"
        if self.kind is TargetKind.DISK:
            return f"disk:{self.node_id}:{self.disk_path or ''}:{self.host}"
        if self.kind is TargetKind.TABLET:
            return f"tablet:{self.tablet_id}" if self.tablet_id is not None else f"tablet:any@{self.host}"
        if self.kind is TargetKind.DATACENTER:
            return f"dc:{self.group_id}"
        if self.kind is TargetKind.PILE:
            return f"pile:{self.group_id}"
        return f"host:{self.host}"

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["kind"] = self.kind.value
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "ChaosTarget | None":
        if not data or not isinstance(data, dict):
            return None
        host = data.get("host")
        if not host:
            return None
        kind_raw = data.get("kind", TargetKind.HOST.value)
        try:
            kind = TargetKind(kind_raw) if not isinstance(kind_raw, TargetKind) else kind_raw
        except ValueError:
            kind = TargetKind.HOST
        return cls(
            kind=kind,
            host=str(host),
            node_id=_as_optional_int(data.get("node_id")),
            ic_port=_as_optional_int(data.get("ic_port")),
            slot_idx=_as_optional_int(data.get("slot_idx")),
            disk_path=_as_optional_str(data.get("disk_path")),
            tablet_id=_as_optional_int(data.get("tablet_id")),
            group_id=_as_optional_str(data.get("group_id")),
        )

    @classmethod
    def from_host_or_dict(cls, host: str | None = None, data: dict[str, Any] | None = None) -> "ChaosTarget":
        """Build from explicit target dict, else synthesize HOST target from ``host``."""
        t = cls.from_dict(data)
        if t is not None:
            return t
        if host:
            return cls.for_host(str(host))
        raise ValueError("ChaosTarget requires host or target dict")


def _as_optional_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _as_optional_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


__all__ = [
    "TargetKind",
    "ChaosTarget",
]
