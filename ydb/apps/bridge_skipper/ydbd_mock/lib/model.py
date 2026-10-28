"""Cluster state model: enums, traits, the replicated snapshot, derived public view.

The public enum and status codes come from the generated protos. The stored enum
and the sync stage mirror ydb/core/protos/bridge.proto (the mock must not depend
on it), and they are never on the gRPC wire.
"""
import copy
import dataclasses
import enum
import typing

from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

# Public state values (ints). Never hard-code the numbers.
PUB_UNSPECIFIED = PileState.UNSPECIFIED
PUB_PRIMARY = PileState.PRIMARY
PUB_PROMOTED = PileState.PROMOTED
PUB_SYNCHRONIZED = PileState.SYNCHRONIZED
PUB_NOT_SYNCHRONIZED = PileState.NOT_SYNCHRONIZED
PUB_SUSPENDED = PileState.SUSPENDED
PUB_DISCONNECTED = PileState.DISCONNECTED
# Older schemas do not have the state used by the add/remove-pile API.
PUB_INACTIVE = getattr(PileState, "INACTIVE", None)

SUCCESS = StatusIds.SUCCESS
BAD_REQUEST = StatusIds.BAD_REQUEST
UNAUTHORIZED = StatusIds.UNAUTHORIZED
INTERNAL_ERROR = StatusIds.INTERNAL_ERROR
UNAVAILABLE = StatusIds.UNAVAILABLE
TIMEOUT = StatusIds.TIMEOUT
PRECONDITION_FAILED = StatusIds.PRECONDITION_FAILED
NOT_FOUND = StatusIds.NOT_FOUND
UNSUPPORTED = StatusIds.UNSUPPORTED
GENERIC_ERROR = StatusIds.GENERIC_ERROR


def public_name(value: int) -> str:
    """Name of a public PileState value, e.g. 'PRIMARY'. Unknown values print as the number."""
    try:
        return PileState.State.Name(value)
    except ValueError:
        return str(value)


def public_value(name: str) -> int:
    return PileState.State.Value(name)


class Stored(enum.IntEnum):
    """NKikimrBridge.TClusterState.EPileState."""
    DISCONNECTED = 0
    NOT_SYNCHRONIZED_1 = 1
    NOT_SYNCHRONIZED_2 = 2
    SYNCHRONIZED = 3
    SUSPENDED = 4
    INACTIVE = 5


class Stage(enum.IntEnum):
    """NKikimrBridge.TGroupState.EStage."""
    BLOCKS = 0
    WRITE_KEEP = 1
    WRITE_KEEP_BARRIER_DONOTKEEP = 2
    WRITE_KEEP_BARRIER_DONOTKEEP_DATA = 3
    SYNCED = 4


# (data quorum, config quorum, allows connection)
TRAITS = {
    Stored.SYNCHRONIZED: (True, True, True),
    Stored.NOT_SYNCHRONIZED_1: (False, False, True),
    Stored.NOT_SYNCHRONIZED_2: (True, True, True),
    Stored.DISCONNECTED: (False, False, False),
    Stored.SUSPENDED: (False, True, True),
    Stored.INACTIVE: (False, False, False),
}


def requires_data_quorum(stored: int) -> bool:
    return TRAITS[Stored(stored)][0]


def requires_config_quorum(stored: int) -> bool:
    return TRAITS[Stored(stored)][1]


def allows_connection(stored: int) -> bool:
    return TRAITS[Stored(stored)][2]


@dataclasses.dataclass
class ClusterState:
    """The replicated snapshot. Peers exchange it and each instance persists it."""
    per_pile: typing.List[int]
    primary: int
    promoted: int
    generation: int
    quorum_piles_last: typing.List[str] = dataclasses.field(default_factory=list)
    # Unsynced piles only: the generation at which the pile became unsynced.
    sync: typing.Dict[int, int] = dataclasses.field(default_factory=dict)
    # Storage-config revision is separate from the public bridge generation. BSC
    # completion/error notifications change details without changing that generation.
    revision: int = 0
    bridged: bool = True
    self_management: bool = True
    configured_piles: typing.Optional[typing.List[str]] = None
    registry: typing.Optional[typing.List[typing.Optional[str]]] = None
    conversion_in_progress: bool = False
    pending_pile_operation: typing.Optional[dict] = None

    def copy(self) -> "ClusterState":
        return ClusterState(
            per_pile=[Stored(x) for x in self.per_pile],
            primary=self.primary,
            promoted=self.promoted,
            generation=self.generation,
            quorum_piles_last=list(self.quorum_piles_last),
            sync=dict(self.sync),
            revision=self.revision,
            bridged=self.bridged,
            self_management=self.self_management,
            configured_piles=copy.deepcopy(self.configured_piles),
            registry=copy.deepcopy(self.registry),
            conversion_in_progress=self.conversion_in_progress,
            pending_pile_operation=copy.deepcopy(self.pending_pile_operation),
        )

    def to_json(self) -> dict:
        return {
            "per_pile": [int(x) for x in self.per_pile],
            "primary": self.primary,
            "promoted": self.promoted,
            "generation": self.generation,
            "quorum_piles_last": list(self.quorum_piles_last),
            "sync": {str(k): v for k, v in sorted(self.sync.items())},
            "revision": self.revision,
            "bridged": self.bridged,
            "self_management": self.self_management,
            "configured_piles": self.configured_piles,
            "registry": self.registry,
            "conversion_in_progress": self.conversion_in_progress,
            "pending_pile_operation": copy.deepcopy(self.pending_pile_operation),
        }

    @staticmethod
    def from_json(data: dict) -> "ClusterState":
        return ClusterState(
            per_pile=[Stored(x) for x in data["per_pile"]],
            primary=int(data["primary"]),
            promoted=int(data["promoted"]),
            generation=int(data["generation"]),
            quorum_piles_last=list(data.get("quorum_piles_last", [])),
            sync={int(k): int(v) for k, v in data.get("sync", {}).items()},
            revision=int(data.get("revision", data["generation"])),
            bridged=bool(data.get("bridged", True)),
            self_management=bool(data.get("self_management", True)),
            configured_piles=copy.deepcopy(data.get("configured_piles")),
            registry=copy.deepcopy(data.get("registry")),
            conversion_in_progress=bool(data.get("conversion_in_progress", False)),
            pending_pile_operation=copy.deepcopy(data.get("pending_pile_operation")),
        )

    def num_piles(self) -> int:
        return len(self.per_pile)

    def validate(self, num_piles: int) -> None:
        """Reject a snapshot that does not fit a cluster of `num_piles` piles (a peer is never trusted blindly)."""
        n = len(self.per_pile)
        if n != num_piles:
            raise ValueError("snapshot has %d piles, the cluster has %d" % (n, num_piles))
        if not 0 <= self.primary < n:
            raise ValueError("primary pile %d is out of range" % self.primary)
        if not 0 <= self.promoted < n:
            raise ValueError("promoted pile %d is out of range" % self.promoted)
        if self.generation < 1:
            raise ValueError("generation %d is not positive" % self.generation)
        if self.revision < 0:
            raise ValueError("negative storage revision")
        if self.registry is not None:
            names = [name for name in self.registry if name is not None]
            if len(names) != len(set(names)) or not all(isinstance(name, str) for name in names):
                raise ValueError("invalid pile registry")
            if self.configured_piles is None or set(names) != set(self.configured_piles):
                raise ValueError("registry does not match configured piles")
        if self.pending_pile_operation is not None:
            op = self.pending_pile_operation
            if op.get("kind") not in ("ADD", "REMOVE") or not isinstance(op.get("pile_name"), str):
                raise ValueError("invalid pending pile operation")
        for index in self.sync:
            if not 0 <= index < n:
                raise ValueError("sync record for pile %d is out of range" % index)

    def stored(self, index: int) -> Stored:
        return Stored(self.per_pile[index])

    @property
    def version(self) -> typing.Tuple[int, int]:
        return self.generation, self.revision

    def registered_names(self, inventory: typing.Sequence[str]) -> typing.List[str]:
        return list(inventory) if self.registry is None else [n for n in self.registry if n is not None]

    def describe(self, names: typing.Optional[typing.Sequence[str]] = None) -> str:
        parts = []
        for i, s in enumerate(self.per_pile):
            name = names[i] if names else str(i)
            parts.append("%s=%s" % (name, public_name(public_state(self, i))))
        return "gen=%d %s" % (self.generation, " ".join(parts))


def initial_state(num_piles: int) -> ClusterState:
    """Same as GenerateBridgeInitialState: all SYNCHRONIZED, pile 0 primary, generation 1."""
    return ClusterState(
        per_pile=[Stored.SYNCHRONIZED] * num_piles,
        primary=0,
        promoted=0,
        generation=1,
    )


def public_state(state: ClusterState, index: int) -> int:
    """GetPublicState from rpc_bridge.cpp. Primary is checked before promoted."""
    if index == state.primary:
        return PUB_PRIMARY
    if index == state.promoted:
        return PUB_PROMOTED
    stored = Stored(state.per_pile[index])
    if stored == Stored.DISCONNECTED:
        return PUB_DISCONNECTED
    if stored in (Stored.NOT_SYNCHRONIZED_1, Stored.NOT_SYNCHRONIZED_2):
        return PUB_NOT_SYNCHRONIZED
    if stored == Stored.SYNCHRONIZED:
        return PUB_SYNCHRONIZED
    if stored == Stored.INACTIVE:
        return PUB_INACTIVE
    return PUB_SUSPENDED


def public_states(state: ClusterState) -> typing.List[int]:
    return [public_state(state, i) for i in range(len(state.per_pile))]


def changed_piles(old: ClusterState, new: ClusterState) -> typing.List[int]:
    return [i for i in range(min(len(old.per_pile), len(new.per_pile))) if old.per_pile[i] != new.per_pile[i]]


def derive(
    old: ClusterState,
    *,
    per_pile: typing.Optional[typing.Sequence[int]] = None,
    updates: typing.Optional[typing.Dict[int, int]] = None,
    primary: typing.Optional[int] = None,
    promoted: typing.Optional[int] = None,
    quorum_piles: typing.Optional[typing.Sequence[str]] = None,
) -> ClusterState:
    """Build the next snapshot: generation + 1, with the sync record effects of the transition.

    `per_pile` replaces the whole vector, `updates` replaces individual entries.
    Sync record rules (the same for admin requests and follow-ups):
    - a pile that changed to DISCONNECTED or SUSPENDED is marked unsynced at the new generation, except
      SUSPENDED -> DISCONNECTED, which keeps the record of the suspension (NotifyBridgeSuspended);
    - a pile that changed to SYNCHRONIZED drops its record;
    - NOT_SYNCHRONIZED_1 and _2 leave the record untouched.
    """
    new = old.copy()
    if per_pile is not None:
        new.per_pile = [Stored(x) for x in per_pile]
    for index, value in (updates or {}).items():
        new.per_pile[index] = Stored(value)
    if primary is not None:
        new.primary = primary
    if promoted is not None:
        new.promoted = promoted
    if quorum_piles is not None:
        new.quorum_piles_last = list(quorum_piles)
    new.generation = old.generation + 1
    new.revision = old.revision + 1
    for i in changed_piles(old, new):
        now = new.stored(i)
        if now == Stored.DISCONNECTED and old.stored(i) == Stored.SUSPENDED and i in new.sync:
            continue
        if now in (Stored.DISCONNECTED, Stored.SUSPENDED):
            new.sync[i] = new.generation
        elif now == Stored.SYNCHRONIZED:
            new.sync.pop(i, None)
    return new


def derive_metadata(old: ClusterState) -> ClusterState:
    new = old.copy()
    new.revision += 1
    return new


def required_set(new: ClusterState) -> typing.List[int]:
    """Piles that must accept a commit: those whose NEW stored state has the config-quorum trait."""
    return [i for i, s in enumerate(new.per_pile) if requires_config_quorum(s)]


def push_targets(old: ClusterState, new: ClusterState) -> typing.List[int]:
    """Piles outside the required set that should still hear about the commit (best effort)."""
    required = set(required_set(new))
    out = []
    for i in range(len(new.per_pile)):
        if i in required:
            continue
        if allows_connection(new.per_pile[i]) or allows_connection(old.per_pile[i]):
            out.append(i)
    return out


class SyncTracker:
    """Local, non-replicated stage of the pile syncer and the BSC flag.

    Every instance applies the same rules to the same commit sequence, so they agree
    without replicating anything. The stage does not bump the generation.
    """

    def __init__(self) -> None:
        self.stage: typing.Dict[int, Stage] = {}
        self.bsc_cleared: typing.Dict[int, bool] = {}

    def on_commit(self, old: ClusterState, new: ClusterState) -> None:
        for i in changed_piles(old, new):
            now = new.stored(i)
            if now in (Stored.DISCONNECTED, Stored.SUSPENDED):
                self.stage[i] = Stage.BLOCKS
                self.bsc_cleared[i] = False
            elif now == Stored.SYNCHRONIZED:
                self.stage.pop(i, None)
                self.bsc_cleared.pop(i, None)
        # Entries that exist only for piles that are unsynced in the snapshot.
        for i in list(self.stage):
            if i not in new.sync:
                self.stage.pop(i, None)
                self.bsc_cleared.pop(i, None)
        for i in new.sync:
            if old.sync.get(i) != new.sync[i]:
                # A newer sync record starts fresh even when the stored state also changed during a pull.
                self.stage[i] = Stage.BLOCKS
                self.bsc_cleared[i] = False
            self.stage.setdefault(i, Stage.BLOCKS)
            self.bsc_cleared.setdefault(i, False)

    def reset_from(self, state: ClusterState) -> None:
        """Forget everything and re-seed from a snapshot (restart, or installing a far newer one)."""
        self.stage.clear()
        self.bsc_cleared.clear()
        for i in state.sync:
            self.stage[i] = Stage.BLOCKS
            self.bsc_cleared[i] = False

    def step(self, index: int) -> bool:
        """Advance one syncer stage. Returns True when something changed."""
        cur = self.stage.get(index, Stage.BLOCKS)
        if cur < Stage.SYNCED:
            self.stage[index] = Stage(cur + 1)
            return True
        if not self.bsc_cleared.get(index, False):
            self.bsc_cleared[index] = True
            return True
        return False

    def synced(self, index: int) -> bool:
        return self.stage.get(index, Stage.BLOCKS) == Stage.SYNCED

    def done(self, index: int) -> bool:
        return self.synced(index) and self.bsc_cleared.get(index, False)

    def to_json(self) -> dict:
        return {
            "stage": {str(i): Stage(s).name for i, s in sorted(self.stage.items())},
            "bsc_cleared": {str(i): v for i, v in sorted(self.bsc_cleared.items())},
        }


@dataclasses.dataclass(frozen=True)
class MockError:
    """An application error: it becomes a ready Operation with this status and message."""
    status: int
    message: str

    def __str__(self) -> str:
        return "%s: %s" % (StatusIds.StatusCode.Name(self.status), self.message)
