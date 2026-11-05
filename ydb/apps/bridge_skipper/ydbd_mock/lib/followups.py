"""Automatic follow-ups: which are due for a snapshot, and what each commit produces.

Pure rules, no timers and no I/O. The instance arms a timer per due follow-up and
re-checks the condition when it fires.

| kind         | condition                                         | effect                         |
| promote      | promoted != primary, promoted stored SYNCHRONIZED | commit: primary = promoted     |
| suspend      | pile stored SUSPENDED                             | commit: pile -> DISCONNECTED   |
| connect      | pile stored NOT_SYNCHRONIZED_1 and connected      | commit: pile -> _2             |
| sync_step    | pile _2, connected, stage < SYNCED                | local tracker only, no commit  |
| bsc_done     | pile _2, connected, stage SYNCED, BSC not cleared | local tracker only, no commit  |
| synchronized | pile _2, stage SYNCED and BSC cleared             | commit: pile -> SYNCHRONIZED   |
"""
import dataclasses
import typing

from ydb.apps.bridge_skipper.ydbd_mock.lib import model
from ydb.apps.bridge_skipper.ydbd_mock.lib.model import ClusterState, Stored, SyncTracker

PROMOTE = "promote"
SUSPEND = "suspend"
CONNECT = "connect"
SYNC_STEP = "sync_step"
BSC_DONE = "bsc_done"
SYNCHRONIZED = "synchronized"
CONVERT = "convert"
ADD = "add"
REMOVE = "remove"

COMMITTING = (PROMOTE, SUSPEND, CONNECT, SYNCHRONIZED, CONVERT, ADD, REMOVE)
KINDS = COMMITTING + (SYNC_STEP, BSC_DONE)


@dataclasses.dataclass(frozen=True)
class Due:
    kind: str
    pile: int

    @property
    def key(self) -> typing.Tuple[str, int]:
        return (self.kind, self.pile)

    @property
    def commits(self) -> bool:
        return self.kind in COMMITTING


def due_followups(
    state: ClusterState,
    tracker: SyncTracker,
    connected: typing.Callable[[int], bool],
) -> typing.List[Due]:
    out: typing.List[Due] = []
    if not state.bridged:
        return out
    if state.conversion_in_progress:
        out.append(Due(CONVERT, state.primary))
    if state.pending_pile_operation:
        op = state.pending_pile_operation
        out.append(Due(op["kind"].lower(), op["pile_index"]))
    if state.promoted != state.primary and state.stored(state.promoted) == Stored.SYNCHRONIZED:
        out.append(Due(PROMOTE, state.promoted))
    for i in range(state.num_piles()):
        stored = state.stored(i)
        if stored == Stored.SUSPENDED:
            out.append(Due(SUSPEND, i))
        elif stored == Stored.NOT_SYNCHRONIZED_1:
            if connected(i):
                out.append(Due(CONNECT, i))
        elif stored == Stored.NOT_SYNCHRONIZED_2:
            if tracker.done(i):
                out.append(Due(SYNCHRONIZED, i))
            elif connected(i):
                if tracker.synced(i):
                    out.append(Due(BSC_DONE, i))
                else:
                    out.append(Due(SYNC_STEP, i))
    return out


def base_delay(kind: str, timings) -> float:
    return {
        PROMOTE: timings.promote_delay,
        SUSPEND: timings.suspend_delay,
        CONNECT: timings.connect_delay,
        SYNC_STEP: timings.sync_step_delay,
        BSC_DONE: timings.sync_step_delay,
        SYNCHRONIZED: 0.0,
        CONVERT: timings.lifecycle_delay,
        ADD: timings.lifecycle_delay,
        REMOVE: timings.lifecycle_delay,
    }[kind]


def delay_for(due: Due, self_index: int, timings) -> float:
    """The lowest-index instance normally wins the race."""
    return base_delay(due.kind, timings) + self_index * timings.followup_stagger


def build_commit(state: ClusterState, due: Due) -> typing.Optional[ClusterState]:
    """The snapshot a committing follow-up proposes, or None if its condition no longer holds."""
    if due.kind == CONVERT:
        if not state.conversion_in_progress:
            return None
        new = model.derive_metadata(state)
        new.conversion_in_progress = False
        return new
    if due.kind in (ADD, REMOVE):
        op = state.pending_pile_operation
        if not op or op["pile_index"] != due.pile or op["kind"].lower() != due.kind:
            return None
        new = (model.derive(state, updates={due.pile: Stored.INACTIVE}) if due.kind == REMOVE
               else model.derive_metadata(state))
        new.pending_pile_operation = None
        if due.kind == REMOVE:
            new.sync.pop(due.pile, None)
        return new
    if due.kind == PROMOTE:
        if state.promoted != due.pile or state.promoted == state.primary:
            return None
        if state.stored(due.pile) != Stored.SYNCHRONIZED:
            return None
        return model.derive(state, primary=due.pile)
    if due.kind == SUSPEND:
        if state.stored(due.pile) != Stored.SUSPENDED:
            return None
        return model.derive(state, updates={due.pile: Stored.DISCONNECTED})
    if due.kind == CONNECT:
        if state.stored(due.pile) != Stored.NOT_SYNCHRONIZED_1:
            return None
        return model.derive(state, updates={due.pile: Stored.NOT_SYNCHRONIZED_2})
    if due.kind == SYNCHRONIZED:
        if state.stored(due.pile) != Stored.NOT_SYNCHRONIZED_2:
            return None
        return model.derive(state, updates={due.pile: Stored.SYNCHRONIZED})
    return None
