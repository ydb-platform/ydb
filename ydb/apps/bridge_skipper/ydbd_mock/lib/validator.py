"""UpdateClusterState validator.

A pure function that mirrors ydb/core/grpc_services/rpc_bridge.cpp and then
ydb/core/blobstorage/nodewarden/distconf_invoke_bridge.cpp, step by step and in the
same order, with the messages copied verbatim. The order decides which error a
client sees.

Pile ids in messages print as TBridgePileId does: the raw value, which is the pile
index plus one.
"""
import dataclasses
import typing

from ydb.apps.bridge_skipper.ydbd_mock.lib import model
from ydb.apps.bridge_skipper.ydbd_mock.lib.model import ClusterState, MockError, Stored

Update = typing.Tuple[str, int]


@dataclasses.dataclass
class Validation:
    state: typing.Optional[ClusterState] = None
    error: typing.Optional[MockError] = None

    @property
    def ok(self) -> bool:
        return self.error is None


def pile_id(index: int) -> int:
    """How TBridgePileId prints: FromPileIndex(i) is i + 1."""
    return index + 1


def _fail(status: int, message: str) -> Validation:
    return Validation(error=MockError(status, message))


def validate_request(updates: typing.Sequence[Update]) -> typing.Optional[MockError]:
    """Request-only checks (ValidateRequest in rpc_bridge.cpp)."""
    if not updates:
        return MockError(model.BAD_REQUEST, "Updates list cannot be empty")
    seen = set()
    primary = None
    promoted = None
    for name, state in updates:
        if name in seen:
            return MockError(model.BAD_REQUEST, "duplicate update for pile name# %s" % name)
        seen.add(name)
        if state == model.PUB_PRIMARY:
            if primary is not None:
                return MockError(model.BAD_REQUEST, "multiple primary piles are not allowed in a single request")
            primary = name
        elif state == model.PUB_PROMOTED:
            if promoted is not None:
                return MockError(model.BAD_REQUEST, "multiple promoted piles are not allowed in a single request")
            promoted = name
    return None


def _to_stored(public: int) -> typing.Optional[Stored]:
    if public in (model.PUB_PRIMARY, model.PUB_PROMOTED, model.PUB_SYNCHRONIZED):
        return Stored.SYNCHRONIZED
    if public == model.PUB_NOT_SYNCHRONIZED:
        return Stored.NOT_SYNCHRONIZED_1
    if public == model.PUB_DISCONNECTED:
        return Stored.DISCONNECTED
    if public == model.PUB_SUSPENDED:
        return Stored.SUSPENDED
    return None


def validate_update(
    current: ClusterState,
    updates: typing.Sequence[Update],
    quorum_piles: typing.Sequence[str],
    pile_names: typing.Sequence[str],
) -> Validation:
    """Validate an admin request against `current` and build the next snapshot."""
    err = validate_request(updates)
    if err is not None:
        return Validation(error=err)

    configured = current.registered_names(pile_names)
    name_to_index = {name: i for i, name in enumerate(pile_names) if name in configured}

    def display_id(index):
        return pile_id(index) if current.registry is None else current.registry.index(pile_names[index]) + 1

    # 5. Unknown names, before anything else in this group.
    for name, _ in updates:
        if name not in name_to_index:
            return _fail(model.BAD_REQUEST, "Invalid pile name# %s" % name)

    requested = {name_to_index[name]: state for name, state in updates}

    # 6. Updates to a suspended pile are prohibited, whatever the requested state is.
    for name, _ in updates:
        index = name_to_index[name]
        if current.stored(index) == Stored.SUSPENDED:
            return _fail(model.BAD_REQUEST, "cannot update suspended pile: %d" % display_id(index))
        if current.stored(index) == Stored.INACTIVE:
            return _fail(model.BAD_REQUEST, "pile is not added to the cluster: %s" % name)

    # Roles are derived publicly; omitted piles retain their exact internal state.
    new_per_pile: typing.List[Stored] = list(current.per_pile)
    final_primary: typing.Optional[int] = None
    final_promoted: typing.Optional[int] = None
    for i in range(current.num_piles()):
        public = requested.get(i)
        if public is None:
            public = model.public_state(current, i)
        if public == model.PUB_PRIMARY:
            if final_primary is not None:
                return _fail(
                    model.BAD_REQUEST,
                    "Multiple primary piles are not allowed, found %d and %d" % (display_id(final_primary), display_id(i)))
            final_primary = i
        elif public == model.PUB_PROMOTED:
            if final_promoted is not None:
                return _fail(
                    model.BAD_REQUEST,
                    "Multiple promoted piles are not allowed, found %d and %d" % (display_id(final_promoted), display_id(i)))
            final_promoted = i
        if i not in requested:
            continue
        if model.PUB_INACTIVE is not None and public == model.PUB_INACTIVE:
            return _fail(
                model.BAD_REQUEST,
                "Pile can't be switched to INACTIVE state, it has to be removed from the cluster instead")
        stored = _to_stored(public)
        if stored is None:
            return _fail(model.INTERNAL_ERROR, "Unsupported pile state")
        new_per_pile[i] = stored

    # 8. One primary is mandatory.
    if final_primary is None:
        return _fail(model.BAD_REQUEST, "Request must result in a state with one primary pile")
    # 9. Promoted defaults to primary.
    if final_promoted is None:
        final_promoted = final_primary

    # 10. Neither may be suspended.
    if new_per_pile[final_primary] == Stored.SUSPENDED:
        return _fail(model.BAD_REQUEST, "primary pile cannot be suspended")
    if new_per_pile[final_promoted] == Stored.SUSPENDED:
        return _fail(model.BAD_REQUEST, "promoted pile cannot be suspended")

    # 12. quorum_piles (step 11, the generation bump, happens in derive()).
    for name in quorum_piles:
        index = name_to_index.get(name)
        if index is None:
            return _fail(model.BAD_REQUEST, "Unknown pile name in quorum: %s" % name)
        if new_per_pile[index] == Stored.SUSPENDED:
            return _fail(model.BAD_REQUEST, "quorum cannot include suspended pile: %s" % name)

    # 13. Distconf: the new config alone.
    if new_per_pile[final_primary] != Stored.SYNCHRONIZED:
        return _fail(model.INTERNAL_ERROR, "Incorrect primary pile state")
    if new_per_pile[final_promoted] != Stored.SYNCHRONIZED:
        return _fail(model.INTERNAL_ERROR, "Incorrect promoted pile state")

    # 14. Distconf: each changed transition, in pile order. The first bad one wins.
    num_different = 0
    for i in range(current.num_piles()):
        cur = current.stored(i)
        new = new_per_pile[i]
        if cur == new:
            continue
        num_different += 1
        pending = current.pending_pile_operation
        if pending and pending["pile_name"] == pile_names[i]:
            return _fail(model.INTERNAL_ERROR,
                         "Pile '%s' is being %s the cluster, its state can't be changed until BSC finishes" % (
                             pile_names[i], "added to" if pending["kind"] == "ADD" else "removed from"))
        if new == Stored.DISCONNECTED or new == Stored.SUSPENDED:
            continue
        if new == Stored.SYNCHRONIZED:
            return _fail(model.INTERNAL_ERROR, "Can't switch to SYNCHRONIZED directly")
        if new == Stored.NOT_SYNCHRONIZED_1:
            if cur == Stored.SYNCHRONIZED:
                return _fail(model.INTERNAL_ERROR, "Invalid transition from SYNCHRONIZED to NOT_SYNCHRONIZED_1")
            continue
        if new == Stored.NOT_SYNCHRONIZED_2:
            return _fail(model.INTERNAL_ERROR, "Can't switch to NOT_SYNCHRONIZED_2 directly")

    # 15. More than one stored change.
    if num_different > 1 and any(new_per_pile[i] != Stored.DISCONNECTED
                                 for i in range(current.num_piles()) if current.stored(i) != new_per_pile[i]):
        return _fail(model.INTERNAL_ERROR, "Too many state changes in new configuration")

    new_state = model.derive(
        current,
        per_pile=new_per_pile,
        primary=final_primary,
        promoted=final_promoted,
        quorum_piles=list(quorum_piles),
    )
    return Validation(state=new_state)
