"""One instance's copy of the cluster state, with the prepared slot and persistence."""
import dataclasses
import json
import os
import tempfile
import time
import typing

from ydb.apps.bridge_skipper.ydbd_mock.lib import model
from ydb.apps.bridge_skipper.ydbd_mock.lib.model import ClusterState


class ReplicaError(ValueError):
    """The persisted replica exists but cannot be used."""


@dataclasses.dataclass
class Prepared:
    proposal_id: str
    state: ClusterState
    sender: int
    since: float


class Replica:
    def __init__(
        self,
        num_piles: int,
        path: typing.Optional[str] = None,
        clock: typing.Callable[[], float] = time.monotonic,
    ) -> None:
        self.num_piles = num_piles
        self.path = path
        self._clock = clock
        self.state: ClusterState = model.initial_state(num_piles)
        self.max_generation = self.state.generation
        self.detached = False
        self.prepared: typing.Optional[Prepared] = None

    # ----- persistence -----

    def load(self) -> bool:
        """Load the persisted replica. Returns False and keeps the initial snapshot when there is no file.

        A file that exists but cannot be used raises ReplicaError: bootstrapping over it would make the
        pile forget its generation, and the demo refuses a cluster that is behind its own state file.
        """
        if not self.path or not os.path.exists(self.path):
            return False
        try:
            with open(self.path) as f:
                data = json.load(f)
            state = ClusterState.from_json(data["state"])
            state.validate(self.num_piles)
            max_generation = int(data.get("max_generation", state.generation))
            if max_generation < state.generation:
                raise ValueError("maximum generation is below the replica generation")
        except (OSError, ValueError, KeyError, TypeError) as e:
            raise ReplicaError("cannot use the replica file %s (%s: %s); fix or remove it" % (
                self.path, type(e).__name__, e))
        self.state = state
        self.max_generation = max_generation
        self.detached = bool(data.get("detached", False))
        return True

    def save(
        self,
        state: typing.Optional[ClusterState] = None,
        detached: typing.Optional[bool] = None,
    ) -> None:
        """Persist a candidate before publishing it, or save the current replica at startup."""
        state = self.state if state is None else state
        detached = self.detached if detached is None else detached
        max_generation = max(self.max_generation, state.generation)
        if not self.path:
            self.max_generation = max_generation
            return
        directory = os.path.dirname(self.path) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".replica.", dir=directory)
        try:
            with os.fdopen(fd, "w") as f:
                json.dump({"state": state.to_json(), "detached": detached,
                           "max_generation": max_generation}, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        self.max_generation = max_generation

    # ----- state changes -----

    def install(self, new: ClusterState) -> bool:
        """Install a snapshot with a higher generation. Returns False if it was not newer or we are detached."""
        if self.detached or new.version <= self.state.version:
            return False
        candidate = new.copy()
        self.save(candidate)
        self.state = candidate
        if self.prepared is not None and self.prepared.state.version <= new.version:
            self.prepared = None
        return True

    def force(self, new: ClusterState, detached: bool) -> None:
        """Override from the control API: replace the snapshot whatever its generation."""
        candidate = new.copy()
        self.save(candidate, detached)
        self.state = candidate
        self.detached = detached
        self.prepared = None

    def set_detached(self, detached: bool) -> None:
        self.save(detached=detached)
        self.detached = detached

    def reset(self, generation: int) -> None:
        state = model.initial_state(self.num_piles)
        state.generation = generation
        self.force(state, detached=False)

    # ----- two-phase commit, receiver side -----

    def expire_prepared(self, ttl: float) -> bool:
        if self.prepared is not None and self._clock() - self.prepared.since > ttl:
            self.prepared = None
            return True
        return False

    def try_prepare(
        self,
        proposal_id: str,
        new: ClusterState,
        sender: int,
        ttl: float,
    ) -> typing.Tuple[bool, typing.Optional[str]]:
        """Returns (accepted, refusal reason). Reasons: detached, generation, busy."""
        if self.detached:
            return False, "detached"
        if new.revision != self.state.revision + 1 or new.generation not in (
                self.state.generation, self.state.generation + 1):
            return False, "generation"
        self.expire_prepared(ttl)
        if self.prepared is not None and self.prepared.proposal_id != proposal_id:
            return False, "busy"
        self.prepared = Prepared(proposal_id, new.copy(), sender, self._clock())
        return True, None

    def abort(self, proposal_id: str) -> None:
        if self.prepared is not None and self.prepared.proposal_id == proposal_id:
            self.prepared = None

    def blocks_commit(self, proposal_id: str, new: ClusterState) -> bool:
        """Another proposal holds the slot at (or beyond) the generation of a commit that was never prepared here."""
        p = self.prepared
        return p is not None and p.proposal_id != proposal_id and p.state.version >= new.version

    def take_prepared(self, proposal_id: str) -> typing.Optional[ClusterState]:
        if self.prepared is not None and self.prepared.proposal_id == proposal_id:
            state = self.prepared.state
            self.prepared = None
            return state
        return None
