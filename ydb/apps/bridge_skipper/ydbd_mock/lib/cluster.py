"""PileInstance: one pile of the mock bridge cluster.

It owns the replica, the commit pipeline (two-phase commit with a compare-and-set on
the generation), the follow-up scheduler, the heartbeat and anti-entropy loops, and the
peer handlers. Everything runs on one asyncio loop, with no `await` between a check and
the write it guards, so no locks are needed.
"""
import asyncio
import dataclasses
import itertools
import random
import time
import typing

from ydb.apps.bridge_skipper.ydbd_mock.lib import followups, lifecycle, model, validator
from ydb.apps.bridge_skipper.ydbd_mock.lib.connectivity import Reachability
from ydb.apps.bridge_skipper.ydbd_mock.lib.events import EventLog
from ydb.apps.bridge_skipper.ydbd_mock.lib.faults import NodeFaults
from ydb.apps.bridge_skipper.ydbd_mock.lib.model import ClusterState, MockError, Stored, SyncTracker
from ydb.apps.bridge_skipper.ydbd_mock.lib.peers import PeerBlocked, PeerError, Transport
from ydb.apps.bridge_skipper.ydbd_mock.lib.replica import Replica
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Topology


@dataclasses.dataclass
class Outcome:
    """Result of a proposal: the committed snapshot, an application error, or 'no longer needed'."""
    state: typing.Optional[ClusterState] = None
    error: typing.Optional[MockError] = None
    skipped: bool = False

    @property
    def ok(self) -> bool:
        return self.state is not None


@dataclasses.dataclass
class _PhaseResult:
    committed: bool
    refusals: typing.Dict[int, dict]


@dataclasses.dataclass
class _Timer:
    due: followups.Due
    due_at: float
    task: "asyncio.Task"
    started: bool = False


class PileInstance:
    def __init__(
        self,
        topology: Topology,
        pile: typing.Union[str, int],
        transport: Transport,
        state_dir: typing.Optional[str] = None,
        rng: typing.Optional[random.Random] = None,
        events: typing.Optional[EventLog] = None,
    ) -> None:
        self.topo = topology
        self.index = pile if isinstance(pile, int) else topology.pile_index(pile)
        self.name = topology.pile_names[self.index]
        self.names = list(topology.pile_names)
        self.transport = transport
        # per instance: the control API changes them on one pile only (in-process tests share a topology)
        self.timings = dataclasses.replace(topology.timings)
        self.policies = dataclasses.replace(topology.policies)
        self.rng = rng or random.Random()
        self.events = events or EventLog(self.name)
        self.pile_info = topology.pile(self.index)
        self.node_faults: typing.List[NodeFaults] = [NodeFaults() for _ in self.pile_info.nodes]

        path = None
        if state_dir:
            path = "%s/replica.json" % state_dir
        self.replica = Replica(len(self.names), path)
        if topology.initial:
            self.replica.state = lifecycle.configure(self.replica.state, topology.initial, self.names, initial=True)
        self.tracker = SyncTracker()
        self.reach = Reachability(
            self.index, len(self.names), lambda: self.timings.heartbeat_timeout, own_ready=self.ready)

        self._running = False
        self._reset_epoch = 0
        self._reset_pending = False
        self._timers: typing.Dict[typing.Tuple[str, int], _Timer] = {}
        self._retry_not_before: typing.Dict[typing.Tuple[str, int], float] = {}
        self._refused_pull: typing.Dict[int, typing.Tuple[int, int]] = {}
        self._bg: typing.Set["asyncio.Task"] = set()
        self._aborts: typing.Set["asyncio.Task"] = set()   # aborts in flight: stop() lets them finish
        # called (synchronously) when the own stored state or a node fault changed: the process re-evaluates
        # which gRPC listeners may run
        self.on_listeners_changed: typing.Optional[typing.Callable[[], None]] = None
        self._heartbeat_task: typing.Optional["asyncio.Task"] = None
        self._pid = itertools.count(1)
        self._last_ready = True
        self._peer_versions = {}
        self._held_followups = set()
        self._lifecycle_error = None
        self._state_changed = asyncio.Event()

    # ----- lifecycle -----

    async def start(self) -> None:
        loaded = self.replica.load()
        if not loaded:
            self.replica.save()  # persist the bootstrap snapshot, so a restart finds it
        self.tracker.reset_from(self.replica.state)
        self.reach.restart()
        self._running = True
        self._last_ready = self.ready()
        self.events.append(
            "start", loaded=loaded, generation=self.replica.state.generation, detached=self.replica.detached,
            state=self.replica.state.describe(self.names))
        self._heartbeat_task = asyncio.get_running_loop().create_task(self._heartbeat_loop())
        self._rearm()
        self._notify_listeners()

    def serves(self) -> bool:
        """run.cpp: a node serves gRPC only while its own pile has the config-quorum trait."""
        if not self.policies.close_grpc_when_disconnected or not self.state.bridged:
            return True
        return model.requires_config_quorum(self.replica.state.stored(self.index))

    def _notify_listeners(self) -> None:
        if self.on_listeners_changed is not None:
            self.on_listeners_changed()

    async def stop(self) -> None:
        self._running = False
        tasks = [t.task for t in self._timers.values()]
        self._timers.clear()
        if self._heartbeat_task is not None:
            tasks.append(self._heartbeat_task)
            self._heartbeat_task = None
        tasks.extend(self._bg)
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._bg.clear()
        if self._aborts:   # a cancelled proposal tells the peers to drop their prepared slots
            await asyncio.wait(list(self._aborts), timeout=self.timings.peer_timeout + 0.2)
        self.events.append("stop")

    # ----- views -----

    @property
    def state(self) -> ClusterState:
        return self.replica.state

    def ready(self) -> bool:
        """A strict majority of the nodes of this pile are not down."""
        up = sum(1 for f in self.node_faults if not f.down)
        return up * 2 > len(self.node_faults)

    def connected(self, pile: int) -> bool:
        return self.reach.connected(pile)

    def vector(self, state: typing.Optional[ClusterState] = None) -> typing.Dict[str, str]:
        state = state or self.replica.state
        return {self.names[i]: model.public_name(model.public_state(state, i)) for i in range(len(self.names))}

    def pending_followups(self) -> typing.List[dict]:
        loop_now = asyncio.get_running_loop().time()
        out = []
        for key, timer in sorted(self._timers.items()):
            out.append({
                "kind": timer.due.kind,
                "pile": self.names[timer.due.pile],
                "due_in": round(max(0.0, timer.due_at - loop_now), 3),
                "started": timer.started,
            })
        return out

    def state_json(self) -> dict:
        s = self.replica.state
        piles = []
        for i, name in enumerate(self.names):
            sync = None
            if i in s.sync or s.stored(i) in (Stored.NOT_SYNCHRONIZED_1, Stored.NOT_SYNCHRONIZED_2):
                sync = {
                    "stage": model.Stage(self.tracker.stage.get(i, model.Stage.BLOCKS)).name,
                    "bsc_cleared": self.tracker.bsc_cleared.get(i, False),
                    "became_unsynced_generation": s.sync.get(i),
                }
            piles.append({
                "name": name,
                "index": i,
                "registry_slot": ((s.registry or self.names).index(name)
                                  if name in (self.names if s.registry is None else s.registry) else None),
                "configured": name in lifecycle.configured(s, self.names),
                "public": model.public_name(model.public_state(s, i)),
                "stored": s.stored(i).name,
                "connected": self.connected(i),
                "sync": sync,
            })
        return {
            "cluster_name": self.topo.cluster_name,
            "pile": self.name,
            "generation": s.generation,
            "storage_revision": s.revision,
            "bridged": s.bridged,
            "self_management": s.self_management,
            "configured_piles": lifecycle.configured(s, self.names),
            "registry": list(self.names) if s.registry is None else list(s.registry),
            "conversion_in_progress": s.conversion_in_progress,
            "pending_pile_operation": s.pending_pile_operation,
            "held_followups": sorted(self._held_followups),
            "lifecycle_error": self._lifecycle_error,
            "max_generation": self.replica.max_generation,
            "reset_pending": self._reset_pending,
            "primary": self.names[s.primary],
            "promoted": self.names[s.promoted],
            "detached": self.replica.detached,
            "ready": self.ready(),
            "piles": piles,
            "quorum_piles_last": list(s.quorum_piles_last),
            "pending_followups": self.pending_followups(),
            "peers": self.reach.to_json(self.names),
            "blocked": sorted(self.names[p] for p in self.reach.blocked),
            "prepared": None if self.replica.prepared is None else {
                "proposal_id": self.replica.prepared.proposal_id,
                "generation": self.replica.prepared.state.generation,
            },
            "nodes": [
                {"id": n.node_id, "label": n.label, "host": n.host, "port": n.grpc_port,
                 "faults": self.node_faults[n.index_in_pile].to_json()}
                for n in self.pile_info.nodes
            ],
            "timings": self.timings.to_dict(),
            "policies": {k: v for k, v in self.policies.to_dict().items() if k not in ("admin_token", "read_token")},
        }

    # ----- admin API -----

    async def admin_update(
        self,
        updates: typing.Sequence[validator.Update],
        quorum_piles: typing.Sequence[str] = (),
        accepted: typing.Optional[typing.Callable] = None,
    ) -> Outcome:
        if self._reset_pending:
            error = MockError(model.INTERNAL_ERROR, "mock: reset is in progress")
            self.events.append("reject", status=error.status, message=error.message)
            return Outcome(error=error)
        reset_epoch = self._reset_epoch

        def make_new(cur: ClusterState):
            if not cur.bridged:
                return MockError(model.PRECONDITION_FAILED,
                                 "Bridge cluster state is not configured or not available")
            result = validator.validate_update(cur, updates, quorum_piles, self.names)
            return result.state if result.ok else result.error

        async def run() -> Outcome:
            outcome = await self._commit_loop(
                make_new, "admin", self.timings.quorum_fail_delay, reset_epoch=reset_epoch, accepted=accepted)
            if outcome.error is not None:
                self.events.append("reject", status=outcome.error.status, message=outcome.error.message)
            return outcome

        # The distconf actor finishes a proposal whether or not the client is still waiting. A client deadline
        # or cancel must not abandon the pipeline halfway, with prepared slots on the peers.
        return await asyncio.shield(self._spawn(run()))

    async def admin_lifecycle(self, kind: str, name: str = "", accepted=None, committed=None) -> Outcome:
        epoch = self._reset_epoch

        async def run():
            outcome = await self._commit_loop(
                lambda cur: lifecycle.start(cur, kind, name, self.names), kind,
                self.timings.quorum_fail_delay, reset_epoch=epoch, accepted=accepted)
            if not outcome.ok:
                return outcome
            if committed is not None:
                await committed()
            if kind == "convert":
                return outcome
            while True:
                if epoch != self._reset_epoch:
                    return Outcome(error=MockError(model.INTERNAL_ERROR, "mock: operation interrupted by reset"))
                state = self.state
                pending = state.pending_pile_operation
                same_pile_pending = pending and pending["pile_name"] == name
                registered = name in state.registered_names(self.names)
                stored = state.stored(self.names.index(name))
                finished = (registered and stored != Stored.INACTIVE if kind == "add"
                            else not registered or stored == Stored.INACTIVE)
                if state.bridged and not same_pile_pending and finished:
                    return Outcome(state=state.copy())
                if same_pile_pending and pending.get("error_reason"):
                    what = ("Pile is added to the cluster, but its parts of storage groups can't be created"
                            if kind == "add" else "Pile is being removed from the cluster, but its parts of storage groups can't be deleted")
                    return Outcome(error=MockError(
                        model.GENERIC_ERROR, "%s (this is retried until it succeeds): %s" % (
                            what, pending["error_reason"])))
                changed = self._state_changed
                await changed.wait()

        return await asyncio.shield(self._spawn(run()))

    async def configure_topology(self, values: dict) -> Outcome:
        # Validate before enqueueing the proposal and repeat against the eventual snapshot.
        lifecycle.configure(self.state, values, self.names)
        return await self._commit_loop(lambda cur: lifecycle.configure(cur, values, self.names),
                                       "configure", self.timings.quorum_fail_delay)

    def control_followups(self, values: dict) -> None:
        unknown = set(values) - {"hold", "release", "error_reason"}
        if unknown:
            raise ValueError("unknown followup options: %s" % sorted(unknown))
        for action in ("hold", "release"):
            entries = values.get(action, [])
            if not isinstance(entries, list) or not all(isinstance(x, str) for x in entries):
                raise ValueError("%s must be a list" % action)
            for entry in entries:
                kind, separator, pile = entry.partition(":")
                if kind not in followups.KINDS or (separator and pile not in self.names):
                    raise ValueError("unknown followup gate %r" % entry)
        reason = values.get("error_reason", self._lifecycle_error)
        if reason is not None and not isinstance(reason, str):
            raise ValueError("error_reason must be a string or null")
        self._held_followups.update(values.get("hold", []))
        self._held_followups.difference_update(values.get("release", []))
        self._lifecycle_error = reason or None
        self.events.append("followup.controls", values=values)
        self._rearm()

    def _signal_state(self) -> None:
        self._state_changed.set()
        self._state_changed = asyncio.Event()

    def _spawn(self, coro: typing.Awaitable) -> "asyncio.Task":
        task = asyncio.get_running_loop().create_task(coro)
        self._bg.add(task)

        def done(t: "asyncio.Task") -> None:
            self._bg.discard(t)
            if not t.cancelled() and t.exception() is not None:
                self.events.append("internal.error", where="pipeline", error=repr(t.exception()))

        task.add_done_callback(done)
        return task

    # ----- commit pipeline -----

    def _new_proposal_id(self) -> str:
        return "%s:%d:%04x" % (self.name, next(self._pid), self.rng.getrandbits(16))

    async def _call(self, peer: int, op: str, payload: dict) -> dict:
        if peer in self.reach.blocked:
            raise PeerError("blocked")
        return await self.transport.call(self.index, peer, op, payload, self.timings.peer_timeout)

    def _local_prepare(self, proposal_id: str, new: ClusterState, sender: int) -> dict:
        generation = self.replica.state.generation
        revision = self.replica.state.revision
        if self._reset_pending:
            return {"ok": False, "reason": "reset", "generation": generation}
        if self.replica.detached:
            return {"ok": False, "reason": "detached", "generation": generation}
        if not self.ready():
            return {"ok": False, "reason": "not_ready", "generation": generation}
        ok, reason = self.replica.try_prepare(proposal_id, new, sender, self.timings.prepare_ttl)
        if ok:
            return {"ok": True, "generation": generation, "revision": revision}
        return {"ok": False, "reason": reason, "generation": generation, "revision": revision}

    def _participants(self, old: ClusterState, new: ClusterState) -> typing.Tuple[typing.List[int], typing.List[int]]:
        """(required, observers). Required piles must accept. Observers are every other pile that may be
        committing a different change (its old or new state lets it take part), so that two proposals whose
        required sets do not overlap still exclude each other, as they do at the single distconf root."""
        if not self.policies.enforce_config_quorum:
            return [self.index], []
        required = model.required_set(new)
        observers = [
            i for i in range(len(self.names))
            if i not in required and (
                i == self.index or model.allows_connection(old.per_pile[i]) or model.allows_connection(new.per_pile[i]))]
        return required, observers

    async def _two_phase(self, new: ClusterState, cause: str) -> _PhaseResult:
        if self._reset_pending:
            return _PhaseResult(False, {self.index: {
                "ok": False, "reason": "reset", "generation": self.replica.state.generation}})
        reset_epoch = self._reset_epoch
        old = self.replica.state
        required, observers = self._participants(old, new)
        asked = sorted(set(required) | set(observers))
        proposal_id = self._new_proposal_id()
        snapshot = new.to_json()

        async def prepare(peer: int):
            if peer == self.index:
                return peer, self._local_prepare(proposal_id, new, self.index)
            try:
                reply = await self._call(peer, "prepare", {"proposal_id": proposal_id, "snapshot": snapshot})
            except PeerError as e:
                reason = "blocked" if e.reason == "blocked" else "unreachable"
                return peer, {"ok": False, "reason": reason, "detail": e.reason}
            return peer, reply

        try:
            results = await asyncio.gather(*(prepare(p) for p in asked))
        except asyncio.CancelledError:
            self._abort_everywhere(proposal_id, asked)
            raise
        if reset_epoch != self._reset_epoch:
            self._abort_everywhere(proposal_id, asked)
            return _PhaseResult(False, {self.index: {
                "ok": False, "reason": "reset", "generation": self.replica.state.generation}})
        replies = dict(results)
        accepted = [p for p in asked if replies[p].get("ok")]
        # a required pile must accept; any other pile only counts when it is busy with, or already past, this generation
        refusals = {
            p: r for p, r in replies.items()
            if not r.get("ok") and (p in required or self._conflicts(new, r))}
        # a timed-out prepare may have been accepted: the abort reaches the peer even though the reply did not
        lost = [p for p, r in replies.items() if not r.get("ok") and r.get("detail") == "timeout"]
        for p in lost:
            self._abort_in_background(p, proposal_id)
        if refusals:
            await asyncio.gather(*(self._abort_one(p, proposal_id) for p in accepted))
            return _PhaseResult(False, refusals)

        try:
            installed = self._install(new, cause=cause, origin=self.name, via="commit")
        except OSError:
            # Persistence failed before publishing the decision. Release every reservation we requested.
            self._abort_everywhere(proposal_id, asked)
            raise
        if not installed:
            await asyncio.gather(*(self._abort_one(p, proposal_id) for p in accepted))
            reason = "detached" if self.replica.detached else "generation"
            return _PhaseResult(False, {self.index: {
                "ok": False, "reason": reason, "generation": self.replica.state.generation}})
        # the decision is taken: it is delivered even if the proposer is cancelled meanwhile
        recipients = [p for p in accepted if p != self.index]
        await asyncio.shield(self._spawn(self._deliver(old, new, proposal_id, snapshot, cause, recipients)))
        return _PhaseResult(True, {})

    @staticmethod
    def _conflicts(new: ClusterState, reply: dict) -> bool:
        reason = reply.get("reason")
        if reason == "busy":
            return True
        # the peer is at or past the generation we propose; a peer that is behind will be pushed to later
        peer_version = (int(reply.get("generation", 0)), int(reply.get("revision", 0)))
        return reason == "generation" and peer_version >= new.version

    async def _deliver(self, old: ClusterState, new: ClusterState, proposal_id: str, snapshot: dict, cause: str,
                       recipients: typing.List[int]) -> None:
        await asyncio.gather(*(self._send_commit(p, proposal_id, snapshot, cause) for p in recipients))
        self._schedule_push(old, new, skip=recipients)

    def _abort_everywhere(self, proposal_id: str, peers: typing.Iterable[int]) -> None:
        self.replica.abort(proposal_id)
        for p in peers:
            if p != self.index:
                self._abort_in_background(p, proposal_id)

    def _abort_in_background(self, peer: int, proposal_id: str) -> None:
        task = asyncio.get_running_loop().create_task(self._abort_one(peer, proposal_id))
        self._aborts.add(task)
        task.add_done_callback(self._aborts.discard)

    async def _abort_one(self, peer: int, proposal_id: str) -> None:
        if peer == self.index:
            self.replica.abort(proposal_id)
            return
        try:
            await self._call(peer, "abort", {"proposal_id": proposal_id})
        except PeerError:
            pass

    async def _send_commit(self, peer: int, proposal_id: str, snapshot: dict, cause: str) -> None:
        try:
            await self._call(peer, "commit", {
                "proposal_id": proposal_id, "snapshot": snapshot, "cause": cause})
        except PeerError as e:
            self.events.append("commit.send_failed", peer=self.names[peer], reason=e.reason)

    def _schedule_push(self, old: ClusterState, new: ClusterState, skip: typing.Sequence[int] = ()) -> None:
        candidates = (range(len(self.names)) if old.registry != new.registry or old.configured_piles != new.configured_piles
                      or old.bridged != new.bridged else model.push_targets(old, new))
        targets = [p for p in candidates if p != self.index and p not in skip]
        if not targets:
            return
        task = asyncio.get_running_loop().create_task(self._push_all(new, targets))
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    async def _push_all(self, new: ClusterState, targets: typing.List[int]) -> None:
        async def one(peer: int) -> None:
            try:
                reply = await self._call(peer, "push", {"snapshot": new.to_json()})
                self.events.append("push", peer=self.names[peer], generation=new.generation,
                                   installed=bool(reply.get("installed")))
            except PeerError as e:
                self.events.append("push.failed", peer=self.names[peer], generation=new.generation, reason=e.reason)

        await asyncio.gather(*(one(p) for p in targets))

    async def _pull(self, peer: int, why: str) -> bool:
        """Fetch the peer's snapshot and install it if it is newer and allows this pile to connect."""
        if self.replica.detached:
            return False
        reset_epoch = self._reset_epoch
        try:
            raw = await self._call(peer, "state", {})
            snap = ClusterState.from_json(raw)
            snap.validate(len(self.names))
        except (PeerError, KeyError, ValueError):
            return False
        if reset_epoch != self._reset_epoch:
            return False
        if snap.version <= self.replica.state.version:
            return False
        if not model.allows_connection(snap.per_pile[self.index]) and not (
                snap.registry != self.state.registry or snap.configured_piles != self.state.configured_piles):
            if self._refused_pull.get(peer) != snap.version:
                self._refused_pull[peer] = snap.version
                self.events.append(
                    "pull.refused", peer=self.names[peer], generation=snap.generation,
                    reason="peer state does not allow this pile to connect", why=why)
            return False
        return self._install(snap, cause="install", origin=self.names[peer], via="pull")

    def _install(self, new: ClusterState, cause: str, origin: str, via: str) -> bool:
        old = self.replica.state
        if not self.replica.install(new):
            return False
        self.tracker.on_commit(old, new)
        changed = [self.names[i] for i in model.changed_piles(old, new)]
        self.events.append(
            "commit", cause=cause, via=via, origin=origin, generation=new.generation,
            before=self.vector(old), after=self.vector(new), changed=changed,
            stored=[model.Stored(x).name for x in new.per_pile])
        self._rearm()
        self._notify_listeners()
        self._signal_state()
        return True

    async def _commit_loop(
        self,
        make_new: typing.Callable[[ClusterState], typing.Union[ClusterState, MockError, None]],
        cause: str,
        deadline_seconds: float,
        reset_epoch: typing.Optional[int] = None,
        accepted: typing.Optional[typing.Callable] = None,
    ) -> Outcome:
        if reset_epoch is None:
            reset_epoch = self._reset_epoch
        reset_error = MockError(model.INTERNAL_ERROR, "mock: proposal aborted by reset")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + deadline_seconds
        attempt = 0
        while True:
            if self._reset_pending or reset_epoch != self._reset_epoch:
                return Outcome(error=reset_error)
            current = self.replica.state
            new = make_new(current)
            if isinstance(new, MockError):
                return Outcome(error=new)
            if new is None:
                return Outcome(skipped=True)
            if accepted is not None:
                hook, accepted = accepted, None
                await hook()
                # A held acceptance can outlive its original snapshot.
                continue
            result = await self._two_phase(new, cause)
            if result.committed:
                return Outcome(state=new)
            if reset_epoch != self._reset_epoch:
                return Outcome(error=reset_error)
            attempt += 1
            refusals = result.refusals
            summary = {self.names[p]: r.get("reason") for p, r in sorted(refusals.items())}
            if loop.time() >= deadline:
                piles = ", ".join(self.names[p] for p in sorted(refusals))
                self.events.append("propose.fail", cause=cause, attempts=attempt, refusals=summary,
                                   generation=new.generation)
                return Outcome(error=MockError(
                    model.INTERNAL_ERROR, "mock: no config quorum for piles [%s]" % piles))
            self.events.append("propose.retry", cause=cause, attempt=attempt, refusals=summary,
                               generation=new.generation)

            # A peer ahead of us: catch up if it lets us, otherwise this is distconf's generation error.
            ahead = [((r.get("generation", 0), r.get("revision", 0)), p) for p, r in refusals.items()
                     if r.get("reason") == "generation" and
                     (r.get("generation", 0), r.get("revision", 0)) > current.version]
            if ahead:
                if self.replica.state.version > current.version:
                    continue  # the replica moved on its own (a commit or push arrived): re-validate
                peer_generation, peer = max(ahead)
                pulled = await self._pull(peer, why=cause)
                if reset_epoch != self._reset_epoch:
                    return Outcome(error=reset_error)
                if not pulled and self.replica.state.version <= current.version:
                    return Outcome(error=MockError(
                        model.INTERNAL_ERROR,
                        "New cluster state generation# %d expected# %d" % (new.generation, peer_generation[0] + 1)))
                continue
            # Peers behind us: tell them what they missed.
            behind = [p for p, r in refusals.items()
                      if r.get("reason") == "generation" and
                      (r.get("generation", 0), r.get("revision", 0)) < current.version]
            for p in behind:
                if reset_epoch != self._reset_epoch:
                    return Outcome(error=reset_error)
                if model.allows_connection(current.per_pile[p]):
                    try:
                        await self._call(p, "push", {"snapshot": current.to_json()})
                    except PeerError:
                        pass
            if any(r.get("reason") == "busy" for r in refusals.values()):
                await asyncio.sleep(self.rng.uniform(0.02, 0.08))
            else:
                await asyncio.sleep(0.1)

    # ----- peer handlers -----

    async def handle_peer(self, op: str, src: int, payload: dict) -> dict:
        if src in self.reach.blocked:
            raise PeerBlocked()
        state = self.replica.state
        if op == "ping":
            return {"pile": self.name, "generation": state.generation, "revision": state.revision,
                    "ready": self.ready()}
        if op == "state":
            return state.to_json()
        if op == "prepare":
            new = self._peer_snapshot(payload)
            reply = self._local_prepare(payload["proposal_id"], new, src)
            if not reply["ok"]:
                self.events.append("prepare.refused", origin=self.names[src], reason=reply["reason"],
                                   generation=new.generation, local_generation=state.generation)
            return reply
        if op == "abort":
            self.replica.abort(payload["proposal_id"])
            return {"ok": True}
        if op == "commit":
            new = self._peer_snapshot(payload)
            if self.replica.blocks_commit(payload["proposal_id"], new):
                # somebody else holds the slot at this generation: this commit was never prepared here
                self.events.append("commit.refused", origin=self.names[src], generation=new.generation,
                                   reason="another proposal is prepared")
                return {"ok": True, "installed": False, "generation": self.replica.state.generation}
            installed = self._install(new, cause=payload.get("cause", "install"), origin=self.names[src],
                                      via="commit")
            return {"ok": True, "installed": installed, "generation": self.replica.state.generation}
        if op == "push":
            new = self._peer_snapshot(payload)
            installed = self._install(new, cause="install", origin=self.names[src], via="push")
            return {"ok": True, "installed": installed, "generation": self.replica.state.generation}
        raise ValueError("unknown peer op %r" % op)

    def _peer_snapshot(self, payload: dict) -> ClusterState:
        """The snapshot of a peer request. A malformed one is the sender's error: nothing is changed."""
        snapshot = ClusterState.from_json(payload["snapshot"])
        snapshot.validate(len(self.names))
        return snapshot

    # ----- heartbeat, anti-entropy -----

    async def heartbeat_once(self) -> None:
        peers = [p for p in range(len(self.names)) if p != self.index]

        async def ping(peer: int) -> None:
            if peer in self.reach.blocked:
                return
            try:
                reply = await self._call(peer, "ping", {})
            except PeerError:
                return
            self.reach.record_ok(peer, reply.get("ready", True), int(reply.get("generation", 0)))
            self._peer_versions[peer] = (int(reply.get("generation", 0)), int(reply.get("revision", 0)))

        await asyncio.gather(*(ping(p) for p in peers))
        self._connectivity_changed()
        if self.replica.expire_prepared(self.timings.prepare_ttl):
            self.events.append("prepare.expired")
        await self._anti_entropy()
        self._rearm()

    def _connectivity_changed(self) -> None:
        for peer, up in self.reach.evaluate():
            self.events.append("peer.up" if up else "peer.down", peer=self.names[peer])
        ready = self.ready()
        if ready != self._last_ready:
            self._last_ready = ready
            self.events.append("ready" if ready else "not_ready")

    async def _anti_entropy(self) -> None:
        if not self.policies.anti_entropy or self.replica.detached or not self._running:
            return
        local = self.replica.state.version
        candidates = sorted(
            ((gen, p) for p, gen in self._peer_versions.items()
             if gen > local and self.reach.reachable(p)),
            reverse=True)
        for gen, peer in candidates:
            if self._refused_pull.get(peer) == gen:
                continue
            if await self._pull(peer, why="anti-entropy"):
                break

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                await self.heartbeat_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # keep the loop alive, show the problem
                self.events.append("internal.error", where="heartbeat", error=repr(e))
            await asyncio.sleep(self.timings.heartbeat_interval)

    def on_nodes_changed(self) -> None:
        """Call after a node fault changed, so readiness and follow-ups are re-evaluated."""
        self._connectivity_changed()
        self._rearm()
        self._notify_listeners()

    # ----- follow-ups -----

    def _followups_enabled(self) -> bool:
        if not self._running or self.replica.detached or self._reset_pending:
            return False
        if self.replica.state.stored(self.index) in (Stored.DISCONNECTED, Stored.INACTIVE):
            return False
        return self.ready()

    def _current_due(self) -> typing.Dict[typing.Tuple[str, int], followups.Due]:
        if not self._followups_enabled():
            return {}
        return {d.key: d for d in followups.due_followups(self.replica.state, self.tracker, self.connected)
                if d.kind not in self._held_followups and
                "%s:%s" % (d.kind, self.names[d.pile]) not in self._held_followups}

    def _rearm(self) -> None:
        if not self._running:
            return
        wanted = self._current_due()
        for key, timer in list(self._timers.items()):
            if key not in wanted and not timer.started:
                del self._timers[key]
                timer.task.cancel()
                self.events.append("followup.cancelled", followup=key[0], pile=self.names[key[1]])
        for key, due in wanted.items():
            if key not in self._timers:
                self._arm(due)

    def _arm(self, due: followups.Due) -> None:
        loop = asyncio.get_running_loop()
        delay = followups.delay_for(due, self.index, self.timings)
        not_before = self._retry_not_before.pop(due.key, None)
        if not_before is not None:
            delay = max(delay, not_before - loop.time())
        task = loop.create_task(self._run_followup(due, delay))
        self._timers[due.key] = _Timer(due, loop.time() + delay, task)
        if due.kind not in (followups.SYNC_STEP, followups.BSC_DONE):
            self.events.append("followup.armed", followup=due.kind, pile=self.names[due.pile], delay=round(delay, 3))

    async def _run_followup(self, due: followups.Due, delay: float) -> None:
        key = due.key
        timer = self._timers.get(key)
        finished = False
        try:
            await asyncio.sleep(delay)
            if key not in self._current_due():
                self.events.append("followup.cancelled", followup=due.kind, pile=self.names[due.pile])
                finished = True
                return
            if timer is not None:
                timer.started = True
            if due.kind in (followups.SYNC_STEP, followups.BSC_DONE):
                self.tracker.step(due.pile)
                self.events.append(
                    "sync.step", followup=due.kind, pile=self.names[due.pile],
                    stage=model.Stage(self.tracker.stage.get(due.pile, model.Stage.BLOCKS)).name,
                    bsc_cleared=self.tracker.bsc_cleared.get(due.pile, False))
            else:
                def make_new(cur: ClusterState):
                    if key not in self._current_due():
                        return None
                    if due.kind in (followups.ADD, followups.REMOVE) and self._lifecycle_error:
                        if cur.pending_pile_operation.get("error_reason") == self._lifecycle_error:
                            return None
                        new = model.derive_metadata(cur)
                        new.pending_pile_operation["error_reason"] = self._lifecycle_error
                        return new
                    return followups.build_commit(cur, due)

                outcome = await self._commit_loop(make_new, due.kind, self.timings.quorum_fail_delay)
                if not outcome.ok and not outcome.skipped:
                    self._retry_not_before[key] = asyncio.get_running_loop().time() + self.timings.followup_retry
                    self.events.append(
                        "followup.failed", followup=due.kind, pile=self.names[due.pile],
                        message=outcome.error.message if outcome.error else "")
                elif due.kind in (followups.ADD, followups.REMOVE) and self._lifecycle_error:
                    self._retry_not_before[key] = asyncio.get_running_loop().time() + self.timings.followup_retry
            finished = True
        finally:
            if self._timers.get(key) is timer:
                self._timers.pop(key, None)
            if finished and self._running:
                self._rearm()

    # ----- control -----

    def set_blocked(self, piles: typing.Iterable[int]) -> None:
        self.reach.blocked = {p for p in piles if p != self.index}
        self.events.append("link", blocked=sorted(self.names[p] for p in self.reach.blocked))
        self._connectivity_changed()
        self._rearm()

    def prepare_reset(self) -> None:
        """Stop new decisions before the supervisor chooses a generation above all committed snapshots."""
        if self._reset_pending:
            return
        self._reset_pending = True
        self._reset_epoch += 1
        self._signal_state()
        for timer in self._timers.values():
            timer.task.cancel()
        self._timers.clear()
        self.events.append("reset.prepare", generation=self.replica.state.generation)

    def cancel_reset(self) -> None:
        """Leave the reset barrier without undoing any snapshot already installed by reset_local."""
        if not self._reset_pending:
            return
        self._reset_pending = False
        self.events.append("reset.cancel", generation=self.replica.state.generation)
        self._rearm()

    def reset_local(self, generation: int) -> None:
        # Do not clear faults or invalidate work unless the new baseline was persisted successfully.
        self.replica.reset(generation)
        if self.topo.initial:
            self.replica.force(lifecycle.configure(self.state, self.topo.initial, self.names, initial=True), False)
        self._reset_epoch += 1
        for timer in self._timers.values():
            timer.task.cancel()
        self._timers.clear()
        self._retry_not_before.clear()
        self._refused_pull.clear()
        self._peer_versions.clear()
        self._held_followups.clear()
        self._lifecycle_error = None
        for faults in self.node_faults:
            faults.clear()
        self.reach.blocked = set()
        self.reach.restart()
        self.tracker.reset_from(self.replica.state)
        self._last_ready = self.ready()
        self.events.append("reset", generation=generation)
        self._rearm()
        self._notify_listeners()
        self._signal_state()

    def override_replica(
        self,
        public: typing.Dict[str, int],
        generation: typing.Optional[int] = None,
        detach: bool = False,
    ) -> ClusterState:
        """Replace this instance's replica (control API). Unnamed piles keep their public state."""
        if self._reset_pending:
            raise ValueError("cannot override a replica while reset is pending")
        for name in public:
            if name not in self.names:
                raise KeyError(name)
        cur = self.replica.state
        publics = {i: model.public_state(cur, i) for i in range(len(self.names))}
        for name, value in public.items():
            publics[self.names.index(name)] = value
        per_pile: typing.List[Stored] = []
        primary = promoted = None
        for i in range(len(self.names)):
            value = publics[i]
            stored = (cur.stored(i) if self.names[i] not in public else
                      Stored.INACTIVE if value == model.PUB_INACTIVE else validator._to_stored(value))
            if stored is None:
                raise ValueError("unsupported pile state %r" % (value,))
            per_pile.append(stored)
            if value == model.PUB_PRIMARY:
                if primary is not None:
                    raise ValueError("multiple primary piles")
                primary = i
            elif value == model.PUB_PROMOTED:
                if promoted is not None:
                    raise ValueError("multiple promoted piles")
                promoted = i
        if primary is None:
            raise ValueError("one primary pile is required")
        new = cur.copy()
        new.per_pile = per_pile
        new.primary = primary
        new.promoted = primary if promoted is None else promoted
        new.generation = cur.generation if generation is None else generation
        new.sync = {i: cur.sync.get(i, 1) for i, s in enumerate(per_pile)
                    if s not in (Stored.SYNCHRONIZED, Stored.INACTIVE)}
        self.replica.force(new, detach)
        self.tracker.reset_from(new)
        self.events.append("replica.override", generation=new.generation, detached=detach, state=new.describe(self.names))
        self._rearm()
        self._notify_listeners()
        self._signal_state()
        return new

    def clear_replica_override(self) -> None:
        if self._reset_pending:
            raise ValueError("cannot reattach a replica while reset is pending")
        self.replica.set_detached(False)
        self.events.append("replica.reattach")
        self._rearm()
        self._notify_listeners()


def wall_clock() -> float:
    return time.time()
