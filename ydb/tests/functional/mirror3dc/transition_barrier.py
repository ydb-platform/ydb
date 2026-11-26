"""Control mirror3dc automatic transitions in a running ydbd cluster."""

from dataclasses import dataclass
from html.parser import HTMLParser
import json
import logging
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ydb.core.protos import blobstorage_base3_pb2


logger = logging.getLogger(__name__)


class _ControlInputs(HTMLParser):
    def __init__(self, names):
        super().__init__()
        self.names = names
        self.values = {}

    def handle_starttag(self, tag, attrs):
        if tag == 'input':
            attrs = dict(attrs)
            name = attrs.get('name')
            if name in self.names and attrs.get('value') is not None:
                self.values[name] = int(attrs['value'])


@dataclass(frozen=True)
class TransitionBarrierSnapshot:
    group_id: int
    node_id: int
    coordinator: str
    generation: int
    from_state: int
    to_state: int
    held: bool


class TransitionBarrier:
    """Hold selected source states and release one exact group generation.

    ``nodes`` maps node IDs to harness nodes exposing ``host`` and ``mon_port``.
    One shared set of controls selects one group per node; a zero mask disables it.
    Controls must already be registered by NodeWarden; HTTP 200 alone is not an
    acknowledgement. All writes verify the named inputs in the returned ICB page.

    Use ``arm()`` again after restarting nodes: controls are process-local. After
    ``wait_reached(...)``, pass the returned snapshot's generation to ``release``.
    Release acknowledges the controls only, not completion of the transition.
    """

    def __init__(self, nodes, group_id, states, *, check_errors=None, timeout=60,
                 request_timeout=3, poll_interval=0.1, event_logger=None):
        self.nodes = dict(nodes)
        self.group_id = int(group_id)
        if not self.nodes or not 0 <= self.group_id < 1 << 32:
            raise ValueError('A valid group ID and at least one node are required')
        if min(timeout, request_timeout, poll_interval) <= 0:
            raise ValueError('Timeouts and polling interval must be positive')
        self.check_errors = check_errors or (lambda: None)
        self.events = event_logger or logger
        self.timeout = timeout
        self.request_timeout = request_timeout
        self.poll_interval = poll_interval
        self.group_name = 'Mirror3dcTransitionBarrierGroupId'
        self.hold_name = 'Mirror3dcTransitionBarrierHoldStateMask'
        self.allow_name = 'Mirror3dcTransitionBarrierAllowGeneration'
        self.state_mask = self._state_mask(states)
        self.allowed_generation = 0
        self.armed = False

    @staticmethod
    def _state_mask(states):
        valid_states = set(blobstorage_base3_pb2.TGroupState.EState.values())
        mask = 0
        for state in states:
            state = int(state)
            if state not in valid_states:
                raise ValueError(f'Unknown mirror3dc group state: {state}')
            mask |= 1 << state
        if not mask:
            raise ValueError('At least one source state is required')
        return mask

    def _deadline(self, timeout):
        timeout = self.timeout if timeout is None else timeout
        if timeout <= 0:
            raise ValueError('Timeout must be positive')
        return time.monotonic() + timeout

    def _request(self, node_id, path, deadline, values=None):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Barrier HTTP deadline expired')
        node = self.nodes[node_id]
        url = f'http://{node.host}:{node.mon_port}{path}'
        data = None if values is None else urlencode(values).encode('ascii')
        with urlopen(Request(url, data=data), timeout=min(self.request_timeout, remaining)) as response:
            return response.read().decode('utf-8')

    def _write_controls(self, node_id, values, deadline, *, expected=None):
        html = self._request(node_id, '/actors/icb', deadline, values)
        expected = {**values, **(expected or {})}
        inputs = _ControlInputs(expected)
        inputs.feed(html)
        if inputs.values != expected:
            raise ValueError(f'ICB controls absent or not applied: expected={expected}, actual={inputs.values}')

    def _apply(self, values, deadline, *, expected=None):
        pending = set(self.nodes)
        errors = {}
        while pending:
            for node_id in tuple(pending):
                self.check_errors()
                try:
                    self._write_controls(node_id, values, deadline, expected=expected)
                except (OSError, ValueError) as error:
                    errors[node_id] = str(error)
                else:
                    pending.remove(node_id)
                    errors.pop(node_id, None)
            if pending:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f'Group {self.group_id} barrier controls did not apply: {errors}')
                self.check_errors()
                time.sleep(min(self.poll_interval, remaining))

    def arm(self, states=None, *, timeout=None):
        """Wait for registration and hold the selected states on every node."""
        if states is not None:
            self.state_mask = self._state_mask(states)
        self.events.info('CONTROL BARRIER_ARM_REQUEST group_id=%s mask=%s generation=0',
                         self.group_id, self.state_mask)
        deadline = self._deadline(timeout)
        try:
            self._apply({self.hold_name: 0}, deadline)
            self._apply({self.group_name: self.group_id, self.allow_name: 0}, deadline,
                        expected={self.hold_name: 0})
            self._apply({self.hold_name: self.state_mask}, deadline,
                        expected={self.group_name: self.group_id, self.allow_name: 0})
        except Exception:
            self.events.exception('CONTROL BARRIER_ARM_FAILED group_id=%s mask=%s generation=0',
                                  self.group_id, self.state_mask)
            raise
        self.allowed_generation = 0
        self.armed = True
        self.events.info('CONTROL BARRIER_ARMED group_id=%s mask=%s generation=0',
                         self.group_id, self.state_mask)
        return self

    def release(self, generation, *, timeout=None):
        """Allow this generation; every later generation remains held."""
        if not self.armed:
            raise RuntimeError('Arm the transition barrier before releasing a generation')
        generation = int(generation)
        if not 0 < generation < 1 << 32:
            raise ValueError('Released generation must be a positive uint32')
        self.events.info('CONTROL BARRIER_RELEASE_REQUEST group_id=%s mask=%s generation=%s',
                         self.group_id, self.state_mask, generation)
        try:
            self._apply({self.allow_name: generation}, self._deadline(timeout),
                        expected={self.group_name: self.group_id, self.hold_name: self.state_mask})
        except Exception:
            self.events.exception('CONTROL BARRIER_RELEASE_FAILED group_id=%s mask=%s generation=%s',
                                  self.group_id, self.state_mask, generation)
            raise
        self.allowed_generation = generation
        self.events.info('CONTROL BARRIER_RELEASED group_id=%s mask=%s generation=%s',
                         self.group_id, self.state_mask, generation)

    def _snapshots(self, node_id, deadline):
        counters = json.loads(self._request(node_id, '/counters/json', deadline))
        coordinators = {}
        for sensor in counters.get('sensors', []):
            labels = sensor.get('labels', {})
            if (labels.get('counters') == 'bscontroller'
                    and labels.get('component') == 'mirror3dc_transition_barrier'
                    and labels.get('group') == str(self.group_id)
                    and labels.get('coordinator')):
                coordinators.setdefault(labels['coordinator'], {})[labels.get('sensor')] = sensor.get('value')
        required = {'Held', 'Generation', 'FromState', 'ToState'}
        return [
            TransitionBarrierSnapshot(
                group_id=self.group_id, node_id=node_id, coordinator=coordinator,
                generation=int(values['Generation']), from_state=int(values['FromState']),
                to_state=int(values['ToState']), held=bool(values['Held']),
            )
            for coordinator, values in coordinators.items()
            if required <= values.keys() and all(values[name] is not None for name in required)
        ]

    def wait_reached(self, from_state, *, generation=None, to_state=None, timeout=240):
        """Wait for one held coordinator, preserving its identity and generation."""
        if not self.armed:
            raise RuntimeError('Arm the transition barrier before waiting')
        from_state = int(from_state)
        generation = None if generation is None else int(generation)
        to_state = None if to_state is None else int(to_state)
        self.events.info('CONTROL BARRIER_WAIT_REQUEST group_id=%s from=%s to=%s generation=%s',
                         self.group_id, from_state, to_state, generation)
        try:
            snapshot = self._wait_reached(from_state, generation, to_state, timeout)
        except Exception:
            self.events.exception('CONTROL BARRIER_WAIT_FAILED group_id=%s from=%s to=%s generation=%s',
                                  self.group_id, from_state, to_state, generation)
            raise
        self.events.info('CONTROL BARRIER_HELD group_id=%s from=%s to=%s generation=%s coordinator=%s node=%s',
                         self.group_id, snapshot.from_state, snapshot.to_state, snapshot.generation,
                         snapshot.coordinator, snapshot.node_id)
        return snapshot

    def _wait_reached(self, from_state, generation, to_state, timeout):
        deadline = self._deadline(timeout)
        observed = {}
        while True:
            for node_id in self.nodes:
                self.check_errors()
                try:
                    snapshots = self._snapshots(node_id, deadline)
                except (OSError, ValueError) as error:
                    observed[node_id] = str(error)
                    continue
                observed[node_id] = snapshots
                for snapshot in snapshots:
                    # Held is latched until proposal/reset, even after release.
                    if (snapshot.held and snapshot.generation != self.allowed_generation
                            and snapshot.from_state == from_state
                            and (generation is None or snapshot.generation == generation)
                            and (to_state is None or snapshot.to_state == to_state)):
                        return snapshot
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f'Group {self.group_id} did not reach transition barrier '
                    f'from={from_state}, to={to_state}, generation={generation}; observed={observed}'
                )
            self.check_errors()
            time.sleep(min(self.poll_interval, remaining))

    def clear(self, *, timeout=30):
        """Disable the mask before resetting the target, reporting every failed cleanup."""
        self.events.info('CONTROL BARRIER_CLEAR_REQUEST group_id=%s mask=0 generation=0', self.group_id)
        try:
            self._clear(timeout)
        except Exception:
            self.events.exception('CONTROL BARRIER_CLEAR_FAILED group_id=%s mask=0 generation=0', self.group_id)
            raise
        self.events.info('CONTROL BARRIER_CLEARED group_id=%s mask=0 generation=0', self.group_id)

    def _clear(self, timeout):
        deadline = self._deadline(timeout)
        errors = {}
        for node_id in self.nodes:
            try:
                self._write_controls(node_id, {self.hold_name: 0}, deadline)
                self._write_controls(node_id, {self.group_name: 0, self.allow_name: 0}, deadline,
                                     expected={self.hold_name: 0})
            except (OSError, ValueError) as error:
                errors[node_id] = str(error)
        self.armed = False
        self.allowed_generation = 0
        if errors:
            raise RuntimeError(f'Failed to clear group {self.group_id} barrier controls: {errors}')

    def __enter__(self):
        try:
            return self.arm()
        except BaseException:
            try:
                self.clear()
            except Exception:
                self.events.exception('Failed to clear transition barrier after arming failed')
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.clear()
        except Exception:
            if exc_type is None:
                raise
            self.events.exception('Failed to clear transition barrier while handling %r', exc_value)
        return False
