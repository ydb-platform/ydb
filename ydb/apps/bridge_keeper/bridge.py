import json
import copy
import logging
import random
import requests
import subprocess
import sys
import threading
import time
import traceback
import yaml

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Request healthcheck from the next in RR endpoint every amount of time
HEALTH_CHECK_INTERVAL = 0.1

# Refresh piles list via CLI every amount of time
PILES_REFRESH_INTERVAL = 0.5

# Consider a pile responsive if last health check is within this threshold (seconds)
# Initial healthecks to endpoints of the same pile are evenly distributed across
# this interval of time
RESPONSIVENESS_THRESHOLD_SECONDS = 2.0

MINIMAL_EXPECTED_ENDPOINTS_PER_PILE = 3

# Consider healthcheck reply valid only within this amount of time
HEALTH_REPLY_TTL_SECONDS = 5.0

# Currently when pile goes from 'NOT SYNCHRONIZED -> SYNCHRONIZED' there might be
# healthchecks reporting it bad, we should ignore them for some time.
TO_SYNC_TRANSITION_GRACE_PERIOD = RESPONSIVENESS_THRESHOLD_SECONDS * 3

# Emit an info-level "All is good" at most once per this period (seconds)
ALL_GOOD_INFO_PERIOD_SECONDS = 60.0

STATUS_COLOR = {
    # ydb reported statuses

    "PRIMARY": "green",
    "SYNCHRONIZED": "green",
    "DISCONNECTED": "red",
    "NOT_SYNCHRONIZED": "yellow",
    "PROMOTED": "cyan",
    "DEMOTED": "magenta",
    "SUSPENDED": "grey50",

    # derived statuses

    "UNRESPONSIVE": "red",
    "UNKNOWN": "red",
}

# Keep at most this many endpoints per pile when resolving
# TODO: consider larger number of larger installations
MAX_ENDPOINTS_PER_PILE = 3


class AsyncHealthcheckRunner:
    """
    Periodically performs healthcheck requests to viewer endpoints concurrently.

    - Constructor accepts endpoints and shuffles them
    - start() begins asynchronous round-robin dispatch in a background thread
    - Maintains a dictionary: reporter pile name -> set of failed pile names
    - get_health_state_and_piles() is thread-safe and returns a snapshot for Bridgekeeper
    """

    def __init__(self, endpoints, path_to_cli, initial_piles, use_https=False):
        self.endpoints = list(endpoints)
        self.path_to_cli = path_to_cli
        self.initial_piles = initial_piles
        self.use_https = use_https

        random.shuffle(self.endpoints)

        self._executor = ThreadPoolExecutor(max_workers=max(1, min(4, len(self.endpoints) or 1)))
        self._stop_event = threading.Event()
        self._thread = None
        self._lock = threading.Lock()

        # reporter_pile -> set(failed_pile_names)
        self._reporter_to_failed = {}
        # reporter_pile -> last health check timestamp
        self._reporter_last_ts = {}

        # endpoint -> Future (or None if idle)
        self._inflight = {}
        self._rr_index = 0

        # piles list snapshot (dict: pile_name -> state string)
        self._piles_list = {}
        self._piles_generation = None
        self._last_piles_refresh_ts = 0.0

        # Track mapping from endpoint -> pile and last response time per endpoint
        # TODO: probably get ridd of _endpoint_to_pile if we have initial mapping?
        self._endpoint_to_pile = {}
        self._endpoint_last_ts = {}

        # Set by keeper
        self._primary_hint = None

    def start(self):
        if not self.endpoints:
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="AsyncHealthcheckRunner", daemon=True)
        self._thread.start()

    def stop(self, wait=False):
        self._stop_event.set()
        if wait and self._thread:
            self._thread.join(timeout=10)
        self._executor.shutdown(wait=True)

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run_loop(self):
        try:
            self._run_loop_impl()
        except Exception as e:
            logger.exception(f"AsyncHealthcheckRunner's run loop got exception: {e}")

    def _run_loop_impl(self):
        # Pre-fill inflight slots with None
        for endpoint in self.endpoints:
            self._inflight.setdefault(endpoint, None)

        # start healthecks for the same pile evenly distributed within RESPONSIVENESS_THRESHOLD_SECONDS
        # TODO: proper impl for the case when piles have different number of endpoints
        if self.initial_piles and len(self.initial_piles) > 0:
            endpoints_multiqueue = []
            for pile_endpoints in self.initial_piles.values():
                endpoints_multiqueue.append(pile_endpoints)
            endpoints_per_pile = max(len(endpoints_multiqueue[0]), MINIMAL_EXPECTED_ENDPOINTS_PER_PILE)
            delay = RESPONSIVENESS_THRESHOLD_SECONDS / endpoints_per_pile
            logger.debug(f"Starting initial {endpoints_per_pile} healthecks per pile with delay between {delay}")

            for i in range(endpoints_per_pile):
                for pile_endpoints in endpoints_multiqueue:
                    if i < len(pile_endpoints):
                        endpoint = pile_endpoints[i]
                        future = self._inflight.get(endpoint)
                        if future is None:
                            if not self._stop_event.is_set():
                                self._inflight[endpoint] = self._executor.submit(self._do_request, endpoint)
                time.sleep(delay)

            logger.debug("Initial healthecks started")

        while not self._stop_event.is_set():
            # Periodically refresh piles list via CLI
            monotonicNow = time.monotonic()
            if monotonicNow - self._last_piles_refresh_ts >= PILES_REFRESH_INTERVAL:
                try:
                    self._fetch_piles_list()
                except Exception:
                    logger.debug("Piles refresh failed", exc_info=True)
                self._last_piles_refresh_ts = monotonicNow

            # Round-robin choose next endpoint
            endpoint = self.endpoints[self._rr_index]
            self._rr_index = (self._rr_index + 1) % len(self.endpoints)

            future = self._inflight.get(endpoint)
            if future is None:
                # Schedule new request
                if not self._stop_event.is_set():
                    self._inflight[endpoint] = self._executor.submit(self._do_request, endpoint)
            elif future.done():
                # Process completed request and start a new one
                self._process_future(endpoint, future)
                if not self._stop_event.is_set():
                    self._inflight[endpoint] = self._executor.submit(self._do_request, endpoint)
            else:
                # Opportunistically process any other completed futures
                for ep, fut in list(self._inflight.items()):
                    if fut is not None and fut.done():
                        self._process_future(ep, fut)
                        if not self._stop_event.is_set():
                            self._inflight[ep] = self._executor.submit(self._do_request, ep)

            time.sleep(HEALTH_CHECK_INTERVAL)

    def _fetch_piles_list(self):
        # we want to prefer pile, which is good: either any is good until
        # failover or use hinted primary
        with self._lock:
            primary_hint = self._primary_hint

        endpoints = None
        strict_order = False
        if primary_hint:
            endpoints = self.get_ordered_endpoints(primary_hint)
            if len(endpoints) != 0:
                strict_order = True

        if not endpoints or len(endpoints) == 0:
            endpoints = self.endpoints

        cmd = ['admin', 'cluster', 'bridge', 'list', '--format=json']
        result = execute_cli_command(self.path_to_cli, cmd, endpoints, strict_order=strict_order)
        if result is None:
            return
        try:
            parsed = json.loads(result.stdout.decode())
            generation = parsed['generation']

            # it is OK to read without lock, since we are the only writer
            if self._piles_generation is not None and generation <= self._piles_generation:
                return

            piles_map = {}
            for item in parsed["piles"]:
                name = item.get('pile_name')
                state = item.get('state')
                if name is None:
                    continue
                piles_map[name] = state
            with self._lock:
                self._piles_list = dict(piles_map)
                self._piles_generation = generation
        except Exception:
            logger.debug("Failed to parse piles list JSON", exc_info=True)

    def _do_request(self, endpoint):
        try:
            scheme = 'https' if self.use_https else 'http'
            r = requests.get(
                f'{scheme}://{endpoint}:8765/viewer/healthcheck',
                params={'merge_records': 'true', 'timeout': '1000'},
                verify = False,
                )
            r.raise_for_status()
            data = r.json()

            #logger.debug(f"Finished health request to {endpoint}: {data}")

            issues = data.get('issue_log', [])
            this_pile = data['location']['pile']['name']
            failed_groups = filter(
                lambda issue: 'GROUP' in issue.get('type', '') and 'pile' in issue.get('location', {}).get('storage', {}).get('pool', {}).get('group', {})
                    and (issue.get('status') == 'RED' or issue.get('status') == 'YELLOW'),
                issues,
            )
            failed_piles = set(
                map(lambda issue: issue['location']['storage']['pool']['group']['pile']['name'], failed_groups)
            )
            return (this_pile, failed_piles)
        except Exception as e:
            # Swallow errors; caller treats as no update from this endpoint
            logger.debug(f"Healthcheck request failed for {endpoint}: {e}")
            return None

    def _process_future(self, endpoint, future):
        try:
            result = future.result()
        except Exception:
            result = None

        if result is None:
            return

        reporter_pile, failed_piles = result
        monotonicNow = time.monotonic()
        with self._lock:
            self._reporter_to_failed[reporter_pile] = set(failed_piles)
            self._reporter_last_ts[reporter_pile] = monotonicNow
            # Track per-endpoint recency and pile mapping
            self._endpoint_to_pile[endpoint] = reporter_pile
            self._endpoint_last_ts[endpoint] = monotonicNow

    def get_health_state_and_piles(self):
        """Thread-safe snapshot: (reporter pile -> {failed_piles: set, last_ts: float}), pile_name -> state string)"""
        with self._lock:
            health_state = {
                pile: {
                    'failed_piles': set(failed),
                    'last_ts': self._reporter_last_ts.get(pile, 0.0),
                }
                for pile, failed in self._reporter_to_failed.items()
            }

            piles = dict(self._piles_list)

            return (health_state, piles, self._piles_generation)

    def get_ordered_endpoints(self, pile: str):
        """Thread-safe snapshot of pile endpoints ordered by response recency (most recent first)."""
        with self._lock:
            candidates = [ep for ep, p in self._endpoint_to_pile.items() if p == pile]

        ranked = sorted(candidates, key=lambda ep: self._endpoint_last_ts.get(ep, 0.0), reverse=True)
        return list(ranked)

    def set_primary_hint(self, new_primary: str):
        with self._lock:
            self._primary_hint = new_primary


class PileState:
    def __init__(self):
        # healthcheck from this pile works
        self.responsive = False

        # healthcheck from other pile reported this pile alive
        self.reported_alive = False

        # healthcheck from this pile reported this pile alive
        self.self_reported_alive = False

        # state of this pile as reported by `admin cluster bridge list`
        self.admin_reported_state = None

        # any healthcheck reported this pile in group issues
        self.reported_bad = None

        self.synced_since = None

    def is_healthy(self):
        if self.admin_reported_state == 'PRIMARY':
            if not self.self_reported_alive:
                return False
            return not self.reported_bad
        elif self.admin_reported_state == 'SYNCHRONIZED':
            if self.reported_bad:
                if not self.synced_since:
                    # sanity check, shouldn't happen
                    return False
                if time.monotonic() - self.synced_since < TO_SYNC_TRANSITION_GRACE_PERIOD:
                    return True
            return not self.reported_bad
        else:
            return True

    def can_vote_for_bad_piles(self):
        if not self.self_reported_alive:
            return False

        if self.admin_reported_state == 'PRIMARY':
            return True

        if self.admin_reported_state == 'SYNCHRONIZED':
            # TODO: this is a quick hack
            if self.synced_since:
                return time.monotonic() - self.synced_since > TO_SYNC_TRANSITION_GRACE_PERIOD
            return True

        return False

    def is_fit_for_promote(self):
        return not self.reported_bad and self.admin_reported_state == 'SYNCHRONIZED' and (self.self_reported_alive or self.reported_alive)

    def get_state(self):
        if self.admin_reported_state:
            return self.admin_reported_state

        if not self.responsive:
            return "UNRESPONSIVE"

        return "UNKNOWN"

    def get_state_and_status(self):
        status = 'healthy' if self.is_healthy() else 'unhealthy'
        state = self.get_state()
        return f"{state}:{status}"

    def get_color(self):
        if self.reported_bad or not self.is_healthy():
            return "red"

        state = self.get_state()
        if state in STATUS_COLOR:
            return STATUS_COLOR[state]

        return "red"

    def __str__(self):
        s = (
            f"PileState(good={self.is_healthy()}, responsive={self.responsive}, "
            f"reported_alive={self.reported_alive}, "
            f"self_reported_alive={self.self_reported_alive}, "
            f"admin_reported_state={self.admin_reported_state}, "
            f"reported_bad={self.reported_bad}"
        )

        if self.synced_since:
            synced_elapsed = time.monotonic() - self.synced_since
            s += f", synced for {synced_elapsed:.3f}s"

        s += ")"

        return s


    def __eq__(self, other):
        if not isinstance(other, PileState):
            return NotImplemented
        return (
            self.responsive == other.responsive and
            self.reported_alive == other.reported_alive and
            self.self_reported_alive == other.self_reported_alive and
            self.admin_reported_state == other.admin_reported_state and
            self.reported_bad == other.reported_bad and
            self.synced_since == other.synced_since # however, it means that admin_reported_state is different
        )


class TotalState:
    def __init__(self, generation):
        self.WallTimestamp = time.time()
        self.MonotonicTs = time.monotonic()
        self.Piles = {} # pile name -> dict
        self.PrimaryName = None
        self.Generation = generation

    def is_disaster(self) -> bool:
        if self.PrimaryName is None:
            return True

        if self.PrimaryName not in self.Piles:
            return True

        return not self.Piles[self.PrimaryName]

    def __eq__(self, other):
        if not isinstance(other, TotalState):
            return NotImplemented
        if self.Generation != other.Generation:
            return False
        if self.PrimaryName != other.PrimaryName:
            return False
        if len(self.Piles) != len(other.Piles):
            return False
        if set(self.Piles.keys()) != set(other.Piles.keys()):
            return False
        for name in self.Piles.keys():
            if self.Piles[name] != other.Piles[name]:
                return False

        return True

    def __str__(self):
        ts = int(self.WallTimestamp)
        piles_parts = []
        details_parts = []
        for name in sorted(self.Piles.keys()):
            pile = self.Piles[name]

            # FIXME
            state_status = pile.get_state_and_status()
            piles_parts.append(f"{name}:{state_status}")
            details_parts.append(f"{name}={pile}")
        piles_str = ", ".join(piles_parts)
        details_str = ", ".join(details_parts)
        return (f"TotalState(ts={ts}, primary={self.PrimaryName}, gen={self.Generation}, "
            f"piles=[{piles_str}], details=[{details_str}])")


class TransitionHistory:
    def __init__(self):
        self.state_history = []

        # Each transition is a textual representation of
        # transitions in self.state_history from i to i + 1.
        # Transition timestamp is ts of i + 1, delta is difference between i and i + 1
        self.transitions = []

    def get_transitions(self) -> List[str]:
        return list(self.transitions)

    def add_state(self, state):
        ts = state.WallTimestamp
        ts_str = time.strftime("%H:%M:%S", time.gmtime(ts))
        if len(self.state_history) == 0:
            logger.debug(f"Added first state: {state}")
            self.state_history.append(state)
            return

        prev_state = self.state_history[-1]
        if state == prev_state:
            return

        self.state_history.append(state)

        # Build single-line transition summary
        changes = []
        # Elapsed time between previous and current states
        delta_s = state.MonotonicTs - prev_state.MonotonicTs if hasattr(prev_state, 'MonotonicTs') else 0
        delta_str = f"+{delta_s:.1f}s"
        if prev_state.PrimaryName != state.PrimaryName:
            changes.append(f"PRIMARY {prev_state.PrimaryName} → {state.PrimaryName}")

        prev_piles = prev_state.Piles
        curr_piles = state.Piles
        all_names = sorted(set(prev_piles.keys()) | set(curr_piles.keys()))
        for name in all_names:
            prev_pile = prev_piles.get(name)
            curr_pile = curr_piles.get(name)
            if prev_pile is None and curr_pile is not None:
                curr_status = curr_pile.get_state_and_status()
                changes.append(f"{name} appeared ({curr_status})")
                continue
            if curr_pile is None and prev_pile is not None:
                changes.append(f"{name} disappeared")
                continue
            if prev_pile is None and curr_pile is None:
                # sanity check, should not happen
                continue

            prev_status = prev_pile.get_state_and_status()
            curr_status = curr_pile.get_state_and_status()

            # Only include piles whose state or status actually changed
            if prev_status != curr_status:
                changes.append(f"{name} {prev_status} → {curr_status}")

        if changes:
            summary = f"{ts_str} ({delta_str})  " + "; ".join(changes)
            logger.info(summary)
            self.transitions.append(summary)


class Bridgekeeper:
    def __init__(self, endpoints: List[str], path_to_cli: str, initial_piles: Dict[str, List[str]], use_https: bool = False, auto_failover: bool = True):
        self.endpoints = endpoints
        self.path_to_cli = path_to_cli
        self.initial_piles = initial_piles
        self.use_https = use_https
        self.auto_failover = auto_failover

        self.async_checker = AsyncHealthcheckRunner(endpoints, path_to_cli, initial_piles, use_https=use_https)
        self.async_checker.start()

        # TODO: avoid hack, sleep to give async_checker time to get data
        time.sleep(RESPONSIVENESS_THRESHOLD_SECONDS)

        self.current_state = None
        self.state_history = TransitionHistory()
        self._state_lock = threading.Lock()
        self._run_thread = None
        self._last_all_good_info_ts = 0.0
        self._stop_event = threading.Event()

    def _set_sync_time_if_any(self, new_state):
        # it is OK to read self.current_state without lock, since we are the only writer
        if not self.current_state:
            return

        current_piles = self.current_state.Piles
        if len(current_piles) == 0:
            return

        for pile_name, pile in new_state.Piles.items():
            if pile.admin_reported_state == 'SYNCHRONIZED' and pile_name in current_piles:
                current_pile = current_piles[pile_name]
                if current_pile.admin_reported_state != 'SYNCHRONIZED':
                    pile.synced_since = new_state.MonotonicTs
                else:
                    pile.synced_since = current_pile.synced_since

    def _do_healthcheck(self):
        # TODO: it seems that sometimes this calls takes too long for unknown yet reason,
        # thus it's important to call before TotalState() constructor, which inits
        # timestamps.
        health_state, cluster_admin_piles, generation = self.async_checker.get_health_state_and_piles()

        new_state = TotalState(generation)
        for pile_name, state in (cluster_admin_piles or {}).items():
            new_state.Piles[pile_name] = PileState()
            new_state.Piles[pile_name].admin_reported_state = state
            if state == 'PRIMARY':
                new_state.PrimaryName = pile_name

        monotonicNow = time.monotonic()
        for pile_name, info in health_state.items():
            #logger.debug(f"Observed {pile_name} state: {info}")
            last_ts = info.get('last_ts', 0.0)
            pile = new_state.Piles.setdefault(pile_name, PileState())
            if monotonicNow - last_ts <= RESPONSIVENESS_THRESHOLD_SECONDS:
                pile.responsive = True

            # merge how this pile sees itself
            observed_failed_piles = info.get('failed_piles', set())
            if len(observed_failed_piles) > 0:
                if pile_name not in observed_failed_piles:
                    pile.self_reported_alive = True
                else:
                    # note: it is self reported bad
                    pile.reported_bad = True
            else:
                pile.self_reported_alive = True

            # merge how this piles sees other piles
            if pile.can_vote_for_bad_piles():
                for bad_pile_name in observed_failed_piles:
                    if bad_pile_name == pile_name:
                        continue
                    bad_pile = new_state.Piles.setdefault(bad_pile_name, PileState())
                    bad_pile.reported_bad = True

            # how other piles see this pile
            for other_pile_name, other_pile in new_state.Piles.items():
                if other_pile_name == pile_name or other_pile.reported_bad:
                    continue
                # we assume that if pile is not seen bad, it is seen good
                if not other_pile_name in observed_failed_piles:
                    other_pile.reported_alive = True

        self._set_sync_time_if_any(new_state)

        if new_state != self.current_state:
            logger.info(f"State changed, new state: {new_state}")
        else:
            logger.debug(f"Current state: {new_state}")

        with self._state_lock:
            self.current_state = new_state
            self.state_history.add_state(self.current_state)

    def _decide(self) -> List[str]:
        # decide is called within the same thread as do_healthcheck,
        # so we can access everything without locking
        if self.current_state is None:
            return None

        if len(self.current_state.Piles) == 0:
            logger.error("No piles!")

        bad_piles = [pile for pile, state in self.current_state.Piles.items() if not state.is_healthy()]

        monotonicNow = time.monotonic()
        if len(bad_piles) == 0:
            short_message = "All is good"
            long_message = f"All is good: {self.current_state}"
        else:
            short_message = "There are bad piles"
            long_message = f"There are bad piles: {self.current_state}"

        if monotonicNow - self._last_all_good_info_ts >= ALL_GOOD_INFO_PERIOD_SECONDS:
            logger.info(long_message)
            self._last_all_good_info_ts = monotonicNow
        else:
            logger.debug(short_message)

        new_primary_name = None
        primary_name = self.current_state.PrimaryName

        if not primary_name:
            logger.error("No known primary, selecting new one")
        elif primary_name in bad_piles:
            logger.error("Primary is reported unhealthy, selecting new one")

        if not primary_name or primary_name in bad_piles:
            current_primary = self.current_state.Piles[primary_name]
            if current_primary and not current_primary.is_fit_for_promote():
                candidates = []
                for pile_name, state in self.current_state.Piles.items():
                    if state.is_fit_for_promote():
                        candidates.append(pile_name)
                if len(candidates) == 0:
                    logger.critical("No candidates for new primary")
                    return None

                # TODO: try to choose best one?
                new_primary_name = random.choice(candidates)
                new_primary = self.current_state.Piles[new_primary_name]
                self.async_checker.set_primary_hint(new_primary_name)
                logger.info(f"Pile '{new_primary_name}' is chosen as new primary: {new_primary}")
        else:
            self.async_checker.set_primary_hint(primary_name)

        commands = []
        for pile_name in bad_piles:
            if (pile_name == primary_name and new_primary_name is None) or (pile_name == new_primary_name):
                continue

            commands.append(['--assume-yes', 'admin', 'cluster', 'bridge', 'failover'])
            commands[-1].extend(['--pile', pile_name])
            if primary_name == pile_name:
                commands[-1].extend(['--new-primary', new_primary_name])
            else:
                logger.warning(f"Pile {pile_name} is unhealty, failover will be performed")

            ''' NOTE: uncomment after generation support is added to cli
            if self.generation and len(commands) == 1:
                commands[-1].extend(['--uiid', self.generation])
            else:
                commands[-1].append('--ignore-uuid-validation')
            '''

        return ((new_primary_name or primary_name), commands,)

    def _apply_decision(self, primary: str, commands: List[List[str]]):
        # primary is either working primary or candidate,
        # thus we prefer its endpoints, because at least some are healthy
        endpoints = self.async_checker.get_ordered_endpoints(primary)
        strict_order = True
        if len(endpoints) == 0:
            logger.warning(f"No endpoints for primary / primary candidate '{primary}', fall back to all endpoints")
            endpoints = self.endpoints
            strict_order = False

        if commands and len(commands) > 0:
            some_failed = False
            for command in commands:
                command_str = self.path_to_cli + " " + " ".join(command)
                if self.auto_failover:
                    logger.info(f"Executing command: {command_str}")
                    result = execute_cli_command(self.path_to_cli, command, self.endpoints, strict_order=strict_order)
                    if result is None:
                        some_failed = True
                        logger.error(f"Failed to apply command {command}")
                else:
                    logger.warning(f"Autofailover disabled, please execute the command: {command_str}")
            if some_failed:
                logger.critical("Failover failed: cluster might be down!")
            else:
                if self.auto_failover:
                    logger.info("Failover commands executed successfully")

    def _maintain_once(self):
        self._do_healthcheck()
        decision = self._decide()
        if decision:
            primary, commands = decision
            self._apply_decision(primary, commands)

    def run(self):
        # TODO: gracefully stop
        self._stop_event.clear()
        try:
            while not self._stop_event.is_set():
                if not self.async_checker.is_alive():
                    raise RuntimeError("AsyncHealthcheckRunner thread is not running")
                time.sleep(1)
                self._maintain_once()
        except Exception as e:
            logger.exception(f"Keeper's run loop got exception: {e}")
        finally:
            try:
                self.async_checker.stop(wait=True)
            except Exception:
                pass

    def run_async(self):
        if self._run_thread:
            return None

        self._stop_event.clear()
        self._run_thread = threading.Thread(target=self.run, name="Bridgekeeper.run", daemon=True)
        self._run_thread.start()

        return self._run_thread

    def stop_async(self):
        if not self._run_thread:
            return

        try:
            self._stop_event.set()
            if self._run_thread.is_alive():
                self._run_thread.join(timeout=10)
        except Exception:
            pass

    def get_state_and_history(self) -> Tuple[Optional[TotalState], List[str]]:
        with self._state_lock:
            state_copy = copy.deepcopy(self.current_state) if self.current_state is not None else None
            transitions_copy = list(self.state_history.get_transitions())
        return (state_copy, transitions_copy)


def execute_cli_command(
        path_to_cli: str, cmd: List[str], endpoints: List[str], strict_order: bool = False)-> Optional[subprocess.CompletedProcess]:

    random_order_endpoints = list(endpoints)

    if not strict_order:
        random.shuffle(random_order_endpoints)

    for endpoint in random_order_endpoints:
        try:
            full_cmd = [path_to_cli, '-e', f'grpc://{endpoint}:2135'] + cmd
            result = subprocess.run(full_cmd, capture_output=True)
            if result.returncode == 0:
                return result
            logger.debug(f"{cmd} failed for {endpoint} with code {result.returncode}, stdout: {result.stdout}")
        except Exception as e:
            logger.debug(f"CLI command failed for endpoint {endpoint}: {e}")
            continue

    return None


def execute_cli_command_parallel(path_to_cli: str, cmd: List[str], endpoints: List[str]) -> Optional[subprocess.CompletedProcess]:
    """Run the CLI command against majority of provided endpoints concurrently and return the first completed result.

    Other in-flight calls are ignored once the first completes.
    """
    if not endpoints:
        return None

    shuffled = list(endpoints)
    random.shuffle(shuffled)
    # Use only a strict majority of endpoints
    majority_size = (len(shuffled) // 2) + 1 if shuffled else 0
    targets = shuffled[:majority_size]

    if len(targets) <= 1:
        return execute_cli_command(path_to_cli, cmd, targets)

    with ThreadPoolExecutor(max_workers=min(len(targets), 8)) as executor:
        future_map = {executor.submit(execute_cli_command, path_to_cli, cmd, [ep]): ep for ep in targets}
        for future in as_completed(future_map.keys()):
            try:
                res = future.result()
            except Exception:
                res = None
            # Return immediately regardless of success; caller may check returncode
            return res

    return None


def resolve(endpoint: str, path_to_cli: str) -> Dict[str, List[str]]:
    """ Returns pile names -> list of their endpoints """

    logger.debug(f"Trying to resolve {endpoint} to piles and other endpoints")

    cmd = ['admin', 'cluster', 'config', 'fetch']
    list_result = execute_cli_command(path_to_cli, cmd, [endpoint,])

    if list_result is None:
        return None

    text = list_result.stdout.decode()
    try:
        data = yaml.safe_load(text)
    except Exception:
        logger.debug("Failed to parse YAML from cluster config fetch")
        return None

    piles_to_hosts = {}

    try:
        hosts = data.get('config', {}).get('hosts', []) or []
        for entry in hosts:
            host = entry.get('host')
            location = entry.get('location', {}) or {}
            pile_name = location.get('bridge_pile_name')
            if not host or not pile_name:
                continue
            piles_to_hosts.setdefault(pile_name, []).append(host)
    except Exception:
        logger.debug("Failed to build piles_to_hosts mapping from YAML")
        return None

    # Cap to at most MAX_ENDPOINTS_PER_PILE random hosts per pile
    for pile, hosts in list(piles_to_hosts.items()):
        if not hosts:
            continue
        k = min(MAX_ENDPOINTS_PER_PILE, len(hosts))
        try:
            piles_to_hosts[pile] = random.sample(list(hosts), k)
        except ValueError:
            piles_to_hosts[pile] = list(hosts)[:k]

    summary = ", ".join(f"{pile}: {len(hosts)}" for pile, hosts in piles_to_hosts.items())
    logger.info("Piles host counts (capped to %d per pile): %s", MAX_ENDPOINTS_PER_PILE, summary)

    return piles_to_hosts


def get_max_status_length():
    max_length = 0
    for status in STATUS_COLOR.keys():
        max_length = max(max_length, len(status))
    return max_length
