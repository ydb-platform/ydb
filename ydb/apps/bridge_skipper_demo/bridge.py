import health
import json
import pickle
import os
from cli_wrapper import *

import collections
import copy
import logging
import sys
import threading
import time
import yaml

from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Currently when pile goes from 'NOT SYNCHRONIZED -> SYNCHRONIZED' there might be
# healthchecks reporting it bad, we should ignore them for some time.
TO_SYNC_TRANSITION_GRACE_PERIOD = health.HEALTH_REPLY_TTL_SECONDS * 2

# Emit an info-level "All is good" / "There are bad piles" at most once per this period (seconds)
STATUS_INFO_PERIOD_SECONDS = 60.0


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

    "UNKNOWN": "red",
}


class PileState:
    def __init__(self):
        # original PileHealth reported by this pile
        # TODO: set to default value to simplify the code
        self.pile_health = None

        # state of this pile as reported by `admin cluster bridge list`
        self.admin_reported_state = None

        # in case of not synced -> synced transition we might want
        # to have some grace period (TO_SYNC_TRANSITION_GRACE_PERIOD)s
        self.synced_since = None

        self.marked_bad = False

    def _is_in_sync_grace_period(self):
        if self.admin_reported_state == "SYNCHRONIZED":
            if not self.synced_since:
                # sanity check, shouldn't happen
                return False
            if time.monotonic() - self.synced_since < TO_SYNC_TRANSITION_GRACE_PERIOD:
                # ignore for some time
                return True
        return False

    def get_state(self):
        if self.admin_reported_state:
            return self.admin_reported_state

        return "UNKNOWN"

    def is_good(self):
        if not self.admin_reported_state:
            return False

        if self.admin_reported_state in ["PRIMARY", "SYNCHRONIZED"]:
            if self.marked_bad and not self._is_in_sync_grace_period():
                return False

        return True

    def get_color(self):
        if not self.is_good():
            return "red"

        state = self.get_state()
        if state in STATUS_COLOR:
            return STATUS_COLOR[state]

        return "red"

    def __str__(self):
        s = f"PileState(admin_reported_state={self.admin_reported_state}, is_bad={self.marked_bad}"

        if self.synced_since:
            synced_elapsed = time.monotonic() - self.synced_since
            s += f", synced for {synced_elapsed:.3f}s"

        if self.pile_health:
            s += f", {self.pile_health}"

        s += ")"

        return s

    def __eq__(self, other):
        if not isinstance(other, PileState):
            return NotImplemented
        # note, we don't compare pile_health, because we want to compare globally visible state only
        return (
            self.admin_reported_state == other.admin_reported_state and
            self.marked_bad == other.marked_bad and
            self.synced_since == other.synced_since # however, it means that admin_reported_state is also different
        )


class GlobalState:
    def __init__(self, pile_world_views: Dict[str, health.PileWorldView], prev_state: "GlobalState"):
        self.wall_ts = time.time()
        self.monotonic_ts = time.monotonic()

        self.piles = {} # pile name -> PileState
        self.generation = None # latest known generation
        self.wait_for_generation = None # don't do anything until we see this generation or newer
        self.primary_name = None # primary according latest generation

        # pile_name -> number of piles reporting it bad including itself
        # Not part of state, just aggregation over self.piles.
        self._bad_piles = collections.defaultdict(int)

        # step 1: find the latest admin state. Here we assume that no configuration
        # mistakes done and latest generation is good and the latest one.

        latest_generation, pile_with_latest_generation = health.get_latest_generation_pile(pile_world_views)

        # step 2: get pile state from the latest generation (either new one or previously known)

        has_previous_generation = prev_state and prev_state.generation

        # there should be either just obtained generation or previous one
        if not (latest_generation or has_previous_generation):
            logger.error(f"healthcheck failed, no admin config found: {pile_world_views}")
            return

        if latest_generation and (not has_previous_generation or latest_generation >= prev_state.generation):
            # new generation available
            logger.debug(f"New config generation: {latest_generation}")
            admin_items = pile_world_views[pile_with_latest_generation].admin_states.piles
            self.generation = latest_generation
            for pile_name, admin_item in admin_items.items():
                self.piles[pile_name] = PileState()
                self.piles[pile_name].admin_reported_state = admin_item.admin_reported_state
                if admin_item.admin_reported_state == "PRIMARY":
                    self.primary_name = pile_name
        else:
            # we have seen more recent generation, fall back to it
            logger.debug(f"Fall back to previous known generation: {prev_state.generation}")
            self.generation = prev_state.generation
            for pile_name, pile in prev_state.piles.items():
                self.piles[pile_name] = PileState()
                self.piles[pile_name].admin_reported_state = pile.admin_reported_state
                if pile.admin_reported_state == "PRIMARY":
                    self.primary_name = pile_name

        if prev_state and prev_state.wait_for_generation and prev_state.wait_for_generation > self.generation:
            self.wait_for_generation = prev_state.wait_for_generation

        # step 3: process pile health part

        for pile_name, view in pile_world_views.items():
            if view.health and view.health.self_check_result:
                self.piles[pile_name].pile_health = view.health
                for bad_pile_name in view.health.bad_piles:
                    self._bad_piles[bad_pile_name] += 1

        self._set_sync_time_if_any(prev_state)

        # now, choose some pile (preferably Primary) as having the right view
        # and mark bad piles if any

        pile_name_with_right_view = self.get_pile_with_right_view()
        if pile_name_with_right_view:
            pile_with_right_view = self.piles.get(pile_name_with_right_view)
            if pile_with_right_view and pile_with_right_view.pile_health:
                for bad_pile_name in pile_with_right_view.pile_health.bad_piles:
                    if bad_pile_name in self.piles:
                        self.piles[bad_pile_name].marked_bad = True
        else:
            # all are bad
            for pile in self.piles.values():
                pile.marked_bad = True

    def _set_sync_time_if_any(self, prev_state):
        if prev_state is None:
            return

        prev_piles = prev_state.piles
        if len(prev_piles) == 0:
            return

        for pile_name, pile in self.piles.items():
            if pile.admin_reported_state == "SYNCHRONIZED" and pile_name in prev_piles:
                prev_pile = prev_piles[pile_name]
                if prev_pile.admin_reported_state != "SYNCHRONIZED":
                    pile.synced_since = self.monotonic_ts
                else:
                    pile.synced_since = prev_pile.synced_since

    def _get_pile_good_and_synchronized(self) -> Optional[str]:
        # we prefer at most pile which responds to healthchecks and considered good by others.
        # however, it might happen there is no network between us and good pile.
        non_responsive_but_good = None
        responsive_and_good = None
        for pile_name, pile in self.piles.items():
            if pile.admin_reported_state == "SYNCHRONIZED" and self._bad_piles[pile_name] == 0:
                non_responsive_but_good = pile_name
                if pile.pile_health is not None and pile.pile_health.is_responsive():
                    responsive_and_good = pile_name
                    if pile.pile_health.thinks_it_is_healthy():
                        return pile_name

        return responsive_and_good or non_responsive_but_good

    def __eq__(self, other):
        if not isinstance(other, GlobalState):
            return NotImplemented
        if self.generation != other.generation:
            return False

        # we intentionally ignore wait_for_generation

        if self.primary_name != other.primary_name:
            return False
        if set(self.piles.keys()) != set(other.piles.keys()):
            return False

        for name in self.piles.keys():
            if self.piles[name] != other.piles[name]:
                return False

        return True

    def __str__(self):
        piles_parts = []
        details_parts = []
        for name in sorted(self.piles.keys()):
            pile = self.piles[name]
            state = pile.get_state()
            piles_parts.append(f"{name}:{state}")
            details_parts.append(f"{name}={pile}")
        piles_str = ", ".join(piles_parts)
        details_str = ", ".join(details_parts)

        s = f"GlobalState(ts={self.wall_ts}, gen={self.generation}"
        if self.wait_for_generation:
            s += f", wait_generation={self.wait_for_generation}"

        s += f", primary={self.primary_name}, piles=[{piles_str}], details=[{details_str}])"
        return s

    def has_responsive_piles(self):
        for pile in self.piles.values():
            if pile.pile_health and pile.pile_health.is_responsive():
                return True
        return False

    def get_endpoints(self, pile_name):
        if pile_name not in self.piles:
            return []

        pile = self.piles[pile_name]
        endpoints = pile.pile_health.get_endpoints() if pile.pile_health else []
        if len(endpoints) > 0:
            return endpoints

        # No endpoints for this pile, might be network split between us and pile.
        # Fallback to any pile not considered as bad
        for other_pile in self.piles.values():
            if other_pile.is_good():
                endpoints = other_pile.pile_health.get_endpoints() if other_pile.pile_health else []
                if len(endpoints) > 0:
                    return endpoints

        return []

    def get_pile_with_right_view(self) -> Optional[str]:
        """ If primary is good it returns him, otherwise returns pile good enough to become new primary """

        if not self.has_responsive_piles():
            return None

        if self.primary_name and self._bad_piles[self.primary_name] == 0:
            # We have a primary and nobody argues he is good enough.
            # Thus, its world view is the right one
            return self.primary_name
        elif self.primary_name is None:
            # there is no primary: try to find SYNCHRONIZED pile not in bad piles and use its view
            return self._get_pile_good_and_synchronized()
        elif self._bad_piles[self.primary_name] > 0:
            # not everybody is happy with current primary (potentially even primary might be unhappy with itself)
            primary_pile = self.piles[self.primary_name]
            primary_is_responsive = primary_pile.pile_health and primary_pile.pile_health.is_responsive()

            good_synchronized_pile_name = self._get_pile_good_and_synchronized()
            if good_synchronized_pile_name:
                # an easy case: there is "not bad" synchronized,
                # which everybody (we have connection with) agrees on including Primary
                sync_pile = self.piles[good_synchronized_pile_name]

                sync_is_responsive = sync_pile.pile_health and sync_pile.pile_health.is_responsive()

                if sync_is_responsive and primary_is_responsive:
                    # if both sync_pile and primary_pile are responsive,
                    # we check what they think about each other
                    if self.primary_name not in sync_pile.pile_health.bad_piles:
                        if self.primary_name not in primary_pile.pile_health.bad_piles:
                            # Somebody thinks that primary is bad, however:
                            # 1. Primary doesn't think it is bad.
                            # 2. There is a synced pile which everybody agrees is good.
                            # 3. Sync node doesn't consider Primary as bad.
                            # -> keep current primary's world view
                            return self.primary_name
                elif not primary_is_responsive and sync_is_responsive:
                    # 1. Primary is considered as bad by somebody (not itself, since it is unresponsive)
                    # 2. There is a synced and reponsive pile considered OK by everybody we
                    # can connect to (not responsive ones might have a different opinion)
                    if self.primary_name not in sync_pile.pile_health.bad_piles:
                        # assume network issue between us and primary,
                        # keep current primary
                        return self.primary_name

                    # TODO: recheck, there is a split here.
                    # primary is considered bad by this very good sync pile,
                    # use this good as primary
                    return good_synchronized_pile_name
                else:
                    # 1. Primary is considered as bad by somebody (not itself, since it is unresponsive)
                    # 2. There is a synced pile which everybody (we have connection with) agrees is good.
                    # 3. However, this good synced pile is unresponsive and we can't get its world view
                    return None

            # primary is reported bad by somebody or itself, there is no good sync
            # pile, about which everybody (responsive) agrees it is good
            # we might have split brain, e.g. consider as example two piles setup, which lost
            # interconnect between them: each one would think another one is bad

            if len(self.piles) > 2:
                # TODO
                logger.error("Currently demo doesn't support more than 2 piles")
                sys.exit(1)

            # we have a simple case: there are two piles and each one is reported as bad
            if primary_is_responsive and primary_pile.pile_health.thinks_it_is_healthy():
                # primary believes it is OK, trust him
                return self.primary_name

            # other pile thinks it is good, then it is good
            for pile_name, pile in self.piles.items():
                if pile.admin_reported_state == "SYNCHRONIZED":
                    if pile.pile_health and pile.pile_health.thinks_it_is_healthy():
                        return pile_name

        return None


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
        ts = state.wall_ts
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
        delta_s = state.monotonic_ts - prev_state.monotonic_ts if hasattr(prev_state, "monotonic_ts") else 0
        delta_str = f"+{delta_s:.1f}s"
        if prev_state.primary_name != state.primary_name:
            changes.append(f"PRIMARY {prev_state.primary_name} → {state.primary_name}")

        prev_piles = prev_state.piles
        curr_piles = state.piles
        all_names = sorted(set(prev_piles.keys()) | set(curr_piles.keys()))
        for name in all_names:
            prev_pile = prev_piles.get(name)
            curr_pile = curr_piles.get(name)
            if prev_pile is None and curr_pile is not None:
                curr_status = curr_pile.get_state()
                changes.append(f"{name} appeared ({curr_status})")
                continue
            if curr_pile is None and prev_pile is not None:
                changes.append(f"{name} disappeared")
                continue
            if prev_pile is None and curr_pile is None:
                # sanity check, should not happen
                continue

            prev_good = prev_pile.is_good()
            curr_good = curr_pile.is_good()
            if prev_good != curr_good:
                if curr_good:
                    changes.append(f"{name} became good")
                else:
                    changes.append(f"{name} became bad")

            prev_status = prev_pile.get_state()
            curr_status = curr_pile.get_state()

            # Only include piles whose state or status actually changed
            if prev_status != curr_status:
                changes.append(f"{name} {prev_status} → {curr_status}")

        if changes:
            summary = f"{ts_str} ({delta_str})  " + "; ".join(changes)
            logger.info(summary)
            self.transitions.append(summary)


class BridgeSkipper:
    def __init__(self, path_to_cli: str, initial_piles: Dict[str, List[str]], auto_failover: bool = True, state_path: Optional[str] = None, ydb_auth_opts: Optional[List[str]] = None):
        self.path_to_cli = path_to_cli
        self.initial_piles = initial_piles
        self.auto_failover = auto_failover
        self.state_path = state_path
        self.ydb_auth_opts = list(ydb_auth_opts or [])

        self.async_checker = health.AsyncHealthcheckRunner(path_to_cli, initial_piles, ydb_auth_opts=self.ydb_auth_opts)
        self.async_checker.start()

        # TODO: avoid hack, sleep to give async_checker time to get data
        time.sleep(health.HEALTH_REPLY_TTL_SECONDS)

        self.current_state = None
        self.state_history = TransitionHistory()

        # Attempt to load previous state if provided
        try:
            self._load_state()
            # Check that loaded state generation is not ahead of what cluster reports
            if self.current_state:
                try:
                    pile_world_views = self.async_checker.get_health_state()
                    latest_generation = health.get_latest_generation_pile(pile_world_views)[0]
                    logger.info(f"Loaded state: {self.current_state}, cluster reported latest generation: {latest_generation}")
                    if self.current_state.generation and latest_generation and self.current_state.generation > latest_generation:
                        logger.error(f"Generation {self.current_state.generation} from state file is ahead of cluster generation {latest_generation}")
                        self.async_checker.stop(wait=True)
                        sys.exit(1)
                except Exception as e:
                    logger.error(f"Failed to get health state or latest generation: {e}")

        except Exception as e:
            logger.debug(f"Failed to load saved state: {e}")

        self._state_lock = threading.Lock()
        self._run_thread = None
        self._last_status_info_ts = 0.0
        self._stop_event = threading.Event()

    def _do_healthcheck(self):
        # TODO: get_health_state() call might take too long time,
        # thus it's important to call it before GlobalState() constructor, which inits timestamps.

        pile_world_views = self.async_checker.get_health_state()
        if len(pile_world_views) == 0:
            logger.error("healthcheck failed, not piles info!")
            return

        # it is OK to read current_state without lock, because we are the only writer
        new_state = GlobalState(pile_world_views, self.current_state)

        if new_state != self.current_state:
            logger.info(f"State changed, new state: {new_state}")
        else:
            logger.debug(f"Current state: {new_state}")

        with self._state_lock:
            self.current_state = new_state
            self.state_history.add_state(self.current_state)
        try:
            self._save_state()
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def _decide(self) -> Optional[Tuple[str, List[List[str]]]]:
        # decide is called within the same thread as do_healthcheck,
        # so we can access everything without locking
        if self.current_state is None:
            return None

        if len(self.current_state.piles) == 0:
            logger.error("No piles!")

        if not self.current_state.has_responsive_piles():
            logger.critical(f"All piles are unresponsive: {self.current_state}")
            return None

        if (self.current_state.wait_for_generation and
                self.current_state.generation < self.current_state.wait_for_generation):
            logger.debug(f"Current generation {self.current_state}, waiting >= {self.current_state.wait_for_generation}")
            return None

        bad_piles = [pile for pile, state in self.current_state.piles.items() if not state.is_good()]

        monotonicNow = time.monotonic()
        if len(bad_piles) == 0:
            short_message = "All is good"
            long_message = f"All is good: {self.current_state}"
        else:
            short_message = "There are bad piles"
            long_message = f"There are bad piles: {self.current_state}"

        if monotonicNow - self._last_status_info_ts >= STATUS_INFO_PERIOD_SECONDS:
            logger.info(long_message)
            self._last_status_info_ts = monotonicNow
        else:
            logger.debug(short_message)

        new_primary_name = None
        primary_name = self.current_state.primary_name

        if not primary_name:
            logger.error("No known primary, selecting new one")
        elif primary_name in bad_piles:
            logger.error("Primary is reported as bad, selecting new one")

        # note, that it might be the same as current primary
        new_primary_name = self.current_state.get_pile_with_right_view()
        if new_primary_name is None:
            logger.critical("there is no good enough pile to be a primary")
            return None

        commands = []
        for pile_name in bad_piles:
            if (pile_name == primary_name and new_primary_name is None) or (pile_name == new_primary_name):
                continue

            commands.append(["--assume-yes", "admin", "cluster", "bridge", "failover"])
            commands[-1].extend(["--pile", pile_name])
            if primary_name == pile_name:
                commands[-1].extend(["--new-primary", new_primary_name])
            else:
                logger.warning(f"Pile {pile_name} is unhealthy, failover will be performed")

            ''' NOTE: uncomment after generation support is added to cli
            if self.generation and len(commands) == 1:
                commands[-1].extend(["--uiid", self.generation])
            else:
                commands[-1].append("--ignore-uuid-validation")
            '''

        return ((new_primary_name or primary_name), commands,)

    def _apply_decision(self, primary: str, commands: List[List[str]]):
        # "primary" is either good primary or candidate to primary,
        # thus we prefer its endpoints, because at least some are healthy.
        # However, there might be network split between keeper and primary, so that
        # we try to use other endpoints
        endpoints = self.current_state.get_endpoints(primary)
        if len(endpoints) == 0:
            logger.critical(f"No endpoints for primary / primary candidate '{primary}', fall back to all endpoints")
            return

        if commands and len(commands) > 0:
            some_failed = False
            succeeded_count = 0
            for command in commands:
                if self.auto_failover:
                    result = execute_cli_command(
                        self.path_to_cli,
                        command,
                        endpoints,
                        ydb_auth_opts=self.ydb_auth_opts,
                        print_info=True)
                    if result is None:
                        some_failed = True
                        logger.error(f"Failed to apply command {command}")
                    else:
                        succeeded_count += 1
                else:
                    logger.warning(f"Autofailover disabled, please execute the command: {command_str}")
            if some_failed:
                logger.critical("Failover failed: cluster might be down!")
            else:
                if self.auto_failover:
                    commands_s = "command" if succeeded_count == 1 else "commands"
                    new_min_gen = self.current_state.generation + succeeded_count
                    logger.info(f"{succeeded_count} failover {commands_s} executed successfully, expecting gen >= {new_min_gen}")
                    with self._state_lock:
                        self.current_state.wait_for_generation = new_min_gen
                    try:
                        self._save_state()
                    except Exception as e:
                        logger.error(f"Failed to save state: {e}")

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
        self._run_thread = threading.Thread(target=self.run, name="BridgeSkipper.run", daemon=True)
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

    def get_state_and_history(self) -> Tuple[Optional[GlobalState], List[str]]:
        with self._state_lock:
            state_copy = copy.deepcopy(self.current_state) if self.current_state is not None else None
            transitions_copy = list(self.state_history.get_transitions())
        return (state_copy, transitions_copy)

    def _save_state(self):
        if not self.state_path:
            return
        state_path = self.state_path
        # Truncate and write binary pickle, fsync for durability
        fd = None
        try:
            fd = os.open(state_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            with os.fdopen(fd, "wb") as f:
                pickle.dump((self.current_state, self.state_history), f, protocol=pickle.HIGHEST_PROTOCOL)
                f.flush()
                os.fsync(f.fileno())
            fd = None
        finally:
            try:
                if fd is not None:
                    os.close(fd)
            except Exception:
                pass

    def _load_state(self):
        if not self.state_path:
            return
        if not os.path.exists(self.state_path):
            return
        try:
            with open(self.state_path, "rb") as f:
                state, history = pickle.load(f)
            # Assign loaded objects as-is
            self.current_state = state
            self.state_history = history
        except Exception as e:
            logger.debug(f"Failed to read state file: {e}")


def get_max_status_length():
    max_length = 0
    for status in STATUS_COLOR.keys():
        max_length = max(max_length, len(status))
    return max_length


# TODO: resolve must return proper error text when fails
def resolve(endpoint: str, path_to_cli: str, ydb_auth_opts: Optional[List[str]] = None) -> Optional[Dict[str, List[str]]]:
    """ Returns pile names -> list of their endpoints """

    logger.debug(f"Trying to resolve {endpoint} to piles and other endpoints")

    cmd = ["admin", "cluster", "config", "fetch"]
    list_result = execute_cli_command(path_to_cli, cmd, [endpoint,], ydb_auth_opts=ydb_auth_opts)

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
        hosts = data.get("config", {}).get("hosts", []) or []
        for entry in hosts:
            host = entry.get("host")
            location = entry.get("location", {}) or {}
            pile_name = location.get("bridge_pile_name")
            if not host or not pile_name:
                continue
            piles_to_hosts.setdefault(pile_name, []).append(host)
    except Exception:
        logger.debug("Failed to build piles_to_hosts mapping from YAML")
        return None

    return piles_to_hosts
