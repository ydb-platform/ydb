from cli_wrapper import *

import collections
import copy
import json
import logging
import random
import requests
import threading
import time

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any

# TODO: lower HEALTH_REPLY_TTL_SECONDS and YDB_HEALTHCHECK_TIMEOUT_MS with new
# version of ydbd healthcheck

# Delay between health checks
HEALTH_CHECK_INTERVAL_SECONDS = 0.2

# Refresh piles list via CLI every amount of time
PILES_REFRESH_INTERVAL_SECONDS = 0.5

# A minimum number of endpoints in any pile, must be odd
MINIMAL_EXPECTED_ENDPOINTS_PER_PILE = 3

# a minimal number of endpoints to gather the quorum
MINIMAL_MAJORITY_SIZE = MINIMAL_EXPECTED_ENDPOINTS_PER_PILE // 2 + 1

# Consider healthcheck reply valid only within this amount of time
HEALTH_REPLY_TTL_SECONDS = 10.0

# Send at most this amount of health checks per pile (but wait for majority)
# note, that when there are more than HEALTH_ENDPOINTS_PER_PILE we don't have
# a strict quorum, see PileHealth and async_do_pile_http_health_check.
#
# In case of 3 nodes, there might be at most 1 faulty (returning wrong health data) node.
# In case of 5 nodes – 2. However, when there are more than HEALTH_ENDPOINTS_PER_PILE nodes,
# we don't use strict check: just use random HEALTH_ENDPOINTS_PER_PILE nodes and their majority.
HEALTH_ENDPOINTS_PER_PILE = 5

# Timeout for YDB healthcheck request in milliseconds
YDB_HEALTHCHECK_TIMEOUT_MS = int((HEALTH_REPLY_TTL_SECONDS - 1) * 1000)


logger = logging.getLogger(__name__)


class EndpointHealthCheckResult:
    def __init__(self, self_check_result: Optional[str], bad_piles: List[str]):
        # See ydb_monitoring.proto (SelfCheck::Result):
        # GOOD, DEGRADED, MAINTENANCE_REQUIRED, EMERGENCY
        self.self_check_result = self_check_result

        # just list of pile names
        self.bad_piles = bad_piles


class PileHealth:
    """
    Pile health state obtained by AsyncHealthcheckRunner.

    - Uses quorum from multiple endpoints to avoid split brains and faulty nodes.
    """

    def __init__(self, name: str, results: Optional[Dict[str, EndpointHealthCheckResult]] = None):
        self.name = name

        self.ts = time.time()
        self.monoTs = time.monotonic()

        # endpoints which built the quorum
        self.quorum_endpoints = []

        # result itself: each field is what majority of endpoints agree with
        self.self_check_result = None
        self.bad_piles = []

        self._endpoint_to_result = {}

        if results and len(results) >= MINIMAL_MAJORITY_SIZE:
            for endpoint, result in results.items():
                self._endpoint_to_result[endpoint] = result

            self._calculate_state()

    def _calculate_state(self):
        self_check_result_counter = collections.defaultdict(int)
        bad_pile_vote_counter = collections.defaultdict(int)

        for endpoint, result in self._endpoint_to_result.items():
            self_check_result_counter[result.self_check_result] += 1
            for pile_name in result.bad_piles:
                bad_pile_vote_counter[pile_name] += 1

        total_reports = len(self._endpoint_to_result)
        majority_threshold = min(MINIMAL_MAJORITY_SIZE, total_reports // 2 + 1)

        for self_check_result, count in self_check_result_counter.items():
            if count >= majority_threshold:
                self.self_check_result = self_check_result
                break

        for bad_pile_name, count in bad_pile_vote_counter.items():
            if count >= majority_threshold:
                self.bad_piles.append(bad_pile_name)

        self.bad_piles.sort()

        # We are not paranoid and a little bit sloppy,
        # so that choose quorum nodes just based on self_check_result.
        # Normally, we would want the same nodes agree on both check result and bad piles
        for endpoint, result in self._endpoint_to_result.items():
            if result.self_check_result == self.self_check_result:
                self.quorum_endpoints.append(endpoint)

    def __str__(self):
        bad = sorted(self.bad_piles) if self.bad_piles else []
        quorum = list(self.quorum_endpoints or [])
        age = time.monotonic() - self.monoTs
        return (
            f"PileHealth(name={self.name}, self_check={self.self_check_result}, "
            f"bad_piles={bad}, quorum={quorum}, age={age:.1f}, responsive={self.is_responsive()})"
        )

    def __eq__(self, other):
        return (
            self.has_quorum() == other.has_quorum() and
            self.self_check_result == other.self_check_result and
            self.bad_piles == other.bad_piles
        )

    def is_responsive(self):
        if len(self._endpoint_to_result) == 0:
            return False

        return time.monotonic() - self.monoTs < HEALTH_REPLY_TTL_SECONDS

    def has_quorum(self):
        return len(self.quorum_endpoints) >= MINIMAL_MAJORITY_SIZE

    def self_reported_bad(self):
        return self.name in self.bad_piles

    # note, other piles might have a different opinion
    def thinks_it_is_healthy(self):
        return self.has_quorum() and self.self_check_result and not self.self_reported_bad()

    def get_endpoints(self) -> List[str]:
        if not self.is_responsive():
            return []
        if len(self.quorum_endpoints) > 0:
            return list(self.quorum_endpoints)

        return self._endpoint_to_result.keys()


class PileAdminItem:
    def __init__(self, name: str, admin_reported_state: str):
        self.name = name

        # E.g. "PRIMARY", "SYNCHRONIZED", "DISCONNECTED",
        # "NOT_SYNCHRONIZED", "PROMOTED", "DEMOTED", "SUSPENDED"
        self.admin_reported_state = admin_reported_state

    def __str__(self):
        return f"PileAdminItem(name={self.name}, state={self.admin_reported_state})"


class PileAdminStates:
    """
    List of configured piles and their admin reported states

    AsyncHealthcheckRunner must obtain it from any endpoing belonging to PileHealth.quorum_endpoints
    """
    def __init__(self, generation: int, piles: Dict[str, PileAdminItem]):
        self.generation = generation
        self.piles = piles

    def __str__(self):
        try:
            parts = []
            for name in sorted(self.piles.keys()):
                item = self.piles[name]
                parts.append(f"{name}:{item.admin_reported_state}")
            piles_str = ", ".join(parts)
        except Exception:
            piles_str = "<unavailable>"
        return f"PileAdminStates(gen={self.generation}, piles=[{piles_str}])"


# health check result + PileAdminStates
class PileWorldView:
    def __init__(self, name: str, health: Optional[PileHealth] = None, admin_states: Optional[PileAdminStates] = None):
        self.name = name
        self.health = health
        self.admin_states = admin_states

    def __str__(self):
        health_str = str(self.health) if self.health is not None else "None"
        if self.admin_states is not None:
            admin_str = f"gen={self.admin_states.generation}, count={len(self.admin_states.piles)}"
        else:
            admin_str = "None"
        return f"PileWorldView(name={self.name}, health={health_str}, admin={admin_str})"

    def __repr__(self):
        return self.__str__()


def get_latest_generation_pile(pile_world_views: Dict[str, PileWorldView]) -> Tuple[Optional[int], Optional[str]]:
    latest_generation = None
    pile_with_latest_generation = None
    for pile_name, view in pile_world_views.items():
        if view.admin_states and view.admin_states.generation:
            if not latest_generation or view.admin_states.generation > latest_generation:
                latest_generation = view.admin_states.generation
                pile_with_latest_generation = pile_name
    return (latest_generation, pile_with_latest_generation,)


def _async_fetch_pile_list(path_to_cli: str, endpoints: List[str], executor: ThreadPoolExecutor, ydb_auth_opts: Optional[List[str]] = None) -> Future:
    def worker() -> Optional[PileAdminStates]:
        cmd = ["admin", "cluster", "bridge", "list", "--format=json"]
        result = execute_cli_command_parallel(path_to_cli, cmd, endpoints, ydb_auth_opts=ydb_auth_opts)
        if result is None:
            return None
        try:
            parsed = json.loads(result.stdout.decode())
            generation = parsed["generation"]
        except Exception:
            logger.debug("Failed to parse piles list JSON", exc_info=True)
            return None

        piles_map = {}
        for item in parsed.get("piles", []):
            name = item.get("pile_name")
            state = item.get("state")
            if name is None:
                continue
            piles_map[name] = PileAdminItem(name, state)

        return PileAdminStates(generation, piles_map)

    return executor.submit(worker)


def _async_pile_health_check(
        path_to_cli: str,
        pile_name: str,
        endpoints,
        executor: ThreadPoolExecutor,
        ydb_auth_opts: Optional[List[str]] = None,
) -> Future:
    def filter_pile_issue(issue):
        try:
            type = issue["type"]
            status = issue["status"]
            pile_name = issue["location"]["storage"]["pool"]["group"]["pile"]["name"]

            if pile_name and ("GROUP" in type) and status == "RED":
                return True
        except:
            return False

    def process_http_health_result(data: Dict[str, Dict[str, Any]]) -> EndpointHealthCheckResult:
        self_check_result = None
        bad_piles = []

        if "self_check_result" in data:
            self_check_result = data["self_check_result"]

        issues = data.get("issue_log", []) or []
        failed_groups = filter(filter_pile_issue, issues)
        failed_piles = set(
            map(lambda issue: issue["location"]["storage"]["pool"]["group"]["pile"]["name"], failed_groups)
        )
        bad_piles = list(failed_piles)

        return EndpointHealthCheckResult(self_check_result, bad_piles)

    def fetch_health(path_to_cli, endpoint: str, ydb_auth_opts: Optional[List[str]) -> Optional[Tuple[str, Dict[str, Any]]]:
        try:
            start_ts = time.monotonic()
            cmd = ["-d", "", "monitoring", "healthcheck", "-v", "--format=json", "--no-merge", "--no-cache"]
            cmd += ["--timeout", str(YDB_HEALTHCHECK_TIMEOUT_MS)]
            result = execute_cli_command(path_to_cli, cmd, [endpoint,], ydb_auth_opts=ydb_auth_opts)
            if result is None:
                return None
            data = json.loads(result.stdout.decode())
            end_ts = time.monotonic()
            delta = end_ts - start_ts
            if delta > HEALTH_REPLY_TTL_SECONDS:
                logger.warning(f"Healthcheck to {endpoint} took {delta:.1f} seconds, which is above TTL")
            return (endpoint, data)
        except Exception as e:
            logger.debug(f"_async_pile_health_check: request failed for pile '{pile_name}' {endpoint}: {e}")
            return None

    def worker() -> PileHealth:
        endpoint_to_result: Dict[str, EndpointHealthCheckResult] = {}
        if not endpoints:
            return PileHealth(pile_name, {})

        future_map = {executor.submit(fetch_health, path_to_cli, endpoint, ydb_auth_opts): endpoint for endpoint in endpoints}
        for f in as_completed(list(future_map.keys())):
            try:
                res = f.result()
                if res is None:
                    continue
                endpoint, data = res
                endpoint_to_result[endpoint] = process_http_health_result(data)
            except Exception:
                continue

        return PileHealth(pile_name, endpoint_to_result)

    return executor.submit(worker)


class AsyncHealthcheckRunner:
    """
    Periodically performs healthcheck requests and obtains pile list.

    Based on healtheck maintains how each pile sees itself and others.
    It's up to the caller to build global state based on these visions.

    - Constructor accepts pile to its endpoints map
    - start() begins asynchronous healthcheck a background thread
    - get_health_state() is thread-safe and returns a snapshot for BridgeSkipper
    """

    def __init__(self, path_to_cli, initial_piles, ydb_auth_opts: Optional[List[str]] = None):
        self.path_to_cli = path_to_cli
        self.pile_to_endpoints = initial_piles
        self.ydb_auth_opts = list(ydb_auth_opts or [])

        self._lock = threading.Lock()

        self._pile_world_views = {}
        for pile_name in self.pile_to_endpoints.keys():
            self._pile_world_views[pile_name] = PileWorldView(pile_name)

        # we need enough parallel activities to not deadlock
        parallel_activities = len(initial_piles) * HEALTH_ENDPOINTS_PER_PILE + 1

        self._executor = ThreadPoolExecutor(max_workers=max(4, parallel_activities))
        self._stop_event = threading.Event()
        self._thread = None

        self._last_piles_refresh_ts = 0
        self._last_healtheck = 0

    def start(self):
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

    def get_health_state(self) -> Dict[str, PileWorldView]:
        """Thread-safe snapshot: {pile_name -> PileWorldView} """
        with self._lock:
            return copy.deepcopy(self._pile_world_views)

    def _get_pile_random_endpoints(self, pile_name: str) -> List[str]:
        selected = list(self.pile_to_endpoints[pile_name] or [])
        if len(selected) > HEALTH_ENDPOINTS_PER_PILE:
            random.shuffle(selected)
            selected = selected[:HEALTH_ENDPOINTS_PER_PILE]
        return selected

    def _run_loop(self):
        try:
            self._run_loop_impl()
        except Exception as e:
            logger.exception(f"AsyncHealthcheckRunner's run loop got exception: {e}")

    def _run_loop_impl(self):
        while not self._stop_event.is_set():
            # healthcheck
            monotonicNow = time.monotonic()
            time_since_healthcheck = monotonicNow - self._last_healtheck
            if time_since_healthcheck >= HEALTH_CHECK_INTERVAL_SECONDS:
                self._do_healthcheck()
            else:
                delta = HEALTH_CHECK_INTERVAL_SECONDS - time_since_healthcheck
                time.sleep(delta)

            # intentionally after the healthech to know quorum nodes
            monotonicNow = time.monotonic()
            if monotonicNow - self._last_piles_refresh_ts >= PILES_REFRESH_INTERVAL_SECONDS:
                try:
                    self._update_pile_lists()
                except Exception:
                    logger.debug("Piles refresh failed", exc_info=True)
                self._last_piles_refresh_ts = monotonicNow

    def _update_pile_lists(self):
        future_to_pile: Dict[Future, str] = {}
        for pile_name, pileView in self._pile_world_views.items():
            if pileView.health and pileView.health.quorum_endpoints and len(pileView.health.quorum_endpoints) > 0:
                endpoints = list(pileView.health.quorum_endpoints)
            else:
                continue

            # We send multiple requests, but wait for any to succeed
            # Note, that we use quorum nodes expecting any
            # to return correct result
            endpoints = endpoints[:MINIMAL_EXPECTED_ENDPOINTS_PER_PILE]

            future = _async_fetch_pile_list(self.path_to_cli, endpoints, self._executor, self.ydb_auth_opts)
            future_to_pile[future] = pile_name

        for future in as_completed(list(future_to_pile.keys())):
            pile_name = future_to_pile.get(future)
            try:
                admin_states = future.result()
            except Exception:
                admin_states = None
            if admin_states is None:
                continue
            pileView = self._pile_world_views.get(pile_name)
            if pileView is None:
                continue
            if pileView.admin_states is None or pileView.admin_states.generation < admin_states.generation:
                logger.debug(f"Updated pile list: {admin_states}")
                with self._lock:
                    pileView.admin_states = admin_states

    def _do_healthcheck(self):
        future_to_pile: Dict[Future, str] = {}
        done_count = 0
        for pile_name, pileView in self._pile_world_views.items():
            endpoints = self._get_pile_random_endpoints(pile_name)

            # Limit endpoints to a reasonable number
            endpoints = endpoints[:HEALTH_ENDPOINTS_PER_PILE]

            logger.trace(f"Running health check of {pile_name} using {endpoints}")

            future = _async_pile_health_check(
                self.path_to_cli,
                pile_name,
                endpoints,
                self._executor,
                self.ydb_auth_opts)
            future_to_pile[future] = pile_name

        # TODO: we can break as soon as have quorum, but for now and for simplicity
        # we wait for all replies
        for future in as_completed(list(future_to_pile.keys())):
            pile_name = future_to_pile.get(future)
            try:
                health = future.result()
            except Exception:
                health = None
            if health is None:
                continue
            pileView = self._pile_world_views.get(pile_name)
            if pileView is None:
                continue

            done_count += 1
            with self._lock:
                pileView.health = health

        self._last_healtheck = time.monotonic()

        logger.trace(f"Finished healthcheck, got {done_count} pile updates")
