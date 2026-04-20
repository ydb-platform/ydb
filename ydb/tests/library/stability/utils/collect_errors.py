from copy import deepcopy
from datetime import datetime
from pytz import timezone
import json

import urllib
import urllib.request
import allure
import yatest
import os
import time
import logging
from ydb.tests.library.stability.utils.remote_execution import execute_command, execute_ssh
from ydb.tests.library.stability.utils.results_models import StressUtilTestResults
from ydb.tests.library.stability.utils.upload_results import test_event_report
from ydb.tests.olap.lib.allure_utils import NodeErrors
from ydb.tests.olap.lib.ydb_cluster import YdbCluster

# Timeouts for background SSH commands executed via execute_ssh + .wait()
_COREDUMP_ANALYZE_TIMEOUT = 600   # breakpad analyzer can be slow on many cores
_COREDUMP_SEARCH_TIMEOUT = 300    # find + cat JSON files
_LOG_COLLECT_TIMEOUT = 600        # unified_agent select for full logs
_TAR_TIMEOUT = 120                # tar -czf archive

# Nemesis orchestrator warden polling parameters
_WARDEN_POLL_INTERVAL_S = 60
_WARDEN_POLL_TIMEOUT_S = 600

# Default orchestrator port (must match deploy.py _NEMESIS_ORCHESTRATOR_PORT)
_NEMESIS_ORCHESTRATOR_PORT = 31434


class WardenCheckResult:
    """Represents a single warden check result from the nemesis orchestrator.

    Attributes:
        name: Check name (e.g. 'kikimr_grep_dmesg_safety_warden_factory__GrepDMesgForPatternsSafetyWarden_0')
        status: Check status ('ok', 'failed', 'error', 'running', etc.)
        violations: List of violation strings found by this check
        affected_hosts: Set of hosts affected by violations
        check_type: Type of check ('safety', 'liveness', or 'orchestrator')
        details: Raw details dict from the orchestrator response
    """

    def __init__(self, name: str, status: str, violations: list[str] = None,
                 affected_hosts: set[str] = None, details: dict = None,
                 check_type: str = 'safety'):
        self.name = name
        self.status = status
        self.violations = violations or []
        self.affected_hosts = affected_hosts or set()
        self.check_type = check_type
        self.details = details or {}

    def is_ok(self) -> bool:
        return self.status == 'ok'

    def is_violation(self) -> bool:
        """Return True if the check found actual violations (not an infrastructure error)."""
        return self.status == 'violation'

    def is_error(self) -> bool:
        """Return True if the check failed due to an infrastructure error (timeout, exception, etc.)."""
        return self.status == 'error'

    def __repr__(self):
        return (f"WardenCheckResult(name={self.name!r}, status={self.status!r}, "
                f"type={self.check_type!r}, violations={len(self.violations)}, "
                f"affected_hosts={self.affected_hosts})")


class WardenResults:
    """Aggregated results from all warden checks returned by the nemesis orchestrator.

    Attributes:
        checks: Dictionary of check name -> WardenCheckResult
        raw_response: The raw JSON response from the orchestrator
        poll_success: Whether polling completed successfully
        error_message: Error message if polling failed
    """

    def __init__(self):
        self.checks: dict[str, WardenCheckResult] = {}
        self.raw_response: dict = {}
        self.poll_success: bool = False
        self.error_message: str = ''

    def get_all_violations(self) -> list[str]:
        """Return all violation strings from all checks."""
        violations = []
        for check in self.checks.values():
            violations.extend(check.violations)
        return violations

    def get_failed_checks(self) -> dict[str, WardenCheckResult]:
        """Return only checks that are not 'ok'."""
        return {name: check for name, check in self.checks.items() if not check.is_ok()}

    def has_violations(self) -> bool:
        """Return True if any check has violations."""
        return any(not check.is_ok() for check in self.checks.values())

    def get_hosts_with_oom(self) -> set[str]:
        """Return set of hosts that had OOM based on dmesg warden checks."""
        oom_hosts = set()
        for name, check in self.checks.items():
            name_lower = name.lower()
            if ('grepdmesg' in name_lower or 'grep_dmesg' in name_lower) and not check.is_ok():
                oom_hosts.update(check.affected_hosts)
        return oom_hosts

    def get_hosts_with_verify_fails(self) -> dict[str, int]:
        """Return dict of host -> verify fail count from orchestrator aggregated checks."""
        verify_hosts = {}
        for name, check in self.checks.items():
            name_lower = name.lower()
            if 'verify' in name_lower and 'failed' in name_lower and not check.is_ok():
                for host in check.affected_hosts:
                    verify_hosts[host] = verify_hosts.get(host, 0) + len(check.violations)
        return verify_hosts

    def get_hosts_with_sanitizer(self) -> set[str]:
        """Return set of hosts that had sanitizer errors."""
        san_hosts = set()
        for name, check in self.checks.items():
            name_lower = name.lower()
            if 'sanitizer' in name_lower and not check.is_ok():
                san_hosts.update(check.affected_hosts)
        return san_hosts


class AgentErrorsCollector:
    """Collects diagnostic errors from the nemesis orchestrator and cluster nodes.

    Uses the nemesis orchestrator's warden API to trigger and collect safety/liveness
    check results, replacing direct SSH-based log parsing with orchestrator-mediated checks.
    Still uses direct SSH for coredump collection (breakpad analysis).
    """

    def __init__(self, hosts: set[str], nodes):
        self.hosts = hosts
        self.nodes = nodes
        self._orchestrator_endpoint: str | None = None

    def _get_orchestrator_endpoint(self) -> str:
        """Return ``http://<orchestrator_host>:<port>`` base URL.

        The orchestrator runs on the first host (sorted), matching deploy.py convention.
        """
        if self._orchestrator_endpoint is not None:
            return self._orchestrator_endpoint

        if not self.hosts:
            raise RuntimeError("Cannot determine orchestrator host: self.hosts is empty")

        orchestrator_host = sorted(self.hosts)[0]
        self._orchestrator_endpoint = f"http://{orchestrator_host}:{_NEMESIS_ORCHESTRATOR_PORT}"
        logging.info(f"Nemesis orchestrator endpoint: {self._orchestrator_endpoint}")
        return self._orchestrator_endpoint

    # ------------------------------------------------------------------
    # Warden checks via orchestrator
    # ------------------------------------------------------------------

    def _request_warden_checks(self) -> WardenResults:
        """Trigger warden checks on the orchestrator and poll for results.

        Sends POST to /api/hosts/warden/start to trigger checks,
        then polls GET /api/hosts/warden/results until all checks complete.

        Returns:
            WardenResults with parsed check results.
        """
        results = WardenResults()
        endpoint = self._get_orchestrator_endpoint()
        start_url = f"{endpoint}/api/hosts/warden/start"

        try:
            logging.info(f"Triggering warden checks: POST {start_url}")
            req = urllib.request.Request(start_url, method="POST")
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode())
                logging.info(f"Warden start response: {body}")
        except Exception as exc:
            error_msg = f"Error triggering warden checks at {start_url}: {exc}"
            logging.error(error_msg)
            results.error_message = error_msg
            return results

        return self._poll_warden_results()

    def _poll_warden_results(self) -> WardenResults:
        """Poll the orchestrator for warden check results until all complete.

        Returns:
            WardenResults with parsed check results.
        """
        results = WardenResults()
        endpoint = self._get_orchestrator_endpoint()
        url = f"{endpoint}/api/hosts/warden/results"
        deadline = time.time() + _WARDEN_POLL_TIMEOUT_S
        attempt = 0

        logging.info(f"Polling nemesis orchestrator warden results: {url}")

        while time.time() < deadline:
            attempt += 1
            try:
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=10) as resp:
                    body = json.loads(resp.read().decode())
                    results.raw_response = body
                    logging.info(f"Warden results response: {body}")

                    # Check if all checks are completed
                    all_completed = True
                    if isinstance(body, dict):
                        for key, value in body.items():
                            if isinstance(value, dict) and value.get('status') in ('running', 'pending'):
                                all_completed = False
                                break
                    elif isinstance(body, list):
                        for item in body:
                            if isinstance(item, dict) and item.get('status') in ('running', 'pending'):
                                all_completed = False
                                break

                    if all_completed:
                        logging.info(f"Warden checks completed after {attempt} attempt(s)")
                        results.poll_success = True
                        results.checks = self._parse_warden_response(body)
                        return results

                logging.debug(f"Warden checks still running after attempt {attempt}")
            except Exception as exc:
                logging.debug(f"Warden checks attempt {attempt} failed: {exc}")

            time.sleep(_WARDEN_POLL_INTERVAL_S)

        error_msg = f"Warden checks timeout after {attempt} attempts ({_WARDEN_POLL_TIMEOUT_S}s)"
        logging.error(error_msg)
        results.error_message = error_msg
        return results

    def _parse_warden_response(self, body: dict | list) -> dict[str, WardenCheckResult]:
        """Parse the raw orchestrator warden response into WardenCheckResult objects.

        The response format may vary; this method handles both dict and list formats:
        - Dict format: {check_name: {status, violations, affected_hosts, ...}, ...}
        - List format: [{name, status, violations, affected_hosts, ...}, ...]
        - Per-host format: {host: {safety_checks: [...], ...}, _orchestrator: {...}}

        Returns:
            Dictionary of check name -> WardenCheckResult
        """
        checks = {}

        if isinstance(body, dict):
            # Check if this is a per-host format with _orchestrator key
            if '_orchestrator' in body or any(
                isinstance(v, dict) and 'safety_checks' in v
                for v in body.values()
            ):
                checks = self._parse_per_host_response(body)
            else:
                # Simple dict format: {check_name: {status, ...}}
                for name, info in body.items():
                    if isinstance(info, dict):
                        check = WardenCheckResult(
                            name=name,
                            status=info.get('status', 'unknown'),
                            violations=info.get('violations', []),
                            affected_hosts=set(info.get('affected_hosts', [])),
                            details=info,
                        )
                        checks[name] = check

        elif isinstance(body, list):
            for item in body:
                if isinstance(item, dict):
                    name = item.get('name', f'check_{len(checks)}')
                    check = WardenCheckResult(
                        name=name,
                        status=item.get('status', 'unknown'),
                        violations=item.get('violations', []),
                        affected_hosts=set(item.get('affected_hosts', [])),
                        details=item,
                    )
                    checks[name] = check

        return checks

    def _parse_per_host_response(self, body: dict) -> dict[str, WardenCheckResult]:
        """Parse per-host warden response format.

        Format:
        {
            "host1": {"safety_checks": [...], "liveness_checks": [...], ...},
            "host2": {...},
            "_orchestrator": {"safety_checks": [...], "liveness_checks": [...], ...}
        }

        The ``_orchestrator`` entry contains cluster-wide checks (especially
        liveness checks that are *only* present there and never in per-host
        entries).  It is processed with the same logic as host entries.

        Returns:
            Dictionary of check name -> WardenCheckResult (aggregated across hosts)
        """
        checks: dict[str, WardenCheckResult] = {}

        # Process ALL entries including _orchestrator.
        # _orchestrator holds cluster-wide liveness checks that do not appear
        # in per-host entries, so it must NOT be skipped.
        for key, entry_data in body.items():
            if not isinstance(entry_data, dict):
                continue

            is_orchestrator = key.startswith('_')
            host = None if is_orchestrator else key

            for check_category in ('safety_checks', 'liveness_checks'):
                check_type = 'safety' if check_category == 'safety_checks' else 'liveness'
                if is_orchestrator:
                    check_type = f'orchestrator_{check_type}'
                entry_checks = entry_data.get(check_category, [])
                if not isinstance(entry_checks, list):
                    continue

                for check_info in entry_checks:
                    if not isinstance(check_info, dict):
                        continue
                    name = check_info.get('name', 'unknown')
                    status = check_info.get('status', 'unknown')
                    violations = check_info.get('violations', [])

                    if name in checks:
                        # Merge with existing check
                        existing = checks[name]
                        if status != 'ok':
                            existing.status = status
                            if host:
                                existing.affected_hosts.add(host)
                        existing.violations.extend(violations)
                    else:
                        affected = set()
                        if status != 'ok' and host:
                            affected.add(host)
                        checks[name] = WardenCheckResult(
                            name=name,
                            status=status,
                            violations=violations,
                            affected_hosts=affected,
                            details=check_info,
                            check_type=check_type,
                        )

            # Also handle any flat orchestrator-level entries that are not
            # inside safety_checks/liveness_checks arrays (legacy format).
            if is_orchestrator:
                for sub_name, sub_info in entry_data.items():
                    if sub_name in ('safety_checks', 'liveness_checks'):
                        continue  # Already processed above
                    if not isinstance(sub_info, dict):
                        continue
                    if sub_name not in checks:
                        checks[sub_name] = WardenCheckResult(
                            name=sub_name,
                            status=sub_info.get('status', 'unknown'),
                            violations=sub_info.get('violations', []),
                            affected_hosts=set(sub_info.get('affected_hosts', [])),
                            details=sub_info,
                            check_type='orchestrator',
                        )

        return checks

    # ------------------------------------------------------------------
    # Diagnostics collection (orchestrator + SSH for coredumps)
    # ------------------------------------------------------------------

    def check_nodes_diagnostics_with_timing(
        self, result: StressUtilTestResults, start_time: float, end_time: float
    ) -> tuple[list[NodeErrors], WardenResults]:
        """Collect node diagnostic information using the nemesis orchestrator.

        Triggers warden checks via the orchestrator API and collects coredumps
        via SSH (breakpad). Combines results into NodeErrors list.

        Each collection step (warden checks, coredumps) is isolated so that
        a failure in one does not prevent the others from completing.

        Args:
            result: Workload execution results (errors are added to it)
            start_time: Start timestamp for diagnostics interval
            end_time: End timestamp for diagnostics interval

        Returns:
            Tuple of:
            - list[NodeErrors]: List of node error objects containing diagnostic info
            - WardenResults: Full warden check results for report generation
        """
        # Step 1: Collect warden check results from orchestrator (independent)
        warden_results = WardenResults()
        try:
            warden_results = self._request_warden_checks()
        except Exception as exc:
            error_msg = f"Failed to collect warden checks: {exc}"
            logging.error(error_msg)
            warden_results.error_message = error_msg

        # Step 2: Collect coredumps via SSH (independent, may timeout)
        core_hashes = {}
        try:
            core_hashes = self.get_core_hashes_by_pod(start_time, end_time)
            pass  # Coredump collection currently disabled
        except Exception as exc:
            logging.error(f"Failed to collect coredumps (warden results preserved): {exc}")

        if not warden_results.poll_success:
            logging.warning(
                f"Warden checks did not complete successfully: {warden_results.error_message}"
            )

        # Extract host-level issues from warden results
        oom_hosts = warden_results.get_hosts_with_oom()
        verify_hosts = warden_results.get_hosts_with_verify_fails()
        sanitizer_hosts = warden_results.get_hosts_with_sanitizer()

        # Build NodeErrors for each node with issues
        node_errors = []
        for node in self.nodes:
            has_cores = bool(core_hashes.get(node.slot, []))
            has_oom = node.host in oom_hosts
            has_verifies = node.host in verify_hosts
            has_san_errors = node.host in sanitizer_hosts

            if has_cores or has_oom or has_verifies or has_san_errors:
                node_error = NodeErrors(node, 'diagnostic info collected')
                node_error.core_hashes = core_hashes.get(node.slot, [])
                node_error.was_oom = has_oom
                if has_verifies:
                    node_error.verifies = verify_hosts[node.host]
                node_errors.append(node_error)

                # Add errors to result
                if has_verifies:
                    result.add_error(f'Node {node.host} had {verify_hosts[node.host]} VERIFY fails')
                if has_cores:
                    result.add_error(f'Node {node.slot} has {len(node_error.core_hashes)} coredump(s)')
                if has_oom:
                    result.add_error(f'Node {node.slot} experienced OOM')
                if has_san_errors:
                    result.add_error(f'Node {node.host} has SAN errors')

        return node_errors, warden_results

    def get_core_hashes_by_pod(self, start_time: float, end_time: float) -> dict[str, list[tuple[str, str, str]]]:
        """Collect coredump hashes from all hosts via SSH/breakpad.

        Args:
            start_time: Start of time interval (Unix timestamp)
            end_time: End of time interval (Unix timestamp)

        Returns:
            Dictionary of slot -> list of (core_id, core_hash, version) tuples
        """
        core_processes = {
            h: execute_ssh(h, 'sudo flock /tmp/brk_pad /Berkanavt/breakpad/bin/kikimr_breakpad_analizer.sh')
            for h in self.hosts
        }

        core_hashes = {}
        for h, exec in core_processes.items():
            exec.wait(check_exit_code=False, timeout=_COREDUMP_ANALYZE_TIMEOUT)
            if exec.returncode != 0:
                logging.error(f'Error while process coredumps on host {h}: {exec.stderr}')
            exec = execute_ssh(h, ('find /coredumps/ -name "sended_*.json" '
                                   f'-mmin -{(10 + time.time() - start_time) / 60} -mmin +{(-10 + time.time() - end_time) / 60}'
                                   ' | while read FILE; do cat $FILE; echo -n ","; done'))
            exec.wait(check_exit_code=False, timeout=_COREDUMP_SEARCH_TIMEOUT)
            if exec.returncode == 0:
                for core in json.loads(f'[{exec.stdout.strip(",")}]'):
                    slot = f"{core.get('slot', '')}@{h}"
                    core_hashes.setdefault(slot, [])
                    v2_info = (core.get('core_id', ''), core.get('core_hash', ''), 'v2')
                    if (v2_info[0] is None and v2_info[1] is None) or (v2_info[0] == 0 and v2_info[1] == 0):
                        v3_info = (core.get('core_uuid_v3', ''), core.get('core_hash_v3', ''), 'v3')
                        core_hashes[slot].append(v3_info)
                    else:
                        core_hashes[slot].append(v2_info)
            else:
                logging.error(f'Error while search coredumps on host {h}: {exec.stderr}')
        return core_hashes

    def attach_nemesis_logs(self, start_time):
        """Attach nemesis logs to Allure report.

        Args:
            start_time: Start time of nemesis service
        """
        tz = timezone('Europe/Moscow')
        start = datetime.fromtimestamp(start_time, tz).strftime("%Y-%m-%d %H:%M:%S")
        end = datetime.fromtimestamp(datetime.now().timestamp(), tz).strftime("%Y-%m-%d %H:%M:%S")
        cmd = f"sudo journalctl -u nemesis-agent -S '{start}' -U '{end}'"
        nemesis_logs = {}
        for host in self.hosts:
            try:
                nemesis_logs[host] = execute_command(host, cmd, timeout=30).stdout
            except BaseException as e:
                logging.error(f'Failed to read nemesis logs: {e}')

        for host, stdout in nemesis_logs.items():
            allure.attach(stdout, f'Nemesis_{host}_logs', allure.attachment_type.TEXT)

    def attach_kikimr_logs(self, start_time, attach_name):
        """Attach kikimr logs to Allure report.

        Args:
            start_time: Start time for log collection
            attach_name: Name prefix for attachments
        """
        tz = timezone('Europe/Moscow')
        start = datetime.fromtimestamp(start_time, tz).isoformat()
        cmd = f"ulimit -n 100500;unified_agent select -S '{start}' -s {{storage}}{{container}}"
        exec_kikimr = {
            '': {},
        }
        exec_start = deepcopy(exec_kikimr)
        for host in self.hosts:
            for c in exec_kikimr.keys():
                try:
                    exec_kikimr[c][host] = execute_ssh(host, cmd.format(
                        storage='kikimr',
                        container=f' -m k8s_container:{c}' if c else ''
                    ))
                except BaseException as e:
                    logging.error(e)
            for c in exec_start.keys():
                try:
                    exec_start[c][host] = execute_ssh(host, cmd.format(
                        storage='kikimr-start',
                        container=f' -m k8s_container:{c}' if c else ''))
                except BaseException as e:
                    logging.error(e)

        error_log = ''
        for c, execs in exec_start.items():
            for host, e in sorted(execs.items()):
                e.wait(check_exit_code=False, timeout=_LOG_COLLECT_TIMEOUT)
                error_log += f'{host}:\n{e.stdout if e.returncode == 0 else e.stderr}\n'
            allure.attach(error_log, f'{attach_name}_{c}_stderr', allure.attachment_type.TEXT)

        for c, execs in exec_kikimr.items():
            dir = os.path.join(yatest.common.tempfile.gettempdir(), f'{attach_name}_{c}_logs')
            os.makedirs(dir, exist_ok=True)
            for host, e in execs.items():
                e.wait(check_exit_code=False, timeout=_LOG_COLLECT_TIMEOUT)
                with open(os.path.join(dir, host), 'w') as f:
                    f.write(e.stdout if e.returncode == 0 else e.stderr)
            archive = dir + '.tar.gz'
            yatest.common.execute(['tar', '-C', dir, '-czf', archive, '.'], timeout=_TAR_TIMEOUT)
            allure.attach.file(archive, f'{attach_name}_{c}_logs', extension='tar.gz')

    def perform_verification_with_cluster_check(self, workload_names: list[str], nemesis_enabled: bool) -> None:
        """Perform pre-test cluster verification and record the results.

        Args:
            workload_names: List of workload names being tested
            nemesis_enabled: Whether nemesis is enabled for the test
        """
        with allure.step("Pre-test verification"):
            cluster_issue = self._check_cluster_health()

            test_event_report(
                event_kind='ClusterCheck',
                workload_names=workload_names,
                nemesis_enabled=nemesis_enabled,
                verification_phase="pre_test_verification",
                check_type="cluster_availability",
                cluster_issue=cluster_issue
            )

            if cluster_issue.get("issue_type") is not None:
                raise Exception(f"Cluster verification failed: {cluster_issue['issue_description']}")

    def _check_cluster_health(self) -> dict:
        """Check cluster health status and return problem information.

        Returns:
            dict: Cluster issue information or empty dict if all OK
        """
        try:
            wait_error = YdbCluster.wait_ydb_alive(int(os.getenv('WAIT_CLUSTER_ALIVE_TIMEOUT', 2 * 60)))
            if wait_error:
                return create_cluster_issue("cluster_not_alive", f"Cluster functionality check failed: {wait_error}")

            nodes = YdbCluster.get_cluster_nodes(db_only=False)
            if not nodes:
                return create_cluster_issue("cluster_no_nodes", "No working cluster nodes found")

            return create_cluster_issue(None, "Cluster check passed successfully", len(nodes))

        except Exception as e:
            return create_cluster_issue("cluster_check_exception", f"Exception during cluster check: {e}")

    def check_nemesis_status(self, nemesis_started: bool, nemesis_enabled: bool, workload_names: list[str]) -> None:
        """Check nemesis status after workload execution.

        Args:
            nemesis_started: Whether nemesis actually started
            nemesis_enabled: Whether nemesis was enabled for the test
            workload_names: List of workload names being tested
        """
        if nemesis_enabled:
            if not nemesis_started:
                cluster_issue = create_cluster_issue(
                    "nemesis_startup_failed",
                    "Nemesis was enabled for test but failed to start. Check nemesis management logs for details.",
                    0
                )
                test_event_report(
                    event_kind='ClusterCheck',
                    workload_names=workload_names,
                    nemesis_enabled=nemesis_enabled,
                    verification_phase="post_workload_verification",
                    check_type="nemesis_status_check",
                    cluster_issue=cluster_issue
                )
                logging.warning("Nemesis was enabled but not started for test")
            else:
                cluster_issue = create_cluster_issue(
                    None,
                    "Nemesis was successfully running during workload execution",
                    1
                )
                test_event_report(
                    event_kind='ClusterCheck',
                    workload_names=workload_names,
                    nemesis_enabled=nemesis_enabled,
                    verification_phase="post_workload_verification",
                    check_type="nemesis_status_check",
                    cluster_issue=cluster_issue
                )
                logging.info("Nemesis was successfully running for test")


def create_cluster_issue(issue_type: str, description: str, nodes_count: int = 0) -> dict:
    """Create cluster issue information.

    Args:
        issue_type: Issue type (None if all OK)
        description: Problem description
        nodes_count: Node count (0 if problem, actual count if OK)

    Returns:
        dict: Cluster issue statistics
    """
    if issue_type is None:
        return {
            "issue_type": None,
            "issue_description": description,
            "nodes_count": nodes_count,
            "is_critical": False
        }
    else:
        return {
            "issue_type": issue_type,
            "issue_description": description,
            "nodes_count": nodes_count,
            "is_critical": True
        }
