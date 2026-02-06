from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime
from pytz import timezone
import json
import allure
import yatest
import os
import time
import re
import logging
from ydb.tests.library.stability.utils.remote_execution import execute_command, execute_ssh
from ydb.tests.library.stability.utils.results_models import StressUtilTestResults
from ydb.tests.library.stability.utils.upload_results import test_event_report
from ydb.tests.olap.lib.allure_utils import NodeErrors
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class ErrorsCollector:
    def __init__(self, hosts: set[str], nodes):
        self.hosts = hosts
        self.nodes = nodes

    def check_nodes_diagnostics_with_timing(self, result: StressUtilTestResults, start_time: float, end_time: float) -> list[NodeErrors]:
        """
        Collects node diagnostic information within a custom time interval.
        Checks for coredumps and OOMs across all nodes from saved state.

        Args:
            result: Workload execution results
            start_time: Start timestamp for diagnostics interval
            end_time: End timestamp for diagnostics interval

        Returns:
            list[NodeErrors]: List of node error objects containing diagnostic info
        """

        # Собираем диагностическую информацию для всех хостов
        core_hashes = self.get_core_hashes_by_pod(start_time, end_time)
        ooms = self.get_hosts_with_omms(start_time, end_time)
        hosts_with_sanitizer = self.get_sanitizer_events(start_time, end_time)
        hosts_with_verifies = self.count_verify_fails(start_time, end_time)

        # Создаем NodeErrors для каждой ноды с диагностической информацией
        node_errors = []
        for node in self.nodes:
            # Create NodeErrors only if there are coredumps or OOMs
            has_cores = bool(core_hashes.get(node.slot, []))
            has_oom = node.host in ooms
            has_verifies = node.host in hosts_with_verifies
            has_san_errors = node.host in hosts_with_sanitizer

            if has_cores or has_oom or node.host in hosts_with_verifies or node.host in hosts_with_sanitizer:
                node_error = NodeErrors(node, 'diagnostic info collected')
                node_error.core_hashes = core_hashes.get(node.slot, [])
                node_error.was_oom = has_oom
                if node.host in hosts_with_verifies:
                    node_error.verifies = hosts_with_verifies[node.host]
                if node.host in hosts_with_sanitizer:
                    node_error.sanitizer_errors = hosts_with_sanitizer[node.host][0]
                    node_error.sanitizer_output = hosts_with_sanitizer[node.host][1]
                node_errors.append(node_error)

                # Add errors to result (cores and OOMs are considered errors)
                if has_verifies:
                    result.add_error(f'Node {node.host} had {hosts_with_verifies[node.host]} VERIFY fails')
                if has_cores:
                    result.add_error(f'Node {node.slot} has {len(node_error.core_hashes)} coredump(s)')
                if has_oom:
                    result.add_error(f'Node {node.slot} experienced OOM')
                if has_san_errors:
                    result.add_error(f'Node {node.host} has SAN errors')

        return node_errors

    def check_nodes_verifies_with_timing(self, start_time: float, end_time: float):
        """Aggregates information about VERIFY failed errors across hosts.

        Collects and analyzes verification failures within specified time range,
        grouping errors by unique patterns and counting occurrences per host.

        Args:
            hosts: Set of target hostnames to analyze
            start_time: Beginning of analysis time interval (Unix timestamp)
            end_time: End of analysis time interval (Unix timestamp)

        Returns:
            Dictionary with verification failure patterns as keys and structured information as values:
            {
                "verify failed at example.cpp:123": {
                    "full_trace": "Full VERIFY failed text",
                    "hosts_count": {
                        "host1.example.com": 3,
                        "host2.example.com": 1
                    }
                }
            }
        """
        return self._get_verify_fails(start_time, end_time)

    def get_core_hashes_by_pod(self, start_time: float, end_time: float) -> dict[str, list[tuple[str, str]]]:
        core_processes = {
            h: execute_ssh(h, 'sudo flock /tmp/brk_pad /Berkanavt/breakpad/bin/kikimr_breakpad_analizer.sh')
            for h in self.hosts
        }

        core_hashes = {}
        for h, exec in core_processes.items():
            exec.wait(check_exit_code=False)
            if exec.returncode != 0:
                logging.error(f'Error while process coredumps on host {h}: {exec.stderr}')
            exec = execute_ssh(h, ('find /coredumps/ -name "sended_*.json" '
                                   f'-mmin -{(10 + time.time() - start_time) / 60} -mmin +{(-10 + time.time() - end_time) / 60}'
                                   ' | while read FILE; do cat $FILE; echo -n ","; done'))
            exec.wait(check_exit_code=False)
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

    def get_sanitizer_events(self, start_time: float, end_time: float) -> dict[str, str]:
        """Aggregates information about sanitizer errors across hosts.

        Collects and analyzes sanitizer failures within specified time range.

        Args:
            hosts: Set of target hostnames to analyze
            start_time: Beginning of analysis time interval (Unix timestamp)
            end_time: End of analysis time interval (Unix timestamp)

        Returns:
            Dictionary with hosts as keys and number of occurances and sanitizer output (first 150 lines per error) as values:
            {
                "host1.example.com": (18, "First 150 lines of every ThreadSanitizer error"),
            }
        """
        tz = timezone('Europe/Moscow')
        start = datetime.fromtimestamp(start_time, tz).isoformat()
        end = datetime.fromtimestamp(end_time + 10, tz).isoformat()

        sanitizer_regex_params = r'(ERROR|WARNING): (AddressSanitizer|MemorySanitizer|ThreadSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer)'

        core_processes = {
            h: execute_ssh(h, f"ulimit -n 100500;unified_agent select -S '{start}' -U '{end}' -s kikimr-start | grep -P -A 150 '{sanitizer_regex_params}'")
            for h in self.hosts
        }

        def count_regex_matches(pattern, text):
            return sum(1 for _ in re.finditer(pattern, text))

        host_logs = {}
        for h, exec in core_processes.items():
            exec.wait(check_exit_code=False)
            if exec.returncode != 0:
                logging.error(f'Error while process sanitizers on host {h}: {exec.stderr}')
            else:
                host_logs[h] = (count_regex_matches(sanitizer_regex_params, exec.stdout), exec.stdout)
        return host_logs

    def _get_verify_fails(
        self,
        start_time: float,
        end_time: float
    ) -> dict[str, dict[str, str | dict[str, int]]]:
        """Aggregates information about VERIFY failed errors across hosts.

        Collects and analyzes verification failures within specified time range,
        grouping errors by unique patterns and counting occurrences per host.

        Args:
            hosts: Set of target hostnames to analyze
            start_time: Beginning of analysis time interval (Unix timestamp)
            end_time: End of analysis time interval (Unix timestamp)

        Returns:
            Dictionary with verification failure patterns as keys and structured information as values:
            {
                "verify failed at example.cpp:123": {
                    "full_trace": "Full VERIFY failed text",
                    "hosts_count": {
                        "host1.example.com": 3,
                        "host2.example.com": 1
                    }
                }
            }
        """
        tz = timezone('Europe/Moscow')
        start = datetime.fromtimestamp(start_time, tz).isoformat()
        end = datetime.fromtimestamp(end_time + 10, tz).isoformat()

        # Matches VERIFY failed error blocks in logs.
        # Capture groups:
        #   1. The entire VERIFY failed block (full match).
        #   2. The indented lines following the VERIFY failed header (typically stack trace or error details).
        #   3. Lines starting with a digit and a dot (e.g., stack frame lines), possibly repeated.

        verify_regex_params = r'(VERIFY failed \(.+\):\s*.*\n(\s+.*\n\s+.*\n)(\d+\..*\n)*)'
        verify_regex = re.compile(verify_regex_params)

        core_processes = {
            h: execute_ssh(h, fr"ulimit -n 100500;unified_agent select -S '{start}' -U '{end}' -s kikimr-start | grep -Pzo -i '{verify_regex_params}' | head -n 50000")
            for h in self.hosts
        }

        host_matches = defaultdict(lambda: [])
        for h, exec in core_processes.items():
            exec.wait(check_exit_code=False, timeout=1200)
            if exec.returncode != 0:
                logging.error(f'Error while process VERIFY fails on host {h}: {exec.stderr}')
            else:
                verifies_stdout = exec.stdout
                matches = verify_regex.findall(verifies_stdout)
                for m in matches:
                    host_matches[h].append((m[0], m[1]))
        verifies_info = defaultdict(lambda: {
            'full_trace': None,
            'hosts_count': defaultdict(0)
        })
        for host, verify_match in host_matches.items():
            verifies_patterns = list(map(lambda m: m[1], verify_match))
            unique_verify_patterns = set(verifies_patterns)
            counters = Counter(verifies_patterns)
            for pattern in unique_verify_patterns:
                if pattern in verifies_info:
                    verifies_info[pattern]['hosts_count'][host] = counters[pattern]
                else:
                    verifies_info[pattern] = {
                        'full_trace': next(m for m in verify_match if m[1] == pattern),
                        'hosts_count': {host: counters[pattern]}
                    }
        return verifies_info

    def count_verify_fails(self, start_time: float, end_time: float) -> dict[str, int]:
        tz = timezone('Europe/Moscow')
        start = datetime.fromtimestamp(start_time, tz).isoformat()
        end = datetime.fromtimestamp(end_time + 10, tz).isoformat()

        verify_regex_params = r'VERIFY failed'

        core_processes = {
            h: execute_ssh(h, fr"ulimit -n 100500;unified_agent select -S '{start}' -U '{end}' -s kikimr-start | grep -ic '{verify_regex_params}'")
            for h in self.hosts
        }

        total_host_verify_fails = dict()
        for h, exec in core_processes.items():
            exec.wait(check_exit_code=False)
            if exec.returncode != 0:
                logging.error(f'Error while process VERIFY fails on host {h}: {exec.stderr}')
            else:
                total_host_verify_fails[h] = int(exec.stdout)

        return total_host_verify_fails

    def get_hosts_with_omms(self, start_time: float, end_time: float) -> set[str]:
        tz = timezone('Europe/Moscow')
        start = datetime.fromtimestamp(start_time, tz).strftime("%Y-%m-%d %H:%M:%S")
        end = datetime.fromtimestamp(end_time, tz).strftime("%Y-%m-%d %H:%M:%S")
        oom_cmd = f'sudo journalctl -k -q --no-pager -S "{start}" -U "{end}" --grep "Out of memory: Kill" --case-sensitive=false'
        ooms = set()
        for h in self.hosts:
            exec = execute_ssh(h, oom_cmd)
            exec.wait(check_exit_code=False)
            if exec.returncode == 0:
                if exec.stdout:
                    ooms.add(h)
            else:
                logging.error(f'Error while search OOMs on host {h}: {exec.stderr}')
        return ooms

    def attach_nemesis_logs(self, start_time):
        """
        Attach nemesis logs to Allure report

        Args:
            start_time: Start time of nemesis service
        """
        tz = timezone('Europe/Moscow')
        start = datetime.fromtimestamp(start_time, tz).strftime("%Y-%m-%d %H:%M:%S")
        end = datetime.fromtimestamp(datetime.now().timestamp(), tz).strftime("%Y-%m-%d %H:%M:%S")
        cmd = f"sudo journalctl -u nemesis -S '{start}' -U '{end}'"
        nemesis_logs = {}
        for host in self.hosts:
            try:
                nemesis_logs[host] = execute_command(host, cmd, timeout=30).stdout
            except BaseException as e:
                logging.error(f'Failed to read nemesis logs: {e}')

        for host, stdout in nemesis_logs.items():
            allure.attach(stdout, f'Nemesis_{host}_logs', allure.attachment_type.TEXT)

    def attach_kikimr_logs(self, start_time, attach_name):
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
                e.wait(check_exit_code=False)
                error_log += f'{host}:\n{e.stdout if e.returncode == 0 else e.stderr}\n'
            allure.attach(error_log, f'{attach_name}_{c}_stderr', allure.attachment_type.TEXT)

        for c, execs in exec_kikimr.items():
            dir = os.path.join(yatest.common.tempfile.gettempdir(), f'{attach_name}_{c}_logs')
            os.makedirs(dir, exist_ok=True)
            for host, e in execs.items():
                e.wait(check_exit_code=False)
                with open(os.path.join(dir, host), 'w') as f:
                    f.write(e.stdout if e.returncode == 0 else e.stderr)
            archive = dir + '.tar.gz'
            yatest.common.execute(['tar', '-C', dir, '-czf', archive, '.'])
            allure.attach.file(archive, f'{attach_name}_{c}_logs', extension='tar.gz')

    def perform_verification_with_cluster_check(self, workload_names: list[str], nemesis_enabled: bool) -> None:
        """
        Performs pre-test cluster verification and records the results.

        Args:
            workload_names: List of workload names being tested
            nemesis_enabled: Whether nemesis is enabled for the test
        """

        with allure.step("Pre-test verification"):
            # Выполняем проверки кластера
            cluster_issue = self._check_cluster_health()

            # Записываем результат проверки кластера через универсальный метод
            test_event_report(
                event_kind='ClusterCheck',
                workload_names=workload_names,
                nemesis_enabled=nemesis_enabled,
                verification_phase="pre_test_verification",
                check_type="cluster_availability",
                cluster_issue=cluster_issue
            )

            # Если проверка кластера не прошла успешно, поднимаем исключение
            if cluster_issue.get("issue_type") is not None:
                raise Exception(f"Cluster verification failed: {cluster_issue['issue_description']}")

    def _check_cluster_health(self) -> dict:
        """
        Checks cluster health status and returns problem information.

        Returns:
            dict: Cluster issue information or empty dict if all OK
        """
        try:
            # Проверяем доступность кластера
            wait_error = YdbCluster.wait_ydb_alive(int(os.getenv('WAIT_CLUSTER_ALIVE_TIMEOUT', 20 * 60)))
            if wait_error:
                return create_cluster_issue("cluster_not_alive", f"Cluster functionality check failed: {wait_error}")

            # Проверяем наличие нод
            nodes = YdbCluster.get_cluster_nodes(db_only=False)
            if not nodes:
                return create_cluster_issue("cluster_no_nodes", "No working cluster nodes found")

            # Кластер OK
            return create_cluster_issue(None, "Cluster check passed successfully", len(nodes))

        except Exception as e:
            return create_cluster_issue("cluster_check_exception", f"Exception during cluster check: {e}")

    def check_nemesis_status(self, nemesis_started: bool, nemesis_enabled: bool, workload_names: list[str]) -> None:
        """
        Checks nemesis status after workload execution.
        Creates ClusterCheck record if nemesis was supposed to run but didn't start.

        Args:
            nemesis_started: Whether nemesis actually started
            nemesis_enabled: Whether nemesis was enabled for the test
            workload_names: List of workload names being tested
        """
        # Проверяем, должен ли был запуститься nemesis
        if nemesis_enabled:
            # Nemesis должен был запуститься - проверяем статус
            if not nemesis_started:
                # Nemesis должен был запуститься, но не запустился
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
                # Nemesis запустился успешно - создаем успешную ClusterCheck запись
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
    """
    Creates cluster issue information.

    Args:
        issue_type: Issue type (None if all OK)
        description: Problem description
        nodes_count: Node count (0 if problem, actual count if OK)

    Returns:
        dict: Cluster issue statistics with keys:
            - issue_type: str or None
            - issue_description: str
            - nodes_count: int
            - is_critical: bool
    """
    if issue_type is None:
        # Кластер OK
        return {
            "issue_type": None,
            "issue_description": description,
            "nodes_count": nodes_count,
            "is_critical": False
        }
    else:
        # Есть проблема
        return {
            "issue_type": issue_type,
            "issue_description": description,
            "nodes_count": nodes_count,
            "is_critical": True
        }
