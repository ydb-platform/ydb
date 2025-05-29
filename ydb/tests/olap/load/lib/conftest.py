from __future__ import annotations
import allure
import json
import logging
import os
import pytest
import yatest

from allure_commons._core import plugin_manager
from allure_pytest.listener import AllureListener
from copy import deepcopy
from datetime import datetime
from pytz import timezone
from time import time
from typing import Optional, Union
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper, WorkloadType, CheckCanonicalPolicy
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.allure_utils import allure_test_description, NodeErrors
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import ScenarioTestHelper


class LoadSuiteBase:
    class QuerySettings:
        def __init__(self, iterations: Optional[int] = None, timeout: Optional[float] = None, query_prefix: Optional[str] = None) -> None:
            self.iterations = iterations
            self.timeout = timeout
            self.query_prefix = query_prefix

    iterations: int = 5
    workload_type: WorkloadType = None
    timeout: float = 1800.
    check_canonical: CheckCanonicalPolicy = CheckCanonicalPolicy.NO
    query_syntax: str = ''
    query_settings: dict[int, LoadSuiteBase.QuerySettings] = {}
    scale: Optional[int] = None
    query_prefix: str = get_external_param('query-prefix', '')
    verify_data: bool = True
    __nodes_state: Optional[dict[str, YdbCluster.Node]] = None

    @classmethod
    def get_external_path(cls) -> str:
        if not hasattr(cls, 'external_folder'):
            return ''
        result = os.getenv('EXTERNAL_DATA')
        if result is None:
            result = os.getenv('ARCADIA_EXTERNAL_DATA', '')
            if result:
                result = yatest.common.source_path(result)

        if result and cls.external_folder:
            return os.path.join(result, cls.external_folder)
        return result

    @classmethod
    def suite(cls) -> str:
        result = cls.__name__
        if result.startswith('Test'):
            return result[4:]
        return result

    @classmethod
    def _get_query_settings(cls, query_num: Optional[int] = None, query_name: Optional[str] = None) -> QuerySettings:
        result = LoadSuiteBase.QuerySettings(
            iterations=cls.iterations,
            timeout=cls.timeout,
            query_prefix=cls.query_prefix
        )
        for key in query_name, query_num:
            if key is None:
                continue
            q = cls.query_settings.get(key, LoadSuiteBase.QuerySettings())
            if q.iterations is not None:
                result.iterations = q.iterations
            if q.timeout is not None:
                result.timeout = q.timeout
            if q.query_prefix is not None:
                result.query_prefix = q.query_prefix
        return result

    @classmethod
    @allure.step('check tables size')
    def check_tables_size(cls, folder: Optional[str], tables: dict[str, int]):
        wait_error = YdbCluster.wait_ydb_alive(
            int(os.getenv('WAIT_CLUSTER_ALIVE_TIMEOUT', 20 * 60)), (
                f'{YdbCluster.tables_path}/{folder}'
                if folder is not None
                else [f'{YdbCluster.tables_path}/{t}' for t in tables.keys()]
            ))
        if wait_error is not None:
            pytest.fail(f'Cluster is dead: {wait_error}')
        sth = ScenarioTestHelper(None)
        errors: list[str] = []
        for table, expected_size in tables.items():
            if folder is None:
                table_full = table
            elif folder.endswith('/') or table.startswith('/'):
                table_full = f'{folder}{table}'
            else:
                table_full = f'{folder}/{table}'
            size = sth.get_table_rows_count(table_full)
            if size != expected_size:
                errors.append(f'table `{table}`: expect {expected_size}, but actually is {size};')
        if len(errors) > 0:
            msg = "\n".join(errors)
            pytest.fail(f'Unexpected tables size in `{folder}`:\n {msg}')

    @staticmethod
    def __execute_ssh(host: str, cmd: str):
        ssh_cmd = ['ssh', "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]
        ssh_user = os.getenv('SSH_USER')
        if ssh_user is not None:
            ssh_cmd += ['-l', ssh_user]
        ssh_key_file = os.getenv('SSH_KEY_FILE')
        if ssh_key_file is not None:
            ssh_cmd += ['-i', ssh_key_file]
        return yatest.common.execute(ssh_cmd + [host, cmd], wait=False, text=True)

    @classmethod
    def __hide_query_text(cls, text, query_text):
        if os.getenv('SECRET_REQUESTS', '') != '1' or not query_text:
            return text
        return text.replace(query_text, '<Query text hided by sequrity reasons>')

    @classmethod
    def __attach_logs(cls, start_time, attach_name, query_text):
        hosts = [node.host for node in filter(lambda x: x.role == YdbCluster.Node.Role.STORAGE, YdbCluster.get_cluster_nodes())]
        tz = timezone('Europe/Moscow')
        start = datetime.fromtimestamp(start_time, tz).isoformat()
        cmd = f"ulimit -n 100500;unified_agent select -S '{start}' -s {{storage}}{{container}}"
        exec_kikimr = {
            '': {},
        }
        exec_start = deepcopy(exec_kikimr)
        for host in hosts:
            for c in exec_kikimr.keys():
                try:
                    exec_kikimr[c][host] = cls.__execute_ssh(host, cmd.format(
                        storage='kikimr',
                        container=f' -m k8s_container:{c}' if c else ''
                    ))
                except BaseException as e:
                    logging.error(e)
            for c in exec_start.keys():
                try:
                    exec_start[c][host] = cls.__execute_ssh(host, cmd.format(
                        storage='kikimr-start',
                        container=f' -m k8s_container:{c}' if c else ''))
                except BaseException as e:
                    logging.error(e)

        error_log = ''
        for c, execs in exec_start.items():
            for host, e in sorted(execs.items()):
                e.wait(check_exit_code=False)
                error_log += f'{host}:\n{e.stdout if e.returncode == 0 else e.stderr}\n'
            allure.attach(cls.__hide_query_text(error_log, query_text), f'{attach_name}_{c}_stderr', allure.attachment_type.TEXT)

        for c, execs in exec_kikimr.items():
            dir = os.path.join(yatest.common.tempfile.gettempdir(), f'{attach_name}_{c}_logs')
            os.makedirs(dir, exist_ok=True)
            for host, e in execs.items():
                e.wait(check_exit_code=False)
                with open(os.path.join(dir, host), 'w') as f:
                    f.write(cls.__hide_query_text(e.stdout if e.returncode == 0 else e.stderr, query_text))
            archive = dir + '.tar.gz'
            yatest.common.execute(['tar', '-C', dir, '-czf', archive, '.'])
            allure.attach.file(archive, f'{attach_name}_{c}_logs', extension='tar.gz')

    @classmethod
    def save_nodes_state(cls) -> None:
        cls.__nodes_state = {n.slot: n for n in YdbCluster.get_cluster_nodes(db_only=True)}

    @classmethod
    def __get_core_hashes_by_pod(cls, hosts: set[str], start_time: float, end_time: float) -> dict[str, list[tuple[str, str]]]:
        core_processes = {
            h: cls.__execute_ssh(h, 'sudo flock /tmp/brk_pad /Berkanavt/breakpad/bin/kikimr_breakpad_analizer.sh')
            for h in hosts
        }

        core_hashes = {}
        for h, exec in core_processes.items():
            exec.wait(check_exit_code=False)
            if exec.returncode != 0:
                logging.error(f'Error while process coredumps on host {h}: {exec.stderr}')
            exec = cls.__execute_ssh(h, ('find /coredumps/ -name "sended_*.json" '
                                         f'-mmin -{(10 + time() - start_time) / 60} -mmin +{(-10 + time() - end_time) / 60}'
                                         ' | while read FILE; do cat $FILE; echo -n ","; done'))
            exec.wait(check_exit_code=False)
            if exec.returncode == 0:
                for core in json.loads(f'[{exec.stdout.strip(",")}]'):
                    slot = f"{core.get('slot', '')}@{h}"
                    core_hashes.setdefault(slot, [])
                    core_hashes[slot].append((core.get('core_id', ''), core.get('core_hash', '')))
            else:
                logging.error(f'Error while search coredumps on host {h}: {exec.stderr}')
        return core_hashes

    @classmethod
    def __get_hosts_with_omms(cls, hosts: set[str], start_time: float, end_time: float) -> set[str]:
        tz = timezone('Europe/Moscow')
        start = datetime.fromtimestamp(start_time, tz).strftime("%Y-%m-%d %H:%M:%S")
        end = datetime.fromtimestamp(end_time, tz).strftime("%Y-%m-%d %H:%M:%S")
        oom_cmd = f'sudo journalctl -k -q --no-pager -S "{start}" -U "{end}" --grep "Out of memory: Kill" --case-sensitive=false'
        ooms = set()
        for h in hosts:
            exec = cls.__execute_ssh(h, oom_cmd)
            exec.wait(check_exit_code=False)
            if exec.returncode == 0:
                if exec.stdout:
                    ooms.add(h)
            else:
                logging.error(f'Error while search OOMs on host {h}: {exec.stderr}')
        return ooms

    @classmethod
    def check_nodes(cls, result: YdbCliHelper.WorkloadRunResult, end_time: float) -> list[NodeErrors]:
        if cls.__nodes_state is None:
            return []
        node_errors = []
        fail_hosts = set()
        for node in YdbCluster.get_cluster_nodes(db_only=True):
            saved_node = cls.__nodes_state.get(node.slot)
            if saved_node is not None:
                if node.start_time > saved_node.start_time:
                    node_errors.append(NodeErrors(node, 'was restarted'))
                    fail_hosts.add(node.host)
                del cls.__nodes_state[node.slot]
        for _, node in cls.__nodes_state.items():
            node_errors.append(NodeErrors(node, 'is down'))
            fail_hosts.add(node.host)
        cls.__nodes_state = None
        if len(node_errors) == 0:
            return []

        core_hashes = cls.__get_core_hashes_by_pod(fail_hosts, result.start_time, end_time)
        ooms = cls.__get_hosts_with_omms(fail_hosts, result.start_time, end_time)
        for node in node_errors:
            node.core_hashes = core_hashes.get(f'{node.node.slot}', [])
            node.was_oom = node.node.host in ooms

        for err in node_errors:
            result.add_error(f'Node {err.node.slot} {err.message}')
        return node_errors

    @classmethod
    def process_query_result(cls, result: YdbCliHelper.WorkloadRunResult, query_name: str, upload: bool):
        def _get_duraton(stats, field):
            r = stats.get(field)
            return float(r) / 1e3 if r is not None else None

        def _duration_text(duration: float | int):
            s = f'{int(duration)}s ' if duration >= 1 else ''
            return f'{s}{int(duration * 1000) % 1000}ms'

        def _attach_plans(plan: YdbCliHelper.QueryPlan, name: str) -> None:
            if plan is None:
                return
            if plan.plan is not None:
                allure.attach(json.dumps(plan.plan), f'{name} json', attachment_type=allure.attachment_type.JSON)
            if plan.table is not None:
                allure.attach(plan.table, f'{name} table', attachment_type=allure.attachment_type.TEXT)
            if plan.ast is not None:
                allure.attach(plan.ast, f'{name} ast', attachment_type=allure.attachment_type.TEXT)
            if plan.svg is not None:
                allure.attach(plan.svg, f'{name} svg', attachment_type=allure.attachment_type.SVG)
            if plan.stats is not None:
                allure.attach(plan.stats, f'{name} stats', attachment_type=allure.attachment_type.TEXT)

        if result.query_out is not None:
            allure.attach(result.query_out, 'Query output', attachment_type=allure.attachment_type.TEXT)

        if result.explain.final_plan is not None:
            with allure.step('Explain'):
                _attach_plans(result.explain.final_plan, 'Plan')

        for iter_num in sorted(result.iterations.keys()):
            iter_res = result.iterations[iter_num]
            s = allure.step(f'Iteration {iter_num}')
            if iter_res.time:
                s.params['duration'] = _duration_text(iter_res.time)
            try:
                with s:
                    _attach_plans(iter_res.final_plan, 'Final plan')
                    _attach_plans(iter_res.in_progress_plan, 'In-progress plan')
                    if iter_res.error_message:
                        pytest.fail(iter_res.error_message)
            except BaseException:
                pass

        query_text = ''

        if result.stdout is not None:
            begin_text = 'Query text:\n'
            begin_pos = result.stdout.find(begin_text)
            if begin_pos >= 0:
                begin_pos += len(begin_text)
                end_pos = result.stdout.find("\n\n\titeration")
                if end_pos < 0:
                    end_pos = len(result.stdout)
                query_text = result.stdout[begin_pos:end_pos]
            if os.getenv('SECRET_REQUESTS', '') != '1':
                allure.attach(query_text, 'Query text', attachment_type=allure.attachment_type.TEXT)
            allure.attach(cls.__hide_query_text(result.stdout, query_text), 'Stdout', attachment_type=allure.attachment_type.TEXT)

        if result.stderr is not None:
            allure.attach(cls.__hide_query_text(result.stderr, query_text), 'Stderr', attachment_type=allure.attachment_type.TEXT)
        end_time = time()
        allure_test_description(
            cls.suite(), query_name,
            start_time=result.start_time, end_time=end_time, node_errors=cls.check_nodes(result, end_time)
        )
        stats = result.get_stats(query_name)
        for p in ['Mean']:
            if p in stats:
                allure.dynamic.parameter(p, _duration_text(stats[p] / 1000.))
        if os.getenv('NO_KUBER_LOGS') is None and not result.success:
            cls.__attach_logs(start_time=result.start_time, attach_name='kikimr', query_text=query_text)
        allure.attach(json.dumps(stats, indent=2), 'Stats', attachment_type=allure.attachment_type.JSON)
        if upload:
            ResultsProcessor.upload_results(
                kind='Load',
                suite=cls.suite(),
                test=query_name,
                timestamp=end_time,
                is_successful=result.success,
                min_duration=_get_duraton(stats, 'Min'),
                max_duration=_get_duraton(stats, 'Max'),
                mean_duration=_get_duraton(stats, 'Mean'),
                median_duration=_get_duraton(stats, 'Median'),
                statistics=stats,
            )
        if not result.success:
            exc = pytest.fail.Exception('\n'.join([result.error_message, result.warning_message]))
            if result.traceback is not None:
                exc = exc.with_traceback(result.traceback)
            raise exc
        if result.warning_message:
            raise Exception(result.warning_message)

    @classmethod
    def setup_class(cls) -> None:
        start_time = time()
        result = YdbCliHelper.WorkloadRunResult()
        result.iterations[0] = YdbCliHelper.Iteration()
        result.add_error(YdbCluster.wait_ydb_alive(int(os.getenv('WAIT_CLUSTER_ALIVE_TIMEOUT', 20 * 60))))
        result.traceback = None
        if not result.error_message and hasattr(cls, 'do_setup_class'):
            try:
                cls.do_setup_class()
            except BaseException as e:
                result.add_error(str(e))
                result.traceback = e.__traceback__
        result.iterations[0].time = time() - start_time
        query_name = '_Verification'
        result.add_stat(query_name, 'Mean', 1000 * result.iterations[0].time)
        nodes_start_time = [n.start_time for n in YdbCluster.get_cluster_nodes(db_only=False)]
        first_node_start_time = min(nodes_start_time) if len(nodes_start_time) > 0 else 0
        result.start_time = max(start_time - 600, first_node_start_time)
        cls.process_query_result(result, query_name, True)

    @classmethod
    def teardown_class(cls) -> None:
        """
        Общий метод очистки для всех тестовых классов.
        Может быть переопределен в наследниках для специфичной очистки.
        """
        with allure.step('Base teardown: checking for custom cleanup'):
            if hasattr(cls, 'do_teardown_class'):
                try:
                    logging.info(f"Executing custom teardown for {cls.__name__}")
                    cls.do_teardown_class()
                    allure.attach(
                        f"Custom teardown completed for {cls.__name__}",
                        'Custom teardown result',
                        allure.attachment_type.TEXT
                    )
                except Exception as e:
                    error_msg = f"Error during custom teardown for {cls.__name__}: {e}"
                    logging.error(error_msg)
                    allure.attach(error_msg, 'Custom teardown error', allure.attachment_type.TEXT)
            else:
                logging.info(f"No custom teardown defined for {cls.__name__}")
                allure.attach(
                    f"No custom teardown needed for {cls.__name__}",
                    'Teardown result',
                    allure.attachment_type.TEXT
                )

    @classmethod
    def kill_workload_processes(cls, process_names: Union[str, list[str]],
                                target_dir: Optional[str] = None) -> None:
        """
        Удобный метод для остановки workload процессов на всех нодах кластера.
        Использует YdbCluster.kill_processes_on_nodes.

        Args:
            process_names: имя процесса или список имен процессов для остановки
            target_dir: директория, в которой искать процессы (опционально)
        """
        with allure.step(f'Killing workload processes: {process_names}'):
            try:
                results = YdbCluster.kill_processes_on_nodes(
                    process_names=process_names,
                    target_dir=target_dir
                )

                total_killed = 0
                for host, host_results in results.items():
                    for process_name, process_result in host_results.items():
                        total_killed += process_result.get('killed_count', 0)

                success_msg = f"Successfully processed {len(results)} hosts, killed {total_killed} processes"
                logging.info(success_msg)
                allure.attach(success_msg, 'Kill processes result', allure.attachment_type.TEXT)

            except Exception as e:
                error_msg = f"Error killing workload processes: {e}"
                logging.error(error_msg)
                allure.attach(error_msg, 'Kill processes error', allure.attachment_type.TEXT)
                raise

    def run_workload_test(self, path: str, query_num: Optional[int] = None, query_name: Optional[str] = None) -> None:
        assert query_num is not None or query_name is not None
        for plugin in plugin_manager.get_plugin_manager().get_plugins():
            if isinstance(plugin, AllureListener):
                allure_test_result = plugin.allure_logging.get_test(None)
                if allure_test_result is not None:
                    for param in allure_test_result.parameters:
                        if param.name in {'query_num', 'query_name'}:
                            param.mode = allure.parameter_mode.HIDDEN.value
        qparams = self._get_query_settings(query_num=query_num, query_name=query_name)
        if query_name is None:
            query_name = f'Query{query_num:02d}'
        self.save_nodes_state()
        result = YdbCliHelper.workload_run(
            path=path,
            query_names=[query_name],
            iterations=qparams.iterations,
            workload_type=self.workload_type,
            timeout=qparams.timeout,
            check_canonical=self.check_canonical,
            query_syntax=self.query_syntax,
            scale=self.scale,
            query_prefix=qparams.query_prefix,
            external_path=self.get_external_path(),
        )[query_name]
        self.process_query_result(result, query_name, True)


class LoadSuiteParallel(LoadSuiteBase):
    threads: int = 0

    def get_query_list() -> list[str]:
        return []

    def get_path() -> str:
        return ''

    __results: dict[str, YdbCliHelper.WorkloadRunResult] = {}

    @classmethod
    def do_setup_class(cls):
        qparams = cls._get_query_settings()
        cls.save_nodes_state()
        cls.__results = YdbCliHelper.workload_run(
            path=cls.get_path(),
            query_names=cls.get_query_list(),
            iterations=qparams.iterations,
            workload_type=cls.workload_type,
            timeout=qparams.timeout,
            check_canonical=cls.check_canonical,
            query_syntax=cls.query_syntax,
            scale=cls.scale,
            query_prefix=qparams.query_prefix,
            external_path=cls.get_external_path(),
            threads=cls.threads
        )

    def test(self, query_name):
        self.process_query_result(result=self.__results[query_name], query_name=query_name, upload=True)


def pytest_generate_tests(metafunc):
    if issubclass(metafunc.cls, LoadSuiteParallel):
        metafunc.parametrize("query_name", metafunc.cls.get_query_list() + ["Sum"])
