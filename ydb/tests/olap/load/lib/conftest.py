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
import ydb.tests.olap.lib.remote_execution as re


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
                folder if folder is not None
                else [t for t in tables.keys()]
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
    def execute_ssh(host: str, cmd: str):
        local = re.is_localhost(host)
        if local:
            ssh_cmd = cmd
        else:
            ssh_cmd = ['ssh', "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]
            ssh_user = os.getenv('SSH_USER')
            if ssh_user is not None:
                ssh_cmd += ['-l', ssh_user]
            ssh_key_file = os.getenv('SSH_KEY_FILE')
            if ssh_key_file is not None:
                ssh_cmd += ['-i', ssh_key_file]
            ssh_cmd += [host, cmd]
        return yatest.common.execute(ssh_cmd, wait=False, text=True, shell=local)

    @classmethod
    def __hide_query_text(cls, text, query_text):
        if os.getenv('SECRET_REQUESTS', '') != '1' or not query_text:
            return text
        return text.replace(query_text, '<Query text hided by sequrity reasons>')

    @classmethod
    def __attach_logs(cls, start_time, attach_name, query_text, ignore_roles=False):
        logging.info("=== __ATTACH_LOGS METHOD CALLED ===")
        logging.info(f"Parameters:")
        logging.info(f"  - start_time: {start_time}")
        logging.info(f"  - attach_name: {attach_name}")
        logging.info(f"  - query_text length: {len(query_text) if query_text else 0}")
        logging.info(f"  - ignore_roles: {ignore_roles}")
        
        if ignore_roles:
            # Получаем уникальные хосты кластера без фильтрации по роли
            logging.info("Getting unique cluster hosts without role filtering...")
            all_nodes = YdbCluster.get_cluster_nodes()
            logging.info(f"  - Total cluster nodes: {len(all_nodes)}")
            
            unique_hosts = set(node.host for node in all_nodes)
            hosts = sorted(unique_hosts)
            logging.info(f"  - Unique hosts found: {len(hosts)}")
            logging.info(f"  - Hosts: {hosts}")
        else:
            # Оригинальная логика - только STORAGE ноды
            logging.info("Getting STORAGE nodes only (original logic)...")
            all_nodes = YdbCluster.get_cluster_nodes()
            storage_nodes = list(filter(lambda x: x.role == YdbCluster.Node.Role.STORAGE, all_nodes))
            hosts = [node.host for node in storage_nodes]
            logging.info(f"  - Total cluster nodes: {len(all_nodes)}")
            logging.info(f"  - STORAGE nodes found: {len(storage_nodes)}")
            logging.info(f"  - STORAGE hosts: {hosts}")

        if not hosts:
            logging.warning("No hosts found for log collection!")
            allure.attach(
                "No cluster hosts found, no kikimr logs collected.",
                f"{attach_name} logs info",
                allure.attachment_type.TEXT
            )
            logging.info("=== __ATTACH_LOGS COMPLETED (NO HOSTS) ===")
            return

        logging.info(f"Starting log collection from {len(hosts)} hosts...")

        tz = timezone('Europe/Moscow')
        start = datetime.fromtimestamp(start_time, tz).isoformat()
        cmd = f"ulimit -n 100500;unified_agent select -S '{start}' -s {{storage}}{{container}}"
        
        logging.info(f"Log collection setup:")
        logging.info(f"  - Timezone: {tz}")
        logging.info(f"  - Start time formatted: {start}")
        logging.info(f"  - Command template: {cmd}")
        
        exec_kikimr = {
            '': {},
        }
        exec_start = deepcopy(exec_kikimr)
        
        logging.info("Executing log collection commands on all hosts...")
        
        for host in hosts:
            logging.info(f"  Processing host: {host}")
            
            for c in exec_kikimr.keys():
                try:
                    kikimr_cmd = cmd.format(
                        storage='kikimr',
                        container=f' -m k8s_container:{c}' if c else ''
                    )
                    logging.info(f"    - Executing kikimr command: {kikimr_cmd}")
                    exec_kikimr[c][host] = cls.execute_ssh(host, kikimr_cmd)
                    logging.info(f"    - Kikimr command started for {host}")
                except BaseException as e:
                    logging.error(f"    - Error executing kikimr command on {host}: {e}")
                    
            for c in exec_start.keys():
                try:
                    start_cmd = cmd.format(
                        storage='kikimr-start',
                        container=f' -m k8s_container:{c}' if c else ''
                    )
                    logging.info(f"    - Executing start command: {start_cmd}")
                    exec_start[c][host] = cls.execute_ssh(host, start_cmd)
                    logging.info(f"    - Start command started for {host}")
                except BaseException as e:
                    logging.error(f"    - Error executing start command on {host}: {e}")

        logging.info("Processing start command results...")
        error_log = ''
        for c, execs in exec_start.items():
            logging.info(f"  Processing container '{c}' start results...")
            for host, e in sorted(execs.items()):
                logging.info(f"    - Waiting for start command on {host}...")
                e.wait(check_exit_code=False)
                
                if e.returncode == 0:
                    logging.info(f"    - Start command on {host} completed successfully")
                    logging.info(f"    - Stdout length: {len(e.stdout) if e.stdout else 0}")
                    error_log += f'{host}:\n{e.stdout}\n'
                else:
                    logging.warning(f"    - Start command on {host} failed with code {e.returncode}")
                    logging.warning(f"    - Stderr: {e.stderr[:200]}..." if e.stderr else "    - No stderr")
                    error_log += f'{host}:\n{e.stderr}\n'
                    
            logging.info(f"  - Attaching start results for container '{c}'")
            allure.attach(cls.__hide_query_text(error_log, query_text), f'{attach_name}_{c}_stderr', allure.attachment_type.TEXT)

        logging.info("Processing kikimr command results...")
        for c, execs in exec_kikimr.items():
            logging.info(f"  Processing container '{c}' kikimr results...")
            
            dir = os.path.join(yatest.common.tempfile.gettempdir(), f'{attach_name}_{c}_logs')
            logging.info(f"  - Creating directory: {dir}")
            os.makedirs(dir, exist_ok=True)
            
            for host, e in execs.items():
                logging.info(f"    - Waiting for kikimr command on {host}...")
                e.wait(check_exit_code=False)
                
                file_path = os.path.join(dir, host)
                logging.info(f"    - Writing results to: {file_path}")
                
                with open(file_path, 'w') as f:
                    if e.returncode == 0:
                        logging.info(f"    - Kikimr command on {host} completed successfully")
                        logging.info(f"    - Stdout length: {len(e.stdout) if e.stdout else 0}")
                        content = cls.__hide_query_text(e.stdout, query_text)
                    else:
                        logging.warning(f"    - Kikimr command on {host} failed with code {e.returncode}")
                        logging.warning(f"    - Stderr: {e.stderr[:200]}..." if e.stderr else "    - No stderr")
                        content = cls.__hide_query_text(e.stderr, query_text)
                    
                    f.write(content)
                    logging.info(f"    - Written {len(content)} characters to {file_path}")
                    
            archive = dir + '.tar.gz'
            logging.info(f"  - Creating archive: {archive}")
            
            try:
                yatest.common.execute(['tar', '-C', dir, '-czf', archive, '.'])
                logging.info(f"  - Archive created successfully")
                
                allure.attach.file(archive, f'{attach_name}_{c}_logs', extension='tar.gz')
                logging.info(f"  - Archive attached to Allure report")
                
            except Exception as e:
                logging.error(f"  - Error creating archive: {e}")

        logging.info("=== __ATTACH_LOGS COMPLETED SUCCESSFULLY ===")

    @classmethod
    def save_nodes_state(cls) -> None:
        cls.__nodes_state = {n.slot: n for n in YdbCluster.get_cluster_nodes(db_only=True)}

    @classmethod
    def __get_core_hashes_by_pod(cls, hosts: set[str], start_time: float, end_time: float) -> dict[str, list[tuple[str, str]]]:
        core_processes = {
            h: cls.execute_ssh(h, 'sudo flock /tmp/brk_pad /Berkanavt/breakpad/bin/kikimr_breakpad_analizer.sh')
            for h in hosts
        }

        core_hashes = {}
        for h, exec in core_processes.items():
            exec.wait(check_exit_code=False)
            if exec.returncode != 0:
                logging.error(f'Error while process coredumps on host {h}: {exec.stderr}')
            exec = cls.execute_ssh(h, ('find /coredumps/ -name "sended_*.json" '
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
            exec = cls.execute_ssh(h, oom_cmd)
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
            start_time=result.start_time, end_time=end_time, node_errors=cls.check_nodes(result, end_time),
            workload_result=result, workload_params=None
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
        cls._setup_start_time = time()
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
        result.iterations[0].time = time() - cls._setup_start_time
        query_name = '_Verification'
        result.add_stat(query_name, 'Mean', 1000 * result.iterations[0].time)
        nodes_start_time = [n.start_time for n in YdbCluster.get_cluster_nodes(db_only=False)]
        first_node_start_time = min(nodes_start_time) if len(nodes_start_time) > 0 else 0
        result.start_time = max(cls._setup_start_time - 600, first_node_start_time)
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
                allure_test_result = plugin.allure_logger.get_test(None)
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

    @classmethod
    def check_nodes_diagnostics(cls, result: YdbCliHelper.WorkloadRunResult, end_time: float) -> list[NodeErrors]:
        """
        Собирает диагностическую информацию о нодах без проверки перезапусков/падений.
        Проверяет coredump'ы и OOM для всех нод из сохраненного состояния.
        """
        return cls.check_nodes_diagnostics_with_timing(result, result.start_time, end_time)

    @classmethod
    def check_nodes_diagnostics_with_timing(cls, result: YdbCliHelper.WorkloadRunResult, start_time: float, end_time: float) -> list[NodeErrors]:
        """
        Собирает диагностическую информацию о нодах с кастомным временным интервалом.
        Проверяет coredump'ы и OOM для всех нод из сохраненного состояния.

        Args:
            result: результат выполнения workload
            start_time: время начала интервала для диагностики
            end_time: время окончания интервала для диагностики
        """
        if cls.__nodes_state is None:
            return []

        # Получаем все хосты из сохраненного состояния
        all_hosts = {node.host for node in cls.__nodes_state.values()}

        # Собираем диагностическую информацию для всех хостов
        core_hashes = cls.__get_core_hashes_by_pod(all_hosts, start_time, end_time)
        ooms = cls.__get_hosts_with_omms(all_hosts, start_time, end_time)

        # Создаем NodeErrors для каждой ноды с диагностической информацией
        node_errors = []
        for node in cls.__nodes_state.values():
            # Создаем NodeErrors только если есть coredump'ы или OOM
            has_cores = bool(core_hashes.get(node.slot, []))
            has_oom = node.host in ooms

            if has_cores or has_oom:
                node_error = NodeErrors(node, 'diagnostic info collected')
                node_error.core_hashes = core_hashes.get(node.slot, [])
                node_error.was_oom = has_oom
                node_errors.append(node_error)

                # Добавляем ошибки в результат (cores и OOM - это errors)
                if has_cores:
                    result.add_error(f'Node {node.slot} has {len(node_error.core_hashes)} coredump(s)')
                if has_oom:
                    result.add_error(f'Node {node.slot} experienced OOM')

        cls.__nodes_state = None
        return node_errors

    def _collect_workload_params(self, result, workload_name):
        """
        Собирает параметры workload для отчёта.
        1. Сначала добавляет ключевые параметры из params_to_include (гарантированно попадут в отчёт).
        2. Затем добавляет все остальные параметры из статистики workload, кроме исключённых (служебных), чтобы не потерять новые или специфичные метрики.
        """
        workload_params = {}
        workload_stats = result.get_stats(workload_name)
        if workload_stats:
            # 1. Добавляем только самые важные параметры (гарантированно попадут в отчёт)
            params_to_include = [
                'total_runs', 'planned_duration', 'actual_duration',
                'use_iterations', 'workload_type', 'table_type',
                'total_iterations', 'total_threads'
            ]
            for param in params_to_include:
                if param in workload_stats:
                    workload_params[param] = workload_stats[param]
            # 2. Добавляем все остальные параметры, кроме исключённых служебных
            # Это позволяет автоматически включать новые/дополнительные метрики
            for key, value in workload_stats.items():
                if key not in workload_params and key not in ['success_rate', 'successful_runs', 'failed_runs']:
                    workload_params[key] = value
        return workload_params

    def _diagnose_nodes(self, result, workload_name):
        """Проводит диагностику нод (cores/oom) и возвращает список ошибок"""
        logging.info("=== DIAGNOSE NODES ===")
        
        with allure.step("Diagnose cluster nodes") as main_step:
            main_step.name = "Diagnose cluster nodes"
            
            try:
                end_time = time()
                diagnostics_start_time = getattr(result, 'workload_start_time', result.start_time)
                
                logging.info(f"Diagnostics parameters:")
                logging.info(f"  - diagnostics_start_time: {diagnostics_start_time}")
                logging.info(f"  - end_time: {end_time}")
                logging.info(f"  - workload_name: {workload_name}")
                
                with allure.step("Check nodes for cores/OOM") as check_step:
                    check_step.name = "Check nodes for cores/OOM"
                    
                    logging.info("Calling check_nodes_diagnostics_with_timing...")
                    node_errors = type(self).check_nodes_diagnostics_with_timing(result, diagnostics_start_time, end_time)
                    
                    logging.info(f"Node diagnostics completed:")
                    logging.info(f"  - Found {len(node_errors)} nodes with issues")
                    
                    if node_errors:
                        error_summary = []
                        for error in node_errors:
                            error_summary.append(f"  - {error.host}: {error.error_type}")
                            logging.info(f"  - {error.host}: {error.error_type}")
                        
                        check_step.name = f"❌ Found issues on {len(node_errors)} nodes"
                        
                        allure.attach(
                            f"Nodes with issues: {len(node_errors)}\n" + "\n".join(error_summary),
                            "Node Issues Found",
                            allure.attachment_type.TEXT
                        )
                    else:
                        check_step.name = f"✅ No node issues found"
                        logging.info("  ✓ No node issues found")
                        
                        allure.attach(
                            "No cores or OOM detected on any nodes",
                            "Node Diagnostics Clean",
                            allure.attachment_type.TEXT
                        )

                # Обновляем статистику
                with allure.step("Update node statistics") as stats_step:
                    stats_step.name = "Update node statistics"
                    
                    result.add_stat(workload_name, "nodes_with_issues", len(node_errors))
                    logging.info(f"  - Updated nodes_with_issues = {len(node_errors)}")
                    
                    stats_step.name = f"✅ Statistics updated (nodes_with_issues: {len(node_errors)})"
                    
                    allure.attach(
                        f"nodes_with_issues statistic set to: {len(node_errors)}",
                        "Statistics Update",
                        allure.attachment_type.TEXT
                    )

                main_step.name = f"{'❌' if node_errors else '✅'} Node diagnostics: {len(node_errors)} issues found"
                
                logging.info("=== DIAGNOSE NODES COMPLETED ===")
                return node_errors
                
            except Exception as e:
                logging.error(f"Error during node diagnostics: {e}")
                import traceback
                traceback_str = traceback.format_exc()
                logging.error(f"Traceback: {traceback_str}")
                
                main_step.name = f"❌ Node diagnostics failed"
                
                allure.attach(
                    f"Node diagnostics failed: {e}\n\nTraceback:\n{traceback_str}",
                    "Node Diagnostics Error",
                    allure.attachment_type.TEXT
                )
                
                return []

    def _update_summary_flags(self, result, workload_name):
        """Обновляет флаги summary (with_warnings, with_errors)"""
        logging.info("=== UPDATE SUMMARY FLAGS ===")
        
        with allure.step("Update summary flags") as main_step:
            main_step.name = "Update summary flags"
            
            has_warnings = bool(result.warnings)
            has_errors = bool(result.errors)
            
            logging.info(f"Summary flags:")
            logging.info(f"  - has_warnings: {has_warnings} (count: {len(result.warnings) if result.warnings else 0})")
            logging.info(f"  - has_errors: {has_errors} (count: {len(result.errors) if result.errors else 0})")
            
            with allure.step("Set warning flags") as warn_step:
                # Устанавливаем флаги предупреждений (оба варианта для совместимости)
                result.add_stat(workload_name, "with_warnings", has_warnings)
                result.add_stat(workload_name, "with_warrnings", has_warnings)  # Для обратной совместимости
                result.add_stat(workload_name, "workload_warnings", has_warnings)
                
                logging.info(f"  - Set with_warnings = {has_warnings}")
                logging.info(f"  - Set with_warrnings = {has_warnings} (legacy)")
                logging.info(f"  - Set workload_warnings = {has_warnings}")
                
                warn_step.name = f"{'⚠️' if has_warnings else '✅'} Warnings: {has_warnings}"

            with allure.step("Set error flags") as error_step:
                # Устанавливаем флаги ошибок
                result.add_stat(workload_name, "with_errors", has_errors)
                result.add_stat(workload_name, "workload_errors", has_errors)
                
                logging.info(f"  - Set with_errors = {has_errors}")
                logging.info(f"  - Set workload_errors = {has_errors}")
                
                error_step.name = f"{'❌' if has_errors else '✅'} Errors: {has_errors}"

            allure.attach(
                f"Summary flags updated:\n"
                f"with_warnings: {has_warnings}\n"
                f"with_errors: {has_errors}\n"
                f"warnings count: {len(result.warnings) if result.warnings else 0}\n"
                f"errors count: {len(result.errors) if result.errors else 0}",
                "Summary Flags",
                allure.attachment_type.TEXT
            )
            
            main_step.name = f"✅ Summary flags updated (W:{has_warnings}, E:{has_errors})"
            
        logging.info("=== UPDATE SUMMARY FLAGS COMPLETED ===")

    def _create_allure_report(self, result, workload_name, workload_params, node_errors, use_node_subcols):
        """Формирует allure-отчёт по результатам workload"""
        logging.info("=== CREATE ALLURE REPORT ===")
        
        with allure.step("Create Allure report") as main_step:
            main_step.name = "Create Allure report"
            
            end_time = time()
            start_time = result.start_time if result.start_time else end_time - 1
            
            logging.info(f"Report parameters:")
            logging.info(f"  - workload_name: {workload_name}")
            logging.info(f"  - start_time: {start_time}")
            logging.info(f"  - end_time: {end_time}")
            logging.info(f"  - node_errors count: {len(node_errors)}")
            logging.info(f"  - use_node_subcols: {use_node_subcols}")
            logging.info(f"  - workload_params keys: {list(workload_params.keys()) if workload_params else 'None'}")
            
            with allure.step("Prepare report data") as prep_step:
                prep_step.name = "Prepare report data"
                
                # Добавляем информацию о времени выполнения
                additional_table_strings = {}
                
                if workload_params and workload_params.get("actual_duration") is not None:
                    actual_duration = workload_params["actual_duration"]
                    planned_duration = workload_params.get("planned_duration", 1800)
                    
                    actual_minutes = int(actual_duration) // 60
                    actual_seconds = int(actual_duration) % 60
                    planned_minutes = int(planned_duration) // 60
                    planned_seconds = int(planned_duration) % 60
                    
                    additional_table_strings["execution_time"] = (
                        f"Actual: {actual_minutes}m {actual_seconds}s (Planned: {planned_minutes}m {planned_seconds}s)"
                    )
                    logging.info(f"  - Execution time: {additional_table_strings['execution_time']}")

                # Информация об итерациях и потоках
                if workload_params and "total_iterations" in workload_params and "total_threads" in workload_params:
                    total_iterations = workload_params["total_iterations"]
                    total_threads = workload_params["total_threads"]
                    
                    if total_iterations == 1 and total_threads > 1:
                        additional_table_strings["execution_mode"] = (
                            f"Single iteration with {total_threads} parallel threads"
                        )
                    elif total_iterations > 1:
                        avg_threads = workload_params.get("avg_threads_per_iteration", 1)
                        additional_table_strings["execution_mode"] = (
                            f"{total_iterations} iterations with avg {avg_threads:.1f} threads per iteration"
                        )
                    
                    logging.info(f"  - Execution mode: {additional_table_strings.get('execution_mode', 'Standard')}")

                prep_step.name = f"✅ Report data prepared ({len(additional_table_strings)} extra fields)"

            with allure.step("Generate Allure description") as gen_step:
                gen_step.name = "Generate Allure description"
                
                try:
                    from ydb.tests.olap.lib.allure_utils import allure_test_description
                    
                    logging.info("  - Calling allure_test_description...")
                    allure_test_description(
                        suite="workload",
                        test=workload_name,
                        start_time=start_time,
                        end_time=end_time,
                        addition_table_strings=additional_table_strings,
                        node_errors=node_errors,
                        workload_result=result,
                        workload_params=workload_params,
                        use_node_subcols=use_node_subcols,
                    )
                    
                    logging.info("  ✓ Allure description generated successfully")
                    gen_step.name = f"✅ Allure description generated"
                    
                    allure.attach(
                        f"Allure description generated successfully\n"
                        f"Suite: workload\n"
                        f"Test: {workload_name}\n"
                        f"Node errors: {len(node_errors)}\n"
                        f"Use subcols: {use_node_subcols}",
                        "Allure Generation Result",
                        allure.attachment_type.TEXT
                    )
                    
                except Exception as e:
                    logging.error(f"  ✗ Error generating Allure description: {e}")
                    import traceback
                    traceback_str = traceback.format_exc()
                    logging.error(f"  ✗ Traceback: {traceback_str}")
                    
                    gen_step.name = f"❌ Allure description failed"
                    
                    allure.attach(
                        f"Allure description generation failed: {e}\n\nTraceback:\n{traceback_str}",
                        "Allure Generation Error",
                        allure.attachment_type.TEXT
                    )

            main_step.name = f"✅ Allure report created for {workload_name}"
            
        logging.info("=== CREATE ALLURE REPORT COMPLETED ===")

    def _handle_final_status(self, result, workload_name, node_errors):
        """Обрабатывает финальный статус теста: fail, broken, etc."""
        from ydb.tests.olap.lib.utils import get_external_param
        
        logging.info(f"=== HANDLE FINAL STATUS FOR {workload_name} ===")
        
        with allure.step(f"Handle final status for {workload_name}") as main_step:
            main_step.name = f"Handle final status for {workload_name}"
            
            stats = result.get_stats(workload_name)
            node_issues = stats.get("nodes_with_issues", 0) if stats else 0
            workload_errors = []
            if result.errors:
                for err in result.errors:
                    if "coredump" not in err.lower() and "oom" not in err.lower():
                        workload_errors.append(err)

            logging.info(f"Status analysis:")
            logging.info(f"  - node_issues: {node_issues}")
            logging.info(f"  - total errors: {len(result.errors) if result.errors else 0}")
            logging.info(f"  - workload_errors: {len(workload_errors)}")
            logging.info(f"  - total warnings: {len(result.warnings) if result.warnings else 0}")

            # Определяем статус теста по приоритету
            test_status = "PASS"
            test_reason = "No issues detected"

            with allure.step("Determine test status") as status_step:
                status_step.name = "Determine test status"
                
                if node_issues > 0:
                    test_status = "FAIL"
                    test_reason = f"Cores/OOM detected on {node_issues} nodes"
                    logging.info(f"  → FAIL: {test_reason}")
                    status_step.name = f"❌ FAIL: {test_reason}"
                elif workload_errors:
                    test_status = "BROKEN"
                    test_reason = f"{len(workload_errors)} workload errors detected"
                    logging.info(f"  → BROKEN: {test_reason}")
                    status_step.name = f"🔴 BROKEN: {test_reason}"
                elif result.warnings:
                    test_status = "WARNING"
                    test_reason = f"{len(result.warnings)} warnings detected"
                    logging.info(f"  → WARNING: {test_reason}")
                    status_step.name = f"⚠️ WARNING: {test_reason}"
                else:
                    logging.info(f"  → PASS: {test_reason}")
                    status_step.name = f"✅ PASS: {test_reason}"

                allure.attach(
                    f"Test Status: {test_status}\n"
                    f"Reason: {test_reason}\n"
                    f"Node issues: {node_issues}\n"
                    f"Workload errors: {len(workload_errors)}\n"
                    f"Warnings: {len(result.warnings) if result.warnings else 0}",
                    "Status Decision",
                    allure.attachment_type.TEXT
                )

            # Логика attach_logs
            with allure.step("Decide on log attachment") as attach_step:
                attach_step.name = "Decide on log attachment"
                
                cluster_log = get_external_param("cluster_log", "")
                should_attach = False
                attach_reason = ""

                logging.info(f"Attach logs decision:")
                logging.info(f"  - cluster_log parameter: '{cluster_log}'")
                
                with allure.step("Check cluster_log parameter") as param_step:
                    if cluster_log == "all":
                        should_attach = True
                        attach_reason = "cluster_log=all (always attach)"
                        logging.info(f"  → {attach_reason}")
                        param_step.name = f"✅ cluster_log=all → Always attach"
                    elif cluster_log == "never":
                        should_attach = False
                        attach_reason = "cluster_log=never (never attach)"
                        logging.info(f"  → {attach_reason}")
                        param_step.name = f"❌ cluster_log=never → Never attach"
                    else:
                        # Автоматическое решение на основе проблем
                        if node_issues > 0 or workload_errors or result.warnings:
                            should_attach = True
                            attach_reason = f"Problems detected: nodes={node_issues}, errors={len(workload_errors)}, warnings={len(result.warnings) if result.warnings else 0}"
                            logging.info(f"  → {attach_reason}")
                            param_step.name = f"🔍 Auto-detect → Problems found → Attach"
                        else:
                            should_attach = False
                            attach_reason = "No problems detected (clean run)"
                            logging.info(f"  → {attach_reason}")
                            param_step.name = f"✅ Auto-detect → Clean run → No attach"

                allure.attach(
                    f"Should attach logs: {should_attach}\n"
                    f"Reason: {attach_reason}\n"
                    f"cluster_log param: '{cluster_log}'",
                    "Attach Decision",
                    allure.attachment_type.TEXT
                )

                # Выполняем attach_logs если нужно
                if should_attach:
                    with allure.step("Attach cluster logs") as logs_step:
                        logs_step.name = "Attach cluster logs"
                        
                        logging.info(f"Attaching logs: {attach_reason}")
                        
                        # Проверяем наличие метода __attach_logs
                        attach_logs_method = getattr(self, "__attach_logs", None)
                        logging.info(f"  - __attach_logs method available: {attach_logs_method is not None}")
                        
                        if attach_logs_method:
                            try:
                                # Получаем время начала workload или используем время начала результата
                                start_time = getattr(result, 'workload_start_time', result.start_time)
                                query_text = getattr(result, 'stdout', '') or ''
                                
                                logging.info(f"  - Calling __attach_logs with start_time={start_time}")
                                logging.info(f"  - Query text length: {len(query_text)}")
                                
                                # Вызываем метод прикладывания логов
                                attach_logs_method(
                                    start_time=start_time,
                                    attach_name=f"{workload_name}_logs",
                                    query_text=query_text,
                                    ignore_roles=False
                                )
                                
                                logging.info("  ✓ Logs attached successfully")
                                logs_step.name = f"✅ Logs attached ({attach_reason})"
                                
                                allure.attach(
                                    f"Logs attached successfully\n"
                                    f"Start time: {start_time}\n"
                                    f"Query text length: {len(query_text)}\n"
                                    f"Reason: {attach_reason}",
                                    "Logs Attachment Result",
                                    allure.attachment_type.TEXT
                                )
                                
                            except Exception as e:
                                logging.error(f"  ✗ Error attaching logs: {e}")
                                import traceback
                                traceback_str = traceback.format_exc()
                                logging.error(f"  ✗ Traceback: {traceback_str}")
                                
                                logs_step.name = f"❌ Log attachment failed"
                                
                                allure.attach(
                                    f"Log attachment failed: {e}\n\nTraceback:\n{traceback_str}",
                                    "Logs Attachment Error",
                                    allure.attachment_type.TEXT
                                )
                        else:
                            logging.warning("  ⚠️ __attach_logs method not available")
                            logs_step.name = f"⚠️ __attach_logs method not available"
                            
                            allure.attach(
                                "__attach_logs method not available - logs cannot be attached",
                                "Logs Attachment Warning",
                                allure.attachment_type.TEXT
                            )
                else:
                    with allure.step("Skip log attachment") as skip_step:
                        skip_step.name = f"Skip log attachment ({attach_reason})"
                        logging.info(f"Skipping log attachment: {attach_reason}")
                        
                        allure.attach(
                            f"Log attachment skipped\n"
                            f"Reason: {attach_reason}",
                            "Logs Attachment Skipped",
                            allure.attachment_type.TEXT
                        )

                attach_step.name = f"{'✅' if should_attach else '❌'} Logs: {attach_reason}"

            # Применяем финальный статус
            with allure.step("Apply final test status") as final_step:
                final_step.name = "Apply final test status"
                
                if test_status == "FAIL":
                    result.success = False
                    if workload_errors:
                        for error in workload_errors:
                            result.add_error(error)
                    logging.info(f"  → Test marked as FAILED")
                    final_step.name = f"❌ Test marked as FAILED"
                    
                    # Вызываем pytest.fail для немедленного завершения
                    import pytest
                    pytest.fail(f"Test failed: {test_reason}")
                    
                elif test_status == "BROKEN":
                    result.success = False
                    for error in workload_errors:
                        result.add_error(error)
                    logging.info(f"  → Test marked as BROKEN")
                    final_step.name = f"🔴 Test marked as BROKEN"
                    
                    # Вызываем pytest.fail для немедленного завершения
                    import pytest
                    pytest.fail(f"Test broken: {test_reason}")
                    
                elif test_status == "WARNING":
                    # WARNING не делает тест неуспешным, но добавляет предупреждения
                    logging.info(f"  → Test has warnings but continues")
                    final_step.name = f"⚠️ Test has warnings but continues"
                else:
                    logging.info(f"  → Test passed successfully")
                    final_step.name = f"✅ Test passed successfully"

                allure.attach(
                    f"Final test status applied: {test_status}\n"
                    f"Result.success: {result.success}\n"
                    f"Reason: {test_reason}",
                    "Final Status Applied",
                    allure.attachment_type.TEXT
                )

            main_step.name = f"{'✅' if test_status == 'PASS' else '❌' if test_status in ['FAIL', 'BROKEN'] else '⚠️'} {test_status}: {test_reason}"

        logging.info(f"=== HANDLE FINAL STATUS COMPLETED ===")
        logging.info(f"Final result: {test_status} - {test_reason}")

    def _upload_results(self, result, workload_name):
        stats = result.get_stats(workload_name)
        if stats is not None:
            stats["aggregation_level"] = "aggregate"
            stats["run_id"] = ResultsProcessor.get_run_id()
        end_time = time()
        ResultsProcessor.upload_results(
            kind='Load',
            suite=type(self).suite(),
            test=workload_name,
            timestamp=end_time,
            is_successful=result.success,
            statistics=stats,
        )

    def _upload_results_per_workload_run(self, result, workload_name):
        suite = type(self).suite()
        agg_stats = result.get_stats(workload_name)
        nemesis_enabled = agg_stats.get("nemesis_enabled") if agg_stats else None
        run_id = ResultsProcessor.get_run_id()
        for iter_num, iteration in result.iterations.items():
            runs = getattr(iteration, "runs", None) or [iteration]
            for run_idx, run in enumerate(runs):
                if getattr(run, "error_message", None):
                    resolution = "error"
                elif getattr(run, "warning_message", None):
                    resolution = "warning"
                elif hasattr(run, "timeout") and run.timeout:
                    resolution = "timeout"
                else:
                    resolution = "ok"

                stats = {
                    "iteration": iter_num,
                    "run_index": run_idx,
                    "duration": getattr(run, "time", None),
                    "resolution": resolution,
                    "error_message": getattr(run, "error_message", None),
                    "warning_message": getattr(run, "warning_message", None),
                    "nemesis_enabled": nemesis_enabled,
                    "aggregation_level": "per_run",
                    "run_id": run_id,
                }
                ResultsProcessor.upload_results(
                    kind='Stress',
                    suite=suite,
                    test=f"{workload_name}__iter_{iter_num}__run_{run_idx}",
                    timestamp=time(),
                    is_successful=(resolution == "ok"),
                    duration=stats["duration"],
                    statistics=stats,
                )

    def process_workload_result_with_diagnostics(self, result, workload_name, check_scheme=True, use_node_subcols=False):
        """
        Обрабатывает результат workload с добавлением диагностической информации
        """
        # 1. Сбор параметров workload
        workload_params = self._collect_workload_params(result, workload_name)

        # 2. Диагностика нод (cores/oom)
        node_errors = self._diagnose_nodes(result, workload_name)

        # --- ВАЖНО: выставляем nodes_with_issues для корректного fail ---
        stats = result.get_stats(workload_name)
        if stats is not None:
            result.add_stat(workload_name, "nodes_with_issues", len(node_errors))

        # 3. Формирование summary/статистики
        self._update_summary_flags(result, workload_name)

        # 4. Формирование allure-отчёта
        self._create_allure_report(result, workload_name, workload_params, node_errors, use_node_subcols)

        # 5. Обработка ошибок/статусов (fail, broken, etc)
        self._handle_final_status(result, workload_name, node_errors)

        # 6. Загрузка агрегированных результатов
        self._upload_results(result, workload_name)
        # 7. Загрузка результатов по каждому запуску workload
        self._upload_results_per_workload_run(result, workload_name)


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
