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
    float_mode: str = ''
    __nodes_state: Optional[dict[str, YdbCluster.Node]] = None

    @classmethod
    def get_users(cls) -> list[str]:
        return ['']

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
        if ignore_roles:
            # Получаем уникальные хосты кластера без фильтрации по роли
            hosts = sorted(set(node.host for node in YdbCluster.get_cluster_nodes()))
        else:
            # Оригинальная логика - только STORAGE ноды
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
                    exec_kikimr[c][host] = cls.execute_ssh(host, cmd.format(
                        storage='kikimr',
                        container=f' -m k8s_container:{c}' if c else ''
                    ))
                except BaseException as e:
                    logging.error(e)
            for c in exec_start.keys():
                try:
                    exec_start[c][host] = cls.execute_ssh(host, cmd.format(
                        storage='kikimr-start',
                        container=f' -m k8s_container:{c}' if c else ''))
                except BaseException as e:
                    logging.error(e)

        if not hosts:
            allure.attach(
                "No cluster hosts found, no kikimr logs collected.",
                f"{attach_name} logs info",
                allure.attachment_type.TEXT
            )
            return

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
        """
        Проверяет состояние нод кластера после выполнения workload
        
        Сравнивает текущее состояние с состоянием до запуска workload и nemesis
        """
        if cls.__nodes_state is None:
            logging.warning("No initial nodes state available for comparison")
            return []
        
        node_errors = []
        fail_hosts = set()
        
        # Получаем текущие ноды кластера
        current_nodes = YdbCluster.get_cluster_nodes(db_only=True)
        current_nodes_by_slot = {node.slot: node for node in current_nodes}
        
        # Проверяем каждую сохраненную ноду
        for slot, saved_node in cls.__nodes_state.items():
            current_node = current_nodes_by_slot.get(slot)
            
            if current_node is None:
                # Нода не найдена - она down
                node_errors.append(NodeErrors(saved_node, 'is down'))
                fail_hosts.add(saved_node.host)
                logging.warning(f"Node {slot} is down (not found in current cluster)")
            elif current_node.start_time > saved_node.start_time:
                # Нода была перезапущена
                node_errors.append(NodeErrors(current_node, 'was restarted'))
                fail_hosts.add(current_node.host)
                logging.info(f"Node {slot} was restarted (start_time: {saved_node.start_time} -> {current_node.start_time})")
            else:
                # Нода в порядке
                logging.debug(f"Node {slot} is healthy")
        
        # Очищаем состояние
        cls.__nodes_state = None
        
        if len(node_errors) == 0:
            logging.info("All nodes are healthy")
            return []

        # Получаем cores и OOM для проблемных хостов
        logging.info(f"Checking cores and OOM for {len(fail_hosts)} problematic hosts")
        core_hashes = cls.__get_core_hashes_by_pod(fail_hosts, result.start_time, end_time)
        ooms = cls.__get_hosts_with_omms(fail_hosts, result.start_time, end_time)
        
        # Обогащаем ошибки информацией о cores и OOM
        for node_error in node_errors:
            node_error.core_hashes = core_hashes.get(f'{node_error.node.slot}', [])
            node_error.was_oom = node_error.node.host in ooms
            
            # Логируем детали проблемы
            if node_error.core_hashes:
                logging.error(f"Node {node_error.node.slot} has {len(node_error.core_hashes)} cores")
            if node_error.was_oom:
                logging.error(f"Node {node_error.node.slot} had OOM")
        
        # Добавляем ошибки в результат
        for err in node_errors:
            result.add_error(f'Node {err.node.slot} {err.message}')
        
        logging.info(f"Found {len(node_errors)} node issues: {[f'{err.node.slot}:{err.message}' for err in node_errors]}")
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
            users=self.get_users(),
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
        try:
            end_time = time()
            diagnostics_start_time = getattr(result, 'workload_start_time', result.start_time)
            node_errors = type(self).check_nodes_diagnostics_with_timing(result, diagnostics_start_time, end_time)
        except Exception as e:
            logging.error(f"Error getting nodes state: {e}")
            result.add_warning(f"Error getting nodes state: {e}")
            node_errors = []
        return node_errors

    def _update_summary_flags(self, result, workload_name):
        """Обновляет summary-флаги для warning/error по всем итерациям"""
        has_warning = False
        has_error = False

        # Проверяем ошибки и предупреждения в итерациях
        for iteration in getattr(result, "iterations", {}).values():
            if hasattr(iteration, "warning_message") and iteration.warning_message:
                has_warning = True
            if hasattr(iteration, "error_message") and iteration.error_message:
                has_error = True

        # Проверяем ошибки и предупреждения в основном результате
        if result.warnings:
            has_warning = True
        if result.errors:
            has_error = True

        # Для обратной совместимости также проверяем старые поля
        if hasattr(result, "warning_message") and result.warning_message:
            has_warning = True
        if hasattr(result, "error_message") and result.error_message:
            has_error = True

        stats = result.get_stats(workload_name)
        if stats is not None:
            stats["with_warnings"] = has_warning
            stats["with_errors"] = has_error

    def _create_allure_report(self, result, workload_name, workload_params, node_errors, use_node_subcols):
        """Формирует allure-отчёт по результатам workload"""
        end_time = time()
        start_time = result.start_time if result.start_time else end_time - 1
        additional_table_strings = {}
        if workload_params.get('actual_duration') is not None:
            actual_duration = workload_params['actual_duration']
            planned_duration = workload_params.get('planned_duration', getattr(self, 'timeout', 0))
            actual_minutes = int(actual_duration) // 60
            actual_seconds = int(actual_duration) % 60
            planned_minutes = int(planned_duration) // 60
            planned_seconds = int(planned_duration) % 60
            additional_table_strings['execution_time'] = f"Actual: {actual_minutes}m {actual_seconds}s (Planned: {planned_minutes}m {planned_seconds}s)"
        if 'total_iterations' in workload_params and 'total_threads' in workload_params:
            total_iterations = workload_params['total_iterations']
            total_threads = workload_params['total_threads']
            if total_iterations == 1 and total_threads > 1:
                additional_table_strings['execution_mode'] = f"Single iteration with {total_threads} parallel threads"
            elif total_iterations > 1:
                avg_threads = workload_params.get('avg_threads_per_iteration', 1)
                additional_table_strings['execution_mode'] = f"{total_iterations} iterations with avg {avg_threads:.1f} threads per iteration"
        allure_test_description(
            suite=type(self).suite(),
            test=workload_name,
            start_time=start_time,
            end_time=end_time,
            addition_table_strings=additional_table_strings,
            node_errors=node_errors,
            workload_result=result,
            workload_params=workload_params,
            use_node_subcols=use_node_subcols
        )

    def _handle_final_status(self, result, workload_name, node_errors):
        """
        Обрабатывает финальный статус теста по простой логике:
        
        1. 🔴 FAIL: критические проблемы с нодами (OOM, cores)
        2. 🔴 FAIL: все итерации не выполнились успешно
        3. 🟡 WARNING: есть timeout, но не критично
        4. 🟢 PASS: все хорошо
        """
        stats = result.get_stats(workload_name)
        node_issues = stats.get("nodes_with_issues", 0) if stats else 0
        
        # --- ПРИКРЕПЛЯЕМ ЛОГИ ЕСЛИ НУЖНО ---
        cluster_log_mode = get_external_param('cluster_log', 'default')
        attach_logs_method = getattr(type(self), "_LoadSuiteBase__attach_logs", None)
        if attach_logs_method:
            try:
                # Прикрепляем логи если есть проблемы или включен режим 'all'
                if cluster_log_mode == 'all' or node_issues > 0 or result.errors:
                    attach_logs_method(
                        start_time=getattr(result, "start_time", None),
                        attach_name="kikimr",
                        query_text="",
                        ignore_roles=True
                    )
            except Exception as e:
                logging.warning(f"Failed to attach kikimr logs: {e}")

        # --- 1. FAIL ЕСЛИ ЕСТЬ КРИТИЧЕСКИЕ ПРОБЛЕМЫ С НОДАМИ ---
        critical_node_issues = sum(1 for err in node_errors if err.was_oom or err.core_hashes)
        if critical_node_issues > 0:
            error_msg = f"Test failed: found {critical_node_issues} node(s) with OOM/cores"
            pytest.fail(error_msg)
        
        # --- 2. FAIL ЕСЛИ ВСЕ ИТЕРАЦИИ НЕ ВЫПОЛНИЛИСЬ УСПЕШНО ---
        total_iterations = len(result.iterations)
        if total_iterations > 0:
            # Считаем успешные итерации (без ошибок в stderr)
            successful_iterations = 0
            for iteration in result.iterations.values():
                if hasattr(iteration, 'stderr') and iteration.stderr:
                    stderr_clean = iteration.stderr.strip()
                    if not stderr_clean or stderr_clean == "warning: permanently added":
                        successful_iterations += 1
                else:
                    successful_iterations += 1
            
            if successful_iterations == 0:
                pytest.fail("Test failed: all iterations failed to execute successfully")
        
        # --- 3. WARNING ЕСЛИ ЕСТЬ TIMEOUT (но не fail) ---
        timeout_iterations = sum(1 for iteration in result.iterations.values() 
                               if hasattr(iteration, 'error_message') and 
                               'timeout' in iteration.error_message.lower())
        if timeout_iterations > 0:
            logging.warning(f"Test completed with {timeout_iterations} timeout iterations")
        
        # --- 4. PASS - все остальные случаи ---
        logging.info("Test completed successfully")
        
        # Логируем предупреждения если есть
        if result.warning_message:
            logging.warning(f"Workload completed with warnings: {result.warning_message}")

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

    @classmethod
    def get_query_list(cls) -> list[str]:
        return []

    @classmethod
    def get_path(cls) -> str:
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
            threads=cls.threads,
            users=cls.get_users(),
        )

    def test(self, query_name):
        self.process_query_result(result=self.__results[query_name], query_name=query_name, upload=True)


def pytest_generate_tests(metafunc):
    if issubclass(metafunc.cls, LoadSuiteParallel):
        metafunc.parametrize("query_name", metafunc.cls.get_query_list() + ["Sum"])
