# -*- coding: utf-8 -*-

from concurrent.futures import ThreadPoolExecutor
import getpass
import os
import logging
import tempfile
import stat
import sys
import argparse
import re
from argparse import RawTextHelpFormatter

from library.python import resource

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster # noqa
from ydb.tests.library.wardens.factories import safety_warden_factory, liveness_warden_factory # noqa

logger = logging.getLogger('ydb.connection')
logger.setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)


STRESS_BINARIES_DEPLOY_PATH = '/Berkanavt/nemesis/bin/'

# Define a simpler approach to timestamping
DATE_FORMAT = '%Y-%m-%d %H:%M:%S.%N'

# Simplified timestamp wrapper that doesn't use nested quotes
TIMESTAMP_WRAPPER_CMD = "timestamp_cmd() { while IFS= read -r line; do d=$(TZ=UTC date +'%Y-%m-%d %H:%M:%S.%N'); printf \"[%s] %s\\n\" \"$d\" \"$line\"; done; }; timestamp_cmd"

DICT_OF_SERVICES = {
    'nemesis' : {
        'status': """
            if systemctl is-active --quiet nemesis; then
                echo "Running"
            else
                echo "Stopped"
            fi""",
        'start_command' : "sudo service nemesis restart",
        'stop_command' : "sudo service nemesis stop"
    }

}

DICT_OF_PROCESSES = {
    'olap_workload' : {
        'status' : """
            if ps aux | grep -E "/Berkanavt/nemesis/bin/olap_workload|/tmp/olap_workload_wrapper.sh" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi"""
    },
    'oltp_workload' : {
        'status' : """
            if ps aux | grep -E "/Berkanavt/nemesis/bin/oltp_workload|/tmp/oltp_workload_wrapper.sh" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi""",
    },
    'node_broker_workload': {
        'status': """
            if ps aux | grep -E "/Berkanavt/nemesis/bin/node_broker_workload|/tmp/node_broker_workload_wrapper.sh" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi""",
    },
    'transfer_workload_row': {
        'status': """
            if ps aux | grep -E "/Berkanavt/nemesis/bin/transfer_workload.*mode row|/tmp/transfer_workload_row_wrapper.sh" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi""",
    },
    'transfer_workload_column': {
        'status': """
            if ps aux | grep -E "/Berkanavt/nemesis/bin/transfer_workload.*mode column|/tmp/transfer_workload_column_wrapper.sh" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi""",
    },
    's3_backups_workload': {
        'status': """
            if ps aux | grep -E "/Berkanavt/nemesis/bin/s3_backups_workload|/tmp/s3_backups_workload_wrapper.sh" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi""",
    },
    'simple_queue_column': {
        'status': """
            if ps aux | grep -E "/Berkanavt/nemesis/bin/simple_queue.*mode column|/tmp/simple_queue_column_wrapper.sh" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi"""
    },
    'simple_queue_row' : {
        'status' : """
            if ps aux | grep -E "/Berkanavt/nemesis/bin/simple_queue.*mode row|/tmp/simple_queue_row_wrapper.sh" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi"""
    },
    'workload_log_column' : {
        'status' : """
            if ps aux | grep -E "/Berkanavt/nemesis/bin/ydb_cli.*workload.*log.*run.*bulk_upsert.*log_workload_column|/tmp/workload_log_column_wrapper.sh" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi"""
    },
    'workload_log_row' : {
        'status' : """
            if ps aux | grep -E "/Berkanavt/nemesis/bin/ydb_cli.*workload.*log.*run.*bulk_upsert.*log_workload_row|/tmp/workload_log_row_wrapper.sh" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi"""
    },
    'workload_log_column_select' : {
        'status' : """
            if ps aux | grep -E "/Berkanavt/nemesis/bin/ydb_cli.*workload.*log.*run.*select.*log_workload_column|/tmp/workload_log_column_select_wrapper.sh" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi"""
    },
    'workload_log_row_select' : {
        'status' : """
            if ps aux | grep -E "/Berkanavt/nemesis/bin/ydb_cli.*workload.*log.*run.*select.*log_workload_row|/tmp/workload_log_row_select_wrapper.sh" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi"""
    },
    'workload_topic' : {
        'status' : """
            if ps aux | grep -E "/Berkanavt/nemesis/bin/ydb_cli.*workload.*topic.*run.*|/tmp/workload_topic.sh" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi"""
    }
}


# Создаем кастомный класс ArgumentParser для улучшения сообщений об ошибках
class CustomArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        # Если ошибка связана с отсутствием аргумента --name
        if "--name" in message:
            workload_choices = list(DICT_OF_PROCESSES.keys())
            self.print_usage(sys.stderr)
            self.exit(2, f"{self.prog}: error: {message}\n\nAvailable workload names:\n" +
                      "\n".join(f"  - {choice}" for choice in workload_choices) + "\n")
        # Если ошибка связана с неверным значением для ACTION
        elif "ACTION" in message and "invalid choice" in message:
            # Определяем доступные действия на момент вызова
            actions_help = {
                "get_errors": "Show all errors from YDB logs",
                "get_errors_aggr": "Show aggregated errors from YDB logs (grouped by similarity)",
                "get_errors_last": "Show only the most recent errors from YDB logs",
                "get_state": "Display current state of all services and workloads",
                "clean_workload": "Clean the database objects for a specific workload (requires --name)",
                "cleanup": "Clean all logs and dumps",
                "cleanup_logs": "Clean only logs",
                "cleanup_dumps": "Clean only core dumps",
                "deploy_ydb": "Deploy YDB cluster and configure it",
                "deploy_tools": "Deploy workload tools to the cluster nodes",
                "start_nemesis": "Start the nemesis service",
                "stop_nemesis": "Stop the nemesis service",
                "start_default_workloads": "Start all default workloads on the cluster",
                "start_workload_simple_queue_row": "Start simple_queue workload with row storage",
                "start_workload_simple_queue_column": "Start simple_queue workload with column storage",
                "start_workload_olap_workload": "Start OLAP workload for analytical load testing",
                "start_workload_oltp_workload": "Start OLTP workload for transactional load testing",
                "start_workload_node_broker_workload": "Start Node Broker workload",
                "start_workload_transfer_workload": "Start topic to table transfer workload",
                "start_workload_s3_backups_workload": "Start auto removal of tmp tables workload",
                "start_workload_log": "Start log workloads with both row and column storage",
                "start_workload_log_column": "Start log workload with column storage",
                "start_workload_log_row": "Start log workload with row storage",
                "start_workload_topic": "Start topic workload",
                "stop_workloads": "Stop all workloads",
                "stop_workload": "Stop a specific workload (requires --name)",
                "perform_checks": "Run safety and liveness checks on the cluster",
                "get_workload_outputs": "Show output from workload processes (supports --mode and --last_n_lines)"
            }

            self.print_usage(sys.stderr)
            self.exit(2, f"{self.prog}: error: {message}\n\nAvailable actions:\n" +
                      "\n".join(f"  - {action}" for action in sorted(actions_help.keys())) + "\n")
        # Если ошибка связана с аргументом --mode
        elif "--mode" in message:
            self.print_usage(sys.stderr)
            self.exit(2, f"{self.prog}: error: {message}\n\nAvailable modes for --mode:\n" +
                      "  - out (show stdout only)\n" +
                      "  - err (show stderr/errors only, default)\n" +
                      "  - all (show both stdout and stderr)\n")
        else:
            super().error(message)


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class StabilityCluster:
    def __init__(self, ssh_username, cluster_path, ydbd_path=None, ydbd_next_path=None, yaml_config=None):
        self.working_dir = os.path.join(tempfile.gettempdir(), "ydb_stability")
        os.makedirs(self.working_dir, exist_ok=True)
        self.ssh_username = ssh_username
        self.slice_directory = cluster_path
        self.ydbd_path = ydbd_path
        self.ydbd_next_path = ydbd_next_path
        self.yaml_config = yaml_config

        self.artifacts = (
            self._unpack_resource('nemesis'),
            self._unpack_resource('simple_queue'),
            self._unpack_resource('olap_workload'),
            self._unpack_resource('oltp_workload'),
            self._unpack_resource('node_broker_workload'),
            self._unpack_resource('transfer_workload'),
            self._unpack_resource('s3_backups_workload'),
            self._unpack_resource('statistics_workload'),
            self._unpack_resource('ydb_cli'),
        )

        if self.yaml_config is None:
            self.kikimr_cluster = ExternalKiKiMRCluster(
                cluster_template=self.slice_directory,
                kikimr_configure_binary_path=self._unpack_resource("cfg"),
                kikimr_path=self.ydbd_path,
                kikimr_next_path=self.ydbd_next_path,
                ssh_username=self.ssh_username,
                deploy_cluster=True,
            )
        else:
            self.kikimr_cluster = ExternalKiKiMRCluster(
                cluster_template=self.slice_directory,
                kikimr_configure_binary_path=None,
                kikimr_path=self.ydbd_path,
                kikimr_next_path=self.ydbd_next_path,
                ssh_username=self.ssh_username,
                yaml_config=self.yaml_config,
            )

    def _unpack_resource(self, name):
        res = resource.find(name)
        path_to_unpack = os.path.join(self.working_dir, name)
        with open(path_to_unpack, "wb") as f:
            f.write(res)

        st = os.stat(path_to_unpack)
        os.chmod(path_to_unpack, st.st_mode | stat.S_IEXEC)
        return path_to_unpack

    def clean_trace(self, traces):
        cleaned_lines = []
        for line in traces.split('\n'):
            line = re.sub(r' @ 0x[a-fA-F0-9]+', '', line)
            # Убираем все до текста ошибки или указателя на строку кода
            match_verify = re.search(r'VERIFY|FAIL|signal 11|signal 6|signal 15|uncaught exception|ERROR: AddressSanitizer|SIG', line)
            match_code_file_line = re.search(r'\s+(\S+\.cpp:\d+).*', line)

            if match_verify:
                cleaned_lines.append(match_verify.group())
            elif match_code_file_line:
                cleaned_lines.append(match_code_file_line.group())

        return "\n".join(cleaned_lines)

    def is_sublist(self, shorter, longer):
        """ Check if 'shorter' is a sublist in 'longer' from the start """
        return longer[:len(shorter)] == shorter

    def find_unique_traces_with_counts(self, all_traces):
        clean_traces_dict = {}
        unique_traces = {}

        for trace in all_traces:
            clean_trace = self.clean_trace(trace)
            if clean_traces_dict.get(clean_trace):
                clean_traces_dict[clean_trace].append(trace)
            else:
                clean_traces_dict[clean_trace] = [trace]

        clean_traces_dict = dict(sorted(clean_traces_dict.items(), key=lambda item: len(item[0])))
        for trace in clean_traces_dict:
            for unique in unique_traces:
                if self.is_sublist(trace, unique):
                    unique_traces[unique] = unique_traces[unique] + clean_traces_dict[trace]
                    break
                elif self.is_sublist(unique, trace):
                    unique_traces[trace] = unique_traces[unique] + clean_traces_dict[trace]
                    del unique_traces[unique]
                    break
            if not unique_traces.get(trace):
                unique_traces[trace] = clean_traces_dict[trace]

        return dict(sorted(unique_traces.items(), key=lambda item: len(item[1]), reverse=True))

    def process_lines(self, text):
        traces = []
        trace = ""
        for host in text:
            host = host.split('\n')
            for line in host:
                if line in ("--", "\n", ""):
                    traces.append(trace)
                    trace = ""
                else:
                    trace = trace + line + '\n'
        return traces

    def get_all_errors(self, mode='all'):
        all_results = []
        if mode == 'all' or mode == 'raw' or mode == 'aggr':
            command = """
                    ls -ltr /Berkanavt/kikim*/logs/kikimr* |
                    awk '{print $NF}' |
                    while read file; do
                    case "$file" in
                        *.txt) cat "$file" ;;
                        *.gz) zcat "$file" ;;
                        *) cat "$file" ;;
                    esac
                    done |
                    grep -E 'VERIFY|FAIL |signal 11|signal 6|signal 15|uncaught exception|ERROR: AddressSanitizer|SIG' -A 40 -B 20
                    """
        elif mode == 'last':
            command = """
                    ls -ltr /Berkanavt/kikim*/logs/kikimr |
                    awk '{print $NF}' |
                    while read file; do
                    cat "$file" | grep -E 'VERIFY|FAIL |signal 11|signal 6|signal 15|uncaught exception|ERROR: AddressSanitizer|SIG' -A 40 -B 20 | tail -120
                    echo "--"
                    done
                    """
        for node in self.kikimr_cluster.nodes.values():
            result = node.ssh_command(command, raise_on_error=False)
            if result:
                all_results.append(result.decode('utf-8'))
        all_results = self.process_lines(all_results)
        return all_results

    def get_errors(self, mode='raw'):
        errors = self.get_all_errors(mode=mode)
        if mode == 'raw' or mode == 'last':
            print('Traces:')
            for trace in errors:
                print(f"{trace}\n{'-'*60}")
        else:
            unique_traces = self.find_unique_traces_with_counts(errors)
            for trace in unique_traces:
                print(f"Trace (Occurrences: {len(unique_traces[trace])}):\n{trace}\n{'-'*60}")

    def perform_checks(self):
        safety_violations = safety_warden_factory(self.kikimr_cluster, self.ssh_username, lines_after=20, cut=False, modification_days=3).list_of_safety_violations()
        liveness_violations = liveness_warden_factory(self.kikimr_cluster, self.ssh_username).list_of_liveness_violations
        coredumps_search_results = {}
        for node in self.kikimr_cluster.nodes.values():
            result = node.ssh_command('find /coredumps/ -type f | wc -l', raise_on_error=False)
            coredumps_search_results[node.host.split(':')[0]] = int(result.decode('utf-8'))
        minidumps_search_results = {}
        for node in self.kikimr_cluster.nodes.values():
            result = node.ssh_command('''
            if [ -d "/Berkanavt/minidumps/" ]; then
                find /Berkanavt/minidumps/ -type f | wc -l
                else
                echo 0
            fi
            ''', raise_on_error=False)
            minidumps_search_results[node.host.split(':')[0]] = int(result.decode('utf-8'))

        print("SAFETY WARDEN:")
        for i, violation in enumerate(safety_violations):
            print("[{}]".format(i))
            print(violation.replace('\n\n', '\n'))

        print("LIVENESS WARDEN:")
        for i, violation in enumerate(liveness_violations):
            print("[{}]".format(i))
            print(violation)

        print("SAFETY WARDEN (total: {})".format(len(safety_violations)))
        print("LIVENESS WARDEN (total: {})".format(len(liveness_violations)))
        print("COREDUMPS:")
        for node in coredumps_search_results:
            print(f'    {node}: {coredumps_search_results[node]}')
        print("MINIDUMPS:")
        for node in coredumps_search_results:
            print(f'    {node}: {minidumps_search_results[node]}')

    def start_nemesis(self):
        self.prepare_config_files()
        with ThreadPoolExecutor() as pool:
            pool.map(lambda node: node.ssh_command(DICT_OF_SERVICES['nemesis']['start_command'], raise_on_error=True), self.kikimr_cluster.nodes.values())

    def stop_workloads_all_nodes(self):
        with ThreadPoolExecutor() as pool:
            pool.map(self.stop_workloads, self.kikimr_cluster.nodes.values())

    def stop_workloads(self, node):
        # Сначала получим список всех screen сессий для диагностики
        screen_sessions = node.ssh_command(
            'screen -ls || true',  # || true to handle case when no screens exist
            raise_on_error=False
        )

        if screen_sessions:
            screen_output = screen_sessions.decode('utf-8')
            print(f"{bcolors.OKCYAN}Debug: Screen sessions before cleanup:{bcolors.ENDC}")
            for line in screen_output.splitlines():
                print(f"{bcolors.OKCYAN}  {line}{bcolors.ENDC}")

            # Правильно завершаем каждую screen-сессию - живую и мертвую
            node.ssh_command(
                'screen -ls | grep -E "(Detached|Dead)" | cut -f1 -d"." | xargs -r kill -9',
                raise_on_error=False
            )

            # Пытаемся очистить мертвые сессии
            node.ssh_command(
                'screen -wipe',
                raise_on_error=False
            )

        # На всякий случай убиваем все потенциальные процессы рабочих нагрузок
        node.ssh_command(
            'pkill -f "SCREEN.*workload\\|simple_queue\\|olap_workload\\|oltp_workload\\|node_broker_workload\\|transfer_workload\\|s3_backups_workload"',
            raise_on_error=False
        )

        # Убиваем все wrapper-скрипты
        node.ssh_command(
            'pkill -f "_wrapper.sh"',
            raise_on_error=False
        )

        # Надежно убиваем все основные процессы рабочих нагрузок
        for workload_pattern in [
            "/Berkanavt/nemesis/bin/simple_queue",
            "/Berkanavt/nemesis/bin/olap_workload",
            "/Berkanavt/nemesis/bin/oltp_workload",
            "/Berkanavt/nemesis/bin/ydb_cli.*workload.*log.*run",
            "/Berkanavt/nemesis/bin/ydb_cli.*workload.*topic.*"
        ]:
            node.ssh_command(
                f'pkill -9 -f "{workload_pattern}"',
                raise_on_error=False
            )

        # На всякий случай попробуем еще раз убить все screen
        node.ssh_command(
            'sudo pkill screen',
            raise_on_error=False
        )

        # Финальная проверка, остались ли какие-то сессии
        after_sessions = node.ssh_command(
            'screen -ls || true',
            raise_on_error=False
        )

        if after_sessions:
            after_output = after_sessions.decode('utf-8')
            if "No Sockets found" not in after_output:
                print(f"{bcolors.WARNING}Warning: Some screen sessions still exist after cleanup:{bcolors.ENDC}")
                for line in after_output.splitlines():
                    print(f"{bcolors.WARNING}  {line}{bcolors.ENDC}")
            else:
                print(f"{bcolors.OKGREEN}All screen sessions cleaned up successfully.{bcolors.ENDC}")

    def stop_workload(self, workload_name):
        """Остановка конкретной рабочей нагрузки на всех узлах"""
        if workload_name not in DICT_OF_PROCESSES:
            print(f"{bcolors.FAIL}Ошибка: Неизвестный workload '{workload_name}'{bcolors.ENDC}")
            return

        print(f"{bcolors.BOLD}{bcolors.HEADER}=== Остановка {workload_name} на всех nodes ==={bcolors.ENDC}")

        for node in self.kikimr_cluster.nodes.values():
            node_host = node.host.split(':')[0]
            print(f"{bcolors.BOLD}Останавливаем {workload_name} на {node_host}:{bcolors.ENDC}")

            # Находим все связанные процессы с этой рабочей нагрузкой
            node.ssh_command(
                f"pkill -9 -f '/tmp/{workload_name}_wrapper.sh'",
                raise_on_error=False
            )

            # Остановка всех screen-сессий с указанным именем, включая мертвые (Dead)
            node.ssh_command(
                f"screen -ls | grep -E '(Detached|Dead).*{workload_name}' | cut -f1 -d'.' | xargs -r kill -9",
                raise_on_error=False
            )

            # Пытаемся удалить мертвые сессии напрямую
            node.ssh_command(
                "screen -wipe",
                raise_on_error=False
            )

            # Убиваем процессы фактической рабочей нагрузки, нужный шаблон берем из конфигурации
            status_cmd = DICT_OF_PROCESSES[workload_name]['status']
            grep_pattern = next((line for line in status_cmd.split('\n') if 'grep' in line and '-v grep' not in line), '')
            if grep_pattern:
                # Извлекаем шаблон grep из команды проверки статуса
                grep_parts = grep_pattern.split('grep')
                if len(grep_parts) > 1:
                    pattern = grep_parts[1].strip().replace('"', '').replace("'", "").split('|')[0]
                    node.ssh_command(
                        f"pkill -9 -f '{pattern}'",
                        raise_on_error=False
                    )

            # Дополнительная проверка по имени нагрузки
            node.ssh_command(
                f"ps aux | grep -E '{workload_name}|{workload_name.replace('_', '.*')}' | grep -v grep | awk '{{print $2}}' | xargs -r kill -9",
                raise_on_error=False
            )

            # Проверка результата
            result = node.ssh_command(DICT_OF_PROCESSES[workload_name]['status'], raise_on_error=False)
            status = result.decode('utf-8').replace('\n', '')
            if status == 'Stopped':
                print(f"{bcolors.OKGREEN}Workload {workload_name} успешно остановлена на {node_host}{bcolors.ENDC}")
            else:
                print(f"{bcolors.FAIL}Не удалось остановить {workload_name} на {node_host}, статус: {status}{bcolors.ENDC}")

        print(f"{bcolors.OKGREEN}Завершено выполнение остановки {workload_name} на всех узлах{bcolors.ENDC}")

    def stop_nemesis(self):
        print(f"{bcolors.BOLD}{bcolors.HEADER}=== ОСТАНОВКА NEMESIS ==={bcolors.ENDC}")

        # Останавливаем nemesis на всех нодах
        with ThreadPoolExecutor() as pool:
            pool.map(lambda node: node.ssh_command(DICT_OF_SERVICES['nemesis']['stop_command'], raise_on_error=False), self.kikimr_cluster.nodes.values())

        print(f"{bcolors.OKGREEN}Nemesis остановлен на всех нодах{bcolors.ENDC}")

        # Автоматически восстанавливаем кластер после остановки nemesis
        print(f"\n{bcolors.BOLD}{bcolors.OKCYAN}=== АВТОМАТИЧЕСКОЕ ВОССТАНОВЛЕНИЕ ПОСЛЕ ОСТАНОВКИ NEMESIS ==={bcolors.ENDC}")
        self.restore_cluster()

    def restore_cluster(self):
        """
        Восстанавливает сетевую связанность кластера, нарушенную nemesis.
        Восстанавливает только порты и маршруты, не трогает сервисы YDB.
        """
        print(f"{bcolors.BOLD}{bcolors.HEADER}=== ВОССТАНОВЛЕНИЕ СЕТЕВОЙ СВЯЗАННОСТИ ==={bcolors.ENDC}")

        # Счетчики для итоговой статистики
        total_nodes = len(self.kikimr_cluster.nodes.values())
        restored_nodes = 0
        cleared_fw_rules = 0
        cleared_routes = 0
        available_ports = 0

        with ThreadPoolExecutor() as pool:
            # Восстанавливаем сетевую связанность всех нод параллельно
            results = list(pool.map(self._restore_node, self.kikimr_cluster.nodes.values()))

        # Подсчитываем результаты
        for result in results:
            if result:
                restored_nodes += 1
                cleared_fw_rules += result.get('fw_cleared', 0)
                cleared_routes += result.get('routes_cleared', 0)
                available_ports += result.get('available_ports', 0)

        # Выводим итоговую статистику
        print(f"\n{bcolors.BOLD}📊 ИТОГОВАЯ СТАТИСТИКА:{bcolors.ENDC}")
        print(f"  ✅ Восстановлено нод: {restored_nodes}/{total_nodes}")

        # Статистика нарушений
        total_violations = cleared_fw_rules + cleared_routes
        if total_violations > 0:
            print(f"  🔧 Исправлено нарушений: {total_violations}")
            print(f"    • Правил ip6tables: {cleared_fw_rules}")
            print(f"    • Недоступных маршрутов: {cleared_routes}")
        else:
            print("  ✅ Нарушений не обнаружено")

        # Итоговое сообщение
        if restored_nodes == total_nodes:
            if total_violations > 0:
                print(f"\n{bcolors.OKGREEN}🎉 Восстановление завершено! Исправлено {total_violations} нарушений.{bcolors.ENDC}")
            else:
                print(f"\n{bcolors.OKGREEN}🎉 Восстановление завершено! Нарушений не обнаружено.{bcolors.ENDC}")
        else:
            print(f"\n{bcolors.WARNING}⚠️  Восстановление завершено с предупреждениями{bcolors.ENDC}")

    def _restore_node(self, node):
        """
        Восстанавливает сетевую связанность ноды кластера.
        Восстанавливает только порты и маршруты, не трогает сервисы YDB.

        Args:
            node: Нода кластера для восстановления

        Returns:
            dict: Статистика восстановления ноды
        """
        node_host = node.host.split(':')[0]

        # Статистика для возврата
        stats = {
            'node': node_host,
            'fw_cleared': 0,
            'routes_cleared': 0,
            'available_ports': 0,
            'success': False
        }

        try:
            # 1. Проверяем и восстанавливаем сетевые правила
            fw_rules_cleared = 0
            routes_cleared = 0

            # Проверяем правила ip6tables YDB_FW
            fw_check = node.ssh_command("sudo /sbin/ip6tables -w -L YDB_FW 2>/dev/null | grep -v 'Chain YDB_FW' | grep -v 'target' | wc -l", raise_on_error=False)
            if fw_check:
                try:
                    rules_count = int(fw_check.decode('utf-8').strip())
                    if rules_count > 0:
                        fw_result = node.ssh_command("sudo /sbin/ip6tables -w -F YDB_FW", raise_on_error=False)
                        if fw_result:
                            fw_rules_cleared = rules_count
                            print(f"    🔧 Найдено и очищено {rules_count} правил ip6tables")
                except (ValueError, AttributeError):
                    pass

            # Проверяем и удаляем недоступные маршруты
            unreach_result = node.ssh_command("sudo /usr/bin/ip -6 route show | grep unreachable", raise_on_error=False)
            if unreach_result:
                unreach_routes = unreach_result.decode('utf-8').strip().split('\n')
                found_routes = 0
                for route in unreach_routes:
                    if route.strip():
                        ip_match = re.search(r'unreachable\s+([^\s]+)', route)
                        if ip_match:
                            ip = ip_match.group(1)
                            node.ssh_command(f"sudo /usr/bin/ip -6 route del unreachable {ip}", raise_on_error=False)
                            routes_cleared += 1
                            found_routes += 1

                if found_routes > 0:
                    print(f"    🔧 Найдено и удалено {found_routes} недоступных маршрутов")

            # 2. Проверяем сетевую доступность
            ports_to_check = [2135, 2136, 8765, 19001]
            available_ports_count = 0

            for port in ports_to_check:
                port_result = node.ssh_command(f"nc -z localhost {port}", raise_on_error=False)
                if port_result:
                    available_ports_count += 1

            # Обновляем статистику
            stats['fw_cleared'] = fw_rules_cleared
            stats['routes_cleared'] = routes_cleared
            stats['available_ports'] = available_ports_count
            stats['success'] = True

            # Краткий вывод для ноды с указанием нарушений
            status_icon = "✅" if stats['success'] else "❌"
            violations = []
            if fw_rules_cleared > 0:
                violations.append(f"FW:{fw_rules_cleared}")
            if routes_cleared > 0:
                violations.append(f"Routes:{routes_cleared}")

            if violations:
                violations_str = f" [Исправлено: {', '.join(violations)}]"
            else:
                violations_str = " [Нарушений не найдено]"

            print(f"{status_icon} {node_host}: Ports={available_ports_count}/4{violations_str}")

        except Exception as e:
            print(f"❌ {node_host}: Ошибка - {e}")

        return stats

    def get_state(self):
        logging.getLogger().setLevel(logging.WARNING)
        state_objects_dic = dict(list(DICT_OF_SERVICES.items()) + list(DICT_OF_PROCESSES.items()))
        for node_id, node in enumerate(self.kikimr_cluster.nodes.values()):
            node_host = node.host.split(':')[0]
            print(f'{bcolors.BOLD}{node_host}{bcolors.ENDC}:')
            for state_object in state_objects_dic:
                try:
                    result = node.ssh_command(
                        state_objects_dic[state_object]['status'],
                        raise_on_error=True
                    )
                    status = result.decode('utf-8').replace('\n', '')
                    if status == 'Running' :
                        status = bcolors.OKGREEN + status + bcolors.ENDC
                    else:
                        status = bcolors.FAIL + status + bcolors.ENDC
                    print(f'\t{state_object}:\t{status}')
                except Exception as e:
                    print(f'\t{state_object}:\t{bcolors.FAIL}{e}{bcolors.ENDC}')

    def cleanup(self, mode='all'):
        for node in self.kikimr_cluster.nodes.values():
            if mode in ['all', 'dumps']:
                node.ssh_command('sudo find /coredumps/ -type f -exec rm -f {} +', raise_on_error=False)
                node.ssh_command('sudo find /Berkanavt/kikim*/minidumps/ -type f -exec rm -f {} +', raise_on_error=False)
                node.ssh_command('sudo find /Berkanavt/kikimr/bin/versions/ -type f -exec rm -f {} +', raise_on_error=False)
            if mode in ['all', 'logs']:
                node.ssh_command('sudo find /Berkanavt/kikim*/logs/kikimr* -type f -exec rm -f {} +', raise_on_error=False)
                node.ssh_command('sudo find /Berkanavt/nemesis/log/ -type f -exec rm -f {} +', raise_on_error=False)
        if mode in ['all', 'logs']:
            self.kikimr_cluster.cleanup_logs()

    def deploy_ydb(self):
        self.cleanup()
        self.kikimr_cluster.start()

        with open(self._unpack_resource("tbl_profile.txt")) as f:
            self.kikimr_cluster.client.console_request(f.read())

        self.kikimr_cluster.client.update_self_heal(True)

        node = list(self.kikimr_cluster.nodes.values())[0]
        node.ssh_command("/Berkanavt/kikimr/bin/kikimr admin console validator disable bootstrap", raise_on_error=True)

        self.deploy_tools()
        self.get_state()

    def deploy_node_tools(self, node):
        node.ssh_command(["sudo", "mkdir", "-p", STRESS_BINARIES_DEPLOY_PATH], raise_on_error=False)
        for artifact in self.artifacts:
            node_artifact_path = os.path.join(
                STRESS_BINARIES_DEPLOY_PATH,
                os.path.basename(
                    artifact
                )
            )
            node.copy_file_or_dir(
                artifact,
                node_artifact_path
            )
            node.ssh_command(f"sudo chmod 777 {node_artifact_path}", raise_on_error=False)

    def prepare_config_files(self):
        with ThreadPoolExecutor() as pool:
            if self.yaml_config is None:
                pool.map(lambda node: node.copy_file_or_dir(
                    self.slice_directory,
                    '/Berkanavt/kikimr/cfg/cluster.yaml'
                ), self.kikimr_cluster.nodes.values())
            else:
                pool.map(lambda node: node.copy_file_or_dir(
                    self.slice_directory,
                    '/Berkanavt/kikimr/cfg/databases.yaml'
                ), self.kikimr_cluster.nodes.values())
                pool.map(lambda node: node.copy_file_or_dir(
                    self.yaml_config,
                    '/Berkanavt/kikimr/cfg/config.yaml'
                ), self.kikimr_cluster.nodes.values())

    def deploy_tools(self):
        with ThreadPoolExecutor(len(self.kikimr_cluster.nodes)) as pool:
            pool.map(self.deploy_node_tools, self.kikimr_cluster.nodes.values())
        self.prepare_config_files()

    def get_workload_outputs(self, mode='err', last_n_lines=10):
        """Capture last N lines of output from all running workload screens."""
        logging.getLogger().setLevel(logging.WARNING)
        for node_id, node in enumerate(self.kikimr_cluster.nodes.values()):
            node_host = node.host.split(':')[0]
            print(f'\n{bcolors.BOLD}{bcolors.HEADER}=== {node_host} ==={bcolors.ENDC}')

            # First check running processes to match get_state behavior
            running_workloads = {}  # workload_name -> pid
            for workload_name, workload in DICT_OF_PROCESSES.items():
                result = node.ssh_command(workload['status'], raise_on_error=False)
                if result and 'Running' in result.decode('utf-8'):
                    # Get the PID of the screen process for this workload
                    ps_result = node.ssh_command(
                        f'ps aux | grep "[S]CREEN.*{workload_name}\\|[s]creen.*{workload_name}" | grep -v grep',
                        raise_on_error=False
                    )
                    if ps_result:
                        try:
                            pid = ps_result.decode('utf-8').split()[1]
                            running_workloads[workload_name] = pid
                            print(f"{bcolors.OKCYAN}Debug: Found screen process for {workload_name} with PID {pid}{bcolors.ENDC}")
                        except (IndexError, ValueError):
                            running_workloads[workload_name] = None
                            print(f"{bcolors.WARNING}Warning: Could not extract PID for {workload_name} from ps output:{bcolors.ENDC}")
                            print(ps_result.decode('utf-8'))

            if running_workloads:
                print(f"{bcolors.OKCYAN}Found running workloads: {', '.join(running_workloads.keys())}{bcolors.ENDC}")

            # Get list of running screen sessions with their proper names
            result = node.ssh_command(
                'screen -ls || true',  # || true to handle case when no screens exist
                raise_on_error=False
            )
            if not result:
                print(f"{bcolors.WARNING}No screen sessions found{bcolors.ENDC}")
                continue

            screens = result.decode('utf-8').strip().split('\n')
            print(f"{bcolors.OKCYAN}Debug: Found screen sessions:{bcolors.ENDC}")
            for screen in screens:
                print(f"{bcolors.OKCYAN}  {screen}{bcolors.ENDC}")

            found_screens = False
            for screen in screens:
                if not screen.strip() or 'Socket' in screen:  # Skip socket directory line
                    continue

                # Пропускаем мертвые сессии
                if 'Dead' in screen:
                    continue

                # Extract PID from screen listing (format: PID..hostname)
                try:
                    screen_pid = screen.strip().split('.')[0].split('\t')[0]
                    screen_name = screen.strip().split('\t')[0].split('.')[1]
                    print(f"{bcolors.OKCYAN}Debug: Processing screen with PID {screen_pid}, name {screen_name}{bcolors.ENDC}")
                except IndexError:
                    print(f"{bcolors.WARNING}Warning: Could not extract PID from screen line: {screen}{bcolors.ENDC}")
                    continue

                # Match screen session with workload by PID
                for workload_name, pid in running_workloads.items():
                    if pid == screen_pid:
                        found_screens = True
                        print(f'\n{bcolors.BOLD}{bcolors.OKCYAN}=== {workload_name} ==={bcolors.ENDC}')

                        # Get log output - with our new approach, both stdout and stderr go to the same log file
                        log_file = f'/tmp/{workload_name}.out.log'

                        # Display all log lines if requested
                        if mode in ['out', 'all']:
                            result = node.ssh_command(
                                f'tail -n {last_n_lines} {log_file} 2>/dev/null || true',
                                raise_on_error=False
                            )
                            print(f"\n{bcolors.BOLD}{bcolors.OKGREEN}LOG OUTPUT (last {last_n_lines} lines):{bcolors.ENDC}")
                            if result:
                                output_lines = result.decode('utf-8').split('\n')
                                for line in output_lines:
                                    if line.strip():  # Skip empty lines
                                        print(line)
                            else:
                                print(f"{bcolors.WARNING}No output found{bcolors.ENDC}")

                        # Show only errors if requested
                        if mode in ['err', 'all']:
                            result = node.ssh_command(
                                f'cat {log_file} 2>/dev/null | grep -E "error:|Error:|ERROR|FATAL|WARN|WARNING|exit.*status" | tail -n {last_n_lines} || true',
                                raise_on_error=False
                            )
                            print(f"\n{bcolors.BOLD}{bcolors.FAIL}ERROR MESSAGES (last {last_n_lines} error/warning lines):{bcolors.ENDC}")
                            if result:
                                error_lines = result.decode('utf-8').split('\n')
                                for line in error_lines:
                                    if line.strip():  # Skip empty lines
                                        if 'FATAL' in line or 'exit.*status' in line:
                                            print(f"{bcolors.BOLD}{bcolors.FAIL}{line}{bcolors.ENDC}")
                                        elif 'ERROR' in line.capitalize():
                                            print(f"{bcolors.FAIL}{line}{bcolors.ENDC}")
                                        elif 'WARN' in line or 'WARNING' in line:
                                            print(f"{bcolors.WARNING}{line}{bcolors.ENDC}")
                                        else:
                                            print(line)
                            else:
                                print(f"{bcolors.WARNING}No errors found{bcolors.ENDC}")

                        # Ensure we detach from the screen session
                        node.ssh_command(f'screen -d {screen_pid} 2>/dev/null || true', raise_on_error=False)

            if not found_screens and running_workloads:
                print(f"\n{bcolors.WARNING}Warning: Found running workloads but no matching screen sessions. The workloads might be running outside of screen.{bcolors.ENDC}")
                print(f"{bcolors.WARNING}Try checking process output directly:{bcolors.ENDC}")
                for workload in running_workloads:
                    result = node.ssh_command(f'ps aux | grep "{workload}" | grep -v grep', raise_on_error=False)
                    if result:
                        print(f"\n{bcolors.OKCYAN}Process info for {workload}:{bcolors.ENDC}")
                        print(result.decode('utf-8'))

                    # Also check for wrapper script
                    script_path = f'/tmp/{workload}_wrapper.sh'
                    result = node.ssh_command(f'cat {script_path} 2>/dev/null || true', raise_on_error=False)
                    if result:
                        print(f"\n{bcolors.OKCYAN}Wrapper script for {workload}:{bcolors.ENDC}")
                        print(result.decode('utf-8'))

    def _create_workload_command(self, workload_name, command, log_file=None):
        """
        Helper function to create a properly formatted screen command for running a workload.

        Args:
            workload_name: Name of the workload (used for screen session name)
            command: The actual command to run in a loop (without TZ=UTC prefix)
            log_file: Optional custom log file path. Defaults to /tmp/{workload_name}.out.log

        Returns:
            A joined string command ready to be passed to ssh_command
        """
        if log_file is None:
            log_file = f'/tmp/{workload_name}.out.log'

        # Проверяем наличие параметров ограничения времени
        has_time_limit = False
        timeout_seconds = 0  # По умолчанию нет таймаута

        # Проверяем наличие параметра --duration
        if '--duration' in command:
            has_time_limit = True
            try:
                duration_parts = command.split('--duration')
                if len(duration_parts) > 1:
                    duration_value = duration_parts[1].strip().split()[0]
                    if duration_value.isdigit():
                        # Добавляем только 5% к duration в качестве таймаута
                        timeout_seconds = int(duration_value) * 1.05
            except Exception:
                pass

        # Проверяем наличие параметра --seconds (он имеет приоритет, если указаны оба)
        if '--seconds' in command:
            has_time_limit = True
            try:
                seconds_parts = command.split('--seconds')
                if len(seconds_parts) > 1:
                    seconds_value = seconds_parts[1].strip().split()[0]
                    if seconds_value.isdigit():
                        # Добавляем только 5% к seconds в качестве таймаута
                        timeout_seconds = int(seconds_value) * 1.05
            except Exception:
                pass

        # Ограничиваем таймаут разумными пределами только если он указан
        if has_time_limit:
            if timeout_seconds < 30:
                timeout_seconds = 30  # Минимум 30 секунд
            elif timeout_seconds > 86400:
                timeout_seconds = 86400  # Максимум 24 часа

            # Приводим к целому числу
            timeout_seconds = int(timeout_seconds)

            print(f"Debug: Set timeout for {workload_name} to {timeout_seconds} seconds")
        else:
            print(f"Debug: No timeout set for {workload_name}, it will run without time limit")

        # Извлекаем базовую команду для определения очистки при таймауте
        base_command = command.split()[0] if command else ""

        # Create a script content with safer function-based approach
        script_content = f"""#!/bin/bash
# Auto-generated script for {workload_name}

# Define a function to add timestamps to output
timestamp_output() {{
  while IFS= read -r line; do
    current_time=$(date +'{DATE_FORMAT}')
    printf "[%s] %s\\n" "$current_time" "$line"
  done
}}

# Define cleanup function for timeout
handle_timeout() {{
  echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] TIMEOUT: Command took longer than {timeout_seconds} seconds"
  echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Performing emergency cleanup for {workload_name}..."

  # Сохраняем PID процесса bash, который запустил команду (если еще запущен)
  PIDS=$(ps -ef | grep "{command}" | grep -v grep | grep -v timeout | awk '{{print $2}}')

  if [ -n "$PIDS" ]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Found processes to kill: $PIDS"
    for pid in $PIDS; do
      echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Killing process $pid"
      kill -9 $pid 2>/dev/null || true
    done
  else
    echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] No matching processes found"
  fi

  # Специальная очистка в зависимости от типа команды
  if [[ "{base_command}" == *"oltp_workload"* ]]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Performing oltp_workload specific cleanup"
    # Убиваем Python процессы, связанные с oltp_workload
    pkill -9 -f "oltp_workload" || true

  elif [[ "{base_command}" == *"ydb_cli"* ]] && [[ "{command}" =~ workload\\ (log|topic) ]]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Performing workload log specific cleanup"
    # Убиваем процессы ydb_cli workload log
    pkill -9 -f "ydb_cli.*workload.*log" || true

  elif [[ "{base_command}" == *"simple_queue"* ]]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Performing simple_queue specific cleanup"
    # Убиваем процессы simple_queue
    pkill -9 -f "simple_queue" || true

  elif [[ "{base_command}" == *"olap_workload"* ]]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Performing olap_workload specific cleanup"
    # Убиваем процессы olap_workload
    pkill -9 -f "olap_workload" || true

  elif [[ "{base_command}" == *"node_broker_workload"* ]]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Performing node_broker_workload specific cleanup"
    # Убиваем процессы node_broker_workload
    pkill -9 -f "node_broker_workload" || true

  elif [[ "{base_command}" == *"transfer_workload"* ]]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Performing transfer_workload specific cleanup"
    # Убиваем процессы transfer_workload
    pkill -9 -f "transfer_workload" || true

  elif [[ "{base_command}" == *"s3_backups_workload"* ]]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Performing s3_backups_workload specific cleanup"
    # Убиваем процессы s3_backups_workload
    pkill -9 -f "s3_backups_workload" || true
  fi

  echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Emergency cleanup completed"
}}

# Trap for SIGTERM to perform cleanup if the script itself is killed
trap handle_timeout SIGTERM

# Бесконечный цикл для выполнения команды
echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Starting command in continuous loop: {command}"
while true; do
  echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Running command..."
  echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] COMMAND: {command}"
  echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] === COMMAND OUTPUT START ==="

  # Гибридный подход для перехвата вывода
  # Создаем временный файл для хранения вывода в случае необходимости
  TMP_OUT_FILE="/tmp/{workload_name}_output_$$.log"

  if {'true' if has_time_limit else 'false'}; then
    # Первый метод: прямой запуск с отключенной буферизацией
    stdbuf -o0 -e0 timeout -k 10 {timeout_seconds}s {command} 2>&1 | tee "$TMP_OUT_FILE" | while IFS= read -r line || [[ -n "$line" ]]; do
      echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] $line"
    done

    status=${{PIPESTATUS[0]}}
    LINES_COUNT=$(wc -l < "$TMP_OUT_FILE")

    # Если вывод почти пустой и код возврата ошибочный, пробуем второй метод
    if [ $status -eq 1 ] && [ $LINES_COUNT -lt 3 ]; then
      echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Switching to script method for better output capture"
      SCRIPT_FILE="/tmp/{workload_name}_transcript_$$.txt"

      # Второй метод: запуск через script для перехвата особого вывода
      script -q -c "timeout -k 10 {timeout_seconds}s {command}" "$SCRIPT_FILE" >/dev/null 2>&1
      status=$?

      # Обработка и вывод результата с таймстемпами
      cat "$SCRIPT_FILE" | grep -v "^Script " | while IFS= read -r line || [[ -n "$line" ]]; do
        echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] $line"
      done

      rm -f "$SCRIPT_FILE"
    fi
  else
    # То же самое для команд без таймаута
    stdbuf -o0 -e0 {command} 2>&1 | tee "$TMP_OUT_FILE" | while IFS= read -r line || [[ -n "$line" ]]; do
      echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] $line"
    done

    status=${{PIPESTATUS[0]}}
    LINES_COUNT=$(wc -l < "$TMP_OUT_FILE")

    if [ $status -eq 1 ] && [ $LINES_COUNT -lt 3 ]; then
      echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Switching to script method for better output capture"
      SCRIPT_FILE="/tmp/{workload_name}_transcript_$$.txt"

      script -q -c "{command}" "$SCRIPT_FILE" >/dev/null 2>&1
      status=$?

      cat "$SCRIPT_FILE" | grep -v "^Script " | while IFS= read -r line || [[ -n "$line" ]]; do
        echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] $line"
      done

      rm -f "$SCRIPT_FILE"
    fi
  fi

  # Удаляем временный файл
  rm -f "$TMP_OUT_FILE"

  echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] === COMMAND OUTPUT END ==="

  if {'true' if has_time_limit else 'false'} && [ $status -eq 124 ]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] WARNING: Command reached timeout after {timeout_seconds} seconds"
  fi

  echo "[$(date +'%Y-%m-%d %H:%M:%S.%N')] Command exited with status $status, restarting in 5 seconds..."

  # Ждем перед перезапуском
  sleep 5
done
"""

        # Create a temporary script path on the remote host
        script_path = f'/tmp/{workload_name}_wrapper.sh'

        # Use cat with a heredoc approach for more reliable script generation
        cmd = f"cat > {script_path} << 'EOFSCRIPT'\n{script_content}\nEOFSCRIPT\n" + \
              f"chmod +x {script_path} && screen -S {workload_name} -d -m -L -Logfile {log_file} {script_path}"

        return cmd

    def _clean_and_start_workload(self, node, workload_name, command, log_file=None):
        """
        Clean logs and start a workload.

        Args:
            node: Node to run the workload on
            workload_name: Name of the workload
            command: Command to run (without TZ=UTC prefix)
            log_file: Optional custom log file path
        """
        print(f"{bcolors.BOLD}{bcolors.HEADER}=== Запуск {workload_name} ==={bcolors.ENDC}")
        if log_file is None:
            log_file = f'/tmp/{workload_name}.out.log'

        # Clean log file
        node.ssh_command(['rm', '-f', log_file], raise_on_error=False)

        # Create and run command
        screen_command = self._create_workload_command(workload_name, command, log_file)
        print(f"Debug: Running command: {screen_command}")
        node.ssh_command([screen_command], raise_on_error=True)


def path_type(path):
    # Expand the user's home directory if ~ is present
    expanded_path = os.path.expanduser(path)
    # Check if the file exists
    if not os.path.exists(expanded_path):
        raise argparse.ArgumentTypeError(f"The file {expanded_path} does not exist.")
    return expanded_path


def parse_args():
    parser = CustomArgumentParser(
        description="""YDB Stability Testing Tool

This tool provides capabilities for managing and testing YDB clusters in a stability/stress testing environment.
It can deploy YDB, manage workloads, collect logs, and analyze errors.

Common usage scenarios:
1. Deploy YDB and tools:
   tool --cluster_path /path/to/cluster.yaml --ydbd_path /path/to/ydbd deploy_ydb deploy_tools

2. Start workloads:
   tool --cluster_path /path/to/cluster.yaml start_default_workloads

3. Check workload status:
   tool --cluster_path /path/to/cluster.yaml get_state

4. View workload outputs:
   tool --cluster_path /path/to/cluster.yaml get_workload_outputs --mode all

5. Stop specific workload:
   tool --cluster_path /path/to/cluster.yaml stop_workload --name olap_workload

6. Stop all workloads:
   tool --cluster_path /path/to/cluster.yaml stop_workloads
""",
        formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        "--cluster_path",
        required=True,
        type=path_type,
        help="Path to cluster.yaml configuration file defining the cluster topology",
    )
    parser.add_argument(
        "--ydbd_path",
        required=False,
        type=path_type,
        help="Path to ydbd binary to deploy to the cluster",
    )
    parser.add_argument(
        "--next_ydbd_path",
        required=False,
        type=path_type,
        help="Path to next ydbd version binary (for cross-version testing)",
    )
    parser.add_argument(
        "--yaml-config",
        required=False,
        default=None,
        type=path_type,
        help="Path to Yandex DB configuration v2",
    )
    parser.add_argument(
        "--ssh_user",
        required=False,
        default=getpass.getuser(),
        type=str,
        help="SSH username for connecting to cluster nodes (defaults to current user)",
    )

    # Define all available actions
    actions_help = {
        "get_errors": "Show all errors from YDB logs",
        "get_errors_aggr": "Show aggregated errors from YDB logs (grouped by similarity)",
        "get_errors_last": "Show only the most recent errors from YDB logs",
        "get_state": "Display current state of all services and workloads",
        "clean_workload": "Clean the database objects for a specific workload (requires --name)",
        "cleanup": "Clean all logs and dumps",
        "cleanup_logs": "Clean only logs",
        "cleanup_dumps": "Clean only core dumps",
        "deploy_ydb": "Deploy YDB cluster and configure it",
        "deploy_tools": "Deploy workload tools to the cluster nodes",
        "start_nemesis": "Start the nemesis service",
        "stop_nemesis": "Stop the nemesis service and automatically restore cluster",
        "restore_cluster": "Restore network connectivity (ports and routes) caused by nemesis",
        "start_default_workloads": "Start all default workloads on the cluster",
        "start_workload_simple_queue_row": "Start simple_queue workload with row storage",
        "start_workload_simple_queue_column": "Start simple_queue workload with column storage",
        "start_workload_olap_workload": "Start OLAP workload for analytical load testing",
        "start_workload_oltp_workload": "Start OLTP workload for transactional load testing",
        "start_workload_node_broker_workload": "Start Node Broker workload",
        "start_workload_transfer_workload": "Start topic to table transfer workload",
        "start_workload_s3_backups_workload": "Start auto removal of tmp tables workload",
        "start_workload_log": "Start log workloads with both row and column storage",
        "start_workload_log_column": "Start log workload with column storage",
        "start_workload_log_row": "Start log workload with row storage",
        "start_workload_topic": "Start topic workload",
        "stop_workloads": "Stop all workloads",
        "stop_workload": "Stop a specific workload (requires --name)",
        "perform_checks": "Run safety and liveness checks on the cluster",
        "get_workload_outputs": "Show output from workload processes (supports --mode and --last_n_lines)"
    }

    # Group actions by categories for better readability
    action_categories = {
        "CLUSTER MANAGEMENT": [
            "deploy_ydb", "deploy_tools", "start_nemesis", "stop_nemesis",
            "restore_cluster", "get_state", "perform_checks"
        ],
        "ERROR HANDLING": [
            "get_errors", "get_errors_aggr", "get_errors_last"
        ],
        "CLEANUP": [
            "cleanup", "cleanup_logs", "cleanup_dumps", "clean_workload"
        ],
        "WORKLOAD MANAGEMENT": [
            "start_default_workloads", "stop_workloads", "stop_workload",
            "get_workload_outputs"
        ],
        "SPECIFIC WORKLOADS": [
            "start_workload_simple_queue_row", "start_workload_simple_queue_column",
            "start_workload_olap_workload",
            "start_workload_oltp_workload",
            "start_workload_node_broker_workload",
            "start_workload_transfer_workload",
            "start_workload_s3_backups_workload",
            "start_workload_log", "start_workload_log_column", "start_workload_log_row",
            "start_workload_topic",
        ]
    }

    # Создаем понятную структуру для справки с четкими отступами и переносами строк
    actions_help_str = "Actions to execute (one or more of the following):\n"

    # Добавляем действия по категориям, каждое с новой строки и отступом
    for category, actions in action_categories.items():
        actions_help_str += f"\n{category}:\n"
        for action in actions:
            description = actions_help[action]
            actions_help_str += f"  {action}\n      {description}\n"

    parser.add_argument(
        "actions",
        type=str,
        nargs="+",
        choices=list(actions_help.keys()),
        help=actions_help_str,
        metavar="ACTION",
    )

    args, unknown = parser.parse_known_args()

    # Add action-specific arguments
    if "stop_workload" in args.actions:
        workload_choices = list(DICT_OF_PROCESSES.keys())
        workload_help = "Available workloads:\n"
        for wl in workload_choices:
            workload_help += f"  {wl}\n"

        parser.add_argument(
            "--name",
            type=str,
            required=True,
            help=f"Name of the workload to stop\n{workload_help}",
            choices=workload_choices,
            metavar="WORKLOAD_NAME",
        )

    if "clean_workload" in args.actions:
        workload_choices = list(DICT_OF_PROCESSES.keys())
        workload_help = "Available workloads:\n"
        for wl in workload_choices:
            if "log_" in wl:
                workload_help += f"  {wl}\n"

        parser.add_argument(
            "--name",
            type=str,
            required=True,
            help=f"Name of the workload to clean\n{workload_help}",
            choices=workload_choices,
            metavar="WORKLOAD_NAME",
        )

    if "get_workload_outputs" in args.actions:
        parser.add_argument(
            "--last_n_lines",
            type=int,
            default=10,
            help="Number of lines to show from the end of each log (default: 10)",
            metavar="N",
        )
        parser.add_argument(
            "--mode",
            type=str,
            choices=['out', 'err', 'all'],
            default='err',
            help="Which output to show: 'out' (stdout only), 'err' (stderr only), or 'all' (both) (default: err)",
            metavar="MODE",
        )

    return parser.parse_args()


def main():
    args = parse_args()
    ssh_username = args.ssh_user
    yaml_config = args.yaml_config
    print('Initing cluster info')
    stability_cluster = StabilityCluster(
        ssh_username=ssh_username,
        cluster_path=args.cluster_path,
        ydbd_path=args.ydbd_path,
        ydbd_next_path=args.next_ydbd_path,
        yaml_config=yaml_config,
    )

    for action in args.actions:
        print(f'Start action {action}')
        if action == "get_errors":
            stability_cluster.get_errors(mode='raw')
        if action == "get_errors_aggr":
            stability_cluster.get_errors(mode='aggr')
        if action == "get_errors_last":
            stability_cluster.get_errors(mode='last')
        if action == "get_state":
            stability_cluster.get_state()
        if action == "deploy_ydb":
            stability_cluster.deploy_ydb()
        if action == "cleanup":
            stability_cluster.cleanup()
        if action == "cleanup_logs":
            stability_cluster.cleanup('logs')
        if action == "cleanup_dumps":
            stability_cluster.cleanup('dumps')
        if action == "deploy_tools":
            stability_cluster.deploy_tools()
        if action == "start_default_workloads":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                stability_cluster._clean_and_start_workload(
                    node,
                    'simple_queue_row',
                    '/Berkanavt/nemesis/bin/simple_queue --database /Root/db1 --mode row'
                )
                stability_cluster._clean_and_start_workload(
                    node,
                    'simple_queue_column',
                    '/Berkanavt/nemesis/bin/simple_queue --database /Root/db1 --mode column'
                )
                stability_cluster._clean_and_start_workload(
                    node,
                    'olap_workload',
                    f'/Berkanavt/nemesis/bin/olap_workload --database /Root/db1 --path olap_workload_{node_id}'
                )
            stability_cluster.get_state()
        if action == "stop_workload":
            workload_name = args.name
            stability_cluster.stop_workload(workload_name)
            stability_cluster.get_state()
        if action == "clean_workload":
            workload_name = args.name
            if DICT_OF_PROCESSES.get(workload_name):
                store_type_list = []
                if 'column' in workload_name:
                    store_type_list.append('column')
                elif 'row' in workload_name:
                    store_type_list.append('row')
                else:
                    store_type_list = ['column', 'row']
                if 'log_' in workload_name:
                    first_node = stability_cluster.kikimr_cluster.nodes[1]
                    for store_type in store_type_list:
                        first_node.ssh_command([
                            '/Berkanavt/nemesis/bin/ydb_cli',
                            '--endpoint', f'grpc://localhost:{first_node.grpc_port}',
                            '--database', '/Root/db1',
                            'workload', 'log', 'clean',
                            '--path', f'log_workload_{store_type}',
                            ],
                            raise_on_error=True
                        )
                else:
                    print(f"Not supported workload clean command for {workload_name}")
            else:
                print(f"Unknown workload {workload_name}")
            stability_cluster.get_state()
        if "start_workload_log" in action:
            store_type_list = []
            if action == 'start_workload_log_column':
                store_type_list.append('column')
            elif action == 'start_workload_log_row':
                store_type_list.append('row')
            else:
                store_type_list = ['column', 'row']
            first_node = stability_cluster.kikimr_cluster.nodes[1]
            for store_type in store_type_list:
                first_node.ssh_command([
                    '/Berkanavt/nemesis/bin/ydb_cli',
                    '--endpoint', f'grpc://localhost:{first_node.grpc_port}',
                    '--database', '/Root/db1',
                    'workload', 'log', 'init',
                    '--min-partitions', '100',
                    '--partition-size', '10',
                    '--auto-partition', '0',
                    '--store', store_type,
                    '--path', f'log_workload_{store_type}',
                    '--ttl', '4800'
                    ],
                    raise_on_error=False
                )
                for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                    node.ssh_command(['rm', '-f', f'/tmp/workload_log_{store_type}.out.log'], raise_on_error=False)
                    stability_cluster._clean_and_start_workload(
                        node,
                        f'workload_log_{store_type}',
                        (
                            f'/Berkanavt/nemesis/bin/ydb_cli --endpoint grpc://localhost:{node.grpc_port} '
                            f'--database /Root/db1 workload log run bulk_upsert --rows 2000 --threads 10 '
                            f'--timestamp_deviation 180 --seconds 86400 --path log_workload_{store_type}'
                        )
                    )

                    node.ssh_command(['rm', '-f', f'/tmp/workload_log_{store_type}_select.out.log'], raise_on_error=False)
                    stability_cluster._clean_and_start_workload(
                        node,
                        f'workload_log_{store_type}_select',
                        (
                            f'/Berkanavt/nemesis/bin/ydb_cli --verbose --endpoint grpc://localhost:{node.grpc_port} '
                            f'--database /Root/db1 workload log run select --client-timeout 1800000 --threads 1 '
                            f'--seconds 86400 --path log_workload_{store_type}'
                        ),
                        f'/tmp/workload_log_{store_type}_select.out.log'
                    )
            stability_cluster.get_state()
        if action == "start_workload_topic":
            def run_topic_workload(node):
                node.ssh_command(['rm', '-f', '/tmp/workload_topic.out.log'], raise_on_error=False)

                stability_cluster._clean_and_start_workload(
                    node,
                    'workload_topic',
                    (
                        f'/Berkanavt/nemesis/bin/ydb_cli --verbose --endpoint grpc://localhost:{node.grpc_port} '
                        f'--database /Root/db1 workload topic run full -s 60 --byte-rate 100M --use-tx --tx-commit-interval 2000 -p 100 -c 50'
                    ),
                    '/tmp/workload_topic.out.log'
                )
            init_node = list(stability_cluster.kikimr_cluster.nodes.values())[0]
            init_node.ssh_command([
                '/Berkanavt/nemesis/bin/ydb_cli',
                '--verbose',
                '--endpoint',
                f'grpc://localhost:{init_node.grpc_port}',
                '--database',
                '/Root/db1',
                'workload',
                'topic',
                'init',
                '-c',
                '50',
                '-p',
                '100'], raise_on_error=False)
            with ThreadPoolExecutor() as pool:
                pool.map(run_topic_workload, stability_cluster.kikimr_cluster.nodes.values())
            stability_cluster.get_state()
        if action == "start_workload_simple_queue_row":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                stability_cluster._clean_and_start_workload(
                    node,
                    'simple_queue_row',
                    '/Berkanavt/nemesis/bin/simple_queue --database /Root/db1 --mode row'
                )
            stability_cluster.get_state()
        if action == "start_workload_simple_queue_column":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                stability_cluster._clean_and_start_workload(
                    node,
                    'simple_queue_column',
                    '/Berkanavt/nemesis/bin/simple_queue --database /Root/db1 --mode column'
                )
            stability_cluster.get_state()
        if action == "start_workload_olap_workload":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                stability_cluster._clean_and_start_workload(
                    node,
                    'olap_workload',
                    f'/Berkanavt/nemesis/bin/olap_workload --database /Root/db1 --path olap_workload_{node_id}'
                )
            stability_cluster.get_state()
        if action == "start_workload_oltp_workload":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                stability_cluster._clean_and_start_workload(
                    node,
                    'oltp_workload',
                    f'/Berkanavt/nemesis/bin/oltp_workload --database /Root/db1 --path {node_id}'
                )
            stability_cluster.get_state()
        if action == "start_workload_node_broker_workload":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                stability_cluster._clean_and_start_workload(
                    node,
                    'node_broker_workload',
                    f'/Berkanavt/nemesis/bin/node_broker_workload --database /Root/db1 --mon-endpoint http://localhost:{node.mon_port}'
                )
            stability_cluster.get_state()
        if action == "start_workload_transfer_workload":
            modes = ['row', 'column']
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                for mode in modes:
                    stability_cluster._clean_and_start_workload(
                        node,
                        f'transfer_workload_{mode}',
                        f'/Berkanavt/nemesis/bin/transfer_workload --database /Root/db1 --mode {mode}'
                    )
            stability_cluster.get_state()
        if action == "start_workload_s3_backups_workload":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                stability_cluster._clean_and_start_workload(
                    node,
                    's3_backups_workload',
                    '/Berkanavt/nemesis/bin/s3_backups_workload --database /Root/db1'
                )
            stability_cluster.get_state()
        if action == "stop_workloads":
            stability_cluster.stop_workloads_all_nodes()
            stability_cluster.get_state()

        if action == "stop_nemesis":
            stability_cluster.stop_nemesis()
            stability_cluster.get_state()

        if action == "start_nemesis":
            stability_cluster.start_nemesis()
            stability_cluster.get_state()

        if action == "restore_cluster":
            stability_cluster.restore_cluster()
            stability_cluster.get_state()

        if action == "perform_checks":
            stability_cluster.perform_checks()

        if action == "get_workload_outputs":
            stability_cluster.get_workload_outputs(mode=args.mode, last_n_lines=args.last_n_lines)


if __name__ == "__main__":
    main()
