# -*- coding: utf-8 -*-

import getpass
import os
import logging
import tempfile
import stat
import sys
import argparse
import re

from library.python import resource

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster # noqa
from ydb.tests.library.wardens.factories import safety_warden_factory, liveness_warden_factory # noqa

logger = logging.getLogger('ydb.connection')
logger.setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)


STRESS_BINARIES_DEPLOY_PATH = '/Berkanavt/nemesis/bin/'

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
            if ps aux | grep "/Berkanavt/nemesis/bin/olap_workload" | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi""",
        'start_command' : "do /Berkanavt/nemesis/bin/olap_workload --database /Root/db1",
        'stop_command' : ""
    },
    'simple_queue_column' : {
        'status' : """
            if ps aux | grep "/Berkanavt/nemesis/bin/simple" | grep column | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi""",
        'start_command' : "/Berkanavt/nemesis/bin/simple_queue --database /Root/db1 --mode column",
        'stop_command' : ""
    },
    'simple_queue_row' : {
        'status' : """
            if ps aux | grep "/Berkanavt/nemesis/bin/simple" | grep row | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi""",
        'start_command' : "/Berkanavt/nemesis/bin/simple_queue --database /Root/db1 --mode row",
        'stop_command' : ""
    },
    'workload_log_column' : {
        'status' : """
            if ps aux | grep "/Berkanavt/nemesis/bin/ydb_cli" | grep column | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi""",
        'start_command' : "",
        'stop_command' : ""
    },
    'workload_log_row' : {
        'status' : """
            if ps aux | grep "/Berkanavt/nemesis/bin/ydb_cli" | grep row | grep -v grep > /dev/null; then
                echo "Running"
            else
                echo "Stopped"
            fi""",
        'start_command' : "",
        'stop_command' : ""
    }
}


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
    def __init__(self, ssh_username, cluster_path, ydbd_path, ydbd_next_path=None):
        self.working_dir = os.path.join(tempfile.gettempdir(), "ydb_stability")
        os.makedirs(self.working_dir, exist_ok=True)
        self.ssh_username = ssh_username
        self.slice_directory = cluster_path
        self.ydbd_path = ydbd_path
        self.ydbd_next_path = ydbd_next_path

        self.artifacts = (
            self._unpack_resource('nemesis'),
            self._unpack_resource('simple_queue'),
            self._unpack_resource('olap_workload'),
            self._unpack_resource('statistics_workload'),
            self._unpack_resource('ydb_cli'),
        )

        self.kikimr_cluster = ExternalKiKiMRCluster(
            config_path=self.slice_directory,
            kikimr_configure_binary_path=self._unpack_resource("cfg"),
            kikimr_path=self.ydbd_path,
            kikimr_next_path=self.ydbd_next_path,
            ssh_username=self.ssh_username,
            deploy_cluster=True,
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
            match_verify = re.search(r'VERIFY|FAIL|signal 11|signal 6|signal 15|uncaught exception', line)
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

    def get_all_errors(self):
        logging.getLogger().setLevel(logging.WARNING)
        all_results = []
        for node in self.kikimr_cluster.nodes.values():
            result = node.ssh_command("""
                    ls -ltr /Berkanavt/kikimr*/logs/kikimr* |
                    awk '{print $NF}' |
                    while read file; do
                    case "$file" in
                        *.txt) cat "$file" ;;
                        *.gz) zcat "$file" ;;
                        *) cat "$file" ;;
                    esac
                    done |
                    grep -E 'VERIFY|FAIL|signal 11|signal 6|signal 15|uncaught exception' -A 20
                                    """, raise_on_error=False)
            if result:
                all_results.append(result.decode('utf-8'))
        all_results = self.process_lines(all_results)
        return all_results

    def get_errors(self):
        errors = self.get_all_errors()
        unique_traces = self.find_unique_traces_with_counts(errors)
        for trace in unique_traces:
            print(f"Trace (Occurrences: {len(unique_traces[trace])}):\n{trace}\n{'-'*60}")

    def perform_checks(self):

        safety_violations = safety_warden_factory(self.kikimr_cluster, self.ssh_username, lines_after=20, cut=False).list_of_safety_violations()
        liveness_violations = liveness_warden_factory(self.kikimr_cluster, self.ssh_username).list_of_liveness_violations
        coredumps_search_results = {}
        for node in self.kikimr_cluster.nodes.values():
            result = node.ssh_command('find /coredumps/ -type f | wc -l', raise_on_error=False)
            coredumps_search_results[node.host.split(':')[0]] = int(result.decode('utf-8'))

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

    def start_nemesis(self):
        for node in self.kikimr_cluster.nodes.values():
            node.ssh_command(DICT_OF_SERVICES['nemesis']['start_command'], raise_on_error=True)

    def stop_workloads(self):
        for node in self.kikimr_cluster.nodes.values():
            node.ssh_command(
                'sudo pkill screen',
                raise_on_error=True
            )

    def stop_nemesis(self):
        for node in self.kikimr_cluster.nodes.values():
            node.ssh_command(DICT_OF_SERVICES['nemesis']['stop_command'], raise_on_error=False)

    def get_state(self):
        logging.getLogger().setLevel(logging.WARNING)
        state_objects_dic = dict(list(DICT_OF_SERVICES.items()) + list(DICT_OF_PROCESSES.items()))
        for node_id, node in enumerate(self.kikimr_cluster.nodes.values()):
            node_host = node.host.split(':')[0]
            print(f'{bcolors.BOLD}{node_host}{bcolors.ENDC}:')
            for state_object in state_objects_dic:
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

    def cleanup(self, mode='all'):
        if mode in ['all', 'logs']:
            self.kikimr_cluster.cleanup_logs()
        for node in self.kikimr_cluster.nodes.values():
            if mode in ['all', 'dumps']:
                node.ssh_command('sudo rm -rf /coredumps/*', raise_on_error=False)
            if mode in ['all', 'logs']:
                node.ssh_command('sudo rm -rf /Berkanavt/nemesis/log/*', raise_on_error=False)
            if mode == 'all':
                self.stop_nemesis()
                node.ssh_command('sudo pkill screen', raise_on_error=False)
                node.ssh_command('sudo rm -rf /Berkanavt/kikimr/bin/*', raise_on_error=False)

    def deploy_ydb(self):
        self.cleanup()
        self.kikimr_cluster.start()

        with open(self._unpack_resource("tbl_profile.txt")) as f:
            self.kikimr_cluster.client.console_request(f.read())

        self.kikimr_cluster.client.update_self_heal(True)

        node = list(self.kikimr_cluster.nodes.values())[0]
        node.ssh_command("/Berkanavt/kikimr/bin/kikimr admin console validator disable bootstrap", raise_on_error=True)

        self.deploy_tools()

    def deploy_tools(self):
        for node in self.kikimr_cluster.nodes.values():
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


def path_type(path):
    # Expand the user's home directory if ~ is present
    expanded_path = os.path.expanduser(path)
    # Check if the file exists
    if not os.path.exists(expanded_path):
        raise argparse.ArgumentTypeError(f"The file {expanded_path} does not exist.")
    return expanded_path


def parse_args():
    parser = argparse.ArgumentParser(
        description="""Chaos and cross-version testing tool"""
    )
    parser.add_argument(
        "--cluster_path",
        required=True,
        type=path_type,
        help="Path to cluster.yaml",
    )
    parser.add_argument(
        "--ydbd_path",
        required=True,
        type=path_type,
        help="Path to ydbd",
    )
    parser.add_argument(
        "--next_ydbd_path",
        required=False,
        type=path_type,
        help="Path to next ydbd version",
    )
    parser.add_argument(
        "--ssh_user",
        required=False,
        default=getpass.getuser(),
        type=str,
        help="Path to ydbd",
    )
    parser.add_argument(
        "actions",
        type=str,
        nargs="+",
        choices=[
            "get_errors",
            "get_state",
            "cleanup",
            "cleanup_logs",
            "cleanup_dumps",
            "deploy_ydb",
            "deploy_tools",
            "start_nemesis",
            "stop_nemesis",
            "start_all_workloads",
            "start_workload_simple_queue_row",
            "start_workload_simple_queue_column",
            "start_workload_olap_workload",
            "start_workload_log",
            "start_workload_log_column",
            "start_workload_log_row",
            "stop_workloads",
            "perform_checks",
        ],
        help="actions to execute",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    ssh_username = args.ssh_user
    stability_cluster = StabilityCluster(
        ssh_username=ssh_username,
        cluster_path=args.cluster_path,
        ydbd_path=args.ydbd_path,
        ydbd_next_path=args.next_ydbd_path,
    )

    for action in args.actions:
        if action == "get_errors":
            stability_cluster.get_errors()
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
        if action == "start_all_workloads":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                node.ssh_command(
                    'screen -s simple_queue_row -d -m bash -c "while true; do /Berkanavt/nemesis/bin/simple_queue --database /Root/db1 --mode row; done"',
                    raise_on_error=True
                )
                node.ssh_command(
                    'screen -s simple_queue_column -d -m bash -c "while true; do /Berkanavt/nemesis/bin/simple_queue --database /Root/db1 --mode column; done"',
                    raise_on_error=True
                )
                node.ssh_command(
                    'screen -s olap_workload -d -m bash -c "while true; do /Berkanavt/nemesis/bin/olap_workload --database /Root/db1; done"',
                    raise_on_error=True
                )
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
                    'workload', 'log', 'clean',
                    '--path', f'log_workload_{store_type}',
                    ],
                    raise_on_error=True
                )
                first_node.ssh_command([
                    '/Berkanavt/nemesis/bin/ydb_cli',
                    '--endpoint', f'grpc://localhost:{first_node.grpc_port}',
                    '--database', '/Root/db1',
                    'workload', 'log', 'init',
                    '--len', '1000',
                    '--int-cols', '20',
                    '--key-cols', '20',
                    '--min-partitions', '100',
                    '--partition-size', '10',
                    '--auto-partition', '0',
                    '--store', store_type,
                    '--path', f'log_workload_{store_type}',
                    '--ttl', '3600'
                    ],
                    raise_on_error=True
                )
                for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                    node.ssh_command([
                        f'screen -s workload_log_{store_type} -d -m bash -c "while true; do',
                        '/Berkanavt/nemesis/bin/ydb_cli',
                        '--endpoint', f'grpc://localhost:{node.grpc_port}',
                        '--database', '/Root/db1',
                        'workload', 'log', 'run', 'bulk_upsert',
                        '--len', '1000',
                        '--int-cols', '20',
                        '--key-cols', '20',
                        '--threads', '20',
                        '--timestamp_deviation', '180',
                        '--seconds', '86400',
                        '--path', f'log_workload_{store_type}',
                        '; done"'
                        ],
                        raise_on_error=True
                    )
            stability_cluster.get_state()
        if action == "start_workload_simple_queue_row":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                node.ssh_command(
                    'screen -s simple_queue_row -d -m bash -c "while true; do /Berkanavt/nemesis/bin/simple_queue --database /Root/db1 --mode row; done"',
                    raise_on_error=True
                )
            stability_cluster.get_state()
        if action == "start_workload_simple_queue_column":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                node.ssh_command(
                    'screen -s simple_queue_column -d -m bash -c "while true; do /Berkanavt/nemesis/bin/simple_queue --database /Root/db1 --mode column; done"',
                    raise_on_error=True
                )
            stability_cluster.get_state()
        if action == "start_workload_olap_workload":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                node.ssh_command(
                    'screen -s olap_workload -d -m bash -c "while true; do /Berkanavt/nemesis/bin/olap_workload --database /Root/db1; done"',
                    raise_on_error=True
                )

        if action == "stop_workloads":
            stability_cluster.stop_workloads()
            stability_cluster.get_state()

        if action == "stop_nemesis":
            stability_cluster.stop_nemesis()
            stability_cluster.get_state()

        if action == "start_nemesis":
            stability_cluster.start_nemesis()
            stability_cluster.get_state()

        if action == "perform_checks":
            stability_cluster.perform_checks()


if __name__ == "__main__":
    main()
