# -*- coding: utf-8 -*-

import getpass
import os
import logging
import tempfile
import stat
import sys
import argparse

from library.python import resource

logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))

from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster # noqa
from ydb.tests.library.wardens.factories import safety_warden_factory, liveness_warden_factory # noqa

logger = logging.getLogger('ydb.connection')
logger.setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)


STRESS_BINARIES_DEPLOY_PATH = '/Berkanavt/nemesis/bin/'


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

    def perform_checks(self):
        safety_violations = safety_warden_factory(self.kikimr_cluster, self.ssh_username).list_of_safety_violations()
        liveness_violations = liveness_warden_factory(self.kikimr_cluster, self.ssh_username).list_of_liveness_violations

        count = 0
        report = []

        print("SAFETY WARDEN (total: {})".format(len(safety_violations)))
        for i, violation in enumerate(safety_violations):
            print("[{}]".format(i))
            print(violation)
            print()

        print("LIVENESS WARDEN (total: {})".format(len(liveness_violations)))
        for i, violation in enumerate(liveness_violations):
            print("[{}]".format(i))
            print(violation)

            print()
        return count, "\n".join(report)

    def start_nemesis(self):
        for node in self.kikimr_cluster.nodes.values():
            node.ssh_command("sudo service nemesis restart", raise_on_error=True)

    def stop_nemesis(self):
        for node in self.kikimr_cluster.nodes.values():
            node.ssh_command("sudo service nemesis stop", raise_on_error=False)

    def deploy_ydb(self):
        self._stop_nemesis()
        self.kikimr_cluster.start()

        # cleanup nemesis logs
        for node in self.kikimr_cluster.nodes.values():
            node.ssh_command('sudo rm -rf /Berkanavt/nemesis/logs/*', raise_on_error=False)
            node.ssh_command('sudo pkill screen', raise_on_error=False)

        with open(self._unpack_resource("tbl_profile.txt")) as f:
            self.kikimr_cluster.client.console_request(f.read())

        self.kikimr_cluster.client.update_self_heal(True)

        node = list(self.kikimr_cluster.nodes.values())[0]
        node.ssh_command("/Berkanavt/kikimr/bin/kikimr admin console validator disable bootstrap", raise_on_error=True)

        for node in self.kikimr_cluster.nodes.values():
            node.ssh_command(["sudo", "mkdir", "-p", STRESS_BINARIES_DEPLOY_PATH], raise_on_error=False)
            for artifact in self.artifacts:
                node.copy_file_or_dir(
                    artifact,
                    os.path.join(
                        STRESS_BINARIES_DEPLOY_PATH,
                        os.path.basename(
                            artifact
                        )
                    )
                )

    def deploy_tools(self):
        for node in self.kikimr_cluster.nodes.values():
            node.ssh_command(["sudo", "mkdir", "-p", STRESS_BINARIES_DEPLOY_PATH], raise_on_error=False)
            for artifact in self.artifacts:
                node.copy_file_or_dir(
                    artifact,
                    os.path.join(
                        STRESS_BINARIES_DEPLOY_PATH,
                        os.path.basename(
                            artifact
                        )
                    )
                )


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
            "deploy_ydb",
            "deploy_tools",
            "start_nemesis",
            "stop_nemesis",
            "start_workload_simple_queue_row",
            "start_workload_simple_queue_column",
            "start_workload_olap_workload",
            "stop_workload",
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
    )

    for action in args.actions:
        if action == "deploy_ydb":
            stability_cluster.deploy_ydb()
        if action == "deploy_tools":
            stability_cluster.deploy_tools()
        if action == "start_workload_simple_queue_row":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                node.ssh_command(
                    'screen -d -m bash -c "while true; do /Berkanavt/nemesis/bin/simple_queue --database /Root/db1 --mode row; done"',
                    raise_on_error=True
                )
        if action == "start_workload_simple_queue_column":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                node.ssh_command(
                    'screen -d -m bash -c "while true; do /Berkanavt/nemesis/bin/simple_queue --database /Root/db1 --mode column; done"',
                    raise_on_error=True
                )
        if action == "start_workload_olap_workload":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                node.ssh_command(
                    'screen -d -m bash -c "while true; do /Berkanavt/nemesis/bin/olap_workload --database /Root/db1; done"',
                    raise_on_error=True
                )
        if action == "stop_workload":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                node.ssh_command(
                    'sudo pkill screen',
                    raise_on_error=True
                )

        if action == "stop_nemesis":
            stability_cluster.stop_nemesis()

        if action == "start_nemesis":
            stability_cluster.start_nemesis()

        if action == "perform_checks":
            count, report = stability_cluster.perform_checks()
            print(report)


if __name__ == "__main__":
    main()
