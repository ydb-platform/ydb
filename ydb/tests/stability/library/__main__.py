# -*- coding: utf-8 -*-

import getpass
import os
import logging
import tempfile
import stat
import sys
import argparse

import yatest
from library.python import resource

logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))

from ydb.tests.library.common.composite_assert import CompositeAssert # noqa
from ydb.tests.library.harness.kikimr_cluster import ExternalKiKiMRCluster # noqa
from ydb.tests.library.matchers.collection import is_empty # noqa
from ydb.tests.library.wardens.factories import safety_warden_factory, liveness_warden_factory # noqa

logger = logging.getLogger('ydb.connection')
logger.setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)


def get_slice_directory():
    return os.getenv('YDB_CLUSTER_YAML_PATH')


def get_ssh_username():
    return yatest.common.get_param("kikimr.ci.ssh_username")


def get_slice_name():
    return yatest.common.get_param("kikimr.ci.cluster_name", None)


def next_version_kikimr_driver_path():
    return yatest.common.get_param("kikimr.ci.kikimr_driver_next", None)


def kikimr_driver_path():
    return yatest.common.get_param("kikimr.ci.kikimr_driver", None)


def is_deploy_cluster():
    return yatest.common.get_param("kikimr.ci.deploy_cluster", "false").lower() == "true"


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

    def start_nemesis(self):
        for node in self.kikimr_cluster.nodes.values():
            node.ssh_command("sudo service nemesis restart", raise_on_error=True)

    def stop_nemesis(self):
        for node in self.kikimr_cluster.nodes.values():
            node.ssh_command("sudo service nemesis stop", raise_on_error=False)

    def setup(self):
        self._stop_nemesis()
        self.kikimr_cluster.start()

        if is_deploy_cluster:
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
            "start_nemesis",
            "stop_nemesis",
            "start_workload_simple_queue",
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
        setup_command = False
        if setup_command:
            stability_cluster.setup()

        if action == "start_workload_simple_queue":
            for node_id, node in enumerate(stability_cluster.kikimr_cluster.nodes.values()):
                node.ssh_command(
                    'screen -d -m bash -c "while true; do /Berkanavt/nemesis/bin/simple_queue --database /Root/db1 ; done"',
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
            composite_assert = CompositeAssert()
            composite_assert.assert_that(
                safety_warden_factory(stability_cluster.kikimr_cluster, ssh_username).list_of_safety_violations(),
                is_empty(),
                "No safety violations by Safety Warden"
            )

            composite_assert.assert_that(
                liveness_warden_factory(stability_cluster.kikimr_cluster, ssh_username).list_of_liveness_violations,
                is_empty(),
                "No liveness violations by liveness warden",
            )

            composite_assert.finish()


if __name__ == "__main__":
    main()
