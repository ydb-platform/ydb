import os
import unittest
from unittest.mock import patch

from ydb.tests.olap.lib import ydb_cli
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper


class TestDeployRemoteCli(unittest.TestCase):
    def setUp(self):
        self.cli_path = '/build directory/custom-ydb'
        cli_patch = patch.object(YdbCliHelper, 'get_cli_path', return_value=self.cli_path)
        self.get_cli_path = cli_patch.start()
        self.addCleanup(cli_patch.stop)
        remote_patch = patch.object(ydb_cli, 'remote_execution')
        self.remote = remote_patch.start()
        self.addCleanup(remote_patch.stop)
        self.remote.is_localhost.return_value = False
        self.remote.get_remote_tmp_path.return_value = '/tmp/ydb_cli/deployment'
        self.remote.deploy_binary.return_value = {'success': True}
        client_patch = patch.object(ydb_cli.YdbCluster, 'get_client_host', return_value='client-host')
        self.client_host = client_patch.start()
        self.addCleanup(client_patch.stop)
        time_patch = patch.object(ydb_cli, 'time', return_value=123.5)
        time_patch.start()
        self.addCleanup(time_patch.stop)

    def test_local_host_reuses_cli_without_deployment(self):
        self.remote.is_localhost.return_value = True

        self.assertEqual(YdbCliHelper.deploy_remote_cli('localhost'), self.cli_path)

        self.remote.is_localhost.assert_called_once_with('localhost')
        self.remote.get_remote_tmp_path.assert_not_called()
        self.remote.deploy_binary.assert_not_called()
        self.client_host.assert_not_called()

    def test_local_host_with_target_dir_still_reuses_cli(self):
        self.remote.is_localhost.return_value = True

        self.assertEqual(YdbCliHelper.deploy_remote_cli('localhost', '/tmp/upload'), self.cli_path)

        self.remote.deploy_binary.assert_not_called()
        self.remote.get_remote_tmp_path.assert_not_called()

    def test_local_relative_path_is_safe_for_shell_execution(self):
        self.get_cli_path.return_value = 'relative directory/custom-ydb'
        self.remote.is_localhost.return_value = True

        result = YdbCliHelper.deploy_remote_cli('localhost')

        self.assertTrue(os.path.isabs(result))
        self.assertEqual(result, os.path.join(os.getcwd(), 'relative directory/custom-ydb'))
        self.remote.deploy_binary.assert_not_called()

    def test_default_host_uses_cluster_client(self):
        self.remote.is_localhost.return_value = True

        self.assertEqual(YdbCliHelper.deploy_remote_cli(), self.cli_path)

        self.client_host.assert_called_once_with()
        self.remote.is_localhost.assert_called_once_with('client-host')
        self.remote.deploy_binary.assert_not_called()

    def test_remote_host_deploys_to_generated_directory(self):
        result = YdbCliHelper.deploy_remote_cli('remote-host')

        self.remote.get_remote_tmp_path.assert_called_once_with('remote-host', 'ydb_cli', '123.5')
        self.remote.deploy_binary.assert_called_once_with(self.cli_path, 'remote-host', '/tmp/ydb_cli/deployment')
        self.assertEqual(result, '/tmp/ydb_cli/deployment/custom-ydb')

    def test_remote_host_preserves_requested_directory_and_binary_name(self):
        target_dir = '/tmp/upload directory'

        result = YdbCliHelper.deploy_remote_cli('remote-host', target_dir)

        self.remote.get_remote_tmp_path.assert_not_called()
        self.remote.deploy_binary.assert_called_once_with(self.cli_path, 'remote-host', target_dir)
        self.assertEqual(result, os.path.join(target_dir, 'custom-ydb'))

    def test_remote_deployment_failure_is_reported(self):
        self.remote.deploy_binary.return_value = {'success': False, 'error': 'deployment failed'}

        with self.assertRaisesRegex(AssertionError, 'deployment failed'):
            YdbCliHelper.deploy_remote_cli('remote-host')
