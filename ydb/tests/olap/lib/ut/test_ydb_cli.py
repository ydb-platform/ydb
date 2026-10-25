import os
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from ydb.tests.olap.lib import remote_execution
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class TestDeployCli(unittest.TestCase):
    def test_local_cli_is_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            cli_path = os.path.join(directory, 'local cli')
            with open(cli_path, 'w') as cli:
                cli.write('#!/bin/sh\nprintf "local cli\\n"\n')
            os.chmod(cli_path, 0o755)
            target_dir = os.path.join(directory, 'deployment')

            with patch.object(YdbCliHelper, 'get_cli_path', return_value=cli_path), \
                    patch.object(YdbCluster, 'get_client_host', return_value='localhost'):
                for host in ('', 'localhost', '127.0.0.1', '::1'):
                    with self.subTest(host=host):
                        deployed_path = YdbCliHelper.deploy_remote_cli(host, target_dir)
                        self.assertEqual(deployed_path, cli_path)
                        self.assertEqual(subprocess.check_output([deployed_path]), b'local cli\n')
                        self.assertFalse(os.path.exists(target_dir))

    def test_remote_cli_is_deployed(self):
        with patch.object(YdbCliHelper, 'get_cli_path', return_value='/build/ydb'), \
                patch.object(remote_execution, 'is_localhost', return_value=False), \
                patch.object(remote_execution, 'get_remote_tmp_path', return_value='/tmp/cli'), \
                patch.object(remote_execution, 'deploy_binary', return_value={'success': True}) as deploy:
            self.assertEqual(YdbCliHelper.deploy_remote_cli('remote-host'), '/tmp/cli/ydb')
            deploy.assert_called_once_with('/build/ydb', 'remote-host', '/tmp/cli')

    def test_remote_cli_uses_requested_directory(self):
        with patch.object(YdbCliHelper, 'get_cli_path', return_value='/build/custom-cli'), \
                patch.object(remote_execution, 'is_localhost', return_value=False), \
                patch.object(remote_execution, 'deploy_binary', return_value={'success': True}) as deploy:
            self.assertEqual(
                YdbCliHelper.deploy_remote_cli('remote-host', '/tmp/upload'), '/tmp/upload/custom-cli')
            deploy.assert_called_once_with('/build/custom-cli', 'remote-host', '/tmp/upload')

    def test_remote_deployment_failure_is_reported(self):
        with patch.object(YdbCliHelper, 'get_cli_path', return_value='/build/ydb'), \
                patch.object(remote_execution, 'is_localhost', return_value=False), \
                patch.object(remote_execution, 'deploy_binary', return_value={
                    'success': False, 'error': 'copy failed',
                }):
            with self.assertRaisesRegex(AssertionError, 'copy failed'):
                YdbCliHelper.deploy_remote_cli('remote-host', '/tmp/upload')
