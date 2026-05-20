import os
import tempfile
import unittest
from unittest import mock

import ydb.apps.dstool.lib.arg_parser as argparse

from ydb.tools.mnc.cli import command_options
from ydb.tools.mnc.lib import common


def make_parser():
    parser = argparse.ArgumentParser(description='test parser')
    parser.add_argument('--verbose', '-V', dest='verbose', action='store_true', default=False)
    subparsers = parser.add_subparsers(dest='verb', required=True)

    qemu_parser = subparsers.add_parser('qemu')
    qemu_subparsers = qemu_parser.add_subparsers(dest='cmd', required=True)

    run_parser = qemu_subparsers.add_parser('run')
    common.add_common_options(run_parser)
    run_parser.add_argument('--host', required=True)
    run_parser.add_argument('--disk-id', required=True)
    run_parser.add_argument('--no-start-endpoint', action='store_true', default=False)
    run_parser.add_argument('--ssh-port', type=int, default=8679)

    service_parser = subparsers.add_parser('service')
    service_subparsers = service_parser.add_subparsers(dest='cmd', required=True)
    hosts_parser = service_subparsers.add_parser('hosts')
    common.add_common_options(hosts_parser)
    hosts_parser.add_argument('operation', choices=('start', 'stop'))
    hosts_parser.add_argument('--node-type', dest='node_type', default=None)

    return parser


class CommandOptionsTest(unittest.TestCase):
    def test_save_command_options_writes_effective_leaf_options(self):
        parser = make_parser()
        with tempfile.TemporaryDirectory() as home:
            args = parser.parse_args([
                'qemu',
                'run',
                '--config',
                'cfg1',
                '--host',
                'host1',
                '--disk-id',
                'disk1',
                '--no-start-endpoint',
            ])

            with mock.patch.dict(os.environ, {'HOME': home}):
                command_options.save_command_options(parser, args)
                data = command_options.load_cache()

        self.assertEqual(
            data['qemu/run']['tokens'],
            [
                '--config',
                'cfg1',
                '--host',
                'host1',
                '--disk-id',
                'disk1',
                '--no-start-endpoint',
                '--ssh-port',
                '8679',
            ],
        )

    def test_save_cache_uses_restrictive_permissions(self):
        with tempfile.TemporaryDirectory() as home:
            with mock.patch.dict(os.environ, {'HOME': home}):
                command_options.save_cache({'qemu/run': {'tokens': ['--host', 'host1']}})
                path = command_options.cache_path()

            cache_dir = os.path.dirname(path)
            mnc_dir = os.path.dirname(cache_dir)
            self.assertEqual(os.stat(mnc_dir).st_mode & 0o777, 0o700)
            self.assertEqual(os.stat(cache_dir).st_mode & 0o777, 0o700)
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)

    def test_cache_is_disabled_without_home(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(command_options.cache_path())
            command_options.save_cache({'qemu/run': {'tokens': ['--host', 'host1']}})
            self.assertEqual(command_options.load_cache(), {})

    def test_apply_cached_options_injects_missing_required_options(self):
        parser = make_parser()
        with tempfile.TemporaryDirectory() as home:
            with mock.patch.dict(os.environ, {'HOME': home}):
                command_options.save_cache({
                    'qemu/run': {
                        'tokens': [
                            '--config',
                            'cfg1',
                            '--host',
                            'host1',
                            '--disk-id',
                            'old-disk',
                            '--no-start-endpoint',
                        ],
                    },
                })

                argv = command_options.apply_cached_options(parser, ['qemu', 'run', '--disk-id', 'new-disk'])

        args = parser.parse_args(argv)

        self.assertEqual(args.config_name, 'cfg1')
        self.assertEqual(args.host, 'host1')
        self.assertEqual(args.disk_id, 'new-disk')
        self.assertTrue(args.no_start_endpoint)
        self.assertNotIn('old-disk', argv)

    def test_global_options_are_not_cached(self):
        parser = make_parser()
        args = parser.parse_args([
            '--verbose',
            'qemu',
            'run',
            '--config',
            'cfg1',
            '--host',
            'host1',
            '--disk-id',
            'disk1',
        ])

        tokens = command_options.tokens_from_namespace(
            command_options.command_from_namespace(parser, args)[1],
            args,
        )

        self.assertNotIn('--verbose', tokens)

    def test_position_arguments_are_cached_and_can_be_overridden(self):
        parser = make_parser()
        with tempfile.TemporaryDirectory() as home:
            args = parser.parse_args(['service', 'hosts', '--config', 'cfg1', 'start', '--node-type', 'dynamic'])
            with mock.patch.dict(os.environ, {'HOME': home}):
                command_options.save_command_options(parser, args)
                argv = command_options.apply_cached_options(parser, ['service', 'hosts', 'stop'])

        self.assertEqual(argv, ['service', 'hosts', '--config', 'cfg1', '--node-type', 'dynamic', 'stop'])
        parsed = make_parser().parse_args(argv)

        self.assertEqual(parsed.config_name, 'cfg1')
        self.assertEqual(parsed.operation, 'stop')
        self.assertEqual(parsed.node_type, 'dynamic')
        self.assertNotIn('start', argv)


if __name__ == '__main__':
    unittest.main()
