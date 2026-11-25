import argparse
from unittest.mock import patch

import pytest

from ydb.apps.dstool.lib import dstool_cmd_group_virtual_cleanup as cleanup
from ydb.apps.dstool.lib import dstool_cmd_group_virtual_reconfigure as reconfigure


@pytest.mark.parametrize('confirmation', [None, '--confirm-s3-empty', '--confirm-s3-namespace-migration'])
def test_empty_s3_settings_on_wire(tmp_path, confirmation):
    parser = argparse.ArgumentParser()
    reconfigure.add_options(parser)
    options = ['--name', 'vg']
    if confirmation:
        settings = tmp_path / 'empty.json'
        settings.write_text('{}')
        options.extend([confirmation, '--s3-settings', str(settings)])
    args = parser.parse_args(options)
    args.dry_run = True
    with (
        patch.object(reconfigure.common, 'invoke_bsc_request') as invoke,
        patch.object(reconfigure.common, 'print_request_result'),
    ):
        reconfigure.do(args)
    if args.s3_settings:
        args.s3_settings.close()
    request = invoke.call_args.args[0]
    cmd = request.FromString(request.SerializeToString()).Command[0].ReconfigureVirtualGroup
    assert cmd.HasField('S3BackendSettings') == bool(confirmation)
    assert cmd.ConfirmS3Empty == (confirmation == '--confirm-s3-empty')
    assert cmd.ConfirmS3NamespaceMigration == (confirmation == '--confirm-s3-namespace-migration')


@pytest.mark.parametrize(('option', 'field'), [
    ('--credentials', 'Credentials'),
    ('--confirm-exclusive-ownership', 'ConfirmExclusiveOwnership'),
    ('--confirm-cleanup-complete', 'ConfirmCleanupComplete'),
])
def test_retired_s3_cleanup_request(tmp_path, option, field):
    parser = argparse.ArgumentParser()
    cleanup.add_options(parser)
    options = ['--group-id', '100', option]
    if field == 'Credentials':
        keys = tmp_path / 'keys.json'
        keys.write_text('{"AccessKey":"test-key","SecretKey":"test-secret"}')
        options.append(str(keys))
    args = parser.parse_args(options)
    args.dry_run = True
    with (
        patch.object(cleanup.common, 'invoke_bsc_request') as invoke,
        patch.object(cleanup.common, 'print_request_result', autospec=True) as report,
    ):
        cleanup.do(args)
    if args.credentials:
        args.credentials.close()
    request = invoke.call_args.args[0]
    report.assert_called_once_with(args, request, invoke.return_value)
    cmd = request.Command[0].ManageRetiredS3Namespace
    assert cmd.GroupId == 100
    assert cmd.WhichOneof('Action') == field
    if field == 'Credentials':
        safe = cleanup.common._bsc_request_for_display(request)
        assert safe.Command[0].ManageRetiredS3Namespace.HasField('Credentials')
        assert 'test-secret' not in str(safe)
        assert 'test-key' not in str(safe)
        assert cmd.Credentials.SecretKey == 'test-secret'
