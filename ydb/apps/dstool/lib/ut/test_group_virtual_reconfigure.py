import argparse
from unittest.mock import patch

import pytest

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
