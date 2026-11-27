import argparse
from unittest.mock import patch

import pytest

from ydb.apps.dstool.lib import dstool_cmd_group_virtual_reconfigure as reconfigure


@pytest.mark.parametrize('with_settings', [False, True])
@pytest.mark.parametrize('confirm_route_change', [False, True])
def test_empty_s3_settings_on_wire(tmp_path, with_settings, confirm_route_change):
    parser = argparse.ArgumentParser()
    reconfigure.add_options(parser)
    options = ['--name', 'vg']
    if confirm_route_change:
        options.append('--confirm-s3-route-change')
    if with_settings:
        settings = tmp_path / 'empty.json'
        settings.write_text('{}')
        options.extend(['--s3-settings', str(settings)])
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
    assert cmd.HasField('S3BackendSettings') == with_settings
    assert cmd.ConfirmS3RouteChange == confirm_route_change
