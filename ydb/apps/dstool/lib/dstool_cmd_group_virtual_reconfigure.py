import ydb.apps.dstool.lib.common as common
from argparse import FileType
import google.protobuf.json_format as pb_json

description = 'Reconfigure a virtual group backed by BlobDepot'


def add_options(p):
    p.add_argument('--name', type=str, required=True, help='name of virtual group that requires reconfiguring')
    p.add_argument('--confirm-s3-route-change', action='store_true',
                   help='confirm that the new S3 route accesses the same objects, including writes already in flight')
    p.add_argument('--confirm-s3-namespace-migration', action='store_true',
                   help='confirm an offline S3 migration: old writes have been fenced and drained, and referenced objects '
                        'have been copied with their locator suffixes to a dedicated empty destination. To disable S3, '
                        'supply empty S3 settings after verifying that no S3 references remain. Keep the source readable '
                        'and writes quiesced until the tablet and agents use the new configuration')
    p.add_argument('--migrated-s3-name', metavar='NAME',
                   help='replacement physical suffix for a malformed legacy S3 name during a confirmed namespace migration')
    p.add_argument('--s3-settings', type=FileType('r', encoding='utf-8'), metavar='JSON_FILE', help='path to JSON file containing S3 settings')


def do(args):
    request = common.create_bsc_request(args)

    if args.s3_settings:
        s3_settings = args.s3_settings.read()
    else:
        s3_settings = None

    cmd = request.Command.add().ReconfigureVirtualGroup
    cmd.Name = args.name
    cmd.ConfirmS3RouteChange = args.confirm_s3_route_change
    cmd.ConfirmS3NamespaceMigration = args.confirm_s3_namespace_migration
    if args.migrated_s3_name is not None:
        cmd.MigratedS3Name = args.migrated_s3_name

    if s3_settings is not None:
        pb_json.Parse(s3_settings, cmd.S3BackendSettings)

    response = common.invoke_bsc_request(request)
    common.print_request_result(args, request, response)
