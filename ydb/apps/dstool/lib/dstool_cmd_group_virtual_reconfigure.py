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
                        'have been copied with their locator suffixes to a dedicated empty destination. Keep the source readable '
                        'and writes quiesced until the tablet and agents use the new configuration')
    p.add_argument('--confirm-s3-empty', action='store_true',
                   help='remove the S3 route with empty S3 settings after quiescing writers, fencing and draining '
                        'outstanding writes, and verifying that no values reference S3 and no S3 deletes remain')
    p.add_argument('--migrated-s3-name', metavar='NAME',
                   help='physical suffix on first S3 assignment or when correcting a malformed legacy S3 name '
                        'with --confirm-s3-namespace-migration')
    p.add_argument('--s3-settings', type=FileType('r', encoding='utf-8'), metavar='JSON_FILE',
                   help='path to JSON file containing S3 settings. Omit to validate the stored namespace')


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
    cmd.ConfirmS3Empty = args.confirm_s3_empty
    if args.migrated_s3_name is not None:
        cmd.MigratedS3Name = args.migrated_s3_name

    if s3_settings is not None:
        pb_json.Parse(s3_settings, cmd.S3BackendSettings)
        # An explicit {} removes the route. Parsing it alone leaves the protobuf field absent.
        cmd.S3BackendSettings.SetInParent()

    response = common.invoke_bsc_request(request)
    common.print_request_result(args, request, response)
