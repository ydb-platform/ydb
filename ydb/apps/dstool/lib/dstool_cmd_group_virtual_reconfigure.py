import ydb.apps.dstool.lib.common as common
from argparse import FileType
import google.protobuf.json_format as pb_json

description = 'Reconfigure a virtual group backed by BlobDepot'


def add_options(p):
    p.add_argument('--name', type=str, required=True, help='name of virtual group that requires reconfiguring')
    p.add_argument('--confirm-s3-route-change', action='store_true',
                   help='confirm that the new S3 route accesses the same objects, including writes already in flight')
    p.add_argument('--s3-settings', type=FileType('r', encoding='utf-8'), metavar='JSON_FILE',
                   help='path to JSON file containing S3 settings')


def do(args):
    request = common.create_bsc_request(args)

    if args.s3_settings:
        s3_settings = args.s3_settings.read()
    else:
        s3_settings = None

    cmd = request.Command.add().ReconfigureVirtualGroup
    cmd.Name = args.name
    cmd.ConfirmS3RouteChange = args.confirm_s3_route_change

    if s3_settings is not None:
        pb_json.Parse(s3_settings, cmd.S3BackendSettings)
        # An explicit {} removes the route. Parsing it alone leaves the protobuf field absent.
        cmd.S3BackendSettings.SetInParent()

    response = common.invoke_bsc_request(request)
    common.print_request_result(args, request, response)
