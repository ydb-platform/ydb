import ydb.apps.dstool.lib.common as common
from argparse import FileType
import google.protobuf.json_format as pb_json

description = 'Create virtual group backed by BlobDepot'


def add_options(p):
    p.add_argument('--name', type=str, required=True, help='name of virtual group that requires reconfiguring')
    p.add_argument('--s3-settings', type=FileType('r', encoding='utf-8'), metavar='JSON_FILE', help='path to JSON file containing S3 settings')


def do(args):
    request = common.create_bsc_request(args)

    if args.s3_settings:
        s3_settings = args.s3_settings.read()
    else:
        s3_settings = None

    cmd = request.Command.add().ReconfigureVirtualGroup
    cmd.Name = args.name

    if s3_settings is not None:
        pb_json.Parse(s3_settings, cmd.S3BackendSettings)

    response = common.invoke_bsc_request(request)
    common.print_request_result(args, request, response)
