from argparse import FileType

import google.protobuf.json_format as pb_json

import ydb.apps.dstool.lib.common as common

description = 'Manage S3 cleanup for a retired group'


def add_options(p):
    p.add_argument('--group-id', type=int, required=True, help='ID of the retired group')
    action = p.add_mutually_exclusive_group(required=True)
    action.add_argument('--credentials', type=FileType('r', encoding='utf-8'), metavar='JSON_FILE',
                        help='rotate cleanup credentials using a JSON file with AccessKey and SecretKey')
    action.add_argument('--confirm-exclusive-ownership', action='store_true',
                        help='authorize cleanup after verifying exclusive use of the physical prefix across bucket '
                             'aliases and draining all old writers and migration routes')
    action.add_argument('--confirm-cleanup-complete', action='store_true',
                        help='discard cleanup credentials after externally removing all objects and draining outstanding '
                             'mutations; the namespace stays reserved')


def do(args):
    request = common.create_bsc_request(args)
    cmd = request.Command.add().ManageRetiredS3Namespace
    cmd.GroupId = args.group_id
    if args.credentials:
        pb_json.Parse(args.credentials.read(), cmd.Credentials)
    elif args.confirm_exclusive_ownership:
        cmd.ConfirmExclusiveOwnership = True
    else:
        cmd.ConfirmCleanupComplete = True
    response = common.invoke_bsc_request(request)
    common.print_request_result(args, request, response)
