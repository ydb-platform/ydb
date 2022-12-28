import ydb.apps.dstool.lib.common as common
import sys

description = 'Change or query group state'


def add_options(p):
    common.add_group_ids_option(p, required=True)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--query', action='store_true', help='Query group state')
    g.add_argument('--down', action='store_true', help='Change group state to down')
    g.add_argument('--up', action='store_true', help='Change group state back to up')
    p.add_argument('--persist', action='store_true', help='Save changes to blog storage controller')


def do(args):
    controller_id = 0x000000000001001 | common.connection_params.domain << 56

    if args.query:
        res = []
        for group_id in args.group_ids:
            res.append(common.fetch('tablets/app', dict(TabletID=controller_id, page='GetDown', group=group_id)))
        for item in res:
            print('GroupId# {GroupId} Down# {Down} PersistedDown# {PersistedDown}'.format(**item))
    else:
        error_reason = ''
        success = True
        for group_id in args.group_ids:
            res = common.fetch('tablets/app', dict(TabletID=controller_id, page='SetDown', group=group_id, down=int(args.down), persist=int(args.persist)))
            if 'Error' in res:
                error_reason += 'GroupId# {GroupId} Error# {Error}\n'.format(GroupId=group_id, **res)
                success = False
        common.print_status(args, success, error_reason)
        if not success:
            sys.exit(1)
