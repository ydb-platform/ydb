import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table
import json
import sys

description = 'Get blob information from group'


def add_options(p):
    p.add_argument('--group-id', type=int, required=True, help='Group id')
    p.add_argument('--blob-id', type=str, required=True, help='Blob id, e.g. [72075186224037917:2020:4:2:72707:6337498:0]')
    table.TableOutput([]).add_options(p)


def process_vslot(vslot, node_mon_map, blob_id):
    page = 'vdisk/json/getblob'
    params = {
        'node_id': vslot.VSlotId.NodeId,
        'pdisk_id': vslot.VSlotId.PDiskId,
        'vslot_id': vslot.VSlotId.VSlotId,
        'from': blob_id,
        'to': blob_id,
        'internals': 'yes'
    }
    host = node_mon_map[vslot.VSlotId.NodeId]
    data = common.fetch(page, params, host, 'raw').decode('utf-8')
    data = json.loads(data)
    res = []
    if 'logoblobs' in data:
        for blob in data['logoblobs']:
            row = {}
            row['BlobId'] = blob['id']
            row['Status'] = blob['status']
            row['Ingress'] = blob['ingress']
            res.append(row)

    return res


def do(args):
    try:
        columns = [
            'BlobId',
            'Status',
            'Ingress',
        ]

        table_output = table.TableOutput(columns, default_visible_columns=columns)

        base_config = common.fetch_base_config()
        group_map = common.build_group_map(base_config)
        if args.group_id not in group_map:
            raise Exception('Unknown group with id %u' % args.group_id)

        group = group_map[args.group_id]
        vslot_map = common.build_vslot_map(base_config)
        node_mon_map = common.fetch_node_mon_map()

        rows = []
        for vslot in common.vslots_of_group(group, vslot_map):
            res = process_vslot(vslot, node_mon_map, args.blob_id)
            rows.extend(res)

        if rows:
            table_output.dump(rows, args)
    except Exception as e:
        common.print_status(args, success=False, error_reason=e)
        sys.exit(1)
