import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table
from collections import defaultdict

description = 'List nodes'


def add_options(p):
    table.TableOutput([]).add_options(p)


def do(args):
    all_columns = [
        'NodeId',
        'NodeType',
        'FQDN',
        'IcPort',
        'DC',
        'Rack',
    ]
    visible_columns = [
        'NodeId',
        'NodeType',
        'FQDN',
        'IcPort',
        'DC',
        'Rack',
    ]

    table_output = table.TableOutput(all_columns, default_visible_columns=visible_columns)

    base_config = common.fetch_base_config()

    node_map = defaultdict(dict)
    for node in base_config.Node:
        node_data = node_map[node.NodeId]
        node_data['NodeType'] = kikimr_bsconfig.ENodeType.Name(node.Type)
        node_data['FQDN'] = node.HostKey.Fqdn
        node_data['IcPort'] = node.HostKey.IcPort
        node_data['DC'] = node.Location.DataCenter
        node_data['Rack'] = node.Location.Rack

    rows = []
    for node_id, node in node_map.items():
        row = {}
        row['NodeId'] = node_id
        for key, value in node.items():
            row[key] = value
        rows.append(row)

    table_output.dump(rows, args)
