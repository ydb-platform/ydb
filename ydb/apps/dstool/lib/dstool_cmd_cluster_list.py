import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table
from collections import defaultdict

description = 'List cluster'


def add_options(p):
    table.TableOutput([]).add_options(p)


def do(args):
    columns = [
        'Cluster',
        'Hosts',
        'Nodes',
        'Pools',
        'Groups',
        'VDisks',
        'Boxes',
        'PDisks',
    ]
    base_config_and_storage_pools = common.fetch_base_config_and_storage_pools()
    base_config = base_config_and_storage_pools['BaseConfig']
    storage_pools = base_config_and_storage_pools['StoragePools']

    node_fqdn_map = common.build_node_fqdn_map(base_config)

    table_output = table.TableOutput(columns, default_visible_columns=columns)

    stat = defaultdict(set)

    for node in base_config.Node:
        stat['Nodes'].add(node.NodeId)
        stat['Hosts'].add(node_fqdn_map[node.NodeId])

    for pdisk in base_config.PDisk:
        pdisk_id = common.get_pdisk_id(pdisk)
        stat['PDisks'].add(pdisk_id)

    for group in base_config.Group:
        stat['Groups'].add(group.GroupId)

    for vslot in base_config.VSlot:
        vslot_id = common.get_vslot_id(vslot.VSlotId)
        stat['VDisks'].add(vslot_id)

    for sp in storage_pools:
        stat['Boxes'].add(sp.BoxId)
        stat['Pools'].add((sp.BoxId, sp.StoragePoolId))

    row = {}

    for key in stat:
        row[key] = len(stat[key])

    table_output.dump([row], args)
