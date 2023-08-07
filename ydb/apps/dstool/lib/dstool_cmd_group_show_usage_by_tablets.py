import ydb.apps.dstool.lib.grouptool as grouptool
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table
import multiprocessing
import json
import sys
import itertools
from collections import defaultdict

description = 'Estimate groups usage by tablets'


def create_table_output():
    columns = ['TabletId', 'TabletType', 'TabletChannel', 'StoragePool', 'GroupId', 'GroupType', 'Size', 'TabletMaxSize']

    def human_readable_fn(d):
        return d.update((key, '%s GiB' % common.gib_string(d[key])) for key in ['Size', 'TabletMaxSize'] if key in d)

    def tablet_size_max(d, rg):
        sizes = [x['Size'] for x in rg]
        d.update(
            Size=sum(sizes),
            TabletMaxSize=max(sizes)
        )
        return d

    def aggr_size(d, rg):
        return d.update(Size=sum(x['Size'] for x in rg)) or d

    aggregations = {
        'tablet': (['TabletId', 'Size'], tablet_size_max),
        'tablet_type': (['TabletType', 'Size'], aggr_size),
        'channel': (['TabletChannel', 'Size'], aggr_size),
        'group': (['GroupId', 'Size'], aggr_size),
        'group_type': (['GroupType', 'Size'], aggr_size),
    }
    return table.TableOutput(
        cols_order=columns,
        human_readable_fn=human_readable_fn,
        aggregations=aggregations,
        aggr_drop={
            'Size',
            'TabletMaxSize',
        }
    )


table_output = create_table_output()


def add_options(p):
    p.add_argument('--cache-file', type=str, help='Path to the cache file')
    table_output.add_options(p)


def read_cache(args):
    try:
        with open(args.cache_file, 'r') as f:
            j = json.load(f)
        tablet_channel_group_stat = defaultdict(list)
        for row in j['sizes']:
            cols = ['tablet_id', 'tablet_channel', 'group_id', 'tablet_type', 'sp_name']
            tablet_channel_group_stat[tuple(map(row.__getitem__, cols))].append(row['size'])
        group_sizes_map = {}
        for key, value in j['group_sizes_map'].items():
            group_sizes_map[int(key)] = value
        return tablet_channel_group_stat, group_sizes_map
    except Exception as e:
        common.print_if_not_quiet(args, 'Failed to read data from cache file %s: %s' % (args.cache_file, e), file=sys.stderr)


def write_cache(args, tablet_channel_group_stat, group_sizes_map):
    j = dict(
        group_sizes_map=group_sizes_map,
        sizes=[
            {
                'tablet_id': tablet_id,
                'tablet_channel': tablet_channel,
                'group_id': group_id,
                'tablet_type': tablet_type,
                'sp_name': sp_name,
                'size': size,
            }
            for (tablet_id, tablet_channel, group_id, tablet_type, sp_name), sizes in tablet_channel_group_stat.items()
            for size in sizes
        ]
    )
    with open(args.cache_file, 'w') as f:
        json.dump(j, f, indent=2, sort_keys=True)


def fetcher(args):
    host, items = args
    res_q = []
    for group_id, node_id, pdisk_id, vslot_id in items:
        data = grouptool.parse_vdisk_storage(host, node_id, pdisk_id, vslot_id)
        for tablet_id, channel, size in data or []:
            res_q.append((group_id, tablet_id, channel, size))
    return res_q


def do(args):
    tablet_channel_group_stat = None
    group_sizes_map = None

    if args.cache_file:
        res = read_cache(args)
        if res is not None:
            tablet_channel_group_stat, group_sizes_map = res
            common.print_if_verbose(args, 'Using data from cache file %s' % args.cache_file, file=sys.stderr)

    if tablet_channel_group_stat is None:
        base_config_and_storage_pools = common.fetch_base_config_and_storage_pools()
        base_config = base_config_and_storage_pools['BaseConfig']
        node_fqdn_map = common.build_node_fqdn_map(base_config)
        storage_pools = base_config_and_storage_pools['StoragePools']
        sp_map = common.build_storage_pool_names_map(storage_pools)

        group_to_sp_name = {
            g.GroupId: sp_map[g.BoxId, g.StoragePoolId]
            for g in base_config.Group
            if (g.BoxId, g.StoragePoolId) in sp_map
        }

        for group in base_config.Group:
            group_id = group.GroupId
            box_id = group.BoxId
            pool_id = group.StoragePoolId
            if (box_id, pool_id) not in sp_map:
                common.print_if_verbose(args, f"Can't find group {group_id} in box {box_id}, pool {pool_id}", sys.stderr)

        type_map = {
            int(row['TabletId']): row['Type']
            for row in common.fetch('viewer/json/tabletinfo', dict(enums=1)).get('TabletStateInfo', [])
            if 'TabletId' in row and 'Type' in row
        }

        group_sizes_map = {}
        host_requests_map = defaultdict(list)
        for group in base_config.Group:
            group_sizes_map[group.GroupId] = len(group.VSlotId)
            for vslot in group.VSlotId:
                host = node_fqdn_map[vslot.NodeId]
                host_requests_map[host].append((group.GroupId, vslot.NodeId, vslot.PDiskId, vslot.VSlotId))

        tablet_channel_group_stat = defaultdict(list)

        with multiprocessing.Pool(128) as pool:
            for item in itertools.chain.from_iterable(pool.imap_unordered(fetcher, host_requests_map.items())):
                group_id, tablet_id, channel, size = item
                key = tablet_id, channel, group_id, type_map.get(tablet_id, ''), group_to_sp_name.get(group_id)
                tablet_channel_group_stat[key].append(size)

        if args.cache_file:
            write_cache(args, tablet_channel_group_stat, group_sizes_map)

    rows = []
    for (tablet_id, tablet_channel, group_id, tablet_type, sp_name), sizes in tablet_channel_group_stat.items():
        row = {}
        row['TabletId'] = tablet_id
        row['TabletType'] = tablet_type
        row['TabletChannel'] = tablet_channel
        row['GroupId'] = group_id
        row['GroupType'] = 'dynamic' if common.is_dynamic_group(group_id) else 'static'
        row['StoragePool'] = sp_name if common.is_dynamic_group(group_id) else 'None'
        row['Size'] = sum(sizes) * group_sizes_map[group_id] // len(sizes)
        rows.append(row)

    table_output.dump(rows, args)
