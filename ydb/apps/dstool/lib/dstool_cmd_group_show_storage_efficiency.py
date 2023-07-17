import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table
import re
import multiprocessing
import sys
import itertools
from collections import defaultdict

description = 'Estimate storage efficiency of VDisks and pools'


def create_table_output():
    size_cols = ['IdxSize', 'InplaceSize', 'HugeSize', 'CompIdxSize', 'CompInplaceSize', 'CompHugeSize', 'Size', 'CompSize',
                 'HugeHeapTotalSize', 'HugeHeapUsedSize', 'HugeHeapFreeSize', 'HugeHeapWasteSize', 'HugeHeapDefragSize']
    columns = ['StoragePool', 'GroupId', 'Blobs', 'CompBlobs', *size_cols, 'Efficiency', 'HugeHeapEfficiency']

    def aggr_size(d, rg):
        for row in rg:
            for key_col in size_cols + ['Blobs', 'CompBlobs']:
                d[key_col] = d.get(key_col, 0) + row[key_col]
        if d['Size']:
            d['Efficiency'] = d['CompSize'] / d['Size']
        if d['HugeHeapTotalSize']:
            d['HugeHeapEfficiency'] = d['HugeHeapDefragSize'] / d['HugeHeapTotalSize']
        return d

    aggregations = {
        'group': (['GroupId'], aggr_size),
    }
    return table.TableOutput(
        cols_order=columns,
        aggregations=aggregations,
        aggr_drop={'Blobs', 'CompBlobs', *size_cols, 'Efficiency', 'HugeHeapEfficiency'},
        col_units=dict([(x, 'bytes') for x in size_cols] + [('Efficiency', '%')] + [('HugeHeapEfficiency', '%')])
    )


table_output = create_table_output()


def parse_vdisk_storage_efficiency(host, node_id, pdisk_id, vslot_id):
    idx_size, inplace_size, huge_size, comp_idx_size, comp_inplace_size, comp_huge_size = 0, 0, 0, 0, 0, 0
    items, comp_items = 0, 0
    page = 'actors/vdisks/vdisk%09u_%09u' % (pdisk_id, vslot_id)
    size_col = 'Idx / Inplaced / Huge Size'
    items_col = 'Items / WInplData / WHugeData'
    usage_col = 'Idx% / IdxB% / InplB% / HugeB%'
    count_items = True
    try:
        data = common.fetch(page, {}, host, fmt='raw').decode('utf-8')
        for t in re.finditer(r'<thead><tr>(.*?)</tr></thead><tbody>(.*?)</tbody>', data, re.S):
            cols = [m.group(1) for m in re.finditer('<th>(.*?)</th>', t.group(1))]
            if size_col not in cols or usage_col not in cols or items_col not in cols:
                continue
            for row in re.finditer(r'<tr>(.*?)</tr>', t.group(2), re.S):
                cells = dict(zip(cols, re.findall(r'<td>(.*?)</td>', row.group(1), re.S)))
                sizes = list(map(int, re.fullmatch(r'<small>(.*)</small>', cells[size_col]).group(1).split(' / ')))
                items_r = list(map(int, re.fullmatch(r'<small>(.*)</small>', cells[items_col]).group(1).split(' / ')))
                usage = re.fullmatch(r'<small>(.*)</small>', cells[usage_col]).group(1).split(' / ')
                if usage == ['ratio']:
                    continue
                elif usage == ['UNK']:
                    usage = ['100', '100', '100', '100']
                if count_items:
                    items += items_r[0]
                    comp_items += int(items_r[0] * float(usage[0]) / 100)
                idx_size += sizes[0]
                inplace_size += sizes[1]
                huge_size += sizes[2]
                comp_idx_size += int(sizes[0] * float(usage[1]) / 100)
                if usage[2] != 'NA':
                    comp_inplace_size += int(sizes[1] * float(usage[2]) / 100)
                else:
                    assert sizes[1] == 0
                if usage[3] != 'NA':
                    comp_huge_size += int(sizes[2] * float(usage[3]) / 100)
                else:
                    assert sizes[2] == 0
            count_items = False

        slot_count = defaultdict(int)
        used_slot_count = defaultdict(int)

        huge_useful_bytes = 0
        if heap_usage := re.search("<div id='hugeheapusageid' class='collapse'>(.*?)</div>", data):
            if heap_usage := re.search('<tbody>(.*?)</tbody>', heap_usage.group(0)):
                for row in re.finditer(r'<tr><td>(\d+)</td><td>(\d+)</td><td>(\d+)</td></tr>', heap_usage.group(1)):
                    slot_size, num_slots_per_chunk, allocated = map(int, row.group(1, 2, 3))
                    huge_useful_bytes += slot_size * allocated
                    slot_count[slot_size, num_slots_per_chunk] += allocated
                    used_slot_count[slot_size, num_slots_per_chunk] += allocated

        huge_unused_bytes = 0
        if heap_state := re.search("<div id='hugeheapstateid' class='collapse'>(.*?)</div>", data):
            if heap_state := re.search('<tbody>(.*?)</tbody>', heap_state.group(0)):
                for row in re.finditer(r'<tr><td>(\d+) / (\d+)</td><td>(.*?)</td></tr>', heap_state.group(1)):
                    slot_size = int(row.group(1))
                    num_slots_per_chunk = int(row.group(2))
                    for item in re.finditer(r'\[\d+ (\d+)]', row.group(3)):
                        num_free_slots = int(item.group(1))
                        slot_count[slot_size, num_slots_per_chunk] += num_free_slots
                        huge_unused_bytes += slot_size * num_free_slots

        huge_waste_bytes = 0
        huge_defrag_bytes = 0
        if m := re.search(r'<tr><td>ChunkSize</td><td>(\d+)</td></tr>', data):
            chunk_size = int(m.group(1))
            for (slot_size, num_slots_per_chunk), num_slots in slot_count.items():
                assert num_slots % num_slots_per_chunk == 0
                num_chunks = num_slots // num_slots_per_chunk
                huge_waste_bytes += num_chunks * (chunk_size - slot_size * num_slots_per_chunk)
                huge_defrag_bytes += (used_slot_count[slot_size, num_slots_per_chunk] + num_slots_per_chunk - 1) // num_slots_per_chunk * chunk_size
    except Exception as e:
        print('Failed to process VDisk %s: %s' % (page, e))
        return None
    return idx_size, inplace_size, huge_size, comp_idx_size, comp_inplace_size, comp_huge_size, items, comp_items, \
        huge_useful_bytes, huge_unused_bytes, huge_waste_bytes, huge_defrag_bytes


def fetcher(args):
    host, items = args
    res_q = []
    for group_id, node_id, pdisk_id, vslot_id in items:
        row = parse_vdisk_storage_efficiency(host, node_id, pdisk_id, vslot_id)
        if row is not None:
            res_q.append((group_id, row))
    return res_q


def add_options(p):
    table_output.add_options(p)


def do(args):
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

    host_requests_map = defaultdict(list)
    group_size_map = {}
    for group in base_config.Group:
        group_size_map[group.GroupId] = len(group.VSlotId)
        for vslot in group.VSlotId:
            host = node_fqdn_map[vslot.NodeId]
            host_requests_map[host].append((group.GroupId, vslot.NodeId, vslot.PDiskId, vslot.VSlotId))

    results = {}
    found = defaultdict(int)

    with multiprocessing.Pool(128) as pool:
        for group_id, stats in itertools.chain.from_iterable(pool.imap_unordered(fetcher, host_requests_map.items())):
            if r := results.get(group_id):
                assert len(stats) == len(r)
                for i, value in enumerate(stats):
                    r[i] += value
            else:
                results[group_id] = list(stats)
            found[group_id] += 1

    rows = []
    for group_id, values in results.items():
        if found[group_id] != group_size_map[group_id]:
            for i in range(len(values)):
                values[i] = int(values[i] * group_size_map[group_id] / found[group_id])
        rows.append(dict(
            GroupId=group_id,
            StoragePool=group_to_sp_name.get(group_id, 'static'),
            IdxSize=values[0],
            InplaceSize=values[1],
            HugeSize=values[2],
            CompIdxSize=values[3],
            CompInplaceSize=values[4],
            CompHugeSize=values[5],
            Size=values[0] + values[1] + values[2],
            CompSize=values[3] + values[4] + values[5],
            Blobs=values[6],
            CompBlobs=values[7],
            HugeHeapTotalSize=values[8] + values[9] + values[10],
            HugeHeapUsedSize=values[8],
            HugeHeapFreeSize=values[9],
            HugeHeapWasteSize=values[10],
            HugeHeapDefragSize=values[11],
        ))
        if rows[-1]['Size']:
            rows[-1]['Efficiency'] = rows[-1]['CompSize'] / rows[-1]['Size']
        if rows[-1]['HugeHeapTotalSize']:
            rows[-1]['HugeHeapEfficiency'] = rows[-1]['HugeHeapDefragSize'] / rows[-1]['HugeHeapTotalSize']

    table_output.dump(rows, args)
