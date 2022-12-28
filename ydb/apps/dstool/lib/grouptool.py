from typing import NamedTuple
from collections import defaultdict
from functools import reduce
from operator import getitem, itemgetter
from itertools import product
import ydb.apps.dstool.lib.common as common
import re
import sys


def decompose_location_map_by_levels(levels, locations):
    if levels == (0,) * len(levels):
        levels = 10, 20, 10, 40
    id_map = {}

    def get_id(fdom, begin, end):
        return id_map.setdefault(fdom.subs(begin, end), len(id_map) + 1)
    return {
        location: PDiskLocation(
            get_id(location, 0, levels[0]),
            get_id(location, levels[0], levels[1]),
            get_id(location, 0, levels[2]),
            get_id(location, levels[2], levels[3]),
        )
        for location in locations
    }


def decompose_location_map(sp, locations):
    g = sp.Geometry
    levels = g.RealmLevelBegin, g.RealmLevelEnd, g.DomainLevelBegin, g.DomainLevelEnd
    return decompose_location_map_by_levels(levels, locations)


def check_group(content, location_id_map=None):
    main_map = defaultdict(set)
    prefix_map = defaultdict(lambda: defaultdict(set))
    for coord, location in content:
        if location_id_map is not None:
            location = location_id_map[location]
        for coord_depth, location_depth in [(0, 1), (1, 2), (1, 3), (2, 4)]:
            key = coord[:coord_depth]
            value = location[:location_depth]
            main_map[key].add(value)
            if key:
                prefix_map[key[:-1]][key[-1]].add(value)
    errs = []
    for key, value in main_map.items():
        if len(value) != len(set(map(len, value))):
            errs.append('nonunique locations at key "%s": %d != %d' % (':'.join(map(str, key)), len(value), len(set(map(len, value)))))
    for key, subvalues in ((k, list(map(frozenset, v.values()))) for k, v in prefix_map.items()):
        if len(set(subvalues)) != len(subvalues):
            errs.append('intersecting locations at prefix "%s": %d != %d' % (':'.join(map(str, key)), len(set(subvalues)), len(subvalues)))
    return ', '.join(errs) if errs else None


def check_fail_model(coord_to_status, erasure):
    status_per_fdom = {}
    for key, value in coord_to_status.items():
        status_per_fdom[key[:2]] = status_per_fdom.get(key[:2], True) and value
    if erasure == 'none':
        return all(status_per_fdom.values())
    elif erasure in ['block-4-2', 'mirror-3of4']:
        return sum(status_per_fdom.values()) >= len(status_per_fdom) - 2
    elif erasure == 'mirror-3-dc':
        nwdom = defaultdict(int)
        for (fr, fd), status in status_per_fdom.items():
            if not status:
                nwdom[fr] += 1
        return len(nwdom) <= 1 or (len(nwdom) == 2 and any(v == 1 for v in nwdom.values()))
    elif erasure == 'mirror-3':
        return sum(status_per_fdom.values()) >= len(status_per_fdom) - 1
    else:
        assert False, 'unexpected erasure type %s' % erasure


def vdisk_id_from_json(j):
    return itemgetter('GroupID', 'GroupGeneration', 'Ring', 'Domain', 'VDisk')(j)


def vdisk_id_to_string(vdisk_id):
    return '[%08x:%u:%u:%u:%u]' % vdisk_id


class PDiskLocation(NamedTuple):
    realm_prefix: int
    realm_infix: int
    domain_prefix: int
    domain_infix: int

    def __str__(self):
        return ','.join(map(str, self))

    def __repr__(self):
        return 'PDiskLocation(%s)' % ', '.join(map(str, self))


class PDisk(object):
    def __init__(self, pdisk, location, num_used_slots, expected_slot_count, groups):
        assert num_used_slots >= 0
        self.pdisk = pdisk
        self.location = location
        self.num_used_slots = num_used_slots
        self.expected_slot_count = expected_slot_count
        self.groups = groups

    def __str__(self):
        return '%d:%d@%s(%d/%d)/%s' % (self.pdisk.NodeId, self.pdisk.PDiskId, self.location, self.num_used_slots,
                                       self.expected_slot_count, self.groups)

    def __repr__(self):
        return str(self)

    def get_id(self):
        return self.pdisk.NodeId, self.pdisk.PDiskId

    def fits(self, max_num_slots):
        return self.num_used_slots + 1 <= self.expected_slot_count and self.num_used_slots <= max_num_slots


class GroupMapper(object):
    _geom_for_erasure = {
        'mirror-3-dc': (3, 3, 1),
        'block-4-2': (1, 8, 1),
        'mirror-3': (1, 4, 1),
        'none': (1, 1, 1),
        'mirror-3of4': (1, 8, 1),
    }

    def __init__(self, pdisk_map, sp):
        self.pdisk_map = pdisk_map
        self.sp = sp
        self.num_fr, self.num_fdom_in_fr, self.num_vdisks_in_fdom = self.get_geometry()
        self.group_id = 1 << 32

    def create_group(self, content=None):
        group = [[[None for _ in range(self.num_vdisks_in_fdom)] for _ in range(self.num_fdom_in_fr)] for _ in range(self.num_fr)]
        if content is not None:
            for coord, value in content:
                reduce(getitem, coord[:-1], group)[coord[-1]] = value
        return group

    def allocate(self, existing_groups, randomize, honor_existing_groups, group_slot_size, pdisk_free_space):
        for max_num_slots in range(0, 256):
            def reduce_dict(d, threshold):
                res = defaultdict(int)
                for prefix, num_items in d.items():
                    if num_items >= threshold:
                        res[prefix[:-1]] += 1
                return res

            # calculate maximum number of slots suitable for this group
            d = defaultdict(int)
            for disk in self.pdisk_map.values():
                if disk.fits(max_num_slots):
                    d[disk.location[:]] += 1

            # reduce maps
            d = reduce_dict(d, self.num_vdisks_in_fdom)
            d = reduce_dict(d, self.num_fdom_in_fr)
            d = reduce_dict(d, 1)
            d = reduce_dict(d, self.num_fr)
            if d:
                break
        else:
            raise Exception('unable to allocate new group, sorry')

        usable_disks = [
            disk
            for disk in self.pdisk_map.values()
            if disk.fits(max_num_slots)
        ]
        num_matching_disks_in_domain = defaultdict(int)
        for disk in usable_disks:
            num_matching_disks_in_domain[disk.location] += 1

        def remove_disks_with_prefix(prefix):
            prefix_len = len(prefix)
            usable_disks[:] = [
                disk
                for disk in usable_disks
                if disk.location[:prefix_len] != prefix
            ]
            return prefix[:-1]  # drop the last item of the prefix

        group = self.create_group()

        initial_disk_score = {
            pdisk1.get_id(): max(len(pdisk1.groups & pdisk2.groups & existing_groups) for pdisk2 in usable_disks)
            for pdisk1 in usable_disks
        }

        num_disks_in_common = defaultdict(int)  # indexed with neighbor group id
        first = True

        def get_score(disk):
            score = disk.num_used_slots,
            if first:
                score += -initial_disk_score[disk.get_id()],
            common_existing = max((num_disks_in_common[group_id] for group_id in disk.groups if group_id in existing_groups), default=0)
            if honor_existing_groups:
                score += -common_existing,
            common_other = max((num_disks_in_common[group_id] for group_id in disk.groups if group_id not in existing_groups), default=0)
            if randomize:
                score += common_other,
            else:
                score += -common_other,
            if not honor_existing_groups:
                score += -common_existing,
            score += disk.get_id(),
            return score

        subset_disks = {}

        # enumerate allowed disks for specific realm and domain
        def get_allowed_disks(ri, di):
            for key in [(ri, di), (ri,), ()]:
                if key in subset_disks:
                    res = subset_disks[key]
                    break
            else:
                res = usable_disks
            assert res
            return res

        while True:
            options = (
                (disk, ri, di, vi)
                for ri, di, vi in product(range(self.num_fr), range(self.num_fdom_in_fr), range(self.num_vdisks_in_fdom))
                if group[ri][di][vi] is None
                for disk in get_allowed_disks(ri, di)
            )
            res = min(options, key=lambda x: get_score(x[0]), default=None)
            if res is None:
                break
            disk, ri, di, vi = res

            num_matching_disks_in_domain[disk.location] -= 1
            disk.num_used_slots += 1  # mark this slot as used one
            disk.groups.add(self.group_id)  # add just created group to group map

            # adjust number of disks-in-common
            for group_id in disk.groups:
                num_disks_in_common[group_id] += 1

            # store disk in the group
            group[ri][di][vi] = disk.get_id()

            # update disk sets
            base = usable_disks
            for key, prefix_len in [((), 1), ((ri,), 2), ((ri, di), 4)]:
                if key not in subset_disks:
                    prefix = disk.location[:prefix_len]
                    subset_disks[key] = list(filter(lambda disk: disk.location[:prefix_len] == prefix, base))
                    base[:] = list(filter(lambda disk: disk.location[:prefix_len] != prefix, base))
                base = subset_disks[key]

            # remove selected disk from realm/domain disk set
            base[:] = list(filter(lambda x: x.get_id() != disk.get_id(), base))

            first = False

        for existing_group_id in sorted(existing_groups, key=lambda group_id: num_disks_in_common[group_id], reverse=True):
            all_disks = sum(sum(group, []), [])
            if all(group_slot_size[existing_group_id] <= pdisk_free_space[pdisk_id] for pdisk_id in all_disks):
                group_slot_size[self.group_id] = group_slot_size[existing_group_id]
                self.group_id += 1
                return group, existing_group_id

        assert False, 'failed to allocate group'

    def get_geometry(self):
        g = self.sp.Geometry
        geom = g.NumFailRealms, g.NumFailDomainsPerFailRealm, g.NumVDisksPerFailDomain
        if not any(geom):
            geom = self._geom_for_erasure[self.sp.ErasureSpecies]
        return geom


table_re = re.compile('<tbody>(.*?)</tbody>')
row_re = re.compile('<tr>(.*?)</tr>')
cell_re = re.compile('(<td[^>]*>)(.*?)</td>')
tabletid_re = re.compile('<a[^>]*>(.*?)</a>')
datatext_re = re.compile(r' data-text="(\d+)"')


def strip_small(x):
    return x[7:-8] if x.startswith('<small>') and x.endswith('</small>') else x


def parse_vdisk_storage_from_http_api(node_id, pdisk_id, vslot_id):
    page = 'vdisk/json/blobindexstat'
    data = common.fetch(page, dict(node_id=node_id, pdisk_id=pdisk_id, vslot_id=vslot_id), fmt='json')
    res = []
    if 'stat' not in data:
        raise Exception(f'Error resposne "{data}"')
    tablets = data['stat'].get('tablets', [])
    for tablet_info in tablets:
        tablet_id = int(tablet_info['tablet_id'])
        for channel, channel_info in enumerate(tablet_info['channels']):
            size = int(channel_info.get('data_size', 0))
            res.append((tablet_id, channel, size))
    return res


def parse_vdisk_storage_legacy(host, pdisk_id, vslot_id):
    page = 'actors/vdisks/vdisk%09u_%09u' % (pdisk_id, vslot_id)
    data = common.fetch(page, dict(type='stat', dbname='LogoBlobs'), host, fmt='raw')
    res = []
    m = table_re.search(str(data))
    for row in row_re.finditer(m.group(1)):
        data = [
            strip_small(m.group(2)) if m_datatext is None else int(m_datatext.group(1))
            for m in cell_re.finditer(row.group(1))
            for m_datatext in [datatext_re.search(m.group(1))]  # try to find data-text attr in <td>
        ]
        m = tabletid_re.match(data[0])
        tablet_id = int(m.group(1))
        channel = int(data[1])
        size = data[3]
        if not isinstance(size, int):
            factor = None
            if size.endswith('KiB'):
                size = size[:-3]
                factor = 1024.0
            elif size.endswith('MiB'):
                size = size[:-3]
                factor = 1024.0**2
            elif size.endswith('GiB'):
                size = size[:-3]
                factor = 1024.0**3
            elif size.endswith('TiB'):
                size = size[:-3]
                factor = 1024.0**4
            elif size.endswith('PiB'):
                size = size[:-3]
                factor = 1024.0**5
            elif size.endswith('B'):
                size = size[:-1]
                factor = 1.0
            size = int(float(size) * factor)
        res.append((tablet_id, channel, size))
    return res


def parse_vdisk_storage(host, node_id, pdisk_id, vslot_id):
    try:
        if common.connection_params.http:
            return parse_vdisk_storage_from_http_api(node_id, pdisk_id, vslot_id)
        else:
            return parse_vdisk_storage_legacy(host, pdisk_id, vslot_id)
    except Exception as e:
        print('Failed to parse VDisk storage at host %s PDiskId# %d VSlotId# %d error# %s' % (host, pdisk_id, vslot_id, e), file=sys.stderr)
        return None
