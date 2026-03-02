import json
import sys

import ydb.apps.dstool.lib.common as common

description = 'Populate pdisk with selected vdisks'


def add_options(p):
    p.add_argument('--snapshot-from-pdisk', type=str,
                   help='Take snapshot of current VDisks from PDisk [NodeId:PDiskId]')
    p.add_argument('--destination-pdisk', '-d', type=str,
                   help='Destination PDisk for Populate operation [NodeId:PDiskId]')
    p.add_argument('--snapshot-file', type=str, metavar='PATH',
                   help='In snapshot mode write JSON snapshot to PATH; in populate mode read VDisk list from PATH')
    p.add_argument('--suppress-donor-mode', action='store_true',
                   help='Do not leave previous VDisks in donor mode after moving')
    common.add_basic_format_options(p)


def parse_pdisk_id(raw):
    value = raw.strip().strip('[').strip(']')
    node, sep, pdisk = value.partition(':')
    if sep != ':' or not node or not pdisk:
        raise Exception('PDisk id %r must be in format [NodeId:PDiskId]' % raw)
    try:
        return int(node), int(pdisk)
    except ValueError:
        raise Exception('PDisk id %r must contain integer NodeId and PDiskId' % raw)


def format_pdisk_id(pdisk_id):
    return '[%u:%u]' % pdisk_id


def format_vdisk_id(vslot):
    return '[%08x:_:%u:%u:%u]' % (vslot.GroupId, vslot.FailRealmIdx, vslot.FailDomainIdx, vslot.VDiskIdx)


class VSlotsOnPDisk:
    __slots__ = ('active', 'donors')

    def __init__(self, active, donors):
        self.active = active
        self.donors = donors

    @classmethod
    def from_base_config(cls, base_config, pdisk_id):
        donor_vslot_ids = set()
        for vslot in base_config.VSlot:
            for donor in vslot.Donors:
                if common.get_pdisk_id(donor.VSlotId) == pdisk_id:
                    donor_vslot_ids.add(common.get_vslot_id(donor.VSlotId))

        active = []
        donors = []
        for vslot in base_config.VSlot:
            if common.get_pdisk_id(vslot.VSlotId) != pdisk_id:
                continue
            if common.get_vslot_id(vslot.VSlotId) in donor_vslot_ids:
                donors.append(vslot)
            else:
                active.append(vslot)
        return cls(active=active, donors=donors)

    @staticmethod
    def _to_vdisk_ids(vslots):
        return [format_vdisk_id(vslot) for vslot in vslots]

    def active_vdisk_ids(self):
        return self._to_vdisk_ids(self.active)

    def donor_vdisk_ids(self):
        return self._to_vdisk_ids(self.donors)


def flatten_vdisk_ids(vdisk_ids):
    return [vdisk_id for item in vdisk_ids for vdisk_id in item.split() if vdisk_id]


def read_snapshot(path):
    with open(path, 'r', encoding='utf-8') as f:
        raw = f.read()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = raw

    if isinstance(parsed, dict):
        parsed = parsed.get('vdisk_ids', parsed.get('VDiskIds', parsed))

    if isinstance(parsed, str):
        parsed = [parsed]
    if isinstance(parsed, list):
        return flatten_vdisk_ids(parsed)
    raise Exception('Failed to parse VDisk ids from snapshot file %r' % path)


def warn_skipped_donors(args, pdisk_id, skipped_donor_vdisk_ids):
    if not skipped_donor_vdisk_ids:
        return
    if getattr(args, 'quiet', False):
        return

    preview = ', '.join(skipped_donor_vdisk_ids[:3])
    suffix = '...' if len(skipped_donor_vdisk_ids) > 3 else ''
    print(
        'WARNING: skipped %u donor VDisks on PDisk %s while collecting source list: %s%s'
        % (len(skipped_donor_vdisk_ids), format_pdisk_id(pdisk_id), preview, suffix),
        file=sys.stderr,
    )


def make_vdisk_ids_for_populate(args):
    if args.snapshot_file is None:
        raise Exception('Populate mode requires --snapshot-file')
    vdisk_ids = read_snapshot(args.snapshot_file)
    if not vdisk_ids:
        raise Exception('VDisk list is empty')
    return vdisk_ids


def create_populate_request(args, base_config, destination_pdisk_id, vdisk_ids):
    request = common.create_bsc_request(args)
    cmd = request.Command.add().PopulatePDisk
    cmd.DestinationPDisk.TargetPDiskId.NodeId = destination_pdisk_id[0]
    cmd.DestinationPDisk.TargetPDiskId.PDiskId = destination_pdisk_id[1]

    vslots = common.get_vslots_by_vdisk_ids(base_config, vdisk_ids)
    for vslot in vslots:
        item = cmd.VDiskId.add()
        item.GroupID = vslot.GroupId
        item.GroupGeneration = vslot.GroupGeneration
        item.Ring = vslot.FailRealmIdx
        item.Domain = vslot.FailDomainIdx
        item.VDisk = vslot.VDiskIdx

    if args.suppress_donor_mode:
        cmd.SuppressDonorMode = True

    return request


def run_snapshot_mode(args):
    if args.destination_pdisk is not None:
        raise Exception('--destination-pdisk cannot be used with --snapshot-from-pdisk')
    if args.suppress_donor_mode:
        raise Exception('--suppress-donor-mode cannot be used with --snapshot-from-pdisk')

    base_config = common.fetch_base_config()
    source_pdisk_id = parse_pdisk_id(args.snapshot_from_pdisk)

    vslots_on_pdisk = VSlotsOnPDisk.from_base_config(base_config, source_pdisk_id)
    vdisk_ids = vslots_on_pdisk.active_vdisk_ids()
    skipped_donor_vdisk_ids = vslots_on_pdisk.donor_vdisk_ids()
    snapshot = {
        'pdisk_id': args.snapshot_from_pdisk,
        'vdisk_ids': vdisk_ids,
    }

    warn_skipped_donors(args, source_pdisk_id, skipped_donor_vdisk_ids)

    if args.snapshot_file is not None:
        with open(args.snapshot_file, 'w', encoding='utf-8') as f:
            json.dump(snapshot, f, indent=2, sort_keys=True)
            f.write('\n')

    if args.format == 'json':
        print(json.dumps(snapshot))
    else:
        for vdisk_id in snapshot['vdisk_ids']:
            print(vdisk_id)


def run_populate_mode(args):
    if args.snapshot_from_pdisk is not None:
        raise Exception('--snapshot-from-pdisk cannot be used with --destination-pdisk')

    base_config = common.fetch_base_config()
    destination_pdisk_id = parse_pdisk_id(args.destination_pdisk)
    vdisk_ids = make_vdisk_ids_for_populate(args)

    request = create_populate_request(args, base_config, destination_pdisk_id, vdisk_ids)
    response = common.invoke_bsc_request(request)
    common.print_request_result(args, request, response)
    if not common.is_successful_bsc_response(response):
        common.dump_group_mapper_error(response, args)
        sys.exit(1)


def do(args):
    snapshot_mode = args.snapshot_from_pdisk is not None
    populate_mode = args.destination_pdisk is not None

    if snapshot_mode == populate_mode:
        common.print_status(args, success=False, error_reason='Specify exactly one mode: --snapshot-from-pdisk or --destination-pdisk')
        sys.exit(1)

    try:
        if snapshot_mode:
            run_snapshot_mode(args)
        else:
            run_populate_mode(args)
    except Exception as e:
        common.print_status(args, success=False, error_reason=e)
        sys.exit(1)
