import ydb.apps.dstool.lib.common as common
import sys

description = 'Run compaction on vdisks'

def add_options(p):
    common.add_vdisk_ids_option(p, required=True)
    p.add_argument('--full', action='store_true', default=False,
                   help='Run full compaction')
    p.add_argument('--compact-logoblobs', action='store_true', default=False,
                   help='Compact LogoBlobs')
    p.add_argument('--compact-blocks', action='store_true', default=False,
                   help='Compact Blocks')
    p.add_argument('--compact-barriers', action='store_true', default=False,
                   help='Compact Barriers')
    common.add_basic_format_options(p)

def do(args):
    base_config = common.fetch_base_config()
    vslots = common.get_vslots_by_vdisk_ids(base_config, args.vdisk_ids)

    success_count = 0
    error_count = 0

    for vslot in vslots:
        vdisk_id = '[%08x:%u:%u:%u:%u]' % (vslot.GroupId, vslot.GroupGeneration, vslot.FailRealmIdx, vslot.FailDomainIdx, vslot.VDiskIdx)
        node_id = vslot.VSlotId.NodeId
        pdisk_id = vslot.VSlotId.PDiskId
        vslot_id = vslot.VSlotId.VSlotId

        try:
            params = {}
            if args.full:
                dbnames = ['LogoBlobs', 'Blocks', 'Barriers']
            else:
                dbnames = []
                if args.compact_logoblobs:
                    dbnames.append('LogoBlobs')
                if args.compact_blocks:
                    dbnames.append('Blocks')
                if args.compact_barriers:
                    dbnames.append('Barriers')
                if not dbnames:
                    dbnames = ['LogoBlobs']

            for dbname in dbnames:
                path = f'node/{node_id}/actors/vdisks/vdisk{pdisk_id:09d}_{vslot_id:09d}'
                params = {
                    'type': 'dbmainpage',
                    'dbname': dbname,
                    'action': 'compact'
                }
                try:
                    response = common.fetch(path, params, fmt='raw')
                except Exception as e:
                    common.print_if_not_quiet(args, f"Failed to send compaction request to VDisk {vdisk_id} for {dbname}: {e}")
                    error_count += 1
                    continue

            success_count += 1

        except Exception as e:
            common.print_if_not_quiet(args, f"Error processing VDisk {vdisk_id}: {e}")
            error_count += 1

    if success_count > 0:
        common.print_status(args, success=True, error_reason=f"Successfully processed {success_count} VDisk(s)")
    if error_count > 0:
        common.print_status(args, success=False, error_reason=f"Failed to process {error_count} VDisk(s)")
        sys.exit(1)
