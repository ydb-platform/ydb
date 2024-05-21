import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.core.protos.blobstorage_disk_color_pb2 as disk_color
import ydb.apps.dstool.lib.common as common
import sys
import re
from datetime import timedelta

description = 'Set cluster wide settings'


def add_options(p):
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--default-max-slots', type=int, help='Number of maximum slots for PDisks without explicit ExpectedSlotCount setting')
    g.add_argument('--enable-self-heal', action='store_const', const=True, dest='self_heal', help='Enable SelfHeal for cluster')
    g.add_argument('--disable-self-heal', action='store_const', const=False, dest='self_heal', help='Disable SelfHeal for cluster')
    g.add_argument('--enable-donor-mode', action='store_const', const=True, dest='donor_mode', help='Enable donor mode for cluster')
    g.add_argument('--disable-donor-mode', action='store_const', const=False, dest='donor_mode', help='Disable donor mode for cluster')
    g.add_argument('--scrub-periodicity', type=str, metavar='N', help='Scrub periodicity (in NdNhNmNs format or "disable")')
    g.add_argument('--pdisk-space-margin-promille', metavar='N', type=int, help='PDisk space margin measured in ‰')
    g.add_argument('--group-reserve-min', type=int, metavar='N', help='Group reserve constant minimum')
    g.add_argument('--group-reserve-part-ppm', type=int, metavar='N', help='Group reserve linear constant measured in PPM')
    g.add_argument('--max-scrubbed-disks-at-once', type=int, metavar='N', help='Maximum number of simultaneously scrubbed PDisks')
    choices = disk_color.TPDiskSpaceColor.E.keys()
    g.add_argument('--pdisk-space-color-border', choices=choices, help='PDisk space color border')
    choices = kikimr_bsconfig.TSerialManagementStage.E.keys()
    g.add_argument('--disk-management-mode', type=str, choices=choices, help='Disk management mode')
    g.add_argument('--enable-self-heal-local-policy', action='store_const', const=True, dest='self_heal_local_policy', help='Enable SelfHeal local policy for cluster')
    g.add_argument('--disable-self-heal-local-policy', action='store_const', const=False, dest='self_heal_local_policy', help='Disable SelfHeal local policy for cluster')

    common.add_basic_format_options(p)


def create_request(args):
    request = common.create_bsc_request(args)

    if args.disk_management_mode is not None:
        cmd = request.Command.add().MigrateToSerial
        cmd.Stage = kikimr_bsconfig.TSerialManagementStage.E.Value(args.disk_management_mode)
        return request

    cmd = request.Command.add().UpdateSettings
    if args.default_max_slots is not None:
        cmd.DefaultMaxSlots.append(args.default_max_slots)
    if args.self_heal is not None:
        cmd.EnableSelfHeal.append(args.self_heal)
    if args.donor_mode is not None:
        cmd.EnableDonorMode.append(args.donor_mode)
    if args.scrub_periodicity is not None:
        if args.scrub_periodicity == 'disable':
            d = timedelta()
        else:
            m = re.match(r'^((\d+)d)?((\d+)h)?((\d+)m)?((\d+)s)?$', args.scrub_periodicity)
            if m is None:
                raise Exception('Incorrect scrub periodicity format %s' % args.scrub_periodicity)
            d = timedelta(**dict(zip(['days', 'hours', 'minutes', 'seconds'], map(lambda x: int(x or 0), m.group(2, 4, 6, 8)))))
        cmd.ScrubPeriodicitySeconds.append(int(d.total_seconds()))
    if args.pdisk_space_margin_promille is not None:
        if args.pdisk_space_margin_promille < 0 or args.pdisk_space_margin_promille > 1000:
            raise Exception('Incorrect PDisk space margin setting %f' % args.pdisk_space_margin_promille)
        cmd.PDiskSpaceMarginPromille.append(args.pdisk_space_margin_promille)
    if args.group_reserve_min is not None:
        cmd.GroupReserveMin.append(args.group_reserve_min)
    if args.group_reserve_part_ppm is not None:
        cmd.GroupReservePartPPM.append(args.group_reserve_part_ppm)
    if args.max_scrubbed_disks_at_once is not None:
        cmd.MaxScrubbedDisksAtOnce.append(args.max_scrubbed_disks_at_once)
    if args.pdisk_space_color_border is not None:
        cmd.PDiskSpaceColorBorder.append(disk_color.TPDiskSpaceColor.E.Value(args.pdisk_space_color_border))
    if args.self_heal_local_policy is not None:
        cmd.UseSelfHealLocalPolicy.append(args.self_heal_local_policy)

    return request


def perform_request(request):
    return common.invoke_bsc_request(request)


def is_successful_response(response):
    return common.is_successful_bsc_response(response)


def do(args):
    try:
        request = create_request(args)
        response = perform_request(request)
        common.print_request_result(args, request, response)
        if not is_successful_response(response):
            sys.exit(1)
    except Exception as e:
        common.print_status(args, success=False, error_reason=e)
        sys.exit(1)
