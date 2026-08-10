import sys
import time

import ydb.apps.dstool.lib.common as common
import ydb.public.api.protos.draft.ydb_distributed_storage_pb2 as ydb_distributed_storage

description = 'Relocate vdisks to other pdisks'

GROUP_GENERATION_UPDATE_TIMEOUT_SECONDS = 30
GROUP_GENERATION_UPDATE_POLL_INTERVAL_SECONDS = 1


def add_options(p):
    common.add_vdisk_ids_option(p, required=True)
    common.add_allow_unusable_pdisks_option(p)
    common.add_ignore_degraded_group_check_option(p)
    common.add_ignore_failure_model_group_check_option(p)
    common.add_ignore_vslot_quotas_option(p)
    p.add_argument('--move-only-to-operational-pdisks', action='store_true', help='Move VDisks only to operational PDisks')
    p.add_argument('--suppress-donor-mode', action='store_true', help='Do not leave the previous VDisk in donor mode after the moving and drop it')
    common.add_basic_format_options(p)


def create_request(args, vdisk):
    request = ydb_distributed_storage.ReassignVDiskRequest(dry_run=args.dry_run)
    request.vdisk_id.CopyFrom(vdisk.id)
    request.options.suppress_donor_mode = args.suppress_donor_mode
    request.options.allow_existing_ineligible_pdisks = args.allow_unusable_pdisks
    request.options.settle_only_on_operational_pdisks = args.move_only_to_operational_pdisks
    request.options.ignore_target_space_check = args.ignore_vslot_quotas
    request.options.safety.ignore_degraded_groups = args.ignore_degraded_group_check
    request.options.safety.ignore_group_failure_model = args.ignore_failure_model_group_check
    return request


def _create_legacy_request(args):
    base_config = common.fetch_base_config()
    vslots = common.get_vslots_by_vdisk_ids(base_config, args.vdisk_ids)
    vslot_ids = {common.get_vslot_id(vslot.VSlotId) for vslot in vslots if common.get_pdisk_id(vslot.VSlotId)}

    request = common.create_bsc_request(args)
    for vslot in base_config.VSlot:
        if common.get_vslot_id(vslot.VSlotId) not in vslot_ids:
            continue
        command = request.Command.add().ReassignGroupDisk
        command.GroupId = vslot.GroupId
        command.GroupGeneration = vslot.GroupGeneration
        command.FailRealmIdx = vslot.FailRealmIdx
        command.FailDomainIdx = vslot.FailDomainIdx
        command.VDiskIdx = vslot.VDiskIdx
        if args.suppress_donor_mode:
            command.SuppressDonorMode = True

    return request


def get_vdisk_position(vdisk):
    identifier = vdisk.id
    return (identifier.group_id, identifier.fail_realm_idx, identifier.fail_domain_idx,
            identifier.vdisk_idx)


def select_vdisks(args):
    storage = common.fetch_storage_state(vdisks=True)
    vdisks = common.get_vdisks_by_vdisk_ids(storage.vdisks, args.vdisk_ids)

    selected = []
    seen = set()
    for vdisk in vdisks:
        key = get_vdisk_position(vdisk)
        if key not in seen:
            selected.append(vdisk)
            seen.add(key)
    return selected


def get_current_vdisk(vdisk, previous_generation):
    position = get_vdisk_position(vdisk)
    deadline = time.monotonic() + GROUP_GENERATION_UPDATE_TIMEOUT_SECONDS

    while True:
        storage = common.fetch_storage_state(vdisks=True)
        for current in storage.vdisks:
            same_position = get_vdisk_position(current) == position
            has_new_generation = current.id.group_generation > previous_generation
            if same_position and has_new_generation:
                return current

        if time.monotonic() >= deadline:
            raise common.QueryError(
                'VDisk %s did not advance to a new group generation after previous reassignment' % vdisk.id)
        time.sleep(GROUP_GENERATION_UPDATE_POLL_INTERVAL_SECONDS)


def perform_request(request):
    return common.invoke_distributed_storage_request('ReassignVDisk', request)


def is_successful_response(response):
    return common.get_status(response)


def _partial_success_message(successful_requests):
    suffix = '' if successful_requests == 1 else 's'
    return ('%d previous VDisk reassignment%s already succeeded; multi-VDisk evict is not atomic'
            % (successful_requests, suffix))


def _do_legacy(args):
    request = _create_legacy_request(args)
    response = common.invoke_bsc_request(request)
    common.print_request_result(args, request, response)
    if not common.is_successful_bsc_response(response):
        common.dump_group_mapper_error(response, args)
        sys.exit(1)


def _do_distributed_storage(args):
    vdisks = select_vdisks(args)
    reassigned_group_generations = {}
    successful_requests = 0
    try:
        for vdisk in vdisks:
            group_id = vdisk.id.group_id
            if not args.dry_run and group_id in reassigned_group_generations:
                vdisk = get_current_vdisk(vdisk, reassigned_group_generations[group_id])
            request = create_request(args, vdisk)
            response = perform_request(request)
            if not is_successful_response(response):
                error_reason = 'Request has failed: \n{0}\n{1}\n'.format(request, response)
                if successful_requests and not args.dry_run:
                    error_reason += _partial_success_message(successful_requests) + '\n'
                common.print_status(args, False, error_reason)
                sys.exit(1)
            reassigned_group_generations[group_id] = vdisk.id.group_generation
            successful_requests += 1
    except common.DistributedStorageUnavailable as error:
        if successful_requests and not args.dry_run:
            raise common.QueryError('%s; %s' % (error, _partial_success_message(successful_requests))) from error
        raise
    except (common.ConnectionError, common.QueryError) as error:
        if successful_requests and not args.dry_run:
            raise common.QueryError('%s; %s' % (error, _partial_success_message(successful_requests))) from error
        raise
    common.print_status(args, True, '')


def do(args):
    try:
        _do_distributed_storage(args)
    except common.DistributedStorageUnavailable as error:
        common.print_if_verbose(args, 'INFO: %s; falling back to BlobStorageConfig' % error)
        _do_legacy(args)
