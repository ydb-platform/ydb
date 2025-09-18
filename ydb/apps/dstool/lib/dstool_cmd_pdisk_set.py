import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.apps.dstool.lib.common as common
import sys

description = 'Set pdisk properties. It may impact respective vdisks'


def add_options(p):
    common.add_pdisk_ids_option(p, required=True)
    g = p.add_mutually_exclusive_group(required=True)
    statuses = kikimr_bsconfig.EDriveStatus.keys()
    g.add_argument('--status', type=str, choices=statuses, help='Set status')
    decommit_statuses = kikimr_bsconfig.EDecommitStatus.keys()
    g.add_argument('--decommit-status', type=str, choices=decommit_statuses, help='Set decomission status')
    maintenance_statuses = kikimr_bsconfig.TMaintenanceStatus.E.keys()
    g.add_argument('--maintenance-status', type=str, choices=maintenance_statuses, help='Set maintenance status')
    common.add_allow_unusable_pdisks_option(p)
    p.add_argument('--allow-working-disks', action='store_true', help='Allow settlement even if any of enlisted PDisks is still working')
    common.add_ignore_degraded_group_check_option(p)
    common.add_ignore_failure_model_group_check_option(p)
    common.add_ignore_vslot_quotas_option(p)
    p.add_argument('--unavail-as-offline', action='store_true', help='Treat PDisks not reported by Node Whiteboard as offline')
    common.add_basic_format_options(p)


def create_request(args, pdisks, node_id_to_host):
    request = common.create_bsc_request(args)
    for pdisk_id, pdisk in pdisks.items():
        cmd = request.Command.add().UpdateDriveStatus
        cmd.HostKey.Fqdn, cmd.HostKey.IcPort = node_id_to_host[pdisk.NodeId]
        if pdisk.Path:
            cmd.Path = pdisk.Path
        else:
            _, cmd.PDiskId = pdisk_id
        if args.status is not None:
            cmd.Status = kikimr_bsconfig.EDriveStatus.Value(args.status)
        if args.decommit_status is not None:
            cmd.DecommitStatus = kikimr_bsconfig.EDecommitStatus.Value(args.decommit_status)
        if args.maintenance_status is not None:
            cmd.MaintenanceStatus = kikimr_bsconfig.TMaintenanceStatus.E.Value(args.maintenance_status)

    return request


def perform_request(request):
    return common.invoke_bsc_request(request)


def is_successful_response(response):
    return common.is_successful_bsc_response(response)


def get_pdisks(pdisk_ids, base_config):
    pdisks = {}
    for arg in pdisk_ids:
        for id in arg.split():
            # id is in the form '[NodeId:PDiskId]'
            node, pdisk = id.strip('[').strip(']').split(':')
            pdisk_id = int(node), int(pdisk)
            pdisks[pdisk_id] = None

    for pdisk in base_config.PDisk:
        pdisk_id = common.get_pdisk_id(pdisk)
        if pdisk_id in pdisks:
            pdisks[pdisk_id] = pdisk

    return pdisks


def do(args):
    base_config = common.fetch_base_config()
    pdisks = get_pdisks(args.pdisk_ids, base_config)
    node_id_to_host, _ = common.build_node_fqdn_maps(base_config)

    for pdisk_id, pdisk in pdisks.items():
        if pdisk is None:
            common.print_status(args, success=False, error_reason='Unknown pdisk id [%d:%d]' % pdisk_id)
            sys.exit(1)
        node_id = pdisk_id[0]
        if node_id not in node_id_to_host:
            common.print_status(args, success=False, error_reason="Can't determine FQDN for node id %d" % node_id)
            sys.exit(1)

    if not args.allow_working_disks and args.status == 'BROKEN':
        params = dict(node_id=0, enums=1)
        error_reason = ''
        success = True
        pdisk_status = {}
        pdisk_name = {
            pdisk_id: 'PDisk %d:%d (%s:%s)' % (pdisk_id + (node_id_to_host.get(pdisk.NodeId, [None, None])[0], pdisk.Path))
            for pdisk_id, pdisk in pdisks.items()
        }
        for pdisk_id, pdisk in sorted(pdisks.items()):
            try:
                for row in common.fetch('viewer/json/pdiskinfo', params, node_id_to_host[pdisk.NodeId][0]).get('PDiskStateInfo', []):
                    if 'State' in row and row['NodeId'] == pdisk.NodeId and row['PDiskId'] == pdisk.PDiskId:
                        pdisk_status[pdisk_id] = not row['State'].endswith('Error')
            except Exception as e:
                if args.unavail_as_offline:
                    pdisk_status[pdisk_id] = False
                else:
                    pdisk_status[pdisk_id] = None
                    error_reason += "Can't determine %s status: %s\n" % (pdisk_name[pdisk_id], e)
                    success = False
        for pdisk_id, pdisk in sorted(pdisks.items()):
            if pdisk_id not in pdisk_status:
                error_reason += '%s is not found in node JSON monitoring\n' % pdisk_name[pdisk_id]
                success = False
            elif pdisk_status[pdisk_id]:
                error_reason += '%s is reported as working one\n' % pdisk_name[pdisk_id]
                success = False
        if not success:
            common.print_status(args, success, error_reason=error_reason)
            sys.exit(1)

    request = create_request(args, pdisks, node_id_to_host)
    response = perform_request(request)
    common.print_request_result(args, request, response)
    if not is_successful_response(response):
        sys.exit(1)
