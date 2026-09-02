from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
import random
import secrets
import subprocess
import sys
import threading
import time

import ydb.apps.dstool.lib.cluster_workload_config as workload_config
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.grouptool as grouptool
import ydb.core.protos.key_pb2 as key_proto
import ydb.public.api.protos.ydb_bridge_common_pb2 as ydb_bridge_common
from google.protobuf import text_format


description = 'Create workload to stress failure model'


class _PendingNodeRestarts:
    def __init__(self):
        self._node_ids = set()
        self._lock = threading.Lock()

    def add(self, node_id):
        with self._lock:
            if node_id in self._node_ids:
                return False
            self._node_ids.add(node_id)
            return True

    def discard(self, node_id):
        with self._lock:
            self._node_ids.discard(node_id)

    def __contains__(self, node_id):
        with self._lock:
            return node_id in self._node_ids


class _ActionBecameIneligible(RuntimeError):
    pass


def _ensure_action_eligible(eligible, target):
    if not eligible:
        raise _ActionBecameIneligible('%s is no longer eligible' % target)


def _apply_cms_bsc_allowances(request, action_config):
    cms = action_config.ask_cms
    if cms is None:
        return request

    mode = cms.availability_mode
    if mode > workload_config.CmsAvailabilityMode.MAX_AVAILABILITY:
        request.IgnoreDegradedGroupsChecks = True
    if mode == workload_config.CmsAvailabilityMode.FORCE_RESTART:
        request.IgnoreGroupFailModelChecks = True
        request.IgnoreDisintegratedGroupsChecks = True
    return request


def add_options(p):
    p.add_argument(
        '--config-file',
        type=str,
        help='Path to a YAML workload configuration. If set, legacy options are ignored.',
    )

    # Legacy options are retained for compatibility and translated to the YAML
    # configuration proto before the workload starts.
    p.add_argument('--disable-wipes', action='store_true', help='Disable VDisk wipes')
    p.add_argument('--disable-readonly', action='store_true', help='Disable VDisk SetVDiskReadOnly requests')
    p.add_argument('--disable-evicts', action='store_true', help='Disable VDisk evicts')
    p.add_argument('--disable-restarts', action='store_true', help='Disable node restarts')
    p.add_argument('--enable-pdisk-encryption-keys-changes', action='store_true', help='Enable changes of PDisk encryption keys')
    p.add_argument('--enable-kill-tablets', action='store_true', help='Enable tablet killer')
    p.add_argument('--enable-kill-blob-depot', action='store_true', help='Enable BlobDepot killer')
    p.add_argument('--enable-restart-pdisks', action='store_true', help='Enable PDisk restarter')
    p.add_argument('--enable-readonly-pdisks', action='store_true', help='Enable SetPDiskReadOnly requests')
    p.add_argument('--kill-signal', type=str, default='KILL', help='Kill signal to send to restart node')
    p.add_argument('--sleep-before-rounds', type=float, default=1, help='Seconds to sleep before rounds')
    p.add_argument('--no-fail-model-check', action='store_true', help='Do not check VDisk states before taking action')
    p.add_argument('--enable-soft-switch-piles', action='store_true', help='Enable soft switch pile with PROMOTED')
    p.add_argument('--enable-hard-switch-piles', action='store_true', help='Enable hard switch pile with setting PRIMARY')
    p.add_argument('--enable-disconnect-piles', action='store_true', help='Enable disconnect pile')
    p.add_argument('--fixed-pile-for-disconnect', type=int, help='Pile to disconnect')
    p.add_argument('--weight-restarts', type=float, default=1.0, help='weight for restart action')
    p.add_argument('--weight-kill-tablets', type=float, default=1.0, help='weight for kill tablets action')


PDISK_KEY_CONFIG_PATH = '/Berkanavt/kikimr/cfg/pdisk_key.txt'


def make_pdisk_key_config(config):
    return text_format.MessageToString(config)


def _read_pdisk_key_config(host):
    try:
        result = subprocess.run(
            ['ssh', host, 'sudo', 'cat', PDISK_KEY_CONFIG_PATH],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        text = result.stdout.decode('utf-8')
        config = key_proto.TKeyConfig()
        text_format.Parse(text, config)
    except (subprocess.CalledProcessError, UnicodeError, text_format.ParseError) as error:
        raise RuntimeError(
            'Failed to read the existing PDisk key configuration from %s:%s: %s'
            % (host, PDISK_KEY_CONFIG_PATH, error)
        ) from error
    if not config.Keys:
        raise RuntimeError(
            'Existing PDisk key configuration on %s contains no keys' % host
        )
    return config


def _verify_pdisk_key_config_argument(host, pid):
    try:
        result = subprocess.run(
            ['ssh', host, 'sudo', 'cat', '/proc/%s/cmdline' % pid],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
    except subprocess.CalledProcessError as error:
        raise RuntimeError(
            'Failed to inspect the YDB command line on %s for PID %s: %s'
            % (host, pid, error)
        ) from error

    try:
        arguments = [
            argument.decode('utf-8')
            for argument in result.stdout.split(b'\0')
            if argument
        ]
    except UnicodeError as error:
        raise RuntimeError(
            'YDB command line on %s for PID %s is not valid UTF-8'
            % (host, pid)
        ) from error
    configured_path = None
    for index, argument in enumerate(arguments):
        if argument == '--pdisk-key-file' and index + 1 < len(arguments):
            configured_path = arguments[index + 1]
            break
        if argument.startswith('--pdisk-key-file='):
            configured_path = argument.partition('=')[2]
            break
    if configured_path != PDISK_KEY_CONFIG_PATH:
        raise RuntimeError(
            'ChangePDiskKey requires the YDB process on %s to use '
            '--pdisk-key-file %s (found %r)'
            % (host, PDISK_KEY_CONFIG_PATH, configured_path)
        )


def update_pdisk_key_config(node_fqdn_map, config, key_files, node_id):
    host = node_fqdn_map[node_id]
    # Install key material before publishing a configuration that references
    # it.  `sudo tee` also keeps the privileged write on the remote side.
    for path, contents in key_files:
        if path:
            temporary_key_path = path + '.dstool.tmp'
            subprocess.run(
                ['ssh', host, 'sudo', 'tee', temporary_key_path],
                input=contents,
                stdout=subprocess.DEVNULL,
                check=True,
            )
            subprocess.check_call(['ssh', host, 'sudo', 'chmod', '600', temporary_key_path])
            subprocess.check_call(['ssh', host, 'sudo', 'mv', '-f', temporary_key_path, path])
    config_path = PDISK_KEY_CONFIG_PATH
    temporary_path = config_path + '.dstool.tmp'
    subprocess.run(
        ['ssh', host, 'sudo', 'tee', temporary_path],
        input=make_pdisk_key_config(config).encode('utf-8'),
        stdout=subprocess.DEVNULL,
        check=True,
    )
    subprocess.check_call(['ssh', host, 'sudo', 'mv', '-f', temporary_path, config_path])


def _wait_for_pdisk_state(node_id, pdisk_id, expected_state, timeout_seconds=60):
    deadline = time.monotonic() + timeout_seconds
    while True:
        state = common.fetch_json_info('pdiskinfo', nodes=(node_id,)).get(
            (node_id, pdisk_id),
            {},
        ).get('State')
        if state == expected_state:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                'Timed out waiting for PDisk %d:%d to enter state %s (last state: %s)'
                % (node_id, pdisk_id, expected_state, state)
            )
        time.sleep(1)


def _wait_for_node_restart(node_id, previous_start_time, timeout_seconds=60):
    deadline = time.monotonic() + timeout_seconds
    while True:
        node = common.fetch_json_info('sysinfo', nodes=(node_id,)).get(node_id, {})
        start_time = node.get('StartTime')
        if start_time is not None and int(start_time) > previous_start_time:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                'Timed out waiting for node %d to restart (last start time: %s)'
                % (node_id, start_time)
            )
        time.sleep(1)


def _restart_node_process(
    node_id,
    previous_start_time,
    host,
    pid,
    signal_number,
    record_disruption,
):
    subprocess.check_call([
        'ssh',
        host,
        'sudo',
        'kill',
        '-' + signal_number,
        pid,
    ])
    record_disruption()
    _wait_for_node_restart(node_id, previous_start_time)


def _terminate_suspended_node_process(host, pid, signal_number):
    try:
        subprocess.check_call([
            'ssh',
            host,
            'sudo',
            'kill',
            '-' + signal_number,
            pid,
        ])
    finally:
        # A terminating signal can be handled by the process.  Always resume
        # it so that it can either handle that signal or continue running when
        # the signal delivery failed.
        subprocess.call(['ssh', host, 'sudo', 'kill', '-CONT', pid])


def _finish_suspended_node_restart(
    node_id,
    previous_start_time,
    host,
    pid,
    signal_number,
    keep_down_for,
):
    time.sleep(keep_down_for)
    _terminate_suspended_node_process(host, pid, signal_number)
    _wait_for_node_restart(node_id, previous_start_time)


def _start_kept_down_node_restart(
    node_id,
    previous_start_time,
    host,
    pid,
    signal_number,
    keep_down_for,
    record_disruption,
    report_error,
    on_complete,
):
    """Suspend a node now and finish its restart on a background thread."""
    subprocess.check_call(['ssh', host, 'sudo', 'kill', '-STOP', pid])
    try:
        # STOP is already a disruption; record it before returning control to
        # the action stream.
        record_disruption()

        def worker():
            try:
                _finish_suspended_node_restart(
                    node_id,
                    previous_start_time,
                    host,
                    pid,
                    signal_number,
                    keep_down_for,
                )
            except Exception as error:
                # Exceptions on a thread do not flow through the main action
                # loop.  Report them explicitly and let the workload continue.
                report_error(error)
            finally:
                on_complete()

        thread = threading.Thread(
            target=worker,
            name='dstool-restart-node-%d' % node_id,
            # A non-daemon worker is intentional: normal process shutdown must
            # not strand a remotely STOPped YDB process.
            daemon=False,
        )
        thread.start()
        return thread
    except Exception:
        # If the worker cannot be started after STOP was delivered, recover
        # synchronously rather than leaving the process suspended indefinitely.
        try:
            _terminate_suspended_node_process(host, pid, signal_number)
        finally:
            on_complete()
        raise


def _wait_for_node_storage(
    node_id,
    expected_pdisk_ids,
    expected_vslot_ids,
    timeout_seconds=300,
    stable_polls=2,
):
    deadline = time.monotonic() + timeout_seconds
    consecutive_healthy_polls = 0
    last_pdisk_states = {}
    last_unhealthy_vslots = set(expected_vslot_ids)

    while True:
        pdisks = common.fetch_json_info('pdiskinfo', nodes=(node_id,))
        vdisks = common.fetch_json_info('vdiskinfo', nodes=(node_id,))
        last_pdisk_states = {
            pdisk_id: pdisks.get((node_id, pdisk_id), {}).get('State')
            for pdisk_id in expected_pdisk_ids
        }
        last_unhealthy_vslots = {
            vslot_id
            for vslot_id in expected_vslot_ids
            if not (
                vdisks.get(vslot_id, {}).get('VDiskState') == 'OK'
                and vdisks.get(vslot_id, {}).get('Replicated') is True
            )
        }
        healthy = (
            all(state == 'Normal' for state in last_pdisk_states.values())
            and not last_unhealthy_vslots
        )
        consecutive_healthy_polls = consecutive_healthy_polls + 1 if healthy else 0
        if consecutive_healthy_polls >= stable_polls:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                'Timed out waiting for storage on node %d to recover '
                '(PDisk states: %s, unhealthy VSlots: %s)'
                % (node_id, last_pdisk_states, sorted(last_unhealthy_vslots))
            )
        time.sleep(1)


def _pick_action(rng, choices):
    action_name, action = rng.choice(choices)
    print(action_name)
    action[0](*action[1:])


def _add_possible_action(possible_actions, config, name, choices):
    if choices:
        possible_actions.append((
            config.weight,
            name,
            (_pick_action, choices),
        ))


def _make_disconnect_socket_request(target_node_id):
    request = common.kikimr_msgbus.TInterconnectDebug(
        ClosePeerSocketNodeId=target_node_id,
    )
    if common.connection_params.token is not None:
        request.SecurityToken = common.connection_params.token
    return request


def _vslot_is_writable_and_healthy(
    vslot_id,
    vslot,
    vdisk_is_healthy,
    vslot_readonly,
    pdisk_readonly,
):
    return (
        vslot.Ready
        and vdisk_is_healthy
        and vslot_id not in vslot_readonly
        and vslot_id[:2] not in pdisk_readonly
    )


def do(args):
    if args.dry_run:
        raise common.InvalidParameterError(
            common.connection_params.parser,
            '--dry-run',
            True,
            'Dry-run is not supported for the cluster workload because not all actions can be simulated safely.',
        )

    try:
        config = workload_config.parse_workload_config(args)
    except ValueError as error:
        parameter_name = '--config-file' if args.config_file else 'workload options'
        parameter = args.config_file if args.config_file else '<legacy options>'
        raise common.InvalidParameterError(
            common.connection_params.parser,
            parameter_name,
            parameter,
            str(error),
        ) from error
    actions = config.actions
    check_fail_model = config.check_fail_model
    sleep_between_rounds = config.sleep_between_rounds
    max_node_restarts_per_minute = config.max_node_restarts_per_minute
    rng = random.Random(config.random_seed)

    recent_restarts = deque()
    pending_node_restarts = _PendingNodeRestarts()
    config_retries = None

    has_tablet_actions = bool(actions.kill_tablet)
    has_pile_actions = bool(actions.switch_pile or actions.disconnect_pile)
    has_socket_actions = bool(actions.disconnect_socket)

    pile_name_to_endpoints = {}

    while True:
        common.flush_cache()

        try:
            base_config = common.fetch_base_config()
            vslot_map = common.build_vslot_map(base_config)
            node_fqdn_map = common.build_node_fqdn_map(base_config)
        except Exception:
            if config_retries is None:
                config_retries = 3
            elif config_retries == 0:
                raise
            else:
                config_retries -= 1
            continue

        if has_pile_actions:
            pile_name_to_node_id = common.build_pile_to_node_id_map(base_config)
            node_id_to_endpoints = common.fetch_node_to_endpoint_map()
            discovered_pile_endpoints = {
                pile_name: [
                    node_id_to_endpoints[node_id]
                    for node_id in pile_name_to_node_id[pile_name]
                    if node_id in node_id_to_endpoints
                    and node_id_to_endpoints[node_id].grpc_port is not None
                ]
                for pile_name in pile_name_to_node_id
            }
            # Preserve working endpoints across transient viewer omissions;
            # reconnect requests themselves are routed through the live
            # primary pile.
            for pile_name, endpoints in discovered_pile_endpoints.items():
                if endpoints:
                    pile_name_to_endpoints[pile_name] = endpoints
        else:
            node_id_to_endpoints = (
                common.fetch_node_to_endpoint_map()
                if has_socket_actions
                else {}
            )

        if has_tablet_actions:
            tablets = {
                int(tablet['TabletId']): tablet
                for tablet in common.fetch(
                    'viewer/json/tabletinfo',
                    dict(enums=1),
                    cache=False,
                ).get('TabletStateInfo', [])
            }
        else:
            tablets = {}

        sysinfo = {
            int(node['NodeId']): node
            for node in common.fetch(
                'viewer/json/sysinfo',
                dict(fields_required=-1, enums=1),
                cache=False,
            ).get('SystemStateInfo', [])
        }
        start_time_map = {
            node_id: int(node['StartTime'])
            for node_id, node in sysinfo.items()
            if 'StartTime' in node
        }
        node_tenant = {}
        for node_id, node in sysinfo.items():
            tenant_paths = node.get('Tenants', ())
            if isinstance(tenant_paths, str):
                node_tenant[node_id] = tenant_paths
            elif len(tenant_paths) == 1:
                node_tenant[node_id] = tenant_paths[0]
            else:
                # Dynamic nodes with zero or multiple tenant paths belong to
                # serverless layouts, which this workload intentionally does
                # not target.
                node_tenant[node_id] = None

        node_types = {
            node.NodeId: (
                workload_config.NodeType.DYNAMIC
                if node.Type == common.kikimr_bsconfig.NT_DYNAMIC
                else workload_config.NodeType.STORAGE
            )
            for node in base_config.Node
            if node.Type in (
                common.kikimr_bsconfig.NT_STATIC,
                common.kikimr_bsconfig.NT_DYNAMIC,
            )
            if node.Type != common.kikimr_bsconfig.NT_DYNAMIC
            or node_tenant.get(node.NodeId) is not None
        }
        pdisk_map = {
            (pdisk.NodeId, pdisk.PDiskId): pdisk
            for pdisk in base_config.PDisk
        }
        pdisks_by_node = defaultdict(list)
        for pdisk in base_config.PDisk:
            pdisks_by_node[pdisk.NodeId].append(pdisk)
        config_retries = None

        if any(vslot.Ready and vslot.Status != 'READY' for vslot in base_config.VSlot):
            common.print_if_not_quiet(
                args,
                'BaseConfig is changing; waiting for the next round...',
                file=sys.stdout,
            )
            time.sleep(sleep_between_rounds)
            continue

        vslot_readonly = {
            common.get_vslot_id(vslot.VSlotId)
            for vslot in base_config.VSlot
            if vslot.ReadOnly
        }
        pdisk_readonly = {
            (pdisk.NodeId, pdisk.PDiskId)
            for pdisk in base_config.PDisk
            if pdisk.ReadOnly
        }

        if check_fail_model:
            vdisk_status = defaultdict(lambda: False)
            error = False
            for vslot_id, vdisk in common.fetch_json_info('vdiskinfo').items():
                try:
                    key = *vslot_id, *common.get_vdisk_id_json(vdisk['VDiskId'])
                    vdisk_status[key] = vdisk['Replicated'] and vdisk['VDiskState'] == 'OK'
                except KeyError:
                    common.print_if_not_quiet(
                        args,
                        'Failed to fetch VDisk status for VSlotId %s' % (vslot_id,),
                        file=sys.stderr,
                    )
                    error = True
            if error:
                common.print_if_not_quiet(args, 'Waiting for the next round...', file=sys.stdout)
                time.sleep(sleep_between_rounds)
                continue

        def can_act_on_vslot(node_id, pdisk_id=None, vslot_id=None):
            if node_id in pending_node_restarts:
                return False
            if not check_fail_model:
                return True

            def match(value):
                return (
                    node_id == value[0]
                    and pdisk_id in (None, value[1])
                    and vslot_id in (None, value[2])
                )

            for group in base_config.Group:
                if any(map(match, map(common.get_vslot_id, group.VSlotId))):
                    content = {
                        common.get_vdisk_id_short(vslot): (
                            not match(current_vslot_id)
                            and current_vslot_id[0] not in pending_node_restarts
                            and _vslot_is_writable_and_healthy(
                                current_vslot_id,
                                vslot,
                                vdisk_status[current_vslot_id + common.get_vdisk_id(vslot)],
                                vslot_readonly,
                                pdisk_readonly,
                            )
                        )
                        for current_vslot_id in map(common.get_vslot_id, group.VSlotId)
                        for vslot in [vslot_map[current_vslot_id]]
                    }
                    common.print_if_verbose(args, content, file=sys.stderr)
                    if not grouptool.check_fail_model(content, group.ErasureSpecies):
                        return False
            return True

        def can_act_on_pdisk(node_id, pdisk_id):
            if node_id in pending_node_restarts:
                return False
            if not check_fail_model:
                return True

            def match(value):
                return node_id == value[0] and pdisk_id == value[1]

            for group in base_config.Group:
                if any(map(match, map(common.get_vslot_id, group.VSlotId))):
                    if not common.is_dynamic_group(group.GroupId):
                        return False

                    content = {
                        common.get_vdisk_id_short(vslot): (
                            not match(current_vslot_id)
                            and current_vslot_id[0] not in pending_node_restarts
                            and _vslot_is_writable_and_healthy(
                                current_vslot_id,
                                vslot,
                                vdisk_status[current_vslot_id + common.get_vdisk_id(vslot)],
                                vslot_readonly,
                                pdisk_readonly,
                            )
                        )
                        for current_vslot_id in map(common.get_vslot_id, group.VSlotId)
                        for vslot in [vslot_map[current_vslot_id]]
                    }
                    common.print_if_verbose(args, content, file=sys.stderr)
                    if not grouptool.check_fail_model(content, group.ErasureSpecies):
                        return False
            return True

        def can_request_cms(action_config, node_id):
            return (
                action_config.ask_cms is None
                or (
                    node_id in sysinfo
                    and node_id in node_types
                    and bool(sysinfo[node_id].get('Host'))
                )
            )

        def ask_cms(action_config, node_id, pdisk_id=None, pdisk_ids=None, duration_seconds=60):
            if action_config.ask_cms is None:
                return

            if pdisk_id is None and pdisk_ids is None:
                action_type = common.kikimr_cms.TAction.RESTART_SERVICES
                services = (
                    'dynnode'
                    if node_types[node_id] == workload_config.NodeType.DYNAMIC
                    else 'storage',
                )
                devices = ()
            else:
                action_type = common.kikimr_cms.TAction.REPLACE_DEVICES
                services = ()
                if pdisk_ids is None:
                    pdisk_ids = (pdisk_id,)
                devices = tuple(pdisk_map[(node_id, item)].Path for item in pdisk_ids)

            error = common.cms_permission_request(
                'dstool-workload',
                str(node_id),
                'dstool cluster workload action',
                max(1, round(duration_seconds * 1_000_000)),
                action_config.ask_cms.availability_mode,
                action_type,
                services=services,
                devices=devices,
            )
            if error is not None:
                raise RuntimeError('CMS permission was not granted: %s' % error)

        def do_restart(node_id, action_config):
            _ensure_action_eligible(
                can_act_on_vslot(node_id),
                'node %d' % node_id,
            )
            node = sysinfo[node_id]
            keep_down_for = action_config.keep_down_for
            ask_cms(action_config, node_id, duration_seconds=max(60, keep_down_for))

            pid = str(node['PID'])
            host = node['Host']
            signal_number = str(workload_config.restart_signal_number(action_config.signal))
            if keep_down_for:
                if not pending_node_restarts.add(node_id):
                    return
                try:
                    _start_kept_down_node_restart(
                        node_id,
                        start_time_map[node_id],
                        host,
                        pid,
                        signal_number,
                        keep_down_for,
                        lambda: recent_restarts.append(datetime.now(timezone.utc)),
                        lambda error: common.print_if_not_quiet(
                            args,
                            'Failed to complete kept-down restart for node %d: %s'
                            % (node_id, error),
                            file=sys.stderr,
                        ),
                        lambda: pending_node_restarts.discard(node_id),
                    )
                except Exception:
                    # The helper also calls on_complete when setup fails, but
                    # discard here keeps this invariant local and idempotent.
                    pending_node_restarts.discard(node_id)
                    raise
                return

            _restart_node_process(
                node_id,
                start_time_map[node_id],
                host,
                pid,
                signal_number,
                lambda: recent_restarts.append(datetime.now(timezone.utc)),
            )

        def do_restart_pdisk(node_id, pdisk_id, action_config):
            _ensure_action_eligible(
                can_act_on_pdisk(node_id, pdisk_id),
                'PDisk %d:%d' % (node_id, pdisk_id),
            )
            ask_cms(action_config, node_id, pdisk_id)
            request = _apply_cms_bsc_allowances(
                common.kikimr_bsconfig.TConfigRequest(Rollback=args.dry_run),
                action_config,
            )
            command = request.Command.add().RestartPDisk
            command.TargetPDiskId.NodeId = node_id
            command.TargetPDiskId.PDiskId = pdisk_id
            try:
                response = common.invoke_bsc_request(request)
            except Exception as error:
                raise RuntimeError('failed to perform restart request: %s' % error) from error
            if not response.Success:
                raise RuntimeError('Unexpected error from BSC: %s' % response.ErrorDescription)

        def do_readonly_pdisk(node_id, pdisk_id, read_only, action_config):
            if read_only:
                _ensure_action_eligible(
                    can_act_on_pdisk(node_id, pdisk_id),
                    'PDisk %d:%d' % (node_id, pdisk_id),
                )
            ask_cms(action_config, node_id, pdisk_id)
            request = _apply_cms_bsc_allowances(
                common.kikimr_bsconfig.TConfigRequest(Rollback=args.dry_run),
                action_config,
            )
            command = request.Command.add().SetPDiskReadOnly
            command.TargetPDiskId.NodeId = node_id
            command.TargetPDiskId.PDiskId = pdisk_id
            command.Value = read_only
            try:
                response = common.invoke_bsc_request(request)
            except Exception as error:
                raise RuntimeError('failed to perform SetPDiskReadOnly request: %s' % error) from error
            if not response.Success:
                raise RuntimeError('Unexpected error from BSC: %s' % response.ErrorDescription)

        def do_evict(vslot_id, action_config):
            _ensure_action_eligible(
                can_act_on_vslot(*vslot_id),
                'VSlot %s' % (vslot_id,),
            )
            ask_cms(action_config, vslot_id[0], vslot_id[1])
            try:
                request = _apply_cms_bsc_allowances(
                    common.kikimr_bsconfig.TConfigRequest(Rollback=args.dry_run),
                    action_config,
                )
                vslot = vslot_map[vslot_id]
                command = request.Command.add().ReassignGroupDisk
                command.GroupId = vslot.GroupId
                command.GroupGeneration = vslot.GroupGeneration
                command.FailRealmIdx = vslot.FailRealmIdx
                command.FailDomainIdx = vslot.FailDomainIdx
                command.VDiskIdx = vslot.VDiskIdx
                command.SuppressDonorMode = rng.choice([True, False])
                response = common.invoke_bsc_request(request)
                if not response.Success:
                    if 'Error# failed to allocate group: no group options' in response.ErrorDescription:
                        common.print_if_verbose(args, response)
                    else:
                        raise RuntimeError('Unexpected error from BSC: %s' % response.ErrorDescription)
            except Exception as error:
                raise RuntimeError('Failed to perform evict request: %s' % error) from error

        def do_wipe(vslot, action_config):
            vslot_id = common.get_vslot_id(vslot.VSlotId)
            _ensure_action_eligible(
                can_act_on_vslot(*vslot_id),
                'VSlot %s' % (vslot_id,),
            )
            ask_cms(action_config, vslot_id[0], vslot_id[1])
            try:
                request = common.create_wipe_request(args, vslot)
                _apply_cms_bsc_allowances(request, action_config)
                response = common.invoke_bsc_request(request)
                if not response.Success:
                    raise RuntimeError('Unexpected error from BSC: %s' % response.ErrorDescription)
            except Exception as error:
                raise RuntimeError('Failed to perform wipe request: %s' % error) from error

        def do_readonly_vdisk(vslot, action_config):
            read_only = action_config.read_only
            vslot_id = common.get_vslot_id(vslot.VSlotId)
            if read_only:
                _ensure_action_eligible(
                    can_act_on_vslot(*vslot_id),
                    'VSlot %s' % (vslot_id,),
                )
            ask_cms(action_config, vslot_id[0], vslot_id[1])
            try:
                request = common.create_readonly_request(args, vslot, read_only)
                _apply_cms_bsc_allowances(request, action_config)
                response = common.invoke_bsc_request(request)
                if not response.Success:
                    raise RuntimeError('Unexpected error from BSC: %s' % response.ErrorDescription)
            except Exception as error:
                raise RuntimeError('Failed to perform readonly request: %s' % error) from error

        def do_add_pdisk_key(node_id, action_config):
            _ensure_action_eligible(
                can_act_on_vslot(node_id),
                'node %d' % node_id,
            )
            # PDisk keys are loaded only at process startup. This action owns
            # the required restart so a key-change-only workload still performs
            # a complete re-encryption cycle.
            node = sysinfo[node_id]
            _verify_pdisk_key_config_argument(node['Host'], node['PID'])
            current_config = _read_pdisk_key_config(node_fqdn_map[node_id])
            ask_cms(action_config, node_id, duration_seconds=300)
            version = max(key.Version for key in current_config.Keys) + 1
            if version >= 2 ** 64:
                raise RuntimeError('PDisk key version has reached the uint64 limit')
            key_path = '/Berkanavt/kikimr/cfg/pdisk_key_%d.txt' % version
            updated_config = key_proto.TKeyConfig()
            updated_config.CopyFrom(current_config)
            updated_config.Keys.add(
                ContainerPath=key_path,
                Pin=b'',
                Id='Key%d' % version,
                Version=version,
            )
            update_pdisk_key_config(
                node_fqdn_map,
                updated_config,
                ((key_path, secrets.token_bytes(32)),),
                node_id,
            )
            subprocess.check_call([
                'ssh',
                node['Host'],
                'sudo',
                'kill',
                '-9',
                str(node['PID']),
            ])
            _wait_for_node_restart(node_id, start_time_map[node_id])
            _wait_for_node_storage(
                node_id,
                tuple(pdisk.PDiskId for pdisk in pdisks_by_node[node_id]),
                tuple(
                    common.get_vslot_id(vslot.VSlotId)
                    for vslot in base_config.VSlot
                    if vslot.VSlotId.NodeId == node_id
                ),
            )
            # Older keys stay in the remote file so an interrupted or partially
            # completed rotation cannot make an encrypted disk unreadable.

        def do_obliterate_pdisk(node_id, pdisk_id, action_config):
            _ensure_action_eligible(
                can_act_on_pdisk(node_id, pdisk_id),
                'PDisk %d:%d' % (node_id, pdisk_id),
            )
            ask_cms(action_config, node_id, pdisk_id, duration_seconds=600)
            pdisk = pdisk_map[(node_id, pdisk_id)]
            expected_vslot_ids = tuple(
                common.get_vslot_id(vslot.VSlotId)
                for vslot in base_config.VSlot
                if vslot.VSlotId.NodeId == node_id
                and vslot.VSlotId.PDiskId == pdisk_id
            )
            stop_request = _apply_cms_bsc_allowances(
                common.kikimr_bsconfig.TConfigRequest(),
                action_config,
            )
            stop_command = stop_request.Command.add().StopPDisk
            stop_command.TargetPDiskId.NodeId = node_id
            stop_command.TargetPDiskId.PDiskId = pdisk_id
            response = common.invoke_bsc_request(stop_request)
            if not response.Success:
                raise RuntimeError('Failed to stop PDisk: %s' % response.ErrorDescription)
            try:
                _wait_for_pdisk_state(node_id, pdisk_id, 'Stopped')
                subprocess.check_call([
                    'ssh',
                    sysinfo[node_id]['Host'],
                    'sudo',
                    '/Berkanavt/kikimr/bin/kikimr',
                    'admin',
                    'bs',
                    'disk',
                    'obliterate',
                    pdisk.Path,
                ])
            finally:
                restart_request = _apply_cms_bsc_allowances(
                    common.kikimr_bsconfig.TConfigRequest(),
                    action_config,
                )
                restart_command = restart_request.Command.add().RestartPDisk
                restart_command.TargetPDiskId.NodeId = node_id
                restart_command.TargetPDiskId.PDiskId = pdisk_id
                response = common.invoke_bsc_request(restart_request)
                if not response.Success:
                    raise RuntimeError('Failed to restart PDisk: %s' % response.ErrorDescription)
                _wait_for_node_storage(
                    node_id,
                    (pdisk_id,),
                    expected_vslot_ids,
                    timeout_seconds=600,
                )

        def do_kill_tablet(tablet):
            tablet_id = int(tablet['TabletId'])
            print('Killing tablet %d of type %s' % (tablet_id, tablet['Type']))
            common.fetch(
                'tablets',
                dict(RestartTabletID=tablet_id),
                fmt='raw',
                cache=False,
            )

        def do_soft_switch_pile(pile_name):
            print('Switching primary pile to %s with PROMOTED' % pile_name)
            common.promote_pile(pile_name)

        def do_hard_switch_pile(pile_name, all_piles):
            print('Switching primary pile to %s with setting PRIMARY' % pile_name)
            common.set_primary_pile(pile_name, [item for item in all_piles if item != pile_name])

        def do_disconnect_pile(pile_name, replacement_primaries=()):
            print('Disconnecting pile %s' % pile_name)
            replacement_primary = (
                rng.choice(replacement_primaries)
                if replacement_primaries
                else None
            )
            common.disconnect_pile(pile_name, replacement_primary)

        def do_connect_pile(pile_name, primary_pile_name):
            print('Connecting pile %s' % pile_name)
            common.connect_pile(pile_name, primary_pile_name, pile_name_to_endpoints)

        def do_disconnect_socket(source_node_id, target_node_id, action_config):
            if action_config.symmetrical and rng.choice((False, True)):
                source_node_id, target_node_id = target_node_id, source_node_id
            print('Disconnecting socket from node %d to node %d' % (source_node_id, target_node_id))
            request = _make_disconnect_socket_request(target_node_id)
            response = common.invoke_grpc(
                'InterconnectDebug',
                request,
                endpoint=node_id_to_endpoints[source_node_id],
            )
            if response.Status != common.kikimr_msgbus.MSTATUS_OK:
                raise RuntimeError('InterconnectDebug failed with status %s' % response.Status)

        now = datetime.now(timezone.utc)
        while recent_restarts and recent_restarts[0] + timedelta(minutes=1) < now:
            recent_restarts.popleft()

        possible_actions = []

        active_tablets = [
            tablet
            for tablet in tablets.values()
            if tablet.get('State') == 'Active' and tablet.get('Leader')
        ]
        for action_config in actions.kill_tablet:
            choices = [
                (
                    'kill tablet %s of type %s' % (tablet['TabletId'], tablet.get('Type', 'unknown')),
                    (do_kill_tablet, tablet),
                )
                for tablet in active_tablets
                if action_config.matches(
                    int(tablet['TabletId']),
                    workload_config.tablet_type_from_name(tablet.get('Type', '')),
                )
            ]
            _add_possible_action(possible_actions, action_config, 'kill-tablet', choices)

        dynamic_vslots = [
            vslot
            for vslot in base_config.VSlot
            if common.is_dynamic_group(vslot.GroupId)
        ]

        for action_config in actions.evict_vdisk:
            choices = []
            for vslot in dynamic_vslots:
                vslot_id = common.get_vslot_id(vslot.VSlotId)
                if (
                    can_request_cms(action_config, vslot_id[0])
                    and can_act_on_vslot(*vslot_id)
                ):
                    choices.append((
                        'evict vslot id: %s' % (vslot_id,),
                        (do_evict, vslot_id, action_config),
                    ))
            _add_possible_action(possible_actions, action_config, 'evict-vdisk', choices)

        for action_config in actions.wipe_vdisk:
            choices = []
            for vslot in dynamic_vslots:
                vslot_id = common.get_vslot_id(vslot.VSlotId)
                if (
                    can_request_cms(action_config, vslot_id[0])
                    and can_act_on_vslot(*vslot_id)
                ):
                    choices.append((
                        'wipe vslot id: %s' % (vslot_id,),
                        (do_wipe, vslot, action_config),
                    ))
            _add_possible_action(possible_actions, action_config, 'wipe-vdisk', choices)

        for action_config in actions.set_read_only:
            if action_config.component == workload_config.ReadOnlyComponent.VDISK:
                choices = []
                for vslot in dynamic_vslots:
                    vslot_id = common.get_vslot_id(vslot.VSlotId)
                    is_read_only = vslot_id in vslot_readonly
                    if is_read_only == action_config.read_only:
                        continue
                    if not can_request_cms(action_config, vslot_id[0]):
                        continue
                    if action_config.read_only and (
                        not can_act_on_vslot(*vslot_id)
                    ):
                        continue
                    choices.append((
                        '%s vslot id: %s'
                        % ('readonly' if action_config.read_only else 'un-readonly', vslot_id),
                        (do_readonly_vdisk, vslot, action_config),
                    ))
                _add_possible_action(possible_actions, action_config, 'set-vdisk-readonly', choices)
            else:
                choices = []
                for pdisk in base_config.PDisk:
                    pdisk_id = (pdisk.NodeId, pdisk.PDiskId)
                    is_read_only = pdisk_id in pdisk_readonly
                    if is_read_only == action_config.read_only:
                        continue
                    if not can_request_cms(action_config, pdisk.NodeId):
                        continue
                    if action_config.read_only and not can_act_on_pdisk(*pdisk_id):
                        continue
                    choices.append((
                        '%s pdisk node_id: %d, pdisk_id: %d'
                        % (
                            'readonly' if action_config.read_only else 'un-readonly',
                            pdisk.NodeId,
                            pdisk.PDiskId,
                        ),
                        (do_readonly_pdisk, pdisk.NodeId, pdisk.PDiskId, action_config.read_only, action_config),
                    ))
                _add_possible_action(possible_actions, action_config, 'set-pdisk-readonly', choices)

        for action_config in actions.restart_pdisk:
            choices = [
                (
                    'restart pdisk node_id: %d, pdisk_id: %d' % (pdisk.NodeId, pdisk.PDiskId),
                    (do_restart_pdisk, pdisk.NodeId, pdisk.PDiskId, action_config),
                )
                for pdisk in base_config.PDisk
                if can_request_cms(action_config, pdisk.NodeId)
                and can_act_on_pdisk(pdisk.NodeId, pdisk.PDiskId)
            ]
            _add_possible_action(possible_actions, action_config, 'restart-pdisk', choices)

        for action_config in actions.change_pdisk_key:
            choices = [
                (
                    'add new pdisk key for node_id: %d' % node_id,
                    (do_add_pdisk_key, node_id, action_config),
                )
                for node_id in sorted(pdisks_by_node)
                if node_id in node_fqdn_map
                and node_types.get(node_id) == workload_config.NodeType.STORAGE
                and node_id in start_time_map
                and node_id in sysinfo
                and bool(sysinfo[node_id].get('Host'))
                and 'PID' in sysinfo[node_id]
                and can_request_cms(action_config, node_id)
                and can_act_on_vslot(node_id)
                and all(
                    (node_id, pdisk.PDiskId) not in pdisk_readonly
                    for pdisk in pdisks_by_node[node_id]
                )
            ]
            _add_possible_action(possible_actions, action_config, 'change-pdisk-key', choices)

        for action_config in actions.obliterate_pdisk:
            choices = [
                (
                    'obliterate pdisk node_id: %d, pdisk_id: %d' % (pdisk.NodeId, pdisk.PDiskId),
                    (do_obliterate_pdisk, pdisk.NodeId, pdisk.PDiskId, action_config),
                )
                for pdisk in base_config.PDisk
                if pdisk.NodeId in sysinfo
                and bool(sysinfo[pdisk.NodeId].get('Host'))
                and can_request_cms(action_config, pdisk.NodeId)
                and can_act_on_pdisk(pdisk.NodeId, pdisk.PDiskId)
            ]
            _add_possible_action(possible_actions, action_config, 'obliterate-pdisk', choices)

        if (
            start_time_map
            and (
                max_node_restarts_per_minute is None
                or len(recent_restarts) < max_node_restarts_per_minute
            )
        ):
            for action_config in actions.restart_node:
                eligible_nodes = [
                    node_id
                    for node_id in sorted(start_time_map, key=start_time_map.__getitem__)
                    if node_id in node_types
                    and node_id not in pending_node_restarts
                    and bool(sysinfo[node_id].get('Host'))
                    and 'PID' in sysinfo[node_id]
                    if can_act_on_vslot(node_id)
                    and action_config.node_filter.matches(
                        node_id,
                        node_types[node_id],
                        node_tenant.get(node_id),
                    )
                ]
                choices = [
                    (
                        'restart node with id: %d' % node_id,
                        (do_restart, node_id, action_config),
                    )
                    for node_id in eligible_nodes
                ]
                _add_possible_action(
                    possible_actions,
                    action_config,
                    'restart-node',
                    choices,
                )

        if has_pile_actions:
            piles_info = common.get_piles_info()
            pile_names = [pile_state.pile_name for pile_state in piles_info.pile_states]
            piles_count = len(pile_names)
            primary_pile = None
            synchronized_piles = []
            promoted_piles = []
            disconnected_piles = []
            for pile_state in piles_info.pile_states:
                pile_name = pile_state.pile_name
                if pile_state.state == ydb_bridge_common.PileState.PRIMARY:
                    primary_pile = pile_name
                elif pile_state.state == ydb_bridge_common.PileState.SYNCHRONIZED:
                    synchronized_piles.append(pile_name)
                elif pile_state.state == ydb_bridge_common.PileState.PROMOTED:
                    promoted_piles.append(pile_name)
                elif pile_state.state == ydb_bridge_common.PileState.DISCONNECTED:
                    disconnected_piles.append(pile_name)

            can_soft_switch = piles_count == len(synchronized_piles) + int(primary_pile is not None)
            all_connected_piles = (
                ([primary_pile] if primary_pile is not None else [])
                + promoted_piles
                + synchronized_piles
            )
            for action_config in actions.switch_pile:
                if action_config.mode == workload_config.SwitchPileMode.SOFT:
                    choices = [
                        (
                            'soft-switch to pile %s' % pile_name,
                            (do_soft_switch_pile, pile_name),
                        )
                        for pile_name in synchronized_piles
                        if can_soft_switch
                    ]
                    _add_possible_action(possible_actions, action_config, 'soft-switch-pile', choices)
                else:
                    choices = [
                        (
                            'hard-switch to pile %s' % pile_name,
                            (do_hard_switch_pile, pile_name, all_connected_piles),
                        )
                        for pile_name in promoted_piles + synchronized_piles
                    ]
                    _add_possible_action(possible_actions, action_config, 'hard-switch-pile', choices)

            for action_config in actions.disconnect_pile:
                if action_config.operation == workload_config.DisconnectPileOperation.DISCONNECT:
                    eligible_piles = (
                        ([primary_pile] if primary_pile is not None else [])
                        + synchronized_piles
                    )
                    name = 'disconnect-pile'
                else:
                    eligible_piles = disconnected_piles
                    name = 'reconnect-pile'
                if action_config.pile is not None:
                    if action_config.pile >= len(pile_names):
                        eligible_piles = []
                    else:
                        selected_pile = pile_names[action_config.pile]
                        eligible_piles = [
                            pile_name
                            for pile_name in eligible_piles
                            if pile_name == selected_pile
                        ]
                if action_config.operation == workload_config.DisconnectPileOperation.DISCONNECT:
                    choices = [
                        (
                            '%s %s' % (name, pile_name),
                            (
                                do_disconnect_pile,
                                pile_name,
                                tuple(synchronized_piles) if pile_name == primary_pile else (),
                            ),
                        )
                        for pile_name in eligible_piles
                        if pile_name != primary_pile or synchronized_piles
                    ]
                else:
                    choices = [
                        (
                            '%s %s' % (name, pile_name),
                            (do_connect_pile, pile_name, primary_pile),
                        )
                        for pile_name in eligible_piles
                        if primary_pile is not None
                        and pile_name_to_endpoints.get(primary_pile)
                    ]
                _add_possible_action(possible_actions, action_config, name, choices)

        for action_config in actions.disconnect_socket:
            source_nodes = [
                node_id
                for node_id in node_id_to_endpoints
                if node_id in sysinfo and node_id in node_types
                and node_id not in pending_node_restarts
                and action_config.source.matches(
                    node_id,
                    node_types[node_id],
                    node_tenant.get(node_id),
                )
            ]
            target_nodes = [
                node_id
                for node_id in node_id_to_endpoints
                if node_id in sysinfo and node_id in node_types
                and node_id not in pending_node_restarts
                and action_config.target.matches(
                    node_id,
                    node_types[node_id],
                    node_tenant.get(node_id),
                )
            ]
            choices = [
                (
                    'disconnect socket from node %d to node %d' % (source_node_id, target_node_id),
                    (do_disconnect_socket, source_node_id, target_node_id, action_config),
                )
                for source_node_id in source_nodes
                for target_node_id in target_nodes
                if source_node_id != target_node_id
            ]
            _add_possible_action(possible_actions, action_config, 'disconnect-socket', choices)

        if not possible_actions:
            common.print_if_not_quiet(args, 'Waiting for the next round...', file=sys.stdout)
            time.sleep(sleep_between_rounds)
            continue

        selected, = rng.choices(
            possible_actions,
            weights=[weight for weight, _, _ in possible_actions],
        )
        _, action_name, action = selected
        print('%s %s' % (action_name, datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')))

        try:
            action[0](rng, *action[1:])
        except _ActionBecameIneligible as error:
            common.print_if_not_quiet(
                args,
                'Skipped action %s: %s' % (action_name, error),
                file=sys.stderr,
            )
        except Exception as error:
            common.print_if_not_quiet(
                args,
                'Failed to perform action: %s with error: %s' % (action_name, error),
                file=sys.stderr,
            )

        common.print_if_not_quiet(args, 'Waiting for the next round...', file=sys.stdout)
        time.sleep(sleep_between_rounds)
