from collections import defaultdict
from datetime import datetime, timedelta, timezone
import random
import subprocess
import sys
import time

import ydb.apps.dstool.lib.cluster_workload_config as workload_config
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.grouptool as grouptool
import ydb.public.api.protos.draft.ydb_bridge_pb2 as ydb_bridge


description = 'Create workload to stress failure model'


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


def make_pdisk_key_config(pdisk_keys, node_id):
    result = ''
    for key in pdisk_keys[node_id]:
        result += 'Keys {\n'
        result += '  ContainerPath: \\"' + key['path'] + '\\"\n'
        result += '  Pin: \\"' + key['pin'] + '\\"\n'
        result += '  Id: \\"' + key['id'] + '\\"\n'
        result += '  Version: ' + str(key['version']) + '\n'
        result += '}\n'
    return result


def remove_old_pdisk_keys(pdisk_keys, pdisk_key_versions, node_id):
    version = pdisk_key_versions[node_id]
    pdisk_keys[node_id] = [
        key
        for key in pdisk_keys[node_id]
        if key['version'] == version
    ]


def update_pdisk_key_config(node_fqdn_map, pdisk_keys, node_id):
    host = node_fqdn_map[node_id]
    subprocess.run(
        '''ssh {0} "sudo echo '{1}' > /Berkanavt/kikimr/cfg/pdisk_key.txt"'''.format(
            host,
            make_pdisk_key_config(pdisk_keys, node_id),
        ),
        shell=True,
        check=True,
    )
    for key in pdisk_keys[node_id]:
        if key['path']:
            subprocess.run(
                '''ssh {0} "echo '{1}' | sudo tee {2} >/dev/null"'''.format(
                    host,
                    key['file'],
                    key['path'],
                ),
                shell=True,
                check=True,
            )


def _pick_action(rng, choices):
    action_name, action = rng.choice(choices)
    print(action_name)
    action[0](*action[1:])


def _add_possible_action(possible_actions, config, name, choices, is_node_restart=False):
    if choices:
        possible_actions.append((
            config.weight,
            name,
            (_pick_action, choices),
            is_node_restart,
        ))


def do(args):
    config = workload_config.parse_workload_config(args)
    actions = config.actions
    check_fail_model = config.check_fail_model
    sleep_between_rounds = config.sleep_between_rounds
    max_node_restarts_per_minute = config.max_node_restarts_per_minute
    rng = random.Random(config.random_seed)

    recent_restarts = []
    pdisk_keys = {}
    pdisk_key_versions = {}
    config_retries = None

    has_pdisk_key_changes = bool(actions.change_pdisk_key)
    has_tablet_actions = bool(actions.kill_tablet)
    has_pile_actions = bool(actions.switch_pile or actions.disconnect_pile)
    has_socket_actions = bool(actions.disconnect_socket)

    if has_pile_actions:
        base_config = common.fetch_base_config()
        pile_name_to_node_id = common.build_pile_to_node_id_map(base_config)
        piles_count = len(pile_name_to_node_id)
        node_id_to_endpoints = common.fetch_node_to_endpoint_map()
        pile_names = list(sorted(pile_name_to_node_id.keys()))
        pile_id_to_endpoints = {
            index: [node_id_to_endpoints[node_id] for node_id in pile_name_to_node_id[pile_name]]
            for index, pile_name in enumerate(pile_names)
        }

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
        node_tenants = {}
        for node_id, node in sysinfo.items():
            tenants = node.get('Tenants', ())
            node_tenants[node_id] = (tenants,) if isinstance(tenants, str) else tenants

        dynamic_node_ids = {
            node.NodeId
            for node in base_config.Node
            if node.Type == common.kikimr_bsconfig.NT_DYNAMIC
        }
        node_types = defaultdict(lambda: workload_config.NodeType.DYNAMIC)
        node_types.update({
            node_id: (
                workload_config.NodeType.DYNAMIC
                if node_id in dynamic_node_ids
                else workload_config.NodeType.STORAGE
            )
            for node_id in sysinfo
        })
        pdisk_map = {
            (pdisk.NodeId, pdisk.PDiskId): pdisk
            for pdisk in base_config.PDisk
        }
        node_id_to_endpoints = common.fetch_node_to_endpoint_map() if has_socket_actions else {}

        config_retries = None

        for vslot in base_config.VSlot:
            assert not vslot.Ready or vslot.Status == 'READY'

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

        if not pdisk_keys:
            for node_id in {pdisk.NodeId for pdisk in base_config.PDisk}:
                pdisk_key_versions[node_id] = 1
                pdisk_keys[node_id] = [{
                    'path': '',
                    'pin': '',
                    'id': '0',
                    'version': 0,
                    'file': '',
                }]

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
                            and vslot.Ready
                            and vdisk_status[current_vslot_id + common.get_vdisk_id(vslot)]
                        )
                        for current_vslot_id in map(common.get_vslot_id, group.VSlotId)
                        for vslot in [vslot_map[current_vslot_id]]
                    }
                    common.print_if_verbose(args, content, file=sys.stderr)
                    if not grouptool.check_fail_model(content, group.ErasureSpecies):
                        return False
            return True

        def can_act_on_pdisk(node_id, pdisk_id):
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
                            and vslot.Ready
                            and vdisk_status[current_vslot_id + common.get_vdisk_id(vslot)]
                        )
                        for current_vslot_id in map(common.get_vslot_id, group.VSlotId)
                        for vslot in [vslot_map[current_vslot_id]]
                    }
                    common.print_if_verbose(args, content, file=sys.stderr)
                    if not grouptool.check_fail_model(content, group.ErasureSpecies):
                        return False
            return True

        def ask_cms(action_config, node_id, pdisk_id=None, duration_seconds=60):
            if action_config.ask_cms is None:
                return

            node = sysinfo[node_id]
            if pdisk_id is None:
                action_type = common.kikimr_cms.TAction.RESTART_SERVICES
                services = (
                    'dynnode'
                    if node_types[node_id] == workload_config.NodeType.DYNAMIC
                    else 'storage',
                )
                devices = ()
            else:
                action_type = common.kikimr_cms.TAction.REMOVE_DEVICES
                services = ()
                devices = (pdisk_map[(node_id, pdisk_id)].Path,)

            error = common.cms_permission_request(
                'dstool-workload',
                node['Host'],
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
            node = sysinfo[node_id]
            keep_down_for = action_config.keep_down_for
            ask_cms(action_config, node_id, duration_seconds=max(60, keep_down_for))
            if has_pdisk_key_changes and node_id in pdisk_keys:
                update_pdisk_key_config(node_fqdn_map, pdisk_keys, node_id)

            pid = str(node['PID'])
            host = node['Host']
            if keep_down_for:
                # Suspend first so the deployment's process supervisor cannot
                # immediately bring the node back. At the end of the interval,
                # the configured restart signal is delivered and the process
                # is resumed as a fallback for non-terminating signals.
                subprocess.check_call(['ssh', host, 'sudo', 'kill', '-STOP', pid])
                try:
                    time.sleep(keep_down_for)
                    subprocess.check_call([
                        'ssh',
                        host,
                        'sudo',
                        'kill',
                        '-' + action_config.signal,
                        pid,
                    ])
                finally:
                    subprocess.call(['ssh', host, 'sudo', 'kill', '-CONT', pid])
            else:
                subprocess.check_call([
                    'ssh',
                    host,
                    'sudo',
                    'kill',
                    '-' + action_config.signal,
                    pid,
                ])

            if has_pdisk_key_changes and node_id in pdisk_keys:
                remove_old_pdisk_keys(pdisk_keys, pdisk_key_versions, node_id)

        def do_restart_pdisk(node_id, pdisk_id, action_config):
            assert can_act_on_pdisk(node_id, pdisk_id)
            ask_cms(action_config, node_id, pdisk_id)
            request = common.kikimr_bsconfig.TConfigRequest(IgnoreDegradedGroupsChecks=True)
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
            assert not read_only or can_act_on_pdisk(node_id, pdisk_id)
            ask_cms(action_config, node_id, pdisk_id)
            request = common.kikimr_bsconfig.TConfigRequest(IgnoreDegradedGroupsChecks=True)
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
            assert can_act_on_vslot(*vslot_id)
            ask_cms(action_config, vslot_id[0], vslot_id[1])
            try:
                request = common.kikimr_bsconfig.TConfigRequest(IgnoreDegradedGroupsChecks=True)
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
            assert can_act_on_vslot(*vslot_id)
            ask_cms(action_config, vslot_id[0], vslot_id[1])
            try:
                request = common.create_wipe_request(args, vslot)
                common.invoke_bsc_request(request)
            except Exception as error:
                raise RuntimeError('Failed to perform wipe request: %s' % error) from error

        def do_readonly_vdisk(vslot, action_config):
            read_only = action_config.read_only
            vslot_id = common.get_vslot_id(vslot.VSlotId)
            assert not read_only or can_act_on_vslot(*vslot_id)
            ask_cms(action_config, vslot_id[0], vslot_id[1])
            try:
                request = common.create_readonly_request(args, vslot, read_only)
                common.invoke_bsc_request(request)
            except Exception as error:
                raise RuntimeError('Failed to perform readonly request: %s' % error) from error

        def do_add_pdisk_key(node_id, pdisk_id, action_config):
            assert can_act_on_pdisk(node_id, pdisk_id)
            ask_cms(action_config, node_id, pdisk_id)
            pdisk_key_versions[node_id] += 1
            version = pdisk_key_versions[node_id]
            pdisk_keys[node_id].append({
                'path': '/Berkanavt/kikimr/cfg/pdisk_key_%d.txt' % version,
                'pin': '',
                'id': 'Key%d' % version,
                'version': version,
                'file': 'keynumber%d' % version,
            })

        def do_obliterate_pdisk(node_id, pdisk_id, action_config):
            assert can_act_on_pdisk(node_id, pdisk_id)
            ask_cms(action_config, node_id, pdisk_id)
            pdisk = pdisk_map[(node_id, pdisk_id)]
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

        def do_kill_tablet(tablet):
            tablet_id = int(tablet['TabletId'])
            print('Killing tablet %d of type %s' % (tablet_id, tablet['Type']))
            common.fetch(
                'tablets',
                dict(RestartTabletID=tablet_id),
                fmt='raw',
                cache=False,
            )

        def do_soft_switch_pile(pile_id):
            print('Switching primary pile to %d with PROMOTED' % pile_id)
            common.promote_pile(pile_id)

        def do_hard_switch_pile(pile_id, all_piles):
            print('Switching primary pile to %d with setting PRIMARY' % pile_id)
            common.set_primary_pile(pile_id, [item for item in all_piles if item != pile_id])

        def do_disconnect_pile(pile_id):
            print('Disconnecting pile %d' % pile_id)
            common.disconnect_pile(pile_id, pile_id_to_endpoints)

        def do_connect_pile(pile_id):
            print('Connecting pile %d' % pile_id)
            common.connect_pile(pile_id, pile_id_to_endpoints)

        def do_disconnect_socket(source_node_id, target_node_id, action_config):
            if action_config.symmetrical and rng.choice((False, True)):
                source_node_id, target_node_id = target_node_id, source_node_id
            print('Disconnecting socket from node %d to node %d' % (source_node_id, target_node_id))
            request = common.kikimr_msgbus.TInterconnectDebug(
                ClosePeerSocketNodeId=target_node_id,
            )
            response = common.invoke_grpc(
                'InterconnectDebug',
                request,
                endpoint=node_id_to_endpoints[source_node_id],
            )
            if response.Status != common.kikimr_msgbus.MSTATUS_OK:
                raise RuntimeError('InterconnectDebug failed with status %s' % response.Status)

        now = datetime.now(timezone.utc)
        while recent_restarts and recent_restarts[0] + timedelta(minutes=1) < now:
            recent_restarts.pop(0)

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

        allow_destructive_vdisk_action = recent_restarts or not actions.restart_node
        dynamic_vslots = [
            vslot
            for vslot in base_config.VSlot
            if common.is_dynamic_group(vslot.GroupId)
        ]

        for action_config in actions.evict_vdisk:
            choices = []
            for vslot in dynamic_vslots:
                vslot_id = common.get_vslot_id(vslot.VSlotId)
                if allow_destructive_vdisk_action and can_act_on_vslot(*vslot_id):
                    choices.append((
                        'evict vslot id: %s' % (vslot_id,),
                        (do_evict, vslot_id, action_config),
                    ))
            _add_possible_action(possible_actions, action_config, 'evict-vdisk', choices)

        for action_config in actions.wipe_vdisk:
            choices = []
            for vslot in dynamic_vslots:
                vslot_id = common.get_vslot_id(vslot.VSlotId)
                if allow_destructive_vdisk_action and can_act_on_vslot(*vslot_id):
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
                    if action_config.read_only and (
                        not allow_destructive_vdisk_action
                        or not can_act_on_vslot(*vslot_id)
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
                if can_act_on_pdisk(pdisk.NodeId, pdisk.PDiskId)
            ]
            _add_possible_action(possible_actions, action_config, 'restart-pdisk', choices)

        for action_config in actions.change_pdisk_key:
            choices = [
                (
                    'add new pdisk key for node_id: %d, pdisk_id: %d' % (pdisk.NodeId, pdisk.PDiskId),
                    (do_add_pdisk_key, pdisk.NodeId, pdisk.PDiskId, action_config),
                )
                for pdisk in base_config.PDisk
                if can_act_on_pdisk(pdisk.NodeId, pdisk.PDiskId)
            ]
            _add_possible_action(possible_actions, action_config, 'change-pdisk-key', choices)

        for action_config in actions.obliterate_pdisk:
            choices = [
                (
                    'obliterate pdisk node_id: %d, pdisk_id: %d' % (pdisk.NodeId, pdisk.PDiskId),
                    (do_obliterate_pdisk, pdisk.NodeId, pdisk.PDiskId, action_config),
                )
                for pdisk in base_config.PDisk
                if can_act_on_pdisk(pdisk.NodeId, pdisk.PDiskId)
            ]
            _add_possible_action(possible_actions, action_config, 'obliterate-pdisk', choices)

        if start_time_map and len(recent_restarts) < max_node_restarts_per_minute:
            for action_config in actions.restart_node:
                eligible_nodes = [
                    node_id
                    for node_id in sorted(start_time_map, key=start_time_map.__getitem__)
                    if can_act_on_vslot(node_id)
                    and action_config.node_filter.matches(
                        node_id,
                        node_types[node_id],
                        node_tenants.get(node_id, ()),
                    )
                ]
                # Keep the newest half of the eligible nodes untouched. Nodes
                # that are the sole match are still safe because the failure
                # model check above covers every VDisk hosted by the node.
                eligible_nodes = eligible_nodes[:max(1, len(eligible_nodes) // 2)]
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
                    is_node_restart=True,
                )

        if has_pile_actions:
            piles_info = common.get_piles_info()
            primary_pile = None
            synchronized_piles = []
            promoted_piles = []
            disconnected_piles = []
            for index, pile_state in enumerate(piles_info.per_pile_state):
                if pile_state.state == ydb_bridge.PileState.PRIMARY:
                    primary_pile = index
                elif pile_state.state == ydb_bridge.PileState.SYNCHRONIZED:
                    synchronized_piles.append(index)
                elif pile_state.state == ydb_bridge.PileState.PROMOTED:
                    promoted_piles.append(index)
                elif pile_state.state == ydb_bridge.PileState.DISCONNECTED:
                    disconnected_piles.append(index)

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
                            'soft-switch to pile %d' % pile_id,
                            (do_soft_switch_pile, pile_id),
                        )
                        for pile_id in synchronized_piles
                        if can_soft_switch
                    ]
                    _add_possible_action(possible_actions, action_config, 'soft-switch-pile', choices)
                else:
                    choices = [
                        (
                            'hard-switch to pile %d' % pile_id,
                            (do_hard_switch_pile, pile_id, all_connected_piles),
                        )
                        for pile_id in promoted_piles + synchronized_piles
                    ]
                    _add_possible_action(possible_actions, action_config, 'hard-switch-pile', choices)

            for action_config in actions.disconnect_pile:
                if action_config.operation == workload_config.DisconnectPileOperation.DISCONNECT:
                    eligible_piles = (
                        ([primary_pile] if primary_pile is not None else [])
                        + synchronized_piles
                    )
                    operation = do_disconnect_pile
                    name = 'disconnect-pile'
                else:
                    eligible_piles = disconnected_piles
                    operation = do_connect_pile
                    name = 'reconnect-pile'
                if action_config.pile is not None:
                    eligible_piles = [
                        pile_id
                        for pile_id in eligible_piles
                        if pile_id == action_config.pile
                    ]
                choices = [
                    (
                        '%s %d' % (name, pile_id),
                        (operation, pile_id),
                    )
                    for pile_id in eligible_piles
                ]
                _add_possible_action(possible_actions, action_config, name, choices)

        for action_config in actions.disconnect_socket:
            source_nodes = [
                node_id
                for node_id in node_id_to_endpoints
                if node_id in sysinfo
                and action_config.source.matches(
                    node_id,
                    node_types[node_id],
                    node_tenants.get(node_id, ()),
                )
            ]
            target_nodes = [
                node_id
                for node_id in node_id_to_endpoints
                if node_id in sysinfo
                and action_config.target.matches(
                    node_id,
                    node_types[node_id],
                    node_tenants.get(node_id, ()),
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
            weights=[weight for weight, _, _, _ in possible_actions],
        )
        _, action_name, action, is_node_restart = selected
        print('%s %s' % (action_name, datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')))

        try:
            action[0](rng, *action[1:])
            if is_node_restart:
                recent_restarts.append(now)
        except Exception as error:
            common.print_if_not_quiet(
                args,
                'Failed to perform action: %s with error: %s' % (action_name, error),
                file=sys.stderr,
            )

        common.print_if_not_quiet(args, 'Waiting for the next round...', file=sys.stdout)
        time.sleep(sleep_between_rounds)
