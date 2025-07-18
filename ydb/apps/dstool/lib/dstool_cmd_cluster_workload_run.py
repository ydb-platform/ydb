import ydb.apps.dstool.lib.common as common
import time
import random
import subprocess
import ydb.apps.dstool.lib.grouptool as grouptool
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import sys
import ydb.public.api.protos.draft.ydb_bridge_pb2 as ydb_bridge

description = 'Create workload to stress failure model'


def add_options(p):
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
    p.add_argument('--enable-soft-switch-piles', action='store_true', help='Enable soft switch pile with PROMOTE')
    p.add_argument('--enable-hard-switch-piles', action='store_true', help='Enable hard switch pile with setting PRIMARY')
    p.add_argument('--enable-disconnect-piles', action='store_true', help='Enable disconnect pile')
    p.add_argument('--fixed-pile-for-disconnect', type=int, help='Pile to disconnect')


def fetch_start_time_map(base_config):
    start_time_map = {}
    for node_id in {pdisk.NodeId for pdisk in base_config.PDisk}:
        r = common.fetch_json_info('sysinfo', [node_id])
        if len(r) != 1:
            return None
        k, v = r.popitem()
        assert k == node_id
        if 'StartTime' not in v:
            return None
        start_time_map[node_id] = int(v['StartTime'])
    return start_time_map


def make_pdisk_key_config(pdisk_keys, node_id):
    s = ""
    for key in pdisk_keys[node_id]:
        s += "Keys {" + "\n"
        s += "  ContainerPath: " + "\\\"" + key["path"] + "\\\"" + "\n"
        s += "  Pin: " + "\\\"" + key["pin"] + "\\\"" + "\n"
        s += "  Id: " + "\\\"" + key["id"] + "\\\"" + "\n"
        s += "  Version: " + str(key["version"]) + "\n"
        s += "}" + "\n"
    return s


def remove_old_pdisk_keys(pdisk_keys, pdisk_key_versions, node_id):
    v = pdisk_key_versions[node_id]
    for pdisk_key in pdisk_keys[node_id]:
        if pdisk_key["version"] != v:
            pdisk_keys[node_id].remove(pdisk_key)


def update_pdisk_key_config(node_fqdn_map, pdisk_keys, node_id):
    host = node_fqdn_map[node_id]
    subprocess.run('''ssh {0} "sudo echo '{1}' > /Berkanavt/kikimr/cfg/pdisk_key.txt"'''.format(host, make_pdisk_key_config(pdisk_keys, node_id)), shell=True)
    for key in pdisk_keys[node_id]:
        if (len(key["path"]) > 0):
            subprocess.run('''ssh {0} "echo '{1}' | sudo tee {2} >/dev/null"'''.format(host, key["file"], key["path"]), shell=True)


def do(args):
    recent_restarts = []

    pdisk_keys = {}
    pdisk_key_versions = {}

    config_retries = None

    if args.enable_soft_switch_piles or args.enable_hard_switch_piles or args.enable_disconnect_piles:
        base_config = common.fetch_base_config()
        pile_name_to_node_id = common.build_pile_to_node_id_map(base_config)
        piles_count = len(pile_name_to_node_id)
        node_id_to_endpoints = common.fetch_node_to_endpoint_map()
        pile_names = list(sorted(pile_name_to_node_id.keys()))
        pile_id_to_endpoints = {
            idx: [node_id_to_endpoints[node_id] for node_id in pile_name_to_node_id[pile_name]]
            for idx, pile_name in enumerate(pile_names)
        }

    while True:
        common.flush_cache()

        try:
            base_config = common.fetch_base_config()
            vslot_map = common.build_vslot_map(base_config)
            node_fqdn_map = common.build_node_fqdn_map(base_config)
            if args.enable_pdisk_encryption_keys_changes or not args.disable_restarts:
                start_time_map = fetch_start_time_map(base_config)
        except Exception:
            if config_retries is None:
                config_retries = 3
            elif config_retries == 0:
                raise
            else:
                config_retries -= 1
            continue

        tablets = common.fetch_json_info('tabletinfo') if args.enable_kill_tablets or args.enable_kill_blob_depot else {}

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

        if (len(pdisk_keys) == 0):
            # initialize pdisk_keys and pdisk_key_versions
            for node_id in {pdisk.NodeId for pdisk in base_config.PDisk}:
                pdisk_key_versions[node_id] = 1
                pdisk_keys[node_id] = [{"path" : "", "pin" : "", "id" : "0", "version" : 0, "file" : ""}]

        if not args.no_fail_model_check:
            vdisk_status = defaultdict(lambda: False)
            error = False
            for vslot_id, vdisk in common.fetch_json_info('vdiskinfo').items():
                try:
                    key = *vslot_id, *common.get_vdisk_id_json(vdisk['VDiskId'])
                    vdisk_status[key] = vdisk['Replicated'] and vdisk['VDiskState'] == 'OK'
                except KeyError:
                    common.print_if_not_quiet(args, 'Failed to fetch VDisk status for VSlotId %s' % (vslot_id,), file=sys.stderr)
                    error = True
            if error:
                common.print_if_not_quiet(args, 'Waiting for the next round...', file=sys.stdout)
                time.sleep(1)
                continue

        def can_act_on_vslot(node_id, pdisk_id=None, vslot_id=None):
            if args.no_fail_model_check:
                return True

            def match(x):
                return node_id == x[0] and pdisk_id in [None, x[1]] and vslot_id in [None, x[2]]

            for group in base_config.Group:
                if any(map(match, map(common.get_vslot_id, group.VSlotId))):
                    content = {
                        common.get_vdisk_id_short(vslot): not match(vslot_id) and vslot.Ready and vdisk_status[vslot_id + common.get_vdisk_id(vslot)]
                        for vslot_id in map(common.get_vslot_id, group.VSlotId)
                        for vslot in [vslot_map[vslot_id]]
                    }
                    common.print_if_verbose(args, content, file=sys.stderr)
                    if not grouptool.check_fail_model(content, group.ErasureSpecies):
                        return False
            return True

        def can_act_on_pdisk(node_id, pdisk_id):
            def match(x):
                return node_id == x[0] and pdisk_id == x[1]

            for group in base_config.Group:
                if any(map(match, map(common.get_vslot_id, group.VSlotId))):
                    if not common.is_dynamic_group(group.GroupId):
                        return False

                    content = {
                        common.get_vdisk_id_short(vslot): not match(vslot_id) and vslot.Ready and vdisk_status[vslot_id + common.get_vdisk_id(vslot)]
                        for vslot_id in map(common.get_vslot_id, group.VSlotId)
                        for vslot in [vslot_map[vslot_id]]
                    }
                    common.print_if_verbose(args, content, file=sys.stderr)
                    if not grouptool.check_fail_model(content, group.ErasureSpecies):
                        return False
            return True

        def do_restart(node_id):
            host = node_fqdn_map[node_id]
            if args.enable_pdisk_encryption_keys_changes:
                update_pdisk_key_config(node_fqdn_map, pdisk_keys, node_id)
            subprocess.call(['ssh', host, 'sudo', 'killall', '-%s' % args.kill_signal, 'kikimr'])
            subprocess.call(['ssh', host, 'sudo', 'killall', '-%s' % args.kill_signal, 'ydbd'])
            if args.enable_pdisk_encryption_keys_changes:
                remove_old_pdisk_keys(pdisk_keys, pdisk_key_versions, node_id)

        def do_restart_pdisk(node_id, pdisk_id):
            assert can_act_on_vslot(node_id, pdisk_id)
            request = common.kikimr_bsconfig.TConfigRequest(IgnoreDegradedGroupsChecks=True)
            cmd = request.Command.add().RestartPDisk
            cmd.TargetPDiskId.NodeId = node_id
            cmd.TargetPDiskId.PDiskId = pdisk_id
            try:
                response = common.invoke_bsc_request(request)
            except Exception as e:
                raise Exception('failed to perform restart request: %s' % e)
            if not response.Success:
                raise Exception('Unexpected error from BSC: %s' % response.ErrorDescription)

        def do_readonly_pdisk(node_id, pdisk_id, readonly):
            assert can_act_on_vslot(node_id, pdisk_id)
            request = common.kikimr_bsconfig.TConfigRequest(IgnoreDegradedGroupsChecks=True)
            cmd = request.Command.add().SetPDiskReadOnly
            cmd.TargetPDiskId.NodeId = node_id
            cmd.TargetPDiskId.PDiskId = pdisk_id
            cmd.Value = readonly
            try:
                response = common.invoke_bsc_request(request)
            except Exception as e:
                raise Exception('failed to perform SetPDiskReadOnly request: %s' % e)
            if not response.Success:
                raise Exception('Unexpected error from BSC: %s' % response.ErrorDescription)

        def do_evict(vslot_id):
            assert can_act_on_vslot(*vslot_id)
            try:
                request = common.kikimr_bsconfig.TConfigRequest(IgnoreDegradedGroupsChecks=True)
                vslot = vslot_map[vslot_id]
                cmd = request.Command.add().ReassignGroupDisk
                cmd.GroupId = vslot.GroupId
                cmd.GroupGeneration = vslot.GroupGeneration
                cmd.FailRealmIdx = vslot.FailRealmIdx
                cmd.FailDomainIdx = vslot.FailDomainIdx
                cmd.VDiskIdx = vslot.VDiskIdx
                cmd.SuppressDonorMode = random.choice([True, False])
                response = common.invoke_bsc_request(request)
                if not response.Success:
                    if 'Error# failed to allocate group: no group options' in response.ErrorDescription:
                        common.print_if_verbose(args, response)
                    else:
                        raise Exception('Unexpected error from BSC: %s' % response.ErrorDescription)
            except Exception as e:
                raise Exception('Failed to perform evict request: %s' % e)

        def do_wipe(vslot):
            assert can_act_on_vslot(*common.get_vslot_id(vslot.VSlotId))
            try:
                request = common.create_wipe_request(args, vslot)
                common.invoke_bsc_request(request)
            except Exception as e:
                raise Exception('Failed to perform wipe request: %s' % e)

        def do_readonly(vslot, value):
            assert not value or can_act_on_vslot(*common.get_vslot_id(vslot.VSlotId))
            try:
                request = common.create_readonly_request(args, vslot, value)
                common.invoke_bsc_request(request)
            except Exception as e:
                raise Exception('Failed to perform readonly request: %s' % e)

        def do_add_pdisk_key(node_id):
            pdisk_key_versions[node_id] += 1
            v = pdisk_key_versions[node_id]
            pdisk_keys[node_id].append({"path" : "/Berkanavt/kikimr/cfg/pdisk_key_" + str(v) + ".txt",
                                        "pin" : "",
                                        "id" : "Key" + str(v),
                                        "version" : v,
                                        "file" : "keynumber" + str(v)})

        def do_kill_tablet():
            tablet_list = [
                value
                for key, value in tablets.items()
                if value['State'] == 'Active' and value['Leader']
            ]
            item = random.choice(tablet_list)
            tablet_id = int(item['TabletId'])
            print('Killing tablet %d of type %s' % (tablet_id, item['Type']))
            common.fetch('tablets', dict(RestartTabletID=tablet_id), fmt='raw', cache=False)

        def do_kill_blob_depot():
            tablet_list = [
                value
                for key, value in tablets.items()
                if value['State'] == 'Active' and value['Leader'] and value['Type'] == 'BlobDepot'
            ]
            if tablet_list:
                item = random.choice(tablet_list)
                tablet_id = int(item['TabletId'])
                print('Killing tablet %d of type %s' % (tablet_id, item['Type']))
                common.fetch('tablets', dict(RestartTabletID=tablet_id), fmt='raw', cache=False)

        def do_soft_switch_pile(pile_id):
            print(f"Switching primary pile to {pile_id} with PROMOTE")
            common.promote_pile(pile_id)

        def do_hard_switch_pile(pile_id, all_piles):
            print(f"Switching primary pile to {pile_id} with setting PRIMARY")
            common.set_primary_pile(pile_id, [x for x in all_piles if x != pile_id])

        def do_disconnect_pile(pile_id):
            print(f"Disconnecting pile {pile_id}")
            common.disconnect_pile(pile_id)

        def do_connect_pile(pile_id, pile_id_to_hosts):
            print(f"Connecting pile {pile_id}")
            common.connect_pile(pile_id, pile_id_to_hosts)

        ################################################################################################################

        now = datetime.now(timezone.utc)
        while recent_restarts and recent_restarts[0] + timedelta(minutes=1) < now:
            recent_restarts.pop(0)

        possible_actions = []

        if args.enable_kill_tablets:
            possible_actions.append(('kill tablet', (do_kill_tablet,)))
        if args.enable_kill_blob_depot:
            possible_actions.append(('kill blob depot', (do_kill_blob_depot,)))

        evicts = []
        wipes = []
        readonlies = []
        unreadonlies = []
        pdisk_restarts = []
        make_pdisks_readonly = []
        make_pdisks_not_readonly = []

        for vslot in base_config.VSlot:
            if common.is_dynamic_group(vslot.GroupId):
                vslot_id = common.get_vslot_id(vslot.VSlotId)
                node_id, pdisk_id = vslot_id[:2]
                vdisk_id = '[%08x:%d:%d:%d]' % (vslot.GroupId, vslot.FailRealmIdx, vslot.FailDomainIdx, vslot.VDiskIdx)
                if vslot_id in vslot_readonly and not args.disable_readonly:
                    unreadonlies.append(('un-readonly vslot id: %s, vdisk id: %s' % (vslot_id, vdisk_id), (do_readonly, vslot, False)))
                if can_act_on_vslot(*vslot_id) and (recent_restarts or args.disable_restarts):
                    if not args.disable_evicts:
                        evicts.append(('evict vslot id: %s, vdisk id: %s' % (vslot_id, vdisk_id), (do_evict, vslot_id)))
                    if not args.disable_wipes:
                        wipes.append(('wipe vslot id: %s, vdisk id: %s' % (vslot_id, vdisk_id), (do_wipe, vslot)))
                    if not args.disable_readonly:
                        readonlies.append(('readonly vslot id: %s, vdisk id: %s' % (vslot_id, vdisk_id), (do_readonly, vslot, True)))

        for pdisk in base_config.PDisk:
            node_id, pdisk_id = pdisk.NodeId, pdisk.PDiskId

            if can_act_on_pdisk(node_id, pdisk_id):
                if args.enable_restart_pdisks:
                    pdisk_restarts.append(('restart pdisk node_id: %d, pdisk_id: %d' % (node_id, pdisk_id), (do_restart_pdisk, node_id, pdisk_id)))
                if args.enable_readonly_pdisks:
                    make_pdisks_readonly.append(('readonly pdisk node_id: %d, pdisk_id: %d' % (node_id, pdisk_id), (do_readonly_pdisk, node_id, pdisk_id, True)))

            if (node_id, pdisk_id) in pdisk_readonly and args.enable_readonly_pdisks:
                make_pdisks_not_readonly.append(('un-readonly pdisk node_id: %d, pdisk_id: %d' % (node_id, pdisk_id), (do_readonly_pdisk, node_id, pdisk_id, False)))

        def pick(v):
            action_name, action = random.choice(v)
            print(action_name)
            action[0](*action[1:])

        if evicts:
            possible_actions.append(('evict', (pick, evicts)))
        if wipes:
            possible_actions.append(('wipe', (pick, wipes)))
        if readonlies:
            possible_actions.append(('readonly', (pick, readonlies)))
        if unreadonlies:
            possible_actions.append(('un-readonly', (pick, unreadonlies)))
        if pdisk_restarts:
            possible_actions.append(('restart-pdisk', (pick, pdisk_restarts)))
        if make_pdisks_readonly:
            possible_actions.append(('make-pdisks-readonly', (pick, make_pdisks_readonly)))
        if make_pdisks_not_readonly:
            possible_actions.append(('make-pdisks-not-readonly', (pick, make_pdisks_not_readonly)))

        restarts = []

        if args.enable_pdisk_encryption_keys_changes or not args.disable_restarts:
            if start_time_map and len(recent_restarts) < 3:
                # sort so that the latest restarts come first
                nodes_to_restart = sorted(start_time_map, key=start_time_map.__getitem__)
                node_count = len(nodes_to_restart)
                nodes_to_restart = nodes_to_restart[:node_count//2]
                for node_id in nodes_to_restart:
                    if args.enable_pdisk_encryption_keys_changes:
                        possible_actions.append(('add new pdisk key to node with id: %d' % node_id, (do_add_pdisk_key, node_id)))
                    if not args.disable_restarts:
                        restarts.append(('restart node with id: %d' % node_id, (do_restart, node_id)))

        if restarts:
            possible_actions.append(('restart', (pick, restarts)))

        has_pile_operations = args.enable_soft_switch_piles or args.enable_hard_switch_piles or args.enable_disconnect_piles
        if has_pile_operations:
            piles_info = common.get_piles_info()
            print(piles_info)

            primary_pile = None
            synchronized_piles = []
            promoted_piles = []
            non_synchronized_piles = []
            disconnected_piles = []
            for idx, pile_state in enumerate(piles_info.per_pile_state):
                if pile_state.state == ydb_bridge.PileState.PRIMARY:
                    primary_pile = idx
                elif pile_state.state == ydb_bridge.PileState.SYNCHRONIZED:
                    synchronized_piles.append(idx)
                elif pile_state.state == ydb_bridge.PileState.PROMOTE:
                    promoted_piles.append(idx)
                elif pile_state.state == ydb_bridge.PileState.DISCONNECTED:
                    disconnected_piles.append(idx)
                else:
                    non_synchronized_piles.append(idx)

            can_soft_switch = (piles_count == len(synchronized_piles) + int(primary_pile is not None))
            can_hard_switch = (len(synchronized_piles) + len(promoted_piles) > 0)

            if args.enable_soft_switch_piles and can_soft_switch:
                possible_actions.append(('soft-switch-pile', (do_soft_switch_pile, random.choice(synchronized_piles))))
            if args.enable_hard_switch_piles and can_hard_switch:
                possible_actions.append(('hard-switch-pile', (do_hard_switch_pile, random.choice(promoted_piles + synchronized_piles), [primary_pile] + promoted_piles + synchronized_piles)))
            if len(disconnected_piles) > 0:
                possible_actions.append(('connect-pile', (do_connect_pile, random.choice(disconnected_piles), pile_id_to_endpoints)))
            if args.enable_disconnect_piles and len(synchronized_piles) > 0:
                pile_to_disconnect = args.fixed_pile_for_disconnect if args.fixed_pile_for_disconnect is not None else random.choice([primary_pile] + synchronized_piles)
                possible_actions.append(('disconnect-pile', (do_disconnect_pile, pile_to_disconnect)))

        if not possible_actions:
            common.print_if_not_quiet(args, 'Waiting for the next round...', file=sys.stdout)
            time.sleep(1)
            continue

        ################################################################################################################

        action_name, action = random.choice(possible_actions)
        print('%s %s' % (action_name, datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')))

        try:
            action[0](*action[1:])
            if action_name.startswith('restart'):
                recent_restarts.append(now)
        except Exception as e:
            common.print_if_not_quiet(args, 'Failed to perform action: %s with error: %s' % (action_name, e), file=sys.stderr)

        common.print_if_not_quiet(args, 'Waiting for the next round...', file=sys.stdout)
        time.sleep(args.sleep_before_rounds)
