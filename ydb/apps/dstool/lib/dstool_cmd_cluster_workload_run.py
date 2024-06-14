import ydb.apps.dstool.lib.common as common
import time
import random
import subprocess
import ydb.apps.dstool.lib.grouptool as grouptool
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import sys

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
    p.add_argument('--kill-signal', type=str, default='KILL', help='Kill signal to send to restart node')


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

    while True:
        common.flush_cache()

        try:
            base_config = common.fetch_base_config()
            vslot_map = common.build_vslot_map(base_config)
            node_fqdn_map = common.build_node_fqdn_map(base_config)
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

        if (len(pdisk_keys) == 0):
            # initialize pdisk_keys and pdisk_key_versions
            for node_id in {pdisk.NodeId for pdisk in base_config.PDisk}:
                pdisk_key_versions[node_id] = 1
                pdisk_keys[node_id] = [{"path" : "", "pin" : "", "id" : "0", "version" : 0, "file" : ""}]

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

        def do_restart(node_id):
            host = node_fqdn_map[node_id]
            if args.enable_pdisk_encryption_keys_changes:
                update_pdisk_key_config(node_fqdn_map, pdisk_keys, node_id)
            subprocess.call(['ssh', host, 'sudo', 'killall', '-%s' % args.kill_signal, 'kikimr'])
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

        for vslot in base_config.VSlot:
            if common.is_dynamic_group(vslot.GroupId):
                vslot_id = common.get_vslot_id(vslot.VSlotId)
                vdisk_id = '[%08x:%d:%d:%d]' % (vslot.GroupId, vslot.FailRealmIdx, vslot.FailDomainIdx, vslot.VDiskIdx)
                if vslot_id in vslot_readonly and not args.disable_readonly:
                    unreadonlies.append(('un-readonly vslot id: %s, vdisk id: %s' % (vslot_id, vdisk_id), (do_readonly, vslot, False)))
                if can_act_on_vslot(*vslot_id[:2]) and args.enable_restart_pdisks:
                    pdisk_restarts.append(('restart pdisk node_id: %d, pdisk_id: %d' % vslot_id[:2], (do_restart_pdisk, *vslot_id[:2])))
                if can_act_on_vslot(*vslot_id) and (recent_restarts or args.disable_restarts):
                    if not args.disable_evicts:
                        evicts.append(('evict vslot id: %s, vdisk id: %s' % (vslot_id, vdisk_id), (do_evict, vslot_id)))
                    if not args.disable_wipes:
                        wipes.append(('wipe vslot id: %s, vdisk id: %s' % (vslot_id, vdisk_id), (do_wipe, vslot)))
                    if not args.disable_readonly:
                        readonlies.append(('readonly vslot id: %s, vdisk id: %s' % (vslot_id, vdisk_id), (do_readonly, vslot, True)))

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

        restarts = []

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
        time.sleep(1)
