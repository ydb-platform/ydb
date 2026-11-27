"""Run RFC 192 cycles with fixed DC identities and bounded BSQueue reconnect retries."""

import time

from ydb.core.protos.blobstorage_base3_pb2 import TDataCenterState, TGroupState
from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.functional.mirror3dc.kv_workload import BSQUEUE_RECONNECT_GRACE
from ydb.tests.functional.mirror3dc.transition_barrier import TransitionBarrier
from ydb.tests.functional.mirror3dc.transition_layout import check_ensured_layout


STATES = dict(zip(
    'N F F1 F2P F2 S1 S2P S2 R R1P R1 R2P R2'.split(),
    (TGroupState.NORMAL, TGroupState.DC_FAILURE, TGroupState.DC_FAILURE_INV1,
     TGroupState.DC_FAILURE_INV2_PENDING, TGroupState.DC_FAILURE_INV2,
     TGroupState.DC_SOLO_INV1, TGroupState.DC_SOLO_INV2_PENDING, TGroupState.DC_SOLO_INV2,
     TGroupState.DC_RECOVERY_INV1, TGroupState.DC_SOLO_RECOVERY_INV1_PENDING,
     TGroupState.DC_SOLO_RECOVERY_INV1, TGroupState.DC_SOLO_RECOVERY_INV2_PENDING,
     TGroupState.DC_SOLO_RECOVERY_INV2),
))
ROLES = dict(zip(STATES, 'III LII XII XII XII XXI XXI XXI RII XRI XRI XRI XRI'.split()))
ROLE_VALUES = dict(zip(
    'ILXR',
    (TDataCenterState.INCLUDED, TDataCenterState.LEAVING, TDataCenterState.EXCLUDED, TDataCenterState.RECOVERING),
))
AUTOMATIC = dict(zip('F F2P S2P R1P R2P R1 R2 R'.split(), 'F1 F2 S2 R1 R2 F1 F1 N'.split()))


def run_transition_cycle(test, route):
    route = route.split()
    test.assertEqual((route[0], route[-1]), ('N', 'N'))
    initial = test.get_group()
    slots = tuple(slot.SerializeToString() for slot in initial.VSlotId)
    physical_dcs = tuple(realm.DataCenter for realm in initial.FailRealms)
    baseline, visited = set(), []
    workload = None

    def realm_states(state):
        return dict(zip(test.data_centers, (ROLE_VALUES[role] for role in ROLES[state])))

    def snapshot():
        return test.kv_blobs(test.snapshot(physical=True)[1])

    def take_baseline():
        # Only this initial batch pauses writers; the entire route runs under load.
        baseline.update(snapshot())
        test.assertEqual(len(baseline), test.INITIAL_WRITES)

    def remember_workload(handle):
        nonlocal workload
        workload = handle

    def check_configuration(group, state, index):
        test.assertTrue(group.HasField('Mirror3dcState'), str(group))
        test.assertEqual(group.Mirror3dcState, STATES[state], str(group))
        test.assertEqual(group.GroupGeneration, initial.GroupGeneration + index, str(group))
        test.assertEqual(tuple(slot.SerializeToString() for slot in group.VSlotId), slots)
        test.assertEqual(tuple(realm.DataCenter for realm in group.FailRealms), physical_dcs)
        test.assertEqual({realm.DataCenter: realm.State for realm in group.FailRealms}, realm_states(state), str(group))

    def check_layout(state, blobs):
        roles = realm_states(state)
        check_ensured_layout(test, STATES[state], {i: roles[dc] for i, dc in enumerate(physical_dcs)}, blobs)

    def visit(index, barrier):
        state = route[index]
        generation = initial.GroupGeneration + index
        deadline = time.monotonic() + 120
        while True:
            workload.check_errors()
            group = test.get_group()
            if group.Mirror3dcState == STATES[state]:
                break
            test.assertGreater(index, 0, str(group))
            check_configuration(group, route[index - 1], index - 1)
            test.assertLess(time.monotonic(), deadline, f'Timed out entering {state}')
            time.sleep(0.1)
        check_configuration(group, state, index)
        if state in AUTOMATIC:
            barrier.wait_reached(STATES[state], generation=generation, to_state=STATES[AUTOMATIC[state]])
        progress = workload.wait_progress(min_writes=test.WRITERS, min_reads=test.WRITERS)

        def check_baseline():
            current = snapshot()
            test.assertFalse(baseline - current.keys(), 'Baseline KV blobs disappeared')
            check_layout(state, {blob: current[blob] for blob in baseline})

        retry_assertions(check_baseline, timeout_seconds=60, step_seconds=1)
        check_configuration(test.get_group(), state, index)
        visited.append(state)
        test.actions_logger.info('Cycle step %d: %s generation %d, roles=%s, KV %s, E_blobs=%d',
                                 index, TGroupState.EState.Name(STATES[state]), generation,
                                 realm_states(state), progress, len(baseline))

    def expect_reconnect(before, after, generation):
        test.actions_logger.info('CONTROL EXPECT_BSQUEUE_RECONNECT from=%s to=%s generation=%s',
                                 before, after, generation)
        workload.expect_reconnect(BSQUEUE_RECONNECT_GRACE, group_id=test.group_id)

    def advance(before, after, generation, barrier):
        if AUTOMATIC.get(before) == after:
            if before in ('R1P', 'R2P'):
                expect_reconnect(before, after, generation)
            barrier.release(generation)
        elif ROLES[before] == ROLES[after]:
            test.assertIn((before, after), (('F1', 'F2P'), ('S1', 'S2P')))
            test.define_group_state(STATES[after], realm_states(after))
        else:
            changes = [(dc, role) for dc, old, role in zip(test.data_centers, ROLES[before], ROLES[after]) if old != role]
            test.assertEqual(len(changes), 1)
            dc, role = changes[0]
            if role == 'R':
                expect_reconnect(before, after, generation)
            test.define_dc_state(dc, TDataCenterState.EXCLUDED if role in 'XL' else TDataCenterState.INCLUDED)

    def run_cycle(_pause_writes):
        with TransitionBarrier(
            test.cluster.nodes, test.group_id, [STATES[state] for state in AUTOMATIC],
            check_errors=workload.check_errors, event_logger=test.actions_logger,
        ) as barrier:
            visit(0, barrier)
            for index, (before, after) in enumerate(zip(route, route[1:]), 1):
                advance(before, after, initial.GroupGeneration + index - 1, barrier)
                visit(index, barrier)

    def restore_policy():
        for dc in sorted(test.excluded_dcs, reverse=True):
            test.define_dc_state(dc, TDataCenterState.INCLUDED)

    test.addCleanup(restore_policy)
    written = test.run_workload(take_baseline, run_cycle, workload_started=remember_workload)
    test.assertEqual(visited, route)
    check_configuration(test.get_group(), 'N', len(route) - 1)
    final = snapshot()
    test.assertEqual(len(final), written, 'Snapshot must contain every acknowledged KV write')
    test.assertFalse(baseline - final.keys(), 'Baseline KV blobs disappeared')
    check_layout('N', final)
    retry_assertions(test.check_group_ready, timeout_seconds=120)
