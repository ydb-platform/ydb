from collections import Counter
import time

from ydb.core.protos import msgbus_pb2
from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.functional.mirror3dc.mirror3dc_base import Mirror3dcTestBase
from ydb.tests.functional.mirror3dc.transition_cycle import run_transition_cycle
from ydb.tests.functional.mirror3dc.write2x2 import restore_write2x2


class TestMirror3dc(Mirror3dcTestBase):
    def test_first_transition_cycle(self):
        run_transition_cycle(self, 'N F N F F1 R F1 R N')

    def test_transition_cycle_2(self):
        run_transition_cycle(self, 'N F F1 F2P R N')

    def test_transition_cycle_3(self):
        run_transition_cycle(self, 'N F F1 F2P F2 R N')

    def _test_transition_cycle_4(self):
        run_transition_cycle(self, 'N F F1 S1 R1P S1 R1P R1 F1 R N')

    def test_transition_cycle_5(self):
        run_transition_cycle(self, 'N F F1 S1 R1P R1 S1 S2P S2 R2P R2 F1 R N')

    def test_transition_cycle_6(self):
        run_transition_cycle(self, 'N F F1 F2P S2P R1P R1 F1 R N')

    def test_transition_cycle_7(self):
        run_transition_cycle(self, 'N F F1 F2P F2 S2 R2P S2 R2P R2 F1 R N')

    def test_transition_cycle_8(self):
        run_transition_cycle(self, 'N F F1 F2P F2 S2 R2P R2 S2 R2P R2 F1 R N')

    def test_define_group_state_requires_complete_configuration(self):
        before = self.get_group()
        for has_group_state, role_count in ((False, 3), (True, 0), (True, 2), (True, 4)):
            request = msgbus_pb2.TBlobStorageConfigRequest(Domain=1)
            definition = request.Request.Command.add().DefineGroupState.Groups.add(
                GroupId=self.group_id,
                ExpectedGroupGeneration=before.GroupGeneration,
            )
            if has_group_state:
                definition.GroupState = self.GROUP_STATES.NORMAL
            definition.FailRealmStates.extend([self.DC_STATES.INCLUDED] * role_count)
            response = self.send_config(request)
            self.assertFalse(response.Success, str(response))
            self.assertEqual(self.get_group().GroupGeneration, before.GroupGeneration)
            self.check_group_state(self.GROUP_STATES.NORMAL, self.recovered_realm_states())
        self.define_group_state(self.GROUP_STATES.NORMAL, self.recovered_realm_states())
        self.check_group_state(self.GROUP_STATES.NORMAL, self.recovered_realm_states())

    def _test_exclude_one_dc_external_recovery(self):
        dc = self.data_centers[0]
        baseline = {}

        def snapshot_and_exclude_dc():
            # Writers wait after their initial batch while readers may continue.
            _, layouts = self.snapshot()
            baseline.update(layouts)
            self.assertTrue(baseline)
            blobs = self.kv_blobs(baseline)
            self.assertEqual(len(blobs), self.INITIAL_WRITES)
            self.actions_logger.info(
                'Before DC exclusion: %d total blobs, %d KV blobs, layouts %s',
                len(baseline), len(blobs), dict(Counter(self.copy_counts(v) for v in baseline.values())),
            )
            self.actions_logger.info('Before DC exclusion: KV layouts %s',
                                     dict(Counter(self.copy_counts(v) for v in blobs.values())))
            # Extra handoff copies are possible even before excluding a DC.
            self.check_blob_layouts(blobs, copies_per_dc=1, dc_count=3)
            try:
                self.define_dc_state(dc, self.DC_STATES.EXCLUDED, check_generation=True)
            except Exception:
                self.define_dc_state(dc, self.DC_STATES.INCLUDED, check_generation=True)
                raise

        def restore_blobs(pause_writes):
            try:
                self.actions_logger.info('DC %s is excluded; keeping KV workload running for 5 seconds', dc)
                time.sleep(5)
                with pause_writes() as written:
                    candidates, excluded = self.snapshot()
                    new_blobs = self.kv_blobs({blob: layout for blob, layout in excluded.items() if blob not in baseline})
                    self.assertGreater(len(new_blobs), 0, 'No new KV blobs after DC exclusion')
                    self.assertEqual(len(new_blobs), written - self.INITIAL_WRITES)
                    new_candidates = candidates - baseline.keys()
                    self.assertFalse(new_candidates,
                                     f'Recovery candidates absent before DC exclusion: {sorted(new_candidates)}')
                    self.assertFalse(candidates & new_blobs.keys(), 'New KV blobs must not be recovery candidates')
                    self.actions_logger.info('Recovery candidates before restore: %d old blobs, %d new blobs',
                                             len(candidates), len(new_candidates))
                    self.actions_logger.info('New KV blobs before restore: %d, layouts %s', len(new_blobs),
                                             dict(Counter(self.copy_counts(layout) for layout in new_blobs.values())))
                    self.check_blob_layouts(new_blobs)
                self.actions_logger.info('Restoring write2x2 layout while DC %s is excluded', dc)
                restored = restore_write2x2(
                    endpoint=self.viewer_url,
                    viewer_url=self.viewer_url,
                    group_ids=[self.group_id],
                    node_ids=self.recovery_node_ids,
                    attempts=3,
                    retry_interval=1,
                    timeout=30,
                    event_logger=self.actions_logger,
                )
                for blob, layout in sorted(baseline.items()):
                    self.actions_logger.info(
                        'Baseline blob %s: before=%s excluded=%s restored=%s',
                        blob, sorted(layout), sorted(excluded.get(blob, set())), sorted(restored.get(blob, set())),
                    )
                for blob in self.kv_blobs(baseline):
                    self.assertIn(blob, excluded)
                    self.assertIn(blob, restored)
                self.check_blob_layouts({blob: restored[blob] for blob in self.kv_blobs(baseline)})
            finally:
                self.define_dc_state(dc, self.DC_STATES.INCLUDED, check_generation=True)
                self.actions_logger.info('DC %s is included again', dc)

        self.run_workload(snapshot_and_exclude_dc, restore_blobs)
        self.wait_for_group_state(self.GROUP_STATES.NORMAL, self.recovered_realm_states())
        retry_assertions(self.check_group_ready, timeout_seconds=120)

    def test_exclude_one_dc_check_state_include_back(self):
        dc = self.data_centers[0]
        self.run_two_copy_recovery(dc)
        self.return_dc(dc)
        self.wait_for_group_state(
            self.GROUP_STATES.NORMAL, {name: self.DC_STATES.INCLUDED for name in self.data_centers}
        )

    def test_exclude_two_dcs_check_state(self):
        self.run_two_copy_recovery(self.data_centers[0])
        self.define_dc_state(self.data_centers[1], self.DC_STATES.EXCLUDED)
        self.wait_for_group_state(self.recovered_group_state(), self.recovered_realm_states())

    def test_exclude_two_dcs_check_state_include_back(self):
        first_dc, second_dc = self.data_centers[:2]
        self.run_two_copy_recovery(first_dc)
        self.define_dc_state(second_dc, self.DC_STATES.EXCLUDED)
        self.wait_for_group_state(self.recovered_group_state(), self.recovered_realm_states())

        # Return the second DC while keeping the first one excluded.
        self.return_dc(second_dc)
        self.wait_for_group_state(self.recovered_group_state(), self.recovered_realm_states())

        self.return_dc(first_dc)
        self.wait_for_group_state(
            self.GROUP_STATES.NORMAL, {name: self.DC_STATES.INCLUDED for name in self.data_centers}
        )

    def _test_exclude_two_dcs_include_back_under_load(self):
        first_dc, second_dc = self.data_centers[:2]
        normal_states = {name: self.DC_STATES.INCLUDED for name in self.data_centers}

        def exclude_and_return_dcs(_pause_writes):
            self.wait_for_two_copy_recovery()
            self.check_blob_layouts_under_load()

            self.define_dc_state(second_dc, self.DC_STATES.EXCLUDED)
            self.wait_for_group_state(self.recovered_group_state(), self.recovered_realm_states())

            self.return_dc(second_dc)
            self.wait_for_group_state(self.recovered_group_state(), self.recovered_realm_states())

            self.return_dc(first_dc)
            self.wait_for_group_state(self.GROUP_STATES.NORMAL, normal_states)

        # Keep writers and readers running throughout both exclusions and returns.
        # The workload only finishes once the entire group has returned to NORMAL.
        written = self.run_workload(
            lambda: self.define_dc_state(first_dc, self.DC_STATES.EXCLUDED), exclude_and_return_dcs,
        )
        self.check_group_state(self.GROUP_STATES.NORMAL, normal_states)
        _, layouts = self.snapshot()
        self.assertEqual(len(self.kv_blobs(layouts)), written, 'Snapshot must contain every acknowledged KV write')
        self.actions_logger.info('Continuous load through both DC returns: %d writes, %d initial, %d subsequent',
                                 written, self.INITIAL_WRITES, written - self.INITIAL_WRITES)
