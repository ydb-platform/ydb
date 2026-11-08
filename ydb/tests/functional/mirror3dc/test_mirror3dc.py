from collections import Counter
import logging
import time

from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.functional.mirror3dc.mirror3dc_base import Mirror3dcTestBase
from ydb.tests.functional.mirror3dc.write2x2 import restore_write2x2


logger = logging.getLogger(__name__)


class TestMirror3dc(Mirror3dcTestBase):
    def test_exclude_one_dc_external_recovery(self):
        dc = self.data_centers[0]
        baseline = {}

        def snapshot_and_exclude_dc():
            # Writers wait after their initial batch while readers may continue.
            _, layouts = self.snapshot()
            baseline.update(layouts)
            self.assertTrue(baseline)
            blobs = self.kv_blobs(baseline)
            self.assertEqual(len(blobs), self.INITIAL_WRITES)
            logger.info('Before DC exclusion: %d total blobs, %d KV blobs, layouts %s',
                        len(baseline), len(blobs), dict(Counter(self.copy_counts(v) for v in baseline.values())))
            logger.info('Before DC exclusion: KV layouts %s', dict(Counter(self.copy_counts(v) for v in blobs.values())))
            # Extra handoff copies are possible even before excluding a DC.
            self.check_blob_layouts(blobs, copies_per_dc=1, dc_count=3)
            try:
                self.define_dc_state(dc, self.DC_STATES.EXCLUDED, check_generation=True)
            except Exception:
                self.define_dc_state(dc, self.DC_STATES.INCLUDED, check_generation=True)
                raise

        def restore_blobs(pause_writes):
            try:
                logger.info('DC %s is excluded; keeping KV workload running for 5 seconds', dc)
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
                    logger.info('Recovery candidates before restore: %d old blobs, %d new blobs',
                                len(candidates), len(new_candidates))
                    logger.info('New KV blobs before restore: %d, layouts %s', len(new_blobs),
                                dict(Counter(self.copy_counts(layout) for layout in new_blobs.values())))
                    self.check_blob_layouts(new_blobs)
                logger.info('Restoring write2x2 layout while DC %s is excluded', dc)
                restored = restore_write2x2(
                    endpoint=self.viewer_url,
                    viewer_url=self.viewer_url,
                    group_ids=[self.group_id],
                    node_ids=self.recovery_node_ids,
                    attempts=3,
                    retry_interval=1,
                    timeout=30,
                )
                for blob, layout in sorted(baseline.items()):
                    logger.info('Baseline blob %s: before=%s excluded=%s restored=%s',
                                blob, sorted(layout), sorted(excluded.get(blob, set())),
                                sorted(restored.get(blob, set())))
                for blob in self.kv_blobs(baseline):
                    self.assertIn(blob, excluded)
                    self.assertIn(blob, restored)
                self.check_blob_layouts({blob: restored[blob] for blob in self.kv_blobs(baseline)})
            finally:
                self.define_dc_state(dc, self.DC_STATES.INCLUDED, check_generation=True)
                logger.info('DC %s is included again', dc)

        self.run_workload(snapshot_and_exclude_dc, restore_blobs)
        retry_assertions(self.check_group_ready, timeout_seconds=120)

    def _test_exclude_one_dc_check_state_include_back(self):
        dc = self.data_centers[0]
        self.run_automatic_recovery(dc)
        self.define_dc_state(dc, self.DC_STATES.INCLUDED)
        self.wait_for_realm_states({name: self.REALM_STATES.N0 for name in self.data_centers})

    def test_exclude_two_dcs_check_state(self):
        self.run_automatic_recovery(self.data_centers[0])
        self.define_dc_state(self.data_centers[1], self.DC_STATES.EXCLUDED)
        self.wait_for_realm_states(self.recovered_realm_states())

    def _test_exclude_two_dcs_check_state_include_back(self):
        first_dc, second_dc = self.data_centers[:2]
        self.run_automatic_recovery(first_dc)
        self.define_dc_state(second_dc, self.DC_STATES.EXCLUDED)
        self.wait_for_realm_states(self.recovered_realm_states())

        # Return the second DC while keeping the first one excluded.
        self.define_dc_state(second_dc, self.DC_STATES.INCLUDED)
        self.wait_for_realm_states(self.recovered_realm_states())

        self.define_dc_state(first_dc, self.DC_STATES.INCLUDED)
        self.wait_for_realm_states({name: self.REALM_STATES.N0 for name in self.data_centers})

    def _test_exclude_two_dcs_include_back_under_load(self):
        first_dc, second_dc = self.data_centers[:2]
        normal_states = {name: self.REALM_STATES.N0 for name in self.data_centers}

        def exclude_and_return_dcs(_pause_writes):
            self.wait_for_automatic_recovery()
            self.check_blob_layouts_under_load()

            self.define_dc_state(second_dc, self.DC_STATES.EXCLUDED)
            self.wait_for_realm_states(self.recovered_realm_states())

            self.define_dc_state(second_dc, self.DC_STATES.INCLUDED)
            self.wait_for_realm_states(self.recovered_realm_states())

            self.define_dc_state(first_dc, self.DC_STATES.INCLUDED)
            self.wait_for_realm_states(normal_states)

        # Keep writers and readers running throughout both exclusions and returns.
        # The workload only finishes once the entire group has returned to N0.
        written = self.run_workload(
            lambda: self.define_dc_state(first_dc, self.DC_STATES.EXCLUDED), exclude_and_return_dcs,
        )
        self.check_realm_states(normal_states)
        _, layouts = self.snapshot()
        self.assertEqual(len(self.kv_blobs(layouts)), written, 'Snapshot must contain every acknowledged KV write')
        logger.info('Continuous load through both DC returns: %d writes, %d initial, %d subsequent',
                    written, self.INITIAL_WRITES, written - self.INITIAL_WRITES)
