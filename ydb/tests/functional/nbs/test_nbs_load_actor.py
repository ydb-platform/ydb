# -*- coding: utf-8 -*-
from common import NbsTestBase


class TestNbsLoadActor(NbsTestBase):
    """
    Test suite for NBS 2 with load actor
    """

    def test_nbs_load_actor_write(self):
        """
        Create nbs disk and run write-only load actor
        """
        disk_id = self.generate_disk_id()
        self.create_ddisk_pool()
        self.create_disk(disk_id)
        actor_id = self.get_load_actor_adapter_actor_id(disk_id)

        # Run write-only load test
        results = self.run_nbs_load_test(actor_id, write_rate=100, read_rate=0)

        # Verify results
        self.verify_load_test_results(results, expected_blocks_read=False, expected_blocks_written=True)

    def test_nbs_load_actor_read(self):
        """
        Create nbs disk, write some data, and run read-only load actor
        """
        disk_id = self.generate_disk_id()
        self.create_ddisk_pool()
        self.create_disk(disk_id)
        actor_id = self.get_load_actor_adapter_actor_id(disk_id)

        # First write some data so we have something to read
        test_data = self.generate_random_data(4096)
        for block_idx in range(0, 1000, 100):
            self.write(actor_id, block_idx, test_data)

        # Run read-only load test
        results = self.run_nbs_load_test(actor_id, write_rate=0, read_rate=100)

        # Verify results
        self.verify_load_test_results(results, expected_blocks_read=True, expected_blocks_written=False)

    def test_nbs_load_actor_mixed(self):
        """
        Create nbs disk and run mixed read-write (50/50) load actor
        """
        disk_id = self.generate_disk_id()
        self.create_ddisk_pool()
        self.create_disk(disk_id)
        actor_id = self.get_load_actor_adapter_actor_id(disk_id)

        # Write some initial data for the read operations
        test_data = self.generate_random_data(4096)
        for block_idx in range(0, 1000, 100):
            self.write(actor_id, block_idx, test_data)

        # Run mixed read-write load test (50/50)
        results = self.run_nbs_load_test(actor_id, write_rate=50, read_rate=50)

        # Verify results - both reads and writes should happen
        self.verify_load_test_results(results, expected_blocks_read=True, expected_blocks_written=True)

    def test_nbs_load_actor_write_then_read(self):
        """
        Create nbs disk and run write load actor first, then read load actor.
        """
        disk_id = self.generate_disk_id()
        self.create_ddisk_pool()
        self.create_disk(disk_id)
        actor_id = self.get_load_actor_adapter_actor_id(disk_id)

        # First run write-only load test
        write_results = self.run_nbs_load_test(actor_id, write_rate=100, read_rate=0, duration=10)

        # Verify write results
        self.verify_load_test_results(write_results, expected_blocks_read=False, expected_blocks_written=True)

        # Then run read-only load test on the same disk
        read_results = self.run_nbs_load_test(actor_id, write_rate=0, read_rate=100, duration=10)

        # Verify read results
        self.verify_load_test_results(read_results, expected_blocks_read=True, expected_blocks_written=False)

        # Verify that both tests completed successfully with different metrics
        assert write_results['BlocksWritten'] > 0, "Write test should have written blocks"
        assert read_results['BlocksRead'] > 0, "Read test should have read blocks"
        assert (
            'BlocksRead' not in write_results or write_results.get('BlocksRead', 0) == 0
        ), "Write test should not have read blocks"
        assert (
            'BlocksWritten' not in read_results or read_results.get('BlocksWritten', 0) == 0
        ), "Read test should not have written blocks"
