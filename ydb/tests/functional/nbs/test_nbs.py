# -*- coding: utf-8 -*-
from common import NbsTestBase


class TestNbs(NbsTestBase):
    """
    Test suite for NBS 2 basic operations
    """

    def test_nbs_disk_creation(self):
        """
        Create nbs disk and check basic IO operations
        """

        disk_id = self.generate_disk_id()
        self.create_ddisk_pool()
        self.create_disk(disk_id)
        actor_id = self.get_load_actor_adapter_actor_id(disk_id)

        test_data = "vnfjkdnsfvjdfknsjknsdkjnvnjk"
        self.write(actor_id, 0, test_data)
        read_data = self.read(actor_id, 0)

        # Verify the data matches (trimmed to the original length)
        assert read_data[: len(test_data)] == test_data

    def test_nbs_disk_creation_name_with_symbols(self):
        """
        Create nbs disk and check basic IO operations
        """

        disk_id = "disk%1"
        self.create_ddisk_pool()
        self.create_disk(disk_id)
        actor_id = self.get_load_actor_adapter_actor_id(disk_id)

        test_data = "vnfjkdnsfvjdfknsjknsdkjnvnjk"
        self.write(actor_id, 0, test_data)
        read_data = self.read(actor_id, 0)

        # Verify the data matches (trimmed to the original length)
        assert read_data[: len(test_data)] == test_data

    def test_nbs_4gb_disk_read_write(self):
        """
        Create a 4GB disk, write random data to some locations in it, and verify read operations.
        The disk is internally split into 128MB chunks. We test first, middle, and last
        block of some chunks.
        """
        disk_id = self.generate_disk_id()
        block_size = 4096
        # 4GB = 4 * 1024 * 1024 * 1024 bytes = 1,048,576 blocks of 4096 bytes
        blocks_count = 1048576

        # 128MB chunk = 128 * 1024 * 1024 bytes = 32,768 blocks of 4096 bytes
        chunk_size_blocks = 32768
        num_chunks = blocks_count // chunk_size_blocks

        self.create_ddisk_pool()
        self.create_disk(disk_id, blocks_count)
        actor_id = self.get_load_actor_adapter_actor_id(disk_id)

        # Test first, middle, and last block of few 128MB chunk
        test_locations = []

        # Test first, middle, and last block of few 128MB chunks
        for chunk_idx in [0, num_chunks // 2, num_chunks - 1]:
            chunk_start = chunk_idx * chunk_size_blocks
            chunk_middle = chunk_start + (chunk_size_blocks // 2)
            chunk_end = chunk_start + chunk_size_blocks - 1

            test_locations.extend([chunk_start, chunk_middle, chunk_end])

        # Store written data for verification
        written_data = {}

        block_data = self.generate_random_data(block_size)
        # Write data to test locations
        for idx, block_idx in enumerate(test_locations):
            # Generate random data for one block
            # block_data = self.generate_random_data(block_size)
            written_data[block_idx] = block_data

            # Write this block
            self.write(actor_id, block_idx, block_data)

        # Read back and verify all written data
        for idx, (block_idx, expected_data) in enumerate(written_data.items()):
            read_data = self.read(actor_id, block_idx, blocks_count=1)

            # Verify the data matches
            assert (
                read_data[: len(expected_data)] == expected_data
            ), f"Data mismatch at block {block_idx}: expected {len(expected_data)} bytes"

    def test_nbs_multiple_disks_creation(self):
        """
        Create multiple nbs disks and check basic IO operations
        """

        disk_id_1 = self.generate_disk_id()
        disk_id_2 = self.generate_disk_id()
        self.create_ddisk_pool()
        self.create_disk(disk_id_1)
        self.create_disk(disk_id_2)
        actor_id_1 = self.get_load_actor_adapter_actor_id(disk_id_1)
        actor_id_2 = self.get_load_actor_adapter_actor_id(disk_id_2)

        test_data_1 = self.generate_random_data(1024)
        self.write(actor_id_1, 0, test_data_1)
        read_data_1 = self.read(actor_id_1, 0)

        test_data_2 = self.generate_random_data(1024)
        self.write(actor_id_2, 0, test_data_2)
        read_data_2 = self.read(actor_id_2, 0)

        # Verify the data matches (trimmed to the original length)
        assert read_data_1[: len(test_data_1)] == test_data_1
        assert read_data_2[: len(test_data_2)] == test_data_2
