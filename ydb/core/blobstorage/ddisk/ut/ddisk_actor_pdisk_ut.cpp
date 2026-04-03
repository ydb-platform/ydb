#include "ddisk_actor_pdisk_ut.inl"

Y_UNIT_TEST_SUITE(TDDiskActorPDiskTest) {
    Y_UNIT_TEST(WriteAndRead_4KiB_Uring) {
        TestWriteAndRead({}, 4_KB);
    }

    Y_UNIT_TEST(WriteAndRead_4KiB_PDiskFallback) {
        TestWriteAndRead({.ForcePDiskFallback = true}, 4_KB);
    }

    Y_UNIT_TEST(WriteAndRead_8KiB_Uring) {
        TestWriteAndRead({}, 8_KB);
    }

    Y_UNIT_TEST(WriteAndRead_8KiB_PDiskFallback) {
        TestWriteAndRead({.ForcePDiskFallback = true}, 8_KB);
    }

    Y_UNIT_TEST(WriteAndRead_1MiB_Uring) {
        TestWriteAndRead({}, 1_MB);
    }

    Y_UNIT_TEST(WriteAndRead_1MiB_PDiskFallback) {
        TestWriteAndRead({.ForcePDiskFallback = true}, 1_MB);
    }

    Y_UNIT_TEST(CheckVChunksArePerTablet_Uring) {
        TestCheckVChunksArePerTablet({});
    }

    Y_UNIT_TEST(CheckVChunksArePerTablet_PDiskFallback) {
        TestCheckVChunksArePerTablet({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(OverwriteSameOffset_Uring) {
        TestOverwrite({});
    }

    Y_UNIT_TEST(OverwriteSameOffset_PDiskFallback) {
        TestOverwrite({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(ReadUnallocatedChunk_Uring) {
        TestReadUnallocatedChunk({});
    }

    Y_UNIT_TEST(ReadUnallocatedChunk_PDiskFallback) {
        TestReadUnallocatedChunk({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(ManyVChunksPerTablet_Uring) {
        TestManyVChunks({});
    }

    Y_UNIT_TEST(ManyVChunksPerTablet_PDiskFallback) {
        TestManyVChunks({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(MultiTabletInterleavedWrites_Uring) {
        TestMultiTabletInterleaved({});
    }

    Y_UNIT_TEST(MultiTabletInterleavedWrites_PDiskFallback) {
        TestMultiTabletInterleaved({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(MultiTabletInterleavedWritesWithDDiskRestart_Uring) {
        TestMultiTabletInterleavedWritesWithDDiskRestart({});
    }

    Y_UNIT_TEST(MultiTabletInterleavedWritesWithDDiskRestart_PDiskFallback) {
        TestMultiTabletInterleavedWritesWithDDiskRestart({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(MultipleRestarts_Uring) {
        TestMultipleRestarts({});
    }

    Y_UNIT_TEST(MultipleRestarts_PDiskFallback) {
        TestMultipleRestarts({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(OverwriteAfterRestart_Uring) {
        TestOverwriteAfterRestart({});
    }

    Y_UNIT_TEST(OverwriteAfterRestart_PDiskFallback) {
        TestOverwriteAfterRestart({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(EmptyRestart_Uring) {
        TestEmptyRestart({});
    }

    Y_UNIT_TEST(EmptyRestart_PDiskFallback) {
        TestEmptyRestart({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(RestartAfterCutLog_Uring) {
        TestRestartAfterCutLog({});
    }

    Y_UNIT_TEST(RestartAfterCutLog_PDiskFallback) {
        TestRestartAfterCutLog({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(ReadWithoutConnect_Uring) {
        TestReadWithoutConnect({});
    }

    Y_UNIT_TEST(ReadWithoutConnect_PDiskFallback) {
        TestReadWithoutConnect({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(Smoke_2Tablets_2VChunks_1Segment) {
        TestSyncWithDDisk(2, 2, 8, 1);
    }
}

} // NKikimr
