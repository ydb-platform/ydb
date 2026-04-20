#include <ydb/core/blobstorage/ddisk/ut/ddisk_actor_pdisk_ut.h>

Y_UNIT_TEST_SUITE(TDDiskActorPDiskLargeTest) {
    Y_UNIT_TEST(Batch10K_SeqWrite_SeqRead_Uring) {
        TestBatchWriteThenRead({}, 10000, 0, 0, NLog::PRI_ERROR);
    }

    Y_UNIT_TEST(Batch10K_SeqWrite_SeqRead_PDiskFallback) {
        TestBatchWriteThenRead({.ForcePDiskFallback = true}, 10000, 0, 0, NLog::PRI_ERROR);
    }

    Y_UNIT_TEST(Batch10K_SeqWrite_PermutedRead_Uring) {
        constexpr ui32 seeds[] = {11, 17, 31, 37, 41, 43, 59, 67, 71, 79};
        for (ui32 seed : seeds) {
            TestBatchWriteThenRead({}, 10000, 0, seed, NLog::PRI_ERROR);
        }
    }

    Y_UNIT_TEST(Batch10K_SeqWrite_PermutedRead_PDiskFallback) {
        constexpr ui32 seeds[] = {11, 17, 31, 37, 41, 43, 59, 67, 71, 79};
        for (ui32 seed : seeds) {
            TestBatchWriteThenRead({.ForcePDiskFallback = true}, 10000, 0, seed, NLog::PRI_ERROR);
        }
    }

    Y_UNIT_TEST(Batch10K_PermutedWrite_SeqRead_Uring) {
        constexpr ui32 seeds[] = {11, 17, 31, 37, 41, 43, 59, 67, 71, 79};
        for (ui32 seed : seeds) {
            TestBatchWriteThenRead({}, 10000, seed, 0, NLog::PRI_ERROR);
        }
    }

    Y_UNIT_TEST(Batch10K_PermutedWrite_SeqRead_PDiskFallback) {
        constexpr ui32 seeds[] = {11, 17, 31, 37, 41, 43, 59, 67, 71, 79};
        for (ui32 seed : seeds) {
            TestBatchWriteThenRead({.ForcePDiskFallback = true}, 10000, seed, 0, NLog::PRI_ERROR);
        }
    }

    Y_UNIT_TEST(Batch10K_PermutedWrite_PermutedRead_Uring) {
        constexpr ui32 seeds[] = {11, 17, 31, 37, 41, 43, 59, 67, 71, 79};
        for (ui32 seed : seeds) {
            TestBatchWriteThenRead({}, 10000, seed, seed + 100, NLog::PRI_ERROR);
        }
    }

    Y_UNIT_TEST(Batch10K_PermutedWrite_PermutedRead_PDiskFallback) {
        constexpr ui32 seeds[] = {11, 17, 31, 37, 41, 43, 59, 67, 71, 79};
        for (ui32 seed : seeds) {
            TestBatchWriteThenRead({.ForcePDiskFallback = true}, 10000, seed, seed + 100, NLog::PRI_ERROR);
        }
    }

    Y_UNIT_TEST(BatchWriteThenReadMultiTabletInterleaved_Sequential_Uring) {
        TestBatchWriteThenReadMultiTabletInterleaved({}, 0);
    }

    Y_UNIT_TEST(BatchWriteThenReadMultiTabletInterleaved_Sequential_PDiskFallback) {
        TestBatchWriteThenReadMultiTabletInterleaved({.ForcePDiskFallback = true}, 0);
    }

    Y_UNIT_TEST(BatchWriteThenReadMultiTabletInterleaved_Seed42_Uring) {
        TestBatchWriteThenReadMultiTabletInterleaved({}, 42);
    }

    Y_UNIT_TEST(BatchWriteThenReadMultiTabletInterleaved_Seed42_PDiskFallback) {
        TestBatchWriteThenReadMultiTabletInterleaved({.ForcePDiskFallback = true}, 42);
    }

    Y_UNIT_TEST(BatchWriteThenReadMultiTabletInterleaved_Seed7777_Uring) {
        TestBatchWriteThenReadMultiTabletInterleaved({}, 7777);
    }

    Y_UNIT_TEST(BatchWriteThenReadMultiTabletInterleaved_Seed7777_PDiskFallback) {
        TestBatchWriteThenReadMultiTabletInterleaved({.ForcePDiskFallback = true}, 7777);
    }

    Y_UNIT_TEST(BatchWriteThenReadMultiTabletInterleaved_Sequential_WithRestarts_Uring) {
        TestBatchWriteThenReadMultiTabletInterleaved({}, 0, NLog::PRI_INFO, true);
    }

    Y_UNIT_TEST(BatchWriteThenReadMultiTabletInterleaved_Sequential_WithRestarts_PDiskFallback) {
        TestBatchWriteThenReadMultiTabletInterleaved({.ForcePDiskFallback = true}, 0, NLog::PRI_INFO, true);
    }

    Y_UNIT_TEST(BatchWriteThenReadMultiTabletInterleaved_Seed42_WithRestarts_Uring) {
        TestBatchWriteThenReadMultiTabletInterleaved({}, 42, NLog::PRI_INFO, true);
    }

    Y_UNIT_TEST(BatchWriteThenReadMultiTabletInterleaved_Seed42_WithRestarts_PDiskFallback) {
        TestBatchWriteThenReadMultiTabletInterleaved({.ForcePDiskFallback = true}, 42, NLog::PRI_INFO, true);
    }

    Y_UNIT_TEST(BatchWriteThenReadMultiTabletInterleaved_Seed7777_WithRestarts_Uring) {
        TestBatchWriteThenReadMultiTabletInterleaved({}, 7777, NLog::PRI_INFO, true);
    }

    Y_UNIT_TEST(BatchWriteThenReadMultiTabletInterleaved_Seed7777_WithRestarts_PDiskFallback) {
        TestBatchWriteThenReadMultiTabletInterleaved({.ForcePDiskFallback = true}, 7777, NLog::PRI_INFO, true);
    }
}

} // NKikimr
