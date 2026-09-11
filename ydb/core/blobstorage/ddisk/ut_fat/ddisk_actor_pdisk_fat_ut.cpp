#include <ydb/core/blobstorage/ddisk/ut/ddisk_actor_pdisk_common_ut.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TDDiskActorPDiskLargeTest) {
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
