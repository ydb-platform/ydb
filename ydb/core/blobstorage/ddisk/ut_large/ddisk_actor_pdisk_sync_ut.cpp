#include <ydb/core/blobstorage/ddisk/ut/ddisk_actor_pdisk_ut.inl>

Y_UNIT_TEST_SUITE(TDDiskActorPDiskSyncTest) {
    Y_UNIT_TEST(Large_4Tablets_32VChunks_FullBlocks_1Segment) {
        constexpr ui32 blocksPerVChunk = ChunkSize / MinBlockSize;
        TestSyncWithDDisk(4, 32, blocksPerVChunk, 1);
    }

    Y_UNIT_TEST(Large_4Tablets_32VChunks_FullBlocks_16Segments) {
        constexpr ui32 blocksPerVChunk = ChunkSize / MinBlockSize;
        TestSyncWithDDisk(4, 32, blocksPerVChunk, 16);
    }
}

} // NKikimr
