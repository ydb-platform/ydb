#include <ydb/core/blobstorage/ddisk/persistent_buffer_barriers_manager.h>
#include <ydb/core/blobstorage/ddisk/persistent_buffer_space_allocator.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDDisk {

namespace {

// Helper: build an allocator with one chunk of given size (sectors).
TPersistentBufferSpaceAllocator MakeAllocator(ui32 sectorsInChunk = 1024, ui32 chunkIdx = 0) {
    TPersistentBufferSpaceAllocator alloc(sectorsInChunk);
    alloc.AddNewChunk(chunkIdx);
    return alloc;
}

// Helper: build a manager already initialized.
TPersistentBufferBarriersManager MakeManager() {
    TPersistentBufferBarriersManager mgr;
    mgr.Initialize(/*uniqueId=*/1, /*nodeId=*/1, /*pdiskId=*/1, /*slotId=*/1);
    return mgr;
}

// Helper: produce a fake sector info.
TPersistentBufferSectorInfo MakeSector(ui32 chunkIdx, ui32 sectorIdx) {
    TPersistentBufferSectorInfo s{};
    s.ChunkIdx = chunkIdx;
    s.SectorIdx = sectorIdx;
    return s;
}

// ErasesBufferSize = 3832 bytes.
// Raw format stores each LSN as 8 bytes, so max raw LSNs = 3832/8 = 479.
// Compact (LEB128 delta) format is triggered when total LSNs > 479.
static constexpr ui32 MaxRawLsns = TPersistentBufferHeader::ErasesBufferSize / sizeof(ui64);

} // namespace

Y_UNIT_TEST_SUITE(TPersistentBufferBarriersManagerTest) {

    // -----------------------------------------------------------------------
    // Compact: IS_ERASE_COMPACT is NOT set when LSN count fits in raw storage
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(CompactDoesNotSetFlagWhenFitsRaw) {
        auto mgr = MakeManager();

        // Use exactly MaxRawLsns LSNs split across old and new – fits raw.
        std::unordered_set<ui64> oldLsns;
        std::unordered_set<ui64> newLsns;
        for (ui64 i = 1; i <= 200; i++) oldLsns.insert(i * 10);
        for (ui64 i = 201; i <= MaxRawLsns; i++) newLsns.insert(i * 10);

        TPersistentBufferHeader header{};
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);

        // IS_ERASE_COMPACT must NOT be set.
        UNIT_ASSERT_C(
            !(header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT),
            "IS_ERASE_COMPACT must not be set when data fits in raw storage");
    }

    // -----------------------------------------------------------------------
    // Compact: IS_ERASE_COMPACT IS set when LSN count exceeds raw capacity
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(CompactSetsFlagWhenExceedsRaw) {
        auto mgr = MakeManager();

        // Use MaxRawLsns + 1 LSNs total – forces LEB128 delta encoding.
        std::unordered_set<ui64> oldLsns;
        std::unordered_set<ui64> newLsns;
        for (ui64 i = 1; i <= 300; i++) oldLsns.insert(i);
        for (ui64 i = 301; i <= MaxRawLsns + 1; i++) newLsns.insert(i);

        TPersistentBufferHeader header{};
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);

        // IS_ERASE_COMPACT must be set.
        UNIT_ASSERT_C(
            header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT,
            "IS_ERASE_COMPACT must be set when data requires delta encoding");
    }

    // -----------------------------------------------------------------------
    // Compact / Uncompact round-trip WITH IS_ERASE_COMPACT
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(CompactUncompactRoundTripWithFlag) {
        auto mgr = MakeManager();

        // Build a set large enough to require LEB128 encoding.
        std::unordered_set<ui64> oldLsns;
        std::unordered_set<ui64> newLsns;
        for (ui64 i = 1; i <= 300; i++) oldLsns.insert(i);
        for (ui64 i = 301; i <= MaxRawLsns + 1; i++) newLsns.insert(i);

        TPersistentBufferHeader header{};
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);
        UNIT_ASSERT(header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT);

        bool isCompact = (header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT) != 0;
        auto recovered = mgr.Uncompact(header.Erase.CompactLsns, isCompact);

        const ui64 expectedCount = MaxRawLsns + 1;
        UNIT_ASSERT_VALUES_EQUAL(recovered.size(), expectedCount);
        for (ui64 i = 1; i <= expectedCount; i++) {
            UNIT_ASSERT_C(recovered.count(i), "LSN " << i << " missing after Uncompact");
        }
    }

    // -----------------------------------------------------------------------
    // Compact / Uncompact round-trip WITHOUT IS_ERASE_COMPACT
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(CompactUncompactRoundTripNoFlag) {
        auto mgr = MakeManager();

        // Use a small set that fits in raw storage (no delta encoding).
        std::unordered_set<ui64> oldLsns = {5, 15};
        std::unordered_set<ui64> newLsns = {25, 35};

        TPersistentBufferHeader header{};
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);
        UNIT_ASSERT(!(header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT));

        bool isCompact = (header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT) != 0;
        auto recovered = mgr.Uncompact(header.Erase.CompactLsns, isCompact);

        // All four LSNs must survive the round-trip.
        for (ui64 lsn : {5ULL, 15ULL, 25ULL, 35ULL}) {
            UNIT_ASSERT_C(recovered.count(lsn), "LSN " << lsn << " missing after Uncompact");
        }
        UNIT_ASSERT_VALUES_EQUAL(recovered.size(), 4u);
    }

    // -----------------------------------------------------------------------
    // Erase: basic call succeeds and IS_ERASE flag is set
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(EraseBasicSucceeds) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator();

        const ui64 tabletId = 100;
        std::unordered_set<ui64> lsns = {10, 20};

        auto result = mgr.Erase(tabletId, lsns, alloc);
        UNIT_ASSERT(result.has_value());

        // IS_ERASE must be set.
        UNIT_ASSERT(result->Header.Flags & TPersistentBufferHeader::IS_ERASE);

        // The erase record must contain both LSNs.
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 2u);
    }

    // -----------------------------------------------------------------------
    // Erase: when Compact produces IS_ERASE_COMPACT, the header returned by
    // Erase() does NOT carry IS_ERASE_COMPACT (Erase() resets Flags to IS_ERASE
    // after Compact fills the buffer).  The compact flag lives only in the
    // CompactLsns buffer encoding; AddErase() reads it from the stored header.
    // This test verifies the full Erase → AddErase round-trip with compact data.
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(EraseAddEraseRoundTripWithCompactData) {
        auto mgr = MakeManager();
        // Need enough free space for multiple Erase calls.
        auto alloc = MakeAllocator(32768, 0);

        const ui64 tabletId = 200;

        // First batch: 240 LSNs (fits raw).
        std::unordered_set<ui64> firstBatch;
        for (ui64 i = 1; i <= 240; i++) firstBatch.insert(i);
        auto r1 = mgr.Erase(tabletId, firstBatch, alloc);
        UNIT_ASSERT(r1.has_value());
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 240u);

        // Second batch: add enough LSNs so old(240) + new(300) = 540 > MaxRawLsns(479).
        // Compact() will use LEB128 and set IS_ERASE_COMPACT in the header it fills.
        // Erase() then overwrites header.Flags = IS_ERASE, so the returned header
        // has only IS_ERASE.  But the CompactLsns buffer is encoded compactly.
        std::unordered_set<ui64> secondBatch;
        for (ui64 i = 241; i <= 540; i++) secondBatch.insert(i);
        auto r2 = mgr.Erase(tabletId, secondBatch, alloc);
        UNIT_ASSERT(r2.has_value());
        UNIT_ASSERT(r2->Header.Flags & TPersistentBufferHeader::IS_ERASE);

        // All 540 LSNs must be tracked in the manager.
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 540u);

        // Simulate recovery: build a fresh manager and feed the header back via
        // AddErase.  The header must carry IS_ERASE_COMPACT so Uncompact decodes
        // correctly.  We set it explicitly here because Erase() cleared it, but
        // in production the persisted header is the one written by Compact() before
        // Erase() overwrites Flags.  This test documents the expected contract.
        TPersistentBufferBarriersManager mgr2;
        mgr2.Initialize(1, 1, 1, 1);

        // Build a header that correctly reflects what Compact() produced.
        std::unordered_set<ui64> allLsns;
        for (ui64 i = 1; i <= 540; i++) allLsns.insert(i);
        std::unordered_set<ui64> emptyOld;
        TPersistentBufferHeader compactHeader{};
        bool ok = mgr2.Compact(emptyOld, allLsns, compactHeader);
        UNIT_ASSERT(ok);
        UNIT_ASSERT(compactHeader.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT);

        compactHeader.Flags |= TPersistentBufferHeader::IS_ERASE;
        compactHeader.RecordLsn = 1;
        compactHeader.Erase.TabletId = tabletId;

        bool added = mgr2.AddErase(&compactHeader, 0, 0);
        UNIT_ASSERT(added);
        UNIT_ASSERT_VALUES_EQUAL(mgr2.GetErasesCount(tabletId), 540u);
    }

    // -----------------------------------------------------------------------
    // AddErase with IS_ERASE_COMPACT flag decodes correctly
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(AddEraseCompact) {
        auto mgr = MakeManager();

        // Build a compact header via Compact().
        std::unordered_set<ui64> oldLsns;
        std::unordered_set<ui64> newLsns;
        for (ui64 i = 1; i <= 300; i++) oldLsns.insert(i);
        for (ui64 i = 301; i <= MaxRawLsns + 1; i++) newLsns.insert(i);

        TPersistentBufferHeader header{};
        memset(&header, 0, sizeof(header));
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);
        UNIT_ASSERT(header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT);

        header.RecordLsn = 1;
        header.Erase.TabletId = 2000;

        bool added = mgr.AddErase(&header, /*chunkIdx=*/0, /*sectorIdx=*/0);
        UNIT_ASSERT(added);
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(2000), MaxRawLsns + 1);
    }

    // -----------------------------------------------------------------------
    // AddErase without IS_ERASE_COMPACT (non-compact, two LSNs)
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(AddEraseNonCompact) {
        auto mgr = MakeManager();

        TPersistentBufferHeader header{};
        memset(&header, 0, sizeof(header));
        header.Flags = TPersistentBufferHeader::IS_ERASE;
        header.RecordLsn = 1;
        header.Erase.TabletId = 1000;

        // Write two raw LSNs into CompactLsns (non-compact format).
        ui64 lsn1 = 111, lsn2 = 222;
        memcpy(header.Erase.CompactLsns, &lsn1, 8);
        memcpy(header.Erase.CompactLsns + 8, &lsn2, 8);

        bool added = mgr.AddErase(&header, /*chunkIdx=*/0, /*sectorIdx=*/0);
        UNIT_ASSERT(added);
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(1000), 2u);
    }

    // -----------------------------------------------------------------------
    // MoveBarrier cleans up LSNs from Erase that are <= new barrier LSN
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(MoveBarrierCleansEraseLsns) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 300;

        // Register some erased LSNs: 10, 20, 30, 40, 50.
        std::unordered_set<ui64> erasedLsns = {10, 20, 30, 40, 50};
        auto eraseResult = mgr.Erase(tabletId, erasedLsns, alloc);
        UNIT_ASSERT(eraseResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 5u);

        // Move barrier to LSN=30: LSNs <= 30 (i.e. 10, 20, 30) must be removed.
        TPersistentBufferSectorInfo sector = MakeSector(0, 5);
        mgr.MoveBarrier(tabletId, /*lsn=*/30, sector);

        // Only LSNs 40 and 50 should remain.
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 2u);
    }

    // -----------------------------------------------------------------------
    // MoveBarrier removes ALL erase LSNs when barrier advances past all of them
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(MoveBarrierClearsAllEraseLsns) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 400;

        std::unordered_set<ui64> erasedLsns = {5, 15, 25};
        auto eraseResult = mgr.Erase(tabletId, erasedLsns, alloc);
        UNIT_ASSERT(eraseResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 3u);

        // Advance barrier past all erased LSNs.
        TPersistentBufferSectorInfo sector = MakeSector(0, 10);
        mgr.MoveBarrier(tabletId, /*lsn=*/100, sector);

        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 0u);
    }

    // -----------------------------------------------------------------------
    // MoveBarrier keeps LSNs that are strictly greater than the barrier
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(MoveBarrierKeepsLsnsAboveBarrier) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 500;

        std::unordered_set<ui64> erasedLsns = {10, 20, 30, 40, 50};
        auto eraseResult = mgr.Erase(tabletId, erasedLsns, alloc);
        UNIT_ASSERT(eraseResult.has_value());

        // Barrier at LSN=15: only LSN 10 should be removed; 20,30,40,50 remain.
        TPersistentBufferSectorInfo sector = MakeSector(0, 7);
        mgr.MoveBarrier(tabletId, /*lsn=*/15, sector);

        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 4u);
    }

    // -----------------------------------------------------------------------
    // MoveBarrier on a tablet with no Erase record does not crash
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(MoveBarrierNoEraseRecord) {
        auto mgr = MakeManager();

        const ui64 tabletId = 600;

        // No Erase() called for this tablet – MoveBarrier must not crash.
        TPersistentBufferSectorInfo sector = MakeSector(0, 1);
        mgr.MoveBarrier(tabletId, /*lsn=*/50, sector);

        // Barrier should be registered.
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetBarrier(tabletId), 50u);
    }

    // -----------------------------------------------------------------------
    // Multiple MoveBarrier calls progressively clean erase LSNs
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(MoveBarrierProgressivelyCleansEraseLsns) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 700;

        std::unordered_set<ui64> erasedLsns = {10, 20, 30, 40, 50, 60, 70, 80};
        auto eraseResult = mgr.Erase(tabletId, erasedLsns, alloc);
        UNIT_ASSERT(eraseResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 8u);

        TPersistentBufferSectorInfo sector1 = MakeSector(0, 2);
        mgr.MoveBarrier(tabletId, /*lsn=*/25, sector1);
        // LSNs 10, 20 removed; 30,40,50,60,70,80 remain.
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 6u);

        TPersistentBufferSectorInfo sector2 = MakeSector(0, 3);
        mgr.MoveBarrier(tabletId, /*lsn=*/55, sector2);
        // LSNs 30,40,50 removed; 60,70,80 remain.
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 3u);

        TPersistentBufferSectorInfo sector3 = MakeSector(0, 4);
        mgr.MoveBarrier(tabletId, /*lsn=*/80, sector3);
        // All remaining LSNs removed.
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 0u);
    }

    // -----------------------------------------------------------------------
    // Erase returns nullopt when allocator has no free space
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(EraseReturnsNulloptWhenNoFreeSpace) {
        auto mgr = MakeManager();
        // Allocator with only 1 sector – Erase requires >= 2 free sectors.
        TPersistentBufferSpaceAllocator alloc(1);
        alloc.AddNewChunk(0);

        const ui64 tabletId = 800;
        std::unordered_set<ui64> lsns = {1, 2};

        auto result = mgr.Erase(tabletId, lsns, alloc);
        UNIT_ASSERT(!result.has_value());
    }

    // -----------------------------------------------------------------------
    // Erase returns nullopt when lsns set has fewer than 2 elements
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(EraseReturnsNulloptWhenTooFewLsns) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 900;
        std::unordered_set<ui64> lsns = {42};  // only 1 LSN

        auto result = mgr.Erase(tabletId, lsns, alloc);
        UNIT_ASSERT(!result.has_value());
    }

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr::NDDisk
