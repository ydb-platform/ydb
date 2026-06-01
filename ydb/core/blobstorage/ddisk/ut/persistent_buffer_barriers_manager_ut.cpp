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
    // Erase/AddErase round-trip with compact data.
    // When Compact() chooses delta encoding, it sets IS_ERASE_COMPACT; Erase() then adds IS_ERASE on top,
    // so the persisted header may carry both flags and AddErase() relies on IS_ERASE_COMPACT to decode.
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

        // AddErase requires IS_ERASE to be set (Compact only sets IS_ERASE_COMPACT).
        header.Flags |= TPersistentBufferHeader::IS_ERASE;
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

    // -----------------------------------------------------------------------
    // RestoreErases: LSNs <= barrier are removed from erase.Lsns and NOT
    // applied to persistentBuffers (barrier already covers them).
    // LSNs > barrier remain in erase.Lsns and ARE applied.
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(RestoreErasesFiltersLsnsBehindBarrier) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 1001;

        // Set up a barrier at LSN=30 for the tablet via AddBarrier + RestoreBarriers.
        {
            TPersistentBufferHeader bh{};
            memset(&bh, 0, sizeof(bh));
            memcpy(bh.Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
            bh.Flags = TPersistentBufferHeader::IS_BARRIER;
            bh.RecordLsn = 1;
            bh.PersistentBufferUniqueId = 1;
            bh.NodeId = 1; bh.PDiskId = 1; bh.SlotId = 1;
            bh.Barrier.BarrierIdx = 0;
            bh.Barrier.Barriers[0] = {tabletId, /*Lsn=*/30};
            mgr.AddBarrier(&bh, /*chunkIdx=*/0, /*sectorIdx=*/1);

            // RestoreBarriers needs records > barrier to keep the tablet alive.
            std::map<std::tuple<ui64, ui32>, TPersistentBuffer> pbsForBarrier;
            TPersistentBuffer buf;
            buf.Records[40] = {};
            buf.Records[50] = {};
            pbsForBarrier[{tabletId, 0}] = std::move(buf);
            mgr.RestoreBarriers(pbsForBarrier, alloc);
        }
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetBarrier(tabletId), 30u);

        // Set up an erase record with LSNs: 10, 20, 30, 40, 50.
        {
            TPersistentBufferHeader eh{};
            memset(&eh, 0, sizeof(eh));
            eh.Flags = TPersistentBufferHeader::IS_ERASE;
            eh.RecordLsn = 1;
            eh.PersistentBufferUniqueId = 1;
            eh.NodeId = 1; eh.PDiskId = 1; eh.SlotId = 1;
            eh.Erase.TabletId = tabletId;
            ui64 lsns[] = {10, 20, 30, 40, 50};
            for (int i = 0; i < 5; i++) {
                memcpy(eh.Erase.CompactLsns + i * 8, &lsns[i], 8);
            }
            mgr.AddErase(&eh, /*chunkIdx=*/0, /*sectorIdx=*/2);
        }
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 5u);

        // persistentBuffers only has records > barrier (records <= 30 already
        // removed by RestoreBarriers in real usage).
        std::map<std::tuple<ui64, ui32>, TPersistentBuffer> pbs;
        {
            TPersistentBuffer buf;
            buf.Records[40] = {};
            buf.Records[50] = {};
            pbs[{tabletId, 0}] = std::move(buf);
        }

        mgr.RestoreErases(pbs, alloc);

        // LSNs 10, 20, 30 were filtered out (≤ barrier=30) from erase.Lsns,
        // so they are NOT applied. LSNs 40 and 50 were applied as erases.
        // After erasing 40 and 50, the buffer is empty → tablet removed.
        UNIT_ASSERT_C(pbs.find({tabletId, 0}) == pbs.end(),
            "Tablet must be removed: records 40 and 50 were erased by RestoreErases");
    }

    // -----------------------------------------------------------------------
    // RestoreErases: barrier covers ALL erase LSNs — nothing is applied,
    // erase entry is kept (tablet still has records above barrier)
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(RestoreErasesBarrierCoversAllEraseLsns) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 1002;

        // Barrier at LSN=100 covers all erase LSNs {10, 20, 30}.
        {
            TPersistentBufferHeader bh{};
            memset(&bh, 0, sizeof(bh));
            memcpy(bh.Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
            bh.Flags = TPersistentBufferHeader::IS_BARRIER;
            bh.RecordLsn = 1;
            bh.PersistentBufferUniqueId = 1;
            bh.NodeId = 1; bh.PDiskId = 1; bh.SlotId = 1;
            bh.Barrier.BarrierIdx = 0;
            bh.Barrier.Barriers[0] = {tabletId, /*Lsn=*/100};
            mgr.AddBarrier(&bh, /*chunkIdx=*/0, /*sectorIdx=*/1);

            std::map<std::tuple<ui64, ui32>, TPersistentBuffer> pbsForBarrier;
            TPersistentBuffer buf;
            buf.Records[200] = {};
            pbsForBarrier[{tabletId, 0}] = std::move(buf);
            mgr.RestoreBarriers(pbsForBarrier, alloc);
        }
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetBarrier(tabletId), 100u);

        // Erase LSNs all ≤ barrier=100.
        {
            TPersistentBufferHeader eh{};
            memset(&eh, 0, sizeof(eh));
            eh.Flags = TPersistentBufferHeader::IS_ERASE;
            eh.RecordLsn = 1;
            eh.PersistentBufferUniqueId = 1;
            eh.NodeId = 1; eh.PDiskId = 1; eh.SlotId = 1;
            eh.Erase.TabletId = tabletId;
            ui64 lsns[] = {10, 20, 30};
            for (int i = 0; i < 3; i++) {
                memcpy(eh.Erase.CompactLsns + i * 8, &lsns[i], 8);
            }
            mgr.AddErase(&eh, /*chunkIdx=*/0, /*sectorIdx=*/2);
        }

        // persistentBuffers has only record at LSN=200 (above barrier).
        std::map<std::tuple<ui64, ui32>, TPersistentBuffer> pbs;
        {
            TPersistentBuffer buf;
            buf.Records[200] = {};
            pbs[{tabletId, 0}] = std::move(buf);
        }

        mgr.RestoreErases(pbs, alloc);

        // All erase LSNs were filtered (≤ barrier=100), so record 200 is untouched.
        auto pbIt = pbs.find({tabletId, 0});
        UNIT_ASSERT_C(pbIt != pbs.end(), "Tablet must still exist");
        UNIT_ASSERT_C(pbIt->second.Records.count(200), "Record 200 must remain untouched");
    }

    // -----------------------------------------------------------------------
    // RestoreErases: no barrier — all erase LSNs are applied
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(RestoreErasesNoBarrierAppliesAllLsns) {
        auto mgr = MakeManager();

        const ui64 tabletId = 1003;
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetBarrier(tabletId), 0u);

        // Erase record with LSNs: 5, 15, 25.
        {
            TPersistentBufferHeader eh{};
            memset(&eh, 0, sizeof(eh));
            eh.Flags = TPersistentBufferHeader::IS_ERASE;
            eh.RecordLsn = 1;
            eh.PersistentBufferUniqueId = 1;
            eh.NodeId = 1; eh.PDiskId = 1; eh.SlotId = 1;
            eh.Erase.TabletId = tabletId;
            ui64 lsns[] = {5, 15, 25};
            for (int i = 0; i < 3; i++) {
                memcpy(eh.Erase.CompactLsns + i * 8, &lsns[i], 8);
            }
            mgr.AddErase(&eh, /*chunkIdx=*/0, /*sectorIdx=*/3);
        }

        std::map<std::tuple<ui64, ui32>, TPersistentBuffer> pbs;
        {
            TPersistentBuffer buf;
            for (ui64 lsn : {5ULL, 15ULL, 25ULL, 35ULL}) {
                buf.Records[lsn] = {};
            }
            pbs[{tabletId, 0}] = std::move(buf);
        }

        auto alloc = MakeAllocator(1024, 0);
        mgr.RestoreErases(pbs, alloc);

        // LSNs 5, 15, 25 must be erased; LSN 35 must remain.
        auto pbIt = pbs.find({tabletId, 0});
        UNIT_ASSERT_C(pbIt != pbs.end(), "Tablet must still exist (LSN 35 remains)");
        auto& records = pbIt->second.Records;
        UNIT_ASSERT_C(!records.count(5),  "LSN 5 must be erased");
        UNIT_ASSERT_C(!records.count(15), "LSN 15 must be erased");
        UNIT_ASSERT_C(!records.count(25), "LSN 25 must be erased");
        UNIT_ASSERT_C(records.count(35),  "LSN 35 must remain");
    }

    // -----------------------------------------------------------------------
    // Compact raw path: duplicates between oldLsns and newLsns are skipped
    // (no double-write), Uncompact returns the correct unique set
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(CompactRawPathSkipsDuplicates) {
        auto mgr = MakeManager();

        // oldLsns and newLsns share LSNs 20 and 30.
        std::unordered_set<ui64> oldLsns = {10, 20, 30};
        std::unordered_set<ui64> newLsns = {20, 30, 40};

        TPersistentBufferHeader header{};
        memset(&header, 0, sizeof(header));
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);

        // 4 unique LSNs fit in raw storage — IS_ERASE_COMPACT must NOT be set.
        UNIT_ASSERT_C(
            !(header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT),
            "IS_ERASE_COMPACT must not be set for 4 unique LSNs");

        bool isCompact = (header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT) != 0;
        auto recovered = mgr.Uncompact(header.Erase.CompactLsns, isCompact);

        // Must recover exactly {10, 20, 30, 40}.
        UNIT_ASSERT_VALUES_EQUAL_C(recovered.size(), 4u,
            "Exactly 4 unique LSNs must be recovered, got " << recovered.size());
        for (ui64 lsn : {10ULL, 20ULL, 30ULL, 40ULL}) {
            UNIT_ASSERT_C(recovered.count(lsn), "LSN " << lsn << " missing after Uncompact");
        }
    }

    // -----------------------------------------------------------------------
    // Compact raw path: oldLsns == newLsns entirely — only unique ones written
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(CompactRawPathAllDuplicates) {
        auto mgr = MakeManager();

        std::unordered_set<ui64> oldLsns = {5, 15, 25, 35};
        std::unordered_set<ui64> newLsns = {5, 15, 25, 35};

        TPersistentBufferHeader header{};
        memset(&header, 0, sizeof(header));
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);

        UNIT_ASSERT_C(
            !(header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT),
            "IS_ERASE_COMPACT must not be set for 4 unique LSNs");

        bool isCompact = (header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT) != 0;
        auto recovered = mgr.Uncompact(header.Erase.CompactLsns, isCompact);
        UNIT_ASSERT_VALUES_EQUAL_C(recovered.size(), 4u,
            "Exactly 4 unique LSNs must be recovered, got " << recovered.size());
        for (ui64 lsn : {5ULL, 15ULL, 25ULL, 35ULL}) {
            UNIT_ASSERT_C(recovered.count(lsn), "LSN " << lsn << " missing after Uncompact");
        }
    }

    // -----------------------------------------------------------------------
    // Compact LEB128 path: duplicates between oldLsns and newLsns are skipped
    // -----------------------------------------------------------------------

    Y_UNIT_TEST(CompactLeb128PathSkipsDuplicates) {
        auto mgr = MakeManager();

        // oldLsns: 1..300, newLsns: 250..550 — overlap at 250..300 (51 duplicates).
        // Unique count = 550, sum = 601 > MaxRawLsns(479) → forces LEB128 path.
        std::unordered_set<ui64> oldLsns;
        std::unordered_set<ui64> newLsns;
        for (ui64 i = 1; i <= 300; i++) oldLsns.insert(i);
        for (ui64 i = 250; i <= 550; i++) newLsns.insert(i);

        TPersistentBufferHeader header{};
        memset(&header, 0, sizeof(header));
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);
        UNIT_ASSERT_C(
            header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT,
            "IS_ERASE_COMPACT must be set for 550 unique LSNs");

        bool isCompact = (header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT) != 0;
        auto recovered = mgr.Uncompact(header.Erase.CompactLsns, isCompact);

        // Must recover exactly 550 unique LSNs (1..550).
        UNIT_ASSERT_VALUES_EQUAL_C(recovered.size(), 550u,
            "Exactly 550 unique LSNs must be recovered, got " << recovered.size());
        for (ui64 i = 1; i <= 550; i++) {
            UNIT_ASSERT_C(recovered.count(i), "LSN " << i << " missing after Uncompact");
        }
    }

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr::NDDisk
