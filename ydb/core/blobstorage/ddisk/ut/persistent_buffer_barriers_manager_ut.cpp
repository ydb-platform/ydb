#include <ydb/core/blobstorage/ddisk/persistent_buffer_barriers_manager.h>
#include <ydb/core/blobstorage/ddisk/persistent_buffer_space_allocator.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDDisk {

namespace {

TPersistentBufferSpaceAllocator MakeAllocator(ui32 sectorsInChunk = 1024, ui32 chunkIdx = 0) {
    TPersistentBufferSpaceAllocator alloc(sectorsInChunk);
    alloc.AddNewChunk(chunkIdx);
    return alloc;
}

TPersistentBufferBarriersManager MakeManager() {
    TPersistentBufferBarriersManager mgr;
    mgr.Initialize(/*uniqueId=*/1, /*nodeId=*/1, /*pdiskId=*/1, /*slotId=*/1);
    return mgr;
}

TPersistentBufferSectorInfo MakeSector(ui32 chunkIdx, ui32 sectorIdx) {
    TPersistentBufferSectorInfo s{};
    s.ChunkIdx = chunkIdx;
    s.SectorIdx = sectorIdx;
    return s;
}

static constexpr ui32 MaxRawLsns = TPersistentBufferFastErases::ErasesBufferSize / sizeof(ui64);

} // namespace

Y_UNIT_TEST_SUITE(TPersistentBufferBarriersManagerTest) {

    Y_UNIT_TEST(CompactDoesNotSetFlagWhenFitsRaw) {
        auto mgr = MakeManager();

        // Use exactly MaxRawLsns LSNs split across old and new – fits raw.
        std::vector<ui64> oldLsns;
        std::vector<ui64> newLsns;
        for (ui64 i = 1; i <= 200; i++) oldLsns.push_back(i * 10);
        for (ui64 i = 201; i <= MaxRawLsns; i++) newLsns.push_back(i * 10);

        TPersistentBufferFastErases header{};
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);

        // IS_ERASE_COMPACT must NOT be set.
        UNIT_ASSERT_C(
            !(header.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT),
            "IS_ERASE_COMPACT must not be set when data fits in raw storage");
    }

    Y_UNIT_TEST(CompactSetsFlagWhenExceedsRaw) {
        auto mgr = MakeManager();

        // Use MaxRawLsns + 1 LSNs total – forces LEB128 delta encoding.
        std::vector<ui64> oldLsns;
        std::vector<ui64> newLsns;
        for (ui64 i = 1; i <= 300; i++) oldLsns.push_back(i);
        for (ui64 i = 301; i <= MaxRawLsns + 1; i++) newLsns.push_back(i);

        TPersistentBufferFastErases header{};
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);

        // IS_ERASE_COMPACT must be set.
        UNIT_ASSERT_C(
            header.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT,
            "IS_ERASE_COMPACT must be set when data requires delta encoding");
    }

    Y_UNIT_TEST(CompactUncompactRoundTripWithFlag) {
        auto mgr = MakeManager();

        // Build a set large enough to require LEB128 encoding.
        std::vector<ui64> oldLsns;
        std::vector<ui64> newLsns;
        for (ui64 i = 1; i <= 300; i++) oldLsns.push_back(i);
        for (ui64 i = 301; i <= MaxRawLsns + 1; i++) newLsns.push_back(i);

        TPersistentBufferFastErases header{};
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);
        UNIT_ASSERT(header.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT);

        bool isCompact = (header.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT) != 0;
        auto recovered = mgr.Uncompact(header.CompactLsns, isCompact);

        const ui64 expectedCount = MaxRawLsns + 1;
        UNIT_ASSERT_VALUES_EQUAL(recovered.size(), expectedCount);
        for (ui64 i = 1; i <= expectedCount; i++) {
            UNIT_ASSERT_C(recovered[i - 1] == i, "LSN " << i << " missing after Uncompact");
        }
    }

    Y_UNIT_TEST(CompactUncompactRoundTripNoFlag) {
        auto mgr = MakeManager();

        // Use a small set that fits in raw storage (no delta encoding).
        std::vector<ui64> oldLsns = {5, 15};
        std::vector<ui64> newLsns = {25, 35};

        TPersistentBufferFastErases header{};
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);
        UNIT_ASSERT(!(header.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT));

        bool isCompact = (header.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT) != 0;
        auto recovered = mgr.Uncompact(header.CompactLsns, isCompact);
        std::vector<ui64> expected = {5ULL, 15ULL, 25ULL, 35ULL};
        UNIT_ASSERT_VALUES_EQUAL(recovered, expected);

    }

    Y_UNIT_TEST(EraseBasicSucceeds) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator();

        const ui64 tabletId = 100;
        std::vector<ui64> lsns = {10, 20};

        auto result = mgr.Erase(tabletId, 1, lsns, alloc);
        UNIT_ASSERT(result.has_value());

        // IS_ERASE must be set.
        UNIT_ASSERT(result->Header.Header.Flags & TPersistentBufferHeader::IS_ERASE);

        // The erase record must contain both LSNs.
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 2u);
    }

    Y_UNIT_TEST(EraseAddEraseRoundTripWithCompactData) {
        auto mgr = MakeManager();
        // Need enough free space for multiple Erase calls.
        auto alloc = MakeAllocator(32768, 0);

        const ui64 tabletId = 200;

        // First batch: 240 LSNs (fits raw).
        std::vector<ui64> firstBatch;
        for (ui64 i = 1; i <= 240; i++) firstBatch.push_back(i);
        auto r1 = mgr.Erase(tabletId, 1, firstBatch, alloc);
        UNIT_ASSERT(r1.has_value());
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 240u);

        std::vector<ui64> secondBatch;
        for (ui64 i = 241; i <= 540; i++) secondBatch.push_back(i);
        auto r2 = mgr.Erase(tabletId, 1, secondBatch, alloc);
        UNIT_ASSERT(r2.has_value());
        UNIT_ASSERT(r2->Header.Header.Flags & TPersistentBufferHeader::IS_ERASE);

        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 540u);

        TPersistentBufferBarriersManager mgr2;
        mgr2.Initialize(1, 1, 1, 1);

        // Build a header that correctly reflects what Compact() produced.
        std::vector<ui64> allLsns;
        for (ui64 i = 1; i <= 540; i++) allLsns.push_back(i);
        std::vector<ui64> emptyOld;
        TPersistentBufferFastErases compactHeader{};
        bool ok = mgr2.Compact(emptyOld, allLsns, compactHeader);
        UNIT_ASSERT(ok);
        UNIT_ASSERT(compactHeader.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT);

        compactHeader.Header.Flags |= TPersistentBufferHeader::IS_ERASE;
        compactHeader.Header.RecordLsn = 1;
        compactHeader.TabletId = tabletId;

        bool added = mgr2.AddErase((TPersistentBufferHeader*)&compactHeader, 0, 0);
        UNIT_ASSERT(added);
        UNIT_ASSERT_VALUES_EQUAL(mgr2.GetErasesCount(tabletId), 540u);
    }

    Y_UNIT_TEST(AddEraseCompact) {
        auto mgr = MakeManager();

        // Build a compact header via Compact().
        std::vector<ui64> oldLsns;
        std::vector<ui64> newLsns;
        for (ui64 i = 1; i <= 300; i++) oldLsns.push_back(i);
        for (ui64 i = 301; i <= MaxRawLsns + 1; i++) newLsns.push_back(i);

        TPersistentBufferFastErases header{};
        memset(&header, 0, sizeof(header));
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);
        UNIT_ASSERT(header.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT);

        header.Header.Flags |= TPersistentBufferHeader::IS_ERASE;
        header.Header.RecordLsn = 1;
        header.TabletId = 2000;

        bool added = mgr.AddErase((TPersistentBufferHeader*)&header, /*chunkIdx=*/0, /*sectorIdx=*/0);
        UNIT_ASSERT(added);
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(2000), MaxRawLsns + 1);
    }

    Y_UNIT_TEST(AddEraseNonCompact) {
        auto mgr = MakeManager();

        TPersistentBufferFastErases header{};
        memset(&header, 0, sizeof(header));
        header.Header.Flags = TPersistentBufferHeader::IS_ERASE;
        header.Header.RecordLsn = 1;
        header.TabletId = 1000;

        ui64 lsn1 = 111, lsn2 = 222;
        memcpy(header.CompactLsns, &lsn1, 8);
        memcpy(header.CompactLsns + 8, &lsn2, 8);

        bool added = mgr.AddErase((TPersistentBufferHeader*)&header, /*chunkIdx=*/0, /*sectorIdx=*/0);
        UNIT_ASSERT(added);
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(1000), 2u);
    }

    Y_UNIT_TEST(MoveBarrierCleansEraseLsns) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 300;

        // Register some erased LSNs: 10, 20, 30, 40, 50.
        std::vector<ui64> erasedLsns = {10, 20, 30, 40, 50};
        auto eraseResult = mgr.Erase(tabletId, 1, erasedLsns, alloc);
        UNIT_ASSERT(eraseResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 5u);

        // Move barrier to LSN=30: LSNs <= 30 (i.e. 10, 20, 30) must be removed.
        TPersistentBufferSectorInfo sector = MakeSector(0, 5);
        mgr.MoveBarrier(tabletId, 1, /*lsn=*/30, sector);

        // Only LSNs 40 and 50 should remain.
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 2u);
    }

    Y_UNIT_TEST(MoveBarrierClearsAllEraseLsns) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 400;

        std::vector<ui64> erasedLsns = {5, 15, 25};
        auto eraseResult = mgr.Erase(tabletId, 1, erasedLsns, alloc);
        UNIT_ASSERT(eraseResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 3u);

        // Advance barrier past all erased LSNs.
        TPersistentBufferSectorInfo sector = MakeSector(0, 10);
        mgr.MoveBarrier(tabletId, 1, /*lsn=*/100, sector);

        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 0u);
    }

    Y_UNIT_TEST(MoveBarrierKeepsLsnsAboveBarrier) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 500;

        std::vector<ui64> erasedLsns = {10, 20, 30, 40, 50};
        auto eraseResult = mgr.Erase(tabletId, 1, erasedLsns, alloc);
        UNIT_ASSERT(eraseResult.has_value());

        // Barrier at LSN=15: only LSN 10 should be removed; 20,30,40,50 remain.
        TPersistentBufferSectorInfo sector = MakeSector(0, 7);
        mgr.MoveBarrier(tabletId, 1, /*lsn=*/15, sector);

        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 4u);
    }

    Y_UNIT_TEST(MoveBarrierNoEraseRecord) {
        auto mgr = MakeManager();

        const ui64 tabletId = 600;

        // No Erase() called for this tablet – MoveBarrier must not crash.
        TPersistentBufferSectorInfo sector = MakeSector(0, 1);
        mgr.MoveBarrier(tabletId, 1, /*lsn=*/50, sector);

        // Barrier should be registered.
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetBarrier(tabletId).Lsn, 50u);
    }

    Y_UNIT_TEST(MoveBarrierProgressivelyCleansEraseLsns) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 700;

        std::vector<ui64> erasedLsns = {10, 20, 30, 40, 50, 60, 70, 80};
        auto eraseResult = mgr.Erase(tabletId, 1, erasedLsns, alloc);
        UNIT_ASSERT(eraseResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 8u);

        TPersistentBufferSectorInfo sector1 = MakeSector(0, 2);
        mgr.MoveBarrier(tabletId, 1, /*lsn=*/25, sector1);
        // LSNs 10, 20 removed; 30,40,50,60,70,80 remain.
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 6u);

        TPersistentBufferSectorInfo sector2 = MakeSector(0, 3);
        mgr.MoveBarrier(tabletId, 1, /*lsn=*/55, sector2);
        // LSNs 30,40,50 removed; 60,70,80 remain.
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 3u);

        TPersistentBufferSectorInfo sector3 = MakeSector(0, 4);
        mgr.MoveBarrier(tabletId, 1, /*lsn=*/80, sector3);
        // All remaining LSNs removed.
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 0u);
    }

    Y_UNIT_TEST(EraseReturnsNulloptWhenNoFreeSpace) {
        auto mgr = MakeManager();
        // Allocator with only 1 sector – Erase requires >= 2 free sectors.
        TPersistentBufferSpaceAllocator alloc(1);
        alloc.AddNewChunk(0);

        const ui64 tabletId = 800;
        std::vector<ui64> lsns = {1, 2};

        auto result = mgr.Erase(tabletId, 1, lsns, alloc);
        UNIT_ASSERT(!result.has_value());
    }

    Y_UNIT_TEST(EraseReturnsNulloptWhenTooFewLsns) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 900;
        std::vector<ui64> lsns = {42};  // only 1 LSN

        auto result = mgr.Erase(tabletId, 1, lsns, alloc);
        UNIT_ASSERT(!result.has_value());
    }

    Y_UNIT_TEST(RestoreErasesFiltersLsnsBehindBarrier) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 1001;

        // Set up a barrier at LSN=30 for the tablet via AddBarrier + RestoreBarriers.
        {
            TPersistentBufferBarriers bh{};
            memset(&bh, 0, sizeof(bh));
            memcpy(bh.Header.Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
            bh.Header.Flags = TPersistentBufferHeader::IS_BARRIER;
            bh.Header.RecordLsn = 1;
            bh.Header.PersistentBufferUniqueId = 1;
            bh.Header.NodeId = 1; bh.Header.PDiskId = 1; bh.Header.SlotId = 1;
            bh.Header.RecordIdx = 0;
            bh.Barriers[0] = {tabletId, /*Generation=*/1, /*Lsn=*/30};
            mgr.AddBarrier((TPersistentBufferHeader*)&bh, /*chunkIdx=*/0, /*sectorIdx=*/1);

            // RestoreBarriers needs records > barrier to keep the tablet alive.
            std::map<TPersistentBufferId, TPersistentBuffer> pbsForBarrier;
            TPersistentBuffer buf;
            buf.Records[40] = {};
            buf.Records[50] = {};
            pbsForBarrier[{tabletId, 0}] = std::move(buf);
            mgr.RestoreBarriers(pbsForBarrier, alloc);
        }
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetBarrier(tabletId).Lsn, 30u);

        // Set up an erase record with LSNs: 10, 20, 30, 40, 50.
        {
            TPersistentBufferFastErases eh{};
            memset(&eh, 0, sizeof(eh));
            eh.Header.Flags = TPersistentBufferHeader::IS_ERASE;
            eh.Header.RecordLsn = 1;
            eh.Header.PersistentBufferUniqueId = 1;
            eh.Header.NodeId = 1; eh.Header.PDiskId = 1; eh.Header.SlotId = 1;
            eh.TabletId = tabletId;
            ui64 lsns[] = {10, 20, 30, 40, 50};
            for (int i = 0; i < 5; i++) {
                memcpy(eh.CompactLsns + i * 8, &lsns[i], 8);
            }
            mgr.AddErase((TPersistentBufferHeader*)&eh, /*chunkIdx=*/0, /*sectorIdx=*/2);
        }
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 5u);

        // persistentBuffers only has records > barrier (records <= 30 already
        // removed by RestoreBarriers in real usage).
        std::map<TPersistentBufferId, TPersistentBuffer> pbs;
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

    Y_UNIT_TEST(RestoreErasesBarrierCoversAllEraseLsns) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 1002;

        // Barrier at LSN=100 covers all erase LSNs {10, 20, 30}.
        {
            TPersistentBufferBarriers bh{};
            memset(&bh, 0, sizeof(bh));
            memcpy(bh.Header.Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
            bh.Header.Flags = TPersistentBufferHeader::IS_BARRIER;
            bh.Header.RecordLsn = 1;
            bh.Header.PersistentBufferUniqueId = 1;
            bh.Header.NodeId = 1; bh.Header.PDiskId = 1; bh.Header.SlotId = 1;
            bh.Header.RecordIdx = 0;
            bh.Barriers[0] = {tabletId, /*Generation=*/1, /*Lsn=*/100};
            mgr.AddBarrier((TPersistentBufferHeader*)&bh, /*chunkIdx=*/0, /*sectorIdx=*/1);

            std::map<TPersistentBufferId, TPersistentBuffer> pbsForBarrier;
            TPersistentBuffer buf;
            buf.Records[200] = {};
            pbsForBarrier[{tabletId, 0}] = std::move(buf);
            mgr.RestoreBarriers(pbsForBarrier, alloc);
        }
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetBarrier(tabletId).Lsn, 100u);

        // Erase LSNs all ≤ barrier=100.
        {
            TPersistentBufferFastErases eh{};
            memset(&eh, 0, sizeof(eh));
            eh.Header.Flags = TPersistentBufferHeader::IS_ERASE;
            eh.Header.RecordLsn = 1;
            eh.Header.PersistentBufferUniqueId = 1;
            eh.Header.NodeId = 1; eh.Header.PDiskId = 1; eh.Header.SlotId = 1;
            eh.TabletId = tabletId;
            ui64 lsns[] = {10, 20, 30};
            for (int i = 0; i < 3; i++) {
                memcpy(eh.CompactLsns + i * 8, &lsns[i], 8);
            }
            mgr.AddErase((TPersistentBufferHeader*)&eh, /*chunkIdx=*/0, /*sectorIdx=*/2);
        }

        // persistentBuffers has only record at LSN=200 (above barrier).
        std::map<TPersistentBufferId, TPersistentBuffer> pbs;
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

    Y_UNIT_TEST(RestoreErasesNoBarrierAppliesAllLsns) {
        auto mgr = MakeManager();

        const ui64 tabletId = 1003;
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetBarrier(tabletId).Lsn, 0u);

        // Erase record with LSNs: 5, 15, 25.
        {
            TPersistentBufferFastErases eh{};
            memset(&eh, 0, sizeof(eh));
            eh.Header.Flags = TPersistentBufferHeader::IS_ERASE;
            eh.Header.RecordLsn = 1;
            eh.Header.PersistentBufferUniqueId = 1;
            eh.Header.NodeId = 1; eh.Header.PDiskId = 1; eh.Header.SlotId = 1;
            eh.TabletId = tabletId;
            ui64 lsns[] = {5, 15, 25};
            for (int i = 0; i < 3; i++) {
                memcpy(eh.CompactLsns + i * 8, &lsns[i], 8);
            }
            mgr.AddErase((TPersistentBufferHeader*)&eh, /*chunkIdx=*/0, /*sectorIdx=*/3);
        }

        std::map<TPersistentBufferId, TPersistentBuffer> pbs;
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

    Y_UNIT_TEST(CompactRawPathSkipsDuplicates) {
        auto mgr = MakeManager();

        // oldLsns and newLsns share LSNs 20 and 30.
        std::vector<ui64> oldLsns = {10, 20, 30};
        std::vector<ui64> newLsns = {20, 30, 40};

        TPersistentBufferFastErases header{};
        memset(&header, 0, sizeof(header));
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);

        // 4 unique LSNs fit in raw storage — IS_ERASE_COMPACT must NOT be set.
        UNIT_ASSERT_C(
            !(header.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT),
            "IS_ERASE_COMPACT must not be set for 4 unique LSNs");

        bool isCompact = (header.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT) != 0;
        auto recovered = mgr.Uncompact(header.CompactLsns, isCompact);
        std::vector<ui64> expected = {10ULL, 20ULL, 30ULL, 40ULL};
        UNIT_ASSERT_VALUES_EQUAL(recovered, expected);
    }

    Y_UNIT_TEST(CompactRawPathAllDuplicates) {
        auto mgr = MakeManager();

        std::vector<ui64> oldLsns = {5, 15, 25, 35};
        std::vector<ui64> newLsns = oldLsns;

        TPersistentBufferFastErases header{};
        memset(&header, 0, sizeof(header));
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);

        UNIT_ASSERT_C(
            !(header.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT),
            "IS_ERASE_COMPACT must not be set for 4 unique LSNs");

        bool isCompact = (header.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT) != 0;
        auto recovered = mgr.Uncompact(header.CompactLsns, isCompact);
        UNIT_ASSERT_VALUES_EQUAL(recovered, newLsns);
    }

    Y_UNIT_TEST(CompactLeb128PathSkipsDuplicates) {
        auto mgr = MakeManager();

        // oldLsns: 1..300, newLsns: 250..550 — overlap at 250..300 (51 duplicates).
        // Unique count = 550, sum = 601 > MaxRawLsns(479) → forces LEB128 path.
        std::vector<ui64> oldLsns;
        std::vector<ui64> newLsns;
        for (ui64 i = 1; i <= 300; i++) oldLsns.push_back(i);
        for (ui64 i = 250; i <= 550; i++) newLsns.push_back(i);

        TPersistentBufferFastErases header{};
        memset(&header, 0, sizeof(header));
        bool ok = mgr.Compact(oldLsns, newLsns, header);
        UNIT_ASSERT(ok);
        UNIT_ASSERT_C(
            header.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT,
            "IS_ERASE_COMPACT must be set for 550 unique LSNs");

        bool isCompact = (header.Header.Flags & TPersistentBufferHeader::IS_ERASE_COMPACT) != 0;
        auto recovered = mgr.Uncompact(header.CompactLsns, isCompact);

        // Must recover exactly 550 unique LSNs (1..550).
        UNIT_ASSERT_VALUES_EQUAL_C(recovered.size(), 550u,
            "Exactly 550 unique LSNs must be recovered, got " << recovered.size());
        for (ui64 i = 1; i <= 550; i++) {
            UNIT_ASSERT_C(recovered[i - 1] == i, "LSN " << i << " missing after Uncompact");
        }
    }

    // ── Generation-aware barrier semantics ──────────────────────────────────

    // MoveBarrier to a higher generation must clear ALL previously accumulated
    // erase LSNs (they belong to the old generation and are no longer relevant).
    Y_UNIT_TEST(MoveBarrierNewGenerationClearsAllEraseLsns) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 2001;

        // Register erases for generation 1.
        std::vector<ui64> erasedLsns = {10, 20, 30, 40, 50};
        auto eraseResult = mgr.Erase(tabletId, /*generation=*/1, erasedLsns, alloc);
        UNIT_ASSERT(eraseResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 5u);

        // Move barrier to generation 2 (higher generation).
        // All erase LSNs from generation 1 must be discarded.
        TPersistentBufferSectorInfo sector = MakeSector(0, 5);
        mgr.MoveBarrier(tabletId, /*generation=*/2, /*lsn=*/1, sector);

        UNIT_ASSERT_VALUES_EQUAL_C(mgr.GetErasesCount(tabletId), 0u,
            "All erase LSNs from old generation must be cleared when barrier moves to a new generation");
    }

    // MoveBarrier within the same generation must NOT clear erase LSNs that are
    // above the new barrier LSN.
    Y_UNIT_TEST(MoveBarrierSameGenerationKeepsLsnsAboveBarrier) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 2002;

        std::vector<ui64> erasedLsns = {5, 10, 15, 20, 25};
        auto eraseResult = mgr.Erase(tabletId, /*generation=*/3, erasedLsns, alloc);
        UNIT_ASSERT(eraseResult.has_value());
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 5u);

        // Move barrier within the same generation to LSN=12.
        // LSNs 5, 10 (≤12) must be removed; 15, 20, 25 must remain.
        TPersistentBufferSectorInfo sector = MakeSector(0, 6);
        mgr.MoveBarrier(tabletId, /*generation=*/3, /*lsn=*/12, sector);

        UNIT_ASSERT_VALUES_EQUAL_C(mgr.GetErasesCount(tabletId), 3u,
            "Only LSNs above the barrier must remain after MoveBarrier within the same generation");
    }

    // CanFastErase must return false when the barrier is on a different generation
    // than the one being erased.
    Y_UNIT_TEST(CanFastEraseReturnsFalseForDifferentGeneration) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 2003;

        // Set barrier at generation 1.
        TPersistentBufferSectorInfo sector = MakeSector(0, 1);
        mgr.MoveBarrier(tabletId, /*generation=*/1, /*lsn=*/10, sector);
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetBarrier(tabletId).Generation, 1u);

        // Fast erase for generation 2 must be disallowed.
        UNIT_ASSERT_C(!mgr.CanFastErase(tabletId, /*generation=*/2),
            "CanFastErase must return false when barrier generation != requested generation");

        // Fast erase for generation 1 (same as barrier) must be allowed.
        UNIT_ASSERT_C(mgr.CanFastErase(tabletId, /*generation=*/1),
            "CanFastErase must return true when barrier generation == requested generation");
    }

    // CanFastErase must return false when no barrier exists yet (generation 0).
    Y_UNIT_TEST(CanFastEraseReturnsFalseWhenNoBarrier) {
        auto mgr = MakeManager();

        const ui64 tabletId = 2004;
        // No barrier set → GetBarrier returns {tabletId, 0, 0}.
        UNIT_ASSERT_C(!mgr.CanFastErase(tabletId, /*generation=*/1),
            "CanFastErase must return false when no barrier has been set");
    }

    // Erase() must reset its accumulated LSNs when called with a higher generation
    // than the one stored in the erase record.
    Y_UNIT_TEST(EraseResetsLsnsOnGenerationBump) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 2005;

        // First erase batch for generation 1.
        std::vector<ui64> batch1 = {10, 20, 30};
        auto r1 = mgr.Erase(tabletId, /*generation=*/1, batch1, alloc);
        UNIT_ASSERT(r1.has_value());
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 3u);

        // Second erase batch for generation 2 (higher).
        // The old LSNs from generation 1 must be discarded; only the new batch counts.
        std::vector<ui64> batch2 = {5, 15};
        auto r2 = mgr.Erase(tabletId, /*generation=*/2, batch2, alloc);
        UNIT_ASSERT(r2.has_value());
        UNIT_ASSERT_VALUES_EQUAL_C(mgr.GetErasesCount(tabletId), 2u,
            "Erase must reset accumulated LSNs when generation increases");
    }

    // RestoreErases must discard erase records whose generation does not match
    // any entry in persistentBuffers (old-generation records already cleaned up).
    Y_UNIT_TEST(RestoreErasesDropsRecordForMissingGeneration) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 2006;

        // Simulate an erase record for generation 1 that was persisted.
        {
            TPersistentBufferFastErases eh{};
            memset(&eh, 0, sizeof(eh));
            eh.Header.Flags = TPersistentBufferHeader::IS_ERASE;
            eh.Header.RecordLsn = 1;
            eh.Header.PersistentBufferUniqueId = 1;
            eh.Header.NodeId = 1; eh.Header.PDiskId = 1; eh.Header.SlotId = 1;
            eh.TabletId = tabletId;
            eh.Generation = 1;
            ui64 lsns[] = {5, 15, 25};
            for (int i = 0; i < 3; i++) {
                memcpy(eh.CompactLsns + i * 8, &lsns[i], 8);
            }
            mgr.AddErase((TPersistentBufferHeader*)&eh, /*chunkIdx=*/0, /*sectorIdx=*/2);
        }
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 3u);

        // persistentBuffers only has generation 2 records (generation 1 was already
        // cleaned up by RestoreBarriers or never written).
        std::map<TPersistentBufferId, TPersistentBuffer> pbs;
        {
            TPersistentBuffer buf;
            buf.Records[5] = {};
            pbs[{tabletId, 2}] = std::move(buf);
        }

        mgr.RestoreErases(pbs, alloc);

        // The erase record for generation 1 must be dropped because there is no
        // {tabletId, 1} entry in persistentBuffers.
        // The generation-2 record must remain untouched.
        auto pbIt = pbs.find({tabletId, 2});
        UNIT_ASSERT_C(pbIt != pbs.end(), "Generation-2 record must survive");
        UNIT_ASSERT_C(pbIt->second.Records.count(5), "LSN 5 in generation 2 must not be erased");
    }

    // RestoreBarriers must remove all records from older generations when the
    // barrier is on a newer generation.
    Y_UNIT_TEST(RestoreBarriersRemovesOldGenerationRecords) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 2007;

        // Set up a barrier at generation 2, LSN=5.
        {
            TPersistentBufferBarriers bh{};
            memset(&bh, 0, sizeof(bh));
            memcpy(bh.Header.Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
            bh.Header.Flags = TPersistentBufferHeader::IS_BARRIER;
            bh.Header.RecordLsn = 1;
            bh.Header.PersistentBufferUniqueId = 1;
            bh.Header.NodeId = 1; bh.Header.PDiskId = 1; bh.Header.SlotId = 1;
            bh.Header.RecordIdx = 0;
            bh.Barriers[0] = {tabletId, /*Generation=*/2, /*Lsn=*/5};
            mgr.AddBarrier((TPersistentBufferHeader*)&bh, /*chunkIdx=*/0, /*sectorIdx=*/1);
        }

        // persistentBuffers has records from generation 1 (old) and generation 2 (current).
        std::map<TPersistentBufferId, TPersistentBuffer> pbs;
        {
            // Generation 1 records — all must be removed.
            TPersistentBuffer buf1;
            buf1.Records[1] = {};
            buf1.Records[100] = {};
            pbs[{tabletId, 1}] = std::move(buf1);

            // Generation 2 records — LSN ≤ 5 removed, LSN > 5 kept.
            TPersistentBuffer buf2;
            buf2.Records[3] = {};   // ≤ barrier LSN 5 → removed
            buf2.Records[10] = {};  // > barrier LSN 5 → kept
            pbs[{tabletId, 2}] = std::move(buf2);
        }

        mgr.RestoreBarriers(pbs, alloc);

        // Generation 1 must be completely gone.
        UNIT_ASSERT_C(pbs.find({tabletId, 1}) == pbs.end(),
            "All generation-1 records must be removed when barrier is at generation 2");

        // Generation 2: only LSN > 5 must survive.
        auto it2 = pbs.find({tabletId, 2});
        UNIT_ASSERT_C(it2 != pbs.end(), "Generation-2 entry must still exist");
        UNIT_ASSERT_C(!it2->second.Records.count(3), "LSN 3 (≤ barrier) must be removed");
        UNIT_ASSERT_C(it2->second.Records.count(10), "LSN 10 (> barrier) must remain");
    }

    // LSN 1 in generation 2 must be independent from LSN 1 in generation 1.
    // Writing LSN 1 in gen=2 after LSN 1 in gen=1 must not be treated as a duplicate.
    Y_UNIT_TEST(LsnResetAcrossGenerations) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 2008;

        // Erase LSN 1 in generation 1.
        std::vector<ui64> batch1 = {1, 2};
        auto r1 = mgr.Erase(tabletId, /*generation=*/1, batch1, alloc);
        UNIT_ASSERT(r1.has_value());
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetErasesCount(tabletId), 2u);

        // Now erase LSN 1 in generation 2 — must start fresh.
        std::vector<ui64> batch2 = {1, 2};
        auto r2 = mgr.Erase(tabletId, /*generation=*/2, batch2, alloc);
        UNIT_ASSERT(r2.has_value());
        // After generation bump, old LSNs are cleared; only the new batch (2 LSNs) counts.
        UNIT_ASSERT_VALUES_EQUAL_C(mgr.GetErasesCount(tabletId), 2u,
            "LSN counter must reset when generation increases");
    }

    // MoveBarrier to a new generation must update the stored barrier generation.
    Y_UNIT_TEST(MoveBarrierUpdatesGeneration) {
        auto mgr = MakeManager();

        const ui64 tabletId = 2009;

        TPersistentBufferSectorInfo s1 = MakeSector(0, 1);
        mgr.MoveBarrier(tabletId, /*generation=*/1, /*lsn=*/10, s1);
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetBarrier(tabletId).Generation, 1u);
        UNIT_ASSERT_VALUES_EQUAL(mgr.GetBarrier(tabletId).Lsn, 10u);

        TPersistentBufferSectorInfo s2 = MakeSector(0, 2);
        mgr.MoveBarrier(tabletId, /*generation=*/2, /*lsn=*/1, s2);
        UNIT_ASSERT_VALUES_EQUAL_C(mgr.GetBarrier(tabletId).Generation, 2u,
            "Barrier generation must be updated after MoveBarrier to a new generation");
        UNIT_ASSERT_VALUES_EQUAL_C(mgr.GetBarrier(tabletId).Lsn, 1u,
            "Barrier LSN must be set to the new value after generation bump");
    }

    // RestoreBarriers with duplicate barrier records: the one with the higher
    // generation (or higher LSN within the same generation) must win.
    Y_UNIT_TEST(RestoreBarriersDuplicateHigherGenerationWins) {
        auto mgr = MakeManager();
        auto alloc = MakeAllocator(1024, 0);

        const ui64 tabletId = 2010;

        // First barrier record: generation 1, LSN 50.
        {
            TPersistentBufferBarriers bh{};
            memset(&bh, 0, sizeof(bh));
            memcpy(bh.Header.Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
            bh.Header.Flags = TPersistentBufferHeader::IS_BARRIER;
            bh.Header.RecordLsn = 1;
            bh.Header.PersistentBufferUniqueId = 1;
            bh.Header.NodeId = 1; bh.Header.PDiskId = 1; bh.Header.SlotId = 1;
            bh.Header.RecordIdx = 0;
            bh.Barriers[0] = {tabletId, /*Generation=*/1, /*Lsn=*/50};
            mgr.AddBarrier((TPersistentBufferHeader*)&bh, /*chunkIdx=*/0, /*sectorIdx=*/1);
        }

        // Second barrier record (different sector): generation 2, LSN 5.
        // Higher generation must win even though LSN is lower.
        {
            TPersistentBufferBarriers bh{};
            memset(&bh, 0, sizeof(bh));
            memcpy(bh.Header.Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
            bh.Header.Flags = TPersistentBufferHeader::IS_BARRIER;
            bh.Header.RecordLsn = 2;
            bh.Header.PersistentBufferUniqueId = 1;
            bh.Header.NodeId = 1; bh.Header.PDiskId = 1; bh.Header.SlotId = 1;
            bh.Header.RecordIdx = 0;
            bh.Barriers[0] = {tabletId, /*Generation=*/2, /*Lsn=*/5};
            mgr.AddBarrier((TPersistentBufferHeader*)&bh, /*chunkIdx=*/0, /*sectorIdx=*/2);
        }

        // persistentBuffers: only generation-2 records survive (gen-1 was cleaned up).
        std::map<TPersistentBufferId, TPersistentBuffer> pbs;
        {
            TPersistentBuffer buf;
            buf.Records[10] = {};
            pbs[{tabletId, 2}] = std::move(buf);
        }

        mgr.RestoreBarriers(pbs, alloc);

        // The generation-2 barrier (LSN=5) must be active.
        UNIT_ASSERT_VALUES_EQUAL_C(mgr.GetBarrier(tabletId).Generation, 2u,
            "Higher-generation barrier must win during restore");
        UNIT_ASSERT_VALUES_EQUAL_C(mgr.GetBarrier(tabletId).Lsn, 5u,
            "Barrier LSN must match the winning (higher-generation) record");
    }

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr::NDDisk
