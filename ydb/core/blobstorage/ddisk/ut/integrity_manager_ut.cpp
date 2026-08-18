#include <ydb/core/blobstorage/ddisk/integrity_manager.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/overloaded.h>

#include <algorithm>
#include <cstring>
#include <vector>

namespace NKikimr::NDDisk {

namespace {

using TKey = TIntegrityManager::TDataChunkKey;
using TWriteIo = TIntegrityManager::TWriteIo;
using TReadPlan = TIntegrityManager::TReadPlan;

constexpr ui64 TestDDiskId = 0xDD15C1D;
constexpr ui64 TestPDiskGuid = 0x9D15C6151D;

// Small geometries so tests don't need 128 MiB chunks:
//   1 MiB chunk  -> 256 data blocks, 1 TIntegrityBlock per extent (8 KiB pair on disk), 112 extents/chunk
//   160 KiB chunk -> 40 data blocks, 1 block per extent, 4 extents/chunk (fast slot exhaustion)
//   4 MiB chunk  -> 1024 data blocks, 3 blocks per extent (multi-digest coverage)
constexpr ui64 ProductionChunkSize = 128_MB;
constexpr ui64 SmallChunkSize = 1_MB;
constexpr ui64 TinyChunkSize = 160_KB;
constexpr ui64 MultiBlockChunkSize = 4_MB;

struct TActionLog {
    ui32 AllocateRequests = 0;
    std::vector<TWriteIo> Writes;
};

TActionLog Drain(TIntegrityManager& manager) {
    TActionLog log;
    for (auto& action : manager.TakeActions()) {
        std::visit(TOverloaded{
            [&](TIntegrityManager::TAllocateIntegrityChunk&) {
                ++log.AllocateRequests;
            },
            [&](TWriteIo& io) {
                log.Writes.push_back(std::move(io));
            },
        }, action);
    }
    return log;
}

std::vector<TKey> CompleteWrites(TIntegrityManager& manager, const std::vector<TWriteIo>& writes) {
    std::vector<TKey> readyKeys;
    for (const auto& io : writes) {
        for (const auto& key : manager.OnIoCompleted(io.IoId)) {
            readyKeys.push_back(key);
        }
    }
    return readyKeys;
}

// Drives the full allocation cycle for one data chunk, fulfilling chunk allocations from
// nextIntegrityChunkIdx, until the extent is Ready.
void MakeReady(TIntegrityManager& manager, TKey key, TChunkIdx dataChunkIdx, TChunkIdx* nextIntegrityChunkIdx) {
    manager.OnDataChunkAllocated(key, dataChunkIdx);
    while (!manager.IsExtentReady(key)) {
        TActionLog log = Drain(manager);
        for (ui32 i = 0; i < log.AllocateRequests; ++i) {
            manager.OnIntegrityChunkAllocated((*nextIntegrityChunkIdx)++);
        }
        CompleteWrites(manager, log.Writes);
        UNIT_ASSERT_C(log.AllocateRequests || !log.Writes.empty() || manager.IsExtentReady(key),
            "allocation is stuck");
    }
    Y_UNUSED(manager.TakePlacedKeys());
}

void CheckChunkHeader(const TWriteIo& io, TChunkIdx chunkIdx, ui64 generation) {
    UNIT_ASSERT_VALUES_EQUAL(io.ChunkIdx, chunkIdx);
    UNIT_ASSERT_VALUES_EQUAL(io.Data.size(), sizeof(TIntegrityChunkHeader));

    TIntegrityChunkHeader header;
    memcpy(&header, io.Data.data(), sizeof(header));
    UNIT_ASSERT_VALUES_EQUAL(header.Magic, MagicIntegrityChunkHeader);
    UNIT_ASSERT_VALUES_EQUAL(header.FormatVersion, static_cast<ui32>(EIntegrityFormatVersion::BaseAwupf4KiB));
    UNIT_ASSERT_VALUES_EQUAL(header.HeaderSize, sizeof(TIntegrityChunkHeader));
    UNIT_ASSERT_VALUES_EQUAL(header.DDiskId, TestDDiskId);
    UNIT_ASSERT_VALUES_EQUAL(header.PDiskGuid, TestPDiskGuid);
    UNIT_ASSERT_VALUES_EQUAL(header.IntegrityChunkId, chunkIdx);
    UNIT_ASSERT_VALUES_EQUAL(header.IntegrityChunkGeneration, generation);

    const ui64 checksum = std::exchange(header.HeaderChecksum, 0);
    UNIT_ASSERT_VALUES_EQUAL(checksum, CalculateRawChecksum(&header, sizeof(header)));
}

void SplitWrites(const std::vector<TWriteIo>& writes, std::vector<TWriteIo>* headers, std::vector<TWriteIo>* extents) {
    for (const auto& io : writes) {
        if (io.Data.size() == sizeof(TIntegrityChunkHeader)) {
            headers->push_back(io);
        } else {
            extents->push_back(io);
        }
    }
}

void CheckExtentFormat(const TIntegrityManager& manager, const TWriteIo& io, TKey key,
        const TIntegrityManager::TExtentRef& ref, ui64 integrityChunkGeneration) {
    UNIT_ASSERT_VALUES_EQUAL(io.ChunkIdx, ref.IntegrityChunkIdx);
    UNIT_ASSERT_VALUES_EQUAL(io.OffsetInBytes, manager.ExtentOffset(ref.ExtentSlot));
    UNIT_ASSERT_VALUES_EQUAL(io.Data.size(), manager.ExtentOnDiskSize());

    for (ui32 pair = 0; pair < manager.BlocksPerExtent(); ++pair) {
        for (ui32 slot = 0; slot < IntegrityPairSlots; ++slot) {
            TIntegrityBlock block;
            memcpy(&block, io.Data.data() + (pair * IntegrityPairSlots + slot) * sizeof(block), sizeof(block));

            const TIntegrityBlockHeader& header = block.Header;
            UNIT_ASSERT_VALUES_EQUAL(header.Magic, MagicIntegrityBlock);
            UNIT_ASSERT_VALUES_EQUAL(header.FormatVersion,
                static_cast<ui16>(EIntegrityFormatVersion::BaseAwupf4KiB));
            UNIT_ASSERT_VALUES_EQUAL(header.ChecksumBlockIdx, pair);
            UNIT_ASSERT_VALUES_EQUAL(header.OwnerId, key.TabletId);
            UNIT_ASSERT_VALUES_EQUAL(header.VChunkId, key.VChunkIndex);
            UNIT_ASSERT_VALUES_EQUAL(header.VChunkGeneration, ref.VChunkGeneration);
            UNIT_ASSERT_VALUES_EQUAL(header.IntegrityChunkId, ref.IntegrityChunkIdx);
            UNIT_ASSERT_VALUES_EQUAL(header.IntegrityExtentId, ref.ExtentSlot);
            UNIT_ASSERT_VALUES_EQUAL(header.IntegrityChunkGeneration, integrityChunkGeneration);
            UNIT_ASSERT_VALUES_EQUAL(header.PairSequenceNumber, slot); // slot B (seq 1) starts current
            UNIT_ASSERT_VALUES_EQUAL(header.IntegrityBlockDigest, 0);

            for (size_t i = 0; i < sizeof(header.UsedBlocksBitmap); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(header.UsedBlocksBitmap[i], 0);
            }
            for (ui32 i = 0; i < ChecksumsPerIntegrityBlock; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(block.Checksums[i], 0);
            }

            TIntegrityBlock copy = block;
            const ui64 checksum = std::exchange(copy.Header.BlockChecksum, 0);
            UNIT_ASSERT_VALUES_EQUAL(checksum, CalculateRawChecksum(&copy, sizeof(copy)));
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TIntegrityManagerTest) {

    Y_UNIT_TEST(Geometry) {
        {
            TIntegrityManager manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
            UNIT_ASSERT_VALUES_EQUAL(manager.DataBlocksInChunk(), 256);
            UNIT_ASSERT_VALUES_EQUAL(manager.BlocksPerExtent(), 1);
            UNIT_ASSERT_VALUES_EQUAL(manager.ExtentOnDiskSize(), 2 * IntegrityUnitSize);
            UNIT_ASSERT_VALUES_EQUAL(manager.ExtentsPerChunk(),
                (SmallChunkSize - IntegrityChunkHeaderRegionSize) / (2 * IntegrityUnitSize));
        }
        {
            // The runtime geometry for the default PDisk chunk size must match the RFC example.
            TIntegrityManager manager(ProductionChunkSize, TestDDiskId, TestPDiskGuid);
            UNIT_ASSERT_VALUES_EQUAL(manager.DataBlocksInChunk(), 32768);
            UNIT_ASSERT_VALUES_EQUAL(manager.BlocksPerExtent(), 67);
            UNIT_ASSERT_VALUES_EQUAL(manager.ExtentOnDiskSize(), 536_KB);
            UNIT_ASSERT_VALUES_EQUAL(manager.ExtentsPerChunk(), 244);
        }
    }

    Y_UNIT_TEST(FirstChunkAllocationAndFormatting) {
        TIntegrityManager manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 1, .VChunkIndex = 5};

        manager.OnDataChunkAllocated(key, 100);
        UNIT_ASSERT(!manager.IsExtentReady(key));
        UNIT_ASSERT(!manager.FindExtentRef(key));

        // First data chunk: exactly one integrity chunk allocation is requested, no writes yet.
        TActionLog log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.AllocateRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 0);

        // Chunk arrives: header replicas and the extent format write are issued in parallel, and
        // the extent is already placed (IntegrityChunk found) even though nothing is Ready yet.
        manager.OnIntegrityChunkAllocated(500);
        UNIT_ASSERT(manager.FindExtentRef(key));
        UNIT_ASSERT(!manager.IsExtentReady(key));
        {
            const auto placed = manager.TakePlacedKeys();
            UNIT_ASSERT_VALUES_EQUAL(placed.size(), 1);
            UNIT_ASSERT(placed[0] == key);
        }

        log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.AllocateRequests, 0);
        std::vector<TWriteIo> headers;
        std::vector<TWriteIo> extents;
        SplitWrites(log.Writes, &headers, &extents);
        UNIT_ASSERT_VALUES_EQUAL(headers.size(), TIntegrityManager::ChunkHeaderReplicaCount);
        UNIT_ASSERT_VALUES_EQUAL(extents.size(), 1);

        const ui64 chunkGeneration = manager.GetIntegrityChunkGeneration(500);
        UNIT_ASSERT_VALUES_EQUAL(chunkGeneration, 2); // the data chunk consumed generation 1

        std::vector<ui32> offsets;
        for (const auto& io : headers) {
            CheckChunkHeader(io, 500, chunkGeneration);
            offsets.push_back(io.OffsetInBytes);
            UNIT_ASSERT_VALUES_EQUAL(io.OffsetInBytes % IntegrityUnitSize, 0);
            UNIT_ASSERT(io.OffsetInBytes + io.Data.size() <= IntegrityChunkHeaderRegionSize);
        }
        std::sort(offsets.begin(), offsets.end());
        UNIT_ASSERT(std::unique(offsets.begin(), offsets.end()) == offsets.end());

        const auto* ref = manager.FindExtentRef(key);
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->IntegrityChunkIdx, 500);
        UNIT_ASSERT_VALUES_EQUAL(ref->ExtentSlot, 0);
        UNIT_ASSERT_VALUES_EQUAL(ref->VChunkGeneration, 1);
        CheckExtentFormat(manager, extents[0], key, *ref, chunkGeneration);

        // The extent write may finish before the parallel header writes. It must remain not Ready
        // through the first two header completions and become Ready only on the final replica.
        UNIT_ASSERT_VALUES_EQUAL(CompleteWrites(manager, extents).size(), 0);
        UNIT_ASSERT(!manager.IsExtentReady(key));

        for (ui32 i = 0; i + 1 < headers.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(manager.OnIoCompleted(headers[i].IoId).size(), 0);
            UNIT_ASSERT(!manager.IsExtentReady(key));
        }
        const auto readyKeys = manager.OnIoCompleted(headers.back().IoId);
        UNIT_ASSERT_VALUES_EQUAL(readyKeys.size(), 1);
        UNIT_ASSERT(readyKeys[0] == key);
        UNIT_ASSERT(manager.IsExtentReady(key));
        UNIT_ASSERT(!manager.HasActions());
    }

    Y_UNIT_TEST(SlotReuseAndExhaustion) {
        TIntegrityManager manager(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        UNIT_ASSERT_VALUES_EQUAL(manager.ExtentsPerChunk(), 4);

        TChunkIdx nextIntegrityChunkIdx = 700;

        // The first four data chunks fit into one integrity chunk, slots 0..3.
        for (ui32 i = 0; i < 4; ++i) {
            MakeReady(manager, TKey{1, i}, 100 + i, &nextIntegrityChunkIdx);
            const auto* ref = manager.FindExtentRef(TKey{1, i});
            UNIT_ASSERT(ref);
            UNIT_ASSERT_VALUES_EQUAL(ref->IntegrityChunkIdx, 700);
            UNIT_ASSERT_VALUES_EQUAL(ref->ExtentSlot, i);
        }
        UNIT_ASSERT_VALUES_EQUAL(nextIntegrityChunkIdx, 701); // exactly one chunk was allocated

        // Slot exhaustion: the fifth data chunk triggers a second integrity chunk.
        MakeReady(manager, TKey{1, 4}, 104, &nextIntegrityChunkIdx);
        UNIT_ASSERT_VALUES_EQUAL(nextIntegrityChunkIdx, 702);
        const auto* ref = manager.FindExtentRef(TKey{1, 4});
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->IntegrityChunkIdx, 701);
        UNIT_ASSERT_VALUES_EQUAL(ref->ExtentSlot, 0);
    }

    Y_UNIT_TEST(PendingDemandBatchesChunkAllocations) {
        TIntegrityManager manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk

        // Five data chunks allocated before any integrity chunk arrives: demand of 5 extents
        // must produce exactly two chunk allocation requests (4 + 1), not five.
        for (ui32 i = 0; i < 5; ++i) {
            manager.OnDataChunkAllocated(TKey{2, i}, 200 + i);
        }
        TActionLog log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.AllocateRequests, 2);
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 0);

        // Fulfill both; all five extents must eventually become ready.
        manager.OnIntegrityChunkAllocated(710);
        manager.OnIntegrityChunkAllocated(711);
        for (ui32 round = 0; round < 10 && manager.HasActions(); ++round) {
            CompleteWrites(manager, Drain(manager).Writes);
        }
        for (ui32 i = 0; i < 5; ++i) {
            UNIT_ASSERT(manager.IsExtentReady(TKey{2, i}));
        }
    }

    Y_UNIT_TEST(ReadPlans) {
        TIntegrityManager manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 3, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 720;
        MakeReady(manager, key, 300, &nextIntegrityChunkIdx);

        // Tracked, nothing written: all-zero without disk I/O.
        UNIT_ASSERT_EQUAL(manager.MakeReadPlan(key, 0, SmallChunkSize).Kind, TReadPlan::AllZero);

        // Blocks 2..3 written (no checksums).
        manager.OnBlocksWritten(key, 2 * IntegrityUnitSize, 2 * IntegrityUnitSize, {});

        // Whole written range: passthrough (no zeroing needed).
        UNIT_ASSERT_EQUAL(manager.MakeReadPlan(key, 2 * IntegrityUnitSize, 2 * IntegrityUnitSize).Kind,
            TReadPlan::Passthrough);

        // Untouched range: all-zero.
        UNIT_ASSERT_EQUAL(manager.MakeReadPlan(key, 0, 2 * IntegrityUnitSize).Kind, TReadPlan::AllZero);
        UNIT_ASSERT_EQUAL(manager.MakeReadPlan(key, 4 * IntegrityUnitSize, 8 * IntegrityUnitSize).Kind,
            TReadPlan::AllZero);

        // Partially written range: mixed with exact per-block bits.
        {
            const TReadPlan plan = manager.MakeReadPlan(key, 0, 6 * IntegrityUnitSize);
            UNIT_ASSERT_EQUAL(plan.Kind, TReadPlan::Mixed);
            for (ui32 i = 0; i < 6; ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(plan.UsedBlocks.Get(i), (i == 2 || i == 3), "block " << i);
            }
        }

        // Sub-block boundary within the used region: still passthrough.
        {
            const TReadPlan plan = manager.MakeReadPlan(key, 3 * IntegrityUnitSize, IntegrityUnitSize);
            UNIT_ASSERT_EQUAL(plan.Kind, TReadPlan::Passthrough);
        }

        // Unknown chunk: passthrough (safe fallback).
        UNIT_ASSERT_EQUAL(manager.MakeReadPlan(TKey{99, 99}, 0, IntegrityUnitSize).Kind, TReadPlan::Passthrough);
    }

    Y_UNIT_TEST(UnalignedWritesRoundOutward) {
        TIntegrityManager manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 4, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 730;
        MakeReady(manager, key, 400, &nextIntegrityChunkIdx);

        // A write of [2048, 6144) touches blocks 0 and 1: both must count as used.
        manager.OnBlocksWritten(key, 2048, 4096, {});
        const TReadPlan plan = manager.MakeReadPlan(key, 0, 3 * IntegrityUnitSize);
        UNIT_ASSERT_EQUAL(plan.Kind, TReadPlan::Mixed);
        UNIT_ASSERT(plan.UsedBlocks.Get(0));
        UNIT_ASSERT(plan.UsedBlocks.Get(1));
        UNIT_ASSERT(!plan.UsedBlocks.Get(2));
    }

    Y_UNIT_TEST(ChecksumsAndDigests) {
        TIntegrityManager manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        UNIT_ASSERT_VALUES_EQUAL(manager.BlocksPerExtent(), 3);

        const TKey key{.TabletId = 5, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 740;
        MakeReady(manager, key, 500, &nextIntegrityChunkIdx);
        const ui64 generation = manager.FindExtentRef(key)->VChunkGeneration;

        // Blocks 0..1 with checksums, plus one block in the second TIntegrityBlock.
        manager.OnBlocksWritten(key, 0, 2 * IntegrityUnitSize, {0xA, 0xB});
        const ui32 farBlock = ChecksumsPerIntegrityBlock; // first block of digest index 1
        manager.OnBlocksWritten(key, farBlock * IntegrityUnitSize, IntegrityUnitSize, {0xC});

        ui64 checksum = 0;
        UNIT_ASSERT(manager.GetBlockChecksum(key, 0, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xA);
        UNIT_ASSERT(manager.GetBlockChecksum(key, 1, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xB);
        UNIT_ASSERT(manager.GetBlockChecksum(key, farBlock, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xC);
        UNIT_ASSERT(!manager.GetBlockChecksum(key, 2, &checksum)); // never written

        // Digests match manual Contribution() accumulation per RFC.
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0),
            Contribution(generation, 0, 0xA) ^ Contribution(generation, 1, 0xB));
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 1),
            Contribution(generation, farBlock, 0xC));
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 2), 0);

        // Overwrite block 0: digest must be updated incrementally (UpdateRoot semantics).
        manager.OnBlocksWritten(key, 0, IntegrityUnitSize, {0xD});
        ui64 expected = Contribution(generation, 0, 0xA) ^ Contribution(generation, 1, 0xB);
        UpdateRoot(expected, generation, 0, 0xA, 0xD);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0), expected);
        UNIT_ASSERT_VALUES_EQUAL(expected, Contribution(generation, 0, 0xD) ^ Contribution(generation, 1, 0xB));

        // Overwrite block 1 without checksums: its contribution is retired, block stays used.
        manager.OnBlocksWritten(key, IntegrityUnitSize, IntegrityUnitSize, {});
        UNIT_ASSERT(!manager.GetBlockChecksum(key, 1, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0), Contribution(generation, 0, 0xD));
        UNIT_ASSERT_EQUAL(manager.MakeReadPlan(key, IntegrityUnitSize, IntegrityUnitSize).Kind,
            TReadPlan::Passthrough);
    }

    Y_UNIT_TEST(SparseBlockStateAllocation) {
        TIntegrityManager manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        UNIT_ASSERT_VALUES_EQUAL(manager.BlocksPerExtent(), 3);

        const TKey key{.TabletId = 9, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 780;
        MakeReady(manager, key, 800, &nextIntegrityChunkIdx);
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);

        // Checksum-less writes mark blocks used but allocate no checksum state.
        manager.OnBlocksWritten(key, 0, IntegrityUnitSize, {});
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);
        UNIT_ASSERT_EQUAL(manager.MakeReadPlan(key, 0, IntegrityUnitSize).Kind, TReadPlan::Passthrough);

        // A checksummed write allocates exactly one state, covering its whole TIntegrityBlock.
        manager.OnBlocksWritten(key, 0, IntegrityUnitSize, {0xA});
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 1);
        manager.OnBlocksWritten(key, IntegrityUnitSize, IntegrityUnitSize, {0xB});
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 1);

        // A write into the second TIntegrityBlock's range allocates the second state.
        manager.OnBlocksWritten(key, ChecksumsPerIntegrityBlock * IntegrityUnitSize, IntegrityUnitSize, {0xC});
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 2);
    }

    Y_UNIT_TEST(BlockStateLruEviction) {
        // Budget of exactly two cached states.
        TIntegrityManager manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid,
            2 * TIntegrityManager::BlockStateApproxBytes);
        UNIT_ASSERT_VALUES_EQUAL(manager.MaxCachedBlockStates(), 2);
        UNIT_ASSERT_VALUES_EQUAL(manager.BlocksPerExtent(), 3);

        const TKey key{.TabletId = 10, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 790;
        MakeReady(manager, key, 810, &nextIntegrityChunkIdx);
        const ui64 generation = manager.FindExtentRef(key)->VChunkGeneration;

        const ui32 block1 = ChecksumsPerIntegrityBlock;     // first block of TIntegrityBlock 1
        const ui32 block2 = 2 * ChecksumsPerIntegrityBlock; // first block of TIntegrityBlock 2

        // Fill all three TIntegrityBlocks: the oldest state (block 0's) is evicted.
        manager.OnBlocksWritten(key, 0, IntegrityUnitSize, {0xA});
        manager.OnBlocksWritten(key, block1 * IntegrityUnitSize, IntegrityUnitSize, {0xB});
        manager.OnBlocksWritten(key, block2 * IntegrityUnitSize, IntegrityUnitSize, {0xC});
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 2);

        // Evicted: checksum unknown and digest reset, together.
        ui64 checksum = 0;
        UNIT_ASSERT(!manager.GetBlockChecksum(key, 0, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0), 0);

        // The survivors are intact.
        UNIT_ASSERT(manager.GetBlockChecksum(key, block1, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xB);
        UNIT_ASSERT(manager.GetBlockChecksum(key, block2, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xC);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 1),
            Contribution(generation, block1, 0xB));

        // UsedBlocks are unaffected by eviction: reads of the written block still pass through.
        UNIT_ASSERT_EQUAL(manager.MakeReadPlan(key, 0, IntegrityUnitSize).Kind, TReadPlan::Passthrough);

        // Touching a state protects it: overwrite in TIntegrityBlock 1, then allocate a state in a
        // second data chunk - TIntegrityBlock 2's state (now the LRU) is the one evicted.
        manager.OnBlocksWritten(key, block1 * IntegrityUnitSize, IntegrityUnitSize, {0xD});
        const TKey key2{.TabletId = 10, .VChunkIndex = 1};
        MakeReady(manager, key2, 811, &nextIntegrityChunkIdx);
        manager.OnBlocksWritten(key2, 0, IntegrityUnitSize, {0xE});
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 2);

        UNIT_ASSERT(!manager.GetBlockChecksum(key, block2, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 2), 0);
        UNIT_ASSERT(manager.GetBlockChecksum(key, block1, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xD);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 1),
            Contribution(generation, block1, 0xD));
        UNIT_ASSERT(manager.GetBlockChecksum(key2, 0, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xE);
    }

    Y_UNIT_TEST(BlockStatesDroppedOnDelete) {
        TIntegrityManager manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 11, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 795;
        MakeReady(manager, key, 820, &nextIntegrityChunkIdx);

        manager.OnBlocksWritten(key, 0, IntegrityUnitSize, {0xA});
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 1);

        manager.PrepareTabletChunksDeletion(11);
        manager.CommitTabletChunksDeletion(11);
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);

        // Reallocation starts clean: no stale checksums or bitmap.
        manager.OnDataChunkAllocated(key, 821);
        CompleteWrites(manager, Drain(manager).Writes);
        UNIT_ASSERT(manager.IsExtentReady(key));
        UNIT_ASSERT_EQUAL(manager.MakeReadPlan(key, 0, IntegrityUnitSize).Kind, TReadPlan::AllZero);
        ui64 checksum = 0;
        UNIT_ASSERT(!manager.GetBlockChecksum(key, 0, &checksum));
    }

    Y_UNIT_TEST(DeleteAndReuse) {
        TIntegrityManager manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 7, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 750;

        MakeReady(manager, key, 600, &nextIntegrityChunkIdx);
        manager.OnBlocksWritten(key, 0, IntegrityUnitSize, {});
        {
            const auto* ref = manager.FindExtentRef(key);
            UNIT_ASSERT_VALUES_EQUAL(ref->ExtentSlot, 0);
            UNIT_ASSERT_VALUES_EQUAL(ref->VChunkGeneration, 1);
        }

        manager.PrepareTabletChunksDeletion(7);
        manager.CommitTabletChunksDeletion(7);
        UNIT_ASSERT(!manager.IsExtentReady(key));
        UNIT_ASSERT(!manager.FindExtentRef(key));
        // The chunk is now unknown to the manager: reads pass through.
        UNIT_ASSERT_EQUAL(manager.MakeReadPlan(key, 0, IntegrityUnitSize).Kind, TReadPlan::Passthrough);

        // Reallocation reuses the freed slot without a new integrity chunk and bumps the generation
        // (VChunk and integrity chunk generations share one counter: 1 = first VChunk allocation,
        // 2 = the integrity chunk, so the reallocation draws 3).
        manager.OnDataChunkAllocated(key, 601);
        TActionLog log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.AllocateRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 1); // extent format only, chunk header already written
        const auto* ref = manager.FindExtentRef(key);
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->IntegrityChunkIdx, 750);
        UNIT_ASSERT_VALUES_EQUAL(ref->ExtentSlot, 0);
        UNIT_ASSERT_VALUES_EQUAL(ref->VChunkGeneration, 3);
        CheckExtentFormat(manager, log.Writes[0], key, *ref, 2);

        const auto readyKeys = CompleteWrites(manager, log.Writes);
        UNIT_ASSERT_VALUES_EQUAL(readyKeys.size(), 1);
        UNIT_ASSERT(manager.IsExtentReady(key));
        // Old bitmap must not leak into the reallocated chunk.
        UNIT_ASSERT_EQUAL(manager.MakeReadPlan(key, 0, IntegrityUnitSize).Kind, TReadPlan::AllZero);
    }

    Y_UNIT_TEST(PreparedDeletionQuarantinesSlotsUntilCommit) {
        TIntegrityManager manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk
        TChunkIdx nextIntegrityChunkIdx = 755;
        for (ui32 i = 0; i < 4; ++i) {
            MakeReady(manager, TKey{40, i}, 650 + i, &nextIntegrityChunkIdx);
        }
        UNIT_ASSERT_VALUES_EQUAL(nextIntegrityChunkIdx, 756);

        manager.PrepareTabletChunksDeletion(40);

        // Prepared mappings disappear from the next durable snapshot, but all four physical
        // slots remain quarantined while that snapshot is in flight.
        UNIT_ASSERT_VALUES_EQUAL(manager.SnapshotMapping().Extents.size(), 0);
        UNIT_ASSERT(!manager.FindExtentRef(TKey{40, 0}));
        UNIT_ASSERT(manager.TakeReleasableIntegrityChunks().empty());

        const TKey pendingKey{.TabletId = 41, .VChunkIndex = 0};
        manager.OnDataChunkAllocated(pendingKey, 660);
        TActionLog pendingLog = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(pendingLog.Writes.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(pendingLog.AllocateRequests, 1);
        UNIT_ASSERT(!manager.FindExtentRef(pendingKey));

        // Only the durable commit releases the old slots. Reclamation gives slot 0 to the pending
        // extent before considering the integrity chunk for release.
        manager.CommitTabletChunksDeletion(40);
        UNIT_ASSERT(manager.TakeReleasableIntegrityChunks().empty());
        TActionLog formatLog = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(formatLog.Writes.size(), 1);
        const auto* ref = manager.FindExtentRef(pendingKey);
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->IntegrityChunkIdx, 755);
        UNIT_ASSERT_VALUES_EQUAL(ref->ExtentSlot, 0);
        UNIT_ASSERT_VALUES_EQUAL(ref->VChunkGeneration, 6);
        UNIT_ASSERT_VALUES_EQUAL(CompleteWrites(manager, formatLog.Writes).size(), 1);
        UNIT_ASSERT(manager.IsExtentReady(pendingKey));
    }

    Y_UNIT_TEST(PreparedDeletionWaitsForDurabilityAfterFormattingCompletes) {
        TIntegrityManager manager(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 42, .VChunkIndex = 0};

        manager.OnDataChunkAllocated(key, 670);
        Drain(manager);
        manager.OnIntegrityChunkAllocated(757);
        TActionLog log = Drain(manager);
        std::vector<TWriteIo> headers;
        std::vector<TWriteIo> formatLogWrites;
        SplitWrites(log.Writes, &headers, &formatLogWrites);
        UNIT_ASSERT_VALUES_EQUAL(headers.size(), TIntegrityManager::ChunkHeaderReplicaCount);
        UNIT_ASSERT_VALUES_EQUAL(formatLogWrites.size(), 1);
        CompleteWrites(manager, headers);

        manager.PrepareTabletChunksDeletion(42);
        // The physical write may finish first, but it must neither publish readiness nor release
        // its slot/chunk before the deletion snapshot is acknowledged.
        UNIT_ASSERT_VALUES_EQUAL(CompleteWrites(manager, formatLogWrites).size(), 0);
        UNIT_ASSERT(manager.TakeReleasableIntegrityChunks().empty());

        manager.CommitTabletChunksDeletion(42);
        const auto released = manager.TakeReleasableIntegrityChunks();
        UNIT_ASSERT_VALUES_EQUAL(released.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(released[0], 757);
    }

    Y_UNIT_TEST(DeleteWhileFormatInFlight) {
        TIntegrityManager manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 8, .VChunkIndex = 0};

        manager.OnDataChunkAllocated(key, 700);
        Drain(manager);
        manager.OnIntegrityChunkAllocated(760);
        TActionLog log = Drain(manager);
        std::vector<TWriteIo> headers;
        std::vector<TWriteIo> formatLog;
        SplitWrites(log.Writes, &headers, &formatLog);
        UNIT_ASSERT_VALUES_EQUAL(formatLog.size(), 1);
        CompleteWrites(manager, headers);

        // The tablet's chunks are deleted while the extent format write is in flight.
        manager.PrepareTabletChunksDeletion(8);
        manager.CommitTabletChunksDeletion(8);
        UNIT_ASSERT_VALUES_EQUAL(CompleteWrites(manager, formatLog).size(), 0);
        UNIT_ASSERT(!manager.IsExtentReady(key));

        // The slot is reusable afterwards.
        manager.OnDataChunkAllocated(key, 701);
        TActionLog reuseLog = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(reuseLog.AllocateRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(reuseLog.Writes.size(), 1);
        const auto readyKeys = CompleteWrites(manager, reuseLog.Writes);
        UNIT_ASSERT_VALUES_EQUAL(readyKeys.size(), 1);
        UNIT_ASSERT(manager.IsExtentReady(key));
        UNIT_ASSERT_VALUES_EQUAL(manager.FindExtentRef(key)->VChunkGeneration, 3);
    }

    Y_UNIT_TEST(StaleFormatCompletionDoesNotCompleteReusedExtent) {
        TIntegrityManager manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk
        const TKey key{.TabletId = 12, .VChunkIndex = 0};

        manager.OnDataChunkAllocated(key, 900);
        Drain(manager);
        manager.OnIntegrityChunkAllocated(765);
        TActionLog staleDrain = Drain(manager);
        std::vector<TWriteIo> staleHeaders;
        std::vector<TWriteIo> staleFormatWrites;
        SplitWrites(staleDrain.Writes, &staleHeaders, &staleFormatWrites);
        UNIT_ASSERT_VALUES_EQUAL(staleFormatWrites.size(), 1);
        CompleteWrites(manager, staleHeaders);

        // Free the extent while its format write is in flight and immediately reallocate the key.
        manager.PrepareTabletChunksDeletion(12);
        manager.CommitTabletChunksDeletion(12);
        manager.OnDataChunkAllocated(key, 901);
        TActionLog newFormat = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(newFormat.AllocateRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(newFormat.Writes.size(), 1);

        // Slot 0 is withheld until the stale write settles: the new extent gets slot 1.
        const auto* ref = manager.FindExtentRef(key);
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->ExtentSlot, 1);
        UNIT_ASSERT_VALUES_EQUAL(ref->VChunkGeneration, 3);

        // The stale completion must not mark the reallocated extent ready.
        UNIT_ASSERT_VALUES_EQUAL(CompleteWrites(manager, staleFormatWrites).size(), 0);
        UNIT_ASSERT(!manager.IsExtentReady(key));

        // Only the extent's own format write completes it.
        const auto readyKeys = CompleteWrites(manager, newFormat.Writes);
        UNIT_ASSERT_VALUES_EQUAL(readyKeys.size(), 1);
        UNIT_ASSERT(manager.IsExtentReady(key));

        // The stale write has settled, so slot 0 is reusable by the next allocation.
        const TKey key2{.TabletId = 12, .VChunkIndex = 1};
        manager.OnDataChunkAllocated(key2, 902);
        TActionLog log2 = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log2.AllocateRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(log2.Writes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(manager.FindExtentRef(key2)->ExtentSlot, 0);
        CompleteWrites(manager, log2.Writes);
        UNIT_ASSERT(manager.IsExtentReady(key2));
    }

    Y_UNIT_TEST(PendingExtentWaitsForOrphanedSlot) {
        // All four slots taken, one extent freed mid-format: a pending extent must wait for the
        // orphaned write to settle rather than reuse the slot early, and must then be assigned
        // to it (no new chunk needed).
        TIntegrityManager manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk
        TChunkIdx nextIntegrityChunkIdx = 768;
        for (ui32 i = 0; i < 3; ++i) {
            MakeReady(manager, TKey{13, i}, 910 + i, &nextIntegrityChunkIdx);
        }

        // The fourth extent (a different tablet) occupies slot 3 with its format write in flight.
        const TKey inFlightKey{.TabletId = 15, .VChunkIndex = 0};
        manager.OnDataChunkAllocated(inFlightKey, 913);
        TActionLog staleFormat = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(staleFormat.Writes.size(), 1);

        // Free it mid-format. A new allocation finds no free slot (slot 3 is withheld) and no
        // format write can be issued yet - but capacity accounting may request a chunk.
        manager.PrepareTabletChunksDeletion(15);
        manager.CommitTabletChunksDeletion(15);
        const TKey pendingKey{.TabletId = 14, .VChunkIndex = 0};
        manager.OnDataChunkAllocated(pendingKey, 920);
        TActionLog log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 0);
        UNIT_ASSERT(!manager.IsExtentReady(pendingKey));

        // The orphaned write settles: slot 3 is released and the pending extent takes it.
        CompleteWrites(manager, staleFormat.Writes);
        TActionLog formatLog = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(formatLog.Writes.size(), 1);
        const auto* ref = manager.FindExtentRef(pendingKey);
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->ExtentSlot, 3);
        CompleteWrites(manager, formatLog.Writes);
        UNIT_ASSERT(manager.IsExtentReady(pendingKey));
    }

    Y_UNIT_TEST(SnapshotRoundTrip) {
        TIntegrityManager manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk
        TChunkIdx nextIntegrityChunkIdx = 770;

        // Six extents across two tablets -> two integrity chunks.
        std::vector<TKey> keys;
        for (ui32 i = 0; i < 3; ++i) {
            keys.push_back(TKey{10, i});
            keys.push_back(TKey{11, i});
        }
        for (ui32 i = 0; i < keys.size(); ++i) {
            MakeReady(manager, keys[i], 800 + i, &nextIntegrityChunkIdx);
        }
        UNIT_ASSERT_VALUES_EQUAL(nextIntegrityChunkIdx, 772);

        const auto snapshot = manager.SnapshotMapping();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.IntegrityChunks.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Extents.size(), keys.size());
        // Six VChunk generations and two integrity chunk generations were drawn from the shared
        // counter, so the persisted watermark is 8.
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GenerationCounter, 8);

        // Apply to a fresh manager: same refs, all ready, no actions.
        TIntegrityManager restored(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        restored.ApplyMappingSnapshot(snapshot);
        UNIT_ASSERT(!restored.HasActions());
        for (const auto& key : keys) {
            UNIT_ASSERT(restored.IsExtentReady(key));
            const auto* origRef = manager.FindExtentRef(key);
            const auto* restoredRef = restored.FindExtentRef(key);
            UNIT_ASSERT(origRef && restoredRef);
            UNIT_ASSERT_VALUES_EQUAL(restoredRef->IntegrityChunkIdx, origRef->IntegrityChunkIdx);
            UNIT_ASSERT_VALUES_EQUAL(restoredRef->ExtentSlot, origRef->ExtentSlot);
            UNIT_ASSERT_VALUES_EQUAL(restoredRef->VChunkGeneration, origRef->VChunkGeneration);
        }
        UNIT_ASSERT_VALUES_EQUAL(restored.GetIntegrityChunkGeneration(770), 2);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetIntegrityChunkGeneration(771), 7);
        UNIT_ASSERT_VALUES_EQUAL(restored.GetGenerationCounter(), 8);

        // Restored extents have unknown bitmaps: reads pass through unchanged even though nothing
        // was written since the restore (the previous incarnation's bitmap is lost); new writes
        // are tracked again (checksums recorded) but do not change the read plan.
        UNIT_ASSERT_EQUAL(restored.MakeReadPlan(keys[0], 0, TinyChunkSize).Kind, TReadPlan::Passthrough);
        restored.OnBlocksWritten(keys[0], 0, IntegrityUnitSize, {0xA});
        ui64 checksum = 0;
        UNIT_ASSERT(restored.GetBlockChecksum(keys[0], 0, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xA);
        UNIT_ASSERT_EQUAL(restored.MakeReadPlan(keys[0], 0, TinyChunkSize).Kind, TReadPlan::Passthrough);

        // The restored manager keeps allocating into the remaining free slots of known chunks
        // without requesting new integrity chunks (6 used out of 8 -> 2 slots left).
        for (ui32 i = 0; i < 2; ++i) {
            const TKey key{12, i};
            restored.OnDataChunkAllocated(key, 900 + i);
            TActionLog log = Drain(restored);
            UNIT_ASSERT_VALUES_EQUAL(log.AllocateRequests, 0);
            UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 1);
            CompleteWrites(restored, log.Writes);
            UNIT_ASSERT(restored.IsExtentReady(key));
        }
        // And the ninth extent overflows into a new chunk.
        restored.OnDataChunkAllocated(TKey{12, 2}, 902);
        UNIT_ASSERT_VALUES_EQUAL(Drain(restored).AllocateRequests, 1);

        // Deleting a restored tablet and reallocating bumps VChunkGeneration past the persisted
        // watermark (generations 9..11 were drawn by tablet 12 above).
        restored.PrepareTabletChunksDeletion(10);
        restored.CommitTabletChunksDeletion(10);
        restored.OnDataChunkAllocated(TKey{10, 0}, 950);
        Drain(restored);
        const auto* ref = restored.FindExtentRef(TKey{10, 0});
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->VChunkGeneration, 12);
        UNIT_ASSERT(ref->VChunkGeneration > snapshot.GenerationCounter);
    }

    Y_UNIT_TEST(SnapshotExcludesFormattingChunks) {
        TIntegrityManager manager(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 16, .VChunkIndex = 0};

        manager.OnDataChunkAllocated(key, 960);
        UNIT_ASSERT_VALUES_EQUAL(Drain(manager).AllocateRequests, 1);
        manager.OnIntegrityChunkAllocated(775);
        const TActionLog formatting = Drain(manager);

        // ApplyMappingSnapshot restores every listed chunk as Ready, so a chunk whose headers are
        // still in flight must not be exported.
        const auto inFlightSnapshot = manager.SnapshotMapping();
        UNIT_ASSERT_VALUES_EQUAL(inFlightSnapshot.IntegrityChunks.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(inFlightSnapshot.Extents.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(inFlightSnapshot.GenerationCounter, 2);

        const auto readyKeys = CompleteWrites(manager, formatting.Writes);
        UNIT_ASSERT_VALUES_EQUAL(readyKeys.size(), 1);
        const auto readySnapshot = manager.SnapshotMapping();
        UNIT_ASSERT_VALUES_EQUAL(readySnapshot.IntegrityChunks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(readySnapshot.IntegrityChunks[0].ChunkIdx, 775);
        UNIT_ASSERT_VALUES_EQUAL(readySnapshot.Extents.size(), 1);
    }

    Y_UNIT_TEST(ReleasableIntegrityChunks) {
        TIntegrityManager manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk
        TChunkIdx nextIntegrityChunkIdx = 850;

        // Five extents -> two chunks (850 full, 851 holds one).
        for (ui32 i = 0; i < 5; ++i) {
            MakeReady(manager, TKey{30, i}, 1000 + i, &nextIntegrityChunkIdx);
        }
        UNIT_ASSERT_VALUES_EQUAL(nextIntegrityChunkIdx, 852);

        // Nothing is releasable while extents are in place.
        UNIT_ASSERT(manager.TakeReleasableIntegrityChunks().empty());

        // Delete the tablet: both chunks become fully free and are handed back for deallocation.
        manager.PrepareTabletChunksDeletion(30);
        manager.CommitTabletChunksDeletion(30);
        auto released = manager.TakeReleasableIntegrityChunks();
        std::sort(released.begin(), released.end());
        UNIT_ASSERT_VALUES_EQUAL(released.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(released[0], 850);
        UNIT_ASSERT_VALUES_EQUAL(released[1], 851);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityChunkGeneration(850), 0); // forgotten
        UNIT_ASSERT(!manager.HasActions());

        // The next allocation requests a fresh integrity chunk again.
        manager.OnDataChunkAllocated(TKey{30, 0}, 1010);
        UNIT_ASSERT_VALUES_EQUAL(Drain(manager).AllocateRequests, 1);
    }

    Y_UNIT_TEST(ReleasableChunksServePendingExtentsFirst) {
        TIntegrityManager manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk
        TChunkIdx nextIntegrityChunkIdx = 860;
        for (ui32 i = 0; i < 4; ++i) {
            MakeReady(manager, TKey{31, i}, 1100 + i, &nextIntegrityChunkIdx);
        }

        // A fifth extent (another tablet) goes pending: all slots taken, a chunk allocation is
        // queued - and genuinely needed, so it must not be cancellable.
        manager.OnDataChunkAllocated(TKey{32, 0}, 1104);
        TActionLog log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.AllocateRequests, 1);
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 0);
        UNIT_ASSERT(!manager.CancelChunkAllocationIfExcess());

        // Deleting the first tablet frees all four slots, but the pending extent takes one before
        // releasability is decided: the chunk stays owned and the extent's format write goes out.
        manager.PrepareTabletChunksDeletion(31);
        manager.CommitTabletChunksDeletion(31);
        UNIT_ASSERT(manager.TakeReleasableIntegrityChunks().empty());
        log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 1);
        UNIT_ASSERT(manager.FindExtentRef(TKey{32, 0}));

        // With three slots left free and no pending extents, the queued allocation is now excess.
        UNIT_ASSERT(manager.CancelChunkAllocationIfExcess());

        CompleteWrites(manager, log.Writes);
        UNIT_ASSERT(manager.IsExtentReady(TKey{32, 0}));
    }

    Y_UNIT_TEST(OrphanedFormatWriteBlocksChunkRelease) {
        TIntegrityManager manager(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 33, .VChunkIndex = 0};

        // Bring one chunk to Ready with the extent's format write still in flight.
        manager.OnDataChunkAllocated(key, 1200);
        Drain(manager);
        manager.OnIntegrityChunkAllocated(870);
        TActionLog formatDrain = Drain(manager);
        std::vector<TWriteIo> headers;
        std::vector<TWriteIo> formatLogWrites;
        SplitWrites(formatDrain.Writes, &headers, &formatLogWrites);
        UNIT_ASSERT_VALUES_EQUAL(formatLogWrites.size(), 1);
        CompleteWrites(manager, headers);

        // Delete while the format write is in flight: its slot is withheld, so the chunk must not
        // be released - the write could still land on it after PDisk reassigns the chunk.
        manager.PrepareTabletChunksDeletion(33);
        manager.CommitTabletChunksDeletion(33);
        UNIT_ASSERT(manager.TakeReleasableIntegrityChunks().empty());

        // Once the orphaned write settles the chunk is fully free and releasable.
        CompleteWrites(manager, formatLogWrites);
        const auto released = manager.TakeReleasableIntegrityChunks();
        UNIT_ASSERT_VALUES_EQUAL(released.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(released[0], 870);
    }

    Y_UNIT_TEST(FormattingChunkNotReleasable) {
        TIntegrityManager manager(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 34, .VChunkIndex = 0};

        manager.OnDataChunkAllocated(key, 1300);
        Drain(manager);
        manager.OnIntegrityChunkAllocated(880);
        TActionLog headerLog = Drain(manager);
        UNIT_ASSERT(headerLog.Writes.size() >= 1);

        // The extent is freed while the chunk is still writing its headers (and possibly the
        // extent format): the chunk is not releasable until every in-flight write settles.
        manager.PrepareTabletChunksDeletion(34);
        manager.CommitTabletChunksDeletion(34);
        UNIT_ASSERT(manager.TakeReleasableIntegrityChunks().empty());

        CompleteWrites(manager, headerLog.Writes);
        while (manager.HasActions()) {
            CompleteWrites(manager, Drain(manager).Writes);
        }
        const auto released = manager.TakeReleasableIntegrityChunks();
        UNIT_ASSERT_VALUES_EQUAL(released.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(released[0], 880);
    }

    Y_UNIT_TEST(RestoredChunksAreReadyAndHostNewExtents) {
        TIntegrityManager manager(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 35, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 890;
        MakeReady(manager, key, 1400, &nextIntegrityChunkIdx);

        const auto snapshot = manager.SnapshotMapping();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.IntegrityChunks.size(), 1);

        TIntegrityManager restored(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        restored.ApplyMappingSnapshot(snapshot);
        UNIT_ASSERT(!restored.HasActions());
        UNIT_ASSERT(restored.IsExtentReady(key));
        UNIT_ASSERT(restored.IsIntegrityChunkFormatted(890));

        const TKey key2{.TabletId = 35, .VChunkIndex = 1};
        restored.OnDataChunkAllocated(key2, 1401);
        UNIT_ASSERT(restored.FindExtentRef(key2));
        TActionLog log = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(log.AllocateRequests, 0);
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 1);
        CompleteWrites(restored, log.Writes);
        UNIT_ASSERT(restored.IsExtentReady(key2));
    }

    Y_UNIT_TEST(GenerationWatermarkSurvivesRestart) {
        TIntegrityManager manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 36, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 895;
        MakeReady(manager, key, 1500, &nextIntegrityChunkIdx);
        UNIT_ASSERT_VALUES_EQUAL(manager.FindExtentRef(key)->VChunkGeneration, 1);

        // Delete the tablet: the extent vanishes from the mapping, so after a restart only the
        // watermark can prevent the generation from being reused.
        manager.PrepareTabletChunksDeletion(36);
        manager.CommitTabletChunksDeletion(36);
        const auto snapshot = manager.SnapshotMapping();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Extents.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GenerationCounter, 2);

        TIntegrityManager restored(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        restored.ApplyMappingSnapshot(snapshot);

        // Reallocating the same key must draw a fresh generation, not reuse 1.
        restored.OnDataChunkAllocated(key, 1501);
        TActionLog log = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(log.AllocateRequests, 0); // the restored chunk has free slots
        const auto* ref = restored.FindExtentRef(key);
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->VChunkGeneration, 3);
    }

} // Y_UNIT_TEST_SUITE(TIntegrityManagerTest)

} // namespace NKikimr::NDDisk
