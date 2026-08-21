#include <ydb/core/blobstorage/ddisk/integrity_manager.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/overloaded.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <vector>

namespace NKikimr::NDDisk {

namespace {

using TKey = TIntegrityManager::TDataChunkKey;
using TWriteIo = TIntegrityManager::TWriteIo;
using TReadIo = TIntegrityManager::TReadIo;
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
constexpr ui64 PairBoundaryChunkSize = (ChecksumsPerIntegrityBlock + 1) * IntegrityUnitSize;

struct TActionLog {
    ui32 AllocateRequests = 0;
    std::vector<TWriteIo> Writes;
    std::vector<TReadIo> Reads;
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
            [&](TReadIo& io) {
                log.Reads.push_back(std::move(io));
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

TIntegrityBlock MakeIntegrityBlock(TKey key, const TIntegrityManager::TExtentRef& ref,
        ui64 integrityChunkGeneration, ui32 pairIdx, ui64 sequence,
        const std::vector<std::pair<ui32, ui64>>& pureChecksums) {
    TIntegrityBlock block{};
    auto& header = block.Header;
    header.Magic = MagicIntegrityBlock;
    header.FormatVersion = static_cast<ui16>(EIntegrityFormatVersion::BaseAwupf4KiB);
    header.ChecksumBlockIdx = pairIdx;
    header.OwnerId = key.TabletId;
    header.VChunkId = key.VChunkIndex;
    header.VChunkGeneration = ref.VChunkGeneration;
    header.IntegrityChunkId = ref.IntegrityChunkIdx;
    header.IntegrityExtentId = ref.ExtentSlot;
    header.IntegrityChunkGeneration = integrityChunkGeneration;
    header.PairSequenceNumber = sequence;
    for (const auto& [slot, pureChecksum] : pureChecksums) {
        UNIT_ASSERT(slot < ChecksumsPerIntegrityBlock);
        const ui32 blockIdx = pairIdx * ChecksumsPerIntegrityBlock + slot;
        header.UsedBlocksBitmap[slot / 8] |= ui8(1u << (slot % 8));
        block.Checksums[slot] = SealBlockChecksum(pureChecksum, TestDDiskId, TestPDiskGuid,
            key.TabletId, key.VChunkIndex, blockIdx);
        header.IntegrityBlockDigest ^= Contribution(ref.VChunkGeneration, blockIdx, pureChecksum);
    }
    header.BlockChecksum = CalculateRawChecksum(&block, sizeof(block));
    return block;
}

TRope MakeIntegrityPair(TIntegrityBlock a, TIntegrityBlock b) {
    auto data = TRcBuf::UninitializedPageAligned(IntegrityPairSlots * sizeof(TIntegrityBlock));
    memcpy(data.GetDataMut(), &a, sizeof(a));
    memcpy(data.GetDataMut() + sizeof(a), &b, sizeof(b));
    return TRope(std::move(data));
}

void RecalculateBlockChecksum(TIntegrityBlock& block) {
    block.Header.BlockChecksum = 0;
    block.Header.BlockChecksum = CalculateRawChecksum(&block, sizeof(block));
}

TRope MakeFragmentedRope(const TString& data, const std::vector<size_t>& fragmentSizes) {
    TRope rope;
    size_t offset = 0;
    for (const size_t size : fragmentSizes) {
        UNIT_ASSERT(size > 0);
        UNIT_ASSERT(offset + size <= data.size());
        rope.Insert(rope.End(), TRope(TString(data.data() + offset, size)));
        offset += size;
    }
    UNIT_ASSERT_VALUES_EQUAL(offset, data.size());
    return rope;
}

TIntegrityManager::TOperationResult TakeOnlyCompletion(TIntegrityManager& manager) {
    auto completed = manager.TakeCompletedOperations();
    UNIT_ASSERT_VALUES_EQUAL(completed.size(), 1);
    return std::move(completed.front());
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

    Y_UNIT_TEST(ContiguousAndFragmentedRopeChecksums) {
        TString payload = TString::Uninitialized(3 * IntegrityUnitSize);
        char* payloadData = payload.Detach();
        for (size_t i = 0; i < payload.size(); ++i) {
            payloadData[i] = static_cast<char>((i * 37 + i / IntegrityUnitSize * 53) & 0xff);
        }

        std::vector<ui64> expected;
        for (ui32 block = 0; block < 3; ++block) {
            expected.push_back(CalculateRawChecksum(
                payload.data() + block * IntegrityUnitSize, IntegrityUnitSize));
        }
        UNIT_ASSERT_UNEQUAL(expected[0], expected[1]);
        UNIT_ASSERT_UNEQUAL(expected[1], expected[2]);

        const TRope contiguous(payload);
        UNIT_ASSERT_VALUES_EQUAL(CalculatePayloadChecksums(contiguous), expected);
        UNIT_ASSERT_VALUES_EQUAL(
            CalculateBlockChecksum(contiguous.Begin(), payload.size()),
            CalculateRawChecksum(payload.data(), payload.size()));

        const std::array<std::vector<size_t>, 2> layouts{{
            {17, IntegrityUnitSize - 17, 101, IntegrityUnitSize - 101,
                IntegrityUnitSize - 1, 1},
            {1024, 2048, 3072, 4096, 2048},
        }};
        for (const auto& layout : layouts) {
            const TRope fragmented = MakeFragmentedRope(payload, layout);
            UNIT_ASSERT(fragmented.Begin().ContiguousSize() < IntegrityUnitSize);
            UNIT_ASSERT_VALUES_EQUAL(CalculatePayloadChecksums(fragmented), expected);
            UNIT_ASSERT_VALUES_EQUAL(
                CalculateBlockChecksum(fragmented.Begin(), payload.size()),
                CalculateRawChecksum(payload.data(), payload.size()));
        }
    }

    Y_UNIT_TEST(ZeroBlockChecksumProperties) {
        const ui64 zeroChecksum = GetZeroBlockChecksum();
        UNIT_ASSERT_UNEQUAL(zeroChecksum, 0);

        for (const ui32 blocks : {1u, 3u}) {
            TString zeros = TString::Uninitialized(blocks * IntegrityUnitSize);
            memset(zeros.Detach(), 0, zeros.size());
            const TRope rope(zeros);
            const auto checksums = CalculatePayloadChecksums(rope);
            UNIT_ASSERT_VALUES_EQUAL(checksums.size(), blocks);
            for (const ui64 checksum : checksums) {
                UNIT_ASSERT_VALUES_EQUAL(checksum, zeroChecksum);
            }
            UNIT_ASSERT_VALUES_EQUAL(
                CalculateBlockChecksum(rope.Begin(), IntegrityUnitSize), zeroChecksum);
        }

        std::array<ui8, IntegrityUnitSize> zero{};
        UNIT_ASSERT_VALUES_EQUAL(zeroChecksum, CalculateRawChecksum(zero.data(), zero.size()));
        zero.back() = 1;
        UNIT_ASSERT_UNEQUAL(zeroChecksum, CalculateRawChecksum(zero.data(), zero.size()));
    }

    Y_UNIT_TEST(ChecksumSealIdentitySeparation) {
        struct TIdentity {
            ui64 DDiskId;
            ui64 PDiskGuid;
            ui64 TabletId;
            ui64 VChunkIndex;
            ui64 BlockIdx;
        };

        const TIdentity baseline{TestDDiskId, TestPDiskGuid, 77, 9, 12};
        const std::array<TIdentity, 5> changed{{
            {TestDDiskId + 1, TestPDiskGuid, 77, 9, 12},
            {TestDDiskId, TestPDiskGuid + 1, 77, 9, 12},
            {TestDDiskId, TestPDiskGuid, 78, 9, 12},
            {TestDDiskId, TestPDiskGuid, 77, 10, 12},
            {TestDDiskId, TestPDiskGuid, 77, 9, 13},
        }};

        auto salt = [](const TIdentity& identity) {
            return CalculateChecksumIdentitySalt(identity.DDiskId, identity.PDiskGuid,
                identity.TabletId, identity.VChunkIndex, identity.BlockIdx);
        };
        auto seal = [](ui64 checksum, const TIdentity& identity) {
            return SealBlockChecksum(checksum, identity.DDiskId, identity.PDiskGuid,
                identity.TabletId, identity.VChunkIndex, identity.BlockIdx);
        };
        auto unseal = [](ui64 checksum, const TIdentity& identity) {
            return UnsealBlockChecksum(checksum, identity.DDiskId, identity.PDiskGuid,
                identity.TabletId, identity.VChunkIndex, identity.BlockIdx);
        };

        const ui64 pure = 0x123456789abcdef0ull;
        const ui64 sealed = seal(pure, baseline);
        UNIT_ASSERT_VALUES_EQUAL(unseal(sealed, baseline), pure);
        for (const TIdentity& identity : changed) {
            UNIT_ASSERT_UNEQUAL(salt(identity), salt(baseline));
            UNIT_ASSERT_UNEQUAL(seal(pure, identity), sealed);
            UNIT_ASSERT_UNEQUAL(unseal(sealed, identity), pure);
        }
    }

    Y_UNIT_TEST(IntegrityBlockValidationRejectsCorruption) {
        const TKey key{.TabletId = 77, .VChunkIndex = 9};
        const TIntegrityManager::TExtentRef ref{
            .IntegrityChunkIdx = 700,
            .ExtentSlot = 3,
            .VChunkGeneration = 5,
        };
        const ui64 chunkGeneration = 11;
        const TIntegrityBlockIdentity expected{
            .OwnerId = key.TabletId,
            .VChunkId = key.VChunkIndex,
            .VChunkGeneration = ref.VChunkGeneration,
            .IntegrityChunkId = ref.IntegrityChunkIdx,
            .IntegrityExtentId = ref.ExtentSlot,
            .IntegrityChunkGeneration = chunkGeneration,
            .ChecksumBlockIdx = 0,
        };

        const ui64 pure = 0x123456789abcdef0ull;
        const TIntegrityBlock valid =
            MakeIntegrityBlock(key, ref, chunkGeneration, 0, 2, {{0, pure}});
        UNIT_ASSERT(ValidateIntegrityBlock(valid, expected));

        struct TCorruption {
            const char* Name;
            size_t Offset;
            bool RecalculateSelfChecksum;
        };
        const std::array<TCorruption, 11> corruptions{{
            {"magic", offsetof(TIntegrityBlockHeader, Magic), true},
            {"format version", offsetof(TIntegrityBlockHeader, FormatVersion), true},
            {"checksum block index", offsetof(TIntegrityBlockHeader, ChecksumBlockIdx), true},
            {"owner", offsetof(TIntegrityBlockHeader, OwnerId), true},
            {"vchunk", offsetof(TIntegrityBlockHeader, VChunkId), true},
            {"vchunk generation", offsetof(TIntegrityBlockHeader, VChunkGeneration), true},
            {"integrity chunk", offsetof(TIntegrityBlockHeader, IntegrityChunkId), true},
            {"integrity extent", offsetof(TIntegrityBlockHeader, IntegrityExtentId), true},
            {"integrity chunk generation",
                offsetof(TIntegrityBlockHeader, IntegrityChunkGeneration), true},
            {"self checksum", offsetof(TIntegrityBlockHeader, BlockChecksum), false},
            {"checksummed payload", offsetof(TIntegrityBlock, Checksums), false},
        }};
        for (const TCorruption& corruption : corruptions) {
            TIntegrityBlock damaged = valid;
            reinterpret_cast<ui8*>(&damaged)[corruption.Offset] ^= 1;
            if (corruption.RecalculateSelfChecksum) {
                RecalculateBlockChecksum(damaged);
            }
            UNIT_ASSERT_C(!ValidateIntegrityBlock(damaged, expected), corruption.Name);
        }
    }

    Y_UNIT_TEST(IntegrityBlockWinnerSelection) {
        const TKey key{.TabletId = 77, .VChunkIndex = 9};
        const TIntegrityManager::TExtentRef ref{
            .IntegrityChunkIdx = 700,
            .ExtentSlot = 3,
            .VChunkGeneration = 5,
        };
        const ui64 chunkGeneration = 11;
        const TIntegrityBlockIdentity expected{
            .OwnerId = key.TabletId,
            .VChunkId = key.VChunkIndex,
            .VChunkGeneration = ref.VChunkGeneration,
            .IntegrityChunkId = ref.IntegrityChunkIdx,
            .IntegrityExtentId = ref.ExtentSlot,
            .IntegrityChunkGeneration = chunkGeneration,
            .ChecksumBlockIdx = 0,
        };

        struct TCase {
            bool ValidA;
            ui64 SequenceA;
            bool ValidB;
            ui64 SequenceB;
            i32 ExpectedWinner;
        };
        const std::array<TCase, 6> cases{{
            {false, 2, false, 3, -1},
            {true, 2, false, 3, 0},
            {false, 2, true, 3, 1},
            {true, 4, true, 3, 0},
            {true, 2, true, 3, 1},
            {true, 3, true, 3, 1},
        }};
        for (const TCase& test : cases) {
            TIntegrityBlock slots[IntegrityPairSlots]{
                MakeIntegrityBlock(key, ref, chunkGeneration, 0, test.SequenceA, {{0, 0xAA}}),
                MakeIntegrityBlock(key, ref, chunkGeneration, 0, test.SequenceB, {{0, 0xBB}}),
            };
            if (!test.ValidA) {
                ++slots[0].Checksums[0];
            }
            if (!test.ValidB) {
                ++slots[1].Checksums[0];
            }
            UNIT_ASSERT_VALUES_EQUAL(SelectIntegrityBlockWinner(slots, expected), test.ExpectedWinner);
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

        // Blocks 2..3 written with mandatory checksums.
        manager.BeginBlocksWrite(key, 2 * IntegrityUnitSize, 2 * IntegrityUnitSize, {0xA, 0xB});

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

    Y_UNIT_TEST(AlignedWritesMarkExactBlocks) {
        TIntegrityManager manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 4, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 730;
        MakeReady(manager, key, 400, &nextIntegrityChunkIdx);

        manager.BeginBlocksWrite(key, 0, 2 * IntegrityUnitSize, {0xA, 0xB});
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
        manager.BeginBlocksWrite(key, 0, 2 * IntegrityUnitSize, {0xA, 0xB});
        const ui32 farBlock = ChecksumsPerIntegrityBlock; // first block of digest index 1
        manager.BeginBlocksWrite(key, farBlock * IntegrityUnitSize, IntegrityUnitSize, {0xC});

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
        manager.BeginBlocksWrite(key, 0, IntegrityUnitSize, {0xD});
        ui64 expected = Contribution(generation, 0, 0xA) ^ Contribution(generation, 1, 0xB);
        UpdateRoot(expected, generation, 0, 0xA, 0xD);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0), expected);
        UNIT_ASSERT_VALUES_EQUAL(expected, Contribution(generation, 0, 0xD) ^ Contribution(generation, 1, 0xB));

        // Overwrite block 1 with its mandatory checksum.
        manager.BeginBlocksWrite(key, IntegrityUnitSize, IntegrityUnitSize, {0xE});
        UNIT_ASSERT(manager.GetBlockChecksum(key, 1, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xE);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0),
            Contribution(generation, 0, 0xD) ^ Contribution(generation, 1, 0xE));
        UNIT_ASSERT_EQUAL(manager.MakeReadPlan(key, IntegrityUnitSize, IntegrityUnitSize).Kind,
            TReadPlan::Passthrough);
    }

    Y_UNIT_TEST(ExactIntegrityPairBoundaryAndBitmapTail) {
        TIntegrityManager manager(PairBoundaryChunkSize, TestDDiskId, TestPDiskGuid);
        UNIT_ASSERT_VALUES_EQUAL(manager.DataBlocksInChunk(), ChecksumsPerIntegrityBlock + 1);
        UNIT_ASSERT_VALUES_EQUAL(manager.BlocksPerExtent(), 2);

        const TKey key{.TabletId = 6, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 745;
        MakeReady(manager, key, 550, &nextIntegrityChunkIdx);

        std::vector<ui64> firstPairChecksums(ChecksumsPerIntegrityBlock);
        for (ui32 i = 0; i < firstPairChecksums.size(); ++i) {
            firstPairChecksums[i] = 0x10000 + i;
        }
        const ui64 firstOperation = manager.BeginBlocksWrite(
            key, 0, ChecksumsPerIntegrityBlock * IntegrityUnitSize, firstPairChecksums);
        TActionLog firstWrite = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(firstWrite.Writes.size(), 1);
        TIntegrityBlock firstImage;
        memcpy(&firstImage, firstWrite.Writes[0].Data.data(), sizeof(firstImage));
        UNIT_ASSERT_VALUES_EQUAL(firstImage.Header.ChecksumBlockIdx, 0);
        UNIT_ASSERT_VALUES_EQUAL(firstImage.Header.UsedBlocksBitmap[61], 0x0f);
        manager.OnIoCompleted(firstWrite.Writes[0].IoId);
        UNIT_ASSERT_VALUES_EQUAL(TakeOnlyCompletion(manager).OperationId, firstOperation);

        const ui64 finalChecksum = 0x20000;
        const ui64 finalOperation = manager.BeginBlocksWrite(
            key, ChecksumsPerIntegrityBlock * IntegrityUnitSize, IntegrityUnitSize, {finalChecksum});
        TActionLog finalWrite = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(finalWrite.Writes.size(), 1);
        TIntegrityBlock finalImage;
        memcpy(&finalImage, finalWrite.Writes[0].Data.data(), sizeof(finalImage));
        UNIT_ASSERT_VALUES_EQUAL(finalImage.Header.ChecksumBlockIdx, 1);
        UNIT_ASSERT_VALUES_EQUAL(finalImage.Header.UsedBlocksBitmap[0], 1);
        for (size_t i = 1; i < sizeof(finalImage.Header.UsedBlocksBitmap); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(finalImage.Header.UsedBlocksBitmap[i], 0, "bitmap byte " << i);
        }
        for (ui32 slot = 1; slot < ChecksumsPerIntegrityBlock; ++slot) {
            UNIT_ASSERT_VALUES_EQUAL_C(finalImage.Checksums[slot], 0, "checksum slot " << slot);
        }
        UNIT_ASSERT_VALUES_EQUAL(finalImage.Checksums[0],
            SealBlockChecksum(finalChecksum, TestDDiskId, TestPDiskGuid,
                key.TabletId, key.VChunkIndex, ChecksumsPerIntegrityBlock));
        manager.OnIoCompleted(finalWrite.Writes[0].IoId);
        UNIT_ASSERT_VALUES_EQUAL(TakeOnlyCompletion(manager).OperationId, finalOperation);

        const TKey crossingKey{.TabletId = 6, .VChunkIndex = 1};
        MakeReady(manager, crossingKey, 551, &nextIntegrityChunkIdx);
        std::vector<ui64> crossingChecksums(ChecksumsPerIntegrityBlock + 1);
        for (ui32 i = 0; i < crossingChecksums.size(); ++i) {
            crossingChecksums[i] = 0x30000 + i;
        }
        const ui64 crossingOperation = manager.BeginBlocksWrite(
            crossingKey, 0, PairBoundaryChunkSize, crossingChecksums);
        TActionLog crossingWrites = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(crossingWrites.Writes.size(), 2);
        std::array<bool, 2> seenPairs{};
        for (const TWriteIo& write : crossingWrites.Writes) {
            TIntegrityBlock image;
            memcpy(&image, write.Data.data(), sizeof(image));
            UNIT_ASSERT(image.Header.ChecksumBlockIdx < seenPairs.size());
            seenPairs[image.Header.ChecksumBlockIdx] = true;
            manager.OnIoCompleted(write.IoId);
        }
        UNIT_ASSERT(seenPairs[0] && seenPairs[1]);
        UNIT_ASSERT_VALUES_EQUAL(TakeOnlyCompletion(manager).OperationId, crossingOperation);
    }

    Y_UNIT_TEST(SparseBlockStateAllocation) {
        TIntegrityManager manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        UNIT_ASSERT_VALUES_EQUAL(manager.BlocksPerExtent(), 3);

        const TKey key{.TabletId = 9, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 780;
        MakeReady(manager, key, 800, &nextIntegrityChunkIdx);
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);

        // A checksummed write allocates exactly one state, covering its whole TIntegrityBlock.
        manager.BeginBlocksWrite(key, 0, IntegrityUnitSize, {0xA});
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 1);
        manager.BeginBlocksWrite(key, IntegrityUnitSize, IntegrityUnitSize, {0xB});
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 1);

        // A write into the second TIntegrityBlock's range allocates the second state.
        manager.BeginBlocksWrite(key, ChecksumsPerIntegrityBlock * IntegrityUnitSize, IntegrityUnitSize, {0xC});
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

        auto persist = [&](TKey target, ui32 block, ui64 checksum) {
            manager.BeginBlocksWrite(target, block * IntegrityUnitSize, IntegrityUnitSize, {checksum});
            TActionLog actions = Drain(manager);
            UNIT_ASSERT_VALUES_EQUAL(actions.Writes.size(), 1);
            manager.OnIoCompleted(actions.Writes[0].IoId);
            Y_UNUSED(manager.TakeCompletedOperations());
        };

        // Fill all three TIntegrityBlocks durably: the oldest checksum array (block 0's) is
        // evicted, while its pinned digest survives.
        persist(key, 0, 0xA);
        persist(key, block1, 0xB);
        persist(key, block2, 0xC);
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 2);

        // Evicted checksum array, pinned digest retained for lost-write detection.
        ui64 checksum = 0;
        UNIT_ASSERT(!manager.GetBlockChecksum(key, 0, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0),
            Contribution(generation, 0, 0xA));

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
        persist(key, block1, 0xD);
        const TKey key2{.TabletId = 10, .VChunkIndex = 1};
        MakeReady(manager, key2, 811, &nextIntegrityChunkIdx);
        persist(key2, 0, 0xE);
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 2);

        UNIT_ASSERT(!manager.GetBlockChecksum(key, block2, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 2),
            Contribution(generation, block2, 0xC));
        UNIT_ASSERT(manager.GetBlockChecksum(key, block1, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xD);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 1),
            Contribution(generation, block1, 0xD));
        UNIT_ASSERT(manager.GetBlockChecksum(key2, 0, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xE);
    }

    Y_UNIT_TEST(ReadModifyWriteAfterEvictionPreservesUntouchedChecksums) {
        TIntegrityManager manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid,
            TIntegrityManager::BlockStateApproxBytes);
        const TKey key{.TabletId = 17, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 796;
        MakeReady(manager, key, 830, &nextIntegrityChunkIdx);
        const auto ref = *manager.FindExtentRef(key);
        const ui64 chunkGeneration =
            manager.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        const ui64 generation = ref.VChunkGeneration;

        auto persist = [&](ui32 block, ui32 blocks, const std::vector<ui64>& checksums) {
            const ui64 operationId = manager.BeginBlocksWrite(
                key, block * IntegrityUnitSize, blocks * IntegrityUnitSize, checksums);
            TActionLog actions = Drain(manager);
            UNIT_ASSERT_VALUES_EQUAL(actions.Reads.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(actions.Writes.size(), 1);
            manager.OnIoCompleted(actions.Writes[0].IoId);
            UNIT_ASSERT_VALUES_EQUAL(TakeOnlyCompletion(manager).OperationId, operationId);
        };

        persist(0, 2, {0xAA, 0xBB});
        persist(ChecksumsPerIntegrityBlock, 1, {0xCC});
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 1);
        ui64 checksum = 0;
        UNIT_ASSERT(!manager.GetBlockChecksum(key, 0, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0),
            Contribution(generation, 0, 0xAA) ^ Contribution(generation, 1, 0xBB));

        const ui64 operationId =
            manager.BeginBlocksWrite(key, 0, IntegrityUnitSize, {0xDD});
        TActionLog read = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(read.Reads.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(read.Writes.size(), 0);
        manager.OnReadIoCompleted(read.Reads[0].IoId, MakeIntegrityPair(
            MakeIntegrityBlock(key, ref, chunkGeneration, 0, 2, {{0, 0xAA}, {1, 0xBB}}),
            MakeIntegrityBlock(key, ref, chunkGeneration, 0, 1, {})));

        TActionLog write = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(write.Writes.size(), 1);
        TIntegrityBlock image;
        memcpy(&image, write.Writes[0].Data.data(), sizeof(image));
        UNIT_ASSERT_VALUES_EQUAL(image.Header.ChecksumBlockIdx, 0);
        UNIT_ASSERT_VALUES_EQUAL(image.Header.PairSequenceNumber, 3);
        UNIT_ASSERT_VALUES_EQUAL(image.Header.UsedBlocksBitmap[0] & 3, 3);
        UNIT_ASSERT_VALUES_EQUAL(
            UnsealBlockChecksum(image.Checksums[0], TestDDiskId, TestPDiskGuid,
                key.TabletId, key.VChunkIndex, 0),
            0xDD);
        UNIT_ASSERT_VALUES_EQUAL(
            UnsealBlockChecksum(image.Checksums[1], TestDDiskId, TestPDiskGuid,
                key.TabletId, key.VChunkIndex, 1),
            0xBB);
        const ui64 expectedDigest =
            Contribution(generation, 0, 0xDD) ^ Contribution(generation, 1, 0xBB);
        UNIT_ASSERT_VALUES_EQUAL(image.Header.IntegrityBlockDigest, expectedDigest);

        manager.OnIoCompleted(write.Writes[0].IoId);
        const auto result = TakeOnlyCompletion(manager);
        UNIT_ASSERT_VALUES_EQUAL(result.OperationId, operationId);
        UNIT_ASSERT_EQUAL(result.Status, TIntegrityManager::EOperationStatus::Ok);
        UNIT_ASSERT(manager.GetBlockChecksum(key, 0, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xDD);
        UNIT_ASSERT(manager.GetBlockChecksum(key, 1, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xBB);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0), expectedDigest);
    }

    Y_UNIT_TEST(BlockStatesDroppedOnDelete) {
        TIntegrityManager manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 11, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 795;
        MakeReady(manager, key, 820, &nextIntegrityChunkIdx);

        manager.BeginBlocksWrite(key, 0, IntegrityUnitSize, {0xA});
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
        manager.BeginBlocksWrite(key, 0, IntegrityUnitSize, {0xA});
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

        // Restored extents start with unknown bitmaps. A write first reads the pair, then performs
        // a durable RMW and makes the bitmap/checksum state exact.
        UNIT_ASSERT_EQUAL(restored.MakeReadPlan(keys[0], 0, TinyChunkSize).Kind, TReadPlan::Passthrough);
        const ui64 operationId = restored.BeginBlocksWrite(keys[0], 0, IntegrityUnitSize, {0xA});
        TActionLog read = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(read.Reads.size(), 1);
        const auto restoredRef = *restored.FindExtentRef(keys[0]);
        const ui64 restoredChunkGeneration =
            restored.GetIntegrityChunkGeneration(restoredRef.IntegrityChunkIdx);
        restored.OnReadIoCompleted(read.Reads[0].IoId, MakeIntegrityPair(
            MakeIntegrityBlock(keys[0], restoredRef, restoredChunkGeneration, 0, 0, {}),
            MakeIntegrityBlock(keys[0], restoredRef, restoredChunkGeneration, 0, 1, {})));
        TActionLog write = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(write.Writes.size(), 1);
        restored.OnIoCompleted(write.Writes[0].IoId);
        UNIT_ASSERT_VALUES_EQUAL(TakeOnlyCompletion(restored).OperationId, operationId);
        ui64 checksum = 0;
        UNIT_ASSERT(restored.GetBlockChecksum(keys[0], 0, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xA);
        UNIT_ASSERT_EQUAL(restored.MakeReadPlan(keys[0], 0, TinyChunkSize).Kind, TReadPlan::Mixed);

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

    Y_UNIT_TEST(ChecksumPersistenceSealsAndPingPongs) {
        TIntegrityManager manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 40, .VChunkIndex = 7};
        TChunkIdx nextIntegrityChunkIdx = 900;
        MakeReady(manager, key, 1600, &nextIntegrityChunkIdx);
        const auto ref = *manager.FindExtentRef(key);

        const ui64 firstOperation = manager.BeginBlocksWrite(
            key, 0, 2 * IntegrityUnitSize, {0x111, 0x222});
        TActionLog first = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(first.Reads.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(first.Writes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(first.Writes[0].OffsetInBytes, manager.ExtentOffset(ref.ExtentSlot));

        TIntegrityBlock firstImage;
        memcpy(&firstImage, first.Writes[0].Data.data(), sizeof(firstImage));
        UNIT_ASSERT_VALUES_EQUAL(firstImage.Header.PairSequenceNumber, 2);
        UNIT_ASSERT(firstImage.Header.UsedBlocksBitmap[0] & 1);
        UNIT_ASSERT(firstImage.Header.UsedBlocksBitmap[0] & 2);
        UNIT_ASSERT_VALUES_EQUAL(firstImage.Checksums[0],
            SealBlockChecksum(
                0x111, TestDDiskId, TestPDiskGuid, key.TabletId, key.VChunkIndex, 0));
        UNIT_ASSERT_VALUES_EQUAL(firstImage.Checksums[1],
            SealBlockChecksum(
                0x222, TestDDiskId, TestPDiskGuid, key.TabletId, key.VChunkIndex, 1));
        UNIT_ASSERT_VALUES_EQUAL(firstImage.Header.IntegrityBlockDigest,
            Contribution(ref.VChunkGeneration, 0, 0x111)
                ^ Contribution(ref.VChunkGeneration, 1, 0x222));

        manager.OnIoCompleted(first.Writes[0].IoId);
        auto completion = TakeOnlyCompletion(manager);
        UNIT_ASSERT_VALUES_EQUAL(completion.OperationId, firstOperation);
        UNIT_ASSERT_EQUAL(completion.Status, TIntegrityManager::EOperationStatus::Ok);

        const ui64 secondOperation = manager.BeginBlocksWrite(
            key, IntegrityUnitSize, IntegrityUnitSize, {0x333});
        TActionLog second = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(second.Writes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(second.Writes[0].OffsetInBytes,
            manager.ExtentOffset(ref.ExtentSlot) + IntegrityUnitSize);
        TIntegrityBlock secondImage;
        memcpy(&secondImage, second.Writes[0].Data.data(), sizeof(secondImage));
        UNIT_ASSERT_VALUES_EQUAL(secondImage.Header.PairSequenceNumber, 3);
        manager.OnIoCompleted(second.Writes[0].IoId);
        UNIT_ASSERT_VALUES_EQUAL(TakeOnlyCompletion(manager).OperationId, secondOperation);

        const ui64 readOperation = manager.BeginChecksumRead(key, 0, 2 * IntegrityUnitSize);
        completion = TakeOnlyCompletion(manager);
        UNIT_ASSERT_VALUES_EQUAL(completion.OperationId, readOperation);
        UNIT_ASSERT_VALUES_EQUAL(completion.Checksums.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(completion.Checksums[0], 0x111);
        UNIT_ASSERT_VALUES_EQUAL(completion.Checksums[1], 0x333);
    }

    Y_UNIT_TEST(PairWritesSerializeAndCoalesce) {
        TIntegrityManager manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 41, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 910;
        MakeReady(manager, key, 1700, &nextIntegrityChunkIdx);

        const ui64 firstOperation = manager.BeginBlocksWrite(key, 0, IntegrityUnitSize, {0xA});
        TActionLog first = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(first.Writes.size(), 1);

        const ui64 secondOperation = manager.BeginBlocksWrite(
            key, IntegrityUnitSize, IntegrityUnitSize, {0xB});
        UNIT_ASSERT_VALUES_EQUAL(Drain(manager).Writes.size(), 0);

        manager.OnIoCompleted(first.Writes[0].IoId);
        auto completed = manager.TakeCompletedOperations();
        UNIT_ASSERT_VALUES_EQUAL(completed.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(completed[0].OperationId, firstOperation);

        TActionLog second = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(second.Writes.size(), 1);
        TIntegrityBlock image;
        memcpy(&image, second.Writes[0].Data.data(), sizeof(image));
        UNIT_ASSERT_VALUES_EQUAL(image.Header.PairSequenceNumber, 3);
        UNIT_ASSERT(image.Header.UsedBlocksBitmap[0] & 1);
        UNIT_ASSERT(image.Header.UsedBlocksBitmap[0] & 2);
        manager.OnIoCompleted(second.Writes[0].IoId);
        UNIT_ASSERT_VALUES_EQUAL(TakeOnlyCompletion(manager).OperationId, secondOperation);
    }

    Y_UNIT_TEST(RestoredPairSelectsWinnerAndRestoresBitmap) {
        TIntegrityManager original(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 42, .VChunkIndex = 3};
        TChunkIdx nextIntegrityChunkIdx = 920;
        MakeReady(original, key, 1800, &nextIntegrityChunkIdx);
        const auto snapshot = original.SnapshotMapping();
        const auto ref = *original.FindExtentRef(key);
        const ui64 chunkGeneration = original.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);

        TIntegrityManager restored(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        restored.ApplyMappingSnapshot(snapshot);
        const ui64 operationId = restored.BeginChecksumRead(key, 0, 4 * IntegrityUnitSize);
        TActionLog actions = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(actions.Reads.size(), 1);
        const auto a = MakeIntegrityBlock(key, ref, chunkGeneration, 0, 2, {{0, 0x10}});
        const auto b = MakeIntegrityBlock(key, ref, chunkGeneration, 0, 3, {{2, 0x30}});
        restored.OnReadIoCompleted(actions.Reads[0].IoId, MakeIntegrityPair(a, b));

        const auto result = TakeOnlyCompletion(restored);
        UNIT_ASSERT_VALUES_EQUAL(result.OperationId, operationId);
        UNIT_ASSERT_VALUES_EQUAL(result.Checksums.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(result.Checksums[0], GetZeroBlockChecksum());
        UNIT_ASSERT_VALUES_EQUAL(result.Checksums[2], 0x30);
        const auto plan = restored.MakeReadPlan(key, 0, 4 * IntegrityUnitSize);
        UNIT_ASSERT_EQUAL(plan.Kind, TReadPlan::Mixed);
        UNIT_ASSERT(!plan.UsedBlocks.Get(0));
        UNIT_ASSERT(plan.UsedBlocks.Get(2));
    }

    Y_UNIT_TEST(BothInvalidSlotsReportCorruption) {
        TIntegrityManager original(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 43, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 930;
        MakeReady(original, key, 1900, &nextIntegrityChunkIdx);
        const auto snapshot = original.SnapshotMapping();
        const auto ref = *original.FindExtentRef(key);
        const ui64 chunkGeneration = original.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);

        TIntegrityManager restored(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        restored.ApplyMappingSnapshot(snapshot);
        const ui64 operationId = restored.BeginChecksumRead(key, 0, IntegrityUnitSize);
        TActionLog actions = Drain(restored);
        auto a = MakeIntegrityBlock(key, ref, chunkGeneration, 0, 2, {{0, 1}});
        auto b = MakeIntegrityBlock(key, ref, chunkGeneration, 0, 3, {{0, 2}});
        ++a.Checksums[0];
        ++b.Checksums[0];
        restored.OnReadIoCompleted(actions.Reads[0].IoId, MakeIntegrityPair(a, b));
        const auto result = TakeOnlyCompletion(restored);
        UNIT_ASSERT_VALUES_EQUAL(result.OperationId, operationId);
        UNIT_ASSERT_EQUAL(result.Status, TIntegrityManager::EOperationStatus::Corrupted);
        UNIT_ASSERT(!result.LostWriteDetected);
    }

    Y_UNIT_TEST(PendingMultiPairReadPinsChecksumStates) {
        TIntegrityManager original(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 45, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 950;
        MakeReady(original, key, 2100, &nextIntegrityChunkIdx);
        const auto snapshot = original.SnapshotMapping();

        TIntegrityManager restored(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid,
            TIntegrityManager::BlockStateApproxBytes);
        restored.ApplyMappingSnapshot(snapshot);
        const auto ref = *restored.FindExtentRef(key);
        const ui64 chunkGeneration =
            restored.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        const ui32 blocks = ChecksumsPerIntegrityBlock + 1;

        const ui64 operationId =
            restored.BeginChecksumRead(key, 0, blocks * IntegrityUnitSize);
        TActionLog reads = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(reads.Reads.size(), 2);

        restored.OnReadIoCompleted(reads.Reads[0].IoId, MakeIntegrityPair(
            MakeIntegrityBlock(key, ref, chunkGeneration, 0, 0, {{0, 0xAA}}),
            MakeIntegrityBlock(key, ref, chunkGeneration, 0, 1, {{0, 0xAA}})));
        UNIT_ASSERT_VALUES_EQUAL(restored.CachedBlockStates(), 1);
        UNIT_ASSERT(restored.TakeCompletedOperations().empty());

        restored.OnReadIoCompleted(reads.Reads[1].IoId, MakeIntegrityPair(
            MakeIntegrityBlock(key, ref, chunkGeneration, 1, 0, {{0, 0xBB}}),
            MakeIntegrityBlock(key, ref, chunkGeneration, 1, 1, {{0, 0xBB}})));
        auto result = TakeOnlyCompletion(restored);
        UNIT_ASSERT_VALUES_EQUAL(result.OperationId, operationId);
        UNIT_ASSERT_EQUAL(result.Status, TIntegrityManager::EOperationStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(result.Checksums.size(), blocks);
        UNIT_ASSERT_VALUES_EQUAL(result.Checksums.front(), 0xAA);
        UNIT_ASSERT_VALUES_EQUAL(result.Checksums.back(), 0xBB);
        UNIT_ASSERT_VALUES_EQUAL(restored.CachedBlockStates(), 1);
    }

    Y_UNIT_TEST(PendingMultiPairWritePinsChecksumStates) {
        TIntegrityManager original(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 47, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 970;
        MakeReady(original, key, 2300, &nextIntegrityChunkIdx);
        const auto snapshot = original.SnapshotMapping();

        TIntegrityManager restored(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid,
            TIntegrityManager::BlockStateApproxBytes);
        restored.ApplyMappingSnapshot(snapshot);
        const auto ref = *restored.FindExtentRef(key);
        const ui64 chunkGeneration =
            restored.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        const ui32 blocks = ChecksumsPerIntegrityBlock + 1;
        std::vector<ui64> checksums(blocks);
        for (ui32 i = 0; i < blocks; ++i) {
            checksums[i] = 0x1000 + i;
        }

        const ui64 operationId =
            restored.BeginBlocksWrite(key, 0, blocks * IntegrityUnitSize, checksums);
        TActionLog reads = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(reads.Reads.size(), 2);

        restored.OnReadIoCompleted(reads.Reads[0].IoId, MakeIntegrityPair(
            MakeIntegrityBlock(key, ref, chunkGeneration, 0, 0, {{0, 0xAA}}),
            MakeIntegrityBlock(key, ref, chunkGeneration, 0, 1, {{0, 0xAA}})));
        UNIT_ASSERT(restored.TakeCompletedOperations().empty());

        restored.OnReadIoCompleted(reads.Reads[1].IoId, MakeIntegrityPair(
            MakeIntegrityBlock(key, ref, chunkGeneration, 1, 0, {{0, 0xBB}}),
            MakeIntegrityBlock(key, ref, chunkGeneration, 1, 1, {{0, 0xBB}})));
        TActionLog writes = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(writes.Writes.size(), 2);

        ui64 expectedPair0Digest = 0;
        for (ui32 i = 0; i < ChecksumsPerIntegrityBlock; ++i) {
            expectedPair0Digest ^= Contribution(ref.VChunkGeneration, i, checksums[i]);
        }
        const ui64 expectedPair1Digest = Contribution(
            ref.VChunkGeneration, ChecksumsPerIntegrityBlock, checksums.back());
        for (const auto& write : writes.Writes) {
            TIntegrityBlock block;
            memcpy(&block, write.Data.data(), sizeof(block));
            const ui64 expected = block.Header.ChecksumBlockIdx == 0
                ? expectedPair0Digest
                : expectedPair1Digest;
            UNIT_ASSERT_VALUES_EQUAL(block.Header.IntegrityBlockDigest, expected);
            restored.OnIoCompleted(write.IoId);
        }

        auto result = TakeOnlyCompletion(restored);
        UNIT_ASSERT_VALUES_EQUAL(result.OperationId, operationId);
        UNIT_ASSERT_EQUAL(result.Status, TIntegrityManager::EOperationStatus::Ok);
    }

    Y_UNIT_TEST(MultiPairCorruptionKeepsDeletionBusyForSiblingIo) {
        TIntegrityManager original(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 46, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 960;
        MakeReady(original, key, 2200, &nextIntegrityChunkIdx);
        const auto snapshot = original.SnapshotMapping();

        TIntegrityManager restored(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        restored.ApplyMappingSnapshot(snapshot);
        const auto ref = *restored.FindExtentRef(key);
        const ui64 chunkGeneration =
            restored.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        const ui32 blocks = ChecksumsPerIntegrityBlock + 1;

        restored.BeginChecksumRead(key, 0, blocks * IntegrityUnitSize);
        TActionLog reads = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(reads.Reads.size(), 2);

        auto invalid = TRcBuf::UninitializedPageAligned(
            IntegrityPairSlots * sizeof(TIntegrityBlock));
        memset(invalid.GetDataMut(), 0, invalid.size());
        restored.OnReadIoCompleted(reads.Reads[1].IoId, TRope(std::move(invalid)));
        UNIT_ASSERT_EQUAL(
            TakeOnlyCompletion(restored).Status,
            TIntegrityManager::EOperationStatus::Corrupted);
        UNIT_ASSERT(restored.HasInFlightOperationsForTablet(key.TabletId));

        restored.OnReadIoCompleted(reads.Reads[0].IoId, MakeIntegrityPair(
            MakeIntegrityBlock(key, ref, chunkGeneration, 0, 0, {}),
            MakeIntegrityBlock(key, ref, chunkGeneration, 0, 1, {})));
        UNIT_ASSERT(!restored.HasInFlightOperationsForTablet(key.TabletId));

        // The known corruption is detected before any sibling pair read is queued.
        restored.BeginChecksumRead(key, 0, blocks * IntegrityUnitSize);
        UNIT_ASSERT(Drain(restored).Reads.empty());
        UNIT_ASSERT_EQUAL(
            TakeOnlyCompletion(restored).Status,
            TIntegrityManager::EOperationStatus::Corrupted);

        restored.PrepareTabletChunksDeletion(key.TabletId);
        restored.CommitTabletChunksDeletion(key.TabletId);
    }

    Y_UNIT_TEST(PinnedDigestDetectsLostWriteAfterEviction) {
        TIntegrityManager manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid,
            TIntegrityManager::BlockStateApproxBytes);
        const TKey key{.TabletId = 44, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 940;
        MakeReady(manager, key, 2000, &nextIntegrityChunkIdx);
        const auto ref = *manager.FindExtentRef(key);
        const ui64 chunkGeneration = manager.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);

        manager.BeginBlocksWrite(key, 0, IntegrityUnitSize, {0xAA});
        TActionLog pair0Write = Drain(manager);
        manager.OnIoCompleted(pair0Write.Writes[0].IoId);
        Y_UNUSED(manager.TakeCompletedOperations());

        const ui32 pair1Block = ChecksumsPerIntegrityBlock;
        manager.BeginBlocksWrite(key, pair1Block * IntegrityUnitSize, IntegrityUnitSize, {0xBB});
        Y_UNUSED(Drain(manager)); // creating pair 1 evicts the now-idle checksum array of pair 0

        const ui64 readOperation = manager.BeginChecksumRead(key, 0, IntegrityUnitSize);
        TActionLog read = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(read.Reads.size(), 1);
        const auto stale = MakeIntegrityBlock(key, ref, chunkGeneration, 0, 1, {});
        manager.OnReadIoCompleted(read.Reads[0].IoId, MakeIntegrityPair(stale, stale));
        const auto result = TakeOnlyCompletion(manager);
        UNIT_ASSERT_VALUES_EQUAL(result.OperationId, readOperation);
        UNIT_ASSERT_EQUAL(result.Status, TIntegrityManager::EOperationStatus::Corrupted);
        UNIT_ASSERT(result.ErrorReason.Contains("digest mismatch"));
        UNIT_ASSERT(result.LostWriteDetected);
    }

} // Y_UNIT_TEST_SUITE(TIntegrityManagerTest)

} // namespace NKikimr::NDDisk
