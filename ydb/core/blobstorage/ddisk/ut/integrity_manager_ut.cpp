#include "integrity_test_driver.h"
#include <ydb/library/actors/async/ut/common.h>

#include <ydb/library/actors/async/cancellation.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/overloaded.h>
#include <util/generic/scope.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <vector>
#include <set>

namespace NKikimr::NDDisk {

namespace {

using TKey = NIntegrityTest::TFixture::TDataChunkKey;
using TWriteIo = NIntegrityTest::TFixture::TWriteIo;
using TReadIo = NIntegrityTest::TFixture::TReadIo;
using TReadPlan = NIntegrityTest::TFixture::TReadPlan;

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

using TActions = NIntegrityTest::TFixture::TActions;

TActions Drain(NIntegrityTest::TFixture& manager) {
    return manager.TakeActions();
}

void CompleteWrites(NIntegrityTest::TFixture& manager, std::vector<TWriteIo>& writes) {
    for (auto& io : writes) {
        manager.CompleteWrite(io);
    }
}

void CompleteWrites(NIntegrityTest::TFixture& manager, std::vector<TWriteIo>&& writes) {
    CompleteWrites(manager, writes);
}

// Complete captured allocation tokens and formatting I/O until the native extent is ready.
void MakeReady(NIntegrityTest::TFixture& manager, TKey key, TChunkIdx dataChunkIdx, TChunkIdx* nextIntegrityChunkIdx) {
    auto extent = manager.StartExtent(key, dataChunkIdx);
    while (!extent.GetReadyResult().has_value()) {
        auto actions = Drain(manager);
        for (ui64 token : actions.Allocations) {
            manager.CompleteAllocation(token, (*nextIntegrityChunkIdx)++);
        }
        CompleteWrites(manager, actions.Writes);
        UNIT_ASSERT_C(!actions.Allocations.empty() || !actions.Writes.empty() || extent.GetReadyResult().has_value(),
            "allocation is stuck");
    }
    UNIT_ASSERT(extent.GetReadyResult() == true);
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

void CheckExtentFormat(const NIntegrityTest::TFixture& manager, const TWriteIo& io, TKey key,
        const NIntegrityTest::TFixture::TExtentRef& ref, ui64 integrityChunkGeneration)
{
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

TIntegrityBlock MakeIntegrityBlock(TKey key, const NIntegrityTest::TFixture::TExtentRef& ref,
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

TRope JoinRopes(TRope left, TRope right) {
    left.Insert(left.End(), std::move(right));
    return left;
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

TIntegrityManager::TOperationResult ReadReadyResult(NIntegrityTest::TFixture& manager,
        TKey key, ui32 offset, ui32 size)
{
    auto preparation = manager.PrepareRead(key, offset, size);
    UNIT_ASSERT(preparation.Warm);
    UNIT_ASSERT(preparation.MetadataReads.empty());
    UNIT_ASSERT_EQUAL(preparation.Warm->Status, TIntegrityManager::EOperationStatus::Ok);
    return std::move(*preparation.Warm);
}

TIntegrityManager::TOperation PreparePendingRead(NIntegrityTest::TFixture& manager,
    TKey key, ui32 offset, ui32 size)
{
    auto preparation = manager.PrepareRead(key, offset, size);
    UNIT_ASSERT(!preparation.Warm);
    manager.SubmitMetadataReads(std::move(preparation.MetadataReads));
    return preparation.Pending;
}

template <typename TOperation>
TIntegrityManager::TOperationResult ResultOf(const TOperation& operation) {
    UNIT_ASSERT(operation.GetResult());
    return *operation.GetResult();
}

struct TActorWaiters {
    NAsyncTest::TAsyncTestActor::TState State;
    NAsyncTest::TAsyncTestActorRuntime Runtime;
    NAsyncTest::TAsyncTestActorRuntime::TAsyncActorOperations Actor;

    TActorWaiters()
        : Actor(Runtime.StartAsyncActor(State, [](auto*) -> NActors::async<void> {
            co_return;
        }))
    {
    }

    void Observe(TIntegrityManager::TOperation operation, size_t& notifications) {
        Actor.RunAsync([operation = std::move(operation), &notifications]() -> NActors::async<void> {
            while (!operation.GetResult()) {
                co_await operation.WaitChanged();
            }
            ++notifications;
        });
    }
};

} // namespace

void AssertChecksums(TConstArrayRef<ui64> actual, TConstArrayRef<ui64> expected) {
    UNIT_ASSERT_VALUES_EQUAL(actual, expected);
}

Y_UNIT_TEST_SUITE(TIntegrityManagerTest) {
    Y_UNIT_TEST(ReadChecksumsOwnCompactSnapshots) {
        TReadChecksums values;
        UNIT_ASSERT(values.empty());
        values = {42};
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 1);
        auto single = values;
        values.assign(1, 7);
        UNIT_ASSERT_VALUES_EQUAL(single[0], 42);
        absl::InlinedVector<ui64, 1> large(1024, 99);
        const auto* allocation = large.data();
        values = std::move(large);
        UNIT_ASSERT_VALUES_EQUAL(values.data(), allocation);
        auto copy = values;
        UNIT_ASSERT(copy.data() != allocation);
        values.clear();
        UNIT_ASSERT(values.empty());
        UNIT_ASSERT_VALUES_EQUAL(copy.size(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(copy[1000], 99);
        copy.clear();
        UNIT_ASSERT(copy.empty());
    }

    Y_UNIT_TEST(ReadPayloadPreservesOwnershipAndDecodesSegments) {
        auto buffer = TRcBuf::Uninitialized(8192);
        auto* address = buffer.GetDataMut();
        memset(address, 'N', buffer.size());
        TReadPayload native(std::move(buffer));
        UNIT_ASSERT(native.IsNative());
        UNIT_ASSERT_VALUES_EQUAL(native.MutableSpan().data(), address);
        char image[8192];
        native.CopyTo(image, sizeof(image));
        UNIT_ASSERT_VALUES_EQUAL(image[8191], 'N');
        auto rope = std::move(native).IntoRope();
        UNIT_ASSERT_VALUES_EQUAL(rope.Begin().ContiguousData(), address);
        UNIT_ASSERT_VALUES_EQUAL(native.size(), 0);
        TRope segmented(TString(4096, 'A'));
        segmented.Insert(segmented.End(), TRope(TString(4096, 'B')));
        const auto* first = segmented.Begin().ContiguousData();
        TReadPayload fallback(std::move(segmented));
        UNIT_ASSERT(!fallback.IsNative());
        fallback.CopyTo(image, sizeof(image));
        UNIT_ASSERT_VALUES_EQUAL(image[0], 'A');
        UNIT_ASSERT_VALUES_EQUAL(image[4096], 'B');
        auto output = std::move(fallback).IntoRope();
        UNIT_ASSERT_VALUES_EQUAL(output.Begin().ContiguousData(), first);
    }

    Y_UNIT_TEST(CachedReadsCompleteWithoutWork) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{99, 0};
        TChunkIdx nextChunk = 1000;
        MakeReady(manager, key, 2000, &nextChunk);
        auto readCached = [&](ui32 firstBlock, const std::vector<ui64>& checksums, TReadPlan::EKind kind) {
            const auto result = ReadReadyResult(manager, key, firstBlock * IntegrityUnitSize,
                checksums.size() * IntegrityUnitSize);
            AssertChecksums(result.Checksums, checksums);
            UNIT_ASSERT_EQUAL(result.ReadPlan.Kind, kind);
            if (kind == TReadPlan::Mixed) {
                for (ui32 block = 0; block < checksums.size(); ++block) {
                    UNIT_ASSERT_VALUES_EQUAL(result.ReadPlan.UsedBlocks.Get(block),
                        firstBlock + block == 1 || firstBlock + block == 2);
                }
            }
            UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
            UNIT_ASSERT(manager.Submissions.empty());
        };
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);
        readCached(0, {GetZeroBlockChecksum(), GetZeroBlockChecksum()}, TReadPlan::AllZero);
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);
        auto write = manager.Write(key, IntegrityUnitSize, 2 * IntegrityUnitSize, {0xA, 0xB});
        CompleteWrites(manager, Drain(manager).Writes);
        UNIT_ASSERT(write.GetResult());
        readCached(1, {0xA, 0xB}, TReadPlan::Passthrough);
        readCached(0, {GetZeroBlockChecksum(), 0xA, 0xB, GetZeroBlockChecksum()}, TReadPlan::Mixed);
        readCached(4, {GetZeroBlockChecksum(), GetZeroBlockChecksum()}, TReadPlan::AllZero);
    }

    Y_UNIT_TEST(OperationWaitSupportsConcurrentAndRepeatedWaits) {
        auto state = std::make_shared<TIntegrityManager::TOperationState>();
        TIntegrityManager::TOperation operation(state);
        std::vector<TIntegrityManager::TOperationResult> completed;
        NAsyncTest::TAsyncTestActor::TState actorState;
        NAsyncTest::TAsyncTestActorRuntime runtime;
        auto actor = runtime.StartAsyncActor(actorState, [](auto*) -> NActors::async<void> {
            co_return;
        });
        auto observe = [operation, &completed]() -> NActors::async<void> {
            while (!operation.IsDone()) {
                co_await operation.WaitChanged();
            }
            completed.push_back(*operation.GetResult());
        };
        actor.RunAsync(observe);
        actor.RunAsync(observe);
        UNIT_ASSERT(!operation.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(state->Changed.AwaitersCount(), 2);
        actor.RunSync([&] {
            state->Result.emplace();
            state->Result->Checksums = {0xA, 0xB};
            state->Changed.NotifyAll();
        });
        UNIT_ASSERT_VALUES_EQUAL(completed.size(), 2);
        completed[0].Checksums.clear();
        AssertChecksums(completed[1].Checksums, (std::vector<ui64>{0xA, 0xB}));
        UNIT_ASSERT_VALUES_EQUAL(state->Changed.AwaitersCount(), 0);
        actor.RunAsync(observe);
        UNIT_ASSERT_VALUES_EQUAL(completed.size(), 3);
        AssertChecksums(completed.back().Checksums, (std::vector<ui64>{0xA, 0xB}));
        AssertChecksums(operation.GetResult()->Checksums, completed.back().Checksums);
        UNIT_ASSERT_VALUES_EQUAL(state->Changed.AwaitersCount(), 0);
    }

    Y_UNIT_TEST(OperationWaitOwnsStateAndUnlinksOnCancellationOrShutdown) {
        for (const bool cancel : {false, true}) {
            auto completion = std::make_shared<TIntegrityManager::TOperationState>();
            const std::weak_ptr<TIntegrityManager::TOperationState> weak = completion;
            // The operation handle and this strong reference disappear before suspension.
            auto makeWaiter = [](TIntegrityManager::TOperation operation) -> NActors::async<void> {
                while (!operation.IsDone()) {
                    co_await operation.WaitChanged();
                }
            };
            bool resumed = false, finished = false, cancelled = false;
            NActors::TAsyncCancellationScope scope;
            NAsyncTest::TAsyncTestActor::TState state;
            NAsyncTest::TAsyncTestActorRuntime runtime;
            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> NActors::async<void> {
                Y_DEFER { finished = true; };
                const bool success = co_await scope.Wrap([&]() -> NActors::async<void> {
                    co_await makeWaiter(TIntegrityManager::TOperation(std::exchange(completion, {})));
                    resumed = true;
                });
                cancelled = !success;
            });
            UNIT_ASSERT(!completion);
            UNIT_ASSERT(!finished);
            auto retained = weak.lock();
            UNIT_ASSERT(retained);
            UNIT_ASSERT_VALUES_EQUAL(retained->Changed.AwaitersCount(), 1);
            if (cancel) {
                actor.RunSync([&] {
                    scope.Cancel();
                });
            } else {
                runtime.CleanupNode();
            }
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!resumed);
            UNIT_ASSERT_VALUES_EQUAL(cancelled, cancel);
            UNIT_ASSERT_VALUES_EQUAL(retained->Changed.AwaitersCount(), 0);
            retained.reset();
            UNIT_ASSERT(weak.expired());
        }
    }

    Y_UNIT_TEST(ExtentReadinessPreservesMilestoneSemantics) {
        for (const bool complete : {false, true}) {
            auto state = std::make_shared<TIntegrityManager::TExtentState>();
            const TIntegrityManager::TExtent extent(state);
            std::optional<bool> placed, ready;
            TActorWaiters waiters;
            waiters.Actor.RunAsync([&]() -> NActors::async<void> {
                while (!(placed = extent.GetPlacedResult()).has_value()) {
                    co_await extent.WaitChanged();
                }
            });
            waiters.Actor.RunAsync([&]() -> NActors::async<void> {
                while (!(ready = extent.GetReadyResult()).has_value()) {
                    co_await extent.WaitChanged();
                }
            });
            UNIT_ASSERT(!placed && !ready);
            waiters.Actor.RunSync([&] {
                state->Placed = true;
                state->Changed.NotifyAll();
            });
            UNIT_ASSERT(placed == true);
            UNIT_ASSERT(!ready);
            waiters.Actor.RunSync([&] {
                state->Ready = complete;
                state->Failed = true;
                state->Changed.NotifyAll();
            });
            UNIT_ASSERT(ready.has_value());
            UNIT_ASSERT_VALUES_EQUAL(*ready, complete);
            UNIT_ASSERT(extent.GetPlacedResult() == true);
            UNIT_ASSERT_VALUES_EQUAL(*extent.GetReadyResult(), complete);
        }
    }

    Y_UNIT_TEST(CompletedReadRetainsChecksumAndMaskSnapshot) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{99, 0};
        TChunkIdx nextChunk = 1000;
        MakeReady(manager, key, 2000, &nextChunk);
        const auto result = ReadReadyResult(manager, key, 0, IntegrityUnitSize);
        auto write = manager.Write(key, 0, IntegrityUnitSize, {0x123});
        UNIT_ASSERT_EQUAL(ReadReadyResult(manager, key, 0, IntegrityUnitSize).ReadPlan.Kind, TReadPlan::AllZero);
        CompleteWrites(manager, Drain(manager).Writes);
        UNIT_ASSERT(write.GetResult());
        UNIT_ASSERT_EQUAL(result.ReadPlan.Kind, TReadPlan::AllZero);
        AssertChecksums(result.Checksums, std::vector<ui64>{GetZeroBlockChecksum()});
    }

    Y_UNIT_TEST(ImmediateCompletionsPrecedeWait) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{99, 0};
        TChunkIdx nextChunk = 1000;
        MakeReady(manager, key, 2000, &nextChunk);
        auto write = manager.Write(key, 0, IntegrityUnitSize, {0x123});
        CompleteWrites(manager, Drain(manager).Writes);
        TActorWaiters waiters;
        size_t notifications = 0;
        waiters.Actor.RunAsync([&]() -> NActors::async<void> {
            while (!write.GetResult()) {
                co_await write.WaitChanged();
            }
            ++notifications;
        });
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
        AssertChecksums(ReadReadyResult(manager, key, 0, IntegrityUnitSize).Checksums, std::vector<ui64>{0x123});
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
    }

    Y_UNIT_TEST(FailedHeaderResolvesFormattingAndDurabilityWaits) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{99, 0};
        auto extent = manager.StartExtent(key, 2000);
        auto allocation = Drain(manager);
        manager.CompleteAllocation(allocation.Allocations.front(), 1000);
        auto formatting = Drain(manager);
        std::vector<TWriteIo> headers, extents;
        SplitWrites(formatting.Writes, &headers, &extents);
        auto writer = manager.PrepareWrite(key, 0, IntegrityUnitSize);
        UNIT_ASSERT(!writer.IsReady());
        CompleteWrites(manager, extents);
        manager.CompleteWrite(headers[0], false);
        UNIT_ASSERT(!writer.GetResult());
        for (size_t i = 1; i < headers.size(); ++i) {
            manager.CompleteWrite(headers[i]);
        }
        UNIT_ASSERT(extent.GetReadyResult() == false);
        UNIT_ASSERT(writer.IsReady());
        UNIT_ASSERT_EQUAL(writer.GetResult()->Status, TIntegrityManager::EOperationStatus::Failed);
        UNIT_ASSERT(!manager.IsExtentReady(key));
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
    }

    Y_UNIT_TEST(Geometry) {
        {
            NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
            UNIT_ASSERT_VALUES_EQUAL(manager.DataBlocksInChunk(), 256);
            UNIT_ASSERT_VALUES_EQUAL(manager.BlocksPerExtent(), 1);
            UNIT_ASSERT_VALUES_EQUAL(manager.ExtentOnDiskSize(), 2 * IntegrityUnitSize);
            UNIT_ASSERT_VALUES_EQUAL(manager.ExtentsPerChunk(),
                (SmallChunkSize - IntegrityChunkHeaderRegionSize) / (2 * IntegrityUnitSize));
        }
        {
            // The runtime geometry for the default PDisk chunk size must match the RFC example.
            NIntegrityTest::TFixture manager(ProductionChunkSize, TestDDiskId, TestPDiskGuid);
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
        const NIntegrityTest::TFixture::TExtentRef ref{
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
        const NIntegrityTest::TFixture::TExtentRef ref{
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
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 1, .VChunkIndex = 5};

        auto extent = manager.StartExtent(key, 100);
        UNIT_ASSERT(!manager.IsExtentReady(key));
        UNIT_ASSERT(!manager.FindExtentRef(key));

        // First data chunk: exactly one integrity chunk allocation is requested, no writes yet.
        TActions log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.Allocations.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 0);

        // Chunk arrives: header replicas and the extent format write are issued in parallel, and
        // the extent is already placed (IntegrityChunk found) even though nothing is Ready yet.
        manager.CompleteAllocation(log.Allocations.front(), 500);
        UNIT_ASSERT(manager.FindExtentRef(key));
        UNIT_ASSERT(!manager.IsExtentReady(key));
        UNIT_ASSERT(extent.GetPlacedResult() == true);
        UNIT_ASSERT(!extent.GetReadyResult());

        log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.Allocations.size(), 0);
        std::vector<TWriteIo> headers;
        std::vector<TWriteIo> extents;
        SplitWrites(log.Writes, &headers, &extents);
        UNIT_ASSERT_VALUES_EQUAL(headers.size(), NIntegrityTest::TFixture::ChunkHeaderReplicaCount);
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
        CompleteWrites(manager, extents);
        UNIT_ASSERT(!manager.IsExtentReady(key));

        for (ui32 i = 0; i + 1 < headers.size(); ++i) {
            manager.CompleteWrite(headers[i]);
            UNIT_ASSERT(!manager.IsExtentReady(key));
        }
        manager.CompleteWrite(headers.back());
        UNIT_ASSERT(extent.GetReadyResult() == true);
        UNIT_ASSERT(manager.IsExtentReady(key));
        UNIT_ASSERT(manager.Submissions.empty());
    }

    Y_UNIT_TEST(StopRetainsAcceptedFormattingUntilCompletion) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{71, 0};
        auto extent = manager.StartExtent(key, 100);
        auto allocation = Drain(manager);
        manager.CompleteAllocation(allocation.Allocations.front(), 500);
        auto formatting = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(formatting.Writes.size(), 4);
        UNIT_ASSERT(extent.GetPlacedResult() == true);
        manager.Stop();
        UNIT_ASSERT(extent.GetReadyResult() == false);
        UNIT_ASSERT(manager.TakeReleasableIntegrityChunks().empty());
        CompleteWrites(manager, formatting.Writes);
        UNIT_ASSERT(!manager.IsExtentReady(key));
        UNIT_ASSERT(manager.Submissions.empty());
        manager.PrepareTabletChunksDeletion(key.TabletId);
        manager.CommitTabletChunksDeletion(key.TabletId);
        // Failed chunk headers keep the uncommitted integrity chunk out of releasable Ready chunks.
        UNIT_ASSERT(manager.TakeReleasableIntegrityChunks().empty());
    }

    Y_UNIT_TEST(DuplicateAllocationTokenAndOldFormatCannotMutateRecreatedExtent) {
        NIntegrityTest::TFixture manager(140_KB, TestDDiskId, TestPDiskGuid);
        const TKey key{72, 0};
        manager.StartExtent(key, 100);
        auto requests = manager.TakeActions();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        const ui64 token = requests.Allocations.front();
        manager.CompleteAllocation(token, 500);

        {
            manager.CompleteAllocation(token, 501);
            manager.NotifyCompleted();
        }
        UNIT_ASSERT(manager.Returned.empty()); // a duplicate cannot return somebody else's chunk
        auto writes = Drain(manager).Writes;
        std::vector<TWriteIo> headers, extents;
        SplitWrites(writes, &headers, &extents);
        UNIT_ASSERT_VALUES_EQUAL(headers.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(extents.size(), 1);
        const ui64 oldFormatId = extents.front().Id;
        const ui64 oldGeneration = manager.FindExtentRef(key)->VChunkGeneration;

        manager.PrepareTabletChunksDeletion(key.TabletId);
        manager.CommitTabletChunksDeletion(key.TabletId);
        manager.StartExtent(key, 101);
        auto newAllocation = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(newAllocation.Allocations.size(), 1);
        manager.CompleteWrite(extents.front());
        const auto* current = manager.FindExtentRef(key);
        UNIT_ASSERT(current);
        UNIT_ASSERT_VALUES_UNEQUAL(current->VChunkGeneration, oldGeneration);
        UNIT_ASSERT_VALUES_EQUAL(current->IntegrityChunkIdx, 500);
        const ui64 newGeneration = current->VChunkGeneration;
        auto currentWrites = Drain(manager).Writes;
        UNIT_ASSERT_VALUES_EQUAL(currentWrites.size(), 1);

        {
            manager.Submit(manager.TIntegrityManager::CompleteWrite(oldFormatId, false));
            manager.NotifyCompleted();
        }
        UNIT_ASSERT_VALUES_EQUAL(manager.FindExtentRef(key)->VChunkGeneration, newGeneration);
        UNIT_ASSERT(!manager.IsExtentReady(key));
        CompleteWrites(manager, headers);
        CompleteWrites(manager, currentWrites);
        UNIT_ASSERT(manager.IsExtentReady(key));
        manager.CompleteAllocation(newAllocation.Allocations.front(), 501); // pending allocation became excess after slot reuse
        UNIT_ASSERT_VALUES_EQUAL(manager.Returned.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(manager.Returned.front(), 501);
    }

    Y_UNIT_TEST(SlotReuseAndExhaustion) {
        NIntegrityTest::TFixture manager(TinyChunkSize, TestDDiskId, TestPDiskGuid);
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
        NIntegrityTest::TFixture manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk

        // Five data chunks allocated before any integrity chunk arrives: demand of 5 extents
        // must produce exactly two chunk allocation requests (4 + 1), not five.
        for (ui32 i = 0; i < 5; ++i) {
            manager.StartExtent(TKey{2, i}, 200 + i);
        }
        TActions log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.Allocations.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 0);

        // Fulfill both; all five extents must eventually become ready.
        manager.CompleteAllocation(log.Allocations[0], 710);
        manager.CompleteAllocation(log.Allocations[1], 711);
        for (ui32 round = 0; round < 10 && !manager.Submissions.empty(); ++round) {
            CompleteWrites(manager, Drain(manager).Writes);
        }
        for (ui32 i = 0; i < 5; ++i) {
            UNIT_ASSERT(manager.IsExtentReady(TKey{2, i}));
        }
    }

    Y_UNIT_TEST(ReadPlans) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 3, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 720;
        MakeReady(manager, key, 300, &nextIntegrityChunkIdx);

        // Tracked, nothing written: all-zero without disk I/O.
        UNIT_ASSERT_EQUAL(ReadReadyResult(manager, key, 0, SmallChunkSize).ReadPlan.Kind, TReadPlan::AllZero);

        // Blocks 2..3 written with mandatory checksums.
        manager.Write(key, 2 * IntegrityUnitSize, 2 * IntegrityUnitSize, {0xA, 0xB});
        CompleteWrites(manager, Drain(manager).Writes);

        // Whole written range: passthrough (no zeroing needed).
        UNIT_ASSERT_EQUAL(ReadReadyResult(manager, key, 2 * IntegrityUnitSize, 2 * IntegrityUnitSize).ReadPlan.Kind,
            TReadPlan::Passthrough);

        // Untouched range: all-zero.
        UNIT_ASSERT_EQUAL(ReadReadyResult(manager, key, 0, 2 * IntegrityUnitSize).ReadPlan.Kind, TReadPlan::AllZero);
        UNIT_ASSERT_EQUAL(ReadReadyResult(manager, key, 4 * IntegrityUnitSize, 8 * IntegrityUnitSize).ReadPlan.Kind,
            TReadPlan::AllZero);

        // Partially written range: mixed with exact per-block bits.
        {
            const TReadPlan plan = ReadReadyResult(manager, key, 0, 6 * IntegrityUnitSize).ReadPlan;
            UNIT_ASSERT_EQUAL(plan.Kind, TReadPlan::Mixed);
            for (ui32 i = 0; i < 6; ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(plan.UsedBlocks.Get(i), (i == 2 || i == 3), "block " << i);
            }
        }

        // Sub-block boundary within the used region: still passthrough.
        {
            const TReadPlan plan = ReadReadyResult(manager, key, 3 * IntegrityUnitSize, IntegrityUnitSize).ReadPlan;
            UNIT_ASSERT_EQUAL(plan.Kind, TReadPlan::Passthrough);
        }

        // A missing integrity extent cannot produce a valid read snapshot.
        const auto missing = manager.PrepareRead(TKey{99, 99}, 0, IntegrityUnitSize);
        UNIT_ASSERT(missing.Warm);
        UNIT_ASSERT_EQUAL(missing.Warm->Status, TIntegrityManager::EOperationStatus::Corrupted);
        UNIT_ASSERT_VALUES_EQUAL(missing.Warm->ErrorReason, "integrity extent is unavailable");
    }

    Y_UNIT_TEST(AlignedWritesMarkExactBlocks) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 4, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 730;
        MakeReady(manager, key, 400, &nextIntegrityChunkIdx);

        manager.Write(key, 0, 2 * IntegrityUnitSize, {0xA, 0xB});
        CompleteWrites(manager, Drain(manager).Writes);

        const TReadPlan plan = ReadReadyResult(manager, key, 0, 3 * IntegrityUnitSize).ReadPlan;
        UNIT_ASSERT_EQUAL(plan.Kind, TReadPlan::Mixed);
        UNIT_ASSERT(plan.UsedBlocks.Get(0));
        UNIT_ASSERT(plan.UsedBlocks.Get(1));
        UNIT_ASSERT(!plan.UsedBlocks.Get(2));
    }

    Y_UNIT_TEST(ChecksumsAndDigests) {
        NIntegrityTest::TFixture manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        UNIT_ASSERT_VALUES_EQUAL(manager.BlocksPerExtent(), 3);

        const TKey key{.TabletId = 5, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 740;
        MakeReady(manager, key, 500, &nextIntegrityChunkIdx);
        const ui64 generation = manager.FindExtentRef(key)->VChunkGeneration;

        // Blocks 0..1 with checksums, plus one block in the second TIntegrityBlock.
        manager.Write(key, 0, 2 * IntegrityUnitSize, {0xA, 0xB});
        CompleteWrites(manager, Drain(manager).Writes);

        const ui32 farBlock = ChecksumsPerIntegrityBlock; // first block of digest index 1
        manager.Write(key, farBlock * IntegrityUnitSize, IntegrityUnitSize, {0xC});
        CompleteWrites(manager, Drain(manager).Writes);

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
        manager.Write(key, 0, IntegrityUnitSize, {0xD});
        CompleteWrites(manager, Drain(manager).Writes);

        ui64 expected = Contribution(generation, 0, 0xA) ^ Contribution(generation, 1, 0xB);
        UpdateRoot(expected, generation, 0, 0xA, 0xD);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0), expected);
        UNIT_ASSERT_VALUES_EQUAL(expected, Contribution(generation, 0, 0xD) ^ Contribution(generation, 1, 0xB));

        // Overwrite block 1 with its mandatory checksum.
        manager.Write(key, IntegrityUnitSize, IntegrityUnitSize, {0xE});
        CompleteWrites(manager, Drain(manager).Writes);

        UNIT_ASSERT(manager.GetBlockChecksum(key, 1, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xE);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0),
            Contribution(generation, 0, 0xD) ^ Contribution(generation, 1, 0xE));
        UNIT_ASSERT_EQUAL(ReadReadyResult(manager, key, IntegrityUnitSize, IntegrityUnitSize).ReadPlan.Kind,
            TReadPlan::Passthrough);
    }

    Y_UNIT_TEST(ExactIntegrityPairBoundaryAndBitmapTail) {
        NIntegrityTest::TFixture manager(PairBoundaryChunkSize, TestDDiskId, TestPDiskGuid);
        UNIT_ASSERT_VALUES_EQUAL(manager.DataBlocksInChunk(), ChecksumsPerIntegrityBlock + 1);
        UNIT_ASSERT_VALUES_EQUAL(manager.BlocksPerExtent(), 2);

        const TKey key{.TabletId = 6, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 745;
        MakeReady(manager, key, 550, &nextIntegrityChunkIdx);

        std::vector<ui64> firstPairChecksums(ChecksumsPerIntegrityBlock);
        for (ui32 i = 0; i < firstPairChecksums.size(); ++i) {
            firstPairChecksums[i] = 0x10000 + i;
        }
        auto firstOperation = manager.Write(
            key, 0, ChecksumsPerIntegrityBlock * IntegrityUnitSize, firstPairChecksums);
        TActions firstWrite = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(firstWrite.Writes.size(), 1);
        TIntegrityBlock firstImage;
        memcpy(&firstImage, firstWrite.Writes[0].Data.data(), sizeof(firstImage));
        UNIT_ASSERT_VALUES_EQUAL(firstImage.Header.ChecksumBlockIdx, 0);
        UNIT_ASSERT_VALUES_EQUAL(firstImage.Header.UsedBlocksBitmap[61], 0x0f);
        manager.CompleteWrite(firstWrite.Writes[0]);
        UNIT_ASSERT(firstOperation.GetResult());

        const ui64 finalChecksum = 0x20000;
        auto finalOperation = manager.Write(
            key, ChecksumsPerIntegrityBlock * IntegrityUnitSize, IntegrityUnitSize, {finalChecksum});
        TActions finalWrite = Drain(manager);
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
        manager.CompleteWrite(finalWrite.Writes[0]);
        UNIT_ASSERT(finalOperation.GetResult());

        const TKey crossingKey{.TabletId = 6, .VChunkIndex = 1};
        MakeReady(manager, crossingKey, 551, &nextIntegrityChunkIdx);
        std::vector<ui64> crossingChecksums(ChecksumsPerIntegrityBlock + 1);
        for (ui32 i = 0; i < crossingChecksums.size(); ++i) {
            crossingChecksums[i] = 0x30000 + i;
        }
        auto crossingOperation = manager.Write(
            crossingKey, 0, PairBoundaryChunkSize, crossingChecksums);
        TActions crossingWrites = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(crossingWrites.Writes.size(), 1);
        auto& crossingWrite = crossingWrites.Writes.front();
        UNIT_ASSERT_VALUES_EQUAL(crossingWrite.Data.size(), 4 * IntegrityUnitSize);
        for (ui32 pair = 0; pair < 2; ++pair) {
            TIntegrityBlock image;
            memcpy(&image, crossingWrite.Data.data() + pair * IntegrityPairSlots * IntegrityUnitSize, sizeof(image));
            UNIT_ASSERT_VALUES_EQUAL(image.Header.ChecksumBlockIdx, pair);
            UNIT_ASSERT_VALUES_EQUAL(image.Header.PairSequenceNumber, 2);
        }
        manager.CompleteWrite(crossingWrite);
        UNIT_ASSERT(crossingOperation.GetResult());
    }

    Y_UNIT_TEST(SparseBlockStateAllocation) {
        NIntegrityTest::TFixture manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        UNIT_ASSERT_VALUES_EQUAL(manager.BlocksPerExtent(), 3);

        const TKey key{.TabletId = 9, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 780;
        MakeReady(manager, key, 800, &nextIntegrityChunkIdx);
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);

        // A checksummed write allocates exactly one state, covering its whole TIntegrityBlock.
        manager.Write(key, 0, IntegrityUnitSize, {0xA});
        CompleteWrites(manager, Drain(manager).Writes);

        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 1);
        manager.Write(key, IntegrityUnitSize, IntegrityUnitSize, {0xB});
        CompleteWrites(manager, Drain(manager).Writes);

        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 1);

        // A write into the second TIntegrityBlock's range allocates the second state.
        manager.Write(key, ChecksumsPerIntegrityBlock * IntegrityUnitSize, IntegrityUnitSize, {0xC});
        CompleteWrites(manager, Drain(manager).Writes);

        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 2);
    }

    Y_UNIT_TEST(BlockStateLruEviction) {
        // Budget of exactly two cached states.
        NIntegrityTest::TFixture manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid,
            2 * NIntegrityTest::TFixture::BlockStateApproxBytes);
        UNIT_ASSERT_VALUES_EQUAL(manager.MaxCachedBlockStates(), 2);
        UNIT_ASSERT_VALUES_EQUAL(manager.BlocksPerExtent(), 3);

        const TKey key{.TabletId = 10, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 790;
        MakeReady(manager, key, 810, &nextIntegrityChunkIdx);
        const ui64 generation = manager.FindExtentRef(key)->VChunkGeneration;

        const ui32 block1 = ChecksumsPerIntegrityBlock;     // first block of TIntegrityBlock 1
        const ui32 block2 = 2 * ChecksumsPerIntegrityBlock; // first block of TIntegrityBlock 2

        auto persist = [&](TKey target, ui32 block, ui64 checksum) {
            manager.Write(target, block * IntegrityUnitSize, IntegrityUnitSize, {checksum});
            TActions actions = Drain(manager);
            UNIT_ASSERT_VALUES_EQUAL(actions.Writes.size(), 1);
            manager.CompleteWrite(actions.Writes[0]);

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

        // An evicted checksum image is loaded before the read publishes its exact plan.
        auto read = PreparePendingRead(manager, key, 0, IntegrityUnitSize);
        UNIT_ASSERT(!read.GetResult());
        auto loads = Drain(manager).Reads;
        UNIT_ASSERT_VALUES_EQUAL(loads.size(), 1);
        const auto ref = *manager.FindExtentRef(key);
        const auto chunkGeneration = manager.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        manager.CompleteRead(loads.front(), MakeIntegrityPair(
            MakeIntegrityBlock(key, ref, chunkGeneration, 0, 2, {{0, 0xA}}),
            MakeIntegrityBlock(key, ref, chunkGeneration, 0, 1, {})));
        UNIT_ASSERT(read.GetResult());
        UNIT_ASSERT_EQUAL(read.GetResult()->Status, TIntegrityManager::EOperationStatus::Ok);
        UNIT_ASSERT_EQUAL(read.GetResult()->ReadPlan.Kind, TReadPlan::Passthrough);
        AssertChecksums(read.GetResult()->Checksums, (std::vector<ui64>{0xA}));
    }

    Y_UNIT_TEST(ChecksumReadHitRefreshesBlockStateLru) {
        NIntegrityTest::TFixture manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid,
            2 * NIntegrityTest::TFixture::BlockStateApproxBytes);
        const TKey key{.TabletId = 10, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 790;
        MakeReady(manager, key, 810, &nextIntegrityChunkIdx);

        auto persist = [&](ui32 pairIdx, ui64 checksum) {
            auto operationId = manager.Write(key,
                pairIdx * ChecksumsPerIntegrityBlock * IntegrityUnitSize,
                IntegrityUnitSize, {checksum});
            TActions actions = Drain(manager);
            UNIT_ASSERT_VALUES_EQUAL(actions.Writes.size(), 1);
            manager.CompleteWrite(actions.Writes[0]);
            UNIT_ASSERT(operationId.GetResult());
        };
        auto readFirstBlock = [&] {
            const auto result = ReadReadyResult(manager, key, 0, IntegrityUnitSize);
            UNIT_ASSERT(Drain(manager).Reads.empty());
            UNIT_ASSERT_EQUAL(result.Status, NIntegrityTest::TFixture::EOperationStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(result.Checksums.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.Checksums[0], 0xA);
        };

        persist(0, 0xA);
        persist(1, 0xB);
        readFirstBlock();

        // A third metadata block must evict the unread second block, preserving the read hit.
        persist(2, 0xC);
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 2);
        readFirstBlock();
        ui64 checksum = 0;
        UNIT_ASSERT(!manager.GetBlockChecksum(key, ChecksumsPerIntegrityBlock, &checksum));
        UNIT_ASSERT(manager.GetBlockChecksum(key, 2 * ChecksumsPerIntegrityBlock, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xC);
    }

    void TestReadModifyWriteAfterEvictionPreservesUntouchedChecksums(bool cacheDisabled) {
        NIntegrityTest::TFixture manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid,
            cacheDisabled ? 0 : NIntegrityTest::TFixture::BlockStateApproxBytes);
        const TKey key{.TabletId = 17, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 796;
        MakeReady(manager, key, 830, &nextIntegrityChunkIdx);
        const auto ref = *manager.FindExtentRef(key);
        const ui64 chunkGeneration =
            manager.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        const ui64 generation = ref.VChunkGeneration;

        auto persist = [&](ui32 block, ui32 blocks, const std::vector<ui64>& checksums) {
            auto operationId = manager.Write(
                key, block * IntegrityUnitSize, blocks * IntegrityUnitSize, checksums);
            TActions actions = Drain(manager);
            UNIT_ASSERT_VALUES_EQUAL(actions.Reads.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(actions.Writes.size(), 1);
            manager.CompleteWrite(actions.Writes[0]);
            UNIT_ASSERT(operationId.GetResult());
        };

        persist(0, 2, {0xAA, 0xBB});
        persist(ChecksumsPerIntegrityBlock, 1, {0xCC});
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), cacheDisabled ? 0 : 1);
        ui64 checksum = 0;
        UNIT_ASSERT(!manager.GetBlockChecksum(key, 0, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0),
            Contribution(generation, 0, 0xAA) ^ Contribution(generation, 1, 0xBB));

        auto operationId = manager.Write(key, 0, IntegrityUnitSize, {0xDD});
        TActions read = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(read.Reads.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(read.Writes.size(), 0);
        manager.CompleteRead(read.Reads[0], MakeIntegrityPair(
            MakeIntegrityBlock(key, ref, chunkGeneration, 0, 2, {{0, 0xAA}, {1, 0xBB}}),
            MakeIntegrityBlock(key, ref, chunkGeneration, 0, 1, {})));

        TActions write = Drain(manager);
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

        manager.CompleteWrite(write.Writes[0]);
        const auto result = ResultOf(operationId);
        UNIT_ASSERT_EQUAL(result.Status, NIntegrityTest::TFixture::EOperationStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), cacheDisabled ? 0 : 1);
        if (cacheDisabled) {
            UNIT_ASSERT(!manager.GetBlockChecksum(key, 0, &checksum));
            UNIT_ASSERT(!manager.GetBlockChecksum(key, 1, &checksum));
        } else {
            UNIT_ASSERT(manager.GetBlockChecksum(key, 0, &checksum));
            UNIT_ASSERT_VALUES_EQUAL(checksum, 0xDD);
            UNIT_ASSERT(manager.GetBlockChecksum(key, 1, &checksum));
            UNIT_ASSERT_VALUES_EQUAL(checksum, 0xBB);
        }
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0), expectedDigest);
    }

    Y_UNIT_TEST(ReadModifyWriteAfterEvictionPreservesUntouchedChecksums) {
        TestReadModifyWriteAfterEvictionPreservesUntouchedChecksums(false);
    }

    Y_UNIT_TEST(ReadModifyWriteAfterEvictionPreservesUntouchedChecksumsWithoutCache) {
        TestReadModifyWriteAfterEvictionPreservesUntouchedChecksums(true);
    }

    Y_UNIT_TEST(BlockStatesDroppedOnDelete) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 11, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 795;
        MakeReady(manager, key, 820, &nextIntegrityChunkIdx);

        manager.Write(key, 0, IntegrityUnitSize, {0xA});
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 1);

        // Write submits the pair image; deletion stays blocked until it retires.
        UNIT_ASSERT(manager.HasInFlightOperationsForTablet(11));
        CompleteWrites(manager, Drain(manager).Writes);
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(11));
        manager.PrepareTabletChunksDeletion(11);
        manager.CommitTabletChunksDeletion(11);
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);

        // Reallocation starts clean: no stale checksums or bitmap.
        manager.StartExtent(key, 821);
        CompleteWrites(manager, Drain(manager).Writes);
        UNIT_ASSERT(manager.IsExtentReady(key));
        UNIT_ASSERT_EQUAL(ReadReadyResult(manager, key, 0, IntegrityUnitSize).ReadPlan.Kind, TReadPlan::AllZero);
        ui64 checksum = 0;
        UNIT_ASSERT(!manager.GetBlockChecksum(key, 0, &checksum));
    }

    Y_UNIT_TEST(DeleteAndReuse) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 7, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 750;

        MakeReady(manager, key, 600, &nextIntegrityChunkIdx);
        manager.Write(key, 0, IntegrityUnitSize, {0xA});
        {
            const auto* ref = manager.FindExtentRef(key);
            UNIT_ASSERT_VALUES_EQUAL(ref->ExtentSlot, 0);
            UNIT_ASSERT_VALUES_EQUAL(ref->VChunkGeneration, 1);
        }

        // Write submits the pair image; deletion stays blocked until it retires.
        UNIT_ASSERT(manager.HasInFlightOperationsForTablet(7));
        CompleteWrites(manager, Drain(manager).Writes);
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(7));
        manager.PrepareTabletChunksDeletion(7);
        manager.CommitTabletChunksDeletion(7);
        UNIT_ASSERT(!manager.IsExtentReady(key));
        UNIT_ASSERT(!manager.FindExtentRef(key));
        // The deleted extent cannot supply a read snapshot.
        const auto deleted = manager.PrepareRead(key, 0, IntegrityUnitSize);
        UNIT_ASSERT(deleted.Warm);
        UNIT_ASSERT_EQUAL(deleted.Warm->Status, TIntegrityManager::EOperationStatus::Corrupted);
        UNIT_ASSERT_VALUES_EQUAL(deleted.Warm->ErrorReason, "integrity extent is unavailable");

        // Reallocation reuses the freed slot without a new integrity chunk and bumps the generation
        // (VChunk and integrity chunk generations share one counter: 1 = first VChunk allocation,
        // 2 = the integrity chunk, so the reallocation draws 3).
        manager.StartExtent(key, 601);
        TActions log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.Allocations.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 1); // extent format only, chunk header already written
        const auto* ref = manager.FindExtentRef(key);
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->IntegrityChunkIdx, 750);
        UNIT_ASSERT_VALUES_EQUAL(ref->ExtentSlot, 0);
        UNIT_ASSERT_VALUES_EQUAL(ref->VChunkGeneration, 3);
        CheckExtentFormat(manager, log.Writes[0], key, *ref, 2);

        CompleteWrites(manager, log.Writes);
        UNIT_ASSERT(manager.IsExtentReady(key));
        // Old bitmap must not leak into the reallocated chunk.
        UNIT_ASSERT_EQUAL(ReadReadyResult(manager, key, 0, IntegrityUnitSize).ReadPlan.Kind, TReadPlan::AllZero);
    }

    Y_UNIT_TEST(PreparedDeletionQuarantinesSlotsUntilCommit) {
        NIntegrityTest::TFixture manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk
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
        manager.StartExtent(pendingKey, 660);
        TActions pendingLog = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(pendingLog.Writes.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(pendingLog.Allocations.size(), 1);
        UNIT_ASSERT(!manager.FindExtentRef(pendingKey));

        // Only the durable commit releases the old slots. Reclamation gives slot 0 to the pending
        // extent before considering the integrity chunk for release.
        manager.CommitTabletChunksDeletion(40);
        UNIT_ASSERT(manager.TakeReleasableIntegrityChunks().empty());
        TActions formatLog = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(formatLog.Writes.size(), 1);
        const auto* ref = manager.FindExtentRef(pendingKey);
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->IntegrityChunkIdx, 755);
        UNIT_ASSERT_VALUES_EQUAL(ref->ExtentSlot, 0);
        UNIT_ASSERT_VALUES_EQUAL(ref->VChunkGeneration, 6);
        CompleteWrites(manager, formatLog.Writes);
        UNIT_ASSERT(manager.IsExtentReady(pendingKey));
    }

    Y_UNIT_TEST(PreparedDeletionWaitsForDurabilityAfterFormattingCompletes) {
        NIntegrityTest::TFixture manager(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 42, .VChunkIndex = 0};

        manager.StartExtent(key, 670);
        auto allocation = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(allocation.Allocations.size(), 1);
        manager.CompleteAllocation(allocation.Allocations.front(), 757);
        TActions log = Drain(manager);
        std::vector<TWriteIo> headers;
        std::vector<TWriteIo> formatLogWrites;
        SplitWrites(log.Writes, &headers, &formatLogWrites);
        UNIT_ASSERT_VALUES_EQUAL(headers.size(), NIntegrityTest::TFixture::ChunkHeaderReplicaCount);
        UNIT_ASSERT_VALUES_EQUAL(formatLogWrites.size(), 1);
        CompleteWrites(manager, headers);

        manager.PrepareTabletChunksDeletion(42);
        // The physical write may finish first, but it must neither publish readiness nor release
        // its slot/chunk before the deletion snapshot is acknowledged.
        CompleteWrites(manager, formatLogWrites);
        UNIT_ASSERT(manager.TakeReleasableIntegrityChunks().empty());

        manager.CommitTabletChunksDeletion(42);
        const auto released = manager.TakeReleasableIntegrityChunks();
        UNIT_ASSERT_VALUES_EQUAL(released.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(released[0], 757);
    }

    Y_UNIT_TEST(DeleteWhileFormatInFlight) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 8, .VChunkIndex = 0};

        manager.StartExtent(key, 700);
        auto allocation = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(allocation.Allocations.size(), 1);
        manager.CompleteAllocation(allocation.Allocations.front(), 760);
        TActions log = Drain(manager);
        std::vector<TWriteIo> headers;
        std::vector<TWriteIo> formatLog;
        SplitWrites(log.Writes, &headers, &formatLog);
        UNIT_ASSERT_VALUES_EQUAL(formatLog.size(), 1);
        CompleteWrites(manager, headers);

        // The tablet's chunks are deleted while the extent format write is in flight.
        manager.PrepareTabletChunksDeletion(8);
        manager.CommitTabletChunksDeletion(8);
        CompleteWrites(manager, formatLog);
        UNIT_ASSERT(!manager.IsExtentReady(key));

        // The slot is reusable afterwards.
        manager.StartExtent(key, 701);
        TActions reuseLog = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(reuseLog.Allocations.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(reuseLog.Writes.size(), 1);
        CompleteWrites(manager, reuseLog.Writes);
        UNIT_ASSERT(manager.IsExtentReady(key));
        UNIT_ASSERT_VALUES_EQUAL(manager.FindExtentRef(key)->VChunkGeneration, 3);
    }

    Y_UNIT_TEST(StaleFormatCompletionDoesNotCompleteReusedExtent) {
        NIntegrityTest::TFixture manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk
        const TKey key{.TabletId = 12, .VChunkIndex = 0};

        manager.StartExtent(key, 900);
        auto allocation = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(allocation.Allocations.size(), 1);
        manager.CompleteAllocation(allocation.Allocations.front(), 765);
        TActions staleDrain = Drain(manager);
        std::vector<TWriteIo> staleHeaders;
        std::vector<TWriteIo> staleFormatWrites;
        SplitWrites(staleDrain.Writes, &staleHeaders, &staleFormatWrites);
        UNIT_ASSERT_VALUES_EQUAL(staleFormatWrites.size(), 1);
        CompleteWrites(manager, staleHeaders);

        // Free the extent while its format write is in flight and immediately reallocate the key.
        manager.PrepareTabletChunksDeletion(12);
        manager.CommitTabletChunksDeletion(12);
        manager.StartExtent(key, 901);
        TActions newFormat = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(newFormat.Allocations.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(newFormat.Writes.size(), 1);

        // Slot 0 is withheld until the stale write settles: the new extent gets slot 1.
        const auto* ref = manager.FindExtentRef(key);
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->ExtentSlot, 1);
        UNIT_ASSERT_VALUES_EQUAL(ref->VChunkGeneration, 3);

        // The stale completion must not mark the reallocated extent ready.
        CompleteWrites(manager, staleFormatWrites);
        UNIT_ASSERT(!manager.IsExtentReady(key));

        // Only the extent's own format write completes it.
        CompleteWrites(manager, newFormat.Writes);
        UNIT_ASSERT(manager.IsExtentReady(key));

        // The stale write has settled, so slot 0 is reusable by the next allocation.
        const TKey key2{.TabletId = 12, .VChunkIndex = 1};
        manager.StartExtent(key2, 902);
        TActions log2 = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log2.Allocations.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(log2.Writes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(manager.FindExtentRef(key2)->ExtentSlot, 0);
        CompleteWrites(manager, log2.Writes);
        UNIT_ASSERT(manager.IsExtentReady(key2));
    }

    Y_UNIT_TEST(PendingExtentWaitsForOrphanedSlot) {
        // All four slots taken, one extent freed mid-format: a pending extent must wait for the
        // orphaned write to settle rather than reuse the slot early, and must then be assigned
        // to it (no new chunk needed).
        NIntegrityTest::TFixture manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk
        TChunkIdx nextIntegrityChunkIdx = 768;
        for (ui32 i = 0; i < 3; ++i) {
            MakeReady(manager, TKey{13, i}, 910 + i, &nextIntegrityChunkIdx);
        }

        // The fourth extent (a different tablet) occupies slot 3 with its format write in flight.
        const TKey inFlightKey{.TabletId = 15, .VChunkIndex = 0};
        manager.StartExtent(inFlightKey, 913);
        TActions staleFormat = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(staleFormat.Writes.size(), 1);

        // Free it mid-format. A new allocation finds no free slot (slot 3 is withheld) and no
        // format write can be issued yet - but capacity accounting may request a chunk.
        manager.PrepareTabletChunksDeletion(15);
        manager.CommitTabletChunksDeletion(15);
        const TKey pendingKey{.TabletId = 14, .VChunkIndex = 0};
        manager.StartExtent(pendingKey, 920);
        TActions log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 0);
        UNIT_ASSERT(!manager.IsExtentReady(pendingKey));

        // The orphaned write settles: slot 3 is released and the pending extent takes it.
        CompleteWrites(manager, staleFormat.Writes);
        TActions formatLog = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(formatLog.Writes.size(), 1);
        const auto* ref = manager.FindExtentRef(pendingKey);
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->ExtentSlot, 3);
        CompleteWrites(manager, formatLog.Writes);
        UNIT_ASSERT(manager.IsExtentReady(pendingKey));
    }

    Y_UNIT_TEST(SnapshotRoundTrip) {
        NIntegrityTest::TFixture manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk
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
        NIntegrityTest::TFixture restored(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        restored.ApplyMappingSnapshot(snapshot);
        UNIT_ASSERT(restored.Submissions.empty());
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
        // a durable RMW and makes the bitmap/checksum state exact. A reader joins the write and
        // cannot publish a plan before the updated image is durable.
        auto operationId = restored.Write(keys[0], 0, IntegrityUnitSize, {0xA});
        TActions read = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(read.Reads.size(), 1);
        auto restoredRead = PreparePendingRead(restored, keys[0], 0, TinyChunkSize);
        UNIT_ASSERT(!restoredRead.GetResult());
        UNIT_ASSERT(restored.Submissions.empty());
        const auto restoredRef = *restored.FindExtentRef(keys[0]);
        const ui64 restoredChunkGeneration =
            restored.GetIntegrityChunkGeneration(restoredRef.IntegrityChunkIdx);
        restored.CompleteRead(read.Reads[0], MakeIntegrityPair(
            MakeIntegrityBlock(keys[0], restoredRef, restoredChunkGeneration, 0, 0, {}),
            MakeIntegrityBlock(keys[0], restoredRef, restoredChunkGeneration, 0, 1, {})));
        TActions write = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(write.Writes.size(), 1);
        UNIT_ASSERT(!restoredRead.GetResult());
        restored.CompleteWrite(write.Writes[0]);
        UNIT_ASSERT(operationId.GetResult());
        ui64 checksum = 0;
        UNIT_ASSERT(restored.GetBlockChecksum(keys[0], 0, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 0xA);
        UNIT_ASSERT(restoredRead.GetResult());
        UNIT_ASSERT_EQUAL(restoredRead.GetResult()->Status, TIntegrityManager::EOperationStatus::Ok);
        const auto& restoredPlan = restoredRead.GetResult()->ReadPlan;
        UNIT_ASSERT_EQUAL(restoredPlan.Kind, TReadPlan::Mixed);
        for (ui32 block = 0; block < TinyChunkSize / IntegrityUnitSize; ++block) {
            UNIT_ASSERT_VALUES_EQUAL(restoredPlan.UsedBlocks.Get(block), block == 0);
        }

        // The restored manager keeps allocating into the remaining free slots of known chunks
        // without requesting new integrity chunks (6 used out of 8 -> 2 slots left).
        for (ui32 i = 0; i < 2; ++i) {
            const TKey key{12, i};
            restored.StartExtent(key, 900 + i);
            TActions log = Drain(restored);
            UNIT_ASSERT_VALUES_EQUAL(log.Allocations.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 1);
            CompleteWrites(restored, log.Writes);
            UNIT_ASSERT(restored.IsExtentReady(key));
        }
        // And the ninth extent overflows into a new chunk.
        restored.StartExtent(TKey{12, 2}, 902);
        UNIT_ASSERT_VALUES_EQUAL(Drain(restored).Allocations.size(), 1);

        // Deleting a restored tablet and reallocating bumps VChunkGeneration past the persisted
        // watermark (generations 9..11 were drawn by tablet 12 above).
        restored.PrepareTabletChunksDeletion(10);
        restored.CommitTabletChunksDeletion(10);
        restored.StartExtent(TKey{10, 0}, 950);
        Drain(restored);
        const auto* ref = restored.FindExtentRef(TKey{10, 0});
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->VChunkGeneration, 12);
        UNIT_ASSERT(ref->VChunkGeneration > snapshot.GenerationCounter);
    }

    Y_UNIT_TEST(SnapshotExcludesFormattingChunks) {
        NIntegrityTest::TFixture manager(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 16, .VChunkIndex = 0};

        manager.StartExtent(key, 960);
        auto allocation = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(allocation.Allocations.size(), 1);
        manager.CompleteAllocation(allocation.Allocations.front(), 775);
        TActions formatting = Drain(manager);

        // ApplyMappingSnapshot restores every listed chunk as Ready, so a chunk whose headers are
        // still in flight must not be exported.
        const auto inFlightSnapshot = manager.SnapshotMapping();
        UNIT_ASSERT_VALUES_EQUAL(inFlightSnapshot.IntegrityChunks.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(inFlightSnapshot.Extents.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(inFlightSnapshot.GenerationCounter, 2);

        CompleteWrites(manager, formatting.Writes);
        const auto readySnapshot = manager.SnapshotMapping();
        UNIT_ASSERT_VALUES_EQUAL(readySnapshot.IntegrityChunks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(readySnapshot.IntegrityChunks[0].ChunkIdx, 775);
        UNIT_ASSERT_VALUES_EQUAL(readySnapshot.Extents.size(), 1);
    }

    Y_UNIT_TEST(ReleasableIntegrityChunks) {
        NIntegrityTest::TFixture manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk
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
        UNIT_ASSERT(manager.Submissions.empty());

        // The next allocation requests a fresh integrity chunk again.
        manager.StartExtent(TKey{30, 0}, 1010);
        UNIT_ASSERT_VALUES_EQUAL(Drain(manager).Allocations.size(), 1);
    }

    Y_UNIT_TEST(ReleasableChunksServePendingExtentsFirst) {
        NIntegrityTest::TFixture manager(TinyChunkSize, TestDDiskId, TestPDiskGuid); // 4 extents per chunk
        TChunkIdx nextIntegrityChunkIdx = 860;
        for (ui32 i = 0; i < 4; ++i) {
            MakeReady(manager, TKey{31, i}, 1100 + i, &nextIntegrityChunkIdx);
        }

        // A fifth extent (another tablet) goes pending: all slots taken, a chunk allocation is
        // queued - and genuinely needed, so it must not be cancellable.
        manager.StartExtent(TKey{32, 0}, 1104);
        TActions log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.Allocations.size(), 1);
        const auto allocationToken = log.Allocations.front();
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 0);
        UNIT_ASSERT(manager.Returned.empty());

        // Deleting the first tablet frees all four slots, but the pending extent takes one before
        // releasability is decided: the chunk stays owned and the extent's format write goes out.
        manager.PrepareTabletChunksDeletion(31);
        manager.CommitTabletChunksDeletion(31);
        UNIT_ASSERT(manager.TakeReleasableIntegrityChunks().empty());
        log = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 1);
        UNIT_ASSERT(manager.FindExtentRef(TKey{32, 0}));

        // With three slots left free and no pending extents, the queued allocation is now excess.
        manager.CompleteAllocation(allocationToken, 9999);
        UNIT_ASSERT_VALUES_EQUAL(manager.Returned, std::vector<TChunkIdx>{9999});

        CompleteWrites(manager, log.Writes);
        UNIT_ASSERT(manager.IsExtentReady(TKey{32, 0}));
    }

    Y_UNIT_TEST(OrphanedFormatWriteBlocksChunkRelease) {
        NIntegrityTest::TFixture manager(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 33, .VChunkIndex = 0};

        // Bring one chunk to Ready with the extent's format write still in flight.
        manager.StartExtent(key, 1200);
        auto allocation = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(allocation.Allocations.size(), 1);
        manager.CompleteAllocation(allocation.Allocations.front(), 870);
        TActions formatDrain = Drain(manager);
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
        NIntegrityTest::TFixture manager(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 34, .VChunkIndex = 0};

        manager.StartExtent(key, 1300);
        auto allocation = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(allocation.Allocations.size(), 1);
        manager.CompleteAllocation(allocation.Allocations.front(), 880);
        TActions headerLog = Drain(manager);
        UNIT_ASSERT(headerLog.Writes.size() >= 1);

        // The extent is freed while the chunk is still writing its headers (and possibly the
        // extent format): the chunk is not releasable until every in-flight write settles.
        manager.PrepareTabletChunksDeletion(34);
        manager.CommitTabletChunksDeletion(34);
        UNIT_ASSERT(manager.TakeReleasableIntegrityChunks().empty());

        CompleteWrites(manager, headerLog.Writes);
        while (!manager.Submissions.empty()) {
            CompleteWrites(manager, Drain(manager).Writes);
        }
        const auto released = manager.TakeReleasableIntegrityChunks();
        UNIT_ASSERT_VALUES_EQUAL(released.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(released[0], 880);
    }

    Y_UNIT_TEST(RestoredChunksAreReadyAndHostNewExtents) {
        NIntegrityTest::TFixture manager(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 35, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 890;
        MakeReady(manager, key, 1400, &nextIntegrityChunkIdx);

        const auto snapshot = manager.SnapshotMapping();
        UNIT_ASSERT_VALUES_EQUAL(snapshot.IntegrityChunks.size(), 1);

        NIntegrityTest::TFixture restored(TinyChunkSize, TestDDiskId, TestPDiskGuid);
        restored.ApplyMappingSnapshot(snapshot);
        UNIT_ASSERT(restored.Submissions.empty());
        UNIT_ASSERT(restored.IsExtentReady(key));
        UNIT_ASSERT(restored.IsIntegrityChunkFormatted(890));

        const TKey key2{.TabletId = 35, .VChunkIndex = 1};
        restored.StartExtent(key2, 1401);
        UNIT_ASSERT(restored.FindExtentRef(key2));
        TActions log = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(log.Allocations.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(log.Writes.size(), 1);
        CompleteWrites(restored, log.Writes);
        UNIT_ASSERT(restored.IsExtentReady(key2));
    }

    Y_UNIT_TEST(GenerationWatermarkSurvivesRestart) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
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

        NIntegrityTest::TFixture restored(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        restored.ApplyMappingSnapshot(snapshot);

        // Reallocating the same key must draw a fresh generation, not reuse 1.
        restored.StartExtent(key, 1501);
        TActions log = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(log.Allocations.size(), 0); // the restored chunk has free slots
        const auto* ref = restored.FindExtentRef(key);
        UNIT_ASSERT(ref);
        UNIT_ASSERT_VALUES_EQUAL(ref->VChunkGeneration, 3);
    }

    Y_UNIT_TEST(ChecksumPersistenceSealsAndPingPongs) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 40, .VChunkIndex = 7};
        TChunkIdx nextIntegrityChunkIdx = 900;
        MakeReady(manager, key, 1600, &nextIntegrityChunkIdx);
        const auto ref = *manager.FindExtentRef(key);

        auto firstOperation = manager.Write(
            key, 0, 2 * IntegrityUnitSize, {0x111, 0x222});
        TActions first = Drain(manager);
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

        manager.CompleteWrite(first.Writes[0]);
        auto completion = ResultOf(firstOperation);
        UNIT_ASSERT_EQUAL(completion.Status, NIntegrityTest::TFixture::EOperationStatus::Ok);

        auto secondOperation = manager.Write(
            key, IntegrityUnitSize, IntegrityUnitSize, {0x333});
        TActions second = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(second.Writes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(second.Writes[0].OffsetInBytes,
            manager.ExtentOffset(ref.ExtentSlot) + IntegrityUnitSize);
        TIntegrityBlock secondImage;
        memcpy(&secondImage, second.Writes[0].Data.data(), sizeof(secondImage));
        UNIT_ASSERT_VALUES_EQUAL(secondImage.Header.PairSequenceNumber, 3);
        manager.CompleteWrite(second.Writes[0]);
        UNIT_ASSERT(secondOperation.GetResult());

        completion = ReadReadyResult(manager, key, 0, 2 * IntegrityUnitSize);
        UNIT_ASSERT_VALUES_EQUAL(completion.Checksums.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(completion.Checksums[0], 0x111);
        UNIT_ASSERT_VALUES_EQUAL(completion.Checksums[1], 0x333);
    }

    Y_UNIT_TEST(WarmAndColdMetadataChainsDoNotLaunchCoroutines) {
        for (const bool cold : {false, true}) {
            NIntegrityTest::TFixture original(SmallChunkSize, TestDDiskId, TestPDiskGuid);
            const TKey key{42, 3};
            TChunkIdx next = 920;
            MakeReady(original, key, 1800, &next);
            NIntegrityTest::TFixture restored(SmallChunkSize, TestDDiskId, TestPDiskGuid);
            restored.ApplyMappingSnapshot(original.SnapshotMapping());
            auto& manager = cold ? restored : original;
            TIntegrityManager::TWriteOperation first, second;
            {
                first = manager.Write(key, 0, IntegrityUnitSize, {0x10});
                second = manager.PrepareWrite(key, IntegrityUnitSize, IntegrityUnitSize);
                UNIT_ASSERT(!second.IsReady());
                manager.NotifyCompleted();
            }
            auto actions = Drain(manager);
            if (cold) {
                UNIT_ASSERT_VALUES_EQUAL(actions.Reads.size(), 1);
                UNIT_ASSERT(actions.Writes.empty());
                const auto ref = *manager.FindExtentRef(key);
                const auto generation = manager.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
                manager.CompleteRead(actions.Reads.front(), MakeIntegrityPair(
                    MakeIntegrityBlock(key, ref, generation, 0, 0, {}),
                    MakeIntegrityBlock(key, ref, generation, 0, 1, {})));
                actions = Drain(manager);
            }
            UNIT_ASSERT_VALUES_EQUAL(actions.Writes.size(), 1);
            const auto firstImage = actions.Writes.front().Data;
            TIntegrityBlock image;
            memcpy(&image, firstImage.data(), sizeof(image));
            UNIT_ASSERT_VALUES_EQUAL(image.Header.UsedBlocksBitmap[0], 1);
            const auto firstOffset = actions.Writes.front().OffsetInBytes;
            UNIT_ASSERT(!first.GetResult());
            UNIT_ASSERT(!second.GetResult());
            manager.CompleteWrite(actions.Writes.front());
            UNIT_ASSERT(first.GetResult());
            UNIT_ASSERT(!second.GetResult());
            UNIT_ASSERT(second.IsReady());
            manager.SubmitWrite(second, std::vector<ui64>{0x20});
            actions = Drain(manager);
            UNIT_ASSERT_VALUES_EQUAL(actions.Writes.size(), 1);
            UNIT_ASSERT_VALUES_UNEQUAL(actions.Writes.front().OffsetInBytes, firstOffset);
            memcpy(&image, firstImage.data(), sizeof(image));
            UNIT_ASSERT_VALUES_EQUAL(image.Header.UsedBlocksBitmap[0], 1);
            memcpy(&image, actions.Writes.front().Data.data(), sizeof(image));
            UNIT_ASSERT_VALUES_EQUAL(image.Header.UsedBlocksBitmap[0], 3);
            manager.CompleteWrite(actions.Writes.front());
            UNIT_ASSERT(second.GetResult());
            UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
        }
    }

    Y_UNIT_TEST(PairWritesSerializeAndPreserveEveryUpdate) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{41, 0};
        TChunkIdx next = 910;
        MakeReady(manager, key, 1700, &next);
        auto first = manager.Write(key, 0, IntegrityUnitSize, {0xA});
        auto writes = Drain(manager).Writes;
        UNIT_ASSERT_VALUES_EQUAL(writes.size(), 1);
        auto second = manager.PrepareWrite(key, IntegrityUnitSize, IntegrityUnitSize);
        auto third = manager.PrepareWrite(key, 2 * IntegrityUnitSize, IntegrityUnitSize);
        UNIT_ASSERT(!second.IsReady() && !third.IsReady());
        UNIT_ASSERT(Drain(manager).Writes.empty());
        manager.CompleteWrite(writes.front());
        UNIT_ASSERT(first.GetResult());
        UNIT_ASSERT(second.IsReady() && !third.IsReady());
        manager.SubmitWrite(second, std::vector<ui64>{0xB});
        writes = Drain(manager).Writes;
        UNIT_ASSERT_VALUES_EQUAL(writes.size(), 1);
        TIntegrityBlock image;
        memcpy(&image, writes.front().Data.data(), sizeof(image));
        UNIT_ASSERT_VALUES_EQUAL(image.Header.PairSequenceNumber, 3);
        UNIT_ASSERT_VALUES_EQUAL(image.Header.UsedBlocksBitmap[0], 3);
        manager.CompleteWrite(writes.front());
        UNIT_ASSERT(second.GetResult());
        UNIT_ASSERT(third.IsReady());
        manager.SubmitWrite(third, std::vector<ui64>{0xC});
        writes = Drain(manager).Writes;
        UNIT_ASSERT_VALUES_EQUAL(writes.size(), 1);
        memcpy(&image, writes.front().Data.data(), sizeof(image));
        UNIT_ASSERT_VALUES_EQUAL(image.Header.PairSequenceNumber, 4);
        UNIT_ASSERT_VALUES_EQUAL(image.Header.UsedBlocksBitmap[0], 7);
        for (ui32 block = 0; block < 3; ++block) {
            UNIT_ASSERT_VALUES_EQUAL(UnsealBlockChecksum(image.Checksums[block], TestDDiskId,
                TestPDiskGuid, key.TabletId, key.VChunkIndex, block), 0xA + block);
        }
        manager.CompleteWrite(writes.front());
        UNIT_ASSERT(third.GetResult());
        UNIT_ASSERT(Drain(manager).Writes.empty());
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
    }

    Y_UNIT_TEST(SynchronousFollowupCanDeleteAndRecreateExtent) {
        for (const bool recreate : {false, true}) {
            NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
            const TKey key{41, 0};
            TChunkIdx next = 910;
            MakeReady(manager, key, 1700, &next);
            const ui64 oldGeneration = manager.FindExtentRef(key)->VChunkGeneration;
            auto first = manager.Write(key, 0, IntegrityUnitSize, {0xA});
            auto second = manager.PrepareWrite(key, IntegrityUnitSize, IntegrityUnitSize);
            UNIT_ASSERT(!second.IsReady());
            TActorWaiters waiters;
            size_t completed = 0;
            waiters.Actor.RunAsync([&]() -> NActors::async<void> {
                while (!second.IsReady()) {
                    co_await second.WaitChanged();
                }
                manager.SubmitWrite(second, std::vector<ui64>{0xB});
                while (!second.GetResult()) {
                    co_await second.WaitChanged();
                }
                UNIT_ASSERT_EQUAL(second.GetResult()->Status, TIntegrityManager::EOperationStatus::Ok);
                manager.PrepareTabletChunksDeletion(key.TabletId);
                manager.CommitTabletChunksDeletion(key.TabletId);
                if (recreate) {
                    manager.StartExtent(key, 1701);
                }
                ++completed;
            });
            waiters.Actor.RunSync([&] {
                CompleteWrites(manager, Drain(manager).Writes);
            });
            UNIT_ASSERT(first.GetResult());
            UNIT_ASSERT_VALUES_EQUAL(completed, 0);
            waiters.Actor.RunSync([&] {
                CompleteWrites(manager, Drain(manager).Writes);
            });
            UNIT_ASSERT_VALUES_EQUAL(completed, 1);
            UNIT_ASSERT(second.GetResult());
            const auto* ref = manager.FindExtentRef(key);
            if (recreate) {
                UNIT_ASSERT(ref && ref->VChunkGeneration != oldGeneration);
                CompleteWrites(manager, Drain(manager).Writes);
                UNIT_ASSERT(manager.IsExtentReady(key));
            } else {
                UNIT_ASSERT(!ref);
            }
        }
    }

    Y_UNIT_TEST(NativeCoroutineCanPerformManySuccessiveWrites) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{41, 0};
        TChunkIdx next = 910;
        MakeReady(manager, key, 1700, &next);
        TActorWaiters waiters;
        size_t completed = 0;
        waiters.Actor.RunAsync([&]() -> NActors::async<void> {
            for (ui64 checksum = 0; checksum < 128; ++checksum) {
                auto write = manager.Write(key, 0, IntegrityUnitSize, {checksum});
                while (!write.GetResult()) {
                    co_await write.WaitChanged();
                }
                UNIT_ASSERT_EQUAL(write.GetResult()->Status, TIntegrityManager::EOperationStatus::Ok);
                ++completed;
            }
        });
        for (size_t i = 0; i < 128; ++i) {
            waiters.Actor.RunSync([&] {
                auto writes = Drain(manager).Writes;
                UNIT_ASSERT_VALUES_EQUAL(writes.size(), 1);
                manager.CompleteWrite(writes.front());
            });
            UNIT_ASSERT_VALUES_EQUAL(completed, i + 1);
        }
        ui64 checksum = 0;
        UNIT_ASSERT(manager.GetBlockChecksum(key, 0, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(checksum, 127);
        UNIT_ASSERT(manager.Submissions.empty());
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
    }

    Y_UNIT_TEST(TwoPairWriteRemainsOwnedUntilItsSingleFinalCompletion) {
        for (const bool stop : {false, true}) {
            NIntegrityTest::TFixture manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
            const TKey key{41, 0};
            TChunkIdx next = 910;
            MakeReady(manager, key, 1700, &next);
            const ui32 offset = (ChecksumsPerIntegrityBlock - 1) * IntegrityUnitSize;
            auto operation = manager.Write(key, offset, 2 * IntegrityUnitSize, {0xA, 0xB});
            auto writes = Drain(manager).Writes;
            UNIT_ASSERT_VALUES_EQUAL(writes.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(writes.front().Data.size(), 4 * IntegrityUnitSize);
            if (stop) {
                manager.Stop();
            }
            UNIT_ASSERT(!operation.GetResult());
            UNIT_ASSERT(manager.HasInFlightOperationsForTablet(key.TabletId));
            manager.CompleteWrite(writes.front(), false);
            const auto done = ResultOf(operation);
            UNIT_ASSERT_EQUAL(done.Status, TIntegrityManager::EOperationStatus::Failed);
            UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
            manager.PrepareTabletChunksDeletion(key.TabletId);
            manager.CommitTabletChunksDeletion(key.TabletId);
        }
    }

    Y_UNIT_TEST(StalePairReadCompletionAfterRecreationIsIgnored) {
        NIntegrityTest::TFixture original(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{41, 0};
        TChunkIdx next = 910;
        MakeReady(original, key, 1700, &next);
        const auto ref = *original.FindExtentRef(key);
        const auto generation = original.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        manager.ApplyMappingSnapshot(original.SnapshotMapping());
        auto read = PreparePendingRead(manager, key, 0, IntegrityUnitSize);
        TActorWaiters waiters;
        size_t completed = 0;
        waiters.Actor.RunAsync([&]() -> NActors::async<void> {
            while (!read.GetResult()) {
                co_await read.WaitChanged();
            }
            manager.PrepareTabletChunksDeletion(key.TabletId);
            manager.CommitTabletChunksDeletion(key.TabletId);
            manager.StartExtent(key, 1701);
            ++completed;
        });
        auto reads = Drain(manager).Reads;
        UNIT_ASSERT_VALUES_EQUAL(reads.size(), 1);
        const ui64 retiredId = reads.front().Id;
        waiters.Actor.RunSync([&] {
            manager.CompleteRead(reads.front(), MakeIntegrityPair(
                MakeIntegrityBlock(key, ref, generation, 0, 0, {}),
                MakeIntegrityBlock(key, ref, generation, 0, 1, {})));
        });
        UNIT_ASSERT_VALUES_EQUAL(completed, 1);
        UNIT_ASSERT_EQUAL(read.GetResult()->Status, TIntegrityManager::EOperationStatus::Ok);
        const auto newGeneration = manager.FindExtentRef(key)->VChunkGeneration;
        UNIT_ASSERT_VALUES_UNEQUAL(newGeneration, ref.VChunkGeneration);
        waiters.Actor.RunSync([&] {
            TIntegrityManager::TMetadataReadResult stale{retiredId, {false, {}}};
            manager.CompleteMetadataReads(TConstArrayRef<TIntegrityManager::TMetadataReadResult>(&stale, 1));
            manager.NotifyCompleted();
        });
        UNIT_ASSERT_VALUES_EQUAL(completed, 1);
        UNIT_ASSERT_VALUES_EQUAL(manager.FindExtentRef(key)->VChunkGeneration, newGeneration);
    }

    Y_UNIT_TEST(WriterFailureCompletesEveryQueuedOwnerOnce) {
        for (const bool failFirst : {false, true}) {
            NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
            const TKey key{41, 0};
            TChunkIdx next = 910;
            MakeReady(manager, key, 1700, &next);
            auto first = manager.Write(key, 0, IntegrityUnitSize, {0xA});
            auto second = manager.PrepareWrite(key, IntegrityUnitSize, IntegrityUnitSize);
            auto third = manager.PrepareWrite(key, 2 * IntegrityUnitSize, IntegrityUnitSize);
            UNIT_ASSERT(!second.IsReady() && !third.IsReady());
            TActorWaiters waiters;
            std::array<size_t, 3> notifications{};
            auto observe = [&](auto& writer, size_t& count) {
                waiters.Actor.RunAsync([&writer, &count]() -> NActors::async<void> {
                    while (!writer.GetResult()) {
                        co_await writer.WaitChanged();
                    }
                    ++count;
                });
            };
            observe(first, notifications[0]);
            observe(second, notifications[1]);
            observe(third, notifications[2]);
            waiters.Actor.RunSync([&] {
                auto writes = Drain(manager).Writes;
                UNIT_ASSERT_VALUES_EQUAL(writes.size(), 1);
                manager.CompleteWrite(writes.front(), !failFirst);
            });
            if (!failFirst) {
                UNIT_ASSERT_VALUES_EQUAL(notifications[0], 1);
                UNIT_ASSERT_VALUES_EQUAL(notifications[1], 0);
                UNIT_ASSERT_VALUES_EQUAL(notifications[2], 0);
                UNIT_ASSERT(second.IsReady() && !third.IsReady());
                manager.SubmitWrite(second, std::vector<ui64>{0xB});
                waiters.Actor.RunSync([&] {
                    auto writes = Drain(manager).Writes;
                    manager.CompleteWrite(writes.front(), false);
                });
                UNIT_ASSERT_EQUAL(first.GetResult()->Status, TIntegrityManager::EOperationStatus::Ok);
                UNIT_ASSERT_EQUAL(second.GetResult()->Status, TIntegrityManager::EOperationStatus::Failed);
            } else {
                UNIT_ASSERT_EQUAL(first.GetResult()->Status, TIntegrityManager::EOperationStatus::Failed);
                UNIT_ASSERT_EQUAL(second.GetResult()->Status, TIntegrityManager::EOperationStatus::Failed);
            }
            UNIT_ASSERT_EQUAL(third.GetResult()->Status, TIntegrityManager::EOperationStatus::Failed);
            for (size_t count : notifications) {
                UNIT_ASSERT_VALUES_EQUAL(count, 1);
            }
            waiters.Actor.RunSync([&] {
                manager.NotifyCompleted();
            });
            for (size_t count : notifications) {
                UNIT_ASSERT_VALUES_EQUAL(count, 1);
            }
            UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
            UNIT_ASSERT(manager.Submissions.empty());
        }
    }

    Y_UNIT_TEST(ColdMetadataReadAndWriterRetainRequiredCompletionOrder) {
        for (const bool readFirst : {false, true}) {
            for (const int failure : {0, 1, 2}) {
                NIntegrityTest::TFixture original(SmallChunkSize, TestDDiskId, TestPDiskGuid);
                const TKey key{42, 3};
                TChunkIdx next = 920;
                MakeReady(original, key, 1800, &next);
                const auto ref = *original.FindExtentRef(key);
                const auto generation = original.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
                NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
                manager.ApplyMappingSnapshot(original.SnapshotMapping());
                TIntegrityManager::TOperation read;
                TIntegrityManager::TWriteOperation write;
                if (readFirst) {
                    read = PreparePendingRead(manager, key, 0, 2 * IntegrityUnitSize);
                    write = manager.PrepareWrite(key, 2 * IntegrityUnitSize, IntegrityUnitSize);
                    UNIT_ASSERT(!write.IsReady());
                } else {
                    write = manager.Write(key, 2 * IntegrityUnitSize, IntegrityUnitSize, {0x30});
                    read = PreparePendingRead(manager, key, 0, 2 * IntegrityUnitSize);
                }
                TActorWaiters waiters;
                std::vector<int> completed;
                waiters.Actor.RunAsync([&]() -> NActors::async<void> {
                    while (!read.GetResult()) {
                        co_await read.WaitChanged();
                    }
                    completed.push_back(0);
                });
                waiters.Actor.RunAsync([&]() -> NActors::async<void> {
                    while (!write.IsReady()) {
                        co_await write.WaitChanged();
                    }
                    if (readFirst && !write.GetResult()) {
                        manager.SubmitWrite(write, std::vector<ui64>{0x30});
                    }
                    while (!write.GetResult()) {
                        co_await write.WaitChanged();
                    }
                    completed.push_back(1);
                });
                auto actions = Drain(manager);
                UNIT_ASSERT_VALUES_EQUAL(actions.Reads.size(), 1);
                UNIT_ASSERT(actions.Writes.empty());
                auto a = MakeIntegrityBlock(key, ref, generation, 0, 2, {{0, 0x10}});
                auto b = MakeIntegrityBlock(key, ref, generation, 0, 3, {{0, 0x10}});
                if (failure == 2) {
                    ++a.Checksums[0];
                    ++b.Checksums[0];
                }
                waiters.Actor.RunSync([&] {
                    manager.CompleteRead(actions.Reads.front(), MakeIntegrityPair(a, b), failure != 1);
                });
                actions = Drain(manager);
                UNIT_ASSERT(actions.Reads.empty());
                if (!failure) {
                    UNIT_ASSERT_VALUES_EQUAL(completed.size(), readFirst ? 1 : 0);
                    UNIT_ASSERT_VALUES_EQUAL(actions.Writes.size(), 1);
                    TIntegrityBlock image;
                    memcpy(&image, actions.Writes.front().Data.data(), sizeof(image));
                    UNIT_ASSERT_VALUES_EQUAL(image.Checksums[0], b.Checksums[0]);
                    UNIT_ASSERT_VALUES_EQUAL(image.Header.UsedBlocksBitmap[0], 5);
                    waiters.Actor.RunSync([&] {
                        CompleteWrites(manager, actions.Writes);
                    });
                    UNIT_ASSERT_EQUAL(read.GetResult()->Status, TIntegrityManager::EOperationStatus::Ok);
                    UNIT_ASSERT_EQUAL(write.GetResult()->Status, TIntegrityManager::EOperationStatus::Ok);
                    AssertChecksums(read.GetResult()->Checksums, (std::vector<ui64>{0x10, GetZeroBlockChecksum()}));
                } else {
                    UNIT_ASSERT(actions.Writes.empty());
                    const auto status = failure == 1 ? TIntegrityManager::EOperationStatus::Failed
                        : TIntegrityManager::EOperationStatus::Corrupted;
                    UNIT_ASSERT_EQUAL(read.GetResult()->Status, status);
                    UNIT_ASSERT_EQUAL(write.GetResult()->Status, status);
                }
                UNIT_ASSERT_VALUES_EQUAL(completed, (std::vector<int>{0, 1}));
                UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
                UNIT_ASSERT(manager.Submissions.empty());
            }
        }
    }

    Y_UNIT_TEST(StopPendingExtentCompletesBothMilestonesAndReturnsLateAllocation) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        auto extent = manager.StartExtent({99, 0}, 2000);
        auto actions = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(actions.Allocations.size(), 1);
        UNIT_ASSERT(actions.Writes.empty() && actions.Reads.empty());
        std::optional<bool> placed, ready;
        TActorWaiters waiters;
        waiters.Actor.RunAsync([&]() -> NActors::async<void> {
            while (!(placed = extent.GetPlacedResult()).has_value()) {
                co_await extent.WaitChanged();
            }
        });
        waiters.Actor.RunAsync([&]() -> NActors::async<void> {
            while (!(ready = extent.GetReadyResult()).has_value()) {
                co_await extent.WaitChanged();
            }
        });
        UNIT_ASSERT(!placed && !ready);
        const auto before = manager.SnapshotMapping();
        waiters.Actor.RunSync([&] {
            manager.Stop();
        });
        UNIT_ASSERT(placed.has_value() && ready.has_value());
        UNIT_ASSERT(!*placed && !*ready);
        manager.CompleteAllocation(actions.Allocations.front(), 2001);
        UNIT_ASSERT_VALUES_EQUAL(manager.Returned, (std::vector<TChunkIdx>{2001}));
        UNIT_ASSERT(manager.Submissions.empty());
        UNIT_ASSERT(!*placed && !*ready);
        const auto after = manager.SnapshotMapping();
        UNIT_ASSERT_VALUES_EQUAL(after.Extents.size(), before.Extents.size());
        UNIT_ASSERT_VALUES_EQUAL(after.IntegrityChunks.size(), before.IntegrityChunks.size());
        UNIT_ASSERT_VALUES_EQUAL(after.GenerationCounter, before.GenerationCounter);
    }

    Y_UNIT_TEST(RestoredPairSelectsWinnerAndRestoresBitmap) {
        NIntegrityTest::TFixture original(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 42, .VChunkIndex = 3};
        TChunkIdx nextIntegrityChunkIdx = 920;
        MakeReady(original, key, 1800, &nextIntegrityChunkIdx);
        const auto snapshot = original.SnapshotMapping();
        const auto ref = *original.FindExtentRef(key);
        const ui64 chunkGeneration = original.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);

        NIntegrityTest::TFixture restored(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        restored.ApplyMappingSnapshot(snapshot);
        auto operationId = PreparePendingRead(restored, key, 0, 4 * IntegrityUnitSize);
        TActions actions = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(actions.Reads.size(), 1);
        const auto a = MakeIntegrityBlock(key, ref, chunkGeneration, 0, 2, {{0, 0x10}});
        const auto b = MakeIntegrityBlock(key, ref, chunkGeneration, 0, 3, {{2, 0x30}});
        restored.CompleteRead(actions.Reads[0], MakeIntegrityPair(a, b));

        const auto result = ResultOf(operationId);
        UNIT_ASSERT_VALUES_EQUAL(result.Checksums.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(result.Checksums[0], GetZeroBlockChecksum());
        UNIT_ASSERT_VALUES_EQUAL(result.Checksums[2], 0x30);
        const auto& plan = result.ReadPlan;
        UNIT_ASSERT_EQUAL(plan.Kind, TReadPlan::Mixed);
        UNIT_ASSERT(!plan.UsedBlocks.Get(0));
        UNIT_ASSERT(plan.UsedBlocks.Get(2));

        {
            const auto cached = ReadReadyResult(restored, key, 0, 4 * IntegrityUnitSize);
            AssertChecksums(cached.Checksums, result.Checksums);
            UNIT_ASSERT_EQUAL(cached.ReadPlan.Kind, TReadPlan::Mixed);
            UNIT_ASSERT(!cached.ReadPlan.UsedBlocks.Get(0));
            UNIT_ASSERT(cached.ReadPlan.UsedBlocks.Get(2));
            restored.NotifyCompleted();
        }
        UNIT_ASSERT(restored.Submissions.empty());
    }

    Y_UNIT_TEST(BothInvalidSlotsReportCorruption) {
        NIntegrityTest::TFixture original(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 43, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 930;
        MakeReady(original, key, 1900, &nextIntegrityChunkIdx);
        const auto snapshot = original.SnapshotMapping();
        const auto ref = *original.FindExtentRef(key);
        const ui64 chunkGeneration = original.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);

        NIntegrityTest::TFixture restored(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        restored.ApplyMappingSnapshot(snapshot);
        auto operationId = PreparePendingRead(restored, key, 0, IntegrityUnitSize);
        TActions actions = Drain(restored);
        auto a = MakeIntegrityBlock(key, ref, chunkGeneration, 0, 2, {{0, 1}});
        auto b = MakeIntegrityBlock(key, ref, chunkGeneration, 0, 3, {{0, 2}});
        ++a.Checksums[0];
        ++b.Checksums[0];
        restored.CompleteRead(actions.Reads[0], MakeIntegrityPair(a, b));
        const auto result = ResultOf(operationId);
        UNIT_ASSERT_EQUAL(result.Status, NIntegrityTest::TFixture::EOperationStatus::Corrupted);
        UNIT_ASSERT(!result.LostWriteDetected);
    }

    void TestPendingMultiPairReadCapturesWithoutPinningImages(bool cacheDisabled) {
        NIntegrityTest::TFixture original(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 45, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 950;
        MakeReady(original, key, 2100, &nextIntegrityChunkIdx);
        const auto snapshot = original.SnapshotMapping();

        NIntegrityTest::TFixture restored(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid,
            cacheDisabled ? 0 : NIntegrityTest::TFixture::BlockStateApproxBytes);
        restored.ApplyMappingSnapshot(snapshot);
        const auto ref = *restored.FindExtentRef(key);
        const ui64 chunkGeneration =
            restored.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        const ui32 blocks = ChecksumsPerIntegrityBlock + 1;

        auto operationId = PreparePendingRead(restored, key, 0, blocks * IntegrityUnitSize);
        TActions reads = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(reads.Reads.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reads.Reads[0].Size, 2 * IntegrityPairSlots * IntegrityUnitSize);

        restored.CompleteRead(reads.Reads[0], JoinRopes(
            MakeIntegrityPair(
                MakeIntegrityBlock(key, ref, chunkGeneration, 0, 0, {{0, 0xAA}}),
                MakeIntegrityBlock(key, ref, chunkGeneration, 0, 1, {{0, 0xAA}})),
            MakeIntegrityPair(
                MakeIntegrityBlock(key, ref, chunkGeneration, 1, 0, {{0, 0xBB}}),
                MakeIntegrityBlock(key, ref, chunkGeneration, 1, 1, {{0, 0xBB}}))));
        auto result = ResultOf(operationId);
        UNIT_ASSERT_EQUAL(result.Status, NIntegrityTest::TFixture::EOperationStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(result.Checksums.size(), blocks);
        UNIT_ASSERT_VALUES_EQUAL(result.Checksums.front(), 0xAA);
        UNIT_ASSERT_VALUES_EQUAL(result.Checksums.back(), 0xBB);
        UNIT_ASSERT_VALUES_EQUAL(restored.CachedBlockStates(), cacheDisabled ? 0 : 1);

        if (cacheDisabled) {
            auto repeatId = PreparePendingRead(restored, key, 0, IntegrityUnitSize);
            TActions repeat = Drain(restored);
            UNIT_ASSERT_VALUES_EQUAL(repeat.Reads.size(), 1);
            restored.CompleteRead(repeat.Reads[0], MakeIntegrityPair(
                MakeIntegrityBlock(key, ref, chunkGeneration, 0, 0, {{0, 0xAA}}),
                MakeIntegrityBlock(key, ref, chunkGeneration, 0, 1, {{0, 0xAA}})));
            const auto repeatedResult = ResultOf(repeatId);
            UNIT_ASSERT_EQUAL(repeatedResult.Status, NIntegrityTest::TFixture::EOperationStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(repeatedResult.Checksums.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(repeatedResult.Checksums[0], 0xAA);
            UNIT_ASSERT_VALUES_EQUAL(restored.CachedBlockStates(), 0);
        }
    }

    Y_UNIT_TEST(PendingMultiPairReadCapturesWithoutPinningImages) {
        TestPendingMultiPairReadCapturesWithoutPinningImages(false);
    }

    Y_UNIT_TEST(PendingMultiPairReadCapturesWithoutPinningImagesWithoutCache) {
        TestPendingMultiPairReadCapturesWithoutPinningImages(true);
    }

    Y_UNIT_TEST(TwoPairRmwGeometryPreservesCurrentImagesAndRecoversEveryParity) {
        for (const ui32 warmPairs : {0u, 1u, 2u}) {
            for (const ui32 firstSlot : {0u, 1u}) {
                for (const ui32 secondSlot : {0u, 1u}) {
                    NIntegrityTest::TFixture original(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
                    const TKey key{47, 0};
                    TChunkIdx next = 970;
                    MakeReady(original, key, 2300, &next);
                    const auto snapshot = original.SnapshotMapping();
                    const auto ref = *original.FindExtentRef(key);
                    const auto generation = original.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
                    NIntegrityTest::TFixture manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid,
                        2 * TIntegrityManager::BlockStateApproxBytes);
                    manager.ApplyMappingSnapshot(snapshot);
                    const ui32 slots[] = {firstSlot, secondSlot};
                    std::array<TIntegrityBlock, 4> disk;
                    for (ui32 pair = 0; pair < 2; ++pair) {
                        const std::vector<std::pair<ui32, ui64>> untouched{{5 + pair, 0xAA + pair}};
                        disk[pair * 2] = MakeIntegrityBlock(key, ref, generation, pair, 2, untouched);
                        disk[pair * 2 + 1] = MakeIntegrityBlock(key, ref, generation, pair,
                            slots[pair] ? 3 : 1, untouched);
                        if (pair < warmPairs) {
                            auto read = PreparePendingRead(manager, key,
                                pair * ChecksumsPerIntegrityBlock * IntegrityUnitSize, IntegrityUnitSize);
                            auto reads = Drain(manager).Reads;
                            UNIT_ASSERT_VALUES_EQUAL(reads.size(), 1);
                            manager.CompleteRead(reads.front(),
                                MakeIntegrityPair(disk[pair * 2], disk[pair * 2 + 1]));
                            UNIT_ASSERT_EQUAL(read.GetResult()->Status, TIntegrityManager::EOperationStatus::Ok);
                        }
                    }
                    const ui32 firstBlock = ChecksumsPerIntegrityBlock - 1;
                    auto writer = manager.PrepareWrite(key, firstBlock * IntegrityUnitSize, 2 * IntegrityUnitSize);
                    UNIT_ASSERT(writer.IsReady());
                    const ui64 checksums[] = {0xCC, 0xDD};
                    auto context = manager.PrepareMetadataWrite(writer, checksums);
                    const ui32 base = manager.ExtentOffset(ref.ExtentSlot);
                    UNIT_ASSERT_VALUES_EQUAL(context->ReadOffset, base);
                    UNIT_ASSERT_VALUES_EQUAL(context->ReadSize, warmPairs == 2 ? 0 : 4 * IntegrityUnitSize);
                    if (context->ReadSize) {
                        auto region = TRcBuf::UninitializedPageAligned(sizeof(disk));
                        memcpy(region.GetDataMut(), disk.data(), sizeof(disk));
                        UNIT_ASSERT(context->Transform(TReadPayload(std::move(region))));
                    }
                    const bool middle = firstSlot == 0 && secondSlot == 1;
                    UNIT_ASSERT_VALUES_EQUAL(context->WriteOffset, base + (middle ? IntegrityUnitSize : 0));
                    UNIT_ASSERT_VALUES_EQUAL(context->WriteImage.size(), (middle ? 2 : 4) * IntegrityUnitSize);
                    for (ui32 pair = 0; pair < 2; ++pair) {
                        const auto& current = disk[pair * 2 + slots[pair]];
                        TIntegrityBlock updated;
                        memcpy(&updated, context->Pairs[pair].UpdatedImage.data(), sizeof(updated));
                        UNIT_ASSERT_VALUES_EQUAL(updated.Header.PairSequenceNumber, current.Header.PairSequenceNumber + 1);
                        UNIT_ASSERT_VALUES_EQUAL(updated.Checksums[5 + pair], current.Checksums[5 + pair]);
                        const ui32 target = pair ? 0 : ChecksumsPerIntegrityBlock - 1;
                        UNIT_ASSERT_VALUES_EQUAL(UnsealBlockChecksum(updated.Checksums[target], TestDDiskId,
                            TestPDiskGuid, key.TabletId, key.VChunkIndex, firstBlock + pair), checksums[pair]);
                        if (!middle) {
                            UNIT_ASSERT_VALUES_EQUAL(memcmp(context->WriteImage.data()
                                + (pair * 2 + slots[pair]) * IntegrityUnitSize, &current, sizeof(current)), 0);
                        }
                        disk[pair * 2 + 1 - slots[pair]] = updated;
                    }
                    {
                        manager.CompleteMetadataWrite(context, true);
                        manager.NotifyCompleted();
                    }
                    UNIT_ASSERT_EQUAL(writer.GetResult()->Status, TIntegrityManager::EOperationStatus::Ok);
                    NIntegrityTest::TFixture restarted(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid, 0);
                    restarted.ApplyMappingSnapshot(snapshot);
                    auto read = PreparePendingRead(restarted, key, firstBlock * IntegrityUnitSize, 2 * IntegrityUnitSize);
                    auto reads = Drain(restarted).Reads;
                    UNIT_ASSERT_VALUES_EQUAL(reads.size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(reads[0].Size, sizeof(disk));
                    auto region = TRcBuf::UninitializedPageAligned(sizeof(disk));
                    memcpy(region.GetDataMut(), disk.data(), sizeof(disk));
                    restarted.CompleteRead(reads[0], TRope(std::move(region)));
                    UNIT_ASSERT(read.GetResult());
                    AssertChecksums(read.GetResult()->Checksums, checksums);
                    UNIT_ASSERT_EQUAL(read.GetResult()->ReadPlan.Kind, TReadPlan::Passthrough);
                    UNIT_ASSERT_VALUES_EQUAL(restarted.CachedBlockStates(), 0);
                }
            }
        }
    }

    Y_UNIT_TEST(MultiPairCorruptionKeepsDeletionBusyForSiblingIo) {
        NIntegrityTest::TFixture original(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{.TabletId = 46, .VChunkIndex = 0};
        TChunkIdx nextIntegrityChunkIdx = 960;
        MakeReady(original, key, 2200, &nextIntegrityChunkIdx);
        const auto snapshot = original.SnapshotMapping();

        NIntegrityTest::TFixture restored(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        restored.ApplyMappingSnapshot(snapshot);
        const auto ref = *restored.FindExtentRef(key);
        const ui64 chunkGeneration =
            restored.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        const ui32 blocks = ChecksumsPerIntegrityBlock + 1;

        auto read = PreparePendingRead(restored, key, 0, blocks * IntegrityUnitSize);
        TActions reads = Drain(restored);
        UNIT_ASSERT_VALUES_EQUAL(reads.Reads.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(reads.Reads[0].Size, 2 * IntegrityPairSlots * IntegrityUnitSize);

        auto invalid = TRcBuf::UninitializedPageAligned(
            IntegrityPairSlots * sizeof(TIntegrityBlock));
        memset(invalid.GetDataMut(), 0, invalid.size());
        restored.CompleteRead(reads.Reads[0], JoinRopes(
            MakeIntegrityPair(
                MakeIntegrityBlock(key, ref, chunkGeneration, 0, 0, {}),
                MakeIntegrityBlock(key, ref, chunkGeneration, 0, 1, {})),
            TRope(std::move(invalid))));
        UNIT_ASSERT(!restored.HasInFlightOperationsForTablet(key.TabletId));
        UNIT_ASSERT_EQUAL(read.GetResult()->Status,
            NIntegrityTest::TFixture::EOperationStatus::Corrupted);

        // The corrupt pair is remembered, so a later read of the range queues no metadata I/O.
        auto preparation = restored.PrepareRead(key, 0, blocks * IntegrityUnitSize);
        UNIT_ASSERT(preparation.Warm);
        UNIT_ASSERT(preparation.MetadataReads.empty());
        UNIT_ASSERT(Drain(restored).Reads.empty());
        UNIT_ASSERT_EQUAL(preparation.Warm->Status, NIntegrityTest::TFixture::EOperationStatus::Corrupted);

        restored.PrepareTabletChunksDeletion(key.TabletId);
        restored.CommitTabletChunksDeletion(key.TabletId);
    }

    Y_UNIT_TEST(PrepareColdReadClaimsWithoutSubmittingAndSharesLoadsAfterDetach) {
        NIntegrityTest::TFixture original(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{48, 0};
        TChunkIdx next = 980;
        MakeReady(original, key, 2400, &next);
        const auto ref = *original.FindExtentRef(key);
        const auto generation = original.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        NIntegrityTest::TFixture manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid,
            TIntegrityManager::BlockStateApproxBytes);
        manager.ApplyMappingSnapshot(original.SnapshotMapping());
        const ui32 blocks = ChecksumsPerIntegrityBlock + 1;
        auto owner = manager.PrepareRead(key, 0, blocks * IntegrityUnitSize);
        auto joined = manager.PrepareRead(key, 0, blocks * IntegrityUnitSize);
        UNIT_ASSERT(!owner.Warm);
        UNIT_ASSERT(!joined.Warm);
        UNIT_ASSERT_VALUES_EQUAL(owner.MetadataReads.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(owner.MetadataReads[0].Size, 2 * IntegrityPairSlots * IntegrityUnitSize);
        UNIT_ASSERT(joined.MetadataReads.empty());
        UNIT_ASSERT(manager.Submissions.empty());
        TActorWaiters waiters;
        NActors::TAsyncCancellationScope scope;
        size_t detachedNotifications = 0, joinedNotifications = 0;
        bool detached = false;
        waiters.Actor.RunAsync([&, operation = owner.Pending]() -> NActors::async<void> {
            co_await scope.Wrap([&]() -> NActors::async<void> {
                while (!operation.GetResult()) {
                    co_await operation.WaitChanged();
                }
                ++detachedNotifications;
            });
            detached = true;
        });
        waiters.Observe(joined.Pending, joinedNotifications);
        waiters.Actor.RunSync([&] {
            scope.Cancel();
        });
        UNIT_ASSERT(detached);
        owner.Pending = {};
        UNIT_ASSERT(manager.HasInFlightOperationsForTablet(key.TabletId));
        waiters.Actor.RunSync([&] {
            TIntegrityManager::TMetadataReadResult result{owner.MetadataReads[0].Id, {true,
                JoinRopes(
                    MakeIntegrityPair(
                        MakeIntegrityBlock(key, ref, generation, 0, 0, {{0, 0xAA}}),
                        MakeIntegrityBlock(key, ref, generation, 0, 1, {{0, 0xAA}})),
                    MakeIntegrityPair(
                        MakeIntegrityBlock(key, ref, generation, 1, 0, {{0, 0xBB}}),
                        MakeIntegrityBlock(key, ref, generation, 1, 1, {{0, 0xBB}})))}};
            manager.CompleteMetadataReads(TConstArrayRef<TIntegrityManager::TMetadataReadResult>(&result, 1));
            manager.NotifyCompleted();
        });
        UNIT_ASSERT_VALUES_EQUAL(detachedNotifications, 0);
        UNIT_ASSERT_VALUES_EQUAL(joinedNotifications, 1);
        const auto* result = joined.Pending.GetResult();
        UNIT_ASSERT(result);
        UNIT_ASSERT_EQUAL(result->Status, TIntegrityManager::EOperationStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(result->Checksums.size(), blocks);
        UNIT_ASSERT_VALUES_EQUAL(result->Checksums.front(), 0xAA);
        UNIT_ASSERT_VALUES_EQUAL(result->Checksums.back(), 0xBB);
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 1);
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
        waiters.Observe(joined.Pending, joinedNotifications);
        UNIT_ASSERT_VALUES_EQUAL(joinedNotifications, 2);
        manager.PrepareTabletChunksDeletion(key.TabletId);
        manager.CommitTabletChunksDeletion(key.TabletId);
        UNIT_ASSERT_VALUES_EQUAL(result->Checksums.front(), 0xAA);
        UNIT_ASSERT_VALUES_EQUAL(result->Checksums.back(), 0xBB);
    }

    Y_UNIT_TEST(PartialOverlapPublishesSnapshotsBeforeTargetedNotification) {
        NIntegrityTest::TFixture original(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{58, 0};
        TChunkIdx next = 1060;
        MakeReady(original, key, 2502, &next);
        const auto ref = *original.FindExtentRef(key);
        const auto generation = original.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        NIntegrityTest::TFixture manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        manager.ApplyMappingSnapshot(original.SnapshotMapping());
        auto first = manager.PrepareRead(key, 0, IntegrityUnitSize);
        auto overlap = manager.PrepareRead(key, 0,
            (ChecksumsPerIntegrityBlock + 1) * IntegrityUnitSize);
        auto joined = manager.PrepareRead(key, ChecksumsPerIntegrityBlock * IntegrityUnitSize,
            IntegrityUnitSize);
        UNIT_ASSERT(!first.Warm);
        UNIT_ASSERT(!overlap.Warm);
        UNIT_ASSERT(!joined.Warm);
        UNIT_ASSERT_VALUES_EQUAL(first.MetadataReads.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(overlap.MetadataReads.size(), 1);
        UNIT_ASSERT(joined.MetadataReads.empty());
        size_t firstNotifications = 0, overlapNotifications = 0, joinedNotifications = 0;
        TActorWaiters waiters;
        NActors::TAsyncCancellationScope scope;
        waiters.Observe(first.Pending, firstNotifications);
        waiters.Observe(overlap.Pending, overlapNotifications);
        waiters.Actor.RunAsync([&]() -> NActors::async<void> {
            co_await scope.Wrap([&]() -> NActors::async<void> {
                while (!joined.Pending.GetResult()) {
                    co_await joined.Pending.WaitChanged();
                }
                ++joinedNotifications;
            });
        });
        const auto complete = [&](const auto& read, ui32 pair, ui64 checksum) {
            waiters.Actor.RunSync([&] {
                TIntegrityManager::TMetadataReadResult result{read.Id, {true, MakeIntegrityPair(
                    MakeIntegrityBlock(key, ref, generation, pair, 0, {{0, checksum}}),
                    MakeIntegrityBlock(key, ref, generation, pair, 1, {{0, checksum}}))}};
                manager.CompleteMetadataReads(TConstArrayRef<TIntegrityManager::TMetadataReadResult>(&result, 1));
            });
        };
        complete(first.MetadataReads.front(), 0, 0xAA);
        UNIT_ASSERT(first.Pending.IsDone());
        UNIT_ASSERT(!overlap.Pending.IsDone());
        UNIT_ASSERT_VALUES_EQUAL(firstNotifications, 0);
        waiters.Actor.RunSync([&] {
            manager.NotifyCompleted();
        });
        UNIT_ASSERT_VALUES_EQUAL(firstNotifications, 1);
        UNIT_ASSERT_VALUES_EQUAL(overlapNotifications, 0);
        UNIT_ASSERT_VALUES_EQUAL(joinedNotifications, 0);
        complete(overlap.MetadataReads.front(), 1, 0xBB);
        UNIT_ASSERT(overlap.Pending.IsDone() && joined.Pending.IsDone());
        UNIT_ASSERT_VALUES_EQUAL(overlap.Pending.GetResult()->Checksums.front(), 0xAA);
        UNIT_ASSERT_VALUES_EQUAL(overlap.Pending.GetResult()->Checksums.back(), 0xBB);
        // Cancel the parked waiter in its own activation before notifications are scheduled.
        waiters.Actor.RunSync([&] {
            scope.Cancel();
        });
        waiters.Actor.RunSync([&] {
            manager.NotifyCompleted();
        });
        UNIT_ASSERT_VALUES_EQUAL(firstNotifications, 1);
        UNIT_ASSERT_VALUES_EQUAL(overlapNotifications, 1);
        UNIT_ASSERT_VALUES_EQUAL(joinedNotifications, 0);
    }

    Y_UNIT_TEST(WarmWriteRetainsReadableImmutableImageUntilPublication) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{51, 0};
        TChunkIdx next = 1010;
        MakeReady(manager, key, 2501, &next);
        manager.Write(key, 0, IntegrityUnitSize, {0xAA});
        CompleteWrites(manager, Drain(manager).Writes);

        auto writer = manager.PrepareWrite(key, IntegrityUnitSize, IntegrityUnitSize);
        UNIT_ASSERT(writer.IsReady());
        const ui64 checksum = 0xBB;
        auto context = manager.PrepareMetadataWrite(writer, {&checksum, 1});
        UNIT_ASSERT_VALUES_EQUAL(context->ReadSize, 0);
        const auto before = ReadReadyResult(manager, key, 0, 2 * IntegrityUnitSize);
        AssertChecksums(before.Checksums, (std::vector<ui64>{0xAA, GetZeroBlockChecksum()}));
        UNIT_ASSERT_EQUAL(before.ReadPlan.Kind, TReadPlan::Mixed);
        UNIT_ASSERT(!writer.GetResult());
        {
            manager.CompleteMetadataWrite(context, true);
            manager.NotifyCompleted();
        }
        const auto after = ReadReadyResult(manager, key, 0, 2 * IntegrityUnitSize);
        AssertChecksums(after.Checksums, (std::vector<ui64>{0xAA, 0xBB}));
        AssertChecksums(before.Checksums, (std::vector<ui64>{0xAA, GetZeroBlockChecksum()}));
        UNIT_ASSERT(before.ReadPlan.UsedBlocks.Get(0));
        UNIT_ASSERT(!before.ReadPlan.UsedBlocks.Get(1));
    }

    Y_UNIT_TEST(ColdReaderWaitsForFinalRmwWriteRatherThanReadLeg) {
        NIntegrityTest::TFixture original(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{52, 0};
        TChunkIdx next = 1020;
        MakeReady(original, key, 2502, &next);
        const auto ref = *original.FindExtentRef(key);
        const auto generation = original.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid, 0);
        manager.ApplyMappingSnapshot(original.SnapshotMapping());
        auto writer = manager.PrepareWrite(key, IntegrityUnitSize, IntegrityUnitSize);
        UNIT_ASSERT(writer.IsReady());
        const ui64 checksum = 0xBB;
        auto context = manager.PrepareMetadataWrite(writer, {&checksum, 1});
        UNIT_ASSERT_VALUES_EQUAL(context->ReadSize, 2 * IntegrityUnitSize);
        auto reader = manager.PrepareRead(key, 0, 2 * IntegrityUnitSize);
        UNIT_ASSERT(!reader.Warm);
        UNIT_ASSERT(reader.MetadataReads.empty());
        UNIT_ASSERT(!reader.Pending.GetResult());
        UNIT_ASSERT(context->Transform(TReadPayload(MakeIntegrityPair(
            MakeIntegrityBlock(key, ref, generation, 0, 2, {{0, 0xAA}}),
            MakeIntegrityBlock(key, ref, generation, 0, 1, {})))));
        UNIT_ASSERT(!reader.Pending.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);
        {
            manager.CompleteMetadataWrite(context, true);
            manager.NotifyCompleted();
        }
        UNIT_ASSERT(reader.Pending.GetResult());
        AssertChecksums(reader.Pending.GetResult()->Checksums, (std::vector<ui64>{0xAA, 0xBB}));
        UNIT_ASSERT_EQUAL(reader.Pending.GetResult()->ReadPlan.Kind, TReadPlan::Passthrough);
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);
    }

    Y_UNIT_TEST(QueuedAndSelectedWriterOwnershipSurvivesEvictionAndCancellation) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid, 0);
        const TKey key{53, 0};
        TChunkIdx next = 1030;
        MakeReady(manager, key, 2503, &next);
        auto first = manager.PrepareWrite(key, 0, IntegrityUnitSize);
        auto second = manager.PrepareWrite(key, IntegrityUnitSize, IntegrityUnitSize);
        auto third = manager.PrepareWrite(key, 2 * IntegrityUnitSize, IntegrityUnitSize);
        UNIT_ASSERT(first.IsReady());
        UNIT_ASSERT(!second.IsReady());
        UNIT_ASSERT(!third.IsReady());
        const ui64 checksumA = 0xAA;
        auto firstContext = manager.PrepareMetadataWrite(first, {&checksumA, 1});
        {
            manager.CompleteMetadataWrite(firstContext, true);
            manager.NotifyCompleted();
        }
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 1);
        // The selected writer owns the entry before it resumes. A new arrival cannot steal it.
        auto arrival = manager.PrepareWrite(key, 3 * IntegrityUnitSize, IntegrityUnitSize);
        UNIT_ASSERT(!arrival.IsReady());
        UNIT_ASSERT(!third.IsReady());
        {
            second.Cancel();
            arrival.Cancel();
            manager.NotifyCompleted();
        }
        UNIT_ASSERT(third.IsReady());
        UNIT_ASSERT_EQUAL(second.GetResult()->Status, TIntegrityManager::EOperationStatus::Failed);
        const ui64 checksumC = 0xCC;
        auto thirdContext = manager.PrepareMetadataWrite(third, {&checksumC, 1});
        UNIT_ASSERT_VALUES_EQUAL(thirdContext->ReadSize, 0);
        TIntegrityBlock image;
        memcpy(&image, thirdContext->WriteImage.data(), sizeof(image));
        UNIT_ASSERT_VALUES_EQUAL(image.Header.UsedBlocksBitmap[0], 5);
        UNIT_ASSERT_VALUES_EQUAL(UnsealBlockChecksum(image.Checksums[0], TestDDiskId, TestPDiskGuid,
            key.TabletId, key.VChunkIndex, 0), checksumA);
        {
            manager.CompleteMetadataWrite(thirdContext, true);
            manager.NotifyCompleted();
        }
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
    }

    Y_UNIT_TEST(SuccessfulWriterReleaseWakesOnlyNextQueuedWriter) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{54, 0};
        TChunkIdx next = 1040;
        MakeReady(manager, key, 2504, &next);
        auto first = manager.PrepareWrite(key, 0, IntegrityUnitSize);
        auto second = manager.PrepareWrite(key, IntegrityUnitSize, IntegrityUnitSize);
        auto third = manager.PrepareWrite(key, 2 * IntegrityUnitSize, IntegrityUnitSize);
        size_t secondResumed = 0, thirdResumed = 0;
        NAsyncTest::TAsyncTestActor::TState state;
        NAsyncTest::TAsyncTestActorRuntime runtime;
        auto actor = runtime.StartAsyncActor(state, [](auto*) -> NActors::async<void> {
            co_return;
        });
        actor.RunAsync([&]() -> NActors::async<void> {
            while (!second.IsReady()) {
                co_await second.WaitChanged();
                ++secondResumed;
            }
        });
        actor.RunAsync([&]() -> NActors::async<void> {
            while (!third.IsReady()) {
                co_await third.WaitChanged();
                ++thirdResumed;
            }
        });
        const ui64 checksum = 0xAA;
        auto firstContext = manager.PrepareMetadataWrite(first, {&checksum, 1});
        actor.RunSync([&] {
            manager.CompleteMetadataWrite(firstContext, true);
            manager.NotifyCompleted();
        });
        UNIT_ASSERT_VALUES_EQUAL(secondResumed, 1);
        UNIT_ASSERT_VALUES_EQUAL(thirdResumed, 0);
        UNIT_ASSERT(second.IsReady());
        auto secondContext = manager.PrepareMetadataWrite(second, {&checksum, 1});
        actor.RunSync([&] {
            manager.CompleteMetadataWrite(secondContext, true);
            manager.NotifyCompleted();
        });
        UNIT_ASSERT_VALUES_EQUAL(secondResumed, 1);
        UNIT_ASSERT_VALUES_EQUAL(thirdResumed, 1);
        UNIT_ASSERT(third.IsReady());
        third.Cancel();
    }

    Y_UNIT_TEST(CancelledColdOwnerResolvesReadersWithoutStrandingMetadata) {
        NIntegrityTest::TFixture original(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{55, 0};
        TChunkIdx next = 1050;
        MakeReady(original, key, 2505, &next);
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid, 0);
        manager.ApplyMappingSnapshot(original.SnapshotMapping());
        auto writer = manager.PrepareWrite(key, 0, IntegrityUnitSize);
        UNIT_ASSERT(writer.IsReady());
        auto reader = PreparePendingRead(manager, key, 0, IntegrityUnitSize);
        UNIT_ASSERT(!reader.GetResult());
        UNIT_ASSERT(Drain(manager).Reads.empty());
        {
            writer.Cancel();
            manager.NotifyCompleted();
        }
        UNIT_ASSERT(reader.GetResult());
        UNIT_ASSERT_EQUAL(reader.GetResult()->Status, TIntegrityManager::EOperationStatus::Failed);
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
    }

    Y_UNIT_TEST(SubmittedContextRetainsOwnershipAfterHandleCancellationAndDestruction) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid, 0);
        const TKey key{56, 0};
        TChunkIdx next = 1060;
        MakeReady(manager, key, 2506, &next);
        auto first = manager.PrepareWrite(key, 0, IntegrityUnitSize);
        const ui64 checksum = 0xAA;
        auto context = manager.PrepareMetadataWrite(first, {&checksum, 1});
        auto second = manager.PrepareWrite(key, IntegrityUnitSize, IntegrityUnitSize);
        UNIT_ASSERT(!second.IsReady());
        {
            first.Cancel();
            first = {};
            manager.NotifyCompleted();
        }
        UNIT_ASSERT(!second.IsReady());
        UNIT_ASSERT(manager.HasInFlightOperationsForTablet(key.TabletId));
        {
            manager.CompleteMetadataWrite(context, true);
            manager.NotifyCompleted();
        }
        UNIT_ASSERT(second.IsReady());
        auto secondContext = manager.PrepareMetadataWrite(second, {&checksum, 1});
        UNIT_ASSERT_VALUES_EQUAL(secondContext->ReadSize, 0);
        TIntegrityBlock image;
        memcpy(&image, secondContext->WriteImage.data(), sizeof(image));
        UNIT_ASSERT_VALUES_EQUAL(image.Header.UsedBlocksBitmap[0], 3);
        {
            manager.CompleteMetadataWrite(secondContext, true);
            manager.NotifyCompleted();
        }
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);
    }

    Y_UNIT_TEST(ColdRmwFailureCompletesReaderAndEveryQueuedWriterOnce) {
        for (const ui32 failure : {0u, 1u, 2u}) {
            NIntegrityTest::TFixture original(SmallChunkSize, TestDDiskId, TestPDiskGuid);
            const TKey key{57, 1};
            TChunkIdx next = 1070;
            MakeReady(original, key, 2507, &next);
            NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid, 0);
            manager.ApplyMappingSnapshot(original.SnapshotMapping());
            auto first = manager.Write(key, 0, IntegrityUnitSize, {0xAA});
            auto second = manager.PrepareWrite(key, IntegrityUnitSize, IntegrityUnitSize);
            auto third = manager.PrepareWrite(key, 2 * IntegrityUnitSize, IntegrityUnitSize);
            UNIT_ASSERT(!second.IsReady() && !third.IsReady());
            auto reader = PreparePendingRead(manager, key, 0, IntegrityUnitSize);
            auto reads = Drain(manager).Reads;
            UNIT_ASSERT_VALUES_EQUAL(reads.size(), 1);
            TActorWaiters waiters;
            std::array<size_t, 4> notifications{};
            auto observe = [&](auto& operation, size_t& count) {
                waiters.Actor.RunAsync([&operation, &count]() -> NActors::async<void> {
                    while (!operation.GetResult()) {
                        co_await operation.WaitChanged();
                    }
                    ++count;
                });
            };
            observe(first, notifications[0]);
            observe(second, notifications[1]);
            observe(third, notifications[2]);
            observe(reader, notifications[3]);
            const ui32 readSize = failure == 1 ? IntegrityUnitSize : 2 * IntegrityUnitSize;
            auto data = TRcBuf::UninitializedPageAligned(readSize);
            memset(data.GetDataMut(), 0, data.size());
            waiters.Actor.RunSync([&] {
                manager.CompleteRead(reads.front(), TRope(std::move(data)), failure != 0);
            });
            const auto status = failure == 0 ? TIntegrityManager::EOperationStatus::Failed
                : TIntegrityManager::EOperationStatus::Corrupted;
            UNIT_ASSERT_EQUAL(first.GetResult()->Status, status);
            UNIT_ASSERT_EQUAL(second.GetResult()->Status, status);
            UNIT_ASSERT_EQUAL(third.GetResult()->Status, status);
            UNIT_ASSERT_EQUAL(reader.GetResult()->Status, status);
            for (size_t count : notifications) {
                UNIT_ASSERT_VALUES_EQUAL(count, 1);
            }
            waiters.Actor.RunSync([&] {
                manager.NotifyCompleted();
            });
            for (size_t count : notifications) {
                UNIT_ASSERT_VALUES_EQUAL(count, 1);
            }
            UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
            UNIT_ASSERT(manager.Submissions.empty());
        }
    }

    Y_UNIT_TEST(StoppingFailsQueuedWritersButDrainsAcceptedColdRmw) {
        NIntegrityTest::TFixture original(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{57, 0};
        TChunkIdx next = 1070;
        MakeReady(original, key, 2507, &next);
        const auto ref = *original.FindExtentRef(key);
        const auto generation = original.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid, 0);
        manager.ApplyMappingSnapshot(original.SnapshotMapping());
        auto first = manager.PrepareWrite(key, 0, IntegrityUnitSize);
        const ui64 checksum = 0xAA;
        auto context = manager.PrepareMetadataWrite(first, {&checksum, 1});
        auto second = manager.PrepareWrite(key, IntegrityUnitSize, IntegrityUnitSize);
        auto third = manager.PrepareWrite(key, 2 * IntegrityUnitSize, IntegrityUnitSize);
        auto reader = PreparePendingRead(manager, key, 0, IntegrityUnitSize);
        manager.Stop();
        UNIT_ASSERT(second.GetResult() && third.GetResult());
        UNIT_ASSERT_EQUAL(second.GetResult()->Status, TIntegrityManager::EOperationStatus::Failed);
        UNIT_ASSERT_EQUAL(third.GetResult()->Status, TIntegrityManager::EOperationStatus::Failed);
        UNIT_ASSERT(!first.GetResult());
        UNIT_ASSERT(!reader.GetResult());
        UNIT_ASSERT(manager.HasInFlightOperationsForTablet(key.TabletId));
        UNIT_ASSERT(context->Transform(TReadPayload(MakeIntegrityPair(
            MakeIntegrityBlock(key, ref, generation, 0, 0, {}),
            MakeIntegrityBlock(key, ref, generation, 0, 1, {})))));
        {
            manager.CompleteMetadataWrite(context, false);
            manager.NotifyCompleted();
        }
        UNIT_ASSERT_EQUAL(first.GetResult()->Status, TIntegrityManager::EOperationStatus::Failed);
        UNIT_ASSERT_EQUAL(reader.GetResult()->Status, TIntegrityManager::EOperationStatus::Failed);
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
    }

    Y_UNIT_TEST(WaitingWriterUnlinksOnCoroutineCancellationAndForcedTeardown) {
        for (const bool cancel : {false, true}) {
            NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid, 0);
            const TKey key{59, 0};
            TChunkIdx next = 1080;
            MakeReady(manager, key, 2509, &next);
            auto first = manager.PrepareWrite(key, 0, IntegrityUnitSize);
            bool finished = false, resumed = false;
            NActors::TAsyncCancellationScope scope;
            NAsyncTest::TAsyncTestActor::TState state;
            NAsyncTest::TAsyncTestActorRuntime runtime;
            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> NActors::async<void> {
                Y_DEFER { finished = true; };
                co_await scope.Wrap([&]() -> NActors::async<void> {
                    auto waiting = manager.PrepareWrite(key, IntegrityUnitSize, IntegrityUnitSize);
                    while (!waiting.IsReady()) {
                        co_await waiting.WaitChanged();
                    }
                    resumed = true;
                });
            });
            UNIT_ASSERT(!finished);
            if (cancel) {
                actor.RunSync([&] {
                    scope.Cancel();
                });
            } else {
                runtime.CleanupNode();
            }
            UNIT_ASSERT(finished);
            UNIT_ASSERT(!resumed);
            const ui64 checksum = 0xAA;
            auto context = manager.PrepareMetadataWrite(first, {&checksum, 1});
            {
                manager.CompleteMetadataWrite(context, true);
                manager.NotifyCompleted();
            }
            UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
            UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);
        }
    }

    Y_UNIT_TEST(WriterWaitingForExtentFormattingDoesNotSubmitMetadataAndStopsCleanly) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid, 0);
        const TKey key{60, 0};
        auto extent = manager.StartExtent(key, 2510);
        auto allocation = Drain(manager);
        UNIT_ASSERT_VALUES_EQUAL(allocation.Allocations.size(), 1);
        manager.CompleteAllocation(allocation.Allocations.front(), 1090);
        auto formatting = Drain(manager).Writes;
        auto writer = manager.PrepareWrite(key, 0, IntegrityUnitSize);
        UNIT_ASSERT(!writer.IsReady());
        manager.Stop();
        UNIT_ASSERT(writer.IsReady());
        UNIT_ASSERT(writer.GetResult());
        UNIT_ASSERT_EQUAL(writer.GetResult()->Status, TIntegrityManager::EOperationStatus::Failed);
        writer = {};
        CompleteWrites(manager, formatting);
        UNIT_ASSERT(!manager.IsExtentReady(key));
        UNIT_ASSERT(Drain(manager).Writes.empty());
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
    }

    Y_UNIT_TEST(CompletedFollowerRetainsChecksumsAfterImmediateMetadataEviction) {
        NIntegrityTest::TFixture original(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{61, 0};
        TChunkIdx next = 1100;
        MakeReady(original, key, 2511, &next);
        const auto ref = *original.FindExtentRef(key);
        const auto generation = original.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid, 0);
        manager.ApplyMappingSnapshot(original.SnapshotMapping());
        auto initiator = PreparePendingRead(manager, key, 0, 2 * IntegrityUnitSize);
        auto follower = PreparePendingRead(manager, key, 0, 2 * IntegrityUnitSize);
        auto reads = Drain(manager).Reads;
        UNIT_ASSERT_VALUES_EQUAL(reads.size(), 1);
        std::vector<int> notifications;
        TActorWaiters waiters;
        waiters.Actor.RunAsync([&]() -> NActors::async<void> {
            while (!initiator.GetResult()) {
                co_await initiator.WaitChanged();
            }
            notifications.push_back(1);
        });
        waiters.Actor.RunAsync([&]() -> NActors::async<void> {
            while (!follower.GetResult()) {
                co_await follower.WaitChanged();
            }
            notifications.push_back(2);
        });
        waiters.Actor.RunSync([&] {
            manager.CompleteRead(reads.front(), MakeIntegrityPair(
                MakeIntegrityBlock(key, ref, generation, 0, 0, {{0, 0xAA}}),
                MakeIntegrityBlock(key, ref, generation, 0, 1, {{0, 0xAA}})));
        });
        UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 0);
        UNIT_ASSERT_VALUES_EQUAL(notifications, (std::vector<int>{1, 2}));
        manager.PrepareTabletChunksDeletion(key.TabletId);
        manager.CommitTabletChunksDeletion(key.TabletId);
        UNIT_ASSERT(follower.GetResult());
        AssertChecksums(follower.GetResult()->Checksums, (std::vector<ui64>{0xAA, GetZeroBlockChecksum()}));
        UNIT_ASSERT_EQUAL(follower.GetResult()->ReadPlan.Kind, TReadPlan::Mixed);
        UNIT_ASSERT(follower.GetResult()->ReadPlan.UsedBlocks.Get(0));
        UNIT_ASSERT(!follower.GetResult()->ReadPlan.UsedBlocks.Get(1));
    }

    Y_UNIT_TEST(CachedReadSnapshotDoesNotWaitForNeighborMetadataWrite) {
        NIntegrityTest::TFixture manager(SmallChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{49, 0};
        TChunkIdx next = 990;
        MakeReady(manager, key, 2500, &next);
        manager.Write(key, 0, IntegrityUnitSize, {0xAA});
        CompleteWrites(manager, Drain(manager).Writes);

        auto writer = manager.Write(key, 2 * IntegrityUnitSize, IntegrityUnitSize, {0xBB});
        auto writes = Drain(manager).Writes;
        UNIT_ASSERT_VALUES_EQUAL(writes.size(), 1);
        NIntegrityTest::TFixture::TReadPreparation read;
        {
            read = manager.PrepareRead(key, 0, 2 * IntegrityUnitSize);
            manager.NotifyCompleted();
        }
        UNIT_ASSERT(read.Warm);
        UNIT_ASSERT(read.MetadataReads.empty());
        UNIT_ASSERT_EQUAL(read.Warm->Status, TIntegrityManager::EOperationStatus::Ok);
        UNIT_ASSERT_EQUAL(read.Warm->ReadPlan.Kind, TReadPlan::Mixed);
        AssertChecksums(read.Warm->Checksums, (std::vector<ui64>{0xAA, GetZeroBlockChecksum()}));
        UNIT_ASSERT(manager.Submissions.empty());
        UNIT_ASSERT(!writer.GetResult());
        CompleteWrites(manager, writes);
        UNIT_ASSERT_EQUAL(writer.GetResult()->Status, TIntegrityManager::EOperationStatus::Ok);
        AssertChecksums(read.Warm->Checksums, (std::vector<ui64>{0xAA, GetZeroBlockChecksum()}));
        UNIT_ASSERT(read.Warm->ReadPlan.UsedBlocks.Get(0));
        UNIT_ASSERT(!read.Warm->ReadPlan.UsedBlocks.Get(1));
    }

    Y_UNIT_TEST(StoppedReadPublishesFailureOnlyAfterSharedLoadsRetire) {
        NIntegrityTest::TFixture original(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        const TKey key{50, 0};
        TChunkIdx next = 1000;
        MakeReady(original, key, 2600, &next);
        NIntegrityTest::TFixture manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
        manager.ApplyMappingSnapshot(original.SnapshotMapping());
        auto read = manager.PrepareRead(key, 0, (ChecksumsPerIntegrityBlock + 1) * IntegrityUnitSize);
        UNIT_ASSERT(!read.Warm);
        UNIT_ASSERT_VALUES_EQUAL(read.MetadataReads.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(read.MetadataReads[0].Size, 2 * IntegrityPairSlots * IntegrityUnitSize);
        TActorWaiters waiters;
        size_t notifications = 0;
        waiters.Observe(read.Pending, notifications);
        waiters.Actor.RunSync([&] {
            manager.Stop();
        });
        UNIT_ASSERT_VALUES_EQUAL(notifications, 0);
        UNIT_ASSERT(!read.Pending.GetResult());
        UNIT_ASSERT(manager.HasInFlightOperationsForTablet(key.TabletId));
        waiters.Actor.RunSync([&] {
            TIntegrityManager::TMetadataReadResult result{read.MetadataReads[0].Id, {false, {}}};
            manager.CompleteMetadataReads(TConstArrayRef<TIntegrityManager::TMetadataReadResult>(&result, 1));
            manager.NotifyCompleted();
        });
        UNIT_ASSERT_VALUES_EQUAL(notifications, 1);
        UNIT_ASSERT(read.Pending.IsDone());
        UNIT_ASSERT_EQUAL(read.Pending.GetResult()->Status, TIntegrityManager::EOperationStatus::Failed);
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
        manager.PrepareTabletChunksDeletion(key.TabletId);
        manager.CommitTabletChunksDeletion(key.TabletId);
    }

    Y_UNIT_TEST(FailedWarmRmwImageEvictionRetainsCorruptionForKnownHolesAndWriters) {
        for (const bool lostWrite : {false, true}) {
            NIntegrityTest::TFixture original(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid);
            const TKey key{62, 0};
            TChunkIdx next = 1110;
            MakeReady(original, key, 2512, &next);
            const auto ref = *original.FindExtentRef(key);
            const auto generation = original.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
            NIntegrityTest::TFixture manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid,
                TIntegrityManager::BlockStateApproxBytes);
            manager.ApplyMappingSnapshot(original.SnapshotMapping());

            auto warmRead = PreparePendingRead(manager, key, 0, IntegrityUnitSize);
            auto warmLoads = Drain(manager).Reads;
            UNIT_ASSERT_VALUES_EQUAL(warmLoads.size(), 1);
            manager.CompleteRead(warmLoads.front(), MakeIntegrityPair(
                MakeIntegrityBlock(key, ref, generation, 0, 2, {{0, 0xAA}}),
                MakeIntegrityBlock(key, ref, generation, 0, 3, {{0, 0xAA}})));
            UNIT_ASSERT(warmRead.GetResult());
            AssertChecksums(warmRead.GetResult()->Checksums, (std::vector<ui64>{0xAA}));
            UNIT_ASSERT_EQUAL(ReadReadyResult(manager, key, IntegrityUnitSize, IntegrityUnitSize).ReadPlan.Kind,
                TReadPlan::AllZero);

            const ui32 firstBlock = ChecksumsPerIntegrityBlock - 1;
            auto writer = manager.PrepareWrite(key, firstBlock * IntegrityUnitSize, 2 * IntegrityUnitSize);
            UNIT_ASSERT(writer.IsReady());
            const ui64 checksums[] = {0xBB, 0xCC};
            auto context = manager.PrepareMetadataWrite(writer, checksums);
            UNIT_ASSERT_VALUES_EQUAL(context->ReadSize, 4 * IntegrityUnitSize);
            std::array<TIntegrityBlock, 4> disk{
                MakeIntegrityBlock(key, ref, generation, 0, 4, {}),
                MakeIntegrityBlock(key, ref, generation, 0, 5, {}),
                MakeIntegrityBlock(key, ref, generation, 1, 0, {}),
                MakeIntegrityBlock(key, ref, generation, 1, 1, {})};
            if (!lostWrite) {
                ++disk[0].Checksums[0];
                ++disk[1].Checksums[0];
            }
            auto region = TRcBuf::UninitializedPageAligned(sizeof(disk));
            memcpy(region.GetDataMut(), disk.data(), sizeof(disk));
            UNIT_ASSERT(!context->Transform(TReadPayload(std::move(region))));
            UNIT_ASSERT_EQUAL(context->Result.Status, TIntegrityManager::EOperationStatus::Corrupted);
            UNIT_ASSERT_VALUES_EQUAL(context->Result.LostWriteDetected, lostWrite);
            {
                manager.CompleteMetadataWrite(context, false);
                manager.NotifyCompleted();
            }
            UNIT_ASSERT_EQUAL(writer.GetResult()->Status, TIntegrityManager::EOperationStatus::Corrupted);
            UNIT_ASSERT_VALUES_EQUAL(writer.GetResult()->LostWriteDetected, lostWrite);
            UNIT_ASSERT_VALUES_EQUAL(manager.CachedBlockStates(), 1);

            // Loading a healthy third pair evicts the failed warm image, preserving its terminal state.
            const ui32 unrelatedBlock = 2 * ChecksumsPerIntegrityBlock;
            auto unrelated = PreparePendingRead(manager, key, unrelatedBlock * IntegrityUnitSize, IntegrityUnitSize);
            auto unrelatedLoads = Drain(manager).Reads;
            UNIT_ASSERT_VALUES_EQUAL(unrelatedLoads.size(), 1);
            manager.CompleteRead(unrelatedLoads.front(), MakeIntegrityPair(
                MakeIntegrityBlock(key, ref, generation, 2, 0, {{0, 0xDD}}),
                MakeIntegrityBlock(key, ref, generation, 2, 1, {{0, 0xDD}})));
            UNIT_ASSERT(unrelated.GetResult());
            UNIT_ASSERT_EQUAL(unrelated.GetResult()->Status, TIntegrityManager::EOperationStatus::Ok);
            AssertChecksums(unrelated.GetResult()->Checksums, (std::vector<ui64>{0xDD}));
            UNIT_ASSERT(manager.CachedBlockStates() <= manager.MaxCachedBlockStates());
            ui64 checksum = 0;
            UNIT_ASSERT(!manager.GetBlockChecksum(key, 0, &checksum));

            const auto preparation = manager.PrepareRead(key, IntegrityUnitSize, IntegrityUnitSize);
            UNIT_ASSERT(preparation.Warm);
            UNIT_ASSERT(preparation.MetadataReads.empty());
            UNIT_ASSERT_EQUAL(preparation.Warm->Status, TIntegrityManager::EOperationStatus::Corrupted);
            UNIT_ASSERT_VALUES_EQUAL(preparation.Warm->LostWriteDetected, lostWrite);
            auto nextWriter = manager.PrepareWrite(key, IntegrityUnitSize, IntegrityUnitSize);
            UNIT_ASSERT(nextWriter.IsReady());
            UNIT_ASSERT(nextWriter.GetResult());
            UNIT_ASSERT_EQUAL(nextWriter.GetResult()->Status, TIntegrityManager::EOperationStatus::Corrupted);
            UNIT_ASSERT_VALUES_EQUAL(nextWriter.GetResult()->LostWriteDetected, lostWrite);
            UNIT_ASSERT(manager.Submissions.empty());
            UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
            AssertChecksums(warmRead.GetResult()->Checksums, (std::vector<ui64>{0xAA}));
        }
    }

    void TestPinnedDigestDetectsLostWriteAfterEviction(bool rmw) {
        NIntegrityTest::TFixture manager(MultiBlockChunkSize, TestDDiskId, TestPDiskGuid,
            NIntegrityTest::TFixture::BlockStateApproxBytes);
        const TKey key{44, 0};
        TChunkIdx next = 940;
        MakeReady(manager, key, 2000, &next);
        const auto ref = *manager.FindExtentRef(key);
        const auto generation = manager.GetIntegrityChunkGeneration(ref.IntegrityChunkIdx);
        manager.Write(key, 0, IntegrityUnitSize, {0xAA});
        CompleteWrites(manager, Drain(manager).Writes);

        manager.Write(key, ChecksumsPerIntegrityBlock * IntegrityUnitSize, IntegrityUnitSize, {0xBB});
        CompleteWrites(manager, Drain(manager).Writes);

        ui64 checksum = 0;
        UNIT_ASSERT(!manager.GetBlockChecksum(key, 0, &checksum));
        UNIT_ASSERT_VALUES_EQUAL(manager.GetIntegrityBlockDigest(key, 0), Contribution(ref.VChunkGeneration, 0, 0xAA));
        TIntegrityManager::TWriteOperation writer;
        TIntegrityManager::TOperation reader;
        if (rmw) {
            writer = manager.Write(key, 0, IntegrityUnitSize, {0xCC});
        } else {
            reader = PreparePendingRead(manager, key, 0, IntegrityUnitSize);
        }
        auto reads = Drain(manager).Reads;
        UNIT_ASSERT_VALUES_EQUAL(reads.size(), 1);
        const auto stale = MakeIntegrityBlock(key, ref, generation, 0, 1, {});
        manager.CompleteRead(reads.front(), MakeIntegrityPair(stale, stale));
        const auto result = rmw ? ResultOf(writer) : ResultOf(reader);
        UNIT_ASSERT_EQUAL(result.Status, TIntegrityManager::EOperationStatus::Corrupted);
        UNIT_ASSERT(result.ErrorReason.Contains("digest mismatch"));
        UNIT_ASSERT(result.LostWriteDetected);
        UNIT_ASSERT(manager.Submissions.empty());
        UNIT_ASSERT(!manager.HasInFlightOperationsForTablet(key.TabletId));
    }

    Y_UNIT_TEST(PinnedDigestDetectsLostWriteAfterEviction) {
        TestPinnedDigestDetectsLostWriteAfterEviction(false);
    }

    Y_UNIT_TEST(PinnedDigestDetectsLostRmwAfterEviction) {
        TestPinnedDigestDetectsLostWriteAfterEviction(true);
    }

} // Y_UNIT_TEST_SUITE(TIntegrityManagerTest)

} // namespace NKikimr::NDDisk
