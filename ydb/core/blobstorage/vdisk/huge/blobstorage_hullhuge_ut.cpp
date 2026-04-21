#include "blobstorage_hullhuge.h"
#include "blobstorage_hullhugeheap.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>


// change to Cerr if you want logging
#define STR Cnull


namespace NKikimr {

    using namespace NHuge;

    Y_UNIT_TEST_SUITE(TBlobStorageHullHugeKeeperPersState) {

        Y_UNIT_TEST(SerializeParse) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 56896u;
            ui32 milestoneHugeBlobInBytes = 512u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 2;

            auto logf = [] (const TString &state) { STR << state; };
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::Erasure4Plus2Block);
            auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters, TVDiskID(0, 1, 0, 0, 0),
                nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
            std::unique_ptr<THullHugeKeeperPersState> state(
                    new THullHugeKeeperPersState(vctx, chunkSize, appendBlockSize,
                        appendBlockSize, milestoneHugeBlobInBytes, maxBlobInBytes,
                        overhead, 0, false, freeChunksReservation, false, logf));

            state->LogPos = THullHugeRecoveryLogPos(0, 0, 100500, 50000, 70000, 56789, 39482);

            TString serialized(state->Serialize());
            UNIT_ASSERT(THullHugeKeeperPersState::CheckEntryPoint(serialized));
        }

        Y_UNIT_TEST(ChunksSoftLockingIsPropagatedInEntryPointCtor) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 56896u;
            ui32 milestoneHugeBlobInBytes = 512u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 2;
            ui32 hugeBlobSize = 6u << 20u;
            ui64 entryPointLsn = 0;

            auto logf = [] (const TString &state) { STR << state; };
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::Erasure4Plus2Block);
            auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters, TVDiskID(0, 1, 0, 0, 0),
                nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);

            // Use proto entry point to force ParseFromArray -> LoadFromProto path.
            std::unique_ptr<THullHugeKeeperPersState> initial(
                    new THullHugeKeeperPersState(vctx, chunkSize, appendBlockSize,
                        appendBlockSize, milestoneHugeBlobInBytes, maxBlobInBytes,
                        overhead, 0, false, freeChunksReservation, false, logf));
            TString serialized(initial->SaveToProto());
            UNIT_ASSERT(THullHugeKeeperPersState::CheckEntryPoint(serialized));

            std::unique_ptr<THullHugeKeeperPersState> restored(
                    new THullHugeKeeperPersState(vctx, chunkSize, appendBlockSize,
                        appendBlockSize, milestoneHugeBlobInBytes, maxBlobInBytes,
                        overhead, 0, false, freeChunksReservation, entryPointLsn,
                        TRcBuf(serialized), true, logf));

            restored->Heap->AddChunk(5);
            restored->Heap->AddChunk(3);

            THugeSlot slotFromFirstChunk;
            THugeSlot slotFromSecondChunk;
            THugeSlot slot;
            ui32 slotSize = 0;

            for (ui32 i = 0; i < restored->Heap->SlotNumberOfThisSize(hugeBlobSize); ++i) {
                UNIT_ASSERT(restored->Heap->Allocate(hugeBlobSize, &slotFromFirstChunk, &slotSize));
            }

            UNIT_ASSERT(restored->Heap->Allocate(hugeBlobSize, &slotFromSecondChunk, &slotSize));
            const ui32 lockedChunkId = slotFromSecondChunk.GetChunkId();
            restored->Heap->LockChunkForAllocation(lockedChunkId, slotSize);

            restored->Heap->Free(slotFromFirstChunk.GetDiskPart());
            UNIT_ASSERT(restored->Heap->Allocate(hugeBlobSize, &slot, &slotSize));
            UNIT_ASSERT_VALUES_UNEQUAL(slot.GetChunkId(), lockedChunkId);

            // In soft-locking mode allocator may steal from locked chunks when no other free slots remain.
            UNIT_ASSERT(restored->Heap->Allocate(hugeBlobSize, &slot, &slotSize));
            UNIT_ASSERT_VALUES_EQUAL(slot.GetChunkId(), lockedChunkId);
        }
    }

} // NKikimr
