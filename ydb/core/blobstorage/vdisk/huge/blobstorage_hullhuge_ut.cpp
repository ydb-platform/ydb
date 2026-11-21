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

        Y_UNIT_TEST(SerializeWithSlotInFlightInChunkToShred) {
            // Serialization releases the slots in flight for the moment of saving the heap and occupies them again.
            // The only slot of a chunk that is waiting for shredding may belong to a write that is not logged yet.
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 56896u;
            ui32 milestoneHugeBlobInBytes = 512u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 2;
            ui32 hugeBlobSize = 6u << 20u;
            const ui32 chunkId = 5;

            auto logf = [] (const TString &state) { STR << state; };
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::Erasure4Plus2Block);
            auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters, TVDiskID(0, 1, 0, 0, 0),
                nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
            std::unique_ptr<THullHugeKeeperPersState> state(
                    new THullHugeKeeperPersState(vctx, chunkSize, appendBlockSize,
                        appendBlockSize, milestoneHugeBlobInBytes, maxBlobInBytes,
                        overhead, 0, false, freeChunksReservation, false, logf));

            state->Heap->AddChunk(chunkId);
            THugeSlot slot;
            ui32 slotSize = 0;
            UNIT_ASSERT(state->Heap->Allocate(hugeBlobSize, &slot, &slotSize));
            UNIT_ASSERT_VALUES_EQUAL(slot.GetChunkId(), chunkId);
            state->AddSlotInFlight(slot);
            state->Heap->ShredNotify({chunkId});

            for (const bool proto : {false, true}) {
                const TString serialized = proto ? state->SaveToProto() : state->Serialize();
                UNIT_ASSERT(THullHugeKeeperPersState::CheckEntryPoint(serialized));

                // the entry point must own the chunk, so that the put of this slot can be replayed after a restart
                std::unique_ptr<THullHugeKeeperPersState> restored(
                        new THullHugeKeeperPersState(vctx, chunkSize, appendBlockSize,
                            appendBlockSize, milestoneHugeBlobInBytes, maxBlobInBytes,
                            overhead, 0, false, freeChunksReservation, 0, TRcBuf(serialized), false, logf));
                restored->Heap->RecoveryModeAllocate(slot.GetDiskPart());
            }

            // the slot is still allocated and its chunk still waits for shredding
            UNIT_ASSERT(state->Heap->GetForbiddenChunks().contains(chunkId));
            UNIT_ASSERT(state->DeleteSlotInFlight(slot));
            state->Heap->Free(slot.GetDiskPart());
            UNIT_ASSERT_VALUES_EQUAL(state->Heap->RemoveChunk(), chunkId);
        }
    }

} // NKikimr
