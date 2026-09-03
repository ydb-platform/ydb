#include "blobstorage_hullhuge.h"
#include "blobstorage_hullhugeheap.h"
#include "blobstorage_hullhugedefs.h"
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

        Y_UNIT_TEST(StripeHeapEntryPointRoundTrip) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 56896u;
            ui32 milestoneHugeBlobInBytes = 512u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 2;
            ui64 entryPointLsn = 39482;

            auto logf = [] (const TString &state) { STR << state; };
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::Erasure4Plus2Block);
            auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters, TVDiskID(0, 1, 0, 0, 0),
                nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);

            std::unique_ptr<THullHugeKeeperPersState> initial(
                    new THullHugeKeeperPersState(vctx, chunkSize, appendBlockSize,
                        appendBlockSize, milestoneHugeBlobInBytes, maxBlobInBytes,
                        overhead, 0, false, freeChunksReservation, false, logf));
            initial->LogPos = THullHugeRecoveryLogPos(0, 0, 100500, 50000, 70000, 56789, entryPointLsn);

            THugeSlot stripeSlot;
            initial->StripeHeap->Allocate(100, &stripeSlot, 11);
            THugeSlot slotSlot;
            ui32 slotSize = 0;
            initial->Heap->AddChunk(12);
            UNIT_ASSERT(initial->Heap->Allocate(milestoneHugeBlobInBytes, &slotSlot, &slotSize));

            TSet<TChunkIdx> owned;
            initial->GetOwnedChunks(owned);
            UNIT_ASSERT(owned.contains(11));
            UNIT_ASSERT(owned.contains(12));
            // the stripe chunk and the slot chunk are owned exclusively, each is reported exactly once
            UNIT_ASSERT_VALUES_EQUAL(owned.size(), 2u);

            TString serialized(initial->Serialize());
            UNIT_ASSERT(THullHugeKeeperPersState::CheckEntryPoint(serialized));

            std::unique_ptr<THullHugeKeeperPersState> restored(
                    new THullHugeKeeperPersState(vctx, chunkSize, appendBlockSize,
                        appendBlockSize, milestoneHugeBlobInBytes, maxBlobInBytes,
                        overhead, 0, false, freeChunksReservation, entryPointLsn,
                        TRcBuf(serialized), true, logf));
            UNIT_ASSERT(restored->StripeHeap->ContainsChunk(11));

            // the entry point carries the chunk but not its contents; the extent is live only because the recovered
            // database still points at it
            restored->RecoveryOccupyDerived(TDiskPart(11, stripeSlot.GetOffset(), 1));
            restored->FinishStripeDerivation();
            UNIT_ASSERT(restored->StripeHeap->ContainsChunk(11));

            THugeSlot conv = restored->ConvertDiskPart(TDiskPart(11, stripeSlot.GetOffset(), 1));
            UNIT_ASSERT_VALUES_EQUAL(conv.GetChunkId(), 11u);
            TFreeRes freed = restored->FreeBlob(TDiskPart(11, stripeSlot.GetOffset(), 1));
            UNIT_ASSERT_VALUES_EQUAL(freed.ChunkId, 11u);
            UNIT_ASSERT(!restored->StripeHeap->ContainsChunk(11));
        }

        // A chunk goes back to the slot heap the moment its last stripe is freed, and nothing in the log says so:
        // stripes are not tracked during replay at all. So replay would carry the claim forward forever, and the
        // later records that use the chunk as slot storage or hand it back to PDisk would look for it in a heap that
        // no longer has it. Those records are exactly what retires the claim.
        Y_UNIT_TEST(StripeChunkClaimIsRetiredWhenTheChunkLeavesTheStripeHeap) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 56896u;
            ui32 milestoneHugeBlobInBytes = 512u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 0;

            auto logf = [] (const TString &state) { STR << state; };
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::Erasure4Plus2Block);
            auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters, TVDiskID(0, 1, 0, 0, 0),
                nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);

            std::unique_ptr<THullHugeKeeperPersState> state(
                    new THullHugeKeeperPersState(vctx, chunkSize, appendBlockSize,
                        appendBlockSize, milestoneHugeBlobInBytes, maxBlobInBytes,
                        overhead, 0, false, freeChunksReservation, false, logf));
            state->StripeAllocatorEnabled = true;

            // replaying a huge blob written into a stripe: the chunk moves from the slot heap to the stripe heap
            state->Heap->AddChunk(7);
            state->RecoveryClaimStripeChunk(7);
            UNIT_ASSERT(state->StripeHeap->ContainsChunk(7));

            // replaying a slot allocation in the same chunk: the two never hold live data in one chunk at once, so
            // this record proves the stripes are long gone and the chunk is the slot heap's again
            state->RecoveryReleaseStripeChunk(7);
            UNIT_ASSERT(!state->StripeHeap->ContainsChunk(7));
            state->StripeAllocatorEnabled = false;
            THugeSlot slot;
            ui32 slotSize = 0;
            UNIT_ASSERT(state->AllocateBlob(milestoneHugeBlobInBytes, &slot, &slotSize));
            UNIT_ASSERT_VALUES_EQUAL(slot.GetChunkId(), 7u);

            // and the same for a chunk handed back to PDisk while a claim was still outstanding
            state->StripeAllocatorEnabled = true;
            state->Heap->AddChunk(9);
            state->RecoveryClaimStripeChunk(9);
            state->RecoveryReleaseStripeChunk(9);
            state->Heap->RecoveryModeRemoveChunks(TVector<ui32>{9});

            // releasing a chunk the stripe heap never had is a no-op, not a gift to the slot heap
            state->RecoveryReleaseStripeChunk(9);
            TSet<TChunkIdx> owned;
            state->GetOwnedChunks(owned);
            UNIT_ASSERT(!owned.contains(9));
        }

        Y_UNIT_TEST(StripeAllocatorRouting) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 56896u;
            ui32 milestoneHugeBlobInBytes = 512u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 0;

            auto logf = [] (const TString &state) { STR << state; };
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::Erasure4Plus2Block);
            auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters, TVDiskID(0, 1, 0, 0, 0),
                nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);

            std::unique_ptr<THullHugeKeeperPersState> state(
                    new THullHugeKeeperPersState(vctx, chunkSize, appendBlockSize,
                        appendBlockSize, milestoneHugeBlobInBytes, maxBlobInBytes,
                        overhead, 0, false, freeChunksReservation, false, logf));

            state->StripeAllocatorEnabled = true;
            state->Heap->AddChunk(7);
            THugeSlot stripe;
            ui32 key = 0;
            UNIT_ASSERT(state->AllocateBlob(100, &stripe, &key));
            UNIT_ASSERT_VALUES_EQUAL(key, Max<ui32>());
            UNIT_ASSERT(state->StripeHeap->ContainsChunk(7));
            UNIT_ASSERT_VALUES_EQUAL(stripe.GetChunkId(), 7u);

            state->StripeAllocatorEnabled = false;
            state->Heap->AddChunk(8);
            THugeSlot slot;
            ui32 slotKey = 0;
            UNIT_ASSERT(state->AllocateBlob(milestoneHugeBlobInBytes, &slot, &slotKey));
            UNIT_ASSERT_VALUES_UNEQUAL(slotKey, Max<ui32>());
            UNIT_ASSERT(!state->StripeHeap->ContainsChunk(8));
            UNIT_ASSERT_VALUES_EQUAL(slot.GetChunkId(), 8u);
        }

        Y_UNIT_TEST(StripeChunkSizeAccountingOnLastFree) {
            ui32 chunkSize = 134274560u;
            ui32 appendBlockSize = 56896u;
            ui32 milestoneHugeBlobInBytes = 512u << 10u;
            ui32 maxBlobInBytes = 10u << 20u;
            ui32 overhead = 8;
            ui32 freeChunksReservation = 0;

            auto logf = [] (const TString &state) { STR << state; };
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::Erasure4Plus2Block);
            auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters, TVDiskID(0, 1, 0, 0, 0),
                nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);

            std::unique_ptr<THullHugeKeeperPersState> state(
                    new THullHugeKeeperPersState(vctx, chunkSize, appendBlockSize,
                        appendBlockSize, milestoneHugeBlobInBytes, maxBlobInBytes,
                        overhead, 0, false, freeChunksReservation, false, logf));

            // this is exactly what THullHugeKeeper does upon write and upon deletion; the chunk migrates back to the
            // slot heap once its last stripe is freed, so stripe-ness must be sampled before the blob is freed
            auto writeAndDelete = [&](ui32 blobSize) {
                THugeSlot slot;
                ui32 key = 0;
                UNIT_ASSERT(state->AllocateBlob(blobSize, &slot, &key));
                state->AddSlotInFlight(slot);
                state->AddChunkSize(slot);

                const TDiskPart addr = slot.GetDiskPart();
                const bool isStripe = state->IsStripeAddr(addr);
                THugeSlot hugeSlot = state->ConvertDiskPart(addr);
                UNIT_ASSERT(state->DeleteSlotInFlight(hugeSlot));
                state->FreeBlob(addr);
                if (!isStripe) {
                    state->DeleteChunkSize(hugeSlot);
                }
                return isStripe;
            };

            state->StripeAllocatorEnabled = true;
            state->Heap->AddChunk(7);
            UNIT_ASSERT(writeAndDelete(100));
            UNIT_ASSERT(!state->StripeHeap->ContainsChunk(7));

            // freeing the last stripe hands the chunk back to the slot heap, so it is reusable right away
            state->StripeAllocatorEnabled = false;
            UNIT_ASSERT(!writeAndDelete(milestoneHugeBlobInBytes));
        }

        Y_UNIT_TEST(PutRecoveryLogRecStripeFlag) {
            TLogoBlobID id(1, 1, 1, 0, 100, 0, 1);
            TPutRecoveryLogRec slotRec(id, TIngress(), TDiskPart(5, 0, 100), false);
            TPutRecoveryLogRec stripeRec(id, TIngress(), TDiskPart(5, 0, 100), true);
            TString slotData = slotRec.Serialize();
            TString stripeData = stripeRec.Serialize();
            UNIT_ASSERT_VALUES_EQUAL(stripeData.size(), slotData.size() + 1);

            TPutRecoveryLogRec parsedSlot;
            UNIT_ASSERT(parsedSlot.ParseFromString(slotData));
            UNIT_ASSERT(!parsedSlot.IsStripe);

            TPutRecoveryLogRec parsedStripe;
            UNIT_ASSERT(parsedStripe.ParseFromString(stripeData));
            UNIT_ASSERT(parsedStripe.IsStripe);
            UNIT_ASSERT_VALUES_EQUAL(parsedStripe.DiskAddr.ChunkIdx, 5u);
        }
    }

} // NKikimr
