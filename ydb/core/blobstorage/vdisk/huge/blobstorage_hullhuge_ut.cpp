#include "blobstorage_hullhuge.h"
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
            ui32 minHugeBlobInBytes = 512u << 10u;
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
                    new THullHugeKeeperPersState(vctx, chunkSize, appendBlockSize, appendBlockSize,
                        minHugeBlobInBytes, milestoneHugeBlobInBytes, maxBlobInBytes,
                        overhead, freeChunksReservation, logf));

            state->LogPos = THullHugeRecoveryLogPos(0, 0, 100500, 50000, 70000, 56789, 39482);
            NHuge::THugeSlot hugeSlot(453, 0, 234);
            state->AllocatedSlots.insert(hugeSlot);

            TString serialized(state->Serialize());
            UNIT_ASSERT(THullHugeKeeperPersState::CheckEntryPoint(serialized));
        }
    }

} // NKikimr
