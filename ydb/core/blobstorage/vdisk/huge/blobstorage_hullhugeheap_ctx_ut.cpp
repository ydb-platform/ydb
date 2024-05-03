#include "defs.h"
#include "blobstorage_hullhugerecovery.h"
#include "blobstorage_hullhugeheap.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/null.h>
#include <util/generic/ptr.h>

#define STR Cnull

using namespace NKikimr;

namespace NKikimr {

    Y_UNIT_TEST_SUITE(THugeHeapCtxTests) {

        TTestContexts Contexts;
        constexpr ui32 ChunkSize = 135249920;
        constexpr ui32 AppendBlockSize = 4064;

        std::shared_ptr<THugeBlobCtx> CreateHugeBlobCtx() {
            TVDiskConfig cfg(TVDiskConfig::TBaseInfo::SampleForTests());
            auto logFunc = [] (const TString &) {};

            auto repairedHuge = std::make_shared<NHuge::THullHugeKeeperPersState>(
                    Contexts.GetVCtx(),
                    ChunkSize,
                    AppendBlockSize,
                    AppendBlockSize,
                    cfg.MinHugeBlobInBytes,
                    cfg.MilestoneHugeBlobInBytes,
                    cfg.MaxLogoBlobDataSize,
                    cfg.HugeBlobOverhead,
                    cfg.HugeBlobsFreeChunkReservation,
                    logFunc);

            return std::make_shared<THugeBlobCtx>(
                    repairedHuge->Heap->BuildHugeSlotsMap(),
                    true);
        }


        Y_UNIT_TEST(Basic) {
            auto hugeBlobCtx = CreateHugeBlobCtx();
            STR << "============ HugeSlotsMap:\n";
            STR << hugeBlobCtx->HugeSlotsMap->ToString();
            STR << "==========================\n";

            UNIT_ASSERT(*hugeBlobCtx->HugeSlotsMap->GetSlotInfo(600000) == THugeSlotsMap::TSlotInfo(662432, 204));
            UNIT_ASSERT(*hugeBlobCtx->HugeSlotsMap->GetSlotInfo(10000000) == THugeSlotsMap::TSlotInfo(10891520, 12));
        }
    }

} // NKikimr
