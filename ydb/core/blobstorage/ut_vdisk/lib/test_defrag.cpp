#include "test_defrag.h"
#include "helpers.h"
#include <util/stream/null.h>

using namespace NKikimr;

#define STR Cnull

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Defrag empty database
SYNC_TEST_BEGIN(TDefragEmptyDB, TSyncTestBase)
virtual void Scenario(const TActorContext &ctx) {
    // check compaction result
    auto check = [] (TEvBlobStorage::TEvVDefragResult::TPtr &ev) {
        const auto &rec = ev->Get()->Record;
        Y_ABORT_UNLESS(rec.GetEof());
        Y_ABORT_UNLESS(rec.GetFoundChunksToDefrag() == 0);
        Y_ABORT_UNLESS(rec.GetRewrittenRecs() == 0);
        Y_ABORT_UNLESS(rec.GetRewrittenBytes() == 0);
    };

    TAutoPtr<IActor> defragCmd(CreateDefrag(SyncRunner->NotifyID(), Conf, true, check));
    SyncRunner->Run(ctx, defragCmd);
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Defrag completed");
}
SYNC_TEST_END(TDefragEmptyDB, TSyncTestBase)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// we write data for two tablets, then garbage collect data from one tablet, expect 50% of huge blobs collected,
// then run defragmentation
SYNC_TEST_BEGIN(TDefrag50PercentGarbage, TSyncTestBase)
virtual void Scenario(const TActorContext &ctx) {
    TIntrusivePtr<TBlobStorageGroupInfo> ginfo = Conf->GroupInfo;
    TVector<TVDiskID> vdisks = Conf->VDisks->GetVDiskIds();
    TPDiskPutStatusHandler hndl = PDiskPutStatusHandlerDefault;

    // load huge and small blobs
    const ui32 maxBlobs = Max<ui32>();
    const ui64 hugeMaxDataSize = 20 << 20;
    const ui32 hugeMinBlobSize = 64 << 10;
    const ui32 hugeMaxBlobSize = 64 << 10;
    const ui64 smallMaxDataSize = 1 << 20;
    const ui32 smallMinBlobSize = 16;
    const ui32 smallMaxBlobSize = 16 << 10;
    // it generates huge data for tablets with ids 0 and 1
    TGeneratedDataSet dataSetHuge(CreateBlobGenerator(hugeMaxDataSize, maxBlobs, hugeMinBlobSize, hugeMaxBlobSize,
            2, 1, ginfo, vdisks));
    SyncRunner->Run(ctx, ManyPutsToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, &dataSetHuge, hndl));
    // it generates small data for tablets with ids 0 and 1
    TGeneratedDataSet dataSetSmall(CreateBlobGenerator(smallMaxDataSize, maxBlobs, smallMinBlobSize, smallMaxBlobSize,
            2, 100000, ginfo, vdisks));
    SyncRunner->Run(ctx, ManyPutsToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, &dataSetSmall, hndl));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

    // prepare gc command -- it deletes all data from one tablet
    TGCSettings settings;
    settings.TabletID = DefaultTestTabletId;
    settings.RecGen = 1;
    settings.RecGenCounter = 1;
    settings.Channel = 0;
    settings.Collect = true;
    settings.CollectGen = 2;
    settings.CollectStep = 1000000;
    TAutoPtr<IActor> gcCommand(PutGCToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, settings, nullptr, nullptr));

    // set gc settings
    SyncRunner->Run(ctx, gcCommand);
    LOG_NOTICE(ctx, NActorsServices::TEST, "  GC Message sent");

    // syncronize (need to sync barriers)
    SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));

    // check compaction result
    ui32 freedChunks = 0;
    auto check = [&freedChunks] (TEvBlobStorage::TEvVDefragResult::TPtr &ev) {
        const auto &rec = ev->Get()->Record;
        freedChunks += rec.GetFreedChunks().size();
        STR << "FoundChunksToDefrag# " << rec.GetFoundChunksToDefrag()
            << " RewrittenRecs# " << rec.GetRewrittenRecs()
            << " RewrittenBytes# " << rec.GetRewrittenBytes()
            << " FreedChunks# " << FormatList(rec.GetFreedChunks()) << "\n";
    };

    TAllVDisks::TVDiskInstance &instance = Conf->VDisks->Get(0);

    // wait for compaction
    SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), instance, true));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");

    // now defrag only one disk
    TAutoPtr<IActor> defragCmd(CreateDefrag(SyncRunner->NotifyID(), instance, true, check));
    SyncRunner->Run(ctx, defragCmd);
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Defrag completed");

    // repeat
    defragCmd.Reset(CreateDefrag(SyncRunner->NotifyID(), instance, true, check));
    SyncRunner->Run(ctx, defragCmd);
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Defrag completed");

    // repeat
    defragCmd.Reset(CreateDefrag(SyncRunner->NotifyID(), instance, true, check));
    SyncRunner->Run(ctx, defragCmd);
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Defrag completed");

    // check actually freed chunks
    UNIT_ASSERT_VALUES_EQUAL(freedChunks, 3);
}
SYNC_TEST_END(TDefrag50PercentGarbage, TSyncTestBase)

