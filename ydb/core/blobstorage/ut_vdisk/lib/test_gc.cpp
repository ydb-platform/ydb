#include "test_gc.h"
#include "helpers.h"

using namespace NKikimr;

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Put Keep flags into empty db, than VGet data
SYNC_TEST_BEGIN(TGCPutKeepIntoEmptyDB, TSyncTestBase)
virtual void Scenario(const TActorContext &ctx) {
    TLogoBlobID generic(DefaultTestTabletId, 1, 2, 0, 0, 0);
    TLogoBlobID part0(generic, 1);
    TAllVDisks::TVDiskInstance &instance = Conf->VDisks->Get(0);

    // prepare gc command
    TAutoPtr<TVector<TLogoBlobID>> Keep(new TVector<TLogoBlobID>);
    Keep->push_back(generic);
    ui64 tabletID = DefaultTestTabletId;
    ui32 recGen = 1;
    ui32 recGenCounter = 1;
    ui32 channel = 0;
    bool collect = false;
    ui32 collectGen = 0;
    ui32 collectStep = 0;
    TAutoPtr<IActor> gcCommand(CreatePutGC(SyncRunner->NotifyID(), instance, tabletID, recGen, recGenCounter,
                                           channel, collect, collectGen, collectStep, Keep, nullptr));
    // set gc settings
    SyncRunner->Run(ctx, gcCommand);
    LOG_NOTICE(ctx, NActorsServices::TEST, "  GC Message sent");

    // read command
    TAutoPtr<IActor> readCmd;
    auto sendFunc = [part0, &instance](const TActorContext &ctx) {
        auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(instance.VDiskID,
                                                                   TInstant::Max(),
                                                                   NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                   TEvBlobStorage::TEvVGet::EFlags::None,
                                                                   {},
                                                                   {part0});
        ctx.Send(instance.ActorID, req.release());
    };
    auto checkFunc = [](TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        CheckQueryResult(ev, ctx, EQR_OK_NODATA, nullptr);
    };
    readCmd.Reset(CreateOneGet(SyncRunner->NotifyID(), sendFunc, checkFunc));
    SyncRunner->Run(ctx, readCmd);
}
SYNC_TEST_END(TGCPutKeepIntoEmptyDB, TSyncTestBase)


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SYNC_TEST_BEGIN(TGCPutBarrierVDisk0, TSyncTestWithSmallCommonDataset)
virtual void Scenario(const TActorContext &ctx) {
    // prepare gc command
    ui64 tabletID = DefaultTestTabletId;
    ui32 recGen = 1;
    ui32 recGenCounter = 1;
    ui32 channel = 0;
    bool collect = true;
    ui32 collectGen = 1;
    ui32 collectStep = 450;
    TAutoPtr<IActor> gcCommand(CreatePutGC(SyncRunner->NotifyID(), Conf->VDisks->Get(0), tabletID, recGen, recGenCounter,
                                           channel, collect, collectGen, collectStep, nullptr, nullptr));
    // set gc settings
    SyncRunner->Run(ctx, gcCommand);
    LOG_NOTICE(ctx, NActorsServices::TEST, "  GC Message sent");

    // wait for compaction
    SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf->VDisks->Get(0)));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");

    // load data
    SyncRunner->Run(ctx, ManyPutsToOneVDisk(SyncRunner->NotifyID(), Conf->VDisks->Get(0), &DataSet, 1,
                                            NKikimrBlobStorage::EPutHandleClass::TabletLog));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

    // wait for compaction
    SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf->VDisks->Get(0)));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");

    // read command
    TAutoPtr<IActor> readCmd;
    auto sendFunc = [this](const TActorContext &ctx) {
        TAllVDisks::TVDiskInstance &instance = Conf->VDisks->Get(0);
        TLogoBlobID from(DefaultTestTabletId, 4294967295, 4294967295, 0, TLogoBlobID::MaxBlobSize, 0, TLogoBlobID::MaxPartId);
        TLogoBlobID to  (DefaultTestTabletId, 0, 0, 0, 0, 0, 1);
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Test: from=%s to=%s\n", from.ToString().data(), to.ToString().data());
        auto req = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(instance.VDiskID,
                                                                  TInstant::Max(),
                                                                  NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                  TEvBlobStorage::TEvVGet::EFlags::None,
                                                                  {},
                                                                  from,
                                                                  to,
                                                                  10);
        ctx.Send(instance.ActorID, req.release());

        TString pppp("pppp");
        TString qqqqq("qqqqq");
        ExpectedSet.Put(TLogoBlobID(DefaultTestTabletId, 1, 472, 0, pppp.size(), 0), NKikimrProto::OK, {});
        ExpectedSet.Put(TLogoBlobID(DefaultTestTabletId, 1, 915, 0, qqqqq.size(), 0), NKikimrProto::OK, {});
    };
    auto checkFunc = [this](TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        CheckQueryResult(ev, ctx, EQR_OK_EXPECTED_SET, &ExpectedSet);
    };
    readCmd.Reset(CreateOneGet(SyncRunner->NotifyID(), sendFunc, checkFunc));
    SyncRunner->Run(ctx, readCmd);
}
SYNC_TEST_END(TGCPutBarrierVDisk0, TSyncTestWithSmallCommonDataset)


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SYNC_TEST_BEGIN(TGCPutBarrier, TSyncTestWithSmallCommonDataset)
    virtual void Scenario(const TActorContext &ctx) {
        // prepare gc command
        ui64 tabletID = DefaultTestTabletId;
        ui32 recGen = 1;
        ui32 recGenCounter = 1;
        ui32 channel = 0;
        bool collect = true;
        ui32 collectGen = 1;
        ui32 collectStep = 450;
        TAutoPtr<IActor> gcCommand(PutGCToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, tabletID, recGen,
                                                              recGenCounter, channel, collect, collectGen, collectStep,
                                                              nullptr, nullptr));
        // set gc settings
        SyncRunner->Run(ctx, gcCommand);
        LOG_NOTICE(ctx, NActorsServices::TEST, "  GC Message sent");

        // wait for sync
        SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");
        // wait for compaction
        SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");


        // load data
        SyncRunner->Run(ctx, ManyPutsToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, &DataSet));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

        // wait for sync
        SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");
        // wait for compaction
        SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");

        // read command
        TAutoPtr<IActor> readCmd;
        auto sendFunc = [this](const TActorContext &ctx) {
            TAllVDisks::TVDiskInstance &instance = Conf->VDisks->Get(0);
            TLogoBlobID from(DefaultTestTabletId, 4294967295, 4294967295, 0, TLogoBlobID::MaxBlobSize, 0, TLogoBlobID::MaxPartId);
            TLogoBlobID to  (DefaultTestTabletId, 0, 0, 0, 0, 0, 1);
            LOG_NOTICE(ctx, NActorsServices::TEST, "  Test: from=%s to=%s\n", from.ToString().data(), to.ToString().data());
            auto req = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(instance.VDiskID,
                                                                      TInstant::Max(),
                                                                      NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                      TEvBlobStorage::TEvVGet::EFlags::None,
                                                                      {},
                                                                      from,
                                                                      to,
                                                                      10);
            ctx.Send(instance.ActorID, req.release());

            TString pppp("pppp");
            TString qqqqq("qqqqq");
            ExpectedSet.Put(TLogoBlobID(DefaultTestTabletId, 1, 472, 0, pppp.size(), 0), NKikimrProto::OK, {});
            ExpectedSet.Put(TLogoBlobID(DefaultTestTabletId, 1, 915, 0, qqqqq.size(), 0), NKikimrProto::OK, {});
        };
        auto checkFunc = [this](TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
            CheckQueryResult(ev, ctx, EQR_OK_EXPECTED_SET, &ExpectedSet);
        };
        readCmd.Reset(CreateOneGet(SyncRunner->NotifyID(), sendFunc, checkFunc));
        SyncRunner->Run(ctx, readCmd);
    }
SYNC_TEST_END(TGCPutBarrier, TSyncTestWithSmallCommonDataset)


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SYNC_TEST_BEGIN(TGCPutKeepBarrier, TSyncTestWithSmallCommonDataset)
virtual void Scenario(const TActorContext &ctx) {
    // prepare gc command
    ui64 tabletID = DefaultTestTabletId;
    ui32 recGen = 1;
    ui32 recGenCounter = 2;
    ui32 channel = 0;
    bool collect = true;
    ui32 collectGen = 1;
    ui32 collectStep = 1000;
    TAutoPtr<TVector<NKikimr::TLogoBlobID>> keep(new TVector<NKikimr::TLogoBlobID>());
    TString qqqqq("qqqqq");
    keep->push_back(TLogoBlobID(DefaultTestTabletId, 1, 915, 0, qqqqq.size(), 0));
    TAutoPtr<IActor> gcCommand(PutGCToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, tabletID, recGen, recGenCounter,
                                                          channel, collect, collectGen, collectStep, keep, nullptr));
    // set gc settings
    SyncRunner->Run(ctx, gcCommand);
    LOG_NOTICE(ctx, NActorsServices::TEST, "  GC Message sent");

    // wait for sync
    SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");
    // wait for compaction
    SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");


    // load data
    SyncRunner->Run(ctx, ManyPutsToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, &DataSet));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

    // wait for sync
    SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");
    // wait for compaction
    SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");

    // read command
    TAutoPtr<IActor> readCmd;
    auto sendFunc = [this, qqqqq](const TActorContext &ctx) {
        TAllVDisks::TVDiskInstance &instance = Conf->VDisks->Get(0);
        TLogoBlobID from(DefaultTestTabletId, 4294967295, 4294967295, 0, TLogoBlobID::MaxBlobSize, 0, TLogoBlobID::MaxPartId);
        TLogoBlobID to  (DefaultTestTabletId, 0, 0, 0, 0, 0, 1);
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Test: from=%s to=%s\n", from.ToString().data(), to.ToString().data());
        auto req = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(instance.VDiskID,
                                                                  TInstant::Max(),
                                                                  NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                                                                  TEvBlobStorage::TEvVGet::EFlags::None,
                                                                  {},
                                                                  from,
                                                                  to,
                                                                  10);
        ctx.Send(instance.ActorID, req.release());

        ExpectedSet.Put(TLogoBlobID(DefaultTestTabletId, 1, 915, 0, qqqqq.size(), 0), NKikimrProto::OK, {});
    };
    auto checkFunc = [this](TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        CheckQueryResult(ev, ctx, EQR_OK_EXPECTED_SET, &ExpectedSet);
    };
    readCmd.Reset(CreateOneGet(SyncRunner->NotifyID(), sendFunc, checkFunc));
    SyncRunner->Run(ctx, readCmd);
}
SYNC_TEST_END(TGCPutKeepBarrier, TSyncTestWithSmallCommonDataset)


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SYNC_TEST_BEGIN(TGCManyVPutsCompactGCAll, TSyncTestWithSmallCommonDataset)
virtual void Scenario(const TActorContext &ctx) {
    // TEST: write some data, wait for it compaction, than collect all data via GC and wait until
    //       database is clear. We check that we can remove chunks w/o compaction using metadata info only

    // load data
    SyncRunner->Run(ctx, ManyPutsToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, &DataSet));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

    // wait for sync
    SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");
    // wait for compaction
    SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");

    // prepare gc command
    ui64 tabletID = DefaultTestTabletId;
    ui32 recGen = 1;
    ui32 recGenCounter = 2;
    ui32 channel = 0;
    bool collect = true;
    ui32 collectGen = 1;
    ui32 collectStep = 1000;
    TAutoPtr<IActor> gcCommand(PutGCToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, tabletID, recGen, recGenCounter,
                                                          channel, collect, collectGen, collectStep, nullptr, nullptr));
    // set gc settings
    SyncRunner->Run(ctx, gcCommand);
    LOG_NOTICE(ctx, NActorsServices::TEST, "  GC Message sent");

    // read until db is empty
    bool done = false;
    do {
        TAutoPtr<IActor> readCmd;
        auto sendFunc = [this](const TActorContext &ctx) {
            TAllVDisks::TVDiskInstance &instance = Conf->VDisks->Get(0);
            TLogoBlobID from(DefaultTestTabletId, 0, 0, 0, 0, 0, 1);
            TLogoBlobID to  (DefaultTestTabletId, 4294967295, 4294967295, 0, TLogoBlobID::MaxBlobSize, 0, TLogoBlobID::MaxPartId);
            LOG_NOTICE(ctx, NActorsServices::TEST, "  Test: from=%s to=%s", from.ToString().data(), to.ToString().data());
            auto req = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(instance.VDiskID,
                                                                      TInstant::Max(),
                                                                      NKikimrBlobStorage::EGetHandleClass::FastRead,
                                                                      TEvBlobStorage::TEvVGet::EFlags::None,
                                                                      {},
                                                                      from,
                                                                      to,
                                                                      10);
            ctx.Send(instance.ActorID, req.release());
        };
        auto checkFunc = [&done](TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
            TEvBlobStorage::TEvVGetResult *msg = ev->Get();
            Y_ABORT_UNLESS(msg->Record.GetStatus() == NKikimrProto::OK);
            done = msg->Record.ResultSize() == 0;
            LOG_NOTICE(ctx, NActorsServices::TEST, "  Test: emptyDb=%s", (done ? "true" : "false"));
        };
        readCmd.Reset(CreateOneGet(SyncRunner->NotifyID(), sendFunc, checkFunc));
        SyncRunner->Run(ctx, readCmd);
    } while (!done && (Sleep(TDuration::MilliSeconds(100)), true));
}
SYNC_TEST_END(TGCManyVPutsCompactGCAll, TSyncTestWithSmallCommonDataset)
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////




///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SYNC_TEST_BEGIN(TGCManyVPutsDelTablet, TSyncTestWithSmallCommonDataset)
virtual void Scenario(const TActorContext &ctx) {
    // TEST: write some data, wait for it compaction, than send 'complete table deletion' command
    // and wait until database is clear.

    // load data
    SyncRunner->Run(ctx, ManyPutsToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, &DataSet));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

    // wait for sync
    SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");
    // wait for compaction
    SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");

    // prepare gc command
    ui64 tabletID = DefaultTestTabletId;
    ui32 recGen = Max<ui32>();
    ui32 recGenCounter = Max<ui32>();
    ui32 channel = 0;
    bool collect = true;
    ui32 collectGen = Max<ui32>();  // complete tablet deletion
    ui32 collectStep = Max<ui32>();
    TAutoPtr<IActor> gcCommand(PutGCToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, tabletID, recGen, recGenCounter,
                                                          channel, collect, collectGen, collectStep, nullptr, nullptr));
    // set gc settings
    SyncRunner->Run(ctx, gcCommand);
    LOG_NOTICE(ctx, NActorsServices::TEST, "  GC Message sent");

    // read until db is empty
    bool done = false;
    do {
        TAutoPtr<IActor> readCmd;
        auto sendFunc = [this](const TActorContext &ctx) {
            TAllVDisks::TVDiskInstance &instance = Conf->VDisks->Get(0);
            TLogoBlobID from(DefaultTestTabletId, 0, 0, 0, 0, 0, 1);
            TLogoBlobID to  (DefaultTestTabletId, 4294967295, 4294967295, 0, TLogoBlobID::MaxBlobSize, 0, TLogoBlobID::MaxPartId);
            LOG_NOTICE(ctx, NActorsServices::TEST, "  Test: from=%s to=%s", from.ToString().data(), to.ToString().data());
            auto req = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(instance.VDiskID,
                                                                      TInstant::Max(),
                                                                      NKikimrBlobStorage::EGetHandleClass::FastRead,
                                                                      TEvBlobStorage::TEvVGet::EFlags::None,
                                                                      {},
                                                                      from,
                                                                      to,
                                                                      10);
            ctx.Send(instance.ActorID, req.release());
        };
        auto checkFunc = [&done](TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
            TEvBlobStorage::TEvVGetResult *msg = ev->Get();
            Y_ABORT_UNLESS(msg->Record.GetStatus() == NKikimrProto::OK);
            done = msg->Record.ResultSize() == 0;
            LOG_NOTICE(ctx, NActorsServices::TEST, "  Test: emptyDb=%s", (done ? "true" : "false"));
        };
        readCmd.Reset(CreateOneGet(SyncRunner->NotifyID(), sendFunc, checkFunc));
        SyncRunner->Run(ctx, readCmd);
    } while (!done && (Sleep(TDuration::MilliSeconds(100)), true));
}
SYNC_TEST_END(TGCManyVPutsDelTablet, TSyncTestWithSmallCommonDataset)
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
SYNC_TEST_BEGIN(TGCPutManyBarriers, TSyncTestWithSmallCommonDataset)

void SendGC(const TActorContext &ctx, const TGCSettings &s) {
    TAutoPtr<IActor> gcCommand;
    gcCommand.Reset(PutGCToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, s, nullptr, nullptr));
    SyncRunner->Run(ctx, gcCommand);
    LOG_NOTICE(ctx, NActorsServices::TEST, "  GC Message sent");
}

virtual void Scenario(const TActorContext &ctx) {
    // gc1
    TGCSettings s1 {0u, 1u, 1u, 0u, true, 1u, 36u}; // tabletID, recGen, recGenCounter, channel, collect?, collectGen, collectStep
    SendGC(ctx, s1);

    // gc2
    TGCSettings s2 {0u, 1u, 2u, 0u, true, 1u, 37u}; // tabletID, recGen, recGenCounter, channel, collect?, collectGen, collectStep
    SendGC(ctx, s2);

    // gc3
    TGCSettings s3 {0u, 1u, 3u, 1u, true, 1u, 15u}; // tabletID, recGen, recGenCounter, channel, collect?, collectGen, collectStep
    SendGC(ctx, s3);



    //TSmallCommonDataSet dataSet;
    // set gc settings

    // wait for sync
//    SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
//    LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");
    // wait for compaction
//    SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
//    LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");

/*
    // load data
    SyncRunner->Run(ctx, ManyPutsToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, &dataSet));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

    // wait for sync
    SyncRunner->Run(ctx, CreateWaitForSync(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  SYNC done");
    // wait for compaction
    SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
    LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");

    // read command
    TAutoPtr<IActor> readCmd;
    auto sendFunc = [this](const TActorContext &ctx) {
        TAutoPtr<TEvBlobStorage::TEvVGet> req;

        TAllVDisks::TVDiskInstance &instance = Conf->VDisks->Get(0);
        req.Reset(new TEvBlobStorage::TEvVGet(instance.VDiskID));
        TLogoBlobID from(0, 4294967295, 4294967295, 0, 0, TLogoBlobID::HashGeneric, 0, TLogoBlobID::MaxPartId);
        TLogoBlobID to  (0, 0, 0, 0, 0, TLogoBlobID::HashGeneric, 0, 1);
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Test: from=%s to=%s\n", ~from.ToString(), ~to.ToString());
        req->AddRangeQuery(from, to, 10, nullptr);
        ctx.Send(instance.ActorID, req.release());

        ExpectedSet.Put(TLogoBlobID(0, 1, 37, 0, 0, TLogoBlobID::HashGeneric, 0, 1), NKikimrProto::OK, "pppp");
        ExpectedSet.Put(TLogoBlobID(0, 1, 40, 0, 0, TLogoBlobID::HashGeneric, 0, 1), NKikimrProto::OK, "qqqqq");
    };
    auto checkFunc = [this](TEvBlobStorage::TEvVGetResult::TPtr &ev, const TActorContext &ctx) {
        CheckQueryResult(ev, ctx, EQR_OK_EXPECTED_SET, &ExpectedSet);
    };
    readCmd.Reset(CreateOneGet(SyncRunner->NotifyID(), sendFunc, checkFunc));
    SyncRunner->Run(ctx, readCmd);
 */
}
SYNC_TEST_END(TGCPutManyBarriers, TSyncTestWithSmallCommonDataset)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

