#include "test_many.h"
#include "helpers.h"

#include <util/generic/set.h>

using namespace NKikimr;


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TManyPutOneGetActor : public TSyncTestBase {
protected:
    const bool WaitForCompaction;
    std::shared_ptr<TVector<TMsgPackInfo>> MsgPacks;
    const ui64 TabletId;
    const ui64 Shift;
    std::shared_ptr<IPutHandleClassGenerator> HandleClassGen;
    std::shared_ptr<TSet<ui32>> BadSteps;
    const bool WithErrorResponse;

    virtual void Scenario(const TActorContext &ctx) {
        // load data
        SyncRunner->Run(ctx, CreateManyPuts(Conf, SyncRunner->NotifyID(), Conf->VDisks->Get(0), MsgPacks,
                                            TabletId, 0, 1, HandleClassGen, BadSteps, TDuration::Seconds(0)));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

        // wait for compaction
        if (WaitForCompaction) {
            SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
            LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");
        }

        // read
        SyncRunner->Run(ctx, CreateGet(SyncRunner->NotifyID(), Conf->VDisks->Get(0), MsgPacks, TabletId, 0, 1, Shift,
                                       WithErrorResponse));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  GET done");
    }


public:
    TManyPutOneGetActor(TConfiguration *conf, bool waitForCompaction, ui32 msgNum, ui32 msgSize,
                        ui64 tabletId, ui64 shift, NKikimrBlobStorage::EPutHandleClass cls,
                        bool withErrorResponse)
        : TSyncTestBase(conf)
        , WaitForCompaction(waitForCompaction)
        , MsgPacks(new TVector<TMsgPackInfo>{TMsgPackInfo(msgSize, msgNum)})
        , TabletId(tabletId)
        , Shift(shift)
        , HandleClassGen(std::make_shared<TPutHandleClassGenerator>(cls))
        , BadSteps(std::make_shared<TSet<ui32>>())
        , WithErrorResponse(withErrorResponse)
    {}

    TManyPutOneGetActor(TConfiguration *conf, bool waitForCompaction, std::shared_ptr<TVector<TMsgPackInfo>> msgPacks,
                        ui64 tabletId, ui64 shift, NKikimrBlobStorage::EPutHandleClass cls,
                        bool withErrorResponse)
        : TSyncTestBase(conf)
        , WaitForCompaction(waitForCompaction)
        , MsgPacks(msgPacks)
        , TabletId(tabletId)
        , Shift(shift)
        , HandleClassGen(std::make_shared<TPutHandleClassGenerator>(cls))
        , BadSteps(std::make_shared<TSet<ui32>>())
        , WithErrorResponse(withErrorResponse)
    {}
};

void TManyPutOneGet::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TManyPutOneGetActor(conf, WaitForCompaction, MsgPacks, TabletId, Shift,
                                                         HandleClass, WithErrorResponse));
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TManyPutGetActor : public TSyncTestBase {
protected:
    const bool WaitForCompaction;
    const ui32 MsgNum;
    const ui32 MsgSize;
    const ui64 TabletId;
    std::shared_ptr<IPutHandleClassGenerator> HandleClassGen;
    std::shared_ptr<TSet<ui32>> BadSteps;

    virtual void Scenario(const TActorContext &ctx) {
        // load data
        SyncRunner->Run(ctx, CreateManyPuts(Conf, SyncRunner->NotifyID(), Conf->VDisks->Get(0), MsgSize, MsgNum,
                                            TabletId, 0, 1, HandleClassGen, BadSteps, TDuration::Seconds(0)));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

        // wait for compaction
        if (WaitForCompaction) {
            SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
            LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");
        }

        // read
        SyncRunner->Run(ctx, CreateManyGets(SyncRunner->NotifyID(), Conf->VDisks->Get(0), MsgSize, MsgNum,
                                            TabletId, 0, 1, BadSteps));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  GET done");
    }


public:
    TManyPutGetActor(TConfiguration *conf, bool waitForCompaction, ui32 msgNum, ui32 msgSize,
                     ui64 tabletId, NKikimrBlobStorage::EPutHandleClass cls)
        : TSyncTestBase(conf)
        , WaitForCompaction(waitForCompaction)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , TabletId(tabletId)
        , HandleClassGen(std::make_shared<TPutHandleClassGenerator>(cls))
        , BadSteps(std::make_shared<TSet<ui32>>())
    {}
};

void TManyPutGet::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TManyPutGetActor(conf, WaitForCompaction, MsgNum, MsgSize, TabletId, HandleClass));
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TManyMultiPutGetActor : public TSyncTestBase {
protected:
    const bool WaitForCompaction;
    const ui32 MsgNum;
    const ui32 MsgSize;
    const ui32 BatchSize;
    const ui64 TabletId;
    std::shared_ptr<IPutHandleClassGenerator> HandleClassGen;
    std::shared_ptr<TSet<ui32>> BadSteps;

    virtual void Scenario(const TActorContext &ctx) {
        // load data
        SyncRunner->Run(ctx, CreateManyMultiPuts(Conf, SyncRunner->NotifyID(), Conf->VDisks->Get(0),
                                            MsgSize, MsgNum, BatchSize, TabletId, 0, 1, HandleClassGen,
                                            BadSteps, TDuration::Seconds(0)));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

        // wait for compaction
        if (WaitForCompaction) {
            SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
            LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");
        }

        // read
        SyncRunner->Run(ctx, CreateManyGets(SyncRunner->NotifyID(), Conf->VDisks->Get(0), MsgSize, MsgNum,
                                            TabletId, 0, 1, BadSteps));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  GET done");
    }


public:
    TManyMultiPutGetActor(TConfiguration *conf, bool waitForCompaction, ui32 msgNum, ui32 msgSize, ui32 batchSize,
                     ui64 tabletId, NKikimrBlobStorage::EPutHandleClass cls)
        : TSyncTestBase(conf)
        , WaitForCompaction(waitForCompaction)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , BatchSize(batchSize)
        , TabletId(tabletId)
        , HandleClassGen(std::make_shared<TPutHandleClassGenerator>(cls))
        , BadSteps(std::make_shared<TSet<ui32>>())
    {}
};

void TManyMultiPutGet::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(
        new TManyMultiPutGetActor(conf, WaitForCompaction, MsgNum, MsgSize, BatchSize, TabletId, HandleClass));
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TManyPutRangeGetActor : public TSyncTestBase {
protected:
    const bool WaitForCompaction;
    const bool IndexOnly;
    const ui32 MsgNum;
    const ui32 MsgSize;
    std::shared_ptr<IPutHandleClassGenerator> HandleClassGen;
    std::shared_ptr<TSet<ui32>> BadSteps;

    virtual void Scenario(const TActorContext &ctx) {
        // load data
        SyncRunner->Run(ctx, CreateManyPuts(Conf, SyncRunner->NotifyID(), Conf->VDisks->Get(0), MsgSize, MsgNum,
                                            DefaultTestTabletId, 0, 1, HandleClassGen, BadSteps, TDuration::Seconds(0)));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");

        // wait for compaction
        if (WaitForCompaction) {
            SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
            LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");
        }

        // range read
        TLogoBlobID readFrom(DefaultTestTabletId, 4294967295, 4294967295, 0, 0, 0, TLogoBlobID::MaxPartId);
        TLogoBlobID readTo  (DefaultTestTabletId, 0, 0, 0, 0, 0, 1);
        SyncRunner->Run(ctx, CreateRangeGet(SyncRunner->NotifyID(), Conf->VDisks->Get(0), readFrom, readTo, IndexOnly, MsgNum));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  RANGE GET done");
    }


public:
    TManyPutRangeGetActor(TConfiguration *conf, bool waitForCompaction, bool indexOnly, ui32 msgNum, ui32 msgSize,
                          NKikimrBlobStorage::EPutHandleClass cls)
        : TSyncTestBase(conf)
        , WaitForCompaction(waitForCompaction)
        , IndexOnly(indexOnly)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , HandleClassGen(std::make_shared<TPutHandleClassGenerator>(cls))
        , BadSteps(std::make_shared<TSet<ui32>>())
    {
        Y_ABORT_UNLESS(indexOnly);
    }
};

void TManyPutRangeGet::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TManyPutRangeGetActor(conf, WaitForCompaction, IndexOnly, MsgNum, MsgSize,
                                                           HandleClass));
}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TManyPutRangeGet2ChannelsActor : public TSyncTestBase {
protected:
    const bool WaitForCompaction;
    const bool IndexOnly;
    const ui32 MsgNum;
    const ui32 MsgSize;
    std::shared_ptr<IPutHandleClassGenerator> HandleClassGen1;
    std::shared_ptr<IPutHandleClassGenerator> HandleClassGen2;
    std::shared_ptr<TSet<ui32>> BadSteps;

    virtual void Scenario(const TActorContext &ctx) {
        // load data 1
        SyncRunner->Run(ctx, CreateManyPuts(Conf, SyncRunner->NotifyID(), Conf->VDisks->Get(0), MsgSize, MsgNum,
                                            DefaultTestTabletId, 0, 1, HandleClassGen1, BadSteps, TDuration::Seconds(0)));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Data1 is loaded");

        // load data 2
        SyncRunner->Run(ctx, CreateManyPuts(Conf, SyncRunner->NotifyID(), Conf->VDisks->Get(0), MsgSize, MsgNum,
                                            DefaultTestTabletId, 1, 1, HandleClassGen2, BadSteps, TDuration::Seconds(0)));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Data2 is loaded");

        // wait for compaction
        if (WaitForCompaction) {
            SyncRunner->Run(ctx, CreateWaitForCompaction(SyncRunner->NotifyID(), Conf));
            LOG_NOTICE(ctx, NActorsServices::TEST, "  COMPACTION done");
        }

        // range read
        TLogoBlobID readFrom(DefaultTestTabletId, 4294967295, 4294967295, 0, 0, 0, TLogoBlobID::MaxPartId);
        TLogoBlobID readTo  (DefaultTestTabletId, 0, 0, 0, 0, 0, 1);
        SyncRunner->Run(ctx, CreateRangeGet(SyncRunner->NotifyID(), Conf->VDisks->Get(0), readFrom, readTo, IndexOnly, MsgNum));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  RANGE GET done");
    }

public:
    TManyPutRangeGet2ChannelsActor(TConfiguration *conf, bool waitForCompaction, bool indexOnly, ui32 msgNum,
                                   ui32 msgSize, NKikimrBlobStorage::EPutHandleClass cls)
        : TSyncTestBase(conf)
        , WaitForCompaction(waitForCompaction)
        , IndexOnly(indexOnly)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , HandleClassGen1(std::make_shared<TPutHandleClassGenerator>(cls))
        , HandleClassGen2(std::make_shared<TPutHandleClassGenerator>(cls))
        , BadSteps(std::make_shared<TSet<ui32>>())
    {
        Y_ABORT_UNLESS(indexOnly);
    }
};

void TManyPutRangeGet2Channels::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TManyPutRangeGet2ChannelsActor(conf, WaitForCompaction, IndexOnly, MsgNum,
                                                                    MsgSize, HandleClass));
}
