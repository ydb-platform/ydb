#include "test_localrecovery.h"
#include "helpers.h"

#include <util/generic/set.h>

using namespace NKikimr;

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TCheckDbIsEmptyManyPutGetActor : public TSyncTestBase {
    const bool ExpectEmpty;
    const ui32 MsgNum;
    const ui32 MsgSize;
    std::shared_ptr<IPutHandleClassGenerator> HandleClassGen;
    std::shared_ptr<TSet<ui32>> BadSteps;

    virtual void Scenario(const TActorContext &ctx) {
        SyncRunner->Run(ctx, CreateCheckDbEmptyness(SyncRunner->NotifyID(), Conf->VDisks->Get(0), ExpectEmpty));
        SyncRunner->Run(ctx, CreateManyPuts(Conf, SyncRunner->NotifyID(), Conf->VDisks->Get(0), MsgSize, MsgNum,
                                            DefaultTestTabletId, 0, 1, HandleClassGen, BadSteps, TDuration::Seconds(0)));
    }

public:
    TCheckDbIsEmptyManyPutGetActor(TConfiguration *conf, bool expectEmpty, bool /*waitForCompaction*/, ui32 msgNum,
                                   ui32 msgSize,  NKikimrBlobStorage::EPutHandleClass cls)
        : TSyncTestBase(conf)
        , ExpectEmpty(expectEmpty)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , HandleClassGen(std::make_shared<TPutHandleClassGenerator>(cls))
        , BadSteps(std::make_shared<TSet<ui32>>())
    {}
};

void TCheckDbIsEmptyManyPutGet::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TCheckDbIsEmptyManyPutGetActor(conf, ExpectEmpty, WaitForCompaction, MsgNum,
                                                                    MsgSize, HandleClass));
}



///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TManyPutsActor : public TSyncTestBase {
    const ui32 MsgNum;
    const ui32 MsgSize;
    std::shared_ptr<IPutHandleClassGenerator> HandleClassGen;
    std::shared_ptr<TSet<ui32>> BadSteps;

    virtual void Scenario(const TActorContext &ctx) {
        ui64 tabletId = DefaultTestTabletId;
        ui32 channel = 0;
        ui32 gen = 1;
        SyncRunner->Run(ctx, CreateManyPuts(Conf, SyncRunner->NotifyID(), Conf->VDisks->Get(0),
                                            MsgSize, MsgNum, tabletId, channel, gen, HandleClassGen, BadSteps,
                                            TDuration::Seconds(0)));
    }

public:
    TManyPutsActor(TConfiguration *conf, bool /*waitForCompaction*/, ui32 msgNum, ui32 msgSize,
                    NKikimrBlobStorage::EPutHandleClass cls, std::shared_ptr<TSet<ui32>> badSteps)
        : TSyncTestBase(conf)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , HandleClassGen(std::make_shared<TPutHandleClassGenerator>(cls))
        , BadSteps(badSteps)
    {}
};


void TManyPutsTest::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TManyPutsActor(conf, WaitForCompaction, MsgNum, MsgSize, HandleClass, BadSteps));
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TManyGetsActor : public TSyncTestBase {
    const ui32 MsgNum;
    const ui32 MsgSize;
    std::shared_ptr<TSet<ui32>> BadSteps;

    virtual void Scenario(const TActorContext &ctx) {
        ui64 tabletId = DefaultTestTabletId;
        ui32 channel = 0;
        ui32 gen = 1;
        SyncRunner->Run(ctx, CreateManyGets(SyncRunner->NotifyID(), Conf->VDisks->Get(0),
                                            MsgSize, MsgNum, tabletId, channel, gen, BadSteps));
    }

public:
    TManyGetsActor(TConfiguration *conf, bool /*waitForCompaction*/, ui32 msgNum, ui32 msgSize,
                    NKikimrBlobStorage::EPutHandleClass /*cls*/, std::shared_ptr<TSet<ui32>> badSteps)
        : TSyncTestBase(conf)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , BadSteps(badSteps)
    {}
};


void TManyGetsTest::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TManyGetsActor(conf, WaitForCompaction, MsgNum, MsgSize, HandleClass, BadSteps));
}

///////////////////////////////////////////////////////////////////////////

class TManyMultiPutsActor : public TSyncTestBase {
    const ui32 MsgNum;
    const ui32 BatchSize;
    const ui32 MsgSize;
    std::shared_ptr<IPutHandleClassGenerator> HandleClassGen;
    std::shared_ptr<TSet<ui32>> BadSteps;

    virtual void Scenario(const TActorContext &ctx) {
        ui64 tabletId = DefaultTestTabletId;
        ui32 channel = 0;
        ui32 gen = 1;
        SyncRunner->Run(ctx, CreateManyMultiPuts(Conf, SyncRunner->NotifyID(), Conf->VDisks->Get(0),
                                            MsgSize, MsgNum, BatchSize, tabletId, channel, gen, HandleClassGen,
                                            BadSteps, TDuration::Seconds(0)));
    }

public:
    TManyMultiPutsActor(TConfiguration *conf, bool /*waitForCompaction*/, ui32 msgNum, ui32 msgSize, ui32 batchSize,
                    NKikimrBlobStorage::EPutHandleClass cls, std::shared_ptr<TSet<ui32>> badSteps)
        : TSyncTestBase(conf)
        , MsgNum(msgNum)
        , BatchSize(batchSize)
        , MsgSize(msgSize)
        , HandleClassGen(std::make_shared<TPutHandleClassGenerator>(cls))
        , BadSteps(badSteps)
    {}
};


void TManyMultiPutsTest::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TManyMultiPutsActor(conf, WaitForCompaction, MsgNum, BatchSize,
                                                         MsgSize, HandleClass, BadSteps));
}

///////////////////////////////////////////////////////////////////////////
class TChaoticManyPutsActor : public NActors::TActorBootstrapped<TChaoticManyPutsActor> {
    TConfiguration *Conf;
    const ui32 Parallel;
    const ui32 MsgNum;
    const ui32 MsgSize;
    std::shared_ptr<IPutHandleClassGenerator> HandleClassGen;
    const TDuration WorkingTime;
    const TDuration RequestTimeout;
    ui32 Counter;
    TVector<std::shared_ptr<TSet<ui32>>> BadSteps;

    friend class NActors::TActorBootstrapped<TChaoticManyPutsActor>;
    void Bootstrap(const NActors::TActorContext &ctx) {
        Y_UNUSED(ctx);
        for (ui32 i = 0; i < Parallel; i++) {
            ui64 tabletId = i + 1;
            ui32 channel = 0;
            ui32 gen = 1;
            auto badSteps = std::make_shared<TSet<ui32>>();
            ctx.Register(CreateManyPuts(Conf, ctx.SelfID, Conf->VDisks->Get(0),
                                                            MsgSize, MsgNum, tabletId, channel, gen,
                                                            HandleClassGen, badSteps, RequestTimeout));
            BadSteps.push_back(badSteps);
        }

        ctx.Schedule(WorkingTime, new TEvents::TEvWakeup());
        Become(&TThis::StateFunc);
    }

    void Finish(const TActorContext &ctx) {
        AtomicIncrement(Conf->SuccessCount);
        Conf->SignalDoneEvent();
        Die(ctx);
    }

    void Handle(TEvents::TEvCompleted::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Counter++;
        if (Counter == Parallel)
            Finish(ctx);
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::Wakeup, Finish);
        HFunc(TEvents::TEvCompleted, Handle);
    )

public:
    TChaoticManyPutsActor(TConfiguration *conf, ui32 parallel, ui32 msgNum, ui32 msgSize,
                          std::shared_ptr<IPutHandleClassGenerator> cls, TDuration workingTime,
                          TDuration requestTimeout)
        : TActorBootstrapped<TChaoticManyPutsActor>()
        , Conf(conf)
        , Parallel(parallel)
        , MsgNum(msgNum)
        , MsgSize(msgSize)
        , HandleClassGen(cls)
        , WorkingTime(workingTime)
        , RequestTimeout(requestTimeout)
        , Counter(0)
    {}
};

void TChaoticManyPutsTest::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TChaoticManyPutsActor(conf, Parallel, MsgNum, MsgSize, HandleClassGen,
        WorkingTime, RequestTimeout));
}
