#include "test_faketablet.h"
#include "helpers.h"

using namespace NKikimr;

// FIXME: add gargabe collection

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TFakeTabletLogChannelActor
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TFakeTabletLogChannelActor : public NActors::TActorBootstrapped<TFakeTabletLogChannelActor> {


//    TCustomDataSet


    TConfiguration *Conf;
    const ui64 TabletId;
    TAutoPtr<TCustomDataSet> DataSetPtr;
    ui32 Step;
    const ui32 MinHugeBlobSize;

    static constexpr ui32 RecsPerIteration = 100u;
    static constexpr ui32 BlobSize = 10u;

    friend class NActors::TActorBootstrapped<TFakeTabletLogChannelActor>;

    void Bootstrap(const NActors::TActorContext &ctx) {
        Become(&TThis::StateFunc);
        Iteration(ctx);
    }

    void Handle(TEvents::TEvCompleted::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Iteration(ctx);
    }

    void Iteration(const TActorContext &ctx) {
        DataSetPtr.Reset(new TCustomDataSet(TabletId, 1 /*gen*/, 0 /*channel*/, Step, RecsPerIteration, BlobSize,
                                            NKikimrBlobStorage::EPutHandleClass::TabletLog, MinHugeBlobSize, false));
        Step += RecsPerIteration;
        ctx.Register(ManyPutsToCorrespondingVDisks(ctx.SelfID, Conf, DataSetPtr.Get()));
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvents::TEvCompleted, Handle);
    )

public:
    TFakeTabletLogChannelActor(TConfiguration *conf, ui64 tabletId, ui32 minHugeBlobSize)
        : TActorBootstrapped<TFakeTabletLogChannelActor>()
        , Conf(conf)
        , TabletId(tabletId)
        , DataSetPtr()
        , Step(0)
        , MinHugeBlobSize(minHugeBlobSize)
    {}
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TFakeTabletHugeBlobChannelActor
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TFakeTabletHugeBlobChannelActor : public NActors::TActorBootstrapped<TFakeTabletHugeBlobChannelActor> {
    TConfiguration *Conf;
    const ui64 TabletId;
    const ui32 Channel;
    const ui32 HugeBlobSize;
    const ui32 MinHugeBlobSize;
    const TDuration WaitTime;
    TAutoPtr<TCustomDataSet> DataSetPtr;
    ui32 Step;

    friend class NActors::TActorBootstrapped<TFakeTabletHugeBlobChannelActor>;

    void Bootstrap(const NActors::TActorContext &ctx) {
        Become(&TThis::StateFunc);
        Iteration(ctx);
    }

    void Handle(TEvents::TEvCompleted::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        ctx.Schedule(WaitTime, new TEvents::TEvWakeup());
    }

    void Timeout(const TActorContext &ctx) {
        Iteration(ctx);
    }

    void Iteration(const TActorContext &ctx) {
        DataSetPtr.Reset(new TCustomDataSet(TabletId, 1 /*gen*/, Channel, Step, 1, HugeBlobSize,
                                            NKikimrBlobStorage::EPutHandleClass::AsyncBlob, MinHugeBlobSize, true));
        Step++;
        ctx.Register(ManyPutsToCorrespondingVDisks(ctx.SelfID, Conf, DataSetPtr.Get()));
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::Wakeup, Timeout);
        HFunc(TEvents::TEvCompleted, Handle);
    )

public:
    TFakeTabletHugeBlobChannelActor(TConfiguration *conf, ui64 tabletId, ui32 channel, ui32 hugeBlobSize,
                                    ui32 minHugeBlobSize, const TDuration &waitTime)
        : TActorBootstrapped<TFakeTabletHugeBlobChannelActor>()
        , Conf(conf)
        , TabletId(tabletId)
        , Channel(channel)
        , HugeBlobSize(hugeBlobSize)
        , MinHugeBlobSize(minHugeBlobSize)
        , WaitTime(waitTime)
        , DataSetPtr()
        , Step(0)
    {}
};


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TFakeTabletActor
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TFakeTabletActor : public NActors::TActorBootstrapped<TFakeTabletActor> {
    typedef TAutoPtr<IActor> TActorPtr;
    ui32 Counter;
    TActorPtr LogActor;
    TActorPtr HugeBlobActor;

    friend class NActors::TActorBootstrapped<TFakeTabletActor>;

    void Bootstrap(const NActors::TActorContext &ctx) {
        Become(&TThis::StateFunc);
        ctx.Register(LogActor.Release());
        ctx.Register(HugeBlobActor.Release());
        Counter = 2;
    }

    void Finish(const TActorContext &ctx) {
        Die(ctx);
    }

    void Handle(TEvents::TEvCompleted::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Counter--;
        if (Counter == 0)
            Finish(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvents::TEvCompleted, Handle);
    )

public:
    TFakeTabletActor(TConfiguration *conf, ui64 tabletId, ui32 hugeBlobSize, ui32 minHugeBlobSize,
                     const TDuration &waitTime)
        : TActorBootstrapped<TFakeTabletActor>()
        , Counter(2)
        , LogActor(new TFakeTabletLogChannelActor(conf, tabletId, minHugeBlobSize))
        , HugeBlobActor(new TFakeTabletHugeBlobChannelActor(conf, tabletId, 1 /*channel*/, hugeBlobSize,
                                                            minHugeBlobSize, waitTime))
    {}
};



////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TFakeTabletLoadActor
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TFakeTabletLoadActor : public NActors::TActorBootstrapped<TFakeTabletLoadActor> {
    TConfiguration *Conf;
    const ui64 TabletsNum;
    const ui32 HugeBlobSize;
    const ui32 MinHugeBlobSize;
    const TDuration HugeBlobWaitTime;
    ui32 Counter;

    friend class NActors::TActorBootstrapped<TFakeTabletLoadActor>;

    void Bootstrap(const NActors::TActorContext &ctx) {
        for (ui64 i = 0; i < TabletsNum; i++) {
            ctx.Register(new TFakeTabletActor(Conf, i, HugeBlobSize, MinHugeBlobSize,
                                                                  HugeBlobWaitTime));
        }
        Counter = TabletsNum;

        Become(&TThis::StateFunc);
    }

    void Finish(const TActorContext &ctx) {
        AtomicIncrement(Conf->SuccessCount);
        Conf->SignalDoneEvent();
        Die(ctx);
    }

    void Handle(TEvents::TEvCompleted::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Counter--;
        if (Counter == 0)
            Finish(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvents::TEvCompleted, Handle);
    )

public:
    TFakeTabletLoadActor(TConfiguration *conf, ui64 tabletsNum, ui32 hugeBlobSize, ui32 minHugeBlobSize,
                         const TDuration &hugeBlobWaitTime)
        : TActorBootstrapped<TFakeTabletLoadActor>()
        , Conf(conf)
        , TabletsNum(tabletsNum)
        , HugeBlobSize(hugeBlobSize)
        , MinHugeBlobSize(minHugeBlobSize)
        , HugeBlobWaitTime(hugeBlobWaitTime)
        , Counter(0)
    {}
};

void TFakeTabletLoadTest::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TFakeTabletLoadActor(conf, TabletsNum, HugeBlobSize, MinHugeBlobSize,
                                                          HugeBlobWaitTime));
}
