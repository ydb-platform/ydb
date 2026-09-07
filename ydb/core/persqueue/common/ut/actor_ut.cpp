#include <ydb/core/persqueue/common/actor.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/unittest/registar.h>

#include <stdexcept>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NPQ;

namespace {

enum EEv {
    EvHandled = EventSpaceBegin(TEvents::ES_PRIVATE),
    EvText,
};

struct TEvHandled : TEventLocal<TEvHandled, EvHandled> {
    bool Value = false;
    explicit TEvHandled(bool value)
        : Value(value)
    {
    }
};

struct TEvText : TEventLocal<TEvText, EvText> {
    TString Value;
    explicit TEvText(TString value)
        : Value(std::move(value))
    {
    }
};

class TPrefixActor : public TBaseActor<TPrefixActor>
                   , public TConstantLogPrefix {
public:
    explicit TPrefixActor(TActorId parent)
        : TBaseActor<TPrefixActor>(NKikimrServices::PERSQUEUE)
        , Parent(parent)
    {
    }

    TString BuildLogPrefix() const override {
        return " [prefix] ";
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        LOG_T("trace");
        LOG_D("debug");
        LOG_I("info");
        LOG_N("notice");
        LOG_W("warn");
        LOG_E("error");
        LOG_C("crit");
        LOG_A("alert");
        const TString& first = GetLogPrefix();
        const TString& second = GetLogPrefix();
        Y_UNUSED(second);
        Send(Parent, new TEvText(TStringBuilder() << LogBuilder() << first));
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        Send(Parent, new TEvText(EventStr("Handle", ev)));
    }

    void HandlePoison() {
        PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvents::TEvWakeup, Handle);
        cFunc(TEvents::TEvPoison::EventType, HandlePoison);
    )

private:
    const TActorId Parent;
};

class TExceptionActor : public TBaseActor<TExceptionActor>
                      , public TConstantLogPrefix {
public:
    explicit TExceptionActor(TActorId parent)
        : TBaseActor<TExceptionActor>(NKikimrServices::PERSQUEUE)
        , Parent(parent)
    {
    }

    TString BuildLogPrefix() const override {
        return " [exc] ";
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        std::runtime_error exc("boom");
        const bool handled = OnUnhandledException(exc);
        Send(Parent, new TEvHandled(handled));
    }

    void OnException(const std::exception&) override {
    }

    STRICT_STFUNC(StateWork,
        cFunc(TEvents::TEvPoison::EventType, PassAway);
    )

private:
    const TActorId Parent;
};

class TDefaultHooksActor : public TBaseActor<TDefaultHooksActor>
                         , public TConstantLogPrefix {
public:
    explicit TDefaultHooksActor(TActorId parent)
        : TBaseActor<TDefaultHooksActor>(NKikimrServices::PERSQUEUE)
        , Parent(parent)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Send(Parent, new TEvText(GetLogPrefix()));
        std::runtime_error exc("default-hooks");
        const bool handled = OnUnhandledException(exc);
        Send(Parent, new TEvHandled(handled));
    }

    STRICT_STFUNC(StateWork,
        cFunc(TEvents::TEvPoison::EventType, PassAway);
    )

private:
    const TActorId Parent;
};

class TTabletExceptionActor : public TBaseTabletActor<TTabletExceptionActor>
                            , public TConstantLogPrefix {
public:
    TTabletExceptionActor(ui64 tabletId, TActorId tabletActorId)
        : TBaseTabletActor<TTabletExceptionActor>(tabletId, tabletActorId, NKikimrServices::PERSQUEUE)
    {
    }

    TString BuildLogPrefix() const override {
        return " [tablet] ";
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Send(TabletActorId, new TEvText(TString(LogBuilder()) + GetLogPrefix()));
        std::runtime_error exc("tablet-boom");
        OnUnhandledException(exc);
    }

    STRICT_STFUNC(StateWork,
        cFunc(TEvents::TEvPoison::EventType, PassAway);
    )
};

class TPipeActor : public TBaseActor<TPipeActor>
                 , public TConstantLogPrefix {
public:
    explicit TPipeActor(TActorId parent)
        : TBaseActor<TPipeActor>(NKikimrServices::PERSQUEUE)
        , Pipes(this)
        , Parent(parent)
    {
    }

    TString BuildLogPrefix() const override {
        return " [pipe] ";
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Pipes.Close();
        Pipes.SendToTablet(900001, new TEvents::TEvWakeup(), 1);
        Pipes.SendToTablet(900001, new TEvents::TEvWakeup(), 2);
        Pipes.SendToTablet(900002, new TEvents::TEvWakeup(), 3);
        Send(Parent, new TEvHandled(true));
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        const bool matched = Pipes.OnUndelivered(ev);
        Send(Parent, new TEvHandled(matched));
        if (matched) {
            Pipes.SendToTablet(ev->Get()->TabletId, new TEvents::TEvWakeup(), 4);
            Pipes.Close();
            Pipes.Close();
        }
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
    )

private:
    TPipeCacheClient Pipes;
    const TActorId Parent;
};

void InitRuntime(NActors::TTestBasicRuntime& runtime, bool enableRestartOnException) {
    TAppPrepare app;
    app.FeatureFlags.SetEnableTabletRestartOnUnhandledExceptions(enableRestartOnException);
    runtime.Initialize(app.Unwrap());
}

} // namespace

Y_UNIT_TEST_SUITE(TBaseActorTest) {

Y_UNIT_TEST(LogPrefixEventStrAndMacros) {
    NActors::TTestBasicRuntime runtime(1, false);
    InitRuntime(runtime, false);
    auto edge = runtime.AllocateEdgeActor();
    auto actorId = runtime.Register(new TPrefixActor(edge));
    runtime.EnableScheduleForActor(actorId);

    auto prefix = runtime.GrabEdgeEvent<TEvText>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(prefix);
    UNIT_ASSERT(prefix->Get()->Value.Contains("[prefix]"));

    runtime.Send(new IEventHandle(actorId, edge, new TEvents::TEvWakeup()), 0, true);
    auto eventStr = runtime.GrabEdgeEvent<TEvText>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(eventStr);
    UNIT_ASSERT(eventStr->Get()->Value.Contains("Handle"));
    UNIT_ASSERT(eventStr->Get()->Value.Contains("Sender"));
    UNIT_ASSERT(eventStr->Get()->Value.Contains("Cookie"));

    runtime.Send(new IEventHandle(actorId, edge, new TEvents::TEvPoison()), 0, true);
}

Y_UNIT_TEST(UnhandledExceptionDisabled) {
    NActors::TTestBasicRuntime runtime(1, false);
    InitRuntime(runtime, false);
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(new TExceptionActor(edge));

    auto handled = runtime.GrabEdgeEvent<TEvHandled>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(handled);
    UNIT_ASSERT(!handled->Get()->Value);
}

Y_UNIT_TEST(UnhandledExceptionEnabled) {
    NActors::TTestBasicRuntime runtime(1, false);
    InitRuntime(runtime, true);
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(new TExceptionActor(edge));

    auto handled = runtime.GrabEdgeEvent<TEvHandled>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(handled);
    UNIT_ASSERT(handled->Get()->Value);

    auto counters = GetServiceCounters(runtime.GetAppData().Counters, "tablets");
    UNIT_ASSERT(counters);
    UNIT_ASSERT(counters->GetCounter("alerts_exception", true)->Val() >= 1);
}

Y_UNIT_TEST(DefaultOnExceptionAndLogPrefix) {
    NActors::TTestBasicRuntime runtime(1, false);
    InitRuntime(runtime, true);
    auto edge = runtime.AllocateEdgeActor();
    runtime.Register(new TDefaultHooksActor(edge));

    auto prefix = runtime.GrabEdgeEvent<TEvText>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(prefix);
    UNIT_ASSERT_VALUES_EQUAL(prefix->Get()->Value, " ");

    auto handled = runtime.GrabEdgeEvent<TEvHandled>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(handled);
    UNIT_ASSERT(handled->Get()->Value);
}

Y_UNIT_TEST(TabletActorRestartsOnException) {
    NActors::TTestBasicRuntime runtime(1, false);
    InitRuntime(runtime, true);
    auto tablet = runtime.AllocateEdgeActor();
    runtime.Register(new TTabletExceptionActor(42, tablet));

    auto prefix = runtime.GrabEdgeEvent<TEvText>(tablet, TDuration::Seconds(5));
    UNIT_ASSERT(prefix);
    UNIT_ASSERT(prefix->Get()->Value.Contains("[42]"));

    auto poison = runtime.GrabEdgeEvent<TEvents::TEvPoison>(tablet, TDuration::Seconds(5));
    UNIT_ASSERT(poison);
}

Y_UNIT_TEST(PipeCacheClient) {
    NActors::TTestBasicRuntime runtime(1, false);
    InitRuntime(runtime, false);
    auto edge = runtime.AllocateEdgeActor();
    auto pipeEdge = runtime.AllocateEdgeActor();
    runtime.RegisterService(MakePipePerNodeCacheID(false), pipeEdge);

    auto actorId = runtime.Register(new TPipeActor(edge));
    runtime.EnableScheduleForActor(actorId);

    auto f1 = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeEdge, TDuration::Seconds(5));
    auto f2 = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeEdge, TDuration::Seconds(5));
    auto f3 = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeEdge, TDuration::Seconds(5));
    UNIT_ASSERT(f1 && f2 && f3);
    UNIT_ASSERT_VALUES_EQUAL(f1->Get()->TabletId, 900001u);
    UNIT_ASSERT_VALUES_EQUAL(f1->Get()->Options.SubscribeCookie, 1u);
    UNIT_ASSERT_VALUES_EQUAL(f2->Get()->TabletId, 900001u);
    UNIT_ASSERT_VALUES_EQUAL(f2->Get()->Options.SubscribeCookie, 1u);
    UNIT_ASSERT_VALUES_EQUAL(f3->Get()->TabletId, 900002u);

    auto ready = runtime.GrabEdgeEvent<TEvHandled>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(ready);
    UNIT_ASSERT(ready->Get()->Value);

    runtime.Send(new IEventHandle(actorId, edge, new TEvPipeCache::TEvDeliveryProblem(900001, true), 0, 99), 0, true);
    auto mismatch = runtime.GrabEdgeEvent<TEvHandled>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(mismatch);
    UNIT_ASSERT(!mismatch->Get()->Value);

    runtime.Send(new IEventHandle(actorId, edge, new TEvPipeCache::TEvDeliveryProblem(999999, true), 0, 0), 0, true);
    auto unknown = runtime.GrabEdgeEvent<TEvHandled>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(unknown);
    UNIT_ASSERT(!unknown->Get()->Value);

    runtime.Send(new IEventHandle(actorId, edge, new TEvPipeCache::TEvDeliveryProblem(900001, true), 0, 1), 0, true);
    auto match = runtime.GrabEdgeEvent<TEvHandled>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(match);
    UNIT_ASSERT(match->Get()->Value);

    auto extraForward = runtime.GrabEdgeEvent<TEvPipeCache::TEvForward>(pipeEdge, TDuration::Seconds(5));
    UNIT_ASSERT(extraForward);
    UNIT_ASSERT_VALUES_EQUAL(extraForward->Get()->TabletId, 900001u);
    auto unlink = runtime.GrabEdgeEvent<TEvPipeCache::TEvUnlink>(pipeEdge, TDuration::Seconds(5));
    UNIT_ASSERT(unlink);
    UNIT_ASSERT_VALUES_EQUAL(unlink->Get()->TabletId, 0u);
}

} // Y_UNIT_TEST_SUITE(TBaseActorTest)
