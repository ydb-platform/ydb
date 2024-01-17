#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include "actor_tracker.h"

using namespace NActors;

TAtomic SubworkerNew = 0;
TAtomic SubworkerBootstrap = 0;
TAtomic SubworkerPoison = 0;
TAtomic SubworkerDelete = 0;

// the subworker
class TSubworkerActor : public TTrackedActorBootstrapped<TSubworkerActor> {
    using TBase = TTrackedActorBootstrapped<TSubworkerActor>;

public:
    TSubworkerActor() {
        AtomicIncrement(SubworkerNew);
    }

    ~TSubworkerActor() {
        AtomicIncrement(SubworkerDelete);
    }

    void Bootstrap(const TActorContext& /*ctx*/) {
        AtomicIncrement(SubworkerBootstrap);
        Become(&TSubworkerActor::StateFunc);
    }

    void HandlePoison(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
        AtomicIncrement(SubworkerPoison);
        TBase::HandlePoison(ev, ctx);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoison);
            default:
                Y_ABORT();
        }
    }
};

// the basic worker -- it does some work and creates subworkers
class TWorkerActor : public TTrackedActorBootstrapped<TWorkerActor> {
    using TBase = TTrackedActorBootstrapped<TWorkerActor>;

public:
    void Bootstrap(const TActorContext& ctx) {
        // create some subworkers
        for (int i = 0; i < 3; ++i) {
            TActorId actorId = RegisterSubactor(MakeHolder<TSubworkerActor>(), ctx);
            if (!actorId) {
                Die(ctx);
            }
        }

        Become(&TWorkerActor::StateFunc);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, TBase::HandlePoison);
            default:
                Y_ABORT();
        }
    }
};

// the root manager -- the entity than controls all created workers and subworkers
class TManagerActor : public TActorBootstrapped<TManagerActor> {
    TActorTracker Tracker;

public:
    void Bootstrap(const TActorContext& ctx) {
        Tracker.BindToActor(ctx);

        // create some workers
        for (int i = 0; i < 2; ++i) {
            TActorId actorId = Tracker.RegisterSubactor(MakeHolder<TWorkerActor>(), ctx);
            if (!actorId) {
                // a race has occured
                Die(ctx);
            }
        }

        Become(&TManagerActor::StateFunc);
    }

    STFUNC(StateFunc) {
        if (Tracker.HandleTracking(ev)) {
            return;
        }

        switch (ev->GetTypeRewrite()) {
        }
    }
};

Y_UNIT_TEST_SUITE(TActorTracker) {

    Y_UNIT_TEST(Basic) {
        {
            TTestBasicRuntime runtime;
            runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
            TActorId managerId = runtime.Register(new TManagerActor);
            TActorId edge = runtime.AllocateEdgeActor();
            runtime.Schedule(new IEventHandle(managerId, edge, new TEvents::TEvPoisonPill), TDuration::Seconds(1));
            runtime.DispatchEvents();
            TAutoPtr<IEventHandle> handle;
            runtime.GrabEdgeEventRethrow<TEvents::TEvPoisonTaken>(handle);
        }
        UNIT_ASSERT_VALUES_EQUAL(AtomicGet(SubworkerNew), 6);
        UNIT_ASSERT_VALUES_EQUAL(AtomicGet(SubworkerBootstrap), 6);
        UNIT_ASSERT_VALUES_EQUAL(AtomicGet(SubworkerPoison), 6);
        UNIT_ASSERT_VALUES_EQUAL(AtomicGet(SubworkerDelete), 6);
    }

}
