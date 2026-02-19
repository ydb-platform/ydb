#include "poison_pill_helper.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace NYdb::NBS::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMyTestEnv final
{
public:
    TActorId Sender;

private:
    TTestActorRuntimeBase Runtime;

public:
    TMyTestEnv()
    {
        Runtime.Initialize();
        Sender = Runtime.AllocateEdgeActor();
    }

    TActorId Register(IActorPtr actor)
    {
        auto actorId = Runtime.Register(actor.release());
        Runtime.EnableScheduleForActor(actorId);

        return actorId;
    }

    void Send(const TActorId& recipient, IEventBasePtr event, ui64 cookie)
    {
        Runtime.Send(
            new IEventHandle(recipient, Sender, event.release(), 0, cookie));
    }

    void DispatchEvents(TDuration timeout)
    {
        Runtime.DispatchEvents(TDispatchOptions(), timeout);
    }

    TAutoPtr<IEventHandle> GrabPoisonTakenEvent(TDuration timeout)
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TEvents::TEvPoisonTaken>(handle, timeout);
        return handle;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChildActor: public TActor<TChildActor>
{
public:
    TChildActor()
        : TActor(&TThis::Main)
    {}

private:
    void Main(TAutoPtr<IEventHandle>& ev)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        }
    }

    void HandlePoisonPill(const TEvents::TEvPoisonPill::TPtr& ev,
                          const TActorContext& ctx)
    {
        ctx.Send(ev->Sender, std::make_unique<TEvents::TEvPoisonTaken>(), 0,
                 ev->Cookie);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TParentActor
    : public TActor<TParentActor>
    , IPoisonPillHelperOwner
{
private:
    using TBase = TActor<TParentActor>;
    TPoisonPillHelper PoisonPillHelper;
    ui32 ChildCount;

public:
    TParentActor(ui32 childCount)
        : TActor(&TThis::Main)
        , PoisonPillHelper(this)
        , ChildCount(childCount)
    {}

    void Die(const NActors::TActorContext& ctx) override
    {
        TBase::Die(ctx);
    }

private:
    void Main(TAutoPtr<IEventHandle>& ev)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvBootstrap, HandleBootstrap);

            HFunc(TEvents::TEvPoisonPill, PoisonPillHelper.HandlePoisonPill);
            HFunc(TEvents::TEvPoisonTaken, PoisonPillHelper.HandlePoisonTaken);
        }
    }

    void HandleBootstrap(const TEvents::TEvBootstrap::TPtr& ev,
                         const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        {   // We give ownership and take it away immediately.
            auto childId = ctx.Register(new TChildActor());
            PoisonPillHelper.TakeOwnership(ctx, childId);
            ctx.Send(childId, std::make_unique<TEvents::TEvPoisonPill>());
            PoisonPillHelper.ReleaseOwnership(ctx, childId);
        }

        // Give ownership for long time.
        for (ui32 i = 0; i < ChildCount; ++i) {
            PoisonPillHelper.TakeOwnership(ctx,
                                           ctx.Register(new TChildActor()));
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPoisonPillHelperTest)
{
    Y_UNIT_TEST(Basic)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(std::make_unique<TParentActor>(10));

        testEnv.Send(actorId, std::make_unique<TEvents::TEvBootstrap>(), 0);

        testEnv.Send(actorId, std::make_unique<TEvents::TEvPoisonPill>(), 1000);

        auto poisonTakenEvent =
            testEnv.GrabPoisonTakenEvent(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, poisonTakenEvent);
        UNIT_ASSERT_VALUES_EQUAL(1000, poisonTakenEvent->Cookie);
    }

    Y_UNIT_TEST(NoChildren)
    {
        TMyTestEnv testEnv;

        auto actorId = testEnv.Register(std::make_unique<TParentActor>(0));

        testEnv.Send(actorId, std::make_unique<TEvents::TEvBootstrap>(), 0);

        testEnv.Send(actorId, std::make_unique<TEvents::TEvPoisonPill>(), 1000);

        auto poisonTakenEvent =
            testEnv.GrabPoisonTakenEvent(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, poisonTakenEvent);
        UNIT_ASSERT_VALUES_EQUAL(1000, poisonTakenEvent->Cookie);
    }
}
}   // namespace NYdb::NBS::NStorage
