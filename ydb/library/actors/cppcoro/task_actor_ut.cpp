#include "task_actor.h"
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(TaskActor) {

    using namespace NActors;

    enum : ui32 {
        EvBegin = EventSpaceBegin(TEvents::ES_USERSPACE),
        EvRequest,
        EvResponse,
        EvStop,
    };

    struct TEvRequest: public TEventLocal<TEvRequest, EvRequest> {
    };

    struct TEvResponse: public TEventLocal<TEvResponse, EvResponse> {
    };

    struct TEvStop: public TEventLocal<TEvStop, EvStop> {
    };

    TTask<void> SimpleResponder() {
        for (;;) {
            auto ev = co_await TTaskActor::NextEvent;
            Y_ABORT_UNLESS(ev->GetTypeRewrite() == TEvRequest::EventType);
            auto* msg = ev->Get<TEvRequest>();
            Y_UNUSED(msg);
            TTaskActor::SelfId().Send(ev->Sender, new TEvResponse);
        }
    }

    TTask<void> SimpleRequester(TActorId responder, TManualEvent& doneEvent, std::atomic<int>& itemsProcessed) {
        // Note: it's ok to use lambda capture because captures outlive this coroutine
        auto singleRequest = [&]() -> TTask<bool> {
            TTaskActor::SelfId().Send(responder, new TEvRequest);
            auto ev = co_await TTaskActor::NextEvent;
            switch (ev->GetTypeRewrite()) {
                case TEvResponse::EventType:
                    co_return true;
                case TEvStop::EventType:
                    co_return false;
                default:
                    Y_ABORT("Unexpected event");
            }
        };
        while (co_await singleRequest()) {
            ++itemsProcessed;
        }
        doneEvent.Signal();
    }

    void Check(TDuration duration, std::unique_ptr<IEventBase> stopEvent) {
        THolder<TActorSystemSetup> setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 0;
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[setup->ExecutorsCount]);
        for (ui32 i = 0; i < setup->ExecutorsCount; ++i) {
            setup->Executors[i] = new TBasicExecutorPool(i, 5, 10, "basic");
        }
        setup->Scheduler = new TBasicSchedulerThread;

        TActorSystem actorSystem(setup);

        actorSystem.Start();

        TManualEvent doneEvent;
        std::atomic<int> itemsProcessed{0};

        auto responder = actorSystem.Register(TTaskActor::Create(SimpleResponder()));
        auto requester = actorSystem.Register(TTaskActor::Create(SimpleRequester(responder, doneEvent, itemsProcessed)));
        auto deadline = TMonotonic::Now() + duration;
        while (itemsProcessed.load() < 10) {
            UNIT_ASSERT_C(TMonotonic::Now() < deadline, "cannot observe 10 responses in " << duration);
            Sleep(TDuration::MilliSeconds(100));
        }
        actorSystem.Send(requester, stopEvent.release());
        doneEvent.WaitI();

        UNIT_ASSERT_GE(itemsProcessed.load(), 10);

        actorSystem.Stop();
    }

    Y_UNIT_TEST(Basic) {
        Check(TDuration::Seconds(10), std::make_unique<TEvStop>());
    }

} // Y_UNIT_TEST_SUITE(TaskActor)
