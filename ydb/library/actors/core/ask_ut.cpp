#include <library/cpp/testing/unittest/registar.h>

#include "actorsystem.h"

#include <ydb/library/actors/testlib/test_runtime.h>

using namespace NActors;

class TPingPong: public TActor<TPingPong> {
public:
    TPingPong()
        : TActor(&TPingPong::Main)
    {
    }

    STATEFN(Main) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPing, OnPing);
            hFunc(TEvents::TEvBlob, OnBlob);
        }
    }

    void OnPing(const TEvents::TEvPing::TPtr& ev) {
        Send(ev->Sender, new TEvents::TEvPong);
    }

    void OnBlob(const TEvents::TEvBlob::TPtr& ev) {
        Send(ev->Sender, ev->Release().Release());
    }
};

class TPing: public TActor<TPing> {
public:
    TPing()
        : TActor(&TPing::Main)
    {
    }

    STATEFN(Main) {
        Y_UNUSED(ev);
    }
};

THolder<TTestActorRuntimeBase> CreateRuntime() {
    auto runtime = MakeHolder<TTestActorRuntimeBase>();
    runtime->SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto&&) { return false; });
    runtime->Initialize();
    return runtime;
}

Y_UNIT_TEST_SUITE(AskActor) {
    Y_UNIT_TEST(Ok) {
        auto runtime = CreateRuntime();
        auto pingpong = runtime->Register(new TPingPong);

        {
            auto fut = runtime->GetAnyNodeActorSystem()->Ask<TEvents::TEvPong>(
                pingpong,
                THolder(new TEvents::TEvPing));
            runtime->DispatchEvents();
            fut.ExtractValueSync();
        }

        {
            auto fut = runtime->GetAnyNodeActorSystem()->Ask<TEvents::TEvBlob>(
                pingpong,
                THolder(new TEvents::TEvBlob("hello!")));
            runtime->DispatchEvents();
            auto ev = fut.ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(ev->Blob, "hello!");
        }

        {
            auto fut = runtime->GetAnyNodeActorSystem()->Ask<IEventBase>(
                pingpong,
                THolder(new TEvents::TEvPing));
            runtime->DispatchEvents();
            auto ev = fut.ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(ev->Type(), TEvents::TEvPong::EventType);
        }
    }

    Y_UNIT_TEST(Err) {
        auto runtime = CreateRuntime();
        auto pingpong = runtime->Register(new TPingPong);

        {
            auto fut = runtime->GetAnyNodeActorSystem()->Ask<TEvents::TEvBlob>(
                pingpong,
                THolder(new TEvents::TEvPing));
            runtime->DispatchEvents();
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                fut.ExtractValueSync(),
                yexception,
                "received unexpected response HelloWorld: Pong");
        }
    }

    Y_UNIT_TEST(Timeout) {
        auto runtime = CreateRuntime();
        auto ping = runtime->Register(new TPing);

        {
            auto fut = runtime->GetAnyNodeActorSystem()->Ask<TEvents::TEvPong>(
                ping,
                THolder(new TEvents::TEvPing),
                TDuration::Seconds(1));
            auto start = runtime->GetCurrentTime();
            runtime->DispatchEvents({}, TDuration::Seconds(5));
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                fut.ExtractValueSync(),
                yexception,
                "ask timeout");
            UNIT_ASSERT_VALUES_EQUAL(runtime->GetCurrentTime() - start, TDuration::Seconds(1));
        }

        {
            auto fut = runtime->GetAnyNodeActorSystem()->Ask<IEventBase>(
                ping,
                THolder(new TEvents::TEvPing),
                TDuration::Seconds(1));
            auto start = runtime->GetCurrentTime();
            runtime->DispatchEvents({}, TDuration::Seconds(5));
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                fut.ExtractValueSync(),
                yexception,
                "ask timeout");
            UNIT_ASSERT_VALUES_EQUAL(runtime->GetCurrentTime() - start, TDuration::Seconds(1));
        }
    }
}
