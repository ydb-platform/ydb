#include <ydb/library/actors/async/wait_for_event.h>

#include "common.h"
#include <ydb/library/actors/async/cancellation.h>

namespace NAsyncTest {

    Y_UNIT_TEST_SUITE(WaitForEvent) {

        Y_UNIT_TEST(EventDelivered) {
            TVector<TString> sequence;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto handler = [&](IEventHandle::TPtr& ev) {
                sequence.push_back(TStringBuilder() << "received event " << Hex(ev->GetTypeRewrite()));
                return true;
            };

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                auto ev = co_await ActorWaitForEvent<TEvents::TEvWakeup>(123);

                sequence.push_back("returning");
            }, handler);

            ASYNC_ASSERT_SEQUENCE(sequence, "started");

            // Wrong cookie doesn't cause ActorWaitForEvent to resume
            actor.Receive(new TEvents::TEvWakeup, 120);
            ASYNC_ASSERT_SEQUENCE(sequence, "received event 0x00010002");

            // Correct cookie but wrong event type doesn't cause ActorWaitForEvent to resume
            actor.Receive(new TEvents::TEvGone, 123);
            ASYNC_ASSERT_SEQUENCE(sequence, "received event 0x0001000D");

            // Correct cookie and event type cause ActorWaitForEvent to resume
            actor.Receive(new TEvents::TEvWakeup, 123);
            ASYNC_ASSERT_SEQUENCE(sequence, "returning", "finished");
        }

        Y_UNIT_TEST(Cancel) {
            TVector<TString> sequence;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                ASYNC_ASSERT_NO_RETURN(co_await ActorWaitForEvent<TEvents::TEvWakeup>(123));
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started");

            actor.Poison();
            ASYNC_ASSERT_SEQUENCE(sequence, "finished");
        }

        Y_UNIT_TEST(CancelledAlready) {
            TVector<TString> sequence;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&](auto* self) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                self->PassAway();

                ASYNC_ASSERT_NO_RETURN(co_await ActorWaitForEvent<TEvents::TEvWakeup>(123));
            });

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "finished");
        }

        Y_UNIT_TEST(CancelThenDeliver) {
            TVector<TString> sequence;
            TAsyncCancellationScope scope;

            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto handler = [&](IEventHandle::TPtr& ev) {
                sequence.push_back(TStringBuilder() << "received event " << Hex(ev->GetTypeRewrite()));
                return true;
            };

            auto actor = runtime.StartAsyncActor(state, [&](auto*) -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                bool success = co_await scope.Wrap([&]() -> async<void> {
                    Y_DEFER { sequence.push_back("callback finished"); };
                    sequence.push_back("waiting");
                    ASYNC_ASSERT_NO_RETURN(co_await ActorWaitForEvent<TEvents::TEvWakeup>(123));
                });

                if (!success) {
                    sequence.push_back("callback was cancelled");
                }
            }, handler);

            ASYNC_ASSERT_SEQUENCE(sequence, "started", "waiting");

            actor.RunSync([&]{ scope.Cancel(); });
            ASYNC_ASSERT_SEQUENCE(sequence, "callback finished", "callback was cancelled", "finished");

            actor.Receive(new TEvents::TEvWakeup, 123);
            ASYNC_ASSERT_SEQUENCE(sequence, "received event 0x00010002");
        }

    } // Y_UNIT_TEST_SUITE(WaitForEvent)

} // namespace NAsyncTest
