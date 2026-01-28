#include <ydb/library/actors/async/low_priority.h>
#include "common.h"
#include <ydb/library/actors/async/cancellation.h>

namespace NAsyncTest {

    Y_UNIT_TEST_SUITE(LowPriorityQueue) {

        Y_UNIT_TEST(Basics) {
            TVector<TString> sequence;
            TAsyncLowPriorityQueue queue;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state);

            actor.SendAsync([&]() -> async<void> {
                sequence.push_back("started 1");
                Y_DEFER { sequence.push_back("finished 1"); };

                co_await queue.Next();
                sequence.push_back("resumed 1");
            });

            actor.SendAsync([&]() -> async<void> {
                sequence.push_back("started 2");
                Y_DEFER { sequence.push_back("finished 2"); };

                co_await queue.Next();
                sequence.push_back("resumed 2");
            });

            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);

            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence, "started 1");

            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence, "started 2");

            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence, "resumed 1", "finished 1");

            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence, "resumed 2", "finished 2");
        }

        Y_UNIT_TEST(CancelBeforeWait) {
            TVector<TString> sequence;
            TAsyncLowPriorityQueue queue;
            TAsyncCancellationScope scope;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state);

            scope.Cancel();
            actor.SendAsync([&]() -> async<void> {
                sequence.push_back("started");
                Y_DEFER { sequence.push_back("finished"); };

                bool resumed = co_await scope.Wrap([&]() -> async<void> {
                    Y_DEFER { sequence.push_back("inner finished"); };
                    sequence.push_back("calling Next()");
                    co_await queue.Next();
                });
                sequence.push_back(resumed ? "resumed" : "cancelled");
            });

            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);

            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "started",
                "calling Next()",
                "inner finished",
                "cancelled",
                "finished");

            actor.SendSync([&]{
                sequence.push_back("mailbox empty");
            });

            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence, "mailbox empty");
        }

        Y_UNIT_TEST(CancelBeforeResumeTwice) {
            TVector<TString> sequence;
            TAsyncLowPriorityQueue queue;
            TAsyncCancellationScope scope1;
            TAsyncCancellationScope scope2;
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state);

            actor.SendAsync([&]() -> async<void> {
                sequence.push_back("started 1");
                Y_DEFER { sequence.push_back("finished 1"); };

                bool resumed = co_await scope1.Wrap([&]() -> async<void> {
                    Y_DEFER { sequence.push_back("inner finished 1"); };
                    co_await queue.Next();
                });
                sequence.push_back(resumed ? "resumed 1" : "cancelled 1");
            });

            actor.SendSync([&]{
                sequence.push_back("cancel 1");
                scope1.Cancel();
            });

            actor.SendAsync([&]() -> async<void> {
                sequence.push_back("started 2");
                Y_DEFER { sequence.push_back("finished 2"); };

                bool resumed = co_await scope2.Wrap([&]() -> async<void> {
                    Y_DEFER { sequence.push_back("inner finished 2"); };
                    co_await queue.Next();
                });
                sequence.push_back(resumed ? "resumed 2" : "cancelled 2");
            });

            // Process the 1st request, it will enqueue the 1st resume (after the 2nd request)
            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "started 1");

            // Enqueue the 2nd cancellation (after the 1st resume)
            actor.SendSync([&]{
                sequence.push_back("cancel 2");
                scope2.Cancel();
            });

            // Process the 1st cancellation
            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "cancel 1",
                "inner finished 1",
                "cancelled 1",
                "finished 1");

            // Process the 2nd request, it will enqueue the 2nd resume
            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "started 2");

            // This is expected to be enqueued after the 2nd resume
            actor.SendSync([&]{
                sequence.push_back("after the 2nd resume");
            });

            // Process the 1st resume (it must be ignored)
            actor.Step();
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);

            // Process the 2nd cancellation
            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "cancel 2",
                "inner finished 2",
                "cancelled 2",
                "finished 2");

            // Process the 2nd resume (it must be ignored)
            actor.Step();
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);

            // Finally the mailbox is expected to be empty
            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence, "after the 2nd resume");
        }

        Y_UNIT_TEST(MultipleAwaitersSameResume) {
            TVector<TString> sequence;
            TAsyncLowPriorityQueue queue;
            TAsyncCancellationScope scope[3];
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state);

            // Enqueue all 3 requests
            for (int i = 0; i < 3; ++i) {
                actor.SendAsync([&sequence, &queue, &scope, req = i + 1]() -> async<void> {
                    sequence.push_back(TStringBuilder() << "started " << req);
                    Y_DEFER { sequence.push_back(TStringBuilder() << "finished " << req); };

                    bool resumed = co_await scope[req - 1].Wrap([&]() -> async<void> {
                        Y_DEFER { sequence.push_back(TStringBuilder() << "inner finished " << req); };
                        co_await queue.Next();
                    });
                    sequence.push_back(TStringBuilder() << (resumed ? "resumed " : "cancelled ") << req);
                });
            }

            // Enqueue the 1st cancellation
            actor.SendSync([&]{
                sequence.push_back("cancel 1");
                scope[0].Cancel();
            });

            // Process the 1st request, it will schedule the 1st resume event
            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence, "started 1");

            // Before processing request 2 enqueue the 2nd cancel
            // The rule is that cancellation is already in the mailbox before
            // the request is processed, it must cancel the request, thus
            // the 2nd request is not allowed to attach to the 1st resume.
            actor.SendSync([&]{
                sequence.push_back("cancel 2");
                scope[1].Cancel();
            });

            // Process requests 2 and 3, they will not enqueue resumes yet
            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence, "started 2");
            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence, "started 3");

            // Process the 1st cancellation, it will immediately enqueue the 2nd resume
            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "cancel 1",
                "inner finished 1",
                "cancelled 1",
                "finished 1");

            // Enqueue a sequence check
            actor.SendSync([&]{
                sequence.push_back("after the 2nd resume");
            });

            // Process the 1st resume, it must do nothing
            actor.Step();
            ASYNC_ASSERT_SEQUENCE_EMPTY(sequence);

            // Process the 2nd cancel, it must cancel the 2nd request
            // The 3rd request will be attached to this resume event, because
            // it was sent after the request began waiting
            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "cancel 2",
                "inner finished 2",
                "cancelled 2",
                "finished 2");

            // Process the 2nd resume, it will wake up the 3rd request
            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence,
                "inner finished 3",
                "resumed 3",
                "finished 3");

            // Check the sentinel
            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence, "after the 2nd resume");

            // Validate the mailbox is truly empty
            actor.SendSync([&]{ sequence.push_back("mailbox empty"); });
            actor.Step();
            ASYNC_ASSERT_SEQUENCE(sequence, "mailbox empty");
        }

        Y_UNIT_TEST(ShutdownWhileWaiting) {
            TVector<TString> sequence;
            struct TLocalState {
                // Using a unique_ptr inside a shared_ptr for asan to actually detect use-after-free correctly
                std::unique_ptr<TAsyncLowPriorityQueue> Queue = std::make_unique<TAsyncLowPriorityQueue>();
            };
            std::weak_ptr<TLocalState> localState;
            TAsyncCancellationScope scope[3];
            TAsyncTestActor::TState state;
            TAsyncTestActorRuntime runtime;

            auto actor = runtime.StartAsyncActor(state, [&, ownedState = std::make_shared<TLocalState>()](auto*) -> async<void> {
                // Note: ownedState is part of the callback, and thus part of the actor itself
                localState = ownedState;
                co_return;
            });

            // Process all 3 requests
            for (int i = 0; i < 3; ++i) {
                actor.RunAsync([&sequence, &localState, &scope, req = i + 1]() -> async<void> {
                    sequence.push_back(TStringBuilder() << "started " << req);
                    Y_DEFER { sequence.push_back(TStringBuilder() << "finished " << req); };

                    bool resumed = co_await scope[req - 1].Wrap([&]() -> async<void> {
                        Y_DEFER { sequence.push_back(TStringBuilder() << "inner finished " << req); };
                        co_await localState.lock()->Queue->Next();
                    });
                    sequence.push_back(TStringBuilder() << (resumed ? "resumed " : "cancelled ") << req);
                });
            }

            // There will be a single resume event scheduled
            ASYNC_ASSERT_SEQUENCE(sequence,
                "started 1",
                "started 2",
                "started 3");

            // Shutdown the actor system before the event is process
            // Note: low priority queue will be destroyed before the event
            runtime.CleanupNode();

            UNIT_ASSERT(!localState.lock());

            ASYNC_ASSERT_SEQUENCE(sequence,
                "inner finished 1",
                "finished 1",
                "inner finished 2",
                "finished 2",
                "inner finished 3",
                "finished 3");
        }

    } // Y_UNIT_TEST_SUITE(LowPriorityQueue)

} // namespace NAsyncTest
