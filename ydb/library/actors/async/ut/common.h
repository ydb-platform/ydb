#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NAsyncTest {

    using namespace NActors;

    struct TSuspendAwaiter {
        std::coroutine_handle<>* const ResumePtr;
        std::coroutine_handle<>* const CancelPtr;

        bool AwaitReady() noexcept { return false; }
        void AwaitResume() noexcept {}
        void AwaitSuspend(std::coroutine_handle<> h) {
            *ResumePtr = h;
        }
        void AwaitCancel(std::coroutine_handle<> h) noexcept {
            *CancelPtr = h;
        }
    };

    struct TSuspendStdAwaiter {
        std::coroutine_handle<>* const ResumePtr;
        std::coroutine_handle<>* const CancelPtr;

        bool await_ready() noexcept { return false; }
        void await_resume() noexcept {}
        void await_suspend(std::coroutine_handle<> h) {
            *ResumePtr = h;
        }
        void await_cancel(std::coroutine_handle<> h) noexcept {
            *CancelPtr = h;
        }
    };

    struct TSuspendAwaiterWithoutCancel {
        std::coroutine_handle<>* const ResumePtr;

        bool AwaitReady() noexcept { return false; }
        void AwaitResume() noexcept {}
        void AwaitSuspend(std::coroutine_handle<> h) {
            *ResumePtr = h;
        }
    };

    struct TSuspendStdAwaiterWithoutCancel {
        std::coroutine_handle<>* const ResumePtr;

        bool await_ready() noexcept { return false; }
        void await_resume() noexcept {}
        void await_suspend(std::coroutine_handle<> h) {
            *ResumePtr = h;
        }
    };

    class TAsyncTestActor : public TActorBootstrapped<TAsyncTestActor> {
    public:
        struct TEvPrivate {
            enum EEv {
                EvResumeCoroutine = EventSpaceBegin(TEvents::ES_PRIVATE),
            };

            struct TEvResumeCoroutine : public TEventLocal<TEvResumeCoroutine, EvResumeCoroutine> {
                std::coroutine_handle<> Handle;

                TEvResumeCoroutine(std::coroutine_handle<> h) : Handle(h) {}
            };
        };

        struct TState {
            bool Destroyed = false;
            bool PassAwayCalled = false;
            bool CallbackReturned = false;
        };

        TState& State;
        std::function<async<void>(TAsyncTestActor*)> Callback;

        TAsyncTestActor(TState& state, std::function<async<void>(TAsyncTestActor*)> callback)
            : State(state)
            , Callback(std::move(callback))
        {}

        ~TAsyncTestActor() {
            State.Destroyed = true;
        }

        void PassAway() override {
            State.PassAwayCalled = true;
            IActor::PassAway();
        }

        void Bootstrap() {
            Become(&TThis::StateWork);

            CallCallback();
            State.CallbackReturned = true;
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvResumeCoroutine, Handle);
                hFunc(TEvents::TEvPoison, Handle);
                default:
                    Y_ABORT("Unexpected event");
            }
        }

        void Handle(TEvPrivate::TEvResumeCoroutine::TPtr& ev) {
            ev->Get()->Handle.resume();
        }

        void Handle(TEvents::TEvPoison::TPtr&) {
            PassAway();
        }

        void CallCallback() {
            co_await Callback(this);
        }
    };

    class TAsyncTestActorRuntime : public TTestActorRuntimeBase {
    public:
        TAsyncTestActorRuntime() {
            Initialize();

            SetScheduledEventFilter([](auto&, auto&, auto, auto&) {
                // Don't drop scheduled events
                return false;
            });
        }

        void CleanupNode(ui32 nodeIndex = 0) {
            int attempts = 0;
            auto* node = GetRawNode(nodeIndex);
            while (node->MailboxTable->Cleanup()) {
                Y_ABORT_UNLESS(++attempts < 10);
            }
        }

        TActorId StartAsyncActor(TAsyncTestActor::TState& state, std::function<async<void>(TAsyncTestActor*)> callback) {
            auto actorId = Register(new TAsyncTestActor(state, std::move(callback)));
            EnableScheduleForActor(actorId);
            // Run bootstrap for this actor
            TDispatchOptions options;
            options.OnlyMailboxes.push_back(actorId);
            options.CustomFinalCondition = []() { return true; };
            DispatchEvents(options);
            return actorId;
        }

        void Poison(const TActorId& actor) {
            Send(actor, TActorId(), new TEvents::TEvPoison);
        }

        void ResumeCoroutine(const TActorId& actor, std::coroutine_handle<> h) {
            Send(actor, TActorId(), new TAsyncTestActor::TEvPrivate::TEvResumeCoroutine{ h });
        }
    };

} // namespace NAsyncTest
