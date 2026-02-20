#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/join.h>

#define ASYNC_ASSERT_NO_RETURN(A) \
    do { \
        try { \
            (void)(A); \
        } catch (const ::NUnitTest::TAssertException&) { \
            throw; \
        } catch (...) { \
            UNIT_FAIL_IMPL("no return assertion failed", Sprintf("%s throws exception: %s", #A, CurrentExceptionMessage().c_str())); \
        } \
        UNIT_FAIL_IMPL("no-return assertion failed", Sprintf("%s returned", #A)); \
    } while (false)

#define ASYNC_ASSERT_SEQUENCE(S, ...) \
    do { \
        UNIT_ASSERT_NO_DIFF(::JoinSeq('\n', S), ::JoinSeq('\n', TVector<TString>{ __VA_ARGS__ })); \
        S.clear(); \
    } while (false)

#define ASYNC_ASSERT_SEQUENCE_EMPTY(S) \
    do { \
        UNIT_ASSERT_NO_DIFF(::JoinSeq('\n', S), ""); \
    } while (false)

namespace NAsyncTest {

    using namespace NActors;

    class TTestException : public yexception {};

    struct TSuspendAwaiter {
        static constexpr bool IsActorAwareAwaiter = true;

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
        static constexpr bool IsActorAwareAwaiter = true;

        std::coroutine_handle<>* const ResumePtr;

        bool await_ready() noexcept { return false; }
        void await_resume() noexcept {}
        void await_suspend(std::coroutine_handle<> h) {
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
                EvRunAsync,
                EvRunSync,
            };

            struct TEvResumeCoroutine : public TEventLocal<TEvResumeCoroutine, EvResumeCoroutine> {
                std::coroutine_handle<> Handle;

                TEvResumeCoroutine(std::coroutine_handle<> h) : Handle(h) {}
            };

            struct TEvRunAsync : public TEventLocal<TEvRunAsync, EvRunAsync> {
                std::function<async<void>()> Callable;

                TEvRunAsync(std::function<async<void>()> callable)
                    : Callable(std::move(callable))
                {}
            };

            struct TEvRunSync : public TEventLocal<TEvRunSync, EvRunSync> {
                std::function<void()> Callable;

                TEvRunSync(std::function<void()> callable)
                    : Callable(std::move(callable))
                {}
            };
        };

        struct TState {
            bool Destroyed = false;
            bool PassAwayCalled = false;
            bool CallbackReturned = false;
        };

        TState& State;
        std::function<async<void>(TAsyncTestActor*)> Callback;
        std::function<bool(TAutoPtr<IEventHandle>& ev)> Handler;

        TAsyncTestActor(TState& state,
                std::function<async<void>(TAsyncTestActor*)> callback = {},
                std::function<bool(TAutoPtr<IEventHandle>& ev)> handler = {})
            : State(state)
            , Callback(std::move(callback))
            , Handler(std::move(handler))
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

            if (Callback) {
                CallCallback();
            }
            State.CallbackReturned = true;
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvResumeCoroutine, Handle);
                hFunc(TEvPrivate::TEvRunAsync, Handle);
                hFunc(TEvPrivate::TEvRunSync, Handle);
                hFunc(TEvents::TEvPoison, Handle);
                default:
                    if (!Handler || !Handler(ev)) {
                        Y_ABORT("Unexpected event");
                    }
            }
        }

        void Handle(TEvPrivate::TEvResumeCoroutine::TPtr& ev) {
            ev->Get()->Handle.resume();
        }

        void Handle(TEvPrivate::TEvRunAsync::TPtr ev) {
            co_await ev->Get()->Callable();
        }

        void Handle(TEvPrivate::TEvRunSync::TPtr ev) {
            ev->Get()->Callable();
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

        class TAsyncActorOperations {
        public:
            TAsyncActorOperations(TAsyncTestActorRuntime& runtime, const TActorId& actorId)
                : Runtime(runtime)
                , ActorId(actorId)
            {}

            ~TAsyncActorOperations() { /* silence unused warnings */ }

            void Step() const {
                TDispatchOptions options;
                options.OnlyMailboxes.push_back(ActorId);
                options.CustomFinalCondition = []() { return true; };
                Runtime.DispatchEvents(options);
            }

            void Receive(IEventBase* event, ui64 cookie = 0) const {
                Runtime.Send(new IEventHandle(ActorId, TActorId(), event, 0, cookie));
            }

            void Send(IEventBase* event, ui64 cookie = 0) const {
                Runtime.Send(new IEventHandle(ActorId, TActorId(), event, 0, cookie), 0, true);
            }

            void Poison() const {
                Receive(new TEvents::TEvPoison);
            }

            void ResumeCoroutine(std::coroutine_handle<> h) const {
                Receive(new TAsyncTestActor::TEvPrivate::TEvResumeCoroutine(h));
            }

            void RunAsync(std::function<async<void>()> callable) const {
                Receive(new TAsyncTestActor::TEvPrivate::TEvRunAsync(std::move(callable)));
            }

            void SendAsync(std::function<async<void>()> callable) const {
                Send(new TAsyncTestActor::TEvPrivate::TEvRunAsync(std::move(callable)));
            }

            void RunSync(std::function<void()> callable) const {
                Receive(new TAsyncTestActor::TEvPrivate::TEvRunSync(std::move(callable)));
            }

            void SendSync(std::function<void()> callable) const {
                Send(new TAsyncTestActor::TEvPrivate::TEvRunSync(std::move(callable)));
            }

        private:
            TAsyncTestActorRuntime& Runtime;
            const TActorId ActorId;
        };

        template<class... TArgs>
        TAsyncActorOperations StartAsyncActor(TArgs&&... args) {
            auto actorId = Register(new TAsyncTestActor(std::forward<TArgs>(args)...));
            EnableScheduleForActor(actorId);
            TAsyncActorOperations actor(*this, actorId);
            // Run bootstrap for this actor
            actor.Step();
            return actor;
        }
    };

} // namespace NAsyncTest
