#pragma once
#include "abi.h"
#include "result.h"
#include "callback_coroutine.h"
#include <ydb/library/actors/core/actor.h>
#include <coroutine>
#include <utility>

namespace NActors {

    namespace NDetail {

        template<class T>
        class TAsyncAwaiter;

        template<class T>
        class TAsyncPromise;

    } // namespace NDetail

    /**
     * Used when callee cannot finish and produce the result due to cancellation
     */
    class TAsyncCancellation : public yexception {};

    /**
     * A marker-type for actor coroutines (async functions)
     *
     * This class cannot be copied or moved, and may only be used directly in a
     * co_await expression in supported higher level coroutines.
     */
    template<class T>
    class [[nodiscard]] async {
        friend NDetail::TAsyncAwaiter<T>;
        friend NDetail::TAsyncPromise<T>;

    public:
        using promise_type = NDetail::TAsyncPromise<T>;

        ~async() {
            Handle_.destroy();
        }

        std::coroutine_handle<NDetail::TAsyncPromise<T>> GetHandle() const noexcept {
            return Handle_;
        }

    private:
        explicit async(std::coroutine_handle<NDetail::TAsyncPromise<T>> handle)
            : Handle_(handle)
        {}

        async(const async&) = delete;
        async& operator=(const async&) = delete;

    private:
        std::coroutine_handle<NDetail::TAsyncPromise<T>> Handle_;
    };

    namespace NDetail {
        template<class T>
        struct TIsAsyncCoroutineHelper : public std::false_type {};
        template<class T>
        struct TIsAsyncCoroutineHelper<async<T>> : public std::true_type {};
        template<class T>
        struct TAsyncCoroutineResultHelper {};
        template<class T>
        struct TAsyncCoroutineResultHelper<async<T>> { typedef T type; };
    }

    /**
     * Concept matches all async<T> types
     */
    template<class T>
    concept IsAsyncCoroutine = NDetail::TIsAsyncCoroutineHelper<T>::value;

    /**
     * Concept matches all callbacks returning async<T>
     */
    template<class TCallback, class... TArgs>
    concept IsAsyncCoroutineCallback = requires (TCallback&& callback, TArgs&&... args) {
        { std::forward<TCallback>(callback)(std::forward<TArgs>(args)...) } -> IsAsyncCoroutine;
    };

    /**
     * Extracts the result type (T) from the type async<T>
     */
    template<IsAsyncCoroutine T>
    using TAsyncCoroutineResult = typename NDetail::TAsyncCoroutineResultHelper<T>::type;

    namespace NDetail {

        /**
         * Base class for awaiters which can integrate with actors directly
         */
        struct TActorAwareAwaiter {};

        /**
         * Concept matches all types that subclass TActorAwareAwaiter
         */
        template<class TAwaiter>
        concept IsActorAwareAwaiter = std::is_convertible_v<TAwaiter&, TActorAwareAwaiter&>;

        /**
         * Returns a coroutine handle, which would arrange for the specified
         * runnable item to run on the specified actor when resumed. Used to
         * interface with generic C++ coroutines.
         */
        std::coroutine_handle<> MakeBridgeCoroutine(IActor* actor, TActorRunnableItem* item);

        /**
         * Returns a pair of coroutine handles, which would arrange for one of
         * runnable items to run on the specified actor when resumed. Used to
         * interface with generic C++ coroutines.
         *
         * Exactly one coroutine must be resumed or destroyed.
         */
        std::pair<std::coroutine_handle<>, std::coroutine_handle<>> MakeBridgeCoroutines(
            IActor* actor, TActorRunnableItem* item1, TActorRunnableItem* item2);

        template<class TAwaiter, class TPromise = void>
        concept IsAwaiter = requires(TAwaiter& awaiter, std::coroutine_handle<TPromise> h) {
            awaiter.AwaitReady();
            awaiter.AwaitSuspend(h);
            awaiter.AwaitResume();
        };

        template<class TAwaiter>
        concept HasAwaitReadyNoExcept = requires(TAwaiter& awaiter) {
            { awaiter.AwaitReady() } noexcept;
        };

        template<class TAwaiter, class TPromise = void>
        concept HasAwaitSuspendVoid = requires(TAwaiter& awaiter, std::coroutine_handle<TPromise> h) {
            { awaiter.AwaitSuspend(h) } -> std::same_as<void>;
        };

        template<class TAwaiter, class TPromise = void>
        concept HasAwaitSuspendBool = requires(TAwaiter& awaiter, std::coroutine_handle<TPromise> h) {
            { awaiter.AwaitSuspend(h) } -> std::same_as<bool>;
        };

        template<class TAwaiter, class TPromise = void>
        concept HasAwaitSuspendHandle = requires(TAwaiter& awaiter, std::coroutine_handle<TPromise> h) {
            { awaiter.AwaitSuspend(h) } -> std::convertible_to<std::coroutine_handle<>>;
        };

        template<class TAwaiter, class TPromise = void>
        concept HasAwaitSuspendNoExcept = requires(TAwaiter& awaiter, std::coroutine_handle<TPromise> h) {
            { awaiter.AwaitSuspend(h) } noexcept;
        };

        template<class TAwaiter>
        concept HasAwaitResumeNoExcept = requires(TAwaiter& awaiter) {
            { awaiter.AwaitResume() } noexcept;
        };

        template<class TAwaiter>
        concept HasAwaitCancel = requires(TAwaiter& awaiter, std::coroutine_handle<> h) {
            awaiter.AwaitCancel(h);
        };

        template<class TAwaiter>
        concept HasAwaitCancelVoid = requires(TAwaiter& awaiter, std::coroutine_handle<> h) {
            { awaiter.AwaitCancel(h) } -> std::same_as<void>;
        };

        template<class TAwaiter>
        concept HasAwaitCancelNoExcept = requires(TAwaiter& awaiter, std::coroutine_handle<> h) {
            { awaiter.AwaitCancel(h) } noexcept;
        };

        template<class TAwaitable>
        concept HasMemberCoAwait = requires(TAwaitable&& awaitable) {
            std::forward<TAwaitable>(awaitable).operator co_await();
        };

        template<class TAwaitable>
        concept HasGlobalCoAwait = requires(TAwaitable&& awaitable) {
            operator co_await(std::forward<TAwaitable>(awaitable));
        };

        template<class TAwaitable>
        concept HasCoAwait = (
            HasMemberCoAwait<TAwaitable> ||
            HasGlobalCoAwait<TAwaitable>);

        template<class TAwaiter, class TPromise = void>
        concept IsStdAwaiter = requires(TAwaiter& awaiter, std::coroutine_handle<TPromise> h) {
            awaiter.await_ready();
            awaiter.await_suspend(h);
            awaiter.await_resume();
        };

        template<class TAwaitable, class TPromise = void>
        concept IsStdAwaitable = (
            HasCoAwait<TAwaitable> ||
            IsStdAwaiter<TAwaitable, TPromise>);

        template<class TAwaiter>
        concept HasStdAwaitReadyNoExcept = requires(TAwaiter& awaiter) {
            { awaiter.await_ready() } noexcept;
        };

        template<class TAwaiter, class TPromise = void>
        concept HasStdAwaitSuspendVoid = requires(TAwaiter& awaiter, std::coroutine_handle<TPromise> h) {
            { awaiter.await_suspend(h) } -> std::same_as<void>;
        };

        template<class TAwaiter, class TPromise = void>
        concept HasStdAwaitSuspendBool = requires(TAwaiter& awaiter, std::coroutine_handle<TPromise> h) {
            { awaiter.await_suspend(h) } -> std::same_as<bool>;
        };

        template<class TAwaiter, class TPromise = void>
        concept HasStdAwaitSuspendHandle = requires(TAwaiter& awaiter, std::coroutine_handle<TPromise> h) {
            { awaiter.await_suspend(h) } -> std::convertible_to<std::coroutine_handle<>>;
        };

        template<class TAwaiter, class TPromise = void>
        concept HasStdAwaitSuspendNoExcept = requires(TAwaiter& awaiter, std::coroutine_handle<TPromise> h) {
            { awaiter.await_suspend(h) } noexcept;
        };

        template<class TAwaiter>
        concept HasStdAwaitResumeNoExcept = requires(TAwaiter& awaiter) {
            { awaiter.await_resume() } noexcept;
        };

        template<class TAwaiter>
        concept HasStdAwaitCancel = requires(TAwaiter& awaiter, std::coroutine_handle<> h) {
            awaiter.await_cancel(h);
        };

        template<class TAwaiter>
        concept HasStdAwaitCancelVoid = requires(TAwaiter& awaiter, std::coroutine_handle<> h) {
            { awaiter.await_cancel(h) } -> std::same_as<void>;
        };

        template<class TAwaiter>
        concept HasStdAwaitCancelNoExcept = requires(TAwaiter& awaiter, std::coroutine_handle<> h) {
            { awaiter.await_cancel(h) } noexcept;
        };

        /**
         * Returns an awaiter for the provided awaitable
         */
        template<class TAwaitable>
        inline decltype(auto) GetAwaiter(TAwaitable&& awaitable) {
            if constexpr (requires { std::forward<TAwaitable>(awaitable).operator co_await(); }) {
                return std::forward<TAwaitable>(awaitable).operator co_await();
            } else if constexpr (requires { operator co_await(std::forward<TAwaitable>(awaitable)); }) {
                return operator co_await(std::forward<TAwaitable>(awaitable));
            } else {
                return std::forward<TAwaitable>(awaitable);
            }
        }

        class TCleanupCallback;

        template<class TPromise>
        concept HasGetActor = requires (TPromise& promise) {
            { promise.GetActor() } -> std::convertible_to<IActor*>;
        };

        template<class TPromise>
        concept HasGetCancellation = requires (TPromise& promise) {
            { promise.GetCancellation() } -> std::convertible_to<std::coroutine_handle<>>;
        };

        template<class TPromise, class TAwaiter>
        concept HasSetAwaiter = requires (TPromise& promise, TAwaiter* awaiter) {
            { promise.SetAwaiter(awaiter) } -> std::same_as<TCleanupCallback>;
        };

        /**
         * A very small RAII class for type erased cleanup
         */
        class TCleanupCallback {
        public:
            TCleanupCallback() noexcept
                : Fn_(nullptr)
            {}

            TCleanupCallback(void (*fn)(void*) noexcept, void* arg) noexcept
                : Fn_(fn)
                , Arg_(arg)
            {}

            TCleanupCallback(TCleanupCallback&& rhs) noexcept
                : Fn_(rhs.Fn_)
                , Arg_(rhs.Arg_)
            {
                rhs.Fn_ = nullptr;
            }

            TCleanupCallback& operator=(TCleanupCallback&& rhs) noexcept {
                if (this != &rhs) [[likely]] {
                    Cleanup();
                    Fn_ = rhs.Fn_;
                    Arg_ = rhs.Arg_;
                    rhs.Fn_ = nullptr;
                }
                return *this;
            }

            ~TCleanupCallback() noexcept {
                Cleanup();
            }

            explicit operator bool() const noexcept {
                return Fn_ != nullptr;
            }

            void operator()() noexcept {
                Cleanup();
            }

        private:
            void Cleanup() noexcept {
                if (Fn_) {
                    (*Fn_)(Arg_);
                    Fn_ = nullptr;
                }
            }

        private:
            void (*Fn_)(void*) noexcept;
            void* Arg_;
        };

        /**
         * Base class for promises that propagates AwaitCancel to awaiters
         */
        class TAwaitCancelPropagation {
        public:
            template<class TAwaiter>
            TCleanupCallback SetAwaiter(TAwaiter* awaiter) noexcept {
                static_assert(HasAwaitCancelVoid<TAwaiter>, "AwaitCancel must return void");
                static_assert(HasAwaitCancelNoExcept<TAwaiter>, "AwaitCancel must be noexcept");
                CancelFn_ = +[](void* ptr, std::coroutine_handle<> h) noexcept {
                    reinterpret_cast<TAwaiter*>(ptr)->AwaitCancel(h);
                };
                CancelFnArg_ = awaiter;
                return TCleanupCallback(
                    +[](void* ptr) noexcept {
                        reinterpret_cast<TAwaitCancelPropagation*>(ptr)->CancelFn_ = nullptr;
                    },
                    this);
            }

            void AwaitCancel(std::coroutine_handle<> h) noexcept {
                if (CancelFn_) {
                    CancelFn_(CancelFnArg_, h);
                }
            }

        private:
            void (*CancelFn_)(void*, std::coroutine_handle<>) noexcept = nullptr;
            void* CancelFnArg_;
        };

        /**
         * Awaiter implementation for async<T> values, should not be used directly.
         */
        template<class T>
        class TAsyncAwaiter : public TActorAwareAwaiter {
        public:
            explicit TAsyncAwaiter(async<T>&& owner)
                : Handle(owner.GetHandle())
            {}

            bool await_ready() noexcept {
                return false;
            }

            template<class TPromise>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> c) noexcept {
                Handle.promise().SetContinuation(c);
                if constexpr (HasGetCancellation<TPromise>) {
                    Handle.promise().SetCancellation(c.promise().GetCancellation());
                }
                if constexpr (HasSetAwaiter<TPromise, TAsyncPromise<T>>) {
                    if (!Handle.promise().GetCancellation()) {
                        Cleanup = c.promise().SetAwaiter(&Handle.promise());
                    }
                }
                return Handle;
            }

            T await_resume() {
                Cleanup();
                return Handle.promise().Result.ExtractValue();
            }

        private:
            std::coroutine_handle<TAsyncPromise<T>> Handle;
            TCleanupCallback Cleanup;
        };

        /**
         * Common bases class for promises, provides await_transform support
         */
        template<class TPromise>
        class TAsyncAwaitTransform {
        public:
            template<class TAwaiter>
            class TAwaiterProxy {
            public:
                template<class... TArgs>
                explicit TAwaiterProxy(TArgs&&... args)
                    : Awaiter{ std::forward<TArgs>(args)... }
                {}

                TAwaiterProxy(TAwaiterProxy&&) = delete;
                TAwaiterProxy(const TAwaiterProxy&) = delete;
                TAwaiterProxy& operator=(TAwaiterProxy&&) = delete;
                TAwaiterProxy& operator=(const TAwaiterProxy&) = delete;

                bool await_ready() noexcept(HasAwaitReadyNoExcept<TAwaiter>) {
                    return Awaiter.AwaitReady();
                }

                std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> self) {
                    if constexpr (HasAwaitCancel<TAwaiter>) {
                        if (auto cancellation = self.promise().GetCancellation()) {
                            return cancellation;
                        }

                        Cleanup = self.promise().SetAwaiter(&Awaiter);
                    }

                    TCallCleanup<HasAwaitCancel<TAwaiter> && !HasAwaitSuspendNoExcept<TAwaiter, TPromise>> callCleanup{ this };
                    std::coroutine_handle<> result = DoSuspend(self);
                    callCleanup.Cancel();
                    return result;
                }

                decltype(auto) await_resume() noexcept(HasAwaitResumeNoExcept<TAwaiter>) {
                    if constexpr (HasAwaitCancel<TAwaiter>) {
                        Cleanup();
                    }
                    return Awaiter.AwaitResume();
                }

            private:
                std::coroutine_handle<> DoSuspend(std::coroutine_handle<TPromise> self)
                    noexcept(HasAwaitSuspendNoExcept<TAwaiter, TPromise>)
                    requires HasAwaitSuspendVoid<TAwaiter, TPromise>
                {
                    Awaiter.AwaitSuspend(self);
                    return std::noop_coroutine();
                }

                std::coroutine_handle<> DoSuspend(std::coroutine_handle<TPromise> self)
                    noexcept(HasAwaitSuspendNoExcept<TAwaiter, TPromise>)
                    requires HasAwaitSuspendBool<TAwaiter, TPromise>
                {
                    bool suspended = Awaiter.AwaitSuspend(self);
                    if (suspended) {
                        return std::noop_coroutine();
                    } else {
                        return self;
                    }
                }

                std::coroutine_handle<> DoSuspend(std::coroutine_handle<TPromise> self)
                    noexcept(HasAwaitSuspendNoExcept<TAwaiter, TPromise>)
                    requires HasAwaitSuspendHandle<TAwaiter, TPromise>
                {
                    return Awaiter.AwaitSuspend(self);
                }

            private:
                template<bool Enabled = true>
                struct TCallCleanup {
                    TAwaiterProxy* Self;

                    ~TCallCleanup() {
                        if (Self) {
                            Self->Cleanup();
                        }
                    }

                    void Cancel() noexcept {
                        Self = nullptr;
                    }
                };

                template<>
                struct TCallCleanup<false> {
                    TCallCleanup(TAwaiterProxy*) noexcept {}
                    void Cancel() noexcept {}
                };

            private:
                TAwaiter Awaiter;
                TCleanupCallback Cleanup;
            };

            template<class TAwaiter>
            auto await_transform(TAwaiter&& awaiter) noexcept
                requires (IsAwaiter<TAwaiter, TPromise> && !HasCoAwait<TAwaiter>)
            {
                return TAwaiterProxy<TAwaiter&&>{ std::forward<TAwaiter>(awaiter) };
            }

            template<class TDerived, bool Enabled = true>
            class TThreadSafeResumeBridge {
            public:
                TThreadSafeResumeBridge() = default;

                TThreadSafeResumeBridge(const TThreadSafeResumeBridge&) = delete;
                TThreadSafeResumeBridge& operator=(const TThreadSafeResumeBridge&) = delete;

            protected:
                std::coroutine_handle<> CreateResumeBridge(IActor* actor, TActorRunnableItem* resume) {
                    auto pr = MakeBridgeCoroutines(actor, resume, &CancelledTrampoline);
                    CancelBridge = pr.second;
                    return pr.first;
                }

                std::coroutine_handle<> StartCancellation(std::coroutine_handle<> h) noexcept {
                    Cancellation = h;
                    return CancelBridge;
                }

            private:
                struct TCancelledTrampoline : public TActorRunnableItem::TImpl<TCancelledTrampoline> {
                    TThreadSafeResumeBridge* const Self;

                    TCancelledTrampoline(TThreadSafeResumeBridge* self)
                        : Self(self)
                    {}

                    void DoRun(IActor*) noexcept {
                        Self->OnCancelled();
                    }
                };

                void OnCancelled() noexcept {
                    Y_ABORT_UNLESS(Cancellation, "Unexpected cancellation without StartCancellation");
                    static_cast<TDerived&>(*this).OnCancelled(Cancellation);
                }

            private:
                TCancelledTrampoline CancelledTrampoline{ this };
                std::coroutine_handle<> CancelBridge;
                std::coroutine_handle<> Cancellation;
            };

            template<class TDerived>
            class TThreadSafeResumeBridge<TDerived, false> {
            protected:
                std::coroutine_handle<> CreateResumeBridge(IActor* actor, TActorRunnableItem* resume) {
                    return MakeBridgeCoroutine(actor, resume);
                }
            };

            template<class TAwaiter>
            class TStdAwaiterProxy final
                : private TActorRunnableItem::TImpl<TStdAwaiterProxy<TAwaiter>>
                , private TThreadSafeResumeBridge<TStdAwaiterProxy<TAwaiter>, HasStdAwaitCancel<TAwaiter>>
            {
                friend TActorRunnableItem::TImpl<TStdAwaiterProxy<TAwaiter>>;
                friend TThreadSafeResumeBridge<TStdAwaiterProxy<TAwaiter>, HasStdAwaitCancel<TAwaiter>>;

            public:
                template<class... TArgs>
                TStdAwaiterProxy(TArgs&&... args)
                    : Awaiter{ std::forward<TArgs>(args)... }
                {}

                TStdAwaiterProxy(TStdAwaiterProxy&&) = delete;
                TStdAwaiterProxy(const TStdAwaiterProxy&) = delete;
                TStdAwaiterProxy& operator=(TStdAwaiterProxy&&) = delete;
                TStdAwaiterProxy& operator=(const TStdAwaiterProxy&) = delete;

                bool await_ready() noexcept(HasStdAwaitReadyNoExcept<TAwaiter>) {
                    return Awaiter.await_ready();
                }

                std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> self) {
                    IActor* actor = self.promise().GetActor();
                    if (!actor) [[unlikely]] {
                        throw std::logic_error("coroutine not bound to actor");
                    }

                    if constexpr (HasStdAwaitCancel<TAwaiter>) {
                        if (auto cancellation = self.promise().GetCancellation()) {
                            return cancellation;
                        }

                        Cleanup = self.promise().SetAwaiter(this);
                    }

                    TCallCleanup<HasStdAwaitCancel<TAwaiter>> callCleanup{ this };

                    Continuation = self;
                    Bridge = this->CreateResumeBridge(actor, this);

                    TCallDestroyBridge callDestroyBridge{ this };
                    std::coroutine_handle<> result = DoSuspend(Bridge);
                    callDestroyBridge.Cancel();

                    callCleanup.Cancel();

                    // Handle awaiter resuming immediately
                    // Redirect back to self and avoid bridge overhead
                    if (result == Bridge) {
                        DestroyBridge();
                        return self;
                    } else {
                        return result;
                    }
                }

                decltype(auto) await_resume() noexcept(HasStdAwaitResumeNoExcept<TAwaiter>) {
                    if constexpr (HasStdAwaitCancel<TAwaiter>) {
                        Cleanup();
                    }
                    return Awaiter.await_resume();
                }

                void AwaitCancel(std::coroutine_handle<> c) noexcept
                    requires HasStdAwaitCancel<TAwaiter>
                {
                    static_assert(HasStdAwaitCancelVoid<TAwaiter>, "await_cancel must return void");
                    static_assert(HasStdAwaitCancelNoExcept<TAwaiter>, "await_cancel must be noexcept");
                    Awaiter.await_cancel(this->StartCancellation(c));
                }

                void OnCancelled(std::coroutine_handle<> c) noexcept
                    requires HasStdAwaitCancel<TAwaiter>
                {
                    Y_ABORT_UNLESS(Bridge, "Unexpected cancellation without a bridge");
                    DestroyBridge();
                    c.resume();
                }

            private:
                std::coroutine_handle<> DoSuspend(std::coroutine_handle<> target)
                    noexcept(HasStdAwaitSuspendNoExcept<TAwaiter>)
                    requires HasStdAwaitSuspendVoid<TAwaiter>
                {
                    Awaiter.await_suspend(target);
                    return std::noop_coroutine();
                }

                std::coroutine_handle<> DoSuspend(std::coroutine_handle<> target)
                    noexcept(HasStdAwaitSuspendNoExcept<TAwaiter>)
                    requires HasStdAwaitSuspendBool<TAwaiter>
                {
                    bool suspended = Awaiter.await_suspend(target);
                    if (suspended) {
                        return std::noop_coroutine();
                    } else {
                        return target;
                    }
                }

                std::coroutine_handle<> DoSuspend(std::coroutine_handle<> target)
                    noexcept(HasStdAwaitSuspendNoExcept<TAwaiter>)
                    requires HasStdAwaitSuspendHandle<TAwaiter>
                {
                    return Awaiter.await_suspend(target);
                }

            private:
                template<bool Enabled = true>
                struct TCallCleanup {
                    TStdAwaiterProxy* Self;

                    ~TCallCleanup() {
                        if (Self) {
                            Self->Cleanup();
                        }
                    }

                    void Cancel() noexcept {
                        Self = nullptr;
                    }
                };

                template<>
                struct TCallCleanup<false> {
                    TCallCleanup(TStdAwaiterProxy*) noexcept {}
                    void Cancel() noexcept {}
                };

            private:
                struct TCallDestroyBridge {
                    TStdAwaiterProxy* Self;

                    ~TCallDestroyBridge() {
                        if (Self) {
                            Self->DestroyBridge();
                        }
                    }

                    void Cancel() {
                        Self = nullptr;
                    }
                };

                void DestroyBridge() noexcept {
                    Bridge.destroy();
                    Bridge = {};
                }

            private:
                void DoRun(IActor*) noexcept {
                    Y_ABORT_UNLESS(Bridge && Continuation, "Unexpected Run without a bridge or continuation");
                    DestroyBridge();
                    // We may resume recursively
                    Continuation.resume();
                }

            private:
                TAwaiter Awaiter;
                TCleanupCallback Cleanup;
                std::coroutine_handle<> Bridge;
                std::coroutine_handle<> Continuation;
            };

            /**
             * Transforms standard C++ awaiters into a form that makes sure to resume on the same mailbox
             */
            template<class TAwaiter>
            decltype(auto) await_transform(TAwaiter&& awaiter)
                requires (IsStdAwaiter<TAwaiter> && !HasCoAwait<TAwaiter>)
            {
                if constexpr (IsActorAwareAwaiter<TAwaiter>) {
                    // We return the exact same awaiter reference
                    return std::forward<TAwaiter>(awaiter);
                } else {
                    return TStdAwaiterProxy<TAwaiter&&>{ std::forward<TAwaiter>(awaiter) };
                }
            }

            /**
             * Transforms standard C++ awaitables into a form that makes sure to resume on the same mailbox
             */
            template<class TAwaitable>
            decltype(auto) await_transform(TAwaitable&& awaitable)
                requires (HasCoAwait<TAwaitable>)
            {
                using TAwaiter = decltype(GetAwaiter(std::forward<TAwaitable>(awaitable)));
                static_assert(IsStdAwaiter<TAwaiter>, "operator co_await returns a type that is not an awaiter");

                if constexpr (IsActorAwareAwaiter<TAwaiter>) {
                    // We return the exact same awaitable reference
                    // The coroutine implementation will call operator co_await when needed
                    return std::forward<TAwaitable>(awaitable);
                } else {
                    // We use an implicit conversion to support awaiters that cannot be moved
                    struct TImplicitConverter {
                        TAwaitable&& Awaitable;
                        operator TAwaiter() {
                            return GetAwaiter(std::forward<TAwaitable>(Awaitable));
                        }
                    };
                    return TStdAwaiterProxy<TAwaiter>{ TImplicitConverter{ std::forward<TAwaitable>(awaitable) } };
                }
            }

            /**
             * Accepts async<T> by value to only allow co_await of the call itself, not a stored variable.
             *
             * Taking address of async<T> should be safe here, since it has a destructor, and will be allocated as a
             * temporary by the caller. Temporaries are not destroyed until the full expression (co_await) completes.
             */
            template<class T>
            auto await_transform(async<T> c) {
                return TAsyncAwaiter<T>{std::move(c)};
            }
        };

        template<class T>
        class TAsyncPromiseResultHandler {
        public:
            template<class U>
            void return_value(U&& value)
                requires (std::is_convertible_v<U&&, T>)
            {
                this->Result.SetValue(std::forward<U>(value));
            }

            void unhandled_exception() noexcept {
                this->Result.SetException(std::current_exception());
            }

        public:
            TAsyncResult<T> Result;
        };

        template<>
        class TAsyncPromiseResultHandler<void> {
        public:
            void return_void() {
                this->Result.SetValue();
            }

            void unhandled_exception() noexcept {
                this->Result.SetException(std::current_exception());
            }

        public:
            TAsyncResult<void> Result;
        };

        template<class T>
        class TAsyncPromise
            : public TAsyncPromiseResultHandler<T>
            , public TAsyncAwaitTransform<TAsyncPromise<T>>
            , public TAwaitCancelPropagation
        {
        public:
            async<T> get_return_object() noexcept {
                return async<T>(std::coroutine_handle<TAsyncPromise<T>>::from_promise(*this));
            }

            struct TFinalSuspend {
                bool await_ready() noexcept { return false; }
                void await_resume() noexcept {}

                std::coroutine_handle<> await_suspend(std::coroutine_handle<TAsyncPromise<T>> self) noexcept {
                    return self.promise().Continuation;
                }
            };

            constexpr auto initial_suspend() noexcept { return std::suspend_always{}; }
            constexpr auto final_suspend() noexcept { return TFinalSuspend{}; }

            std::coroutine_handle<> GetContinuation() const noexcept {
                return Continuation;
            }

            void SetContinuation(std::coroutine_handle<> c) noexcept {
                Continuation = c;
            }

            template<class TPromise>
            void SetContinuation(std::coroutine_handle<TPromise> c) noexcept
                requires (!std::is_void_v<TPromise>)
            {
                Continuation = c;
                if constexpr (HasGetActor<TPromise>) {
                    Actor = c.promise().GetActor();
                }
            }

            IActor* GetActor() const {
                return Actor;
            }

            void SetActor(IActor* actor) {
                Actor = actor;
            }

            std::coroutine_handle<> GetCancellation() const {
                return Cancellation;
            }

            void SetCancellation(std::coroutine_handle<> h) {
                Cancellation = h;
            }

            void AwaitCancel(std::coroutine_handle<> h) noexcept {
                SetCancellation(h);
                TAwaitCancelPropagation::AwaitCancel(h);
            }

        private:
            std::coroutine_handle<> Continuation;
            IActor* Actor = nullptr;
            std::coroutine_handle<> Cancellation;
        };

        template<class T>
        concept IsActorSubClassType = std::is_convertible_v<T&, IActor&>;

        template<class T>
        concept IsNonReferenceType = !std::is_reference_v<T>;

        class TActorAsyncHandlerPromise final
            : private TActorTask
            , public TAsyncAwaitTransform<TActorAsyncHandlerPromise>
            , public TAwaitCancelPropagation
            , private TCustomCoroutineCallbacks<TActorAsyncHandlerPromise>
        {
            friend TCustomCoroutineCallbacks<TActorAsyncHandlerPromise>;

        public:
            constexpr void get_return_object() noexcept {}

            constexpr auto initial_suspend() noexcept { return std::suspend_never{}; }
            constexpr auto final_suspend() noexcept { return std::suspend_never{}; }

            void unhandled_exception() noexcept;
            void return_void() noexcept {}

            IActor* GetActor() const {
                return &Actor;
            }

            std::coroutine_handle<> GetCancellation() const {
                return Cancellation;
            }

        public:
            template<class... Args>
            TActorAsyncHandlerPromise(IActor& self, Args&&...)
                : Actor(self)
            {
                if (!Actor.RegisterActorTask(this)) {
                    // We start in a cancelled state
                    Cancel();
                }
            }

            ~TActorAsyncHandlerPromise() {
                Actor.UnregisterActorTask(this);
            }

        private:
            void Cancel() noexcept override {
                if (!Cancellation) {
                    Cancellation = this->ToCoroutineHandle();
                    this->AwaitCancel(Cancellation);
                }
            }

            void Destroy() noexcept override {
                std::coroutine_handle<TActorAsyncHandlerPromise>::from_promise(*this).destroy();
            }

            void OnResume() noexcept {
                Destroy();
            }

            void OnDestroy() noexcept {
                Y_ABORT("Unexpected destroy for cancellation coroutine proxy");
            }

        private:
            IActor& Actor;
            std::coroutine_handle<> Cancellation;
        };

    } // namespace NDetail

} // namespace NActors

template<NActors::NDetail::IsActorSubClassType T, NActors::NDetail::IsNonReferenceType... Args>
struct std::coroutine_traits<void, T&, Args...> {
    using promise_type = NActors::NDetail::TActorAsyncHandlerPromise;
};
