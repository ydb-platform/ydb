#pragma once
#include "abi.h"
#include "result.h"
#include "callback_coroutine.h"
#include <ydb/library/actors/core/actor.h>
#include <coroutine>
#include <functional>
#include <utility>

namespace NActors {

    namespace NDetail {
        template<class T>
        class TAsyncPromise;
    }

    /**
     * A marker-type for actor coroutines (async functions)
     *
     * This class cannot be copied or moved, and may only be used directly in a
     * co_await expression in supported higher level coroutines.
     */
    template<class T>
    class [[nodiscard]] async {
        friend NDetail::TAsyncPromise<T>;

    public:
        using result_type = T;
        using promise_type = NDetail::TAsyncPromise<T>;
        using THandle = std::coroutine_handle<promise_type>;

        ~async() {
            Destroy();
        }

        constexpr explicit operator bool() const noexcept {
            return bool(Handle);
        }

        constexpr THandle GetHandle() const noexcept {
            return Handle;
        }

        void Destroy() noexcept {
            if (Handle) {
                Handle.destroy();
                Handle = nullptr;
            }
        }

        class TAwaiter {
        public:
            static constexpr bool IsActorAwareAwaiter = true;

            constexpr bool await_ready() noexcept {
                return false;
            }

            template<class TPromise>
            constexpr std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> caller) noexcept {
                Handle.promise().SetContinuation(caller);
                return Handle;
            }

            T await_resume() {
                return Handle.promise().ExtractValue();
            }

        private:
            constexpr TAwaiter(THandle handle) noexcept : Handle(handle) {}
            friend async;

        private:
            THandle Handle;
        };

        constexpr TAwaiter CoAwaitByValue() && noexcept {
            return TAwaiter{ Handle };
        }

        constexpr async UnsafeMove() && noexcept {
            auto h = Handle;
            Handle = nullptr;
            return async(h);
        }

        constexpr void UnsafeMoveFrom(async&& rhs) noexcept {
            if (Handle) [[unlikely]] {
                Handle.destroy();
            }
            Handle = rhs.Handle;
            rhs.Handle = nullptr;
        }

        static constexpr async UnsafeEmpty() noexcept {
            return async(nullptr);
        }

    private:
        constexpr explicit async(std::nullptr_t) noexcept
            : Handle(nullptr)
        {}

        constexpr explicit async(THandle handle) noexcept
            : Handle(handle)
        {}

        async(const async&) = delete;
        async& operator=(const async&) = delete;

    private:
        THandle Handle;
    };

    namespace NDetail {
        template<class T>
        struct TIsAsyncCoroutineHelper : public std::false_type {};
        template<class T>
        struct TIsAsyncCoroutineHelper<async<T>> : public std::true_type {};
    }

    /**
     * Concept matches all async<T> types
     */
    template<class T>
    concept IsAsyncCoroutine = NDetail::TIsAsyncCoroutineHelper<T>::value;

    /**
     * Concept matches all callables returning any async<T>
     */
    template<class TCallback, class... TArgs>
    concept IsAsyncCoroutineCallable = requires (TCallback&& callback, TArgs&&... args) {
        { std::invoke(std::forward<TCallback>(callback), std::forward<TArgs>(args)...) } -> IsAsyncCoroutine;
    };

    /**
     * Concept matches all callables returning a specific async<R>
     */
    template<class TCallback, class R, class... TArgs>
    concept IsSpecificAsyncCoroutineCallable = requires (TCallback&& callback, TArgs&&... args) {
        { std::invoke(std::forward<TCallback>(callback), std::forward<TArgs>(args)...) } -> std::same_as<async<R>>;
    };

    /**
     * Concept matches all types which are marked as IsActorAwareAwaiter
     */
    template<class TAwaiter>
    concept IsActorAwareAwaiter = (
        std::bool_constant<(std::decay_t<TAwaiter>::IsActorAwareAwaiter == true)>::value ||
        requires { typename std::decay_t<TAwaiter>::IsActorAwareAwaiter; });

    // Forward declarations, defined below
    class TAsyncCancellable;
    class TAsyncCancellationScope;

    /**
     * Abstract interface for tasks which may be dynamically cancelled
     */
    class TAsyncCancellable : private TIntrusiveListItem<TAsyncCancellable> {
        friend TIntrusiveListItem<TAsyncCancellable>;
        friend TAsyncCancellationScope;

    protected:
        ~TAsyncCancellable() = default;

    public:
        virtual void Cancel() noexcept = 0;

        using TIntrusiveListItem<TAsyncCancellable>::Unlink;
    };

    /**
     * Abstract cancellation scope which may cancel multiple cancellable tasks
     */
    class TAsyncCancellationScope {
    public:
        void AddSink(TAsyncCancellable& sink) noexcept {
            if (!Cancelled) [[likely]] {
                Sinks.PushBack(&sink);
            } else {
                sink.Cancel();
            }
        }

        bool HasSinks() const noexcept {
            return !Sinks.Empty();
        }

        void Cancel() noexcept {
            Cancelled = true;
            CancelSinks();
        }

        bool IsCancelled() const noexcept {
            return Cancelled;
        }

        TAsyncCancellationScope& operator+=(TAsyncCancellationScope&& rhs) noexcept {
            if (this != &rhs) [[likely]] {
                Sinks.Append(rhs.Sinks);
                if (Cancelled) {
                    CancelSinks();
                }
            }
            return *this;
        }

        /**
         * Returns a new TAsyncCancellationScope with currently running and
         * non-cancelled handler attached as a sink when awaited. May only be
         * used directly inside an async actor event handler.
         *
         * Defined in cancellation.h
         */
        static auto WithCurrentHandler();

        /**
         * Runs the wrapped coroutine attached to this cancellation scope when
         * awaited. This allows cancelling this coroutine independently from
         * caller cancellation.
         *
         * Defined in cancellation.h
         */
        template<class T>
        auto Wrap(async<T> wrapped);

        /**
         * Runs the wrapped coroutine attached to this cancellation scope when
         * awaited. This allows cancelling this coroutine independently from
         * caller cancellation.
         *
         * Defined in cancellation.h
         */
        template<class TCallback, class... TArgs>
        auto Wrap(TCallback&& callback, TArgs&&... args)
            requires IsAsyncCoroutineCallable<TCallback, TArgs...>;

        /**
         * Runs the wrapped coroutine attached to this cancellation scope when
         * awaited. This allows cancelling this coroutine independently from
         * caller cancellation. The call is shilded from caller's cancallation.
         *
         * Defined in cancellation.h
         */
        template<class T>
        auto WrapShielded(async<T> wrapped);

        /**
         * Runs the wrapped coroutine attached to this cancellation scope when
         * awaited. This allows cancelling this coroutine independently from
         * caller cancellation. The call is shilded from caller's cancallation.
         *
         * Defined in cancellation.h
         */
        template<class TCallback, class... TArgs>
        auto WrapShielded(TCallback&& callback, TArgs&&... args)
            requires IsAsyncCoroutineCallable<TCallback, TArgs...>;

    private:
        void CancelSinks() noexcept {
            while (!Sinks.Empty()) {
                auto* sink = Sinks.PopFront();
                sink->Cancel();
            }
        }

    private:
        TIntrusiveList<TAsyncCancellable> Sinks;
        bool Cancelled = false;
    };

    /**
     * Base class for awaiters which resume within the same actor context
     */
    class TAsyncAwaiterBase
        : private TActorRunnableItem::TImpl<TAsyncAwaiterBase>
    {
        friend TActorRunnableItem::TImpl<TAsyncAwaiterBase>;

    public:
        static constexpr bool IsActorAwareAwaiter = true;

        bool await_ready() noexcept {
            return false;
        }

        void await_suspend(std::coroutine_handle<> h) noexcept {
            Suspend(h);
        }

        std::coroutine_handle<> await_cancel(std::coroutine_handle<> h) noexcept {
            Cancel();
            return h;
        }

        void await_resume() noexcept {}

    protected:
        bool Suspended() const noexcept {
            return bool(Continuation);
        }

        void Suspend(std::coroutine_handle<> h) noexcept {
            Continuation = h;
        }

        void Resume() {
            Y_ASSERT(Continuation);
            TActorRunnableQueue::Schedule(this);
        }

        void Cancel() {
            Y_ASSERT(Continuation);
            TActorRunnableQueue::Cancel(this);
            Continuation = {};
        }

    private:
        void DoRun(IActor*) noexcept {
            Continuation.resume();
        }

    private:
        std::coroutine_handle<> Continuation;
    };

    namespace NDetail {

        /**
         * Returns a coroutine handle, which would arrange for the specified
         * runnable item to run on the specified actor when resumed. Used to
         * interface with generic C++ coroutines.
         *
         * Exactly one call to either resume or destroy is required.
         */
        std::coroutine_handle<> MakeBridgeCoroutine(IActor& actor, TActorRunnableItem& item);

        /**
         * Returns a pair of coroutine handles, which would arrange for one of
         * runnable items to run on the specified actor when resumed. Used to
         * interface with generic C++ coroutines.
         *
         * Exactly one coroutine must be either resumed or destroyed.
         */
        std::pair<std::coroutine_handle<>, std::coroutine_handle<>> MakeBridgeCoroutines(
            IActor& actor, TActorRunnableItem& item1, TActorRunnableItem& item2);

        //
        // Concepts for validating awaitables and awaiters
        //

        template<class TAwaitable>
        concept HasCoAwaitByValue = requires(TAwaitable&& awaitable) {
            std::forward<TAwaitable>(awaitable).CoAwaitByValue();
        };

        template<class TAwaiter, class TAwaitable>
        concept IsSafeCoAwaitByValueAwaiter = (
            // Awaitable is not returning a reference
            !std::is_reference_v<TAwaiter> ||
            // Awaitable is not returning a reference to itself
            !std::is_base_of_v<std::remove_reference_t<TAwaiter>, TAwaitable> ||
            // Returned awaiter reference is not trivially destructible
            !std::is_trivially_destructible_v<std::remove_reference_t<TAwaiter>>);

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

        template<class TAwaitable>
        constexpr decltype(auto) GetAwaiter(TAwaitable&& awaitable) {
            if constexpr (requires { std::forward<TAwaitable>(awaitable).operator co_await(); }) {
                return std::forward<TAwaitable>(awaitable).operator co_await();
            } else if constexpr (requires { operator co_await(std::forward<TAwaitable>(awaitable)); }) {
                return operator co_await(std::forward<TAwaitable>(awaitable));
            } else {
                return std::forward<TAwaitable>(awaitable);
            }
        }

        template<class TAwaitable>
        using TGetAwaiterResultType = decltype(GetAwaiter(std::declval<TAwaitable&&>()));

        template<class TAwaiter, class TPromise = void>
        concept IsAwaiter = requires(TAwaiter& awaiter, std::coroutine_handle<TPromise> h) {
            awaiter.await_ready();
            awaiter.await_suspend(h);
            awaiter.await_resume();
        };

        template<class TAwaitable, class TPromise = void>
        concept IsAwaitable = (
            HasCoAwait<TAwaitable> ||
            IsAwaiter<TAwaitable, TPromise>);

        template<class TAwaiter>
        concept HasAwaitReadyNoExcept = requires(TAwaiter& awaiter) {
            { awaiter.await_ready() } noexcept;
        };

        template<class TAwaiter, class TPromise = void>
        concept HasAwaitSuspendNoExcept = requires(TAwaiter& awaiter, std::coroutine_handle<TPromise> h) {
            { awaiter.await_suspend(h) } noexcept;
        };

        template<class TAwaiter>
        concept HasAwaitResumeNoExcept = requires(TAwaiter& awaiter) {
            { awaiter.await_resume() } noexcept;
        };

        template<class TAwaiter>
        concept HasAwaitCancelled = requires(TAwaiter& awaiter, std::coroutine_handle<> h) {
            // This is a non-standard extension to standard awaiters
            awaiter.await_cancelled(h);
        };

        template<class TAwaiter>
        concept HasAwaitCancel = requires(TAwaiter& awaiter, std::coroutine_handle<> h) {
            // This is a non-standard extension to standard awaiters
            awaiter.await_cancel(h);
        };

        // Forward declaration, defined below
        class TAwaitCancelCleanup;

        /**
         * Handles propagation of await_cancel to at most one awaiter at a time
         */
        class TAwaitCancelSource {
            friend TAwaitCancelCleanup;

        public:
            constexpr TAwaitCancelSource() noexcept = default;

            TAwaitCancelSource(const TAwaitCancelSource&) = delete;
            TAwaitCancelSource& operator=(const TAwaitCancelSource&) = delete;

            ~TAwaitCancelSource() noexcept {
                Y_DEBUG_ABORT_UNLESS(!CancelFn, "TAwaitCancelSource destroyed with an awaiter still subscribed");
            }

            template<class TAwaiter>
            [[nodiscard]] TAwaitCancelCleanup SetAwaiter(TAwaiter& awaiter) noexcept;

            constexpr std::coroutine_handle<> GetCancellation() const noexcept {
                return Cancellation;
            }

            /**
             * Calls the awaiter.await_cancel(h) method and transforms various
             * result types into an optional continuation.
             */
            template<class TAwaiter>
            [[nodiscard]] static std::coroutine_handle<> Notify(
                    TAwaiter& awaiter, std::coroutine_handle<> h)
                noexcept(noexcept(awaiter.await_cancel(h)))
            {
                if constexpr (requires { { awaiter.await_cancel(h) } -> std::same_as<void>; }) {
                    awaiter.await_cancel(h);
                    return nullptr;
                } else if constexpr (requires { { awaiter.await_cancel(h) } -> std::same_as<bool>; }) {
                    if (awaiter.await_cancel(h)) {
                        return h;
                    } else {
                        return nullptr;
                    }
                } else {
                    static_assert(
                        requires { { awaiter.await_cancel(h) } -> std::convertible_to<std::coroutine_handle<>>; },
                        "await_cancel(h) must return void, bool or std::coroutine_handle<>");
                    return awaiter.await_cancel(h);
                }
            }

        protected:
            [[nodiscard]] std::coroutine_handle<> Cancel(std::coroutine_handle<> h) noexcept {
                Cancellation = h;
                if (CancelFn) {
                    return CancelFn(CancelFnArg, h);
                } else {
                    return nullptr;
                }
            }

            constexpr void SetCancellation(std::coroutine_handle<> h) noexcept {
                Cancellation = h;
            }

        private:
            std::coroutine_handle<> Cancellation;
            std::coroutine_handle<> (*CancelFn)(void*, std::coroutine_handle<>) noexcept = nullptr;
            void* CancelFnArg;
        };

        /**
         * A small RAII cleanup object for subscribed awaiters
         */
        class TAwaitCancelCleanup {
            friend class TAwaitCancelSource;

        public:
            constexpr TAwaitCancelCleanup() noexcept = default;

            constexpr TAwaitCancelCleanup(TAwaitCancelCleanup&& rhs) noexcept
                : Source(rhs.Source)
            {
                rhs.Source = nullptr;
            }

            constexpr TAwaitCancelCleanup& operator=(TAwaitCancelCleanup&& rhs) noexcept {
                if (this != &rhs) [[likely]] {
                    if (Source) [[unlikely]] {
                        Source->CancelFn = nullptr;
                    }
                    Source = rhs.Source;
                    rhs.Source = nullptr;
                }
                return *this;
            }

            constexpr ~TAwaitCancelCleanup() noexcept {
                Cleanup();
            }

            constexpr void operator()() noexcept {
                Cleanup();
            }

            constexpr explicit operator bool() const noexcept {
                return Source != nullptr;
            }

        private:
            constexpr TAwaitCancelCleanup(TAwaitCancelSource* source) noexcept
                : Source(source)
            {}

        private:
            constexpr void Cleanup() noexcept {
                if (Source) {
                    Source->CancelFn = nullptr;
                    Source = nullptr;
                }
            }

        private:
            TAwaitCancelSource* Source = nullptr;
        };

        /**
         * Subscribe for awaiter.await_cancel(h) to be called on Cancel(h)
         */
        template<class TAwaiter>
        inline TAwaitCancelCleanup TAwaitCancelSource::SetAwaiter(TAwaiter& awaiter) noexcept {
            Y_DEBUG_ABORT_UNLESS(!CancelFn, "TAwaitCancelSource cannot support more than one awaiter");
            CancelFn = +[](void* ptr, std::coroutine_handle<> h) noexcept {
                return TAwaitCancelSource::Notify(*reinterpret_cast<TAwaiter*>(ptr), h);
            };
            CancelFnArg = std::addressof(awaiter);
            return TAwaitCancelCleanup(this);
        }

        /**
         * Helpers for calling await_suspend overloads
         */
        struct TAwaitSuspendHelper {
            /**
             * Calls await_suspend(h) converting the result to a valid std::coroutine_handle<>
             */
            template<class TAwaiter, class TPromise = void>
            [[nodiscard]] static constexpr std::coroutine_handle<> Suspend(
                    TAwaiter& awaiter, std::coroutine_handle<TPromise> h)
                noexcept(noexcept(awaiter.await_suspend(h)))
            {
                if constexpr (requires { { awaiter.await_suspend(h) } -> std::same_as<void>; }) {
                    awaiter.await_suspend(h);
                    return std::noop_coroutine();
                } else if constexpr (requires { { awaiter.await_suspend(h) } -> std::same_as<bool>; }) {
                    if (!awaiter.await_suspend(h)) {
                        return h;
                    }
                    return std::noop_coroutine();
                } else {
                    static_assert(
                        requires { { awaiter.await_suspend(h) } -> std::convertible_to<std::coroutine_handle<>>; },
                        "await_suspend(h) must return void, bool or std::coroutine_handle<>");
                    return awaiter.await_suspend(h);
                }
            }

            /**
             * Calls await_cancelled(unwind) and continues to await_suspend(resume)
             * when it returns void, false, or nullptr, indicating it wants to
             * suspend, otherwise returns a valid unwind handle.
             */
            template<class TAwaiter, class TPromise = void>
            [[nodiscard]] static constexpr std::coroutine_handle<> SuspendCancelled(
                    TAwaiter& awaiter, std::coroutine_handle<TPromise> resume, std::coroutine_handle<> unwind)
                noexcept(noexcept(awaiter.await_cancelled(unwind)) && noexcept(awaiter.await_suspend(resume)))
            {
                if constexpr (requires { { awaiter.await_cancelled(unwind) } -> std::same_as<void>; }) {
                    awaiter.await_cancelled(unwind);
                } else if constexpr (requires { { awaiter.await_cancelled(unwind) } -> std::same_as<bool>; }) {
                    if (awaiter.await_cancelled(unwind)) {
                        return unwind;
                    }
                } else {
                    static_assert(
                        requires { { awaiter.await_cancelled(unwind) } -> std::convertible_to<std::coroutine_handle<>>; },
                        "await_cancelled(h) must return void, bool or std::coroutine_handle<>");
                    if (std::coroutine_handle<> next = awaiter.await_cancelled(unwind)) {
                        return next;
                    }
                }
                return TAwaitSuspendHelper::Suspend(awaiter, resume);
            }
        };

        /**
         * Transparently handles optional await_cancel subscriptions for single-threaded awaiters
         */
        template<class TAwaiter>
        class TAwaiterProxy {
        public:
            static constexpr bool IsActorAwareAwaiter = true;

            template<class... TArgs>
            constexpr explicit TAwaiterProxy(TArgs&&... args)
                : Awaiter(std::forward<TArgs>(args)...)
            {}

            TAwaiterProxy(TAwaiterProxy&&) = delete;
            TAwaiterProxy(const TAwaiterProxy&) = delete;
            TAwaiterProxy& operator=(TAwaiterProxy&&) = delete;
            TAwaiterProxy& operator=(const TAwaiterProxy&) = delete;

            bool await_ready() noexcept(HasAwaitReadyNoExcept<TAwaiter>) {
                return Awaiter.await_ready();
            }

            template<class TPromise>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> parent) {
                TAwaitCancelSource& source = parent.promise().GetAwaitCancelSource();

                if (std::coroutine_handle<> cancellation = source.GetCancellation()) {
                    if constexpr (HasAwaitCancelled<TAwaiter>) {
                        return TAwaitSuspendHelper::SuspendCancelled(Awaiter, parent, cancellation);
                    }

                    if constexpr (HasAwaitCancel<TAwaiter>) {
                        // Awaiter has await_cancel, which means it clearly
                        // supports cancellation, but not await_cancelled, so
                        // it won't be able to unwind after suspending. Avoid
                        // starting potentially expensive and uncancellable
                        // async operations.
                        return cancellation;
                    }
                } else {
                    if constexpr (HasAwaitCancel<TAwaiter>) {
                        // We want to propagate cancellation to awaiter
                        Cleanup = source.SetAwaiter(Awaiter);
                    }
                }

                TCallCleanup callCleanup{ this };
                std::coroutine_handle<> result = TAwaitSuspendHelper::Suspend(Awaiter, parent);
                callCleanup.Cancel();
                return result;
            }

            decltype(auto) await_resume() noexcept(HasAwaitResumeNoExcept<TAwaiter>) {
                if constexpr (HasAwaitCancel<TAwaiter>) {
                    Cleanup();
                }
                return Awaiter.await_resume();
            }

        private:
            struct TCallCleanup {
                TAwaiterProxy* Self;

                constexpr ~TCallCleanup() {
                    if (Self) {
                        Self->Cleanup();
                    }
                }

                constexpr void Cancel() noexcept {
                    Self = nullptr;
                }
            };

        private:
            TAwaiter Awaiter;
            TAwaitCancelCleanup Cleanup;
        };

        template<class TDerived, bool WithCancellation = true>
        class TThreadSafeResumeBridge
            : private TActorRunnableItem::TImpl<TThreadSafeResumeBridge<TDerived, WithCancellation>>
        {
            friend TActorRunnableItem::TImpl<TThreadSafeResumeBridge<TDerived, WithCancellation>>;

        public:
            TThreadSafeResumeBridge() = default;

            TThreadSafeResumeBridge(const TThreadSafeResumeBridge&) = delete;
            TThreadSafeResumeBridge& operator=(const TThreadSafeResumeBridge&) = delete;

        protected:
            std::coroutine_handle<> CreateResumeBridge(IActor& actor, TActorRunnableItem& resume) {
                auto pr = MakeBridgeCoroutines(actor, resume, *this);
                UnwindBridge = pr.second;
                return pr.first;
            }

            std::coroutine_handle<> StartCancellation(std::coroutine_handle<> h) noexcept {
                Cancellation = h;
                return UnwindBridge;
            }

        private:
            void DoRun(IActor*) noexcept {
                // Note: bridge no longer valid after this call returns
                Y_ABORT_UNLESS(Cancellation, "Unexpected unwind without StartCancellation");
                static_cast<TDerived&>(*this).OnUnwind(Cancellation);
            }

        private:
            std::coroutine_handle<> UnwindBridge;
            std::coroutine_handle<> Cancellation;
        };

        template<class TDerived>
        class TThreadSafeResumeBridge<TDerived, false> {
        protected:
            std::coroutine_handle<> CreateResumeBridge(IActor& actor, TActorRunnableItem& resume) {
                return MakeBridgeCoroutine(actor, resume);
            }
        };

        /**
         * Transparently handles return path and cancellation for standard C++ awaiters
         */
        template<class TAwaiter>
        class TThreadSafeAwaiterProxy final
            : private TActorRunnableItem::TImpl<TThreadSafeAwaiterProxy<TAwaiter>>
            , private TThreadSafeResumeBridge<TThreadSafeAwaiterProxy<TAwaiter>, HasAwaitCancel<TAwaiter> || HasAwaitCancelled<TAwaiter>>
        {
            friend TAwaitCancelSource;
            friend TActorRunnableItem::TImpl<TThreadSafeAwaiterProxy<TAwaiter>>;
            friend TThreadSafeResumeBridge<TThreadSafeAwaiterProxy<TAwaiter>, HasAwaitCancel<TAwaiter> || HasAwaitCancelled<TAwaiter>>;

        public:
            static constexpr bool IsActorAwareAwaiter = true;

            template<class... TArgs>
            constexpr TThreadSafeAwaiterProxy(TArgs&&... args)
                : Awaiter(std::forward<TArgs>(args)...)
            {}

            TThreadSafeAwaiterProxy(TThreadSafeAwaiterProxy&&) = delete;
            TThreadSafeAwaiterProxy(const TThreadSafeAwaiterProxy&) = delete;
            TThreadSafeAwaiterProxy& operator=(TThreadSafeAwaiterProxy&&) = delete;
            TThreadSafeAwaiterProxy& operator=(const TThreadSafeAwaiterProxy&) = delete;

            bool await_ready() noexcept(HasAwaitReadyNoExcept<TAwaiter>) {
                return Awaiter.await_ready();
            }

            template<class TPromise>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> parent) {
                IActor& actor = parent.promise().GetActor();
                TAwaitCancelSource& source = parent.promise().GetAwaitCancelSource();

                Continuation = parent;

                if (auto cancellation = source.GetCancellation()) {
                    if constexpr (HasAwaitCancelled<TAwaiter>) {
                        Bridge = this->CreateResumeBridge(actor, GetResumeItem());
                        TCallDestroyBridge callDestroyBridge{ this };

                        std::coroutine_handle<> unwind = this->StartCancellation(cancellation);
                        std::coroutine_handle<> result = TAwaitSuspendHelper::SuspendCancelled(Awaiter, Bridge, unwind);

                        if (result == unwind) {
                            // Awaiter unwinds immediately
                            // Unwind directly and avoid bridge scheduling overhead
                            return cancellation;
                        }

                        if (result == Bridge) {
                            // Awaiter resumes immediately
                            // Resume directly and avoid bridge scheduling overhead
                            return parent;
                        }

                        callDestroyBridge.Cancel();
                        return result;
                    }

                    if constexpr (HasAwaitCancel<TAwaiter>) {
                        // Awaiter has await_cancel, which means it clearly
                        // supports cancellation, but not await_cancelled, so
                        // it won't be able to unwind after suspending. Avoid
                        // starting potentially expensive and uncancellable
                        // async operations.
                        return cancellation;
                    }
                } else {
                    if constexpr (HasAwaitCancel<TAwaiter>) {
                        Cleanup = source.SetAwaiter(*this);
                    }
                }

                TCallCleanup callCleanup{ this };

                Bridge = this->CreateResumeBridge(actor, GetResumeItem());
                TCallDestroyBridge callDestroyBridge{ this };

                std::coroutine_handle<> result = TAwaitSuspendHelper::Suspend(Awaiter, Bridge);
                callCleanup.Cancel();

                if (result == Bridge) {
                    // Awaiter resumes immediately
                    // Resume directly and avoid bridge scheduling overhead
                    return parent;
                }

                callDestroyBridge.Cancel();
                return result;
            }

            decltype(auto) await_resume() noexcept(HasAwaitResumeNoExcept<TAwaiter>) {
                if constexpr (HasAwaitCancel<TAwaiter>) {
                    Cleanup();
                }
                return Awaiter.await_resume();
            }

        private:
            decltype(auto) await_cancel(std::coroutine_handle<> c) noexcept
                requires HasAwaitCancel<TAwaiter>
            {
                return Awaiter.await_cancel(this->StartCancellation(c));
            }

        private:
            struct TCallCleanup {
                TThreadSafeAwaiterProxy* Self;

                constexpr ~TCallCleanup() {
                    if (Self) {
                        Self->Cleanup();
                    }
                }

                constexpr void Cancel() noexcept {
                    Self = nullptr;
                }
            };

        private:
            struct TCallDestroyBridge {
                TThreadSafeAwaiterProxy* Self;

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
                Bridge = nullptr;
            }

        private:
            TActorRunnableItem& GetResumeItem() {
                // We have multiple TActorRunnableItem base classes
                TActorRunnableItem::TImpl<TThreadSafeAwaiterProxy<TAwaiter>>& base = *this;
                return base;
            }

            void DoRun(IActor*) noexcept {
                // Note: bridge no longer valid after this call returns
                Y_ABORT_UNLESS(Bridge && Continuation, "Unexpected Run without a bridge or continuation");
                Bridge = nullptr;
                // We may resume recursively
                Continuation.resume();
            }

            void OnUnwind(std::coroutine_handle<> unwind) noexcept {
                // Note: bridge no longer valid after this call returns
                Y_ABORT_UNLESS(Bridge, "Unexpected unwind without a bridge");
                Bridge = nullptr;
                unwind.resume();
            }

        private:
            TAwaiter Awaiter;
            TAwaitCancelCleanup Cleanup;
            std::coroutine_handle<> Bridge;
            std::coroutine_handle<> Continuation;
        };

        /**
         * Marks awaitable which has CoAwaitByValue() and which should only be
         * awaited by value as safe to co_await by reference.
         */
        template<class TAwaitable>
        class TUnsafeAwaitableByReference {
        public:
            constexpr TUnsafeAwaitableByReference(TAwaitable&& awaitable) noexcept
                : Awaitable(std::forward<TAwaitable>(awaitable))
            {}

            constexpr decltype(auto) operator co_await() && {
                return std::forward<TAwaitable>(Awaitable).CoAwaitByValue();
            }

        private:
            TAwaitable&& Awaitable;
        };

        template<class TAwaitable>
        TUnsafeAwaitableByReference(TAwaitable&&) -> TUnsafeAwaitableByReference<TAwaitable>;

        /**
         * Common bases class for promises, provides await_transform support
         */
        class TAsyncAwaitTransform {
        public:
            /**
             * Transforms awaitables which can only be awaited by value
             *
             * Such awaitables usually cannot be moved and might hold references
             * to temporaries created as part of a co_await expression. Returning
             * a reference to itself is dangerous, but possible: it must have
             * a non-trivial destructor, otherwise the object will be destroyed
             * before co_await starts.
             */
            template<class TAwaitable>
            static constexpr decltype(auto) await_transform(TAwaitable awaitable)
                requires (HasCoAwaitByValue<TAwaitable>)
            {
                // Note: may be a reference type, even the same reference
                using TAwaiter = decltype(std::forward<TAwaitable>(awaitable).CoAwaitByValue());

                static_assert(IsSafeCoAwaitByValueAwaiter<TAwaiter, TAwaitable>, "CoAwaitByValue returns an unsafe reference");

                static_assert(IsActorAwareAwaiter<TAwaiter>, "CoAwaitByValue must currently return an actor aware awaiter");
                if constexpr (IsActorAwareAwaiter<TAwaiter>) {
                    if constexpr (HasAwaitCancel<TAwaiter> || HasAwaitCancelled<TAwaiter>) {
                        // Use implicit conversion to support awaiters that cannot be moved
                        struct TImplicitConverter {
                            TAwaitable&& Awaitable;
                            constexpr operator TAwaiter() const {
                                return std::forward<TAwaitable>(Awaitable).CoAwaitByValue();
                            }
                        };
                        // We need a proxy which transparently handles special methods
                        return TAwaiterProxy<TAwaiter>{ TImplicitConverter{ std::forward<TAwaitable>(awaitable) } };
                    } else {
                        return std::forward<TAwaitable>(awaitable).CoAwaitByValue();
                    }
                } else {
                    // We use an implicit conversion to support awaiters that cannot be moved
                    struct TImplicitConverter {
                        TAwaitable&& Awaitable;
                        constexpr operator TAwaiter() const {
                            return std::forward<TAwaitable>(Awaitable).CoAwaitByValue();
                        }
                    };
                    // The wrapped awaiter may or may not be a reference type
                    return TThreadSafeAwaiterProxy<TAwaiter>{ TImplicitConverter{ std::forward<TAwaitable>(awaitable) } };
                }
            }

            /**
             * Transforms awaitables which have an operator co_await
             */
            template<class TAwaitable>
            static constexpr decltype(auto) await_transform(TAwaitable&& awaitable)
                requires (
                    HasCoAwait<TAwaitable> &&
                    !HasCoAwaitByValue<TAwaitable>)
            {
                // Note: may be a reference type, even the same reference
                using TAwaiter = decltype(GetAwaiter(std::forward<TAwaitable>(awaitable)));

                if constexpr (IsActorAwareAwaiter<TAwaiter>) {
                    if constexpr (HasAwaitCancel<TAwaiter> || HasAwaitCancelled<TAwaiter>) {
                        // Use implicit conversion to support awaiters that cannot be moved
                        struct TImplicitConverter {
                            TAwaitable&& Awaitable;
                            constexpr operator TAwaiter() const {
                                return GetAwaiter(std::forward<TAwaitable>(Awaitable));
                            }
                        };
                        // We need a proxy which transparently handles special methods
                        return TAwaiterProxy<TAwaiter>{ TImplicitConverter{ std::forward<TAwaitable>(awaitable) } };
                    } else {
                        // We return the awaitable by reference and without any
                        // proxies, since coroutine calls operator co_await and
                        // we must not do that here. Otherwise operator co_await
                        // might be called twice.
                        return std::forward<TAwaitable>(awaitable);
                    }
                } else {
                    // We use an implicit conversion to support awaiters that cannot be moved
                    struct TImplicitConverter {
                        TAwaitable&& Awaitable;
                        constexpr operator TAwaiter() const {
                            return GetAwaiter(std::forward<TAwaitable>(Awaitable));
                        }
                    };
                    // The wrapped awaiter may or may not be a reference type
                    return TThreadSafeAwaiterProxy<TAwaiter>{ TImplicitConverter{ std::forward<TAwaitable>(awaitable) } };
                }
            }

            /**
             * Transform single-threaded awaiters integrating with the actor system
             */
            template<class TAwaiter>
            static constexpr decltype(auto) await_transform(TAwaiter&& awaiter)
                requires (
                    IsActorAwareAwaiter<TAwaiter> &&
                    !HasCoAwait<TAwaiter> &&
                    !HasCoAwaitByValue<TAwaiter>)
            {
                if constexpr (HasAwaitCancel<TAwaiter> || HasAwaitCancelled<TAwaiter>) {
                    // We store awaiter by reference (will outlive co_await when temporary)
                    return TAwaiterProxy<TAwaiter&&>{ std::forward<TAwaiter>(awaiter) };
                } else {
                    // We return awaiter by reference (temporary will outlive co_await)
                    return std::forward<TAwaiter>(awaiter);
                }
            }

            /**
             * Transforms arbitrary awaiters into a form which makes sure to resume on the same mailbox
             */
            template<class TAwaiter>
            static constexpr decltype(auto) await_transform(TAwaiter&& awaiter)
                requires (
                    IsAwaiter<TAwaiter> &&
                    !IsActorAwareAwaiter<TAwaiter> &&
                    !HasCoAwait<TAwaiter> &&
                    !HasCoAwaitByValue<TAwaiter>)
            {
                return TThreadSafeAwaiterProxy<TAwaiter&&>{ std::forward<TAwaiter>(awaiter) };
            }
        };

        // The result of our await_transform for TAwaitable
        template<class TAwaitable>
        using TAsyncAwaitTransformResult = decltype(TAsyncAwaitTransform::await_transform(std::declval<TAwaitable&&>()));

        // Will be true for types which transform into an awaitable
        // Such types need an additional operator co_await call, otherwise the result is an awaiter
        template<class TAwaitable>
        concept IsAsyncAwaitTransformResultAwaitable = HasCoAwait<TAsyncAwaitTransformResult<TAwaitable>>;

        // Will be true for types which transform into a reference type
        // When calling operator co_await such types don't need lifetime extension
        template<class TAwaitable>
        concept IsAsyncAwaitTransformResultReference = std::is_reference_v<TAsyncAwaitTransformResult<TAwaitable>>;

        /**
         * Transforms awaitables into their lifetime-extended awaiters
         */
        template<class TAwaitable, bool NeedCoAwait = IsAsyncAwaitTransformResultAwaitable<TAwaitable>>
        class TAsyncAwaitTransformed {
        public:
            // This is either a reference type (direct awaiter with no proxies),
            // or some value which needs its lifetime extended.
            using TAwaiter = TAsyncAwaitTransformResult<TAwaitable>;

            TAsyncAwaitTransformed(TAwaitable&& awaitable)
                : Awaiter(TAsyncAwaitTransform::await_transform(std::forward<TAwaitable>(awaitable)))
            {}

        public:
            TAwaiter Awaiter;
        };

        /**
         * Transforms awaitables and calls co_await on the result, when the
         * intermediate transformation between await_transform and co_await is
         * a reference type. We don't need to store intermediate reference in
         * that case.
         */
        template<IsAsyncAwaitTransformResultReference TAwaitable>
        class TAsyncAwaitTransformed<TAwaitable, true> {
        public:
            // An intermediate (reference) type
            using TIntermediate = TAsyncAwaitTransformResult<TAwaitable>;
            // The result of operator co_await (may also be a reference type)
            using TAwaiter = TGetAwaiterResultType<TIntermediate>;

            TAsyncAwaitTransformed(TAwaitable&& awaitable)
                : Awaiter(GetAwaiter(TAsyncAwaitTransform::await_transform(std::forward<TAwaitable>(awaitable))))
            {}

        public:
            TAwaiter Awaiter;
        };

        /**
         * Transforms awaitables and calls co_await on the result, when the
         * intermediate transformation between await_transform and co_await is
         * not a reference type. We need to store the intermediate and make
         * sure its lifetime is greater than the resulting awaiter.
         */
        template<class TAwaitable>
        class TAsyncAwaitTransformed<TAwaitable, true> {
        public:
            // An intermediate (non-reference) type
            using TIntermediate = TAsyncAwaitTransformResult<TAwaitable>;
            // The result of operator co_await (may be a reference type)
            using TAwaiter = TGetAwaiterResultType<TIntermediate>;

            TAsyncAwaitTransformed(TAwaitable&& awaitable)
                : Awaitable(TAsyncAwaitTransform::await_transform(std::forward<TAwaitable>(awaitable)))
                , Awaiter(GetAwaiter(static_cast<TIntermediate&&>(Awaitable)))
            {}

        public:
            TIntermediate Awaitable;
            TAwaiter Awaiter;
        };

        class TAsyncPromiseBase {
        public:
            struct TFinalSuspend {
                constexpr bool await_ready() noexcept { return false; }
                constexpr void await_resume() noexcept {}

                template<class TPromise>
                constexpr std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> self) noexcept {
                    TAsyncPromiseBase& promise = self.promise();
                    return promise.Continuation;
                }
            };

            constexpr auto initial_suspend() noexcept { return std::suspend_always{}; }
            constexpr auto final_suspend() noexcept { return TFinalSuspend{}; }

            constexpr IActor& GetActor() noexcept {
                return *Actor;
            }

            constexpr TAwaitCancelSource& GetAwaitCancelSource() noexcept {
                return *Source;
            }

            constexpr std::coroutine_handle<> GetContinuation() const noexcept {
                return Continuation;
            }

            template<class TPromise>
            constexpr void SetContinuation(std::coroutine_handle<TPromise> caller) noexcept {
                IActor& actor = caller.promise().GetActor();
                TAwaitCancelSource& source = caller.promise().GetAwaitCancelSource();
                SetContinuation(actor, source, caller);
            }

            constexpr void SetContinuation(IActor& actor, TAwaitCancelSource& source, std::coroutine_handle<> c) noexcept {
                Actor = &actor;
                Source = &source;
                Continuation = c;
            }

        private:
            std::coroutine_handle<> Continuation;
            IActor* Actor = nullptr;
            TAwaitCancelSource* Source = nullptr;
        };

        template<class T, template<class> class TAsyncResultType = TAsyncResult>
        class TAsyncPromiseResult : public TAsyncResultType<T> {
        public:
            template<class U>
            void return_value(U&& value)
                requires (std::is_convertible_v<U&&, T>)
            {
                this->SetValue(std::forward<U>(value));
            }

            void unhandled_exception() noexcept {
                this->SetException(std::current_exception());
            }
        };

        template<template<class> class TAsyncResultType>
        class TAsyncPromiseResult<void, TAsyncResultType> : public TAsyncResultType<void> {
        public:
            void return_void() {
                this->SetValue();
            }

            void unhandled_exception() noexcept {
                this->SetException(std::current_exception());
            }
        };

        template<class T>
        class TAsyncPromise
            : public TAsyncPromiseBase
            , public TAsyncPromiseResult<T>
            , public TAsyncAwaitTransform
        {
        public:
            constexpr async<T> get_return_object() noexcept {
                return async<T>(std::coroutine_handle<TAsyncPromise<T>>::from_promise(*this));
            }
        };

        template<class T>
        concept IsActorSubClassType = std::is_convertible_v<T&, IActor&>;

        template<class T>
        concept IsNonReferenceType = !std::is_reference_v<T>;

        class TActorAsyncHandlerPromise final
            : private TActorTask
            , private TAsyncCancellable
            , private TAwaitCancelSource
            , private TActorRunnableItem::TImpl<TActorAsyncHandlerPromise>
            , private TCustomCoroutineCallbacks<TActorAsyncHandlerPromise>
            , public TAsyncAwaitTransform
        {
            friend TActorRunnableItem::TImpl<TActorAsyncHandlerPromise>;
            friend TCustomCoroutineCallbacks<TActorAsyncHandlerPromise>;

        public:
            constexpr void get_return_object() noexcept {}

            constexpr auto initial_suspend() noexcept { return std::suspend_never{}; }

            struct TFinalSuspend {
                const bool Ready;

                constexpr bool await_ready() const noexcept { return Ready; }
                constexpr void await_suspend(std::coroutine_handle<>) const noexcept {}
                constexpr void await_resume() const noexcept {}
            };

            auto final_suspend() noexcept {
                return TFinalSuspend{ Finish() };
            }

            void unhandled_exception() noexcept;
            constexpr void return_void() noexcept {}

            constexpr IActor& GetActor() noexcept {
                return Actor;
            }

            constexpr TAwaitCancelSource& GetAwaitCancelSource() noexcept {
                return *this;
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

            struct [[nodiscard]] TCreateAttachedCancellationScopeOp {};

            auto await_transform(TCreateAttachedCancellationScopeOp) noexcept {
                struct TAwaiter {
                    TAsyncCancellationScope Scope;

                    static constexpr bool await_ready() noexcept { return true; }
                    static constexpr void await_suspend(std::coroutine_handle<>) noexcept {}
                    TAsyncCancellationScope await_resume() noexcept {
                        return std::move(Scope);
                    }
                };

                TAwaiter awaiter;
                if (this->GetCancellation()) {
                    awaiter.Scope.Cancel();
                } else {
                    awaiter.Scope.AddSink(*this);
                }
                return awaiter;
            }

            using TAsyncAwaitTransform::await_transform;

        private:
            bool Finish() noexcept {
                switch (State) {
                    case EState::Running: [[likely]]
                        return true;
                    case EState::InCancel: [[unlikely]]
                        // Coroutine resumed recursively(!) in Cancel() and finished
                        // Mark as finished so Cancel() calls Destroy() later
                        State = EState::InCancelFinished;
                        return false;
                    case EState::InCancelFinished: [[unlikely]]
                        // Coroutine already finished, definitely a bug
                        Y_ABORT("BUG: task resumed twice");
                    case EState::InCancelDestroyed: [[unlikely]]
                        // Coroutine resumed cancellation handle from Cancel(), but
                        // now it's trying to also finish normally, definitely a bug
                        Y_ABORT("BUG: task confirmed cancel then resumed");
                    case EState::Scheduled:
                        // We have used this promise to schedule next coroutine,
                        // but now it's finishing normally, so we need to make
                        // sure coroutine is destroyed as well
                        State = EState::ScheduledDestroy;
                        return false;
                    case EState::ScheduledDestroy: [[unlikely]]
                        // Coroutine was already scheduled for destruction and
                        // now it has somehow finished normally, definitely a bug
                        Y_ABORT("BUG: task resumed/cancelled multiple times");
                }
            }

            void Cancel() noexcept override {
                // Protect against Cancel() called multiple times
                if (GetCancellation()) [[unlikely]] {
                    return;
                }

                Y_ABORT_UNLESS(State == EState::Running);

                // Protect against coroutine resuming recursively and destroying itself
                State = EState::InCancel;

                if (auto next = TAwaitCancelSource::Cancel(this->ToCoroutineHandle())) {
                    if (next == this->ToCoroutineHandle()) {
                        // Subscriber wants us to unwind the stack
                        Y_ABORT_UNLESS(State == EState::InCancel, "BUG: task resumed/cancelled multiple times");
                        State = EState::ScheduledDestroy;
                    } else {
                        // Subscriber wants us to resume something else
                        // Remember whether we also had a pending destroy
                        if (State == EState::InCancel) {
                            State = EState::Scheduled;
                        } else {
                            State = EState::ScheduledDestroy;
                        }
                        Pending = next;
                    }
                    TActorRunnableQueue::Schedule(this);
                } else {
                    switch (State) {
                        case EState::InCancel:
                            // Nothing changed, get back to running
                            State = EState::Running;
                            break;
                        case EState::InCancelFinished:
                            // Task finished recursively for some reason. We can
                            // call destroy directly, since there are no code
                            // left to run anyway.
                            Destroy();
                            break;
                        case EState::InCancelDestroyed:
                            // Task cancelled recursively for some reason. We
                            // need to schedule destruction, so it doesn't
                            // happen recursively.
                            State = EState::ScheduledDestroy;
                            TActorRunnableQueue::Schedule(this);
                            break;
                        default: [[unlikely]]
                            Y_ABORT("BUG: unexpected state");
                    }
                }
            }

            void Destroy() noexcept override {
                std::coroutine_handle<TActorAsyncHandlerPromise>::from_promise(*this).destroy();
            }

            void OnResume() noexcept {
                switch (State) {
                    case EState::Running:
                        Destroy();
                        break;
                    case EState::InCancel:
                        State = EState::InCancelDestroyed;
                        break;
                    case EState::InCancelFinished:
                        Y_ABORT("BUG: task resumed then confirmed cancel");
                    case EState::InCancelDestroyed:
                        Y_ABORT("BUG: task confirmed cancel twice");
                    case EState::Scheduled:
                        State = EState::ScheduledDestroy;
                        break;
                    case EState::ScheduledDestroy:
                        Y_ABORT("BUG: task resumed/cancelled multiple times");
                }
            }

            void OnDestroy() noexcept {
                Y_ABORT("Unexpected destroy on cancellation proxy");
            }

            void DoRun(IActor*) noexcept {
                if (Pending) {
                    // Note: even when current state is EState::Scheduled our
                    // cancellation handle may resume and transition to the
                    // destroy state.
                    Pending.resume();
                    Pending = nullptr;
                }

                switch (State) {
                    case EState::Scheduled:
                        // We performed some scheduled work, but now we keep running
                        State = EState::Running;
                        break;
                    case EState::ScheduledDestroy:
                        // We may finally destroy this coroutine
                        Destroy();
                        break;
                    default:
                        Y_ABORT("BUG: unexpected state");
                }
            }

        private:
            enum class EState {
                Running,
                InCancel,
                InCancelFinished,
                InCancelDestroyed,
                Scheduled,
                ScheduledDestroy,
            };

        private:
            IActor& Actor;
            EState State = EState::Running;
            std::coroutine_handle<> Pending;
        };

    } // namespace NDetail

} // namespace NActors

template<NActors::NDetail::IsActorSubClassType T, NActors::NDetail::IsNonReferenceType... Args>
struct std::coroutine_traits<void, T&, Args...> {
    using promise_type = NActors::NDetail::TActorAsyncHandlerPromise;
};
