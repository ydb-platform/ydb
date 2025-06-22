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
            Destroy();
        }

        constexpr explicit operator bool() const noexcept {
            return bool(Handle);
        }

        constexpr std::coroutine_handle<NDetail::TAsyncPromise<T>> GetHandle() const noexcept {
            return Handle;
        }

        void Destroy() noexcept {
            if (Handle) {
                Handle.destroy();
                Handle = nullptr;
            }
        }

    private:
        constexpr explicit async(std::coroutine_handle<NDetail::TAsyncPromise<T>> handle)
            : Handle(handle)
        {}

        async(const async&) = delete;
        async& operator=(const async&) = delete;

    private:
        std::coroutine_handle<NDetail::TAsyncPromise<T>> Handle;
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
     * Concept matches all callbacks returning any async<T>
     */
    template<class TCallback, class... TArgs>
    concept IsAsyncCoroutineCallback = requires (TCallback&& callback, TArgs&&... args) {
        { std::forward<TCallback>(callback)(std::forward<TArgs>(args)...) } -> IsAsyncCoroutine;
    };

    /**
     * Concept matches all callbacks returning a specific async<T>
     */
    template<class TCallback, class T, class... TArgs>
    concept IsSpecificAsyncCoroutineCallback = requires (TCallback&& callback, TArgs&&... args) {
        { std::forward<TCallback>(callback)(std::forward<TArgs>(args)...) } -> std::same_as<async<T>>;
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
         *
         * Exactly one call to either resume or destroy is allowed.
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
        concept HasAwaitCancelBool = requires(TAwaiter& awaiter, std::coroutine_handle<> h) {
            { awaiter.AwaitCancel(h) } -> std::same_as<bool>;
        };

        template<class TAwaiter>
        concept HasAwaitCancelHandle = requires(TAwaiter& awaiter, std::coroutine_handle<> h) {
            { awaiter.AwaitCancel(h) } -> std::convertible_to<std::coroutine_handle<>>;
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
        concept HasStdAwaitCancelBool = requires(TAwaiter& awaiter, std::coroutine_handle<> h) {
            { awaiter.await_cancel(h) } -> std::same_as<bool>;
        };

        template<class TAwaiter>
        concept HasStdAwaitCancelHandle = requires(TAwaiter& awaiter, std::coroutine_handle<> h) {
            { awaiter.await_cancel(h) } -> std::convertible_to<std::coroutine_handle<>>;
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

        // Forward declaration, defined below
        class TAwaitCancelCleanup;

        /**
         * Handles propagation of AwaitCancel to at most one awaiter at a time
         */
        class TAwaitCancelSource {
            friend TAwaitCancelCleanup;

        public:
            TAwaitCancelSource() noexcept = default;

            TAwaitCancelSource(const TAwaitCancelSource&) = delete;
            TAwaitCancelSource& operator=(const TAwaitCancelSource&) = delete;

            ~TAwaitCancelSource() noexcept {
                Y_DEBUG_ABORT_UNLESS(!CancelFn, "TAwaitCancelSource destroyed with an awaiter still subscribed");
            }

            template<class TAwaiter>
            [[nodiscard]] TAwaitCancelCleanup SetAwaiter(TAwaiter& awaiter) noexcept;

            std::coroutine_handle<> GetCancellation() const noexcept {
                return Cancellation;
            }

            /**
             * Calls the awaiter.AwaitCancel(h) method and transforms various
             * result types into an optional continuation.
             */
            template<class TAwaiter>
            [[nodiscard]] static std::coroutine_handle<> Notify(TAwaiter& awaiter, std::coroutine_handle<> h) noexcept {
                if constexpr (HasAwaitCancelVoid<TAwaiter>) {
                    awaiter.AwaitCancel(h);
                    return nullptr;
                } else if constexpr (HasAwaitCancelBool<TAwaiter>) {
                    if (awaiter.AwaitCancel(h)) {
                        return h;
                    } else {
                        return nullptr;
                    }
                } else {
                    static_assert(HasAwaitCancelHandle<TAwaiter>, "AwaitCancel must return void/bool/std::coroutine_handle<>");
                    return awaiter.AwaitCancel(h);
                }
            }

        protected:
            [[nodiscard]] std::coroutine_handle<> Cancel(std::coroutine_handle<> h) noexcept {
                Cancellation = h;
                if (CancelFn) {
                    return CancelFn(CancelFnArg, h);
                } else {
                    return {};
                }
            }

            void SetCancellation(std::coroutine_handle<> h) noexcept {
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
         * Subscribe for awaiter.AwaitCancel(h) to be called on Cancel(h)
         */
        template<class TAwaiter>
        inline TAwaitCancelCleanup TAwaitCancelSource::SetAwaiter(TAwaiter& awaiter) noexcept {
            static_assert(HasAwaitCancelNoExcept<TAwaiter>, "AwaitCancel must be noexcept");
            Y_DEBUG_ABORT_UNLESS(!CancelFn, "TAwaitCancelSource cannot support more than one awaiter");
            CancelFn = +[](void* ptr, std::coroutine_handle<> h) noexcept {
                return TAwaitCancelSource::Notify(*reinterpret_cast<TAwaiter*>(ptr), h);
            };
            CancelFnArg = std::addressof(awaiter);
            return TAwaitCancelCleanup(this);
        }

        /**
         * Awaiter implementation for async<T> values.
         */
        template<class T>
        class TAsyncAwaiter : public TActorAwareAwaiter {
        public:
            constexpr explicit TAsyncAwaiter(async<T>&& owner)
                : Handle(owner.GetHandle())
            {}

            constexpr bool await_ready() noexcept {
                return false;
            }

            template<class TPromise>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> parent) noexcept {
                IActor& actor = parent.promise().GetActor();
                TAwaitCancelSource& source = parent.promise().GetAwaitCancelSource();
                Handle.promise().SetContinuation(parent, actor, source);
                return Handle;
            }

            T await_resume() {
                return Handle.promise().ExtractValue();
            }

        private:
            std::coroutine_handle<TAsyncPromise<T>> Handle;
        };

        /**
         * Transparently handles optional AwaitCancel subscriptions for single-threaded awaiters
         */
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

            template<class TPromise>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> parent) {
                if constexpr (HasAwaitCancel<TAwaiter>) {
                    TAwaitCancelSource& source = parent.promise().GetAwaitCancelSource();
                    if (auto cancellation = source.GetCancellation()) {
                        return cancellation;
                    }

                    Cleanup = source.SetAwaiter(Awaiter);
                }

                // We need cleanup on exceptions only when we subscribe and suspend may throw
                constexpr bool needCleanup = HasAwaitCancel<TAwaiter> && !HasAwaitSuspendNoExcept<TAwaiter, TPromise>;

                TCallCleanup<needCleanup> callCleanup{ this };
                std::coroutine_handle<> result = DoSuspend(parent);
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
            template<class TPromise>
            std::coroutine_handle<> DoSuspend(std::coroutine_handle<TPromise> parent)
                noexcept(HasAwaitSuspendNoExcept<TAwaiter, TPromise>)
                requires HasAwaitSuspendVoid<TAwaiter, TPromise>
            {
                Awaiter.AwaitSuspend(parent);
                return std::noop_coroutine();
            }

            template<class TPromise>
            std::coroutine_handle<> DoSuspend(std::coroutine_handle<TPromise> parent)
                noexcept(HasAwaitSuspendNoExcept<TAwaiter, TPromise>)
                requires HasAwaitSuspendBool<TAwaiter, TPromise>
            {
                bool suspended = Awaiter.AwaitSuspend(parent);
                if (suspended) {
                    return std::noop_coroutine();
                } else {
                    return parent;
                }
            }

            template<class TPromise>
            std::coroutine_handle<> DoSuspend(std::coroutine_handle<TPromise> parent)
                noexcept(HasAwaitSuspendNoExcept<TAwaiter, TPromise>)
                requires HasAwaitSuspendHandle<TAwaiter, TPromise>
            {
                return Awaiter.AwaitSuspend(parent);
            }

        private:
            template<bool Enabled = true>
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

            template<>
            struct TCallCleanup<false> {
                constexpr TCallCleanup(TAwaiterProxy*) noexcept {}
                constexpr void Cancel() noexcept {}
            };

        private:
            TAwaiter Awaiter;
            TAwaitCancelCleanup Cleanup;
        };

        template<class TDerived, bool WithCancellation = true>
        class TThreadSafeResumeBridge {
        public:
            TThreadSafeResumeBridge() = default;

            TThreadSafeResumeBridge(const TThreadSafeResumeBridge&) = delete;
            TThreadSafeResumeBridge& operator=(const TThreadSafeResumeBridge&) = delete;

        protected:
            std::coroutine_handle<> CreateResumeBridge(IActor& actor, TActorRunnableItem& resume) {
                auto pr = MakeBridgeCoroutines(actor, resume, CancelledTrampoline);
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
                // Note: bridge no longer valid after this call returns
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
            std::coroutine_handle<> CreateResumeBridge(IActor& actor, TActorRunnableItem& resume) {
                return MakeBridgeCoroutine(actor, resume);
            }
        };

        /**
         * Transparently handles return path and cancellation for standard C++ awaiters
         */
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

            template<class TPromise>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> parent) {
                IActor& actor = parent.promise().GetActor();

                if constexpr (HasStdAwaitCancel<TAwaiter>) {
                    TAwaitCancelSource& source = parent.promise().GetAwaitCancelSource();
                    if (auto cancellation = source.GetCancellation()) {
                        return cancellation;
                    }

                    Cleanup = source.SetAwaiter(*this);
                }

                TCallCleanup<HasStdAwaitCancel<TAwaiter>> callCleanup{ this };

                Continuation = parent;
                Bridge = this->CreateResumeBridge(actor, *this);

                TCallDestroyBridge callDestroyBridge{ this };
                std::coroutine_handle<> result = DoSuspend(Bridge);
                callDestroyBridge.Cancel();

                callCleanup.Cancel();

                // Handle awaiter resuming immediately
                // Redirect back to parent and avoid bridge overhead
                if (result == Bridge) {
                    DestroyBridge();
                    return parent;
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

            decltype(auto) AwaitCancel(std::coroutine_handle<> c) noexcept
                requires HasStdAwaitCancel<TAwaiter>
            {
                static_assert(HasStdAwaitCancelNoExcept<TAwaiter>, "await_cancel must be noexcept");
                return Awaiter.await_cancel(this->StartCancellation(c));
            }

        private:
            void OnCancelled(std::coroutine_handle<> c) noexcept
                requires HasStdAwaitCancel<TAwaiter>
            {
                // Note: bridge no longer valid after this call returns
                Y_ABORT_UNLESS(Bridge, "Unexpected cancellation without a bridge");
                Bridge = {};
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
                // Note: bridge no longer valid after this call returns
                Y_ABORT_UNLESS(Bridge && Continuation, "Unexpected Run without a bridge or continuation");
                Bridge = {};
                // We may resume recursively
                Continuation.resume();
            }

        private:
            TAwaiter Awaiter;
            TAwaitCancelCleanup Cleanup;
            std::coroutine_handle<> Bridge;
            std::coroutine_handle<> Continuation;
        };

        /**
         * Common bases class for promises, provides await_transform support
         */
        template<class TPromise>
        class TAsyncAwaitTransform {
        public:
            /**
             * Accepts async<T> by value to only allow co_await of the call itself, not a stored variable.
             *
             * Taking address of async<T> should be safe here, since it has a destructor, and will be allocated as a
             * temporary by the caller. Temporaries are not destroyed until the full expression (co_await) completes.
             */
            template<class T>
            decltype(auto) await_transform(async<T> c) {
                return TAsyncAwaiter<T>{std::move(c)};
            }

            template<class TAwaiter>
            decltype(auto) await_transform(TAwaiter&& awaiter) noexcept
                requires (IsAwaiter<TAwaiter, TPromise> && !HasCoAwait<TAwaiter>)
            {
                return TAwaiterProxy<TAwaiter&&>{ std::forward<TAwaiter>(awaiter) };
            }

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
                        operator TAwaiter() const {
                            return GetAwaiter(std::forward<TAwaitable>(Awaitable));
                        }
                    };
                    return TStdAwaiterProxy<TAwaiter>{ TImplicitConverter{ std::forward<TAwaitable>(awaitable) } };
                }
            }
        };

        template<class T, template<class> class TAsyncResultType = TAsyncResult>
        class TAsyncPromiseResultHandler : public TAsyncResultType<T> {
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
        class TAsyncPromiseResultHandler<void, TAsyncResultType> : public TAsyncResultType<void> {
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
            : public TAsyncPromiseResultHandler<T>
            , public TAsyncAwaitTransform<TAsyncPromise<T>>
        {
        public:
            async<T> get_return_object() noexcept {
                return async<T>(std::coroutine_handle<TAsyncPromise<T>>::from_promise(*this));
            }

            struct TFinalSuspend {
                constexpr bool await_ready() noexcept { return false; }
                constexpr void await_resume() noexcept {}

                std::coroutine_handle<> await_suspend(std::coroutine_handle<TAsyncPromise<T>> self) noexcept {
                    return self.promise().Continuation;
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

            constexpr void SetContinuation(std::coroutine_handle<> c, IActor& actor, TAwaitCancelSource& source) noexcept {
                Actor = &actor;
                Source = &source;
                Continuation = c;
            }

        private:
            IActor* Actor = nullptr;
            TAwaitCancelSource* Source = nullptr;
            std::coroutine_handle<> Continuation;
        };

        template<class T>
        concept IsActorSubClassType = std::is_convertible_v<T&, IActor&>;

        template<class T>
        concept IsNonReferenceType = !std::is_reference_v<T>;

        class TActorAsyncHandlerPromise final
            : private TActorTask
            , private TAwaitCancelSource
            , private TActorRunnableItem::TImpl<TActorAsyncHandlerPromise>
            , private TCustomCoroutineCallbacks<TActorAsyncHandlerPromise>
            , public TAsyncAwaitTransform<TActorAsyncHandlerPromise>
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
                    Pending = {};
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
