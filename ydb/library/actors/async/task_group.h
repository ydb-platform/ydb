#pragma once
#include "async.h"
#include "decorator.h"
#include "callback_coroutine.h"
#include <tuple>

namespace NActors {

    namespace NDetail {
        template<class T>
        class TTaskGroupSink;
    }

    /**
     * Task group results have an additional index
     */
    template<class T>
    class TTaskGroupResult : public TAsyncResult<T> {
    public:
        explicit TTaskGroupResult(size_t index)
            : Index(index)
        {}

        TTaskGroupResult(size_t index, TAsyncResult<T>&& rhs)
            : TAsyncResult<T>(std::move(rhs))
            , Index(index)
        {}

        size_t GetIndex() const noexcept {
            return Index;
        }

    private:
        size_t Index;
    };

    /**
     * Task group with concurrent async<T> tasks
     */
    template<class T = void>
    class TTaskGroup {
    public:
        explicit TTaskGroup(NDetail::TTaskGroupSink<T>& sink) noexcept
            : Sink(sink)
        {}

        TTaskGroup(const TTaskGroup&) = delete;
        TTaskGroup& operator=(const TTaskGroup&) = delete;

        /**
         * Schedules the specified async<T> coroutine to run concurrently
         *
         * The returned task index may be used to match results of the added task
         */
        template<class TCallback, class... TArgs>
        size_t Add(TCallback&& callback, TArgs&&... args)
            requires IsSpecificAsyncCoroutineCallable<TCallback, T, TArgs...>
        {
            return Sink.Add(std::forward<TCallback>(callback), std::forward<TArgs>(args)...);
        }

        bool Ready() const {
            return Sink.HasReady();
        }

        size_t Running() const {
            return Sink.GetRunningCount();
        }

        explicit operator bool() const {
            return Sink.GetRunningCount() > 0;
        }

        class TWhenReadyAwaiter;
        class TNextValueAwaiter;
        class TNextResultAwaiter;

        TWhenReadyAwaiter WhenReady() const;
        TNextValueAwaiter Next() const;
        TNextResultAwaiter NextResult() const;

    private:
        NDetail::TTaskGroupSink<T>& Sink;
    };

    namespace NDetail {

        template<class T>
        class TTaskGroupTask
            : public TIntrusiveListItem<TTaskGroupTask<T>>
        {
        public:
            virtual ~TTaskGroupTask() = default;

            virtual T ExtractValue() = 0;
            virtual TTaskGroupResult<T> ExtractResult() = 0;

            virtual void Start() noexcept = 0;
            virtual bool Cancel() noexcept = 0;
        };

        template<class T, class TCallback, class... TArgs>
        class TTaskGroupTaskImpl
            : public TTaskGroupTask<T>
            , private TActorRunnableItem::TImpl<TTaskGroupTaskImpl<T, TCallback>>
        {
            friend TActorRunnableItem::TImpl<TTaskGroupTaskImpl<T, TCallback>>;

        public:
            TTaskGroupTaskImpl(TTaskGroupSink<T>& sink, size_t index, TCallback&& callback, TArgs&&... args)
                : Sink(sink)
                , Index(index)
                , Callback(std::forward<TCallback>(callback))
                , CallbackArgs(std::forward<TArgs>(args)...)
            {}

            virtual T ExtractValue() override {
                if (Coroutine) [[likely]] {
                    TAsyncResult<T>& result = Coroutine.GetHandle().promise();
                    return result.ExtractValue();
                }
                Y_DEBUG_ABORT_UNLESS(CallbackException, "Unexpected ExtractValue call");
                std::rethrow_exception(std::move(CallbackException));
            }

            virtual TTaskGroupResult<T> ExtractResult() override {
                if (Coroutine) [[likely]] {
                    TAsyncResult<T>& result = Coroutine.GetHandle().promise();
                    return TTaskGroupResult<T>(Index, std::move(result));
                }
                Y_DEBUG_ABORT_UNLESS(CallbackException, "Unexpected ExtractResult call");
                TTaskGroupResult<T> result(Index);
                result.SetException(std::move(CallbackException));
                return result;
            }

            void Start() noexcept override {
                // Schedule ourselves to run in background
                Y_ABORT_UNLESS(State == EState::Starting);
                TActorRunnableQueue::Schedule(this);
            }

            bool Cancel() noexcept override {
                if (State == EState::Starting) {
                    State = EState::StartCancelled;
                    return true;
                }
                Y_ABORT_UNLESS(State == EState::Running, "Unexpected state when Cancel() is called");
                State = EState::InCancel;
                // Note: we use the same coroutine handle for cancellation!
                // Task is cancelled when the group is also cancelled, and result will be ignored anyway
                if (auto next = ResumeProxy->Cancel(ResumeProxy.GetHandle())) {
                    Pending = next;
                    Y_ABORT_UNLESS(State == EState::InCancel || State == EState::InCancelFinish);
                    State = (State == EState::InCancel) ? EState::Scheduled : EState::ScheduledFinish;
                    TActorRunnableQueue::Schedule(this);
                    return false;
                }
                Y_ABORT_UNLESS(State == EState::InCancel || State == EState::InCancelFinish);
                if (State == EState::InCancel) {
                    State = EState::Running;
                } else {
                    State = EState::ScheduledFinish;
                    TActorRunnableQueue::Schedule(this);
                }
                return false;
            }

        private:
            class TResumeCallback : private TAwaitCancelSource {
                friend TTaskGroupTaskImpl;
                friend TCallbackCoroutine<TResumeCallback>;

            public:
                IActor& GetActor() noexcept {
                    return Self->Sink.GetActor();
                }

                TAwaitCancelSource& GetAwaitCancelSource() noexcept {
                    return *this;
                }

            private:
                std::coroutine_handle<> operator()() noexcept {
                    return Self->OnResumeCallback();
                }

            private:
                TTaskGroupTaskImpl* Self;
            };

        private:
            void DoRun(IActor*) noexcept {
                switch (State) {
                    case EState::Starting:
                        OnStarting();
                        break;
                    case EState::StartCancelled:
                        // Task was cancelled before it could start
                        // We don't even run the callback in this case
                        Finish().resume();
                        break;
                    case EState::Scheduled:
                    case EState::ScheduledFinish:
                        OnScheduled();
                        break;
                    default:
                        Y_ABORT("BUG: unexpected state");
                }
            }

            void OnStarting() noexcept {
                // Callback might throw an exception and we need to handle it
                try {
                    // Callback allocation failure will be handled just like any callback exception
                    ResumeProxy = MakeCallbackCoroutine<TResumeCallback>();
                    ResumeProxy->Self = this;
                    // Callback outlives the resulting coroutine
                    Coroutine.UnsafeMoveFrom(std::apply(std::move(Callback), std::move(CallbackArgs)));
                } catch (...) {
                    // Capture the callback exception
                    CallbackException = std::current_exception();
                }
                if (!Coroutine) [[unlikely]] {
                    // Notify sink outside the catch body
                    Finish().resume();
                    return;
                }
                State = EState::Running;
                Coroutine.GetHandle().promise().SetContinuation(ResumeProxy.GetHandle());
                Coroutine.GetHandle().resume();
            }

            void OnScheduled() noexcept {
                if (Pending) {
                    Pending.resume();
                    Pending = {};
                }
                if (State == EState::ScheduledFinish) {
                    Finish().resume();
                }
            }

        private:
            [[nodiscard]] std::coroutine_handle<> OnResumeCallback() noexcept {
                switch (State) {
                    case EState::Running:
                        return Finish();
                    case EState::InCancel:
                        State = EState::InCancelFinish;
                        break;
                    case EState::Scheduled:
                        State = EState::ScheduledFinish;
                        break;
                    default:
                        Y_ABORT("BUG: unexpected state");
                }
                return std::noop_coroutine();
            }

            [[nodiscard]] std::coroutine_handle<> Finish() noexcept {
                State = EState::Finished;
                return Sink.TaskFinished(Index, this);
            }

        private:
            enum class EState {
                Starting,
                StartCancelled,
                Running,
                InCancel,
                InCancelFinish,
                Scheduled,
                ScheduledFinish,
                Finished,
            };

        private:
            TTaskGroupSink<T>& Sink;
            const size_t Index;
            Y_NO_UNIQUE_ADDRESS std::decay_t<TCallback> Callback;
            Y_NO_UNIQUE_ADDRESS std::tuple<std::decay_t<TArgs>...> CallbackArgs;
            std::exception_ptr CallbackException;
            TCallbackCoroutine<TResumeCallback> ResumeProxy;
            async<T> Coroutine = async<T>::UnsafeEmpty();
            std::coroutine_handle<> Pending;
            EState State = EState::Starting;
        };

        template<class T>
        class TTaskGroupSink {
        public:
            TTaskGroupSink() = default;

            TTaskGroupSink(const TTaskGroupSink&) = delete;
            TTaskGroupSink& operator=(const TTaskGroupSink&) = delete;

            ~TTaskGroupSink() {
                while (!Ready.Empty()) {
                    auto* task = Ready.PopFront();
                    delete task;
                }
                while (!Running.Empty()) {
                    auto* task = Running.PopFront();
                    delete task;
                }
                while (!Cancelled.Empty()) {
                    auto* task = Cancelled.PopFront();
                    delete task;
                }
            }

            IActor& GetActor() noexcept {
                return *Actor;
            }

            void SetActor(IActor& actor) noexcept {
                Actor = &actor;
            }

            template<class TCallback, class... TArgs>
            size_t Add(TCallback&& callback, TArgs&&... args)
                requires IsSpecificAsyncCoroutineCallable<TCallback, T, TArgs...>
            {
                size_t index = NextIndex++;
                Y_ABORT_UNLESS(!Cancellation, "Cannot add more tasks after task group coroutine returns");
                TTaskGroupTask<T>* task = new TTaskGroupTaskImpl<T, TCallback, TArgs...>(
                    *this, index, std::forward<TCallback>(callback), std::forward<TArgs>(args)...);
                Running.PushBack(task);
                ++RunningCount;
                task->Start();
                return index;
            }

            [[nodiscard]] std::coroutine_handle<> Cancel(std::coroutine_handle<> h) noexcept {
                Y_ABORT_UNLESS(!Cancellation, "Cannot cancel task group multiple times");
                Y_ABORT_UNLESS(!Awaiter, "Cannot cancel task group while there's an awaiter");
                // Cancel all running tasks
                while (!Running.Empty()) {
                    auto* task = Running.PopFront();
                    Cancelled.PushBack(task);
                    if (task->Cancel()) {
                        --RunningCount;
                        delete task;
                    }
                }
                // Destroy all ready tasks while we wait for cancellations
                while (!Ready.Empty()) {
                    auto* task = Ready.PopFront();
                    delete task;
                }
                // Remember where to resume when all tasks finish
                Cancellation = h;
                if (Cancelled.Empty()) {
                    // All tasks have finished
                    Y_DEBUG_ABORT_UNLESS(RunningCount == 0);
                    return Cancellation;
                } else {
                    return std::noop_coroutine();
                }
            }

            [[nodiscard]] std::coroutine_handle<> TaskFinished(size_t, TTaskGroupTask<T>* task) noexcept {
                --RunningCount;
                if (Cancellation) {
                    delete task;
                    if (Cancelled.Empty()) {
                        Y_DEBUG_ABORT_UNLESS(RunningCount == 0);
                        return Cancellation;
                    }
                    return std::noop_coroutine();
                }
                Ready.PushBack(task);
                if (Awaiter) {
                    return std::exchange(Awaiter, {});
                } else {
                    return std::noop_coroutine();
                }
            }

            bool HasReady() const {
                return !Ready.Empty();
            }

            size_t GetRunningCount() const {
                return RunningCount;
            }

            void SetAwaiter(std::coroutine_handle<> h) {
                if (Awaiter) [[unlikely]] {
                    throw std::logic_error("Cannot await task group concurrently");
                }
                if (RunningCount == 0) [[unlikely]] {
                    throw std::logic_error("Cannot await task group without tasks");
                }
                Awaiter = h;
            }

            void RemoveAwaiter() {
                Y_ABORT_UNLESS(Awaiter, "Removing a missing awaiter");
                Awaiter = {};
            }

            T ExtractReadyValue() {
                Y_ABORT_UNLESS(!Ready.Empty(), "Task group has no finished tasks");
                THolder<TTaskGroupTask<T>> task(Ready.PopFront());
                return task->ExtractValue();
            }

            TTaskGroupResult<T> ExtractReadyResult() {
                Y_ABORT_UNLESS(!Ready.Empty(), "Task group has no finished tasks");
                THolder<TTaskGroupTask<T>> task(Ready.PopFront());
                return task->ExtractResult();
            }

        private:
            TIntrusiveList<TTaskGroupTask<T>> Ready;
            TIntrusiveList<TTaskGroupTask<T>> Running;
            TIntrusiveList<TTaskGroupTask<T>> Cancelled;
            IActor* Actor = nullptr;
            size_t NextIndex = 0;
            size_t RunningCount = 0;
            std::coroutine_handle<> Cancellation;
            std::coroutine_handle<> Awaiter;
        };

        template<class T>
        class TWithTaskGroupAwaiterBase {
        public:
            TWithTaskGroupAwaiterBase()
                : Group(Sink)
            {}

        protected:
            TTaskGroupSink<T> Sink;
            TTaskGroup<T> Group;
        };

        template<class T, class R>
        class TWithTaskGroupAwaiter
            : private TWithTaskGroupAwaiterBase<T> // must be above decorator base
            , public TAsyncDecoratorAwaiter<R, TWithTaskGroupAwaiter<T, R>>
        {
            friend TAsyncDecoratorAwaiter<R, TWithTaskGroupAwaiter<T, R>>;
            using TBase = TWithTaskGroupAwaiterBase<T>;
            using TDecorator = TAsyncDecoratorAwaiter<R, TWithTaskGroupAwaiter<T, R>>;

        public:
            template<class TCallback>
            TWithTaskGroupAwaiter(TCallback&& callback)
                : TDecorator(std::forward<TCallback>(callback), this->Group)
            {}

            TWithTaskGroupAwaiter(TWithTaskGroupAwaiter&&) = delete;
            TWithTaskGroupAwaiter(const TWithTaskGroupAwaiter&) = delete;
            TWithTaskGroupAwaiter& operator=(TWithTaskGroupAwaiter&&) = delete;
            TWithTaskGroupAwaiter& operator=(const TWithTaskGroupAwaiter&) = delete;

            TWithTaskGroupAwaiter& CoAwaitByValue() && {
                return *this;
            }

        private:
            std::coroutine_handle<> Start() noexcept {
                this->Sink.SetActor(this->GetActor());
                return TDecorator::Start();
            }

            std::coroutine_handle<> OnResume(std::coroutine_handle<> h) noexcept {
                // Cancel all tasks and delay resume until they finish
                return this->Sink.Cancel(h);
            }

            std::coroutine_handle<> OnUnwind(std::coroutine_handle<> h) noexcept {
                // Cancel all tasks and delay unwind until they finish
                return this->Sink.Cancel(h);
            }
        };

    } // namespace NDetail

    template<class T>
    class [[nodiscard]] TTaskGroup<T>::TWhenReadyAwaiter {
    public:
        static constexpr bool IsActorAwareAwaiter = true;

        TWhenReadyAwaiter(NDetail::TTaskGroupSink<T>& sink)
            : Sink(sink)
        {}

        bool await_ready() const {
            return Sink.HasReady();
        }

        void await_suspend(std::coroutine_handle<> h) {
            Sink.SetAwaiter(h);
        }

        std::coroutine_handle<> await_cancel(std::coroutine_handle<> h) noexcept {
            Sink.RemoveAwaiter();
            return h;
        }

        void await_resume() noexcept {}

    private:
        NDetail::TTaskGroupSink<T>& Sink;
    };

    template<class T>
    class [[nodiscard]] TTaskGroup<T>::TNextValueAwaiter {
    public:
        static constexpr bool IsActorAwareAwaiter = true;

        TNextValueAwaiter(NDetail::TTaskGroupSink<T>& sink)
            : Sink(sink)
        {}

        bool await_ready() const {
            return Sink.HasReady();
        }

        void await_suspend(std::coroutine_handle<> h) {
            Sink.SetAwaiter(h);
        }

        std::coroutine_handle<> await_cancel(std::coroutine_handle<> h) noexcept {
            Sink.RemoveAwaiter();
            return h;
        }

        T await_resume() {
            return Sink.ExtractReadyValue();
        }

    private:
        NDetail::TTaskGroupSink<T>& Sink;
    };

    template<class T>
    class [[nodiscard]] TTaskGroup<T>::TNextResultAwaiter {
    public:
        static constexpr bool IsActorAwareAwaiter = true;

        TNextResultAwaiter(NDetail::TTaskGroupSink<T>& sink)
            : Sink(sink)
        {}

        bool await_ready() const {
            return Sink.HasReady();
        }

        void await_suspend(std::coroutine_handle<> h) {
            Sink.SetAwaiter(h);
        }

        std::coroutine_handle<> await_cancel(std::coroutine_handle<> h) noexcept {
            Sink.RemoveAwaiter();
            return h;
        }

        TTaskGroupResult<T> await_resume() {
            return Sink.ExtractReadyResult();
        }

    private:
        NDetail::TTaskGroupSink<T>& Sink;
    };

    template<class T>
    inline TTaskGroup<T>::TWhenReadyAwaiter TTaskGroup<T>::WhenReady() const {
        return TWhenReadyAwaiter(Sink);
    }

    template<class T>
    inline TTaskGroup<T>::TNextValueAwaiter TTaskGroup<T>::Next() const {
        return TNextValueAwaiter(Sink);
    }

    template<class T>
    inline TTaskGroup<T>::TNextResultAwaiter TTaskGroup<T>::NextResult() const {
        return TNextResultAwaiter(Sink);
    }

    template<class T, IsAsyncCoroutineCallable<TTaskGroup<T>&> TCallback>
    inline auto WithTaskGroup(TCallback&& callback) {
        using TCallbackResult = decltype(std::forward<TCallback>(callback)(std::declval<TTaskGroup<T>&>));
        using R = typename TCallbackResult::result_type;
        return NDetail::TWithTaskGroupAwaiter<T, R>(std::forward<TCallback>(callback));
    }

    template<IsAsyncCoroutineCallable<TTaskGroup<void>&> TCallback>
    inline auto WithTaskGroup(TCallback&& callback) {
        using TCallbackResult = decltype(std::forward<TCallback>(callback)(std::declval<TTaskGroup<void>&>));
        using R = typename TCallbackResult::result_type;
        return NDetail::TWithTaskGroupAwaiter<void, R>(std::forward<TCallback>(callback));
    }

} // namespace NActors
