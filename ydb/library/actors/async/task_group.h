#pragma once
#include "async.h"
#include "symmetric_proxy.h"

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
        template<IsSpecificAsyncCoroutineCallback<T> TCallback>
        size_t Add(TCallback&& callback) {
            return Sink.Add(std::forward<TCallback>(callback));
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
            virtual void Cancel() noexcept = 0;
        };

        template<class T, class TCallback>
        class TTaskGroupTaskImpl
            : public TTaskGroupTask<T>
            , private TAwaitCancelSource
            , private TActorRunnableItem::TImpl<TTaskGroupTaskImpl<T, TCallback>>
            , private TSymmetricTransferCallback<TTaskGroupTaskImpl<T, TCallback>>
        {
            friend TActorRunnableItem::TImpl<TTaskGroupTaskImpl<T, TCallback>>;
            friend TSymmetricTransferCallback<TTaskGroupTaskImpl<T, TCallback>>;

        public:
            TTaskGroupTaskImpl(TTaskGroupSink<T>& sink, size_t index, TCallback&& callback)
                : Sink(sink)
                , Index(index)
                , Callback(std::forward<TCallback>(callback))
            {}

            virtual T ExtractValue() override {
                if (Coroutine) [[likely]] {
                    TAsyncResult<T>& result = Coroutine->GetHandle().promise();
                    return result.ExtractValue();
                }
                Y_DEBUG_ABORT_UNLESS(CallbackException, "Unexpected ExtractValue call");
                std::rethrow_exception(std::move(CallbackException));
            }

            virtual TTaskGroupResult<T> ExtractResult() override {
                if (Coroutine) [[likely]] {
                    TAsyncResult<T>& result = Coroutine->GetHandle().promise();
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

            void Cancel() noexcept override {
                if (State == EState::Starting) {
                    TAwaitCancelSource::SetCancellation(this->ToCoroutineHandle());
                    return;
                }
                Y_ABORT_UNLESS(State == EState::Running, "Unexpected state when Cancel() is called");
                State = EState::InCancel;
                // Note: we use the same coroutine handle for cancellation!
                // Task is cancelled when the group is also cancelled, and result will be ignored anyway
                if (auto next = TAwaitCancelSource::Cancel(this->ToCoroutineHandle())) {
                    Pending = next;
                    Y_ABORT_UNLESS(State == EState::InCancel || State == EState::InCancelFinish);
                    State = (State == EState::InCancel) ? EState::Scheduled : EState::ScheduledFinish;
                    TActorRunnableQueue::Schedule(this);
                    return;
                }
                Y_ABORT_UNLESS(State == EState::InCancel || State == EState::InCancelFinish);
                if (State == EState::InCancel) {
                    State = EState::Running;
                } else {
                    State = EState::ScheduledFinish;
                    TActorRunnableQueue::Schedule(this);
                }
            }

        private:
            void DoRun(IActor*) noexcept {
                switch (State) {
                    case EState::Starting:
                        OnStarting();
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
                if (GetCancellation()) {
                    // Task was cancelled before it could start
                    // We don't even run the callback in this case
                    Finish().resume();
                    return;
                }
                // Callback might throw an exception and we need to handle it
                try {
                    // Use an implcit converter to construct async<T> without moving it
                    struct TImplicitConverter {
                        TTaskGroupTaskImpl& Self;
                        operator async<T>() const {
                            return std::move(Self.Callback)();
                        }
                    };
                    Coroutine.emplace(TImplicitConverter{ *this });
                } catch (...) {
                    // Capture the callback exception
                    CallbackException = std::current_exception();
                }
                if (!Coroutine) {
                    // Notify sink outside the catch body
                    Finish().resume();
                    return;
                }
                State = EState::Running;
                Coroutine->GetHandle().promise().SetContinuation(this->ToCoroutineHandle(), Sink.GetActor(), *this);
                Coroutine->GetHandle().resume();
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
            [[nodiscard]] std::coroutine_handle<> OnResume() noexcept {
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
                Running,
                InCancel,
                InCancelFinish,
                Scheduled,
                ScheduledFinish,
                Finished,
            };

        private:
            TTaskGroupSink<T>& Sink;
            size_t Index;
            std::decay_t<TCallback> Callback;
            std::optional<async<T>> Coroutine;
            std::exception_ptr CallbackException;
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

            template<IsSpecificAsyncCoroutineCallback<T> TCallback>
            size_t Add(TCallback&& callback) {
                size_t index = NextIndex++;
                Y_ABORT_UNLESS(!Cancellation, "Cannot add more tasks after task group coroutine returns");
                TTaskGroupTask<T>* task = new TTaskGroupTaskImpl<T, TCallback>(
                    *this, index, std::forward<TCallback>(callback));
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
                    task->Cancel();
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

        template<class T, class R>
        class TWithTaskGroupAwaiter
            : public TActorAwareAwaiter
            , private TAwaitCancelSource
            , private TSymmetricTransferCallback<TWithTaskGroupAwaiter<T, R>>
        {
            friend TSymmetricTransferCallback<TWithTaskGroupAwaiter<T, R>>;

            template<class TCallback>
            struct TImplicitConverter {
                TCallback&& Callback;
                TTaskGroup<T>& Group;
                operator async<R>() const {
                    return std::forward<TCallback>(Callback)(Group);
                }
            };

        public:
            template<class TCallback>
            TWithTaskGroupAwaiter(TCallback&& callback)
                : Group(Sink)
                , Coroutine(std::in_place, TImplicitConverter<TCallback>{ std::forward<TCallback>(callback), Group })
                , CancelTrampoline(*this)
            {}

            TWithTaskGroupAwaiter(TWithTaskGroupAwaiter&&) = delete;
            TWithTaskGroupAwaiter(const TWithTaskGroupAwaiter&) = delete;
            TWithTaskGroupAwaiter& operator=(TWithTaskGroupAwaiter&&) = delete;
            TWithTaskGroupAwaiter& operator=(const TWithTaskGroupAwaiter&) = delete;

            bool await_ready() noexcept {
                return false;
            }

            template<class TPromise>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> parent) noexcept {
                IActor& actor = parent.promise().GetActor();
                Sink.SetActor(actor);
                TAwaitCancelSource& source = parent.promise().GetAwaitCancelSource();
                if (auto cancellation = source.GetCancellation()) {
                    // Make sure we propagate cancellation downstream
                    AwaitCancel(cancellation);
                } else {
                    // When upstream cancels we will cancel downstream
                    Cleanup = source.SetAwaiter(*this);
                }
                Continuation = parent;
                Coroutine->GetHandle().promise().SetContinuation(this->ToCoroutineHandle(), actor, *this);
                return Coroutine->GetHandle();
            }

            R await_resume() {
                Cleanup();
                return Coroutine->GetHandle().promise().ExtractValue();
            }

            std::coroutine_handle<> AwaitCancel(std::coroutine_handle<> h) noexcept {
                UpstreamCancellation = h;
                return TAwaitCancelSource::Cancel(CancelTrampoline.ToCoroutineHandle());
            }

        private:
            std::coroutine_handle<> OnResume() noexcept {
                // Downstream coroutine wants to return some result
                Y_ABORT_UNLESS(Coroutine, "Unexpected OnResume without a coroutine");
                // We want to cancel all running tasks and then resume upstream
                return Sink.Cancel(Continuation);
            }

            std::coroutine_handle<> OnCancel() noexcept {
                // Downstream coroutine confirmed cancellation
                Y_ABORT_UNLESS(UpstreamCancellation, "Unexpected OnCancel without upstream cancellation");
                // We destroy coroutine and unwind its stack immediately
                Coroutine.reset();
                // We want to cancel all running tasks and then cancel upstream
                return Sink.Cancel(UpstreamCancellation);
            }

        private:
            class TCancelTrampoline : public TSymmetricTransferCallback<TCancelTrampoline> {
            public:
                TCancelTrampoline(TWithTaskGroupAwaiter& self)
                    : Self(self)
                {}

                std::coroutine_handle<> OnResume() noexcept {
                    return Self.OnCancel();
                }

            private:
                TWithTaskGroupAwaiter& Self;
            };

        private:
            TTaskGroupSink<T> Sink;
            TTaskGroup<T> Group;
            std::optional<async<R>> Coroutine;
            TAwaitCancelCleanup Cleanup;
            std::coroutine_handle<> Continuation;
            std::coroutine_handle<> UpstreamCancellation;
            TCancelTrampoline CancelTrampoline;
        };

    } // namespace NDetail

    template<class T>
    class TTaskGroup<T>::TWhenReadyAwaiter {
    public:
        TWhenReadyAwaiter(NDetail::TTaskGroupSink<T>& sink)
            : Sink(sink)
        {}

        bool AwaitReady() const {
            return Sink.HasReady();
        }

        void AwaitSuspend(std::coroutine_handle<> h) {
            Sink.SetAwaiter(h);
        }

        std::coroutine_handle<> AwaitCancel(std::coroutine_handle<> h) noexcept {
            Sink.RemoveAwaiter();
            return h;
        }

        void AwaitResume() noexcept {}

    private:
        NDetail::TTaskGroupSink<T>& Sink;
    };

    template<class T>
    class TTaskGroup<T>::TNextValueAwaiter {
    public:
        TNextValueAwaiter(NDetail::TTaskGroupSink<T>& sink)
            : Sink(sink)
        {}

        bool AwaitReady() const {
            return Sink.HasReady();
        }

        void AwaitSuspend(std::coroutine_handle<> h) {
            Sink.SetAwaiter(h);
        }

        std::coroutine_handle<> AwaitCancel(std::coroutine_handle<> h) noexcept {
            Sink.RemoveAwaiter();
            return h;
        }

        T AwaitResume() {
            return Sink.ExtractReadyValue();
        }

    private:
        NDetail::TTaskGroupSink<T>& Sink;
    };

    template<class T>
    class TTaskGroup<T>::TNextResultAwaiter {
    public:
        TNextResultAwaiter(NDetail::TTaskGroupSink<T>& sink)
            : Sink(sink)
        {}

        bool AwaitReady() const {
            return Sink.HasReady();
        }

        void AwaitSuspend(std::coroutine_handle<> h) {
            Sink.SetAwaiter(h);
        }

        std::coroutine_handle<> AwaitCancel(std::coroutine_handle<> h) noexcept {
            Sink.RemoveAwaiter();
            return h;
        }

        TTaskGroupResult<T> AwaitResume() {
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

    template<class T, IsAsyncCoroutineCallback<TTaskGroup<T>&> TCallback>
    auto WithTaskGroup(TCallback&& callback) {
        using TCallbackResult = decltype(std::forward<TCallback>(callback)(std::declval<TTaskGroup<T>&>));
        using R = TAsyncCoroutineResult<TCallbackResult>;
        return NDetail::TWithTaskGroupAwaiter<T, R>(std::forward<TCallback>(callback));
    }

    template<IsAsyncCoroutineCallback<TTaskGroup<void>&> TCallback>
    auto WithTaskGroup(TCallback&& callback) {
        using TCallbackResult = decltype(std::forward<TCallback>(callback)(std::declval<TTaskGroup<void>&>));
        using R = TAsyncCoroutineResult<TCallbackResult>;
        return NDetail::TWithTaskGroupAwaiter<void, R>(std::forward<TCallback>(callback));
    }

} // namespace NActors
