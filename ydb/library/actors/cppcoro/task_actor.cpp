#include "task_actor.h"
#include "await_callback.h"
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NActors {

    class TTaskActorImpl;

    static Y_POD_THREAD(TTaskActorImpl*) TlsCurrentTaskActor{nullptr};

    struct TCurrentTaskActorGuard {
        TCurrentTaskActorGuard(TTaskActorImpl* current) noexcept {
            Y_ABORT_UNLESS(TlsCurrentTaskActor == nullptr);
            TlsCurrentTaskActor = current;
        }

        ~TCurrentTaskActorGuard() noexcept {
            TlsCurrentTaskActor = nullptr;
        }
    };

    enum : ui32 {
        EvResumeTask = EventSpaceBegin(TEvents::ES_SYSTEM) + 256,
    };

    struct TEvResumeTask : public TEventLocal<TEvResumeTask, EvResumeTask> {
        std::coroutine_handle<> Handle;
        TTaskResult<void>* Result;

        explicit TEvResumeTask(std::coroutine_handle<> handle, TTaskResult<void>* result) noexcept
            : Handle(handle)
            , Result(result)
        {}

        ~TEvResumeTask() noexcept {
            if (Handle) {
                Result->SetException(std::make_exception_ptr(TTaskCancelled()));
                Handle.resume();
            }
        }
    };

    class TTaskActorResult final : public TAtomicRefCount<TTaskActorResult> {
    public:
        bool Finished = false;
    };

    class TTaskActorImpl : public TActor<TTaskActorImpl> {
        friend class TTaskActor;
        friend class TAfterAwaiter;
        friend class TBindAwaiter;

    public:
        TTaskActorImpl(TTask<void>&& task)
            : TActor(&TThis::StateBoot)
            , Task(std::move(task))
        {
            Y_ABORT_UNLESS(Task);
        }

        ~TTaskActorImpl() {
            Stopped = true;
            while (EventAwaiter) {
                // Unblock event awaiter until task stops trying
                TCurrentTaskActorGuard guard(this);
                std::exchange(EventAwaiter, {}).resume();
            }
        }

        void Registered(TActorSystem* sys, const TActorId& parent) override {
            ParentId = parent;
            sys->Send(new IEventHandle(TEvents::TSystem::Bootstrap, 0, SelfId(), SelfId(), {}, 0));
        }

        STATEFN(StateBoot) {
            Y_ABORT_UNLESS(ev->GetTypeRewrite() == TEvents::TSystem::Bootstrap, "Expected bootstrap event");
            TCurrentTaskActorGuard guard(this);
            Become(&TThis::StateWork);
            AwaitThenCallback(std::move(Task).WhenDone(),
                [result = Result](TTaskResult<void>&& outcome) noexcept {
                    result->Finished = true;
                    try {
                        outcome.Value();
                    } catch (TTaskCancelled&) {
                        // ignore
                    }
                });
            Check();
        }

        STATEFN(StateWork) {
            TCurrentTaskActorGuard guard(this);
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvResumeTask, Handle);
            default:
                Y_ABORT_UNLESS(EventAwaiter);
                Event.reset(ev.Release());
                std::exchange(EventAwaiter, {}).resume();
            }
            Check();
        }

        void Handle(TEvResumeTask::TPtr& ev) {
            auto* msg = ev->Get();
            msg->Result->SetValue();
            std::exchange(msg->Handle, {}).resume();
        }

        bool Check() {
            if (Result->Finished) {
                Y_ABORT_UNLESS(!EventAwaiter, "Task terminated while waiting for the next event");
                PassAway();
                return false;
            }

            Y_ABORT_UNLESS(EventAwaiter, "Task suspended without waiting for the next event");
            return true;
        }

        void WaitForEvent(std::coroutine_handle<> h) noexcept {
            Y_ABORT_UNLESS(!EventAwaiter, "Task cannot have multiple awaiters for the next event");
            EventAwaiter = h;
        }

        std::unique_ptr<IEventHandle> FinishWaitForEvent() {
            if (Stopped) {
                throw TTaskCancelled();
            }
            Y_ABORT_UNLESS(Event, "Task does not have current event");
            return std::move(Event);
        }

    private:
        TIntrusivePtr<TTaskActorResult> Result = MakeIntrusive<TTaskActorResult>();
        TTask<void> Task;
        TActorId ParentId;
        std::coroutine_handle<> EventAwaiter;
        std::unique_ptr<IEventHandle> Event;
        bool Stopped = false;
    };

    void TTaskActorNextEvent::await_suspend(std::coroutine_handle<> h) noexcept {
        Y_ABORT_UNLESS(TlsCurrentTaskActor, "Not in a task actor context");
        TlsCurrentTaskActor->WaitForEvent(h);
    }

    std::unique_ptr<IEventHandle> TTaskActorNextEvent::await_resume() {
        Y_ABORT_UNLESS(TlsCurrentTaskActor, "Not in a task actor context");
        return TlsCurrentTaskActor->FinishWaitForEvent();
    }

    IActor* TTaskActor::Create(TTask<void>&& task) {
        return new TTaskActorImpl(std::move(task));
    }

    TActorIdentity TTaskActor::SelfId() noexcept {
        Y_ABORT_UNLESS(TlsCurrentTaskActor, "Not in a task actor context");
        return TlsCurrentTaskActor->SelfId();
    }

    TActorId TTaskActor::ParentId() noexcept {
        Y_ABORT_UNLESS(TlsCurrentTaskActor, "Not in a task actor context");
        return TlsCurrentTaskActor->ParentId;
    }

    void TAfterAwaiter::await_suspend(std::coroutine_handle<> h) noexcept {
        Y_ABORT_UNLESS(TlsCurrentTaskActor, "Not in a task actor context");
        TlsCurrentTaskActor->Schedule(Duration, new TEvResumeTask(h, &Result));
    }

    bool TBindAwaiter::await_ready() noexcept {
        if (TlsCurrentTaskActor && TlsCurrentTaskActor->SelfId() == ActorId) {
            return true;
        }
        return false;
    }

    void TBindAwaiter::await_suspend(std::coroutine_handle<> h) noexcept {
        Sys->Send(new IEventHandle(ActorId, ActorId, new TEvResumeTask(h, &Result)));
    }

} // namespace NActors
