#include "task_actor.h"
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NActors {

    class TTaskActorImpl;

    static Y_POD_THREAD(TTaskActorImpl*) TlsCurrentTaskActor{nullptr};

    struct TCurrentTaskActorGuard {
        TCurrentTaskActorGuard(TTaskActorImpl* current) noexcept {
            Y_VERIFY(TlsCurrentTaskActor == nullptr);
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

        explicit TEvResumeTask(std::coroutine_handle<> handle) noexcept
            : Handle(handle)
        {}

        ~TEvResumeTask() noexcept {
            // TODO: actor may be dead already
        }
    };

    class TTaskActorImpl : public TActor<TTaskActorImpl> {
        friend class TTaskActor;
        friend struct TAfterAwaiter;
        friend struct TBindAwaiter;

    public:
        TTaskActorImpl(TTask<void>&& task)
            : TActor(&TThis::StateBoot)
            , Task(std::move(task))
        {
            Y_VERIFY(Task);
        }

        void Registered(TActorSystem* sys, const TActorId& parent) override {
            ParentId = parent;
            sys->Send(new IEventHandle(TEvents::TSystem::Bootstrap, 0, SelfId(), SelfId(), {}, 0));
        }

        STATEFN(StateBoot) {
            Y_VERIFY(ev->GetTypeRewrite() == TEvents::TSystem::Bootstrap, "Expected bootstrap event");
            TCurrentTaskActorGuard guard(this);
            Become(&TThis::StateWork);
            Task.Start();
            Check();
        }

        STATEFN(StateWork) {
            TCurrentTaskActorGuard guard(this);
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvResumeTask, Handle);
            default:
                Y_VERIFY(EventWaiter);
                Event.reset(ev.Release());
                std::exchange(EventWaiter, {}).resume();
            }
            Check();
        }

        void Handle(TEvResumeTask::TPtr& ev) {
            auto* msg = ev->Get();
            std::exchange(msg->Handle, {}).resume();
        }

        bool Check() {
            if (Task.Done()) {
                Y_VERIFY(!EventWaiter, "Task terminated while waiting for the next event");
                Task.ExtractResult();
                PassAway();
                return false;
            }

            Y_VERIFY(EventWaiter, "Task suspended without waiting for the next event");
            Event.reset();
            return true;
        }

        void WaitForEvent(std::coroutine_handle<> h) noexcept {
            Y_VERIFY(!EventWaiter, "Task cannot have multiple waiters for the next event");
            EventWaiter = h;
        }

        std::unique_ptr<IEventHandle> FinishWaitForEvent() noexcept {
            Y_VERIFY(Event, "Task does not have current event");
            return std::move(Event);
        }

    private:
        TTask<void> Task;
        TActorId ParentId;
        std::coroutine_handle<> EventWaiter;
        std::unique_ptr<IEventHandle> Event;
    };

    void TTaskActorNextEvent::await_suspend(std::coroutine_handle<> h) noexcept {
        Y_VERIFY(TlsCurrentTaskActor, "Not in a task actor context");
        TlsCurrentTaskActor->WaitForEvent(h);
    }

    std::unique_ptr<IEventHandle> TTaskActorNextEvent::await_resume() noexcept {
        Y_VERIFY(TlsCurrentTaskActor, "Not in a task actor context");
        return TlsCurrentTaskActor->FinishWaitForEvent();
    }

    IActor* TTaskActor::Create(TTask<void>&& task) {
        return new TTaskActorImpl(std::move(task));
    }

    TActorIdentity TTaskActor::SelfId() noexcept {
        Y_VERIFY(TlsCurrentTaskActor, "Not in a task actor context");
        return TlsCurrentTaskActor->SelfId();
    }

    TActorId TTaskActor::ParentId() noexcept {
        Y_VERIFY(TlsCurrentTaskActor, "Not in a task actor context");
        return TlsCurrentTaskActor->ParentId;
    }

    void TAfterAwaiter::await_suspend(std::coroutine_handle<> h) noexcept {
        Y_VERIFY(TlsCurrentTaskActor, "Not in a task actor context");
        TlsCurrentTaskActor->Schedule(Duration, new TEvResumeTask(h));
    }

    void TAfterAwaiter::await_resume() {
    }

    bool TBindAwaiter::await_ready() noexcept {
        if (TlsCurrentTaskActor && TlsCurrentTaskActor->SelfId() == ActorId) {
            return true;
        }
        return false;
    }

    void TBindAwaiter::await_suspend(std::coroutine_handle<> h) noexcept {
        Sys->Send(new IEventHandle(ActorId, ActorId, new TEvResumeTask(h)));
    }

    void TBindAwaiter::await_resume() {
    }

} // namespace NActors
