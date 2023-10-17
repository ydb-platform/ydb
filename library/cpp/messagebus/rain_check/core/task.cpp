#include "rain_check.h"

#include <library/cpp/messagebus/actor/temp_tls_vector.h>

#include <util/system/type_name.h>
#include <util/system/tls.h>

using namespace NRainCheck;
using namespace NRainCheck::NPrivate;

using namespace NActor;

namespace {
    Y_POD_STATIC_THREAD(TTaskRunnerBase*)
    ThreadCurrentTask;
}

void TNopSubtaskListener::SetDone() {
}

TNopSubtaskListener TNopSubtaskListener::Instance;

TTaskRunnerBase::TTaskRunnerBase(IEnv* env, ISubtaskListener* parentTask, TAutoPtr<ITaskBase> impl)
    : TActor<TTaskRunnerBase>(env->GetExecutor())
    , Impl(impl)
    , ParentTask(parentTask)
    //, HoldsSelfReference(false)
    , Done(false)
    , SetDoneCalled(false)
{
}

TTaskRunnerBase::~TTaskRunnerBase() {
    Y_ASSERT(Done);
}

namespace {
    struct TRunningInThisThreadGuard {
        TTaskRunnerBase* const Task;
        TRunningInThisThreadGuard(TTaskRunnerBase* task)
            : Task(task)
        {
            Y_ASSERT(!ThreadCurrentTask);
            ThreadCurrentTask = task;
        }

        ~TRunningInThisThreadGuard() {
            Y_ASSERT(ThreadCurrentTask == Task);
            ThreadCurrentTask = nullptr;
        }
    };
}

void NRainCheck::TTaskRunnerBase::Act(NActor::TDefaultTag) {
    Y_ASSERT(RefCount() > 0);

    TRunningInThisThreadGuard g(this);

    //RetainRef();

    for (;;) {
        TTempTlsVector<TSubtaskCompletion*> temp;

        temp.GetVector()->swap(Pending);

        for (auto& pending : *temp.GetVector()) {
            if (pending->IsComplete()) {
                pending->FireCompletionCallback(GetImplBase());
            } else {
                Pending.push_back(pending);
            }
        }

        if (!Pending.empty()) {
            return;
        }

        if (!Done) {
            Done = !ReplyReceived();
        } else {
            if (Pending.empty()) {
                if (!SetDoneCalled) {
                    ParentTask->SetDone();
                    SetDoneCalled = true;
                }
                //ReleaseRef();
                return;
            }
        }
    }
}

bool TTaskRunnerBase::IsRunningInThisThread() const {
    return ThreadCurrentTask == this;
}

TSubtaskCompletion::~TSubtaskCompletion() {
    ESubtaskState state = State.Get();
    Y_ASSERT(state == CREATED || state == DONE || state == CANCELED);
}

void TSubtaskCompletion::FireCompletionCallback(ITaskBase* task) {
    Y_ASSERT(IsComplete());

    if (!!CompletionFunc) {
        TSubtaskCompletionFunc temp = CompletionFunc;
        // completion func must be reset before calling it,
        // because function may set it back
        CompletionFunc = TSubtaskCompletionFunc();
        (task->*(temp.Func))(this);
    }
}

void NRainCheck::TSubtaskCompletion::Cancel() {
    for (;;) {
        ESubtaskState state = State.Get();
        if (state == CREATED && State.CompareAndSet(CREATED, CANCELED)) {
            return;
        }
        if (state == RUNNING && State.CompareAndSet(RUNNING, CANCEL_REQUESTED)) {
            return;
        }
        if (state == DONE && State.CompareAndSet(DONE, CANCELED)) {
            return;
        }
        if (state == CANCEL_REQUESTED || state == CANCELED) {
            return;
        }
    }
}

void TSubtaskCompletion::SetRunning(TTaskRunnerBase* parent) {
    Y_ASSERT(!TaskRunner);
    Y_ASSERT(!!parent);

    TaskRunner = parent;

    parent->Pending.push_back(this);

    parent->RefV();

    for (;;) {
        ESubtaskState current = State.Get();
        if (current != CREATED && current != DONE) {
            Y_ABORT("current state should be CREATED or DONE: %s", ToCString(current));
        }
        if (State.CompareAndSet(current, RUNNING)) {
            return;
        }
    }
}

void TSubtaskCompletion::SetDone() {
    Y_ASSERT(!!TaskRunner);
    TTaskRunnerBase* temp = TaskRunner;
    TaskRunner = nullptr;

    for (;;) {
        ESubtaskState state = State.Get();
        if (state == RUNNING) {
            if (State.CompareAndSet(RUNNING, DONE)) {
                break;
            }
        } else if (state == CANCEL_REQUESTED) {
            if (State.CompareAndSet(CANCEL_REQUESTED, CANCELED)) {
                break;
            }
        } else {
            Y_ABORT("cannot SetDone: unknown state: %s", ToCString(state));
        }
    }

    temp->ScheduleV();
    temp->UnRefV();
}

#if 0
void NRainCheck::TTaskRunnerBase::RetainRef()
{
    if (HoldsSelfReference) {
        return;
    }
    HoldsSelfReference = true;
    Ref();
}

void NRainCheck::TTaskRunnerBase::ReleaseRef()
{
    if (!HoldsSelfReference) {
        return;
    }
    HoldsSelfReference = false;
    DecRef();
}
#endif

void TTaskRunnerBase::AssertInThisThread() const {
    Y_ASSERT(IsRunningInThisThread());
}

TTaskRunnerBase* TTaskRunnerBase::CurrentTask() {
    Y_ABORT_UNLESS(!!ThreadCurrentTask);
    return ThreadCurrentTask;
}

ITaskBase* TTaskRunnerBase::CurrentTaskImpl() {
    return CurrentTask()->GetImplBase();
}

TString TTaskRunnerBase::GetStatusSingleLine() {
    return TypeName(*Impl);
}

bool NRainCheck::AreWeInsideTask() {
    return ThreadCurrentTask != nullptr;
}
