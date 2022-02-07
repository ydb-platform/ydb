#include "coro.h"

#include "coro_stack.h"

#include <util/system/tls.h>
#include <util/system/yassert.h>

using namespace NRainCheck;

TContClosure TCoroTaskRunner::ContClosure(TCoroTaskRunner* runner, TArrayRef<char> memRegion) {
    TContClosure contClosure;
    contClosure.TrampoLine = runner;
    contClosure.Stack = memRegion;
    return contClosure;
}

TCoroTaskRunner::TCoroTaskRunner(IEnv* env, ISubtaskListener* parent, TAutoPtr<ICoroTask> impl)
    : TTaskRunnerBase(env, parent, impl.Release())
    , Stack(GetImpl()->StackSize)
    , ContMachineContext(ContClosure(this, Stack.MemRegion()))
    , CoroDone(false)
{
}

TCoroTaskRunner::~TCoroTaskRunner() {
    Y_ASSERT(CoroDone);
}

Y_POD_STATIC_THREAD(TContMachineContext*)
CallerContext;
Y_POD_STATIC_THREAD(TCoroTaskRunner*)
Task;

bool TCoroTaskRunner::ReplyReceived() {
    Y_ASSERT(!CoroDone);

    TContMachineContext me;

    CallerContext = &me;
    Task = this;

    me.SwitchTo(&ContMachineContext);

    Stack.VerifyNoStackOverflow();

    Y_ASSERT(CallerContext == &me);
    Y_ASSERT(Task == this);

    return !CoroDone;
}

void NRainCheck::TCoroTaskRunner::DoRun() {
    GetImpl()->Run();
    CoroDone = true;
    ContMachineContext.SwitchTo(CallerContext);
}

void NRainCheck::ICoroTask::WaitForSubtasks() {
    Task->ContMachineContext.SwitchTo(CallerContext);
}
