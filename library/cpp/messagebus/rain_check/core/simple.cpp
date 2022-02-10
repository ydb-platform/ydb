#include "simple.h"

using namespace NRainCheck;

TSimpleTaskRunner::TSimpleTaskRunner(IEnv* env, ISubtaskListener* parentTask, TAutoPtr<ISimpleTask> impl)
    : TTaskRunnerBase(env, parentTask, impl.Release())
    , ContinueFunc(&ISimpleTask::Start)
{
}

TSimpleTaskRunner::~TSimpleTaskRunner() {
    Y_ASSERT(!ContinueFunc);
}

bool TSimpleTaskRunner::ReplyReceived() {
    ContinueFunc = (GetImpl()->*(ContinueFunc.Func))();
    return !!ContinueFunc;
}
