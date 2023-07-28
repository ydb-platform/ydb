#include "scheduler_thread.h"

#include "private.h"
#include "invoker_queue.h"

#include <yt/yt/core/misc/hazard_ptr.h>

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TSchedulerThreadBase::~TSchedulerThreadBase()
{
    Stop();
}

TSchedulerThreadBase::TSchedulerThreadBase(
    TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
    const TString& threadGroupName,
    const TString& threadName,
    NThreading::EThreadPriority threadPriority,
    int shutdownPriority)
    : TFiberSchedulerThread(
        threadGroupName,
        threadName,
        threadPriority,
        shutdownPriority)
    , CallbackEventCount_(std::move(callbackEventCount))
{ }

void TSchedulerThreadBase::OnStart()
{ }

void TSchedulerThreadBase::OnStop()
{ }

void TSchedulerThreadBase::Stop(bool graceful)
{
    GracefulStop_.store(graceful);
    TThread::Stop();
}

void TSchedulerThreadBase::Stop()
{
    TThread::Stop();
}

void TSchedulerThreadBase::StartEpilogue()
{
    OnStart();
}

void TSchedulerThreadBase::StopPrologue()
{
    CallbackEventCount_->NotifyAll();
}

void TSchedulerThreadBase::StopEpilogue()
{
    OnStop();
}

////////////////////////////////////////////////////////////////////////////////

TClosure TSchedulerThread::OnExecute()
{
    EndExecute();

    while (true) {
        auto cookie = CallbackEventCount_->PrepareWait();

        // Stop flag must be checked after PrepareWait because thread is notified when stop flag is set.
        // Otherwise, we can miss notification.
        bool stopping = IsStopping();
        if (stopping && !GracefulStop_.load()) {
            CallbackEventCount_->CancelWait();
            return TClosure();
        }

        auto callback = BeginExecute();

        if (callback || stopping) {
            CallbackEventCount_->CancelWait();
            return callback;
        }

        constexpr auto WaitTimeout = TDuration::Seconds(1);
        if (!CallbackEventCount_->Wait(cookie, WaitTimeout)) {
            MaybeRunMaintenance(GetCpuInstant());
        }

        EndExecute();
    }
}

TClosure TSchedulerThread::BeginExecuteImpl(bool dequeued, TEnqueuedAction* action)
{
    if (!dequeued) {
        return {};
    }
    MaybeRunMaintenance(action->StartedAt);
    return std::move(action->Callback);
}

void TSchedulerThread::MaybeRunMaintenance(TCpuInstant now)
{
    // 1B clock cycles between maintenance iterations.
    constexpr i64 MaintenancePeriod = 1'000'000'000;
    if (now > LastMaintenanceInstant_ + MaintenancePeriod) {
        RunMaintenance();
        LastMaintenanceInstant_ = now;
    }
}

void TSchedulerThread::RunMaintenance()
{
    ReclaimHazardPointers(/*flush*/ false);
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
