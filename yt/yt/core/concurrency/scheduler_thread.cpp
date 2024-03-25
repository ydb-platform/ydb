#include "scheduler_thread.h"

#include "private.h"
#include "invoker_queue.h"

#include <yt/yt/core/misc/hazard_ptr.h>

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TSchedulerThread::TSchedulerThread(
    TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
    TString threadGroupName,
    TString threadName,
    NThreading::TThreadOptions options)
    : TFiberSchedulerThread(
        std::move(threadGroupName),
        std::move(threadName),
        std::move(options))
    , CallbackEventCount_(std::move(callbackEventCount))
{ }

TSchedulerThread::~TSchedulerThread()
{
    Stop();
}

void TSchedulerThread::OnStart()
{ }

void TSchedulerThread::OnStop()
{ }

void TSchedulerThread::Stop(bool graceful)
{
    GracefulStop_.store(graceful);
    TThread::Stop();
}

void TSchedulerThread::Stop()
{
    TFiberSchedulerThread::Stop();
    TThread::Stop();
}

void TSchedulerThread::StartEpilogue()
{
    TFiberSchedulerThread::StartEpilogue();
    OnStart();
}

void TSchedulerThread::StopPrologue()
{
    TFiberSchedulerThread::StopPrologue();
    CallbackEventCount_->NotifyAll();
}

void TSchedulerThread::StopEpilogue()
{
    TFiberSchedulerThread::StopEpilogue();
    OnStop();
}

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
