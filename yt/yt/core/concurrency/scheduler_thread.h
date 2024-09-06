#pragma once

#include "fiber_scheduler_thread.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerThread
    : public TFiberSchedulerThread
{
public:
    void Stop(bool graceful);
    void Stop();

protected:
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_;

    std::atomic<bool> GracefulStop_ = false;

    TSchedulerThread(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        TString threadGroupName,
        TString threadName,
        NThreading::TThreadOptions options = {});

    ~TSchedulerThread();

    // NB(arkady-e1ppa): We don't need a customisation point OnStop
    // because the only sensible case when we need to do something
    // after stop is a graceful shutdown for which we might want
    // to clear the queue. Now, every shutdownable queue is
    // either drained automatically (graceful = false) or
    // the Shutdown is graceful (TSchedulerThread::Stop(true)) will
    // be called. In the latter case |OnExecute| loop will
    // continue working until the queue is empty anyway. So we are safe.
    virtual void OnStart();

    TClosure OnExecute() override;

    virtual TClosure BeginExecute() = 0;
    virtual void EndExecute() = 0;

    TClosure BeginExecuteImpl(bool dequeued, TEnqueuedAction* action);

private:
    TCpuInstant LastMaintenanceInstant_ = 0;

    void MaybeRunMaintenance(TCpuInstant now);
    void RunMaintenance();

    void StartEpilogue() override;
    void StopPrologue() override;
    void StopEpilogue() override;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerThread)

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
