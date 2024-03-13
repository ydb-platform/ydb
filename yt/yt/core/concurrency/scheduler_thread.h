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

    virtual void OnStart();
    virtual void OnStop();

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
