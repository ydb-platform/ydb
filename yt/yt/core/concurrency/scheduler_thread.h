#pragma once

#include "fiber_scheduler_thread.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerThreadBase
    : public TFiberSchedulerThread
{
public:
    ~TSchedulerThreadBase();

    void Stop(bool graceful);
    void Stop();

protected:
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_;
    std::atomic<bool> GracefulStop_ = false;

    TSchedulerThreadBase(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const TString& threadGroupName,
        const TString& threadName,
        NThreading::EThreadPriority threadPriority = NThreading::EThreadPriority::Normal,
        int shutdownPriority = 0);

    virtual void OnStart();
    virtual void OnStop();

private:
    void StartEpilogue() override;
    void StopPrologue() override;
    void StopEpilogue() override;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerThreadBase)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerThread
    : public TSchedulerThreadBase
{
protected:
    using TSchedulerThreadBase::TSchedulerThreadBase;

    TClosure OnExecute() override;

    virtual TClosure BeginExecute() = 0;
    virtual void EndExecute() = 0;

    TClosure BeginExecuteImpl(bool dequeued, TEnqueuedAction* action);

private:
    TCpuInstant LastMaintenanceInstant_ = 0;

    void MaybeRunMaintenance(TCpuInstant now);
    void RunMaintenance();
};

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
