#pragma once

#include "scheduler_thread.h"
#include "fair_share_invoker_queue.h"
#include "invoker_queue.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TFairShareQueueSchedulerThread
    : public TSchedulerThread
{
public:
    TFairShareQueueSchedulerThread(
        TFairShareInvokerQueuePtr queue,
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const TString& threadGroupName,
        const TString& threadName);

protected:
    const TFairShareInvokerQueuePtr Queue_;

    TEnqueuedAction CurrentAction_;

    TClosure BeginExecute() override;
    void EndExecute() override;

    void OnStart() override;
};

DEFINE_REFCOUNTED_TYPE(TFairShareQueueSchedulerThread)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
