#pragma once

#include "poller.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct IThreadPoolPoller
    : public IPoller
{
    //! Reconfigures number of polling threads.
    virtual void Reconfigure(int threadCount) = 0;

    //! Reconfigures polling period of thread pool.
    virtual void Reconfigure(TDuration pollingPeriod) = 0;
};

DEFINE_REFCOUNTED_TYPE(IThreadPoolPoller)

////////////////////////////////////////////////////////////////////////////////

IThreadPoolPollerPtr CreateThreadPoolPoller(
    int threadCount,
    const TString& threadNamePrefix,
    TDuration pollingPeriod = TDuration::MilliSeconds(10));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
