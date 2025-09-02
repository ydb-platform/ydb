#pragma once

#include "poller.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct IThreadPoolPoller
    : public IPoller
{
    //! Reconfigures number of polling threads.
    virtual void SetThreadCount(int threadCount) = 0;

    //! Reconfigures polling period of thread pool.
    virtual void SetPollingPeriod(TDuration pollingPeriod) = 0;
};

DEFINE_REFCOUNTED_TYPE(IThreadPoolPoller)

////////////////////////////////////////////////////////////////////////////////

IThreadPoolPollerPtr CreateThreadPoolPoller(
    int threadCount,
    std::string threadNamePrefix,
    TDuration pollingPeriod = TDuration::MilliSeconds(10));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
