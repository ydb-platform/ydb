#pragma once

#include "private.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/threading/thread.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Executes actions in fiber context.
class TFiberSchedulerThread
    : public NThreading::TThread
{
public:
    TFiberSchedulerThread(
        const TString& threadGroupName,
        const TString& threadName,
        NThreading::EThreadPriority threadPriority = NThreading::EThreadPriority::Normal,
        int shutdownPriority = 0);

    //! Empty callback signals about stopping.
    virtual TClosure OnExecute() = 0;

private:
    void ThreadMain() override;

    const TString ThreadGroupName_;
};

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
