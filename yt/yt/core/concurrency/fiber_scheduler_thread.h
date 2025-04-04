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
        std::string threadGroupName,
        std::string threadName,
        NThreading::TThreadOptions options = {});

    //! Empty callback signals about stopping.
    virtual TClosure OnExecute() = 0;

private:
    const std::string ThreadGroupName_;

    void ThreadMain() override;
};

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
