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
        TString threadGroupName,
        TString threadName,
        NThreading::TThreadOptions options = {});

    //! Empty callback signals about stopping.
    virtual TClosure OnExecute() = 0;

private:
    const TString ThreadGroupName_;

    void ThreadMain() override;
};

////////////////////////////////////////////////////////////////////////////////

constexpr int DefaultMaxIdleFibers = 5'000;
void UpdateMaxIdleFibers(int maxIdleFibers);

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
