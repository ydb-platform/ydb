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

inline constexpr ui64 DefaultMaxIdleFibers = 5000;
void UpdateMaxIdleFibers(ui64 maxIdleFibers);

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
