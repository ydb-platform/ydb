#pragma once

#include <library/cpp/yt/memory/intrusive_ptr.h>
#include "library/cpp/yt/memory/new.h"

#include <library/cpp/yt/stockpile/stockpile.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <thread>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TStockpileThreadState)

struct TStockpileThreadState final
{
    std::atomic<bool> ShouldProceed;
};

DEFINE_REFCOUNTED_TYPE(TStockpileThreadState)

////////////////////////////////////////////////////////////////////////////////

class TStockpileManager
{
public:
    static TStockpileManager* Get();

    void Reconfigure(TStockpileOptions options);

    ~TStockpileManager();

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::vector<std::unique_ptr<std::thread>> Threads_;

    TStockpileThreadStatePtr ThreadState_ = New<TStockpileThreadState>();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
