#pragma once

#include <library/cpp/yt/stockpile/stockpile.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <thread>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TStockpileManager
{
public:
    static TStockpileManager* Get();

    void Reconfigure(TStockpileOptions options);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::vector<std::unique_ptr<std::thread>> Threads_;

    std::atomic<bool> ShouldProceed_{true};
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
