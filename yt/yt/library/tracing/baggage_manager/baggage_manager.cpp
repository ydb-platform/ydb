#include "baggage_manager.h"

#include "config.h"

#include <library/cpp/yt/memory/leaky_singleton.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TBaggageManagerImpl
{
public:
    static TBaggageManagerImpl* Get()
    {
        return LeakySingleton<TBaggageManagerImpl>();
    }

    bool IsBaggageAdditionEnabled()
    {
        return EnableBaggageAddition_.load(std::memory_order_relaxed);
    }

    void Configure(const TBaggageManagerConfigPtr& config)
    {
        EnableBaggageAddition_.store(config->EnableBaggageAddition, std::memory_order_relaxed);
    }

private:
    DECLARE_LEAKY_SINGLETON_FRIEND()

    std::atomic<bool> EnableBaggageAddition_ = false;
};

////////////////////////////////////////////////////////////////////////////////

bool TBaggageManager::IsBaggageAdditionEnabled()
{
    return TBaggageManagerImpl::Get()->IsBaggageAdditionEnabled();
}

void TBaggageManager::Configure(const TBaggageManagerConfigPtr& config)
{
    TBaggageManagerImpl::Get()->Configure(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
