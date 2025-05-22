#include "fiber_manager.h"

#include "config.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TFiberManagerImpl
{
public:
    static TFiberManagerImpl* Get()
    {
        return LeakySingleton<TFiberManagerImpl>();
    }

    int GetFiberStackPoolSize(EExecutionStackKind stackKind)
    {
        return FiberStackPoolSizes_[stackKind].load(std::memory_order::relaxed);
    }

    void SetFiberStackPoolSize(EExecutionStackKind stackKind, int poolSize)
    {
        if (poolSize < 1) {
            THROW_ERROR_EXCEPTION("Pool size must be positive");
        }
        FiberStackPoolSizes_[stackKind].store(poolSize);
    }

    int GetMaxIdleFibers()
    {
        return MaxIdleFibers_.load(std::memory_order::relaxed);
    }

    void SetMaxIdleFibers(int maxIdleFibers)
    {
        MaxIdleFibers_.store(maxIdleFibers);
    }

    void Configure(const TFiberManagerConfigPtr& config)
    {
        for (auto [stackKind, poolSize] : config->FiberStackPoolSizes) {
            SetFiberStackPoolSize(stackKind, poolSize);
        }
    }

private:
    DECLARE_LEAKY_SINGLETON_FRIEND()

    TFiberManagerImpl()
    {
        std::ranges::fill(FiberStackPoolSizes_, DefaultFiberStackPoolSize);
    }

    TEnumIndexedArray<EExecutionStackKind, std::atomic<int>> FiberStackPoolSizes_;
    std::atomic<int> MaxIdleFibers_ = DefaultMaxIdleFibers;
};

////////////////////////////////////////////////////////////////////////////////

int TFiberManager::GetFiberStackPoolSize(EExecutionStackKind stackKind)
{
    return TFiberManagerImpl::Get()->GetFiberStackPoolSize(stackKind);
}

void TFiberManager::SetFiberStackPoolSize(EExecutionStackKind stackKind, int poolSize)
{
    TFiberManagerImpl::Get()->SetFiberStackPoolSize(stackKind, poolSize);
}

int TFiberManager::GetMaxIdleFibers()
{
    return TFiberManagerImpl::Get()->GetMaxIdleFibers();
}

void TFiberManager::SetMaxIdleFibers(int maxIdleFibers)
{
    TFiberManagerImpl::Get()->SetMaxIdleFibers(maxIdleFibers);
}

void TFiberManager::Configure(const TFiberManagerConfigPtr& config)
{
    TFiberManagerImpl::Get()->Configure(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
