#include "fiber_manager.h"

#include "config.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/memory/leaky_singleton.h>
#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

#include <util/generic/size_literals.h>

#include <util/system/sanitizers.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace {

// Stack sizes.
#if defined(_asan_enabled_) || defined(_msan_enabled_)
    constexpr size_t DefaultSmallFiberStackSize = 2_MB;
    constexpr size_t DefaultLargeFiberStackSize = 64_MB;
    constexpr size_t DefaultHugeFiberStackSize = 64_MB;
#else
    constexpr size_t DefaultSmallFiberStackSize = 256_KB;
    constexpr size_t DefaultLargeFiberStackSize = 8_MB;
    constexpr size_t DefaultHugeFiberStackSize = 64_MB;
#endif

size_t GetDefaultFiberStackSize(EExecutionStackKind stackKind)
{
    switch (stackKind) {
        case EExecutionStackKind::Small:
            return DefaultSmallFiberStackSize;
        case EExecutionStackKind::Large:
            return DefaultLargeFiberStackSize;
        case EExecutionStackKind::Huge:
            return DefaultHugeFiberStackSize;
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TFiberManagerImpl
{
public:
    static TFiberManagerImpl* Get()
    {
        return LeakySingleton<TFiberManagerImpl>();
    }

    size_t GetFiberStackSize(EExecutionStackKind stackKind)
    {
        return FiberStackSizes_[stackKind].load(std::memory_order::relaxed);
    }

    void SetFiberStackSize(EExecutionStackKind stackKind, size_t stackSize)
    {
        TFiberManager::ValidateFiberStackSize(stackKind, stackSize);
        FiberStackSizes_[stackKind].store(stackSize, std::memory_order::relaxed);
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
        for (auto stackKind : TEnumTraits<EExecutionStackKind>::GetDomainValues()) {
            auto it = config->FiberStackSizes.find(stackKind);
            SetFiberStackSize(
                stackKind,
                it == config->FiberStackSizes.end()
                    ? GetDefaultFiberStackSize(stackKind)
                    : it->second);
        }
        for (auto [stackKind, poolSize] : config->FiberStackPoolSizes) {
            SetFiberStackPoolSize(stackKind, poolSize);
        }
    }

private:
    DECLARE_LEAKY_SINGLETON_FRIEND()

    TFiberManagerImpl()
    {
        for (auto stackKind : TEnumTraits<EExecutionStackKind>::GetDomainValues()) {
            FiberStackSizes_[stackKind].store(
                GetDefaultFiberStackSize(stackKind),
                std::memory_order::relaxed);
        }
        std::ranges::fill(FiberStackPoolSizes_, DefaultFiberStackPoolSize);
    }

    TEnumIndexedArray<EExecutionStackKind, std::atomic<size_t>> FiberStackSizes_;
    TEnumIndexedArray<EExecutionStackKind, std::atomic<int>> FiberStackPoolSizes_;
    std::atomic<int> MaxIdleFibers_ = DefaultMaxIdleFibers;
};

////////////////////////////////////////////////////////////////////////////////

size_t TFiberManager::GetFiberStackSize(EExecutionStackKind stackKind)
{
    return TFiberManagerImpl::Get()->GetFiberStackSize(stackKind);
}

void TFiberManager::ValidateFiberStackSize(EExecutionStackKind stackKind, size_t stackSize)
{
    if (stackSize == 0) {
        THROW_ERROR_EXCEPTION("Size of %Qlv stack must be positive",
            stackKind);
    }
    if (stackSize % GetPageSize() != 0) {
        THROW_ERROR_EXCEPTION("Size of %Qlv stack must be a multiple of the memory page size",
            stackKind)
            .With("stack_size", stackSize)
            .With("page_size", GetPageSize());
    }
}

void TFiberManager::SetFiberStackSize(EExecutionStackKind stackKind, size_t stackSize)
{
    TFiberManagerImpl::Get()->SetFiberStackSize(stackKind, stackSize);
}

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
