#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/fiber_manager.h>
#include <yt/yt/core/concurrency/pooled_execution_stack.h>

#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/threading/execution_stack.h>

#include <util/generic/size_literals.h>

#include <util/system/sanitizers.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFiberManagerTest, DynamicallyConfigureFiberStackSize)
{
    THashMap<EExecutionStackKind, size_t> originalFiberStackSizes;
    for (auto stackKind : TEnumTraits<EExecutionStackKind>::GetDomainValues()) {
        originalFiberStackSizes[stackKind] = TFiberManager::GetFiberStackSize(stackKind);
    }
    auto restoreFiberStackSizes = Finally([originalFiberStackSizes] {
        for (auto [stackKind, stackSize] : originalFiberStackSizes) {
            TFiberManager::SetFiberStackSize(stackKind, stackSize);
        }
    });

#if defined(_asan_enabled_) || defined(_msan_enabled_)
    EXPECT_EQ(2_MB, TFiberManager::GetFiberStackSize(EExecutionStackKind::Small));
    EXPECT_EQ(64_MB, TFiberManager::GetFiberStackSize(EExecutionStackKind::Large));
#else
    EXPECT_EQ(256_KB, TFiberManager::GetFiberStackSize(EExecutionStackKind::Small));
    EXPECT_EQ(8_MB, TFiberManager::GetFiberStackSize(EExecutionStackKind::Large));
#endif
    auto config = New<TFiberManagerConfig>();
    config->FiberStackSizes[EExecutionStackKind::Huge] = 1_MB;
    TFiberManager::Configure(config);

    {
        auto stack = GetPooledExecutionStack(EExecutionStackKind::Huge);
        EXPECT_EQ(1_MB, stack->GetSize());
    }

    auto dynamicConfig = New<TFiberManagerDynamicConfig>();
    dynamicConfig->FiberStackSizes[EExecutionStackKind::Huge] = 2_MB;
    TFiberManager::Configure(config->ApplyDynamic(dynamicConfig));

    // The 1 MB stack is already in the pool and must not be reused.
    auto stack = GetPooledExecutionStack(EExecutionStackKind::Huge);
    EXPECT_EQ(2_MB, stack->GetSize());

    dynamicConfig = New<TFiberManagerDynamicConfig>();
    TFiberManager::Configure(config->ApplyDynamic(dynamicConfig));

    // The active 2 MB stack becomes stale and must not be reused.
    stack.reset();
    stack = GetPooledExecutionStack(EExecutionStackKind::Huge);
    EXPECT_EQ(1_MB, stack->GetSize());
}

TEST(TFiberManagerTest, ValidateFiberStackSize)
{
    const auto invalidStackSize = GetPageSize() + 1;

    auto config = New<TFiberManagerConfig>();
    config->FiberStackSizes[EExecutionStackKind::Huge] = invalidStackSize;

    EXPECT_THROW_WITH_SUBSTRING(
        config->Postprocess(),
        "must be a multiple of the memory page size");
    EXPECT_THROW_WITH_SUBSTRING(
        TFiberManager::SetFiberStackSize(EExecutionStackKind::Huge, invalidStackSize),
        "must be a multiple of the memory page size");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
