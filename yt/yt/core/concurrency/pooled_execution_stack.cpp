#include "pooled_execution_stack.h"

#include "fiber_manager.h"

#include <yt/yt/core/misc/object_pool.h>

#include <library/cpp/yt/threading/execution_stack.h>

#include <util/generic/size_literals.h>

#include <util/system/sanitizers.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// Stack sizes.
#if defined(_asan_enabled_) || defined(_msan_enabled_)
    static constexpr size_t SmallExecutionStackSize = 2_MB;
    static constexpr size_t LargeExecutionStackSize = 64_MB;
    static constexpr size_t HugeExecutionStackSize = 64_MB;
#else
    static constexpr size_t SmallExecutionStackSize = 256_KB;
    static constexpr size_t LargeExecutionStackSize = 8_MB;
    static constexpr size_t HugeExecutionStackSize = 64_MB;
#endif


////////////////////////////////////////////////////////////////////////////////

template <EExecutionStackKind Kind, size_t Size>
class TPooledExecutionStack
    : public NThreading::TExecutionStack
    , public TRefTracked<TPooledExecutionStack<Kind, Size>>
{
public:
    TPooledExecutionStack()
        : TExecutionStack(Size)
    { }
};

std::shared_ptr<NThreading::TExecutionStack> GetPooledExecutionStack(EExecutionStackKind kind)
{
    switch (kind) {
#define XX(kind) \
        case EExecutionStackKind::kind: \
            return ObjectPool<TPooledExecutionStack<EExecutionStackKind::kind, kind ## ExecutionStackSize>>().Allocate();
        XX(Small)
        XX(Large)
        XX(Huge)
#undef XX
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <NConcurrency::EExecutionStackKind Kind, size_t Size>
struct TPooledObjectTraits<NConcurrency::TPooledExecutionStack<Kind, Size>, void>
    : public TPooledObjectTraitsBase<NConcurrency::TPooledExecutionStack<Kind, Size>>
{
    using TStack = NConcurrency::TPooledExecutionStack<Kind, Size>;

    static void Clean(TStack* stack)
    {
#if defined(_asan_enabled_)
        if (stack->GetStack()) {
            NSan::Poison(stack->GetStack(), stack->GetSize());
        }
#else
        Y_UNUSED(stack);
#endif
    }

    static int GetMaxPoolSize()
    {
        return NConcurrency::TFiberManager::GetFiberStackPoolSize(Kind);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
