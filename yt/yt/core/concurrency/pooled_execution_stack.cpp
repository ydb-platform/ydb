#include "pooled_execution_stack.h"

#include "fiber_manager.h"

#include <yt/yt/core/misc/object_pool.h>

#include <library/cpp/yt/threading/execution_stack.h>

#include <util/system/sanitizers.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <EExecutionStackKind Kind>
class TPooledExecutionStack
    : public NThreading::TExecutionStack
    , public TRefTracked<TPooledExecutionStack<Kind>>
{
public:
    TPooledExecutionStack()
        : TExecutionStack(TFiberManager::GetFiberStackSize(Kind))
    { }
};

std::shared_ptr<NThreading::TExecutionStack> GetPooledExecutionStack(EExecutionStackKind kind)
{
    switch (kind) {
#define XX(kind) \
        case EExecutionStackKind::kind: \
            return ObjectPool<TPooledExecutionStack<EExecutionStackKind::kind>>().Allocate();
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

template <NConcurrency::EExecutionStackKind Kind>
struct TPooledObjectTraits<NConcurrency::TPooledExecutionStack<Kind>, void>
    : public TPooledObjectTraitsBase<NConcurrency::TPooledExecutionStack<Kind>>
{
    using TStack = NConcurrency::TPooledExecutionStack<Kind>;

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

    static bool IsReusable(const TStack* stack)
    {
        return stack->GetSize() == NConcurrency::TFiberManager::GetFiberStackSize(Kind);
    }

    static int GetMaxPoolSize()
    {
        return NConcurrency::TFiberManager::GetFiberStackPoolSize(Kind);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
