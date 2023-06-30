#include "thread_affinity.h"

#include <yt/yt/core/actions/invoker_util.h>
#include <yt/yt/core/actions/invoker_pool.h>
#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/misc/proc.h>

namespace NYT::NConcurrency {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

void TThreadAffinitySlot::Check(TThreadId threadId)
{
    auto expectedId = InvalidThreadId;
    if (!BoundId_.compare_exchange_strong(expectedId, threadId)) {
        YT_VERIFY(expectedId == threadId);
    }
}

void TThreadAffinitySlot::Check()
{
    Check(GetCurrentThreadId());
}

TThreadId TThreadAffinitySlot::GetBoundThreadId() const
{
    return BoundId_;
}

bool VerifyInvokerAffinity(const IInvokerPtr& invoker)
{
    auto currentInvoker = GetCurrentInvoker();
    return
        currentInvoker->CheckAffinity(invoker) ||
        invoker->CheckAffinity(currentInvoker);
}

bool VerifySerializedInvokerAffinity(const IInvokerPtr& invoker)
{
    return
        VerifyInvokerAffinity(invoker) &&
        invoker->IsSerialized();
}

bool VerifyInvokerPoolAffinity(const IInvokerPoolPtr& invokerPool)
{
    for (int index = 0; index < invokerPool->GetSize(); ++index) {
        if (VerifyInvokerAffinity(invokerPool->GetInvoker(index))) {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

