#include "allocation_tags.h"
#include "trace_context.h"

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <thread>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using namespace NTracing;

static auto* FreeList = LeakySingleton<TAllocationTagsFreeList>();

void* CreateAllocationTagsData()
{
    auto* traceContext = TryGetCurrentTraceContext();
    if (!traceContext) {
        return nullptr;
    }

    // Need to avoid deadlock from TTraceContext->SetAllocationTags due another allocation.
    auto allocationTags = traceContext->GetAllocationTagsPtr();

    return static_cast<void*>(allocationTags.Release());
}

void* CopyAllocationTagsData(void* userData)
{
    if (userData) {
        auto* allocationTagsPtr = static_cast<TAllocationTags*>(userData);
        allocationTagsPtr->Ref();
    }
    return userData;
}

void DestroyAllocationTagsData(void* userData)
{
    auto* allocationTagsPtr = static_cast<TAllocationTags*>(userData);
    // NB. No need to check for nullptr here, because ScheduleFree already does that.
    FreeList->ScheduleFree(allocationTagsPtr);
}

const TAllocationTags::TTags* ReadAllocationTagsData(void* userData)
{
    if (!userData) {
        return nullptr;
    }

    const auto* allocationTagsPtr = static_cast<TAllocationTags*>(userData);
    return allocationTagsPtr->GetTagsPtr();
}

std::optional<TString> FindTagValue(
    const TAllocationTags::TTags& tags,
    const TString& key)
{
    return TAllocationTags::FindTagValue(tags, key);
}

void StartAllocationTagsCleanupThread(TDuration cleanupInterval)
{
    std::thread backgroundThread([cleanupInterval] {
        for (;;) {
            FreeList->Cleanup();
            Sleep(cleanupInterval);
        }
    });
    backgroundThread.detach();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
