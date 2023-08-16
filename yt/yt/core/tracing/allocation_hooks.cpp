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

void* CopyAllocationTagsData(void* ptr)
{
    if (ptr) {
        auto* allocationTagsPtr = static_cast<TAllocationTags*>(ptr);
        allocationTagsPtr->Ref();
    }
    return ptr;
}

void DestroyAllocationTagsData(void* ptr)
{
    auto* allocationTagsPtr = static_cast<TAllocationTags*>(ptr);
    // NB. No need to check for nullptr here, because ScheduleFree already does that.
    FreeList->ScheduleFree(allocationTagsPtr);
}

const TAllocationTags::TTags& ReadAllocationTagsData(void* ptr)
{
    auto* allocationTagsPtr = static_cast<TAllocationTags*>(ptr);
    if (!allocationTagsPtr) {
        static TAllocationTags::TTags emptyTags;
        return emptyTags;
    }
    return allocationTagsPtr->GetTags();
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
