#include "allocation_tags.h"

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
    auto allocationTagsPtr = traceContext->GetAllocationTags();
    return static_cast<void*>(allocationTagsPtr.Release());
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
    // NB. No need to check for nullptr here, because ScheduleFree already does that
    FreeList->ScheduleFree(allocationTagsPtr);
}

const std::vector<std::pair<TString, TString>>& ReadAllocationTagsData(void* ptr)
{
    auto* allocationTagsPtr = static_cast<TAllocationTags*>(ptr);
    if (!allocationTagsPtr) {
        static std::vector<std::pair<TString, TString>> emptyTags;
        return emptyTags;
    }
    return allocationTagsPtr->GetTags();
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
