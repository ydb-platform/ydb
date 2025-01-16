#include "trace_context.h"

#include <library/cpp/yt/memory/allocation_tags_hooks.h>
#include <library/cpp/yt/memory/leaky_singleton.h>

#include <util/system/thread.h>

#include <thread>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TAllocationTagsReclaimer
{
public:
    TAllocationTagsReclaimer()
    {
        std::thread thread([this] {
            TThread::SetCurrentThreadName("AllocTagsReclaim");
            while (true) {
                DoReclaim();
                Sleep(ReclaimPeriod);
            }
        });
        thread.detach();
    }

    void ScheduleReclaim(TAllocationTagList* list)
    {
        if (GetRefCounter(list)->Unref()) {
            ListsToReclaim_.Push(list);
        }
    }

    static TAllocationTagsReclaimer* Get()
    {
        return LeakySingleton<TAllocationTagsReclaimer>();
    }

private:
    TIntrusiveMpscStack<TAllocationTagList> ListsToReclaim_;

    static constexpr TDuration ReclaimPeriod = TDuration::Seconds(5);

    void DoReclaim()
    {
        auto items = ListsToReclaim_.PopAll();
        while (!items.Empty()) {
            DestroyRefCounted(items.PopFront()->Node());
        }
    }
};

void* CreateAllocationTags()
{
    const auto* traceContext = TryGetCurrentTraceContext();
    if (!traceContext) {
        return nullptr;
    }

    return traceContext->GetAllocationTagList().Release();
}

void* CopyAllocationTags(void* opaque)
{
    if (opaque) {
        static_cast<TAllocationTagList*>(opaque)->Ref();
    }

    return opaque;
}

void DestroyAllocationTags(void* opaque)
{
    if (auto* list = static_cast<TAllocationTagList*>(opaque)) {
        TAllocationTagsReclaimer::Get()->ScheduleReclaim(list);
    }
}

TRange<TAllocationTag> ReadAllocationTags(void* opaque)
{
    if (!opaque) {
        return {};
    }

    const auto* list = static_cast<TAllocationTagList*>(opaque);
    return list->GetTags();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

namespace NYT {

using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

const TAllocationTagsHooks& GetAllocationTagsHooks()
{
    // Boot the reclaimer up here, in a seemingly safe context.
    TAllocationTagsReclaimer::Get();
    static const TAllocationTagsHooks hooks{
        .CreateAllocationTags = CreateAllocationTags,
        .CopyAllocationTags = CopyAllocationTags,
        .DestroyAllocationTags = DestroyAllocationTags,
        .ReadAllocationTags = ReadAllocationTags,
    };
    return hooks;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
