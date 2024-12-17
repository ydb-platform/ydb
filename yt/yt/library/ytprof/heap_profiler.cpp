#include "heap_profiler.h"

#include <yt/yt/library/ytprof/proto/profile.pb.h>

#include <library/cpp/yt/memory/allocation_tags_hooks.h>
#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/memory/leaky_singleton.h>
#include <library/cpp/yt/memory/new.h>

#include <tcmalloc/malloc_extension.h>

#include <util/system/thread.h>

#include <mutex>
#include <thread>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

TMemoryUsageSnapshot::TMemoryUsageSnapshot(TMemoryUsageSnapshot::TData&& data) noexcept
    : Data_(std::move(data))
{ }

const THashMap<TAllocationTagValue, size_t>& TMemoryUsageSnapshot::GetUsageSlice(const TAllocationTagKey& key) const noexcept
{
    if (auto it = Data_.find(key)) {
        return it->second;
    }

    static const THashMap<TAllocationTagValue, size_t> empty;
    return empty;
}

size_t TMemoryUsageSnapshot::GetUsage(const TAllocationTagKey& key, const TAllocationTagKey& value) const noexcept
{
    if (auto it = Data_.find(key)) {
        if (auto usageIt = it->second.find(value)) {
            return usageIt->second;
        }
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

struct TGlobalMemoryUsageSnapshot
{
    static TGlobalMemoryUsageSnapshot* Get()
    {
        return LeakySingleton<TGlobalMemoryUsageSnapshot>();
    }

    TAtomicIntrusivePtr<TMemoryUsageSnapshot> Snapshot{New<TMemoryUsageSnapshot>()};
};

TMemoryUsageSnapshotPtr CollectMemoryUsageSnapshot()
{
    TMemoryUsageSnapshot::TData usage;

    auto snapshot = tcmalloc::MallocExtension::SnapshotCurrent(tcmalloc::ProfileType::kHeap);
    snapshot.Iterate([&] (const tcmalloc::Profile::Sample& sample) {
        for (const auto& [tagKey, tagValue] : GetAllocationTagsHooks().ReadAllocationTags(sample.user_data)) {
            usage[tagKey][tagValue] += sample.sum;
        }
    });

    return New<TMemoryUsageSnapshot>(std::move(usage));
}

void SetGlobalMemoryUsageSnapshot(TMemoryUsageSnapshotPtr snapshot)
{
    TGlobalMemoryUsageSnapshot::Get()->Snapshot.Store(std::move(snapshot));
}

TMemoryUsageSnapshotPtr GetGlobalMemoryUsageSnapshot()
{
    return TGlobalMemoryUsageSnapshot::Get()->Snapshot.Acquire();
}

static std::atomic<TDuration> SnapshotUpdatePeriod;

void EnableMemoryProfilingTags(std::optional<TDuration> snapshotUpdatePeriod)
{
    static std::once_flag hooksFlag;
    std::call_once(hooksFlag, [&] {
        const auto& hooks = GetAllocationTagsHooks();
        tcmalloc::MallocExtension::SetSampleUserDataCallbacks(
            hooks.CreateAllocationTags,
            hooks.CopyAllocationTags,
            hooks.DestroyAllocationTags);
    });

    SnapshotUpdatePeriod.store(snapshotUpdatePeriod.value_or(TDuration::Zero()));
    if (snapshotUpdatePeriod) {
        static std::once_flag threadFlag;
        std::call_once(threadFlag, [&] {
            std::thread thread([] {
                TThread::SetCurrentThreadName("MemSnapUpdate");
                while (true) {
                    auto snapshotUpdatePeriod = SnapshotUpdatePeriod.load();
                    if (snapshotUpdatePeriod == TDuration::Zero()) {
                        Sleep(TDuration::Seconds(1));
                        continue;
                    }
                    auto lastUpdateTime = Now();
                    SetGlobalMemoryUsageSnapshot(CollectMemoryUsageSnapshot());
                    auto currentTime = Now();
                    if (lastUpdateTime + snapshotUpdatePeriod > currentTime) {
                        Sleep(lastUpdateTime + snapshotUpdatePeriod - currentTime);
                    }
                }
            });
            thread.detach();
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
