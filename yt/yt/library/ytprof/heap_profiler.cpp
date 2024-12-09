#include "heap_profiler.h"

#include "symbolize.h"

#include <library/cpp/yt/backtrace/cursors/libunwind/libunwind_cursor.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/memory/allocation_tags_hooks.h>
#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/memory/leaky_singleton.h>
#include <library/cpp/yt/memory/new.h>

#include <util/string/join.h>

#include <util/system/thread.h>

#include <mutex>
#include <thread>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

NProto::Profile ConvertAllocationProfile(const tcmalloc::Profile& snapshot)
{
    NProto::Profile profile;
    profile.add_string_table();

    auto addString = [&] (const std::string& str) {
        auto index = profile.string_table_size();
        profile.add_string_table(ToProto(str));
        return index;
    };

    auto bytesUnitId = addString("bytes");

    {
        auto* sampleType = profile.add_sample_type();
        sampleType->set_type(addString("allocations"));
        sampleType->set_unit(addString("count"));
    }

    {
        auto* sampleType = profile.add_sample_type();
        sampleType->set_type(addString("space"));
        sampleType->set_unit(bytesUnitId);

        auto* periodType = profile.mutable_period_type();
        periodType->set_type(sampleType->type());
        periodType->set_unit(sampleType->unit());
    }

    // profile.set_period(snapshot.Period());

    auto allocatedSizeId = addString("allocated_size");
    auto requestedSizeId = addString("requested_size");
    auto requestedAlignmentId = addString("requested_alignment");

    THashMap<void*, ui64> locations;
    snapshot.Iterate([&] (const tcmalloc::Profile::Sample& sample) {
        auto* protoSample = profile.add_sample();
        protoSample->add_value(sample.count);
        protoSample->add_value(sample.sum);

        auto* allocatedSizeLabel = protoSample->add_label();
        allocatedSizeLabel->set_key(allocatedSizeId);
        allocatedSizeLabel->set_num(sample.allocated_size);
        allocatedSizeLabel->set_num_unit(bytesUnitId);

        auto* requestedSizeLabel = protoSample->add_label();
        requestedSizeLabel->set_key(requestedSizeId);
        requestedSizeLabel->set_num(sample.requested_size);
        requestedSizeLabel->set_num_unit(bytesUnitId);

        auto* requestedAlignmentLabel = protoSample->add_label();
        requestedAlignmentLabel->set_key(requestedAlignmentId);
        requestedAlignmentLabel->set_num(sample.requested_alignment);
        requestedAlignmentLabel->set_num_unit(bytesUnitId);

        for (int i = 0; i < sample.depth; i++) {
            auto ip = sample.stack[i];

            auto it = locations.find(ip);
            if (it != locations.end()) {
                protoSample->add_location_id(it->second);
                continue;
            }

            auto locationId = locations.size() + 1;

            auto* location = profile.add_location();
            location->set_address(reinterpret_cast<ui64>(ip));
            location->set_id(locationId);

            protoSample->add_location_id(locationId);
            locations[ip] = locationId;
        }

        // TODO(gepardo): Deduplicate values in string table
        // for (const auto& [key, value] : GetAllocationTagsHooks().ReadAllocationTags(sample.user_data)) {
        //     auto* label = protoSample->add_label();
        //     label->set_key(addString(key));
        //     label->set_str(addString(value));
        // }
    });

    profile.set_drop_frames(addString(JoinSeq("|", {
        ".*SampleifyAllocation",
        ".*AllocSmall",
        "slow_alloc",
        "TBasicString::TBasicString",
    })));

    Symbolize(&profile, true);
    return profile;
}

NProto::Profile ReadHeapProfile(tcmalloc::ProfileType profileType)
{
    auto snapshot = tcmalloc::MallocExtension::SnapshotCurrent(profileType);
    return ConvertAllocationProfile(snapshot);
}

int AbslStackUnwinder(
    void** frames,
    int* /*framesSizes*/,
    int maxFrames,
    int skipFrames,
    const void* /*uc*/,
    int* /*minDroppedFrames*/)
{
    NBacktrace::TLibunwindCursor cursor;

    for (int i = 0; i < skipFrames + 1; ++i) {
        cursor.MoveNext();
    }

    int count = 0;
    for (int i = 0; i < maxFrames; ++i) {
        if (cursor.IsFinished()) {
            return count;
        }

        // IP point's to return address. Subtract 1 to get accurate line information for profiler.
        frames[i] = reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(cursor.GetCurrentIP()) - 1);
        count++;

        cursor.MoveNext();
    }
    return count;
}

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
        Y_UNUSED(sample);
        // for (const auto& [tagKey, tagValue] : GetAllocationTagsHooks().ReadAllocationTags(sample.user_data)) {
        //     usage[tagKey][tagValue] += sample.sum;
        // }
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

void EnableMemoryProfilingTags(std::optional<TDuration> snapshotUpdatePeriod)
{
    static std::once_flag onceFlag;
    std::call_once(onceFlag, [&] {
        const auto& hooks = GetAllocationTagsHooks();
        Y_UNUSED(hooks);
        // tcmalloc::MallocExtension::SetSampleUserDataCallbacks(
        //     hooks.CreateAllocationTags,
        //     hooks.CopyAllocationTags,
        //     hooks.DestroyAllocationTags);

        if (snapshotUpdatePeriod) {
            std::thread thread([snapshotUpdatePeriod] {
                TThread::SetCurrentThreadName("MemSnapUpdate");
                while (true) {
                    auto lastUpdateTime = Now();
                    SetGlobalMemoryUsageSnapshot(CollectMemoryUsageSnapshot());
                    auto currentTime = Now();
                    if (lastUpdateTime + *snapshotUpdatePeriod > currentTime) {
                        Sleep(lastUpdateTime + *snapshotUpdatePeriod - currentTime);
                    }
                }
            });
            thread.detach();
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
