#include "heap_profiler.h"

#include "symbolize.h"

#include <library/cpp/yt/backtrace/cursors/libunwind/libunwind_cursor.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/memory/leaky_singleton.h>
#include <library/cpp/yt/memory/new.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <util/generic/hash_set.h>
#include <util/string/join.h>
#include <util/string/cast.h>

#include <tcmalloc/malloc_extension.h>

#include <thread>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void* CreateAllocationTagsData()
{
    return nullptr;
}

Y_WEAK void* CopyAllocationTagsData(void* userData)
{
    return userData;
}

Y_WEAK void DestroyAllocationTagsData(void* /*userData*/)
{ }

Y_WEAK const std::vector<std::pair<TString, TString>>* ReadAllocationTagsData(void* /*userData*/)
{
    return nullptr;
}

Y_WEAK std::optional<TString> FindTagValue(
    const std::vector<std::pair<TString, TString>>& tags,
    const TString& key)
{
    Y_UNUSED(tags);
    Y_UNUSED(key);
    return ::ToString(NullMemoryTag);
}

Y_WEAK void StartAllocationTagsCleanupThread(TDuration /*cleanupInterval*/)
{ }

////////////////////////////////////////////////////////////////////////////////

namespace NYTProf {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

NProto::Profile ConvertAllocationProfile(const tcmalloc::Profile& snapshot)
{
    NProto::Profile profile;
    profile.add_string_table();

    auto addString = [&] (TString str) {
        auto index = profile.string_table_size();
        profile.add_string_table(str);
        return index;
    };

    auto sampleType = profile.add_sample_type();
    sampleType->set_type(addString("allocations"));
    sampleType->set_unit(addString("count"));

    sampleType = profile.add_sample_type();
    sampleType->set_type(addString("space"));

    auto bytesUnitId = addString("bytes");
    sampleType->set_unit(bytesUnitId);

    auto periodType = profile.mutable_period_type();
    periodType->set_type(sampleType->type());
    periodType->set_unit(sampleType->unit());

    profile.set_period(snapshot.Period());

    auto allocatedSizeId = addString("allocated_size");
    auto requestedSizeId = addString("requested_size");
    auto requestedAlignmentId = addString("requested_alignment");

    THashMap<void*, ui64> locations;
    snapshot.Iterate([&] (const tcmalloc::Profile::Sample& sample) {
        auto sampleProto = profile.add_sample();
        sampleProto->add_value(sample.count);
        sampleProto->add_value(sample.sum);

        auto allocatedSizeLabel = sampleProto->add_label();
        allocatedSizeLabel->set_key(allocatedSizeId);
        allocatedSizeLabel->set_num(sample.allocated_size);
        allocatedSizeLabel->set_num_unit(bytesUnitId);

        auto requestedSizeLabel = sampleProto->add_label();
        requestedSizeLabel->set_key(requestedSizeId);
        requestedSizeLabel->set_num(sample.requested_size);
        requestedSizeLabel->set_num_unit(bytesUnitId);

        auto requestedAlignmentLabel = sampleProto->add_label();
        requestedAlignmentLabel->set_key(requestedAlignmentId);
        requestedAlignmentLabel->set_num(sample.requested_alignment);
        requestedAlignmentLabel->set_num_unit(bytesUnitId);

        for (int i = 0; i < sample.depth; i++) {
            auto ip = sample.stack[i];

            auto it = locations.find(ip);
            if (it != locations.end()) {
                sampleProto->add_location_id(it->second);
                continue;
            }

            auto locationId = locations.size() + 1;

            auto location = profile.add_location();
            location->set_address(reinterpret_cast<ui64>(ip));
            location->set_id(locationId);

            sampleProto->add_location_id(locationId);
            locations[ip] = locationId;
        }

        // TODO(gepardo): Deduplicate values in string table
        if (const auto* data = ReadAllocationTagsData(sample.user_data)) {
            for (const auto& [key, value] : *data) {
                auto label = sampleProto->add_label();
                label->set_key(addString(key));
                label->set_str(addString(value));
            }
        }
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

TMemoryUsageSnapshot::TMemoryUsageSnapshot(TMemoryUsageSnapshot::TData&& data) noexcept
    : Data_(std::move(data))
{ }

const THashMap<TString, size_t>& TMemoryUsageSnapshot::GetUsage(const TString& tagName) const noexcept
{
    if (auto it = Data_.find(tagName)) {
        return it->second;
    }

    return EmptyHashMap_;
}

size_t TMemoryUsageSnapshot::GetUsage(const TString& tagName, const TString& tag) const noexcept
{
    if (auto it = Data_.find(tagName)) {
        if (auto usageIt = it->second.find(tag)) {
            return usageIt->second;
        }
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

struct TMemoryUsageSnapshotStorage
{
    TAtomicIntrusivePtr<TMemoryUsageSnapshot> Snapshot{New<TMemoryUsageSnapshot>()};
};

TMemoryUsageSnapshotPtr CollectMemoryUsageSnapshot()
{
    TMemoryUsageSnapshot::TData usage;

    auto snapshot = tcmalloc::MallocExtension::SnapshotCurrent(tcmalloc::ProfileType::kHeap);
    snapshot.Iterate([&] (const tcmalloc::Profile::Sample& sample) {
        if (const auto* data = ReadAllocationTagsData(sample.user_data)) {
            for (const auto& [tagName, tag] : *data) {
                usage[tagName][tag] += sample.sum;
            }
        }
    });

    return New<TMemoryUsageSnapshot>(std::move(usage));
}

void UpdateMemoryUsageSnapshot(TMemoryUsageSnapshotPtr usageSnapshot)
{
    auto snapshot = LeakySingleton<TMemoryUsageSnapshotStorage>();
    snapshot->Snapshot.Store(std::move(usageSnapshot));
}

TMemoryUsageSnapshotPtr GetMemoryUsageSnapshot()
{
    const auto snapshot = LeakySingleton<TMemoryUsageSnapshotStorage>();
    return snapshot->Snapshot.Acquire();
}

int AbslStackUnwinder(
    void** frames,
    int*,
    int maxFrames,
    int skipFrames,
    const void*,
    int*)
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

void EnableMemoryProfilingTags(std::optional<TDuration> updateSnapshotPeriod)
{
    StartAllocationTagsCleanupThread(TDuration::Seconds(1));
    tcmalloc::MallocExtension::SetSampleUserDataCallbacks(
        &CreateAllocationTagsData,
        &CopyAllocationTagsData,
        &DestroyAllocationTagsData);

    if (updateSnapshotPeriod) {
        std::thread backgroundThread([updateSnapshotPeriod] {
            TInstant lastUpdateTime;
            TInstant currentTime;

            while (true) {
                lastUpdateTime = Now();
                UpdateMemoryUsageSnapshot(CollectMemoryUsageSnapshot());

                currentTime = Now();
                if (lastUpdateTime + updateSnapshotPeriod.value() > currentTime) {
                    Sleep(lastUpdateTime + updateSnapshotPeriod.value() - currentTime);
                }
            }
        });
        backgroundThread.detach();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTProf
} // namespace NYT
