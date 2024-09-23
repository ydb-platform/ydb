#pragma once

#include "public.h"

#include <yt/yt/library/ytprof/proto/profile.pb.h>

#include <util/datetime/base.h>

#include <library/cpp/yt/memory/allocation_tags.h>

#include <util/generic/hash.h>

#include <tcmalloc/malloc_extension.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

NProto::Profile ConvertAllocationProfile(const tcmalloc::Profile& snapshot);
NProto::Profile ReadHeapProfile(tcmalloc::ProfileType profileType);

int AbslStackUnwinder(
    void** frames,
    int* framesSizes,
    int maxFrames,
    int skipFrames,
    const void* uc,
    int* minDroppedFrames);

////////////////////////////////////////////////////////////////////////////////

class TMemoryUsageSnapshot final
{
public:
    using TData = THashMap<TAllocationTagKey, THashMap<TAllocationTagValue, size_t>>;

    TMemoryUsageSnapshot() = default;
    explicit TMemoryUsageSnapshot(TData&& data) noexcept;

    const THashMap<TAllocationTagKey, size_t>& GetUsageSlice(const TAllocationTagKey& key) const noexcept;
    size_t GetUsage(const TAllocationTagKey& key, const TAllocationTagValue& value) const noexcept;

private:
    const TData Data_;
};

DEFINE_REFCOUNTED_TYPE(TMemoryUsageSnapshot)

////////////////////////////////////////////////////////////////////////////////

//! Builds the current snapshot of memory usage.
TMemoryUsageSnapshotPtr CollectMemoryUsageSnapshot();

//! Updates the global memory usage snapshot.
void SetGlobalMemoryUsageSnapshot(TMemoryUsageSnapshotPtr snapshot);

//! Gets the global memory usage snapshot.
TMemoryUsageSnapshotPtr GetGlobalMemoryUsageSnapshot();

//! If updateSnapshotPeriod is non-null, starts updating global snapshot in background thread.
void EnableMemoryProfilingTags(std::optional<TDuration> snapshotUpdatePeriod = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
