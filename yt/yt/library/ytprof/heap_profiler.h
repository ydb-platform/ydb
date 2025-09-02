#pragma once

#include "public.h"

#include <util/datetime/base.h>

#include <library/cpp/yt/memory/allocation_tags.h>

#include <util/generic/hash.h>

namespace NYT::NYTProf {

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
