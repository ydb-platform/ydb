#pragma once

#include "public.h"

#include <yt/yt/library/ytprof/proto/profile.pb.h>

#include <util/datetime/base.h>

#include <util/generic/hash.h>

#include <tcmalloc/malloc_extension.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

NProto::Profile ConvertAllocationProfile(const tcmalloc::Profile& snapshot);

NProto::Profile ReadHeapProfile(tcmalloc::ProfileType profileType);

int AbslStackUnwinder(void** frames, int*,
                      int maxFrames, int skipFrames,
                      const void*,
                      int*);

////////////////////////////////////////////////////////////////////////////////

class TMemoryUsageSnapshot
    : public virtual TRefCounted
{
public:
    using TData = THashMap<TString, THashMap<TString, size_t>>;

    TMemoryUsageSnapshot() = default;

    TMemoryUsageSnapshot(TMemoryUsageSnapshot&& other) noexcept = default;

    explicit TMemoryUsageSnapshot(TData&& data) noexcept;

    const THashMap<TString, size_t>& GetUsage(const TString& tagName) const noexcept;

    size_t GetUsage(const TString& tagName, const TString& tag) const noexcept;

private:
    const TData Data_;
    static inline const THashMap<TString, size_t> EmptyHashMap_;
};

DEFINE_REFCOUNTED_TYPE(TMemoryUsageSnapshot)

////////////////////////////////////////////////////////////////////////////////

TMemoryUsageSnapshotPtr CollectMemoryUsageSnapshot();

//! Update snapshot in LeakySingleton.
void UpdateMemoryUsageSnapshot(TMemoryUsageSnapshotPtr usageSnapshot);

//! Get snapshot from LeakySingleton.
TMemoryUsageSnapshotPtr GetMemoryUsageSnapshot();

//! If put updateSnapshotPeriod will start updating snapshot in LeakySingleton.
void EnableMemoryProfilingTags(std::optional<TDuration> updateSnapshotPeriod = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
