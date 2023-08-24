#pragma once

#include <yt/yt/library/ytprof/profile.pb.h>

#include <yt/yt/core/tracing/public.h>

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

typedef uintptr_t TMemoryTag;

TMemoryTag GetMemoryTag();

TMemoryTag SetMemoryTag(TMemoryTag newTag);

THashMap<TMemoryTag, ui64> GetEstimatedMemoryUsage();

void UpdateMemoryUsageSnapshot(THashMap<TMemoryTag, ui64> usageSnapshot);

i64 GetEstimatedMemoryUsage(TMemoryTag tag);

void EnableMemoryProfilingTags();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
