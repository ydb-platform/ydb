#pragma once

#include "fwd.h"

namespace NKikimr::NKqp::NScheduler {

// The proxy-object between any memory consumer of the query and the scheduler itself.
// The usage is accounted for the query and its parents, but is limited only by the fair-share of the leaf pool.

struct TSchedulableMemory {
    explicit TSchedulableMemory(const NHdrf::NDynamic::TQueryPtr& query);

    // Without a snapshot the usage is increased unconditionally - the node limit should be checked outside.
    bool TryIncreaseUsage(ui64 bytes);
    void IncreaseUsage(ui64 bytes);
    void DecreaseUsage(ui64 bytes);

    // Returns parent pool's 'fair-share' minus 'usage', negative on overuse
    i64 GetAvailability() const;

    const NHdrf::NDynamic::TQueryPtr Query;
};

} // namespace NKikimr::NKqp::NScheduler
