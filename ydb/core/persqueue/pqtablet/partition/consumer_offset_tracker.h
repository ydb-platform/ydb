#pragma once

#include <ydb/core/persqueue/events/internal.h>
#include <util/datetime/base.h>
#include <vector>

namespace NKikimr::NPQ {

    class TImportantConsumerOffsetTracker {
    public:
        struct TConsumerOffset {
            TDuration AvailabilityPeriod = TDuration::Max();
            ui64 Offset = 0;
        };

        explicit TImportantConsumerOffsetTracker(std::vector<TConsumerOffset> consumers);

        bool ShouldKeepCurrentKey(const TDataKey& currentKey, const TDataKey& nextKey, const TInstant now) const;

    private:
        std::vector<TConsumerOffset> Consumers_;
    };

    bool ImportantConsumerNeedToKeepCurrentKey(const TDuration availabilityPeriod, const ui64 offset, const TDataKey& currentKey, const TDataKey& nextKey, const TInstant now);

} // namespace NKikimr::NPQ
