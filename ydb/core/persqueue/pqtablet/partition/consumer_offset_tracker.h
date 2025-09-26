#pragma once

#include <util/datetime/base.h>
#include <vector>
#include <ydb/core/persqueue/events/internal.h>

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
        static bool ShouldKeep(const TDataKey& currentKey, const TDataKey& nextKey, const TInstant now, const TConsumerOffset& consumer);

    private:
        std::vector<TConsumerOffset> Consumers_;
    };

} // namespace NKikimr::NPQ
