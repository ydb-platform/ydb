#pragma once

#include <ydb/core/base/logoblob.h>

#include <util/generic/function_ref.h>

#include <compare>
#include <map>
#include <optional>
#include <utility>

namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals {

struct THistoryIntervalKey {
    ui32 Channel = 0;
    ui32 From = 0;

    auto operator<=>(const THistoryIntervalKey&) const = default;
};

struct THistoryInterval {
    ui32 To = 0;
    ui32 Group = 0;
};

class THistoryIntervals {
public:
    using TIntervals = std::map<THistoryIntervalKey, THistoryInterval>;

private:
    ui64 TabletId;
    TIntervals Intervals;

public:
    // Intervals for each channel are disjoint half-open generation ranges from tablet history.
    THistoryIntervals(ui64 tabletId, TIntervals intervals);

    // The caller supplies physical BlobStorage references, excluding external-storage blobs.
    void Exclude(const TLogoBlobID& blobId, std::optional<ui32> actualGroup = {}, TFunctionRef<void()> onGroupMismatch = [] {});

    const TIntervals& GetIntervals() const {
        return Intervals;
    }

    TIntervals ExtractIntervals() {
        return std::move(Intervals);
    }

    bool Empty() const {
        return Intervals.empty();
    }
};

}   // namespace NKikimr::NColumnShard::NFindEmptyHistoryIntervals
