#pragma once

#include "config.h"
#include "pdiskid.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <util/datetime/base.h>
#include <util/generic/set.h>

namespace NKikimr::NCms {

using NTabletFlatExecutor::TTransactionContext;

class TLockableItem;

/**
 * Class to hold downtime of tracked item. Downtime is stored as a set
 * of downtime segments. Nearby segments are collapsed into a single segment
 * if gap doesn't exceed configured value. Ignored gap size can be configured
 * dynamically but already collapsed segments never split back.
 */
class TDowntime {
public:
    struct TSegment {
        TInstant Start;
        TInstant End;
        TString Reason;

        TSegment(TInstant start, TInstant end, const TString reason = "")
            : Start(start)
            , End(end)
            , Reason(reason)
        {
        }

        TSegment(TInstant start, TDuration duration, const TString reason = "")
            : Start(start)
            , End(start + duration)
            , Reason(reason)
        {
        }

        TSegment(const TSegment &other) = default;
        TSegment(TSegment &&other) = default;

        TSegment &operator=(const TSegment &other) = default;
        TSegment &operator=(TSegment &&other) = default;

        TDuration Duration() const {
            return End - Start;
        }
    };

    struct TSegmentCmp {
        bool operator()(const TSegment &l, const TSegment &r) const {
            if (l.Start != r.Start)
                return l.Start < r.Start;
            else if (l.End != r.End)
                return l.End < r.End;
            else
                return l.Reason < r.Reason;
        }
    };

    using TSegments = TSet<TSegment, TSegmentCmp>;

public:
    TDowntime(TDuration ignoredDowntimeGap = TDuration::Zero());

    void AddDowntime(TInstant start, TInstant end, const TString &reason = "");
    void AddDowntime(TInstant start, TDuration duration, const TString &reason = "");
    void AddDowntime(const TSegment segment);
    void AddDowntime(const TDowntime &downtime);
    void AddDowntime(const TLockableItem &item, TInstant now);

    const TSegments &GetDowntimeSegments() const {
        return DowntimeSegments;
    }

    bool Empty() const {
        return DowntimeSegments.empty();
    }

    void Clear() {
        DowntimeSegments.clear();
    }

    bool HasUpcomingDowntime(TInstant now, TDuration distance, TDuration duration) const;

    TDuration GetIgnoredDowntimeGap() const;
    void SetIgnoredDowntimeGap(TDuration gap);
    void CleanupOldSegments(TInstant now);

    void Serialize(NKikimrCms::TAvailabilityStats *rec) const;
    void Deserialize(const NKikimrCms::TAvailabilityStats &rec);

private:
    std::pair<TSegments::iterator, bool> CollapseWithPrev(TSegments::iterator it);

private:
    TDuration IgnoredDowntimeGap;
    TSegments DowntimeSegments;
};

class TDowntimes {
public:
    TDowntimes();

    void SetIgnoredDowntimeGap(TDuration gap);
    void CleanupOld(TInstant now);
    void CleanupEmpty();

    bool DbLoadState(TTransactionContext& txc, const TActorContext& ctx);
    void DbStoreState(TTransactionContext& txc, const TActorContext& ctx);

    THashMap<ui32, TDowntime> NodeDowntimes;
    THashMap<TPDiskID, TDowntime, TPDiskIDHash> PDiskDowntimes;

private:
    TDuration IgnoredDowntimeGap;
};

} // namespace NKikimr::NCms
