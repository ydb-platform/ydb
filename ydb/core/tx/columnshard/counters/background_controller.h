#pragma once

#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/base/appdata_fwd.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr::NColumnShard {

class TBackgroundControllerCounters {
private:
    THashMap<ui64, TInstant> LastCompactionFinishByPathId;
    TInstant LastCompactionFinish;

public:
    void OnCompactionFinish(ui64 pathId) {
        TInstant now = TAppData::TimeProvider->Now();
        TInstant& lastFinish = LastCompactionFinishByPathId[pathId];
        lastFinish = std::max(lastFinish, now);

        if (LastCompactionFinish < now) {
            LastCompactionFinish = now;
        }
    }

    TInstant GetLastCompactionFinishInstant(ui64 pathId) const {
        auto findInstant = LastCompactionFinishByPathId.FindPtr(pathId);
        if (!findInstant) {
            return TInstant::Zero();
        }
        return *findInstant;
    }

    void FillStats(ui64 pathId, ::NKikimrTableStats::TTableStats& output) const {
        output.SetLastFullCompactionTs(GetLastCompactionFinishInstant(pathId).Seconds());
    }

    void FillTotalStats(::NKikimrTableStats::TTableStats& output) const {
        output.SetLastFullCompactionTs(LastCompactionFinish.Seconds());
    }
};

} // namespace NKikimr::NColumnShard
