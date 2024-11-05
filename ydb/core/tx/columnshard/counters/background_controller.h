#pragma once

#include <ydb/core/protos/table_stats.pb.h>
#include <util/datetime/base.h>
#include <util/generic/hash.h>

namespace NKikimr::NColumnShard {

class TBackgroundControllerCounters {
private:
    THashMap<ui64, TInstant> LastCompactionFinishByPathId;
    TInstant LastCompactionFinish;

public:
    void OnCompactionFinish(ui64 pathId);

    void FillStats(ui64 pathId, ::NKikimrTableStats::TTableStats& output) const {
        output.SetLastFullCompactionTs(GetLastCompactionFinishInstant(pathId).value_or(TInstant::Zero()).Seconds());
    }

    void FillTotalStats(::NKikimrTableStats::TTableStats& output) const {
        output.SetLastFullCompactionTs(LastCompactionFinish.Seconds());
    }

private:
    std::optional<TInstant> GetLastCompactionFinishInstant(const ui64 pathId) const {
        auto findInstant = LastCompactionFinishByPathId.FindPtr(pathId);
        if (!findInstant) {
            return std::nullopt;
        }
        return *findInstant;
    }
};

} // namespace NKikimr::NColumnShard
