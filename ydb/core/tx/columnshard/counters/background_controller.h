#pragma once

#include <ydb/core/protos/table_stats.pb.h>
#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NColumnShard {

class TBackgroundControllerCounters {
private:
    THashMap<TInternalPathId, TInstant> LastCompactionFinishByPathId;
    TInstant LastCompactionFinish;

public:
    void OnCompactionFinish(TInternalPathId pathId);

    void FillStats(TInternalPathId pathId, ::NKikimrTableStats::TTableStats& output) const {
        output.SetLastFullCompactionTs(GetLastCompactionFinishInstant(pathId).value_or(TInstant::Zero()).Seconds());
    }

    void FillTotalStats(::NKikimrTableStats::TTableStats& output) const {
        output.SetLastFullCompactionTs(LastCompactionFinish.Seconds());
    }

private:
    std::optional<TInstant> GetLastCompactionFinishInstant(const TInternalPathId pathId) const {
        auto findInstant = LastCompactionFinishByPathId.FindPtr(pathId);
        if (!findInstant) {
            return std::nullopt;
        }
        return *findInstant;
    }
};

} // namespace NKikimr::NColumnShard
