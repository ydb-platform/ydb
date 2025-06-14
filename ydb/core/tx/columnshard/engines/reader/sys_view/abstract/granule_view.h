#pragma once
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap::NReader::NSysView::NAbstract {

class TGranuleMetaView {
private:
    using TPortions = std::deque<std::shared_ptr<TPortionInfo>>;
    YDB_READONLY_DEF(NColumnShard::TUnifiedPathId, PathId);
    YDB_READONLY_DEF(TPortions, Portions);
    YDB_READONLY_DEF(std::vector<NStorageOptimizer::TTaskDescription>, OptimizerTasks);
public:
    TGranuleMetaView(const TGranuleMeta& granule, const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId, const bool reverse, const TSnapshot& reqSnapshot)
        : PathId({granule.GetPathId(), schemeShardLocalPathId})
    {
        for (auto&& i : granule.GetPortions()) {
            if (i.second->IsRemovedFor(reqSnapshot)) {
                continue;
            }
            Portions.emplace_back(i.second);
        }

        const auto predSort = [](const std::shared_ptr<TPortionInfo>& l, const std::shared_ptr<TPortionInfo>& r) {
            return l->GetPortionId() < r->GetPortionId();
        };

        std::sort(Portions.begin(), Portions.end(), predSort);
        if (reverse) {
            std::reverse(Portions.begin(), Portions.end());
        }
    }

    void FillOptimizerTasks(const TGranuleMeta& granule, const bool reverse) {
        OptimizerTasks = granule.GetOptimizerTasksDescription();
        std::sort(OptimizerTasks.begin(), OptimizerTasks.end());
        if (reverse) {
            std::reverse(OptimizerTasks.begin(), OptimizerTasks.end());
        }
    }

    bool operator<(const TGranuleMetaView& item) const {
        return PathId.SchemeShardLocalPathId < item.PathId.SchemeShardLocalPathId;
    }

    std::shared_ptr<TPortionInfo> PopFrontPortion() {
        if (Portions.empty()) {
            return nullptr;
        }
        auto result = Portions.front();
        Portions.pop_front();
        return result;
    }
};

}
