#pragma once
#include <ydb/core/tx/columnshard/engines/storage/granule.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NOlap::NReader::NSysView::NAbstract {

class TGranuleMetaView {
private:
    using TPortions = std::deque<std::shared_ptr<TPortionInfo>>;
    YDB_READONLY(ui64, PathId, 0);
    YDB_READONLY_DEF(TPortions, Portions);
    YDB_READONLY_DEF(std::vector<NStorageOptimizer::TTaskDescription>, OptimizerTasks);
public:
    TGranuleMetaView(const TGranuleMeta& granule, const bool reverse)
        : PathId(granule.GetPathId())
    {
        for (auto&& i : granule.GetPortions()) {
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
        return PathId < item.PathId;
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
