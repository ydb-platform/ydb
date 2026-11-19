#pragma once

#include <ydb/core/tx/columnshard/stats/scan/observation.h>

#include <ydb/library/accessor/accessor.h>

#include <util/string/builder.h>

#include <algorithm>
#include <vector>

namespace NKikimr::NOlap::NStats::NScan {

class TIndexRegistry {
    TLocalStats& Local;
    YDB_READONLY_DEF(std::vector<ui64>, ResourceTargets);
    ui32 NextApplicationOrder = 0;

public:
    explicit TIndexRegistry(TLocalStats& local)
        : Local(local)
    {
    }

    TOperationNode* Register(const NColumnShardStats::TIndexDescription& index) {
        if (auto* node = Find(index.GetIndexId())) {
            return node;
        }
        NColumnShardStats::TOperation definition;
        definition.SetKey(TStringBuilder() << "IndexFilter:" << index.GetIndexId());
        definition.SetName("IndexFilter");
        *definition.MutableIndexFilter()->MutableIndex() = index;
        definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
        definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
        return Local.AddChild(Local.GetRoot(), definition);
    }

    TOperationNode* Find(ui64 indexId) const noexcept {
        return Local.GetRoot().FindChildIf([&](const auto& operation) noexcept {
            return operation.HasIndexFilter() && operation.GetIndexFilter().GetIndex().GetIndexId() == indexId;
        });
    }

    void ResetResourceTargets() noexcept {
        ResourceTargets.clear();
    }

    void AddResourceTarget(ui64 indexId) {
        if (std::find(ResourceTargets.begin(), ResourceTargets.end(), indexId) == ResourceTargets.end()) {
            ResourceTargets.push_back(indexId);
            std::sort(ResourceTargets.begin(), ResourceTargets.end());
        }
    }

    ui32 TakeApplicationOrder() noexcept {
        return NextApplicationOrder++;
    }
};

}   // namespace NKikimr::NOlap::NStats::NScan
