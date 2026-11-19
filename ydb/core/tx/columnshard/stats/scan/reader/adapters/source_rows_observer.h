#pragma once

#include <ydb/core/tx/columnshard/stats/scan/observation.h>

#include <optional>

namespace NKikimr::NOlap::NStats::NScan {

namespace NPrivate {

template <class TCollection>
std::optional<ui64> KnownRows(const TCollection& table) {
    const auto& filter = table.GetFilter();
    if (!table.GetFilterUsage() && !filter.IsTotalAllowFilter()) {
        const auto rows = filter.GetFilteredCountIfKnown();
        return rows ? std::optional<ui64>(*rows) : std::nullopt;
    }
    const auto rows = table.GetRecordsCountActualOptional();
    return rows ? std::optional<ui64>(*rows) : std::nullopt;
}

}   // namespace NPrivate

template <class TSource>
class TSourceRowsObserver {
    const TSource& Source;

    void OnRows(TOperationScope& scope, bool input) const {
        if (!scope || !Source.HasStageData()) {
            return;
        }
        const auto rows = NPrivate::KnownRows(Source.GetStageData().GetTable());
        if (!rows) {
            return;
        }
        scope.Observe([&](auto& observation) {
            auto& metric = input ? observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows)
                                 : observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows);
            metric.SetValue(*rows);
        });
    }

public:
    explicit TSourceRowsObserver(const TSource& source)
        : Source(source)
    {
    }

    void OnInputRows(TOperationScope& scope) const {
        OnRows(scope, true);
    }

    void OnOutputRows(TOperationScope& scope) const {
        OnRows(scope, false);
    }

    void OnStageResult(TOperationScope& scope) const {
        if (!scope || !Source.HasStageResult()) {
            return;
        }
        const auto& result = Source.GetStageResult();
        std::optional<ui64> rows;
        if (result.IsEmpty()) {
            rows = 0;
        } else if (const auto& batch = result.GetBatch()) {
            const auto& filter = result.GetNotAppliedFilter();
            if (filter && !filter->IsTotalAllowFilter()) {
                if (const auto count = filter->GetFilteredCountIfKnown()) {
                    rows = *count;
                }
            } else {
                rows = batch->num_rows();
            }
        }
        if (rows) {
            scope.Observe([&](auto& observation) {
                observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows).SetValue(*rows);
            });
        }
    }
};

}   // namespace NKikimr::NOlap::NStats::NScan
