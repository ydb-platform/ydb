#pragma once

#include "context.h"
#include "events.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/common.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering::NPrivate {

class TEvFilterConstructionResult
    : public NActors::TEventLocal<TEvFilterConstructionResult, NColumnShard::TEvPrivate::EvFilterConstructionResult> {
private:
    using TFilters = THashMap<TDuplicateMapInfo, NArrow::TColumnFilter>;
    TConclusion<TFilters> Result;

public:
    TEvFilterConstructionResult(TConclusion<TFilters>&& result)
        : Result(std::move(result))
    {
        if (Result.IsSuccess()) {
            for (const auto& [info, filter] : *Result) {
                AFL_VERIFY(!!filter.GetRecordsCount() && filter.GetRecordsCountVerified() == info.GetRowsCount())(
                                                                                             "filter", filter.GetRecordsCount().value_or(0))(
                                                                                             "info", info.DebugString());
            }
        }
    }

    const TConclusion<TFilters>& GetConclusion() const {
        return Result;
    }

    TFilters&& ExtractResult() {
        return Result.DetachResult();
    }
};

class TEvDuplicateSourceCacheResult
    : public NActors::TEventLocal<TEvDuplicateSourceCacheResult, NColumnShard::TEvPrivate::EvDuplicateSourceCacheResult> {
private:
    using TDataBySource = THashMap<ui64, std::shared_ptr<TColumnsData>>;
    TConclusion<TDataBySource> ColumnData;
    YDB_READONLY_DEF(std::shared_ptr<TInternalFilterConstructor>, Context);

public:
    TEvDuplicateSourceCacheResult(const std::shared_ptr<TInternalFilterConstructor>& context, TConclusion<TDataBySource>&& data)
        : ColumnData(std::move(data))
        , Context(context)
    {
    }

    const TConclusion<TDataBySource>& GetConclusion() const {
        return ColumnData;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering::NPrivate
