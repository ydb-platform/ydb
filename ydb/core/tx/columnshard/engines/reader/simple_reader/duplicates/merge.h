#pragma once

#include "common.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>

#include <ydb/library/actors/interconnect/types.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

class TBuildDuplicateFilters: public NConveyor::ITask {
private:
    THashMap<TDuplicateMapInfo, std::shared_ptr<TColumnsData>> SourcesById;
    std::shared_ptr<arrow::Schema> PKSchema;
    std::vector<std::string> VersionColumnNames;
    TActorId Owner;
    NColumnShard::TDuplicateFilteringCounters Counters;
    std::optional<NArrow::NMerger::TCursor> MaxVersion;
    NArrow::TSimpleRow Finish;
    bool IncludeFinish;

private:
    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override;
    virtual void DoOnCannotExecute(const TString& reason) override;

    virtual TString GetTaskClassIdentifier() const override {
        return "BUILD_DUPLICATE_FILTERS";
    }

public:
    TBuildDuplicateFilters(const std::shared_ptr<arrow::Schema>& sortingSchema, const std::optional<NArrow::NMerger::TCursor>& maxVersion,
        const NArrow::TSimpleRow& finish, const bool includeFinish, const NColumnShard::TDuplicateFilteringCounters& counters,
        const TActorId& owner)
        : PKSchema(sortingSchema)
        , VersionColumnNames(IIndexInfo::GetSnapshotColumnNames())
        , Owner(owner)
        , Counters(counters)
        , MaxVersion(maxVersion)
        , Finish(finish)
        , IncludeFinish(includeFinish) {
        AFL_VERIFY(finish.GetSchema()->Equals(sortingSchema));
    }

    void AddSource(const std::shared_ptr<TColumnsData>& batch, const TDuplicateMapInfo& interval) {
        AFL_VERIFY(interval.GetRows().NumRows());
        AFL_VERIFY(interval.GetRows().GetBegin() < batch->GetData()->GetRecordsCount())("interval", interval.DebugString())(
                                                     "records", batch->GetData()->GetRecordsCount());
        AFL_VERIFY(SourcesById.emplace(interval, batch).second);
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
