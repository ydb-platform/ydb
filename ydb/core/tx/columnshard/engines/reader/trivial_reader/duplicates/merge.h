#pragma once

#include "common.h"
#include "private_events.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

struct TMergeContext {
    std::unique_ptr<NArrow::NMerger::TMergePartialStream> Merger;
    TFiltersBuilder FiltersBuilder;
    const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> Counters;
    ui64 PrevRowsAdded = 0;
    ui64 PrevRowsSkipped = 0;
    bool IsReversed;
    std::shared_ptr<TPortionStore> Portions;
    std::map<ui32, std::shared_ptr<arrow::Field>> FetchingColumns;

    TMergeContext(std::unique_ptr<NArrow::NMerger::TMergePartialStream>&& merger, std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> counters, const bool reversed, const std::shared_ptr<TPortionStore>& portions, const std::map<ui32, std::shared_ptr<arrow::Field>>& fetchingColumns);
};

class TMergeBorders: public NConveyor::ITask {
private:
    TActorId Owner;
    std::shared_ptr<TMergeContext> Context;
    TEvBordersConstructionResult::TPtr Event;
    std::vector<NArrow::TSimpleRow> ReadyBorders;

private:
    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override;
    virtual void DoOnCannotExecute(const TString& reason) override;

    virtual TString GetTaskClassIdentifier() const override;
public:
    TMergeBorders(const TActorId& owner, const std::shared_ptr<TMergeContext>& context, const TEvBordersConstructionResult::TPtr& event, const std::vector<NArrow::TSimpleRow>& readyBorders);
};

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
