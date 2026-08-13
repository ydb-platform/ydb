#pragma once

#include "common.h"
#include "private_events.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

// Immutable-ish config shared by the controller and merge tasks (no Merger / FiltersBuilder).
struct TMergeContext {
    const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> Counters;
    bool IsReversed;
    std::shared_ptr<TPortionStore> Portions;
    std::map<ui32, std::shared_ptr<arrow::Field>> FetchingColumns;
    std::shared_ptr<const TAtomicCounter> AbortionFlag;

    TMergeContext(std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> counters, const bool reversed,
        const std::shared_ptr<TPortionStore>& portions, const std::map<ui32, std::shared_ptr<arrow::Field>>& fetchingColumns,
        const std::shared_ptr<const TAtomicCounter>& abortionFlag);

    bool IsAborted() const {
        return AbortionFlag && AbortionFlag->Val();
    }
};

class TMergeBorders: public NConveyor::ITask {
private:
    TActorId Owner;
    std::shared_ptr<TMergeContext> Context;
    TMergeRuntimeState State;
    TEvBordersConstructionResult::TPtr Event;
    std::vector<NArrow::TSimpleRow> ReadyBorders;

private:
    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override;
    virtual void DoOnCannotExecute(const TString& reason) override;

    virtual TString GetTaskClassIdentifier() const override;

    void SendResult(THashMap<ui64, NArrow::TColumnFilter>&& readyFilters, TConclusionStatus&& conclusion);

public:
    TMergeBorders(const TActorId& owner, const std::shared_ptr<TMergeContext>& context, TMergeRuntimeState&& state,
        const TEvBordersConstructionResult::TPtr& event, const std::vector<NArrow::TSimpleRow>& readyBorders);
};

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
