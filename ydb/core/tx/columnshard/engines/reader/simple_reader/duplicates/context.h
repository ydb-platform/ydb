#pragma once

#include "events.h"

#include <ydb/core/tx/conveyor_composite/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TInternalFilterConstructor: TMoveOnly {
private:
    const TEvRequestFilter::TPtr OriginalRequest;
    const std::shared_ptr<NGroupedMemoryManager::TProcessGuard> ProcessGuard;
    const std::shared_ptr<NGroupedMemoryManager::TScopeGuard> ScopeGuard;
    const std::shared_ptr<NGroupedMemoryManager::TGroupGuard> GroupGuard;
    YDB_READONLY_DEF(std::shared_ptr<NColumnShard::TDuplicateFilteringCounters>, Counters);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, PKSchema);
    NArrow::TSimpleRow MinPK;
    NArrow::TSimpleRow MaxPK;
    bool IsDone = false;

public:
    void SetFilter(NArrow::TColumnFilter&& filter) {
        OriginalRequest->Get()->GetSubscriber()->OnFilterReady(std::move(filter));
        IsDone = true;
    }

    void Abort(const TString& error) {
        OriginalRequest->Get()->GetSubscriber()->OnFailure(error);
        IsDone = true;
    }

    const TEvRequestFilter::TPtr& GetRequest() const {
        return OriginalRequest;
    }

    TInternalFilterConstructor(const TEvRequestFilter::TPtr& request, const TPortionInfo& portionInfo,
        const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters>& counters, const std::shared_ptr<arrow::Schema>& pkSchema);

    ~TInternalFilterConstructor() {
        AFL_VERIFY(IsDone);
    }

    ui64 GetMemoryProcessId() const {
        return ProcessGuard->GetProcessId();
    }
    ui64 GetMemoryScopeId() const {
        return ScopeGuard->GetProcessId();
    }
    ui64 GetMemoryGroupId() const {
        return GroupGuard->GetProcessId();
    }

    const NArrow::TSimpleRow& GetMinPK() const {
        return MinPK;
    }
    const NArrow::TSimpleRow& GetMaxPK() const {
        return MaxPK;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
