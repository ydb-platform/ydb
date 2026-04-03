#include "context.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

std::vector<std::shared_ptr<NGroupedMemoryManager::TStageFeatures>> TFilterBuildingGuard::GetStageFeatures() {
    static const std::vector<std::shared_ptr<NGroupedMemoryManager::TStageFeatures>> StageFeatures = {
        NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildStageFeatures("FILTERS", 2000000000),   // 2 GiB
        NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildStageFeatures("ACCESSORS", 100000000),   // 100 MiB
        NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildStageFeatures("COLUMN_DATA", 1000000000),   // 1 GiB
    };
    return StageFeatures;
}

ui64 TFilterBuildingGuard::GetMemoryProcessId() const {
    return ProcessGuard->GetProcessId();
}

ui64 TFilterBuildingGuard::GetMemoryScopeId() const {
    return ScopeGuard->GetScopeId();
}

ui64 TFilterBuildingGuard::GetMemoryGroupId() const {
    return GroupGuard->GetGroupId();
}

TFilterBuildingGuard::TFilterBuildingGuard()
    : ProcessGuard(NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildProcessGuard(GetStageFeatures()))
    , ScopeGuard(ProcessGuard->BuildScopeGuard(1))
    , GroupGuard(ScopeGuard->BuildGroupGuard())
{
}

TBuildFilterContext::TBuildFilterContext(const TActorId owner, const std::shared_ptr<const TAtomicCounter>& abortionFlag, const TSnapshot& maxVersion,
    TPortionIndex&& portions, const TFieldByColumn& columns, const std::shared_ptr<arrow::Schema>& pkSchema,
    const std::shared_ptr<ISnapshotSchema>& snapshotSchema, const std::shared_ptr<NColumnFetching::TColumnDataManager>& columnDataManager,
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
    const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters>& counters, std::unique_ptr<TFilterBuildingGuard>&& requestGuard,
    const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& contextMemory)
    : Owner(owner)
    , AbortionFlag(abortionFlag)
    , MaxVersion(maxVersion)
    , RequiredPortions(std::move(portions))
    , Columns(columns)
    , PKSchema(pkSchema)
    , SnapshotSchema(snapshotSchema)
    , ColumnDataManager(columnDataManager)
    , DataAccessorsManager(dataAccessorsManager)
    , Counters(counters)
    , RequestGuard(std::move(requestGuard))
    , SelfMemory(contextMemory)
{
    AFL_VERIFY(Owner);
    AFL_VERIFY(Columns.size());
    AFL_VERIFY(PKSchema);
    AFL_VERIFY(SnapshotSchema);
    AFL_VERIFY(ColumnDataManager);
    AFL_VERIFY(DataAccessorsManager);
    AFL_VERIFY(Counters);
    AFL_VERIFY(SelfMemory);
}

std::set<ui32> TBuildFilterContext::GetFetchingColumnIds() const {
    std::set<ui32> columnsToFetch;
    for (const auto& [columnId, _] : Columns) {
        columnsToFetch.emplace(columnId);
    }
    return columnsToFetch;
}

TString TBuildFilterContext::DebugString() const {
    TStringBuilder sb;
    sb << "{";
    sb << "portions=[";
    for (const auto& [id, _] : RequiredPortions) {
        sb << id << ";";
    }
    sb << "]";
    sb << "}";
    return sb;
}

ui64 TBuildFilterContext::GetDataSize() const {
    return RequiredPortions.size() * (sizeof(ui64) + sizeof(TPortionInfo::TConstPtr));
}

TPortionInfo::TConstPtr TBuildFilterContext::GetPortion(const ui64 portionId) const {
    auto* findPortion = RequiredPortions.FindPtr(portionId);
    AFL_VERIFY(findPortion)("id", portionId)("context", DebugString());
    return *findPortion;
}

const TSnapshot& TBuildFilterContext::GetMaxVersion() const {
    return MaxVersion;
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
