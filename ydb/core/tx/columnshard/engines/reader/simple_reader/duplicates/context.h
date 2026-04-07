#pragma once

#include <ydb/core/tx/columnshard/column_fetching/manager.h>
#include <ydb/core/tx/columnshard/counters/duplicate_filtering.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TFilterBuildingGuard: TMoveOnly {
private:
    const std::shared_ptr<NGroupedMemoryManager::TProcessGuard> ProcessGuard;
    const std::shared_ptr<NGroupedMemoryManager::TScopeGuard> ScopeGuard;
    const std::shared_ptr<NGroupedMemoryManager::TGroupGuard> GroupGuard;

    static std::vector<std::shared_ptr<NGroupedMemoryManager::TStageFeatures>> GetStageFeatures();

public:
    ui64 GetMemoryProcessId() const;
    ui64 GetMemoryScopeId() const;
    ui64 GetMemoryGroupId() const;

    TFilterBuildingGuard();
};

class TBuildFilterContext: public NColumnShard::TMonitoringObjectsCounter<TBuildFilterContext>, public TMoveOnly {
private:
    using TFieldByColumn = std::map<ui32, std::shared_ptr<arrow::Field>>;
    using TPortionIndex = THashMap<ui64, TPortionInfo::TConstPtr>;
    YDB_READONLY_DEF(TActorId, Owner);
    YDB_READONLY_DEF(std::shared_ptr<const TAtomicCounter>, AbortionFlag);
    TSnapshot MaxVersion;
    TPortionIndex RequiredPortions;
    YDB_READONLY_DEF(TFieldByColumn, Columns);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, PKSchema);
    YDB_READONLY_DEF(std::shared_ptr<ISnapshotSchema>, SnapshotSchema);
    YDB_READONLY_DEF(std::shared_ptr<NColumnFetching::TColumnDataManager>, ColumnDataManager);
    YDB_READONLY_DEF(std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>, DataAccessorsManager);
    YDB_READONLY_DEF(std::shared_ptr<NColumnShard::TDuplicateFilteringCounters>, Counters);
    YDB_READONLY_DEF(std::unique_ptr<TFilterBuildingGuard>, RequestGuard);
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> SelfMemory;

public:
    TBuildFilterContext(const TActorId owner, const std::shared_ptr<const TAtomicCounter>& abortionFlag, const TSnapshot& maxVersion,
        TPortionIndex&& portions, const TFieldByColumn& columns, const std::shared_ptr<arrow::Schema>& pkSchema,
        const std::shared_ptr<ISnapshotSchema>& snapshotSchema, const std::shared_ptr<NColumnFetching::TColumnDataManager>& columnDataManager,
        const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
        const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters>& counters, std::unique_ptr<TFilterBuildingGuard>&& requestGuard,
        const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& contextMemory);

    TBuildFilterContext(TBuildFilterContext&& other) = default;
    TBuildFilterContext& operator=(TBuildFilterContext&& other) = default;

    std::set<ui32> GetFetchingColumnIds() const;
    TString DebugString() const;
    ui64 GetDataSize() const;
    TPortionInfo::TConstPtr GetPortion(const ui64 portionId) const;
    const TSnapshot& GetMaxVersion() const;
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
