#pragma once

#include "events.h"

#include <ydb/core/tx/columnshard/column_fetching/manager.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TFilterBuildingGuard: TMoveOnly {
private:
    const std::shared_ptr<NGroupedMemoryManager::TProcessGuard> ProcessGuard;
    const std::shared_ptr<NGroupedMemoryManager::TScopeGuard> ScopeGuard;
    const std::shared_ptr<NGroupedMemoryManager::TGroupGuard> GroupGuard;

    static std::vector<std::shared_ptr<NGroupedMemoryManager::TStageFeatures>> GetStageFeatures() {
        static const std::vector<std::shared_ptr<NGroupedMemoryManager::TStageFeatures>> StageFeatures = {
            NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildStageFeatures("FILTERS", 2000000000),   // 2 GiB
            NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildStageFeatures("ACCESSORS", 100000000),   // 100 MiB
            NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildStageFeatures("COLUMN_DATA", 1000000000),   // 1 GiB
        };
        return StageFeatures;
    }

public:
    ui64 GetMemoryProcessId() const {
        return ProcessGuard->GetProcessId();
    }
    ui64 GetMemoryScopeId() const {
        return ScopeGuard->GetScopeId();
    }
    ui64 GetMemoryGroupId() const {
        return GroupGuard->GetGroupId();
    }

    TFilterBuildingGuard();
};

class TJobStatus {
public:
    class TResultInFlightGuard: public TMoveOnly {
    private:
        std::shared_ptr<TJobStatus> Owner;

    public:
        TResultInFlightGuard(const std::shared_ptr<TJobStatus>& owner)
            : Owner(owner)
        {
            AFL_VERIFY(Owner);
            Owner->ResultsInFlight.Inc();
        }

        TResultInFlightGuard(TResultInFlightGuard&& other) = default;
        TResultInFlightGuard& operator=(TResultInFlightGuard&& other) = default;

        ~TResultInFlightGuard() {
            if (Owner) {
                Owner->ResultsInFlight.Dec();
            }
        }
    };

private:
    std::atomic_bool IsDoneFlag = false;
    TAtomicCounter ResultsInFlight;

public:
    bool IsDone() const {
        return IsDoneFlag.load() && !ResultsInFlight.Val();
    }

    void OnDone() {
        AFL_VERIFY(!IsDoneFlag.exchange(true));
    }
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
    YDB_READONLY_DEF(std::shared_ptr<TJobStatus>, Status);
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> SelfMemory;

public:
    TBuildFilterContext(const TActorId owner, const std::shared_ptr<const TAtomicCounter>& abortionFlag, const TSnapshot& maxVersion,
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
        , Status(std::make_shared<TJobStatus>())
        , SelfMemory(contextMemory)
    {
        AFL_VERIFY(Owner);
        AFL_VERIFY(RequiredPortions.size());
        AFL_VERIFY(Columns.size());
        AFL_VERIFY(PKSchema);
        AFL_VERIFY(SnapshotSchema);
        AFL_VERIFY(ColumnDataManager);
        AFL_VERIFY(DataAccessorsManager);
        AFL_VERIFY(Counters);
        AFL_VERIFY(SelfMemory);
    }

    TBuildFilterContext(TBuildFilterContext&& other) = default;
    TBuildFilterContext& operator=(TBuildFilterContext&& other) = default;

    TJobStatus::TResultInFlightGuard MakeResultInFlightGuard() const {
        return TJobStatus::TResultInFlightGuard(Status);
    }

    std::set<ui32> GetFetchingColumnIds() const {
        std::set<ui32> columnsToFetch;
        for (const auto& [columnId, _] : Columns) {
            columnsToFetch.emplace(columnId);
        }
        return columnsToFetch;
    }

    TString DebugString() const {
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

    ui64 GetDataSize() const {
        return RequiredPortions.size() * (sizeof(ui64) + sizeof(TPortionInfo::TConstPtr));
    }

    TPortionInfo::TConstPtr GetPortion(const ui64 portionId) const {
        auto* findPortion = RequiredPortions.FindPtr(portionId);
        AFL_VERIFY(findPortion)("id", portionId)("context", DebugString());
        return *findPortion;
    }

    const TSnapshot& GetMaxVersion() const {
        return MaxVersion;
    }

    ~TBuildFilterContext() {
        if (Status) {
            Status->OnDone();
        }
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
