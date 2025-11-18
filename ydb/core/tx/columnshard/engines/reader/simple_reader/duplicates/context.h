#pragma once

#include "events.h"

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
            NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildStageFeatures("INTERSECTIONS", 10000000),   // 10 MiB
            NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildStageFeatures("ACCESSORS", 100000000),   // 100 MiB
            NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildStageFeatures("COLUMN_DATA", 10000000000),   // 10 GiB
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

class TFilterAccumulator: TMoveOnly {
public:
    enum class EFetchingStage {
        INTERSECTIONS = 0,
        ACCESSORS = 1,
        COLUMN_DATA = 2,
    };

private:
    const TEvRequestFilter::TPtr OriginalRequest;
    bool Done = false;

    std::vector<std::optional<NArrow::TColumnFilter>> Filters;
    ui64 FiltersAccumulated = 0;

private:
    bool IsReady() const {
        if (Filters.empty()) {
            return false;
        }
        return Filters.size() == FiltersAccumulated;
    }

    void Complete() {
        AFL_VERIFY(!IsDone());
        AFL_VERIFY(IsReady());
        NArrow::TColumnFilter result = NArrow::TColumnFilter::BuildAllowFilter();
        for (const auto& filter : Filters) {
            AFL_VERIFY(!!filter);
            result.Append(*filter);
        }
        OriginalRequest->Get()->GetSubscriber()->OnFilterReady(std::move(result));
        Done = true;
        AFL_VERIFY(IsDone());
    }

public:
    void SetIntervalsCount(const ui32 cnt) {
        AFL_VERIFY(Filters.empty());
        AFL_VERIFY(cnt);
        Filters.resize(cnt);
    }

    void AddFilter(const ui32 intervalIdx, const NArrow::TColumnFilter& filterExt) {
        AFL_VERIFY(!IsDone());
        AFL_VERIFY(intervalIdx < Filters.size());
        AFL_VERIFY(!Filters[intervalIdx]);
        Filters[intervalIdx].emplace(filterExt);
        ++FiltersAccumulated;
        if (IsReady()) {
            Complete();
        }
    }

    bool IsDone() const {
        return Done;
    }

    void Abort(const TString& error) {
        OriginalRequest->Get()->GetSubscriber()->OnFailure(error);
        Done = true;
    }

    const TEvRequestFilter::TPtr& GetRequest() const {
        return OriginalRequest;
    }

    TFilterAccumulator(const TEvRequestFilter::TPtr& request);

    ~TFilterAccumulator() {
        AFL_VERIFY(IsDone() || (OriginalRequest->Get()->GetAbortionFlag() && OriginalRequest->Get()->GetAbortionFlag()->Val()) || TActorSystem::IsStopped())("state", DebugString());
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "{";
        sb << "Portion=" << OriginalRequest->Get()->GetSourceId() << ";";
        sb << "ReadyIntervals=[";
        for (const auto& filter : Filters) {
            sb << (!!filter ? '1' : '.');
        }
        sb << "];";
        sb << "}";
        return sb;
    }

    ui64 GetDataSize() const {
        return Filters.capacity() * sizeof(std::optional<NArrow::TColumnFilter>);
    }
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

    static ui64 GetApproximateDataSize(const ui64 intersectionCount) {
        return intersectionCount *
               (sizeof(ui64) + sizeof(TPortionInfo::TConstPtr) + sizeof(TIntervalInfo) + sizeof(std::optional<NArrow::TColumnFilter>));
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
