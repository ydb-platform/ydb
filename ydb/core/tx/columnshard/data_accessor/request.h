#pragma once
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap {

class TDataAccessorsRequest;

class TDataAccessorsResult {
private:
    THashMap<ui64, TString> ErrorsByPathId;
    THashMap<ui64, std::vector<TPortionDataAccessor>> AccessorsByPathId;
    THashMap<ui64, TPortionDataAccessor> PortionsById;
    std::vector<TPortionDataAccessor> Portions;

public:
    const std::vector<TPortionDataAccessor>& GetPortions() const {
        return Portions;
    }

    void Merge(TDataAccessorsResult&& result) {
        for (auto&& i : result.ErrorsByPathId) {
            AFL_VERIFY(ErrorsByPathId.emplace(i.first, i.second).second);
        }
        for (auto&& i : result.AccessorsByPathId) {
            AFL_VERIFY(AccessorsByPathId.emplace(i.first, std::move(i.second)).second);
        }
        for (auto&& i : result.PortionsById) {
            AFL_VERIFY(PortionsById.emplace(i.first, std::move(i.second)).second);
        }
        Portions.insert(Portions.end(), result.Portions.begin(), result.Portions.end());
    }

    const TPortionDataAccessor& GetPortionAccessorVerified(const ui64 portionId) const {
        auto it = PortionsById.find(portionId);
        AFL_VERIFY(it != PortionsById.end());
        return it->second;
    }

    std::vector<TPortionDataAccessor> ExtractPortionsVector() {
        return std::move(Portions);
    }

    void AddData(const ui64 pathId, THashMap<ui64, TPortionDataAccessor>&& accessors) {
        auto info = AccessorsByPathId.emplace(pathId, std::vector<TPortionDataAccessor>());
        AFL_VERIFY(info.second);
        auto& v = info.first->second;
        for (auto&& [portionId, i] : accessors) {
            v.emplace_back(std::move(i));
            AFL_VERIFY(PortionsById.emplace(portionId, v.back()).second);
            Portions.emplace_back(v.back());
        }
    }

    void AddError(const ui64 pathId, const TString& errorMessage) {
        AFL_VERIFY(ErrorsByPathId.emplace(pathId, errorMessage).second);
    }

    bool HasErrors() const {
        return ErrorsByPathId.size();
    }
};

class IDataAccessorRequestsSubscriber: public NColumnShard::TMonitoringObjectsCounter<IDataAccessorRequestsSubscriber> {
private:
    THashSet<ui64> RequestIds;

    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) = 0;

    void OnRequestsFinished(TDataAccessorsResult&& result) {
        DoOnRequestsFinished(std::move(result));
    }

    void RegisterRequestId(const TDataAccessorsRequest& request);

    friend class TDataAccessorsRequest;
    std::optional<TDataAccessorsResult> Result;

public:
    void OnResult(const ui32 requestId, TDataAccessorsResult&& result) {
        AFL_VERIFY(RequestIds.erase(requestId));
        if (!Result) {
            Result = std::move(result);
        } else {
            Result->Merge(std::move(result));
        }
        if (RequestIds.empty()) {
            OnRequestsFinished(std::move(*Result));
        }
    }

    virtual ~IDataAccessorRequestsSubscriber() = default;
};

class TFakeDataAccessorsSubscriber: public IDataAccessorRequestsSubscriber {
private:
    virtual void DoOnRequestsFinished(TDataAccessorsResult&& /*result*/) override {
    }
};

class TPathFetchingState {
public:
    enum class EFetchStage {
        Preparing,
        Fetching,
        Error,
        Fetched
    };

private:
    const ui64 PathId;

    YDB_READONLY(EFetchStage, Stage, EFetchStage::Preparing);
    YDB_READONLY_DEF(TString, ErrorMessage);
    THashMap<ui64, TPortionInfo::TConstPtr> Portions;
    THashMap<ui64, TPortionDataAccessor> PortionAccessors;

public:
    TString DebugString() const {
        TStringBuilder sb;
        sb << "portions_count=" << Portions.size();
        return sb;
    }

    TPathFetchingState(const ui64 pathId)
        : PathId(pathId) {
    }

    const THashMap<ui64, TPortionInfo::TConstPtr>& GetPortions() const {
        return Portions;
    }

    bool IsFinished() {
        return Portions.empty() || Stage == EFetchStage::Error;
    }

    THashMap<ui64, TPortionDataAccessor>&& DetachAccessors() {
        return std::move(PortionAccessors);
    }

    void AddPortion(const TPortionInfo::TConstPtr& portion) {
        AFL_VERIFY(Stage == EFetchStage::Preparing);
        AFL_VERIFY(portion->GetPathId() == PathId);
        AFL_VERIFY(Portions.emplace(portion->GetPortionId(), portion).second);
    }

    void AddAccessor(const TPortionDataAccessor& accessor) {
        AFL_VERIFY(Stage == EFetchStage::Fetching);
        AFL_VERIFY(Portions.erase(accessor.GetPortionInfo().GetPortionId()));
        AFL_VERIFY(PortionAccessors.emplace(accessor.GetPortionInfo().GetPortionId(), accessor).second);
        if (Portions.empty()) {
            AFL_VERIFY(Stage == EFetchStage::Fetching);
            Stage = EFetchStage::Fetched;
        }
    }
    void StartFetch() {
        AFL_VERIFY(Stage == EFetchStage::Preparing);
        Stage = EFetchStage::Fetching;
        AFL_VERIFY(Portions.size());
    }

    void OnError(const TString& errorMessage) {
        AFL_VERIFY(Stage == EFetchStage::Fetching);
        Stage = EFetchStage::Error;
        ErrorMessage = errorMessage;
    }
};

class TDataAccessorsRequest: public NColumnShard::TMonitoringObjectsCounter<TDataAccessorsRequest> {
private:
    static inline TAtomicCounter Counter = 0;
    ui32 FetchStage = 0;
    YDB_READONLY(ui64, RequestId, Counter.Inc());
    THashSet<ui64> PortionIds;
    THashMap<ui64, TPathFetchingState> PathIdStatus;
    THashSet<ui64> PathIds;
    TDataAccessorsResult AccessorsByPathId;
    std::optional<std::vector<ui32>> ColumnIds;
    std::optional<std::vector<ui32>> IndexIds;

    TAtomicCounter PreparingCount = 0;
    TAtomicCounter FetchingCount = 0;
    TAtomicCounter ReadyCount = 0;

    std::shared_ptr<IDataAccessorRequestsSubscriber> Subscriber;

    void CheckReady() {
        if (PathIdStatus.size()) {
            return;
        }
        AFL_VERIFY(!PreparingCount.Val());
        AFL_VERIFY(!FetchingCount.Val());
        FetchStage = 2;
        Subscriber->OnResult(RequestId, std::move(AccessorsByPathId));
        Subscriber = nullptr;
    }

public:
    TString DebugString() const {
        TStringBuilder sb;
        sb << "request_id=" << RequestId << ";";
        for (auto&& i : PathIdStatus) {
            sb << i.first << "={" << i.second.DebugString() << "};";
        }
        return sb;
    }

    TDataAccessorsRequest() = default;

    ui64 PredictAccessorsMemory(const ISnapshotSchema::TPtr& schema) const {
        ui64 result = 0;
        for (auto&& i : PathIdStatus) {
            for (auto&& [_, p] : i.second.GetPortions()) {
                result += p->PredictAccessorsMemory(schema);
            }
        }
        return result;
    }

    bool HasSubscriber() const {
        return !!Subscriber;
    }

    ui32 GetSize() const {
        return PortionIds.size();
    }

    const THashSet<ui64>& GetPathIds() const {
        return PathIds;
    }

    bool IsEmpty() const {
        return PortionIds.empty();
    }

    void RegisterSubscriber(const std::shared_ptr<IDataAccessorRequestsSubscriber>& subscriber) {
        AFL_VERIFY(!Subscriber);
        AFL_VERIFY(FetchStage == 0);
        Subscriber = subscriber;
        Subscriber->RegisterRequestId(*this);
    }

    const THashMap<ui64, TPortionInfo::TConstPtr>& StartFetching(const ui64 pathId) {
        AFL_VERIFY(!!Subscriber);
        AFL_VERIFY(FetchStage <= 1);
        FetchStage = 1;

        auto it = PathIdStatus.find(pathId);
        AFL_VERIFY(it != PathIdStatus.end());
        it->second.StartFetch();

        FetchingCount.Inc();
        AFL_VERIFY(PreparingCount.Dec() >= 0);

        return it->second.GetPortions();
    }

    void AddPortion(const TPortionInfo::TConstPtr& portion) {
        AFL_VERIFY(portion);
        AFL_VERIFY(FetchStage <= 1);
        AFL_VERIFY(PortionIds.emplace(portion->GetPortionId()).second);
        PathIds.emplace(portion->GetPathId());
        auto it = PathIdStatus.find(portion->GetPathId());
        if (it == PathIdStatus.end()) {
            PreparingCount.Inc();
            it = PathIdStatus.emplace(portion->GetPathId(), portion->GetPathId()).first;
        }
        it->second.AddPortion(portion);
    }

    bool IsFetched() const {
        return FetchStage == 2;
    }

    void AddError(const ui64 pathId, const TString& errorMessage) {
        AFL_VERIFY(FetchStage == 1);
        auto itStatus = PathIdStatus.find(pathId);
        AFL_VERIFY(itStatus != PathIdStatus.end());
        itStatus->second.OnError(errorMessage);
        PathIdStatus.erase(itStatus);
        AFL_VERIFY(FetchingCount.Dec() >= 0);
        ReadyCount.Inc();
        AccessorsByPathId.AddError(pathId, errorMessage);
        CheckReady();
    }

    void AddAccessor(const TPortionDataAccessor& accessor) {
        AFL_VERIFY(FetchStage == 1);
        auto pathId = accessor.GetPortionInfo().GetPathId();
        {
            auto itStatus = PathIdStatus.find(pathId);
            AFL_VERIFY(itStatus != PathIdStatus.end());
            itStatus->second.AddAccessor(accessor);
            if (itStatus->second.IsFinished()) {
                AFL_VERIFY(FetchingCount.Dec() >= 0);
                ReadyCount.Inc();
                AccessorsByPathId.AddData(pathId, itStatus->second.DetachAccessors());
                PathIdStatus.erase(itStatus);
            }
        }
        CheckReady();
    }

    void AddData(THashMap<ui64, std::vector<TPortionDataAccessor>>&& accessors) {
        for (auto&& i : accessors) {
            for (auto&& a : i.second) {
                AddAccessor(std::move(a));
            }
        }
    }
};

}   // namespace NKikimr::NOlap
