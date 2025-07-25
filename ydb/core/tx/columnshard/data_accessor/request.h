#pragma once
#include "cache_policy/policy.h"

#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>

#include <ydb/library/signals/object_counter.h>

namespace NKikimr::NOlap {

class TDataAccessorsRequest;

class TDataAccessorsResult: private NNonCopyable::TMoveOnly {
private:
    using TErrorByPathId = THashMap<TInternalPathId, TString>;
    YDB_READONLY_DEF(TErrorByPathId, ErrorsByPathId);
    THashMap<ui64, std::shared_ptr<TPortionDataAccessor>> PortionsById;

public:
    TDataAccessorsResult() = default;

    TDataAccessorsResult(std::vector<std::shared_ptr<TPortionDataAccessor>>&& portions) {
        for (auto&& i : portions) {
            AFL_VERIFY(i);
            const ui64 portionId = i->GetPortionInfo().GetPortionId();
            PortionsById.emplace(portionId, std::move(i));
        }
    }

    THashMap<ui64, std::shared_ptr<TPortionDataAccessor>> ExtractPortions() {
        return std::move(PortionsById);
    }

    const THashMap<ui64, std::shared_ptr<TPortionDataAccessor>>& GetPortions() const {
        return PortionsById;
    }

    std::vector<std::shared_ptr<TPortionDataAccessor>> ExtractPortionsVector() {
        std::vector<std::shared_ptr<TPortionDataAccessor>> portions;
        portions.reserve(PortionsById.size());
        for (auto&& [_, portionInfo] : PortionsById) {
            portions.emplace_back(std::move(portionInfo));
        }
        return portions;
    }

    void Merge(TDataAccessorsResult&& result) {
        for (auto&& i : result.ErrorsByPathId) {
            AFL_VERIFY(ErrorsByPathId.emplace(i.first, i.second).second);
        }
        for (auto&& i : result.PortionsById) {
            AFL_VERIFY(PortionsById.emplace(i.first, std::move(i.second)).second);
        }
    }

    const TPortionDataAccessor& GetPortionAccessorVerified(const ui64 portionId) const {
        auto it = PortionsById.find(portionId);
        AFL_VERIFY(it != PortionsById.end());
        return *it->second;
    }

    std::shared_ptr<TPortionDataAccessor> ExtractPortionAccessorVerified(const ui64 portionId) {
        auto it = PortionsById.find(portionId);
        AFL_VERIFY(it != PortionsById.end());
        auto result = std::move(it->second);
        PortionsById.erase(it);
        return result;
    }

    void AddData(THashMap<ui64, std::shared_ptr<TPortionDataAccessor>>&& accessors) {
        if (PortionsById.empty()) {
            PortionsById = std::move(accessors);
        } else {
            for (auto&& [portionId, i] : accessors) {
                AFL_VERIFY(i);
                AFL_VERIFY(PortionsById.emplace(portionId, std::move(i)).second);
            }
        }
    }

    void AddError(const TInternalPathId pathId, const TString& errorMessage) {
        ErrorsByPathId.emplace(pathId, errorMessage);
    }

    bool HasErrors() const {
        return ErrorsByPathId.size();
    }
};

class IDataAccessorRequestsSubscriber: public NColumnShard::TMonitoringObjectsCounter<IDataAccessorRequestsSubscriber> {
private:
    THashSet<ui64> RequestIds;

    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) = 0;
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const = 0;

    void OnRequestsFinished(TDataAccessorsResult&& result) {
        DoOnRequestsFinished(std::move(result));
    }

    void RegisterRequestId(const TDataAccessorsRequest& request);

    friend class TDataAccessorsRequest;
    std::optional<TDataAccessorsResult> Result;

public:
    void OnResult(const ui64 requestId, TDataAccessorsResult&& result) {
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
    const std::shared_ptr<const TAtomicCounter>& GetAbortionFlag() const {
        return DoGetAbortionFlag();
    }

    virtual ~IDataAccessorRequestsSubscriber() = default;
};

class TFakeDataAccessorsSubscriber: public IDataAccessorRequestsSubscriber {
private:
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Default<std::shared_ptr<const TAtomicCounter>>();
    }
    virtual void DoOnRequestsFinished(TDataAccessorsResult&& /*result*/) override {
    }
};

class TDataAccessorsRequest: public NColumnShard::TMonitoringObjectsCounter<TDataAccessorsRequest> {
private:
    static inline TAtomicCounter Counter = 0;
    YDB_READONLY(ui64, RequestId, Counter.Inc());
    YDB_READONLY(
        NGeneralCache::TPortionsMetadataCachePolicy::EConsumer, Consumer, NGeneralCache::TPortionsMetadataCachePolicy::DefaultConsumer());
    THashMap<ui64, TPortionInfo::TConstPtr> Portions;
    std::shared_ptr<IDataAccessorRequestsSubscriber> Subscriber;

public:
    std::shared_ptr<IDataAccessorRequestsSubscriber> ExtractSubscriber() {
        AFL_VERIFY(HasSubscriber());
        return std::move(Subscriber);
    }

    THashSet<NGeneralCache::TGlobalPortionAddress> BuildAddresses(const NActors::TActorId tabletActorId) const {
        THashSet<NGeneralCache::TGlobalPortionAddress> result;
        for (auto&& [_, p] : Portions) {
            AFL_VERIFY(result.emplace(NGeneralCache::TGlobalPortionAddress(tabletActorId, p->GetAddress())).second);
        }
        return result;
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "request_id=" << RequestId << ";";
        for (auto&& [id, p] : Portions) {
            sb << id << "={" << p->DebugString() << "};";
        }
        return sb;
    }

    TDataAccessorsRequest(const NGeneralCache::TPortionsMetadataCachePolicy::EConsumer consumer)
        : Consumer(consumer) {
    }

    ui64 PredictAccessorsMemory(const ISnapshotSchema::TPtr& schema) const {
        ui64 result = 0;
        for (auto&& [_, p] : Portions) {
            result += p->PredictAccessorsMemory(schema);
        }
        return result;
    }

    bool IsAborted() const {
        AFL_VERIFY(HasSubscriber());
        auto flag = Subscriber->GetAbortionFlag();
        return flag && flag->Val();
    }

    const std::shared_ptr<const TAtomicCounter>& GetAbortionFlag() const {
        AFL_VERIFY(HasSubscriber());
        return Subscriber->GetAbortionFlag();
    }

    bool HasSubscriber() const {
        return !!Subscriber;
    }

    ui32 GetSize() const {
        return Portions.size();
    }

    bool IsEmpty() const {
        return Portions.empty();
    }

    void SetColumnIds(const std::set<ui32>& /*columnIds*/) {
    }

    void RegisterSubscriber(const std::shared_ptr<IDataAccessorRequestsSubscriber>& subscriber) {
        AFL_VERIFY(!Subscriber);
        Subscriber = subscriber;
        Subscriber->RegisterRequestId(*this);
    }

    void AddPortion(const TPortionInfo::TConstPtr& portion) {
        AFL_VERIFY(portion);
        AFL_VERIFY(Portions.emplace(portion->GetPortionId(), portion).second);
    }

    TString GetTaskId() const {
        return TStringBuilder() << "data-accessor-request-" << RequestId;
    }
};

}   // namespace NKikimr::NOlap
