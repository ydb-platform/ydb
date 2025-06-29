#pragma once
#include "request.h"

#include "abstract/collector.h"

#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/general_cache/usage/abstract.h>
#include <ydb/core/tx/general_cache/usage/service.h>

#include <ydb/services/bg_tasks/abstract/interface.h>

namespace NKikimr::NOlap::NDataAccessorControl {

class IDataAccessorsManager {
private:
    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) = 0;
    virtual void DoAddPortion(const TPortionDataAccessor& accessor) = 0;
    virtual void DoRemovePortion(const TPortionInfo::TConstPtr& portion) = 0;
    const NActors::TActorId TabletActorId;

public:
    const NActors::TActorId& GetTabletActorId() const {
        return TabletActorId;
    }

    IDataAccessorsManager(const NActors::TActorId& tabletActorId)
        : TabletActorId(tabletActorId) {
    }

    virtual ~IDataAccessorsManager() = default;

    void AddPortion(const TPortionDataAccessor& accessor) {
        DoAddPortion(accessor);
    }
    void RemovePortion(const TPortionInfo::TConstPtr& portion) {
        DoRemovePortion(portion);
    }
    void AskData(const std::shared_ptr<TDataAccessorsRequest>& request) {
        AFL_VERIFY(request);
        AFL_VERIFY(request->HasSubscriber());
        return DoAskData(request);
    }
};

class TDataAccessorsManagerContainer: public NBackgroundTasks::TControlInterfaceContainer<IDataAccessorsManager> {
private:
    using TBase = NBackgroundTasks::TControlInterfaceContainer<IDataAccessorsManager>;

public:
    using TBase::TBase;
};

class TActorAccessorsManager: public IDataAccessorsManager {
private:
    using TBase = IDataAccessorsManager;
    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) override {
        class TAdapterCallback: public TGeneralCache::ICallback {
        private:
            std::shared_ptr<IDataAccessorRequestsSubscriber> AccessorsCallback;
            const ui64 RequestId;
            virtual bool DoIsAborted() const override {
                return AccessorsCallback->GetAbortionFlag() && AccessorsCallback->GetAbortionFlag()->Val();
            }

            virtual void DoOnResultReady(THashMap<NGeneralCache::TGlobalPortionAddress, TPortionDataAccessor>&& objectAddresses,
                THashSet<NGeneralCache::TGlobalPortionAddress>&& removedAddresses,
                THashMap<NGeneralCache::TGlobalPortionAddress, TString>&& errorAddresses) const override {
                AFL_VERIFY(removedAddresses.empty());
                THashMap<ui64, TPortionDataAccessor> objects;
                for (auto&& i : objectAddresses) {
                    objects.emplace(i.first.GetPortionId(), std::move(i.second));
                }
                TDataAccessorsResult result;
                result.AddData(std::move(objects));
                for (auto&& i : errorAddresses) {
                    result.AddError(i.first.GetPathId(), i.second);
                }
                AccessorsCallback->OnResult(RequestId, std::move(result));
            }

        public:
            TAdapterCallback(const std::shared_ptr<IDataAccessorRequestsSubscriber>& accCallback, const ui64 requestId)
                : AccessorsCallback(accCallback)
                , RequestId(requestId) {
            }
        };

        NKikimr::NGeneralCache::TServiceOperator<NGeneralCache::TPortionsMetadataCachePolicy>::AskObjects(request->GetConsumer(),
            request->BuildAddresses(GetTabletActorId()),
            std::make_shared<TAdapterCallback>(request->ExtractSubscriber(), request->GetRequestId()));
    }
    virtual void DoAddPortion(const TPortionDataAccessor& accessor) override {
        THashMap<NGeneralCache::TGlobalPortionAddress, TPortionDataAccessor> add;
        THashSet<NGeneralCache::TGlobalPortionAddress> remove;
        add.emplace(NGeneralCache::TGlobalPortionAddress(GetTabletActorId(), accessor.GetPortionInfo().GetAddress()), accessor);
        NKikimr::NGeneralCache::TServiceOperator<NGeneralCache::TPortionsMetadataCachePolicy>::ModifyObjects(
            GetTabletActorId(), std::move(add), std::move(remove));
    }
    virtual void DoRemovePortion(const TPortionInfo::TConstPtr& portion) override {
        THashMap<NGeneralCache::TGlobalPortionAddress, TPortionDataAccessor> add;
        THashSet<NGeneralCache::TGlobalPortionAddress> remove;
        remove.emplace(NGeneralCache::TGlobalPortionAddress(GetTabletActorId(), portion->GetAddress()));
        NKikimr::NGeneralCache::TServiceOperator<NGeneralCache::TPortionsMetadataCachePolicy>::ModifyObjects(
            GetTabletActorId(), std::move(add), std::move(remove));
    }

public:
    TActorAccessorsManager(const NActors::TActorId& tabletActorId)
        : TBase(tabletActorId) {
        AFL_VERIFY(!!tabletActorId);
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
