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
    virtual void DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, const bool update) = 0;
    virtual void DoUnregisterController(const TInternalPathId pathId) = 0;
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
    void RegisterController(std::unique_ptr<IGranuleDataAccessor>&& controller, const bool update) {
        AFL_VERIFY(controller);
        return DoRegisterController(std::move(controller), update);
    }
    void UnregisterController(const TInternalPathId pathId) {
        return DoUnregisterController(pathId);
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
        class TAdapterCallback: public NKikimr::NGeneralCache::NPublic::ICallback<NGeneralCache::TPortionsMetadataCachePolicy> {
        private:
            std::shared_ptr<IDataAccessorRequestsSubscriber> AccessorsCallback;
            const ui32 RequestId;
            virtual void DoOnResultReady(THashMap<NGeneralCache::TGlobalPortionAddress, TPortionDataAccessor>&& objectAddresses,
                THashSet<NGeneralCache::TGlobalPortionAddress>&& removedAddresses,
                THashMap<NGeneralCache::TGlobalPortionAddress, TString>&& errorAddresses) const override {
                AFL_VERIFY(removedAddresses.empty());
                AFL_VERIFY(errorAddresses.empty());
                THashMap<ui64, TPortionDataAccessor> objects;
                for (auto&& i : objectAddresses) {
                    objects.emplace(i.first.GetPortionId(), std::move(i.second));
                }
                TDataAccessorsResult result;
                result.AddData(std::move(objects));
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
    virtual void DoRegisterController(std::unique_ptr<IGranuleDataAccessor>&& /*controller*/, const bool /*update*/) override {
    }
    virtual void DoUnregisterController(const TInternalPathId /*pathId*/) override {
    }
    virtual void DoAddPortion(const TPortionDataAccessor& accessor) override {
        THashMap<NGeneralCache::TGlobalPortionAddress, TPortionDataAccessor> objects;
        objects.emplace(NGeneralCache::TGlobalPortionAddress(GetTabletActorId(), accessor.GetPortionInfo().GetAddress()), accessor);
        NKikimr::NGeneralCache::TServiceOperator<NGeneralCache::TPortionsMetadataCachePolicy>::AddObjects(std::move(objects));
    }
    virtual void DoRemovePortion(const TPortionInfo::TConstPtr& /*portion*/) override {
    }

public:
    TActorAccessorsManager(const NActors::TActorId& tabletActorId)
        : TBase(tabletActorId) {
        AFL_VERIFY(!!tabletActorId);
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
