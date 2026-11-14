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
    virtual void DoAddPortion(const std::shared_ptr<TPortionDataAccessor>& accessor) = 0;
    virtual void DoRemovePortion(const TPortionInfo::TConstPtr& portion) = 0;
    const NActors::TActorId TabletActorId;

public:
    const NActors::TActorId& GetTabletActorId() const {
        return TabletActorId;
    }

    IDataAccessorsManager(const NActors::TActorId& tabletActorId)
        : TabletActorId(tabletActorId)
    {
    }

    virtual ~IDataAccessorsManager() = default;

    void AddPortion(const std::shared_ptr<TPortionDataAccessor>& accessor) {
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

            const THashMap<ui64, TPortionInfo::TConstPtr> Portions;

            virtual void DoOnResultReady(THashMap<NGeneralCache::TGlobalPortionAddress, std::shared_ptr<const TPortionChunks>>&& objectAddresses,
                THashSet<NGeneralCache::TGlobalPortionAddress>&& removedAddresses,
                ::NKikimr::NGeneralCache::NPublic::TErrorAddresses<NGeneralCache::TPortionsMetadataCachePolicy>&& errorAddresses) override {
                THashMap<ui64, std::shared_ptr<TPortionDataAccessor>> objects;
                for (auto&& [address, chunks] : objectAddresses) {
                    const auto portion = Portions.FindPtr(address.GetPortionId());
                    AFL_VERIFY(portion)("portion_id", address.GetPortionId());
                    AFL_VERIFY((*portion)->GetAddress() == address.GetInternalPortionAddress());
                    objects.emplace(address.GetPortionId(), std::make_shared<TPortionDataAccessor>(*portion, std::move(chunks)));
                }
                TDataAccessorsResult result;
                result.AddData(std::move(objects));
                result.AddRemovedData(std::move(removedAddresses));
                for (auto&& i : errorAddresses.GetErrors()) {
                    result.AddError(i.first.GetPathId(), i.second);
                }
                AccessorsCallback->OnResult(RequestId, std::move(result));
            }

        public:
            TAdapterCallback(const std::shared_ptr<IDataAccessorRequestsSubscriber>& accCallback, const ui64 requestId,
                THashMap<ui64, TPortionInfo::TConstPtr>&& portions)
                : AccessorsCallback(accCallback)
                , RequestId(requestId)
                , Portions(std::move(portions))
            {
            }
        };

        THashMap<ui64, TPortionInfo::TConstPtr> portions = request->GetPortions();
        NKikimr::NGeneralCache::TServiceOperator<NGeneralCache::TPortionsMetadataCachePolicy>::AskObjects(request->GetConsumer(),
            request->BuildAddresses(GetTabletActorId()),
            std::make_shared<TAdapterCallback>(request->ExtractSubscriber(), request->GetRequestId(), std::move(portions)));
    }

    virtual void DoAddPortion(const std::shared_ptr<TPortionDataAccessor>& accessor) override {
        THashMap<NGeneralCache::TGlobalPortionAddress, std::shared_ptr<const TPortionChunks>> add;
        THashSet<NGeneralCache::TGlobalPortionAddress> remove;
        auto chunks = accessor->GetChunksPtr();
        AFL_VERIFY(chunks);
        const auto portionAddress = accessor->GetPortionInfo().GetAddress();
        if (chunks->GetAddress() != portionAddress) {
            auto retargeted = std::make_shared<TPortionChunks>(*chunks);
            retargeted->ResetAddress(portionAddress);
            chunks = std::move(retargeted);
        }
        NGeneralCache::TGlobalPortionAddress address(GetTabletActorId(), portionAddress);
        add.emplace(address, std::move(chunks));
        NKikimr::NGeneralCache::TServiceOperator<NGeneralCache::TPortionsMetadataCachePolicy>::ModifyObjects(
            GetTabletActorId(), std::move(add), std::move(remove));
    }

    virtual void DoRemovePortion(const TPortionInfo::TConstPtr& portion) override {
        THashMap<NGeneralCache::TGlobalPortionAddress, std::shared_ptr<const TPortionChunks>> add;
        THashSet<NGeneralCache::TGlobalPortionAddress> remove;
        remove.emplace(NGeneralCache::TGlobalPortionAddress(GetTabletActorId(), portion->GetAddress()));
        NKikimr::NGeneralCache::TServiceOperator<NGeneralCache::TPortionsMetadataCachePolicy>::ModifyObjects(
            GetTabletActorId(), std::move(add), std::move(remove));
    }

public:
    TActorAccessorsManager(const NActors::TActorId& tabletActorId)
        : TBase(tabletActorId)
    {
        AFL_VERIFY(!!tabletActorId);
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
