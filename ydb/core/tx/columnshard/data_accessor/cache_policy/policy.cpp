#include "policy.h"

#include <ydb/core/tx/columnshard/data_accessor/abstract/collector.h>

namespace NKikimr::NOlap::NGeneralCache {

std::shared_ptr<IObjectsProcessor> TPortionsMetadataCachePolicy::BuildObjectsProcessor(const NActors::TActorId& serviceActorId) {
    class TAccessorsCallback: public NDataAccessorControl::IAccessorCallback {
    private:
        const NActors::TActorId OwnerActorId;
        const std::shared_ptr<NGeneralCache::IObjectsProcessor<TPortionsMetadataCachePolicy>> Callback;
        THashSet<TAddress> RequestedAddresses;

        virtual void OnAccessorsFetched(std::vector<TPortionDataAccessor>&& accessors) override {
            THashMap<TAddress, TObject> objects;
            for (auto&& i : accessors) {
                const TAddress address(OwnerActorId, i.GetPortionInfo()->GetInternalPathId(), i.GetPortionInfo()->GetPortionId());
                AFL_VERIFY(RequestedAddresses.erase(address));
                objects.emplace(address, std::move(i));
            }
            Callback->OnReceiveData(std::move(objects), std::move(RequestedAddresses));
        }

    public:
        TAccessorsCallback(const NActors::TActorId& ownerActorId,
            const std::shared_ptr<NGeneralCache::IObjectsProcessor<TPortionsMetadataCachePolicy>>& callback,
            THashSet<TAddress>&& requestedAddresses)
            : OwnerActorId(ownerActorId)
            , Callback(callback)
            , RequestedAddresses(std::move(requestedAddresses)) {
        }
    };

    class TObjectsProcessor: public NGeneralCache::IObjectsProcessor<TPortionsMetadataCachePolicy> {
    private:
        virtual void DoAskData(
            const THashMap<EConsumer, THashSet<TAddress>>& objectAddressesByConsumer, const std::shared_ptr<TSelf>& selfPtr) const override {
            THashMap<NActors::TActorId, THashMap<TInternalPathId, THashMap<EConsumer, std::vector<ui64>>>> requests;
            THashSet<TAddress> requestedAddresses;
            for (auto&& [c, addresses] : objectAddressesByConsumer) {
                for (auto&& a : addresses) {
                    requests[a.GetTabletActorId()][a.GetInternalPathId()][c].emplace_back(a.GetPortionId());
                    AFL_VERIFY(requestedAddresses.emplace(a).second);
                }
            }
            for (auto&& i : requests) {
                NActors::TActivationContext::Send(i.first, std::make_unique<NDataAccessorControl::TEvAskTabletDataAccessors>(std::move(request),
                                                               std::make_shared<TAccessorsCallback>(i.first, selfPtr, std::move(i.second))));
            }
        }
        virtual void DoOnReceiveData(THashMap<TAddress, TObject>&& objectAddresses, THashSet<TAddress>&& removedAddresses) const override {
            NActors::TActivationContext::Send(
                ServiceActorId, std::make_unique<NGeneralCache::NSource::TEvents<TPortionsMetadataCachePolicy>>::TEvObjectsInfo(
                                    std::move(objectAddresses, removedAddresses)));
        }

    public:
        TObjectsProcessor(const NActors::TActorId& serviceActorId)
            : ServiceActorId(serviceActorId) {
        }
    };

    return std::make_shared<TObjectsProcessor>(
        NGeneralCache::TServiceOperator<TPortionsMetadataCachePolicy>::MakeServiceId(TActivationContext::ActorSystem()->NodeId));
}
}   // namespace NKikimr::NOlap::NGeneralCache
