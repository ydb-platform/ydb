#include "policy.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/data_accessor/abstract/collector.h>
#include <ydb/core/tx/general_cache/source/events.h>
#include <ydb/core/tx/general_cache/usage/service.h>

namespace NKikimr::NOlap::NGeneralCache {

std::shared_ptr<NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPortionsMetadataCachePolicy>>
TPortionsMetadataCachePolicy::BuildObjectsProcessor(const NActors::TActorId& serviceActorId) {
    class TAccessorsCallback: public NDataAccessorControl::IAccessorCallback {
    private:
        const NActors::TActorId OwnerActorId;
        const std::shared_ptr<NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPortionsMetadataCachePolicy>> Callback;
        THashSet<TAddress> RequestedAddresses;

        virtual void OnAccessorsFetched(std::vector<TPortionDataAccessor>&& accessors) override {
            THashMap<TAddress, TObject> objects;
            for (auto&& i : accessors) {
                const TAddress address(OwnerActorId, i.GetPortionInfo().GetAddress());
                AFL_VERIFY(RequestedAddresses.erase(address));
                objects.emplace(address, std::move(i));
            }
            Callback->OnReceiveData(OwnerActorId, std::move(objects), std::move(RequestedAddresses), {});
        }

    public:
        TAccessorsCallback(const NActors::TActorId& ownerActorId,
            const std::shared_ptr<NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPortionsMetadataCachePolicy>>& callback,
            THashSet<TAddress>&& requestedAddresses)
            : OwnerActorId(ownerActorId)
            , Callback(callback)
            , RequestedAddresses(std::move(requestedAddresses)) {
        }
    };

    class TObjectsProcessor: public NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPortionsMetadataCachePolicy> {
    private:
        using TAddress = TGlobalPortionAddress;
        using TObject = TPortionDataAccessor;
        using TSourceId = NActors::TActorId;
        using EConsumer = TPortionsMetadataCachePolicy::EConsumer;
        using TSelf = NKikimr::NGeneralCache::NSource::IObjectsProcessor<TPortionsMetadataCachePolicy>;

        const NActors::TActorId ServiceActorId;

        class TActorRequestData {
        private:
            THashMap<TInternalPathId, NDataAccessorControl::TPortionsByConsumer> Data;
            THashSet<TAddress> Requested;

        public:
            void Add(const EConsumer consumer, const TAddress& addr) {
                Data[addr.GetPathId()].UpsertConsumer(consumer).AddPortion(addr.GetPortionId());
                AFL_VERIFY(Requested.emplace(addr).second);
            }

            THashMap<TInternalPathId, NDataAccessorControl::TPortionsByConsumer> ExtractRequest() {
                return std::move(Data);
            }
            THashSet<TAddress> ExtractRequestedAddresses() {
                return std::move(Requested);
            }
        };

        virtual void DoAskData(const THashMap<EConsumer, THashSet<TAddress>>& objectAddressesByConsumer, const std::shared_ptr<TSelf>& selfPtr,
            const ui64 cookie) const override {
            THashMap<NActors::TActorId, TActorRequestData> requests;
            for (auto&& [c, addresses] : objectAddressesByConsumer) {
                for (auto&& a : addresses) {
                    requests[a.GetTabletActorId()].Add(c, a);
                }
            }
            for (auto&& i : requests) {
                NActors::TActivationContext::Send(i.first,
                    std::make_unique<NColumnShard::TEvPrivate::TEvAskTabletDataAccessors>(
                        i.second.ExtractRequest(), std::make_shared<TAccessorsCallback>(i.first, selfPtr, i.second.ExtractRequestedAddresses())),
                    0, cookie);
            }
        }
        virtual void DoOnReceiveData(const TSourceId sourceId, THashMap<TAddress, TObject>&& objectAddresses, THashSet<TAddress>&& removedAddresses,
            THashMap<TAddress, TString>&& errors) const override {
            NActors::TActivationContext::Send(
                ServiceActorId, std::make_unique<NKikimr::NGeneralCache::NSource::TEvents<TPortionsMetadataCachePolicy>::TEvObjectsInfo>(
                                    sourceId, std::move(objectAddresses), std::move(removedAddresses), std::move(errors)));
        }

    public:
        TObjectsProcessor(const NActors::TActorId& serviceActorId)
            : ServiceActorId(serviceActorId) {
        }
    };

    return std::make_shared<TObjectsProcessor>(serviceActorId);
}
}   // namespace NKikimr::NOlap::NGeneralCache
