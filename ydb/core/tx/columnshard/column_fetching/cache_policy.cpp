#include "cache_policy.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/data_accessor/abstract/collector.h>
#include <ydb/core/tx/general_cache/source/events.h>
#include <ydb/core/tx/general_cache/usage/service.h>

namespace NKikimr::NOlap::NGeneralCache {

namespace {

class TObjectsProcessor: public NKikimr::NGeneralCache::NSource::IObjectsProcessor<TColumnDataCachePolicy> {
private:
    using TAddress = TColumnDataCachePolicy::TAddress;
    using TObject = TColumnDataCachePolicy::TObject;
    using TSourceId = TActorId;
    using TFetchingContext = NKikimr::NGeneralCache::NSource::TFetchingContext<TColumnDataCachePolicy>;
    using EConsumer = TColumnDataCachePolicy::EConsumer;
    using TSelf = NKikimr::NGeneralCache::NSource::IObjectsProcessor<TColumnDataCachePolicy>;

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
        THashMap<TActorId, THashMap<EConsumer, THashMap<ui32, std::vector<TPortionAddress>>>> columns;
        for (const auto& [consumer, addresses] : objectAddressesByConsumer) {
            for (const auto& address : addresses) {
                columns[address.GetTabletActorId()][consumer][address.GetColumnId()].emplace_back(address.GetInternalPortionAddress());
            }
        }
        for (auto&& [tablet, request] : columns) {
            NActors::TActivationContext::Send(
                tablet, std::make_unique<NColumnShard::TEvPrivate::TEvAskColumnData>(std::move(request), selfPtr), cookie);
        }
    }
    virtual void DoOnReceiveData(const TSourceId sourceId, THashMap<TAddress, TObject>&& objectAddresses, THashSet<TAddress>&& removedAddresses,
        THashMap<TAddress, TString>&& errorAddresses) const override {
        NActors::TActivationContext::Send(
            ServiceActorId, std::make_unique<NKikimr::NGeneralCache::NSource::TEvents<TColumnDataCachePolicy>::TEvObjectsInfo>(
                                sourceId, std::move(objectAddresses), std::move(removedAddresses), std::move(errorAddresses)));
    }

public:
    TObjectsProcessor(const NActors::TActorId& serviceActorId)
        : ServiceActorId(serviceActorId)
    {
    }
};

}   // namespace

std::shared_ptr<NKikimr::NGeneralCache::NSource::IObjectsProcessor<TColumnDataCachePolicy>> TColumnDataCachePolicy::BuildObjectsProcessor(
    const NActors::TActorId& serviceActorId) {
    return std::make_shared<TObjectsProcessor>(serviceActorId);
}
}   // namespace NKikimr::NOlap::NGeneralCache
