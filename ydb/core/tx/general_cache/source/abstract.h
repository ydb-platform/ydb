#pragma once

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/hash.h>

#include <memory>

namespace NKikimr::NGeneralCache::NSource {

template <class TPolicy>
class IObjectsProcessor {
private:
    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;
    using TSourceId = typename TPolicy::TSourceId;
    using EConsumer = typename TPolicy::EConsumer;

    using TSelf = IObjectsProcessor<TPolicy>;
    virtual void DoAskData(const THashMap<EConsumer, THashSet<TAddress>>& objectAddressesByConsumer, const std::shared_ptr<TSelf>& selfPtr) const = 0;
    virtual void DoOnReceiveData(const TSourceId sourceId, const NActors::TActorId& owner, THashMap<TAddress, TObject>&& objectAddresses,
        THashSet<TAddress>&& removedAddresses, THashMap<TAddress, TString>&& errorAddresses) const = 0;

public:
    virtual ~IObjectsProcessor() = default;

    void AskData(const THashMap<EConsumer, THashSet<TAddress>>& objectAddressesByConsumer, const std::shared_ptr<TSelf>& selfPtr) const {
        DoAskData(objectAddressesByConsumer, selfPtr);
    }

    // owner is the actor that answered: a result from an older actor of the same source is dropped.
    void OnReceiveData(const TSourceId sourceId, const NActors::TActorId& owner, THashMap<TAddress, TObject>&& objectAddresses,
        THashSet<TAddress>&& removedAddresses, THashMap<TAddress, TString>&& errorAddresses) const {
        DoOnReceiveData(sourceId, owner, std::move(objectAddresses), std::move(removedAddresses), std::move(errorAddresses));
    }
};

}   // namespace NKikimr::NGeneralCache::NSource
