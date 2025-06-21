#pragma once

namespace NKikimr::NGeneralCache::NSource {

template <class TPolicy>
class IObjectsProcessor {
private:
    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;
    using EConsumer = typename TPolicy::EConsumer;

    using TSelf = IObjectsProcessor<TAddress>;
    virtual void DoAskData(const std::vector<TAddress>& objectAddresses, const std::shared_ptr<TSelf>& selfPtr) const = 0;
    virtual void DoOnReceiveData(THashMap<TAddress, TObject>&& objectAddresses, THashSet<TAddress>&& removedAddresses,
        THashMap<TAddress, TString>&& errorAddresses) const = 0;

public:
    void AskData(const std::vector<TAddress>& objectAddresses, const std::shared_ptr<TSelf>& selfPtr) const {
        DoAskData(objectAddresses, selfPtr);
    }

    void OnReceiveData(
        THashMap<TAddress, TObject>&& objectAddresses, THashSet<TAddress>&& removedAddresses, THashMap<TAddress, TString>&& errorAddresses) const {
        DoOnReceiveData(std::move(objectAddresses), std::move(removedAddresses), std::move(errorAddresses));
    }
};

}   // namespace NKikimr::NGeneralCache::NSource
