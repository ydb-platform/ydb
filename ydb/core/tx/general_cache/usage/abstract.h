#pragma once
#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NKikimr::NGeneralCache::NPublic {

template <class TPolicy>
class ICallback {
private:
    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;

    virtual void DoOnResultReady(THashMap<TAddress, TObject>&& objectAddresses, THashSet<TAddress>&& removedAddresses,
        THashMap<TAddress, TString>&& errorAddresses) const = 0;
    virtual bool DoIsAborted() const = 0;

public:
    virtual ~ICallback() = default;

    bool IsAborted() const {
        return DoIsAborted();
    }
    void OnResultReady(THashMap<TAddress, TObject>&& objectAddresses, THashSet<TAddress>&& removedAddresses,
        THashMap<TAddress, TString>&& errorAddresses) const {
        DoOnResultReady(std::move(objectAddresses), std::move(removedAddresses), std::move(errorAddresses));
    }
};

}   // namespace NKikimr::NGeneralCache::NPublic
