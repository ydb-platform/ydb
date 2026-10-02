#include "peer_priority_provider.h"

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NRpc {

using namespace NThreading;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

class TDummyPeerPriorityProvider
    : public IPeerPriorityProvider
{
public:
    EPeerPriority GetPeerPriority(const std::string& /*address*/) const override
    {
        return EPeerPriority::LocalDc;
    }
};

IPeerPriorityProviderPtr GetDummyPeerPriorityProvider()
{
    return LeakyRefCountedSingleton<TDummyPeerPriorityProvider>();
}

////////////////////////////////////////////////////////////////////////////////

class TYPClusterMatchingPeerPriorityProvider
    : public IPeerPriorityProvider
{
public:
    EPeerPriority GetPeerPriority(const std::string& address) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return (InferYPClusterFromHostName(address) == GetLocalYPCluster())
            ? EPeerPriority::LocalDc
            : EPeerPriority::ForeignDc;
    }
};

IPeerPriorityProviderPtr GetYPClusterMatchingPeerPriorityProvider()
{
    return LeakyRefCountedSingleton<TYPClusterMatchingPeerPriorityProvider>();
}

////////////////////////////////////////////////////////////////////////////////

class TYPClusterMatchingPeerPriorityProviderWithOverrides
    : public IMapPeerPriorityProvider
{
public:
    EPeerPriority GetPeerPriority(const std::string& address) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return (GetYPClusterByAddress(address) == GetLocalYPCluster())
            ? EPeerPriority::LocalDc
            : EPeerPriority::ForeignDc;
    }

    void SetAddressToClusterOverride(THashMap<std::string, std::string> addressToClusterOverride) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        AddressToClusterOverride_.Store(std::move(addressToClusterOverride));
    }

private:
    TAtomicObject<THashMap<std::string, std::string>> AddressToClusterOverride_;

    std::optional<std::string> GetYPClusterByAddress(const std::string& address) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return AddressToClusterOverride_.Read([&address] (const auto& mapping) -> std::optional<std::string> {
            if (auto it = mapping.find(address); it != mapping.end()) {
                return std::optional(it->second);
            }

            return InferYPClusterFromHostName(address);
        });
    }
};

IMapPeerPriorityProviderPtr CreateYPClusterMatchingPeerPriorityProviderWithOverrides()
{
    return New<TYPClusterMatchingPeerPriorityProviderWithOverrides>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
