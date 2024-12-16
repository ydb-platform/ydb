#ifndef TEST_PROXY_SERVICE_INL_H_
#error "Direct inclusion of this file is not allowed, include test_proxy_service.h"
// For the sake of sane code completion.
#include "test_proxy_service.h"
#endif

#include <yt/yt/core/misc/common.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <util/system/type_name.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

inline TServiceMap ConvertToMap(const IServicePtr& service)
{
    return {std::make_pair(service->GetServiceId().ServiceName, service)};
}

inline TServiceMap ConvertToMap(const std::vector<IServicePtr>& services)
{
    TServiceMap map;
    for (const auto& service : services) {
        YT_VERIFY(service);

        EmplaceOrCrash(map, service->GetServiceId().ServiceName, service);
    }

    return map;
}

inline TRealmIdServiceMap ConvertToRealmIdMap(const TServiceMap& map)
{
    TRealmIdServiceMap realmIdMap;

    for (const auto& [serviceName, service] : map) {
        YT_VERIFY(service);

        auto serviceId = service->GetServiceId();
        realmIdMap[serviceId.RealmId][service->GetServiceId().ServiceName] = service;
    }

    return realmIdMap;
}

template <class TContainer>
TRealmIdServiceMap ConvertToRealmIdMap(const TContainer& services)
{
    return ConvertToRealmIdMap(ConvertToMap(services));
}

template <class TContainer>
THashMap<std::string, TRealmIdServiceMap> ConvertToAddressMap(const THashMap<std::string, TContainer>& addressToServices)
{
    THashMap<std::string, TRealmIdServiceMap> result;

    for (const auto& [address, map] : addressToServices) {
        result[address] = ConvertToRealmIdMap(map);
    }

    return result;
}

template <class TAddressContainer, class TDefaultContainer>
IChannelFactoryPtr CreateTestChannelFactory(
    const THashMap<std::string, TAddressContainer>& addressToServices,
    const TDefaultContainer& defaultServices)
{
    return New<TTestChannelFactory>(ConvertToAddressMap(addressToServices), ConvertToRealmIdMap(defaultServices));
}

template <class TDefaultContainer>
IChannelFactoryPtr CreateTestChannelFactoryWithDefaultServices(const TDefaultContainer& defaultServices)
{
    return CreateTestChannelFactory(THashMap<std::string, TDefaultContainer>{}, defaultServices);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
