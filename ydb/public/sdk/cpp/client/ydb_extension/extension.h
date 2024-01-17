#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <library/cpp/monlib/metrics/metric_registry.h>

namespace Ydb {
namespace Discovery {

class ListEndpointsResult;

}
}

namespace NYdb {

class IExtensionApi {
public:
    friend class TDriver;
public:
    virtual ~IExtensionApi() = default;
private:
    void SelfRegister(TDriver driver);
};

class IExtension {
    friend class TDriver;
public:
    virtual ~IExtension() = default;
private:
    void SelfRegister(TDriver driver);
};

namespace NSdkStats {

class IStatApi: public IExtensionApi {
public:
    static IStatApi* Create(TDriver driver);
public:
    virtual void SetMetricRegistry(::NMonitoring::IMetricRegistry* sensorsRegistry) = 0;
    virtual void Accept(::NMonitoring::IMetricConsumer* consumer) const = 0;
};

class DestroyedClientException: public yexception {};

} // namespace NSdkStats


class IDiscoveryMutatorApi: public IExtensionApi {
public:
    struct TAuxInfo {
        std::string_view Database;
        std::string_view DiscoveryEndpoint;
    };
    using TMutatorCb = std::function<TStatus(Ydb::Discovery::ListEndpointsResult* proto, TStatus status, const TAuxInfo& aux)>;

    static IDiscoveryMutatorApi* Create(TDriver driver);
public:
    virtual void SetMutatorCb(TMutatorCb&& mutator) = 0;
};


template<typename TExtension>
void TDriver::AddExtension(typename TExtension::TParams params) {
    typename TExtension::IApi* api = TExtension::IApi::Create(*this);
    auto extension = new TExtension(params, api);
    extension->SelfRegister(*this);
    if (api)
        api->SelfRegister(*this);
}

} // namespace NYdb
