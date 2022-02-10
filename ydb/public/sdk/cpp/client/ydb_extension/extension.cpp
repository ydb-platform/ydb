#include "extension.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections/grpc_connections.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/stats_extractor/extractor.h>
#undef INCLUDE_YDB_INTERNAL_H

namespace NYdb {

void IExtension::SelfRegister(TDriver driver) {
    CreateInternalInterface(driver)->RegisterExtension(this);
}

void IExtensionApi::SelfRegister(TDriver driver) {
    CreateInternalInterface(driver)->RegisterExtensionApi(this);
}

namespace NSdkStats {

IStatApi* IStatApi::Create(TDriver driver) {
    return new TStatsExtractor(CreateInternalInterface(driver));
}

} // namespace YSdkStats

class TDiscoveryMutator : public IDiscoveryMutatorApi {
public:
    TDiscoveryMutator(std::shared_ptr<TGRpcConnectionsImpl> driverImpl)
        : DriverImpl(driverImpl.get())
    { }

    void SetMutatorCb(TMutatorCb&& cb) override {
        DriverImpl->SetDiscoveryMutator(std::move(cb));
    }
private:
    TGRpcConnectionsImpl* DriverImpl;
};

IDiscoveryMutatorApi* IDiscoveryMutatorApi::Create(TDriver driver) {
    return new TDiscoveryMutator(CreateInternalInterface(driver));
}

} // namespace NYdb

