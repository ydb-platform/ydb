#include "ydb_sdk_core_access.h"

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NYdb {
namespace NConsoleClient {

class TDummyClient::TImpl : public TClientImplCommon<TDummyClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {}

    std::shared_ptr<NYdb::ICoreFacility> GetCoreFacility() {
        return DbDriverState_;
    }
};

TDummyClient::TDummyClient(NYdb::TDriver& driver)
    : Impl_(new TImpl(CreateInternalInterface(driver), TCommonClientSettings()))
{}

std::shared_ptr<NYdb::ICoreFacility> TDummyClient::GetCoreFacility() {
    return Impl_->GetCoreFacility();
}

}
}
