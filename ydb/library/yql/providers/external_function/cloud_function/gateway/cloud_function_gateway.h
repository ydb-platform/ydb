#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>
#include <library/cpp/threading/future/future.h>

namespace yandex::cloud::priv::serverless::functions::v1 {
    class Function;
}

namespace NYql::NDq {
namespace NCloudFunction {

class ICloudFunctionGateway {
public:
    using TPtr = std::shared_ptr<ICloudFunctionGateway>;
    virtual NThreading::TFuture<yandex::cloud::priv::serverless::functions::v1::Function> ResolveFunction(const TString& folderId, const TString& functionName) = 0;
    virtual ~ICloudFunctionGateway() = default;
};

ICloudFunctionGateway::TPtr CreateCloudFunctionGateway(
        const TString& apiEndpoint, const TString& sslCaCert,
        const THashMap<TString, TString>& secureParams, const TString& connectionName,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

}
}