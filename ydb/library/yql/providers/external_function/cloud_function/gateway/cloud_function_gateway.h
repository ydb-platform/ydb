#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/external_function/gateway/dq_function_gateway.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

namespace NYql::NDq {
namespace NCloudFunction {

constexpr TStringBuf CloudFunctionProviderName = "CloudFunction";

IDqFunctionGateway::TPtr CreateCloudFunctionGateway(
        const TString& apiEndpoint, const TString& sslCaCert,
        const THashMap<TString, TString>& secureParams, const TString& connectionName,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

void RegisterCloudFunctionGateway(TDqFunctionGatewayFactory& factory,
                                  const TString& apiEndpoint, const TString& sslCaCert,
                                  ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory) {

    factory.Register(TString{CloudFunctionProviderName}, [apiEndpoint, sslCaCert, credentialsFactory]
        (const THashMap<TString, TString>& secureParams, const TString& connectionName) {
            return CreateCloudFunctionGateway(apiEndpoint, sslCaCert,
                                              secureParams, connectionName,
                                              credentialsFactory);
        });
}

}
}