#include "cloud_function_gateway.h"

#include <library/cpp/string_utils/quote/quote.h>
#include <util/string/builder.h>
#include <util/generic/hash.h>

#include <cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/function_service.pb.h>
#include <cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/function_service.grpc.pb.h>
#include <cloud/bitbucket/private-api/yandex/cloud/priv/serverless/functions/v1/function.pb.h>

#include <library/cpp/grpc/client/grpc_client_low.h>
#include <exception>
#include <fmt/format.h>

namespace NYql::NDq {
namespace NCloudFunction {

using namespace yandex::cloud::priv::serverless::functions::v1;

class TCloudFunctionGateway : public ICloudFunctionGateway {
public:
    using TPtr = std::shared_ptr<TCloudFunctionGateway>;

private:
    static constexpr TDuration TIMEOUT = TDuration::MilliSeconds(300);

    class TImpl : public std::enable_shared_from_this<TImpl> {
    public:
        TImpl(const TString& apiEndpoint, const TString& sslCaCert, NYdb::TCredentialsProviderPtr credentialsProvider)
        : Client(std::make_unique<NGrpc::TGRpcClientLow>())
        , CredentialsProvider(std::move(credentialsProvider))
        {
            NGrpc::TGRpcClientConfig grpcConf;
            grpcConf.Locator = apiEndpoint;
            grpcConf.EnableSsl = true;
            grpcConf.SslCaCert = sslCaCert;
            Connection = Client->CreateGRpcServiceConnection<FunctionService>(grpcConf);
        }

        NThreading::TFuture<ListFunctionsResponse> List(const TString& folderId, const TString& functionName) {
            auto promise = NThreading::NewPromise<ListFunctionsResponse>();
            auto callback = [promise, folderId, functionName](NGrpc::TGrpcStatus&& status, ListFunctionsResponse&& resp) mutable {
                if (status.Ok()) {
                    promise.SetValue(std::move(resp));
                } else {
                    const TString exception = fmt::format("Failed to find cloud function '{}' at folder '{}'. Grpc status: {} ({})",
                                                          functionName, folderId, status.GRpcStatusCode, status.Msg);
                    promise.SetException(exception);
                }
            };

            NGrpc::TCallMeta meta;
            meta.Timeout = TIMEOUT;
            meta.Aux.emplace_back("authorization", CredentialsProvider->GetAuthInfo());

            ListFunctionsRequest req;
            req.set_folder_id(folderId);
            req.set_filter(TString{"name=\""} + UrlEscapeRet(functionName, true) + "\"");
            Connection->DoRequest<ListFunctionsRequest, ListFunctionsResponse>(
                std::move(req), std::move(callback),
                &FunctionService::Stub::AsyncList,
                meta
            );
            return promise.GetFuture();
        }

        void Stop() {
            Client.reset();
        }
    private:
        std::unique_ptr<NGrpc::TGRpcClientLow> Client;
        std::unique_ptr<NGrpc::TServiceConnection<FunctionService>> Connection;
        NYdb::TCredentialsProviderPtr CredentialsProvider;
    };

public:
    TCloudFunctionGateway(const TString& apiEndpoint, const TString& sslCaCert, NYdb::TCredentialsProviderPtr credentialsProvider)
    : Impl(std::make_shared<TImpl>(apiEndpoint, sslCaCert, credentialsProvider))
    {
    }

    NThreading::TFuture<Function> ResolveFunction(const TString& folderId, const TString& functionName) {
        auto listResponse = Impl->List(folderId, functionName);
        return listResponse.Apply([folderId, functionName](const NThreading::TFuture<ListFunctionsResponse>& future) -> Function {
            auto functions = future.GetValue().Getfunctions();
            if (!functions.empty()) {
                return functions.at(0);
            }
            throw yexception() << fmt::format("Failed to find cloud function '{}' at folder '{}'", functionName, folderId);
        });
    }

    ~TCloudFunctionGateway() {
        Impl->Stop();
    }

private:
    std::shared_ptr<TImpl> Impl;
};

ICloudFunctionGateway::TPtr CreateCloudFunctionGateway(
        const TString& apiEndpoint, const TString& sslCaCert,
        const THashMap<TString, TString>& secureParams, const TString& connectionName,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory) {

    const auto token = secureParams.find(connectionName);
    if (token == secureParams.end()) {
        throw yexception() << fmt::format("Can't find token by connection name '{}'", connectionName);
    }
    auto credProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token->second, true)->CreateProvider();
    return std::make_shared<TCloudFunctionGateway>(apiEndpoint, sslCaCert, credProviderFactory);
}

} // namespace NCloudFunction
}