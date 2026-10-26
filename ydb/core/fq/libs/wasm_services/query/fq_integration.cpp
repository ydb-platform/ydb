#include "query.h"

#include <ydb/core/fq/libs/common/external_service.h>

#include <google/protobuf/util/json_util.h>
#include <util/generic/yexception.h>

namespace NFq::NWasmServices {

TString PrepareServiceConnection(const FederatedQuery::Connection& connection, const TString& currentIamToken) {
    Y_ENSURE(connection.content().setting().has_external_service(), "Invalid WASM service connection type");
    auto service = connection.content().setting().external_service();
    Y_ENSURE(ValidateExternalService(service, false).Empty(), "Invalid external service connection");
    if (service.auth().has_current_iam()) {
        Y_ENSURE(!currentIamToken.empty(), "Current IAM token is unavailable for external service connection");
        service.mutable_auth()->mutable_token()->set_token(currentIamToken);
    }
    // SecureParams travels through protobuf string fields, so keep its payload UTF-8.
    TString payload;
    Y_ENSURE(google::protobuf::util::MessageToJsonString(service, &payload).ok(), "Cannot prepare external service connection");
    return payload;
}

NYql::TDqFunctionGatewayFactory::TPtr CreateServiceGatewayFactory(
    const NConfig::TWasmServicesConfig& config, const THashMap<TString, FederatedQuery::Connection>& connections) {
    THashMap<TString, TString> aliases;
    for (const auto& [id, connection] : connections) {
        if (connection.content().setting().has_external_service()) {
            Y_ENSURE(!id.empty() && aliases.emplace(connection.content().name(), id).second, "Invalid or duplicate external service connection");
        }
    }
    return CreateServiceGatewayFactoryWithResolver(config, [aliases = std::move(aliases)](const TString& connection) {
        const auto it = aliases.find(connection);
        Y_ENSURE(it != aliases.end(), "Unknown or inaccessible external service connection");
        return NThreading::MakeFuture(TResolvedServiceConnection{ it->second, {} });
    });
}

void RegisterServiceTransforms(NYql::NDq::TDqAsyncIoFactory& factory, const NConfig::TWasmServicesConfig& config,
    std::shared_ptr<ITvmTicketProvider> tvmProvider, ui32 userPool, NMonitoring::TDynamicCounterPtr counters) {
    if (!tvmProvider) {
        tvmProvider = CreateTvmTicketProvider();
    }
    RegisterServiceTransformsWithResolver(
        factory, config, [tvmProvider](const TString& invocation, const THashMap<TString, TString>& secureParams) {
            const auto entry = secureParams.find(ServiceConnectionKey(ServiceInvocationConnection(invocation)));
            Y_ENSURE(entry != secureParams.end(), "External service connection is unavailable");
            FederatedQuery::ExternalService service;
            Y_ENSURE(google::protobuf::util::JsonStringToMessage(entry->second, &service).ok() && ValidateExternalService(service, true).Empty(),
                "Invalid external service connection");
            TBinding binding;
            binding.Protocol = service.protocol() == FederatedQuery::ExternalService::HTTP ? EProtocol::Http : EProtocol::Grpc;
            binding.Endpoint = service.endpoint();
            binding.CaCertificate = service.ca_certificate();
            for (const auto& [key, value] : service.headers()) {
                binding.Headers.emplace_back(key, value);
            }
            if (service.has_tvm_auth()) {
                Y_ENSURE(tvmProvider->IsAvailable(),
                    "TVM ticket provider is not implemented; source/destination credentials are supported, but ticket acquisition is a stub");
                binding.TvmProvider = tvmProvider;
                binding.TvmCredentials = { service.tvm_auth().source_id(), service.tvm_auth().destination_id(), service.tvm_auth().secret() };
            }
            if (service.auth().has_token()) {
                binding.Headers.emplace_back(
                    binding.Protocol == EProtocol::Http ? "Authorization" : "authorization", "Bearer " + service.auth().token().token());
            }
            if (binding.Protocol == EProtocol::Grpc) {
                grpc::SslCredentialsOptions options;
                options.pem_root_certs = service.ca_certificate();
                binding.GrpcCredentials = service.insecure() ? grpc::InsecureChannelCredentials() : grpc::SslCredentials(options);
            }
            return binding;
        }, userPool, std::move(counters));
}

}   // namespace NFq::NWasmServices
