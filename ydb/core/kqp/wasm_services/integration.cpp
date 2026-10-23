#include "connection.h"
#include "integration.h"

#include <ydb/core/external_sources/external_service.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yql/providers/function/provider/dq_function_provider.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <algorithm>

namespace NKikimr::NKqp {
using namespace NFq::NWasmServices;

void RegisterKqpWasmServiceProvider(NYql::TTypeAnnotationContext& types, const NFq::NConfig::TWasmServicesConfig& config,
    TIntrusivePtr<NYql::IKikimrGateway> gateway, const TString& cluster, NExternalSource::IExternalSourceFactory::TPtr externalSources) {
    if (!config.GetEnabled() || !externalSources->IsAvailableProvider(TString(NYql::FunctionProviderName))) {
        return;
    }
    auto state = MakeIntrusive<NYql::NDqFunction::TDqFunctionState>();
    state->Types = &types;
    state->GatewayFactory = CreateServiceGatewayFactoryWithResolver(config, [gateway, cluster, externalSources](const TString& connection) {
        Y_ENSURE(connection.StartsWith('/'), "KQP WASM service CONNECTION must be an absolute database path");
        return gateway
            ->LoadTableMetadata(cluster, connection,
                NYql::IKikimrGateway::TLoadTableMetadataSettings()
                    .WithExternalDatasources(true)
                    .WithAuthInfo(false)
                    .WithRequiredAccess(NACLib::SelectRow)
                    .WithExternalSourceFactory(externalSources))
            .Apply([connection](const auto& future) {
                const auto& result = future.GetValue();
                Y_ENSURE(result.Success() && result.Metadata && result.Metadata->IsExternalDataSource(),
                    "Unknown or inaccessible external service connection");
                return TResolvedServiceConnection{ connection, SerializeWasmServiceConnection(*result.Metadata) };
            });
    });
    types.AddDataSource(NYql::FunctionProviderName, NYql::NDqFunction::CreateDqFunctionDataSource(state));
    types.AddDataSink(NYql::FunctionProviderName, NYql::NDqFunction::CreateDqFunctionDataSink(state));
}

void RegisterKqpWasmServiceTransforms(NYql::NDq::TDqAsyncIoFactory& factory, const NFq::NConfig::TWasmServicesConfig& config,
    std::shared_ptr<ITvmTicketProvider> tvmProvider, ui32 userPool, NMonitoring::TDynamicCounterPtr counters) {
    if (!config.GetEnabled()) {
        return;
    }
    if (!tvmProvider) {
        tvmProvider = CreateTvmTicketProvider();
    }
    RegisterServiceTransformsWithResolver(
        factory, config, [tvmProvider](const TString& invocation, const THashMap<TString, TString>& secureParams) {
            const auto settings = NExternalSource::TServiceConnectionSettings::Deserialize(ServiceInvocationSettings(invocation));
            TBinding binding;
            binding.Protocol = settings.Protocol == "HTTP" ? EProtocol::Http : EProtocol::Grpc;
            binding.Endpoint = settings.Endpoint;
            binding.CaCertificate = settings.CaCertificate;
            if (settings.SecretReference) {
                const auto entry = secureParams.find(ServiceConnectionKey(ServiceInvocationConnection(invocation)));
                Y_ENSURE(entry != secureParams.end(), "External service credential is unavailable");
                const auto& secret = entry->second;
                if (settings.TvmSourceId) {
                    Y_ENSURE(!secret.empty() && secret.size() <= 4096, "Invalid external service TVM secret");
                    Y_ENSURE(tvmProvider->IsAvailable(), "TVM ticket provider is not implemented");
                    binding.TvmProvider = tvmProvider;
                    binding.TvmCredentials = { settings.TvmSourceId, settings.TvmDestinationId, secret };
                } else {
                    Y_ENSURE(!secret.empty() && secret.size() <= 1024 && std::all_of(secret.begin(), secret.end(), [](unsigned char c) {
                        return c > 32 && c < 127;
                    }), "Invalid external service token");
                    binding.Headers.emplace_back(binding.Protocol == EProtocol::Http ? "Authorization" : "authorization", "Bearer " + secret);
                }
            }
            if (binding.Protocol == EProtocol::Grpc) {
                grpc::SslCredentialsOptions options;
                options.pem_root_certs = settings.CaCertificate;
                binding.GrpcCredentials = settings.UseTls ? grpc::SslCredentials(options) : grpc::InsecureChannelCredentials();
            }
            return binding;
        }, userPool, std::move(counters));
}

}   // namespace NKikimr::NKqp
