#include "dq_ydb_read_actor.h"
#include "read_stream.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NDq {

void RegisterYdbReadActorFactory(TDqAsyncIoFactory& factory,
    TYdbDriverFactory driverFactory, IStructuredTokenCredentialsFactory::TPtr credentialsFactory) {
    factory.RegisterSource<NYdb::TSource>("Ydb",
        [driverFactory = std::move(driverFactory), credentialsFactory = std::move(credentialsFactory)](
            NYdb::TSource&& source, IDqAsyncIoFactory::TSourceArguments&& args) {
            NYdb::ValidateSource(source);
            YQL_ENSURE(args.ReadRanges.size() <= 1, "Ydb version 1 supports a single split");
            ::NYdb::NQuery::TClientSettings clientSettings;
            clientSettings.DiscoveryEndpoint(source.GetEndpoint());
            clientSettings.Database(source.GetDatabase());
            clientSettings.DiscoveryMode(::NYdb::EDiscoveryMode::Off);
            clientSettings.SslCredentials(::NYdb::TSslCredentials(source.GetUseTls()));
            if (source.GetToken().empty()) {
                clientSettings.CredentialsProviderFactory(::NYdb::CreateInsecureCredentialsProviderFactory());
            } else {
                const auto it = args.SecureParams.find(source.GetToken());
                YQL_ENSURE(it != args.SecureParams.end(), "Ydb secure parameter is missing");
                YQL_ENSURE(credentialsFactory, "Ydb credentials factory is missing");
                // The structured secret stays in the credential provider, never in source settings or diagnostics.
                try {
                    clientSettings.CredentialsProviderFactory(credentialsFactory->Create(it->second));
                } catch (...) {
                    ythrow yexception() << "Ydb credentials could not be initialized";
                }
            }
            auto client = std::make_shared<::NYdb::NQuery::TQueryClient>(driverFactory(source.GetUseTls()), clientSettings);
            NNative::TReadActorSettings settings;
            settings.Timeout = TDuration::MilliSeconds(source.GetReadTimeoutMs());
            settings.MaxBatchBytes = source.GetMaxBatchBytes();
            settings.MaxRowBytes = NYdb::MaxOutputRowBytes;
            settings.MaxRetries = source.GetMaxRetries();
            for (const auto& column : source.GetColumns()) {
                settings.Columns.push_back(column.GetName());
            }
            return NNative::CreateNativeReadActor(
                [client, source = std::move(source)](const NNative::TReadContext& context) {
                    return NYdb::CreateReadStream(client, source, context);
                }, std::move(settings), std::move(args));
        });
}

void RegisterYdbReadActorFactory(TDqAsyncIoFactory& factory,
    const ::NYdb::TDriver& driver, const ::NYdb::TDriver& tlsDriver, IStructuredTokenCredentialsFactory::TPtr credentialsFactory) {
    RegisterYdbReadActorFactory(factory,
        [driver, tlsDriver](bool useTls) { return useTls ? tlsDriver : driver; }, std::move(credentialsFactory));
}

} // namespace NYql::NDq
