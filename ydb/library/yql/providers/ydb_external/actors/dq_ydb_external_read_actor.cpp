#include "dq_ydb_external_read_actor.h"
#include "read_stream.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NDq {

void RegisterYdbExternalReadActorFactory(TDqAsyncIoFactory& factory,
    TYdbExternalDriverFactory driverFactory, IStructuredTokenCredentialsFactory::TPtr credentialsFactory) {
    factory.RegisterSource<NYdbExternal::TSource>("YdbExternal",
        [driverFactory = std::move(driverFactory), credentialsFactory = std::move(credentialsFactory)](
            NYdbExternal::TSource&& source, IDqAsyncIoFactory::TSourceArguments&& args) {
            NYdbExternal::ValidateSource(source);
            YQL_ENSURE(args.ReadRanges.size() <= 1, "YdbExternal version 1 supports a single split");
            NYdb::NQuery::TClientSettings clientSettings;
            clientSettings.DiscoveryEndpoint(source.GetEndpoint());
            clientSettings.Database(source.GetDatabase());
            clientSettings.DiscoveryMode(NYdb::EDiscoveryMode::Off);
            clientSettings.SslCredentials(NYdb::TSslCredentials(source.GetUseTls()));
            if (source.GetToken().empty()) {
                clientSettings.CredentialsProviderFactory(NYdb::CreateInsecureCredentialsProviderFactory());
            } else {
                const auto it = args.SecureParams.find(source.GetToken());
                YQL_ENSURE(it != args.SecureParams.end(), "YdbExternal secure parameter is missing");
                YQL_ENSURE(credentialsFactory, "YdbExternal credentials factory is missing");
                // The structured secret stays in the credential provider, never in source settings or diagnostics.
                try {
                    clientSettings.CredentialsProviderFactory(credentialsFactory->Create(it->second));
                } catch (...) {
                    ythrow yexception() << "YdbExternal credentials could not be initialized";
                }
            }
            auto client = std::make_shared<NYdb::NQuery::TQueryClient>(driverFactory(source.GetUseTls()), clientSettings);
            NNative::TReadActorSettings settings;
            settings.Timeout = TDuration::MilliSeconds(source.GetReadTimeoutMs());
            settings.MaxBatchBytes = source.GetMaxBatchBytes();
            settings.MaxRowBytes = NYdbExternal::MaxOutputRowBytes;
            settings.MaxRetries = source.GetMaxRetries();
            for (const auto& column : source.GetColumns()) {
                settings.Columns.push_back(column.GetName());
            }
            return NNative::CreateNativeReadActor(
                [client, source = std::move(source)](const NNative::TReadContext& context) {
                    return NYdbExternal::CreateReadStream(client, source, context);
                }, std::move(settings), std::move(args));
        });
}

void RegisterYdbExternalReadActorFactory(TDqAsyncIoFactory& factory,
    const NYdb::TDriver& driver, const NYdb::TDriver& tlsDriver, IStructuredTokenCredentialsFactory::TPtr credentialsFactory) {
    RegisterYdbExternalReadActorFactory(factory,
        [driver, tlsDriver](bool useTls) { return useTls ? tlsDriver : driver; }, std::move(credentialsFactory));
}

} // namespace NYql::NDq
