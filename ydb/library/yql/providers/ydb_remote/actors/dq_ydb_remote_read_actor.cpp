#include "dq_ydb_remote_read_actor.h"
#include "read_stream.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NDq {

void RegisterYdbRemoteReadActorFactory(TDqAsyncIoFactory& factory,
    TYdbRemoteDriverFactory driverFactory, IStructuredTokenCredentialsFactory::TPtr credentialsFactory) {
    factory.RegisterSource<NYdbRemote::TSource>("YdbRemote",
        [driverFactory = std::move(driverFactory), credentialsFactory = std::move(credentialsFactory)](
            NYdbRemote::TSource&& source, IDqAsyncIoFactory::TSourceArguments&& args) {
            NYdbRemote::ValidateSource(source);
            YQL_ENSURE(args.ReadRanges.size() <= 1, "YdbRemote version 1 supports a single split");
            NYdb::NQuery::TClientSettings clientSettings;
            clientSettings.DiscoveryEndpoint(source.GetEndpoint());
            clientSettings.Database(source.GetDatabase());
            clientSettings.DiscoveryMode(NYdb::EDiscoveryMode::Off);
            clientSettings.SslCredentials(NYdb::TSslCredentials(source.GetUseTls()));
            if (source.GetToken().empty()) {
                clientSettings.CredentialsProviderFactory(NYdb::CreateInsecureCredentialsProviderFactory());
            } else {
                const auto it = args.SecureParams.find(source.GetToken());
                YQL_ENSURE(it != args.SecureParams.end(), "YdbRemote secure parameter is missing");
                YQL_ENSURE(credentialsFactory, "YdbRemote credentials factory is missing");
                // The structured secret stays in the credential provider, never in source settings or diagnostics.
                try {
                    clientSettings.CredentialsProviderFactory(credentialsFactory->Create(it->second));
                } catch (...) {
                    ythrow yexception() << "YdbRemote credentials could not be initialized";
                }
            }
            auto client = std::make_shared<NYdb::NQuery::TQueryClient>(driverFactory(source.GetUseTls()), clientSettings);
            NNative::TReadActorSettings settings;
            settings.Timeout = TDuration::MilliSeconds(source.GetReadTimeoutMs());
            settings.MaxBatchBytes = source.GetMaxBatchBytes();
            settings.MaxRowBytes = NYdbRemote::MaxOutputRowBytes;
            settings.MaxRetries = source.GetMaxRetries();
            for (const auto& column : source.GetColumns()) {
                settings.Columns.push_back(column.GetName());
            }
            return NNative::CreateNativeReadActor(
                [client, source = std::move(source)](const NNative::TReadContext& context) {
                    return NYdbRemote::CreateReadStream(client, source, context);
                }, std::move(settings), std::move(args));
        });
}

void RegisterYdbRemoteReadActorFactory(TDqAsyncIoFactory& factory,
    const NYdb::TDriver& driver, const NYdb::TDriver& tlsDriver, IStructuredTokenCredentialsFactory::TPtr credentialsFactory) {
    RegisterYdbRemoteReadActorFactory(factory,
        [driver, tlsDriver](bool useTls) { return useTls ? tlsDriver : driver; }, std::move(credentialsFactory));
}

} // namespace NYql::NDq
