#include "s3_storage.h"
#include "s3_storage_config.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/threading/Executor.h>

#ifndef KIKIMR_DISABLE_S3_OPS
namespace NKikimr::NWrappers::NExternalStorage {

class TS3ThreadsPoolByEndpoint {
private:
    class TPool {
    public:
        std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> Executor;
        ui32 ThreadsCount = 0;

        TPool(const std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor>& executor, const ui32 threadsCount)
            : Executor(executor)
            , ThreadsCount(threadsCount)
        {
        }
    };

    THashMap<TString, TPool> Pools;
    TMutex Mutex;

    std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> GetPoolImpl(const TString& endpoint, const ui32 threadsCount) {
        TGuard<TMutex> g(Mutex);
        auto it = Pools.find(endpoint);
        if (it == Pools.end()) {
            TPool pool(std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(threadsCount), threadsCount);
            it = Pools.emplace(endpoint, std::move(pool)).first;
        } else if (it->second.ThreadsCount < threadsCount) {
            TPool pool(std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(threadsCount), threadsCount);
            it->second = std::move(pool);
        }
        return it->second.Executor;
    }

public:
    static std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> GetPool(const TString& endpoint, const ui32 threadsCount) {
        return Singleton<TS3ThreadsPoolByEndpoint>()->GetPoolImpl(endpoint, threadsCount);
    }
};

namespace {

namespace NPrivate {

template <class TMessage, class TEnum>
Aws::Http::Scheme ParseSchemeImpl(TEnum scheme, bool abortOnFailure = true) {
    switch (scheme) {
        case TMessage::HTTP:
            return Aws::Http::Scheme::HTTP;
        case TMessage::HTTPS:
            return Aws::Http::Scheme::HTTPS;
        default:
            if (abortOnFailure) {
                Y_ABORT("Unknown scheme");
            }
            return Aws::Http::Scheme::HTTP;
    }
}

Aws::Http::Scheme ParseScheme(NKikimrSchemeOp::TS3Settings::EScheme scheme, bool abortOnFailure = true) {
    return ParseSchemeImpl<NKikimrSchemeOp::TS3Settings>(scheme, abortOnFailure);
}

Aws::Http::Scheme ParseScheme(Ydb::Import::ImportFromS3Settings::Scheme scheme, bool abortOnFailure = true) {
    return ParseSchemeImpl<Ydb::Import::ImportFromS3Settings>(scheme, abortOnFailure);
}

Aws::Http::Scheme ParseScheme(Ydb::Export::ExportToS3Settings::Scheme scheme, bool abortOnFailure = true) {
    return ParseSchemeImpl<Ydb::Export::ExportToS3Settings>(scheme, abortOnFailure);
}

template <typename TSettings>
Aws::Client::ClientConfiguration ConfigFromSettings(const TSettings& settings) {
    Aws::Client::ClientConfiguration config;

    // get default value from proto
    auto threadsCount = NKikimrSchemeOp::TS3Settings::default_instance().GetExecutorThreadsCount();

    config.endpointOverride = settings.endpoint();
    config.executor = TS3ThreadsPoolByEndpoint::GetPool(settings.endpoint(), threadsCount);
    config.enableTcpKeepAlive = true;
    config.verifySSL = false;
    config.connectTimeoutMs = 10000;
    config.maxConnections = threadsCount;

    config.scheme = NPrivate::ParseScheme(settings.scheme());

    return config;
}

template <typename TSettings>
Aws::Auth::AWSCredentials CredentialsFromSettings(const TSettings& settings) {
    return Aws::Auth::AWSCredentials(settings.access_key(), settings.secret_key());
}

} // namespace NPrivate

} // anonymous

Aws::Client::ClientConfiguration TS3ExternalStorageConfig::ConfigFromSettings(const NKikimrSchemeOp::TS3Settings& settings) {
    Aws::Client::ClientConfiguration config;

    config.endpointOverride = settings.GetEndpoint();
    if (settings.HasConnectionTimeoutMs()) {
        config.connectTimeoutMs = settings.GetConnectionTimeoutMs();
    }

    if (settings.HasRequestTimeoutMs()) {
        config.requestTimeoutMs = settings.GetRequestTimeoutMs();
    }

    if (settings.HasHttpRequestTimeoutMs()) {
        config.httpRequestTimeoutMs = settings.GetHttpRequestTimeoutMs();
    }

    config.executor = TS3ThreadsPoolByEndpoint::GetPool(settings.GetEndpoint(), settings.GetExecutorThreadsCount());
    config.enableTcpKeepAlive = true;
    config.maxConnections = settings.HasMaxConnectionsCount() ? settings.GetMaxConnectionsCount() : settings.GetExecutorThreadsCount();
    config.caPath = "/etc/ssl/certs";

    config.scheme = NPrivate::ParseScheme(settings.GetScheme());

    if (settings.HasRegion()) {
        config.region = settings.GetRegion();
    }

    if (settings.HasVerifySSL()) {
        config.verifySSL = settings.GetVerifySSL();
    }

    if (settings.HasProxyHost()) {
        config.proxyHost = settings.GetProxyHost();
        config.proxyPort = settings.GetProxyPort();

        config.proxyScheme = NPrivate::ParseScheme(settings.GetProxyScheme(), false);
    }

    return config;
}

Aws::Client::ClientConfiguration TS3ExternalStorageConfig::ConfigFromSettings(const Ydb::Import::ImportFromS3Settings& settings) {
    return NPrivate::ConfigFromSettings(settings);
}

Aws::Client::ClientConfiguration TS3ExternalStorageConfig::ConfigFromSettings(const Ydb::Import::ListObjectsInS3ExportSettings& settings) {
    return NPrivate::ConfigFromSettings(settings);
}

Aws::Client::ClientConfiguration TS3ExternalStorageConfig::ConfigFromSettings(const Ydb::Export::ExportToS3Settings& settings) {
    return NPrivate::ConfigFromSettings(settings);
}

Aws::Auth::AWSCredentials TS3ExternalStorageConfig::CredentialsFromSettings(const NKikimrSchemeOp::TS3Settings& settings) {
    return Aws::Auth::AWSCredentials(settings.GetAccessKey(), settings.GetSecretKey());
}

Aws::Auth::AWSCredentials TS3ExternalStorageConfig::CredentialsFromSettings(const Ydb::Import::ImportFromS3Settings& settings) {
    return NPrivate::CredentialsFromSettings(settings);
}

Aws::Auth::AWSCredentials TS3ExternalStorageConfig::CredentialsFromSettings(const Ydb::Import::ListObjectsInS3ExportSettings& settings) {
    return NPrivate::CredentialsFromSettings(settings);
}

Aws::Auth::AWSCredentials TS3ExternalStorageConfig::CredentialsFromSettings(const Ydb::Export::ExportToS3Settings& settings) {
    return NPrivate::CredentialsFromSettings(settings);
}

TString TS3ExternalStorageConfig::DoGetStorageId() const {
    return TString(Config.endpointOverride.data(), Config.endpointOverride.size());
}

IExternalStorageOperator::TPtr TS3ExternalStorageConfig::DoConstructStorageOperator(bool verbose) const {
    return std::make_shared<TS3ExternalStorage>(Config, Credentials, Bucket, StorageClass, verbose, UseVirtualAddressing);
}

TS3ExternalStorageConfig::TS3ExternalStorageConfig(const Ydb::Import::ImportFromS3Settings& settings)
    : Config(ConfigFromSettings(settings))
    , Credentials(CredentialsFromSettings(settings))
    , UseVirtualAddressing(!settings.disable_virtual_addressing())
{
    Bucket = settings.bucket();
}

TS3ExternalStorageConfig::TS3ExternalStorageConfig(const Ydb::Import::ListObjectsInS3ExportSettings& settings)
    : Config(ConfigFromSettings(settings))
    , Credentials(CredentialsFromSettings(settings))
    , UseVirtualAddressing(!settings.disable_virtual_addressing())
{
    Bucket = settings.bucket();
}

TS3ExternalStorageConfig::TS3ExternalStorageConfig(const Ydb::Export::ExportToS3Settings& settings)
    : Config(ConfigFromSettings(settings))
    , Credentials(CredentialsFromSettings(settings))
    , UseVirtualAddressing(!settings.disable_virtual_addressing())
{
    Bucket = settings.bucket();
}

TS3ExternalStorageConfig::TS3ExternalStorageConfig(
        const Aws::Auth::AWSCredentials& credentials,
        const Aws::Client::ClientConfiguration& config,
        const TString& bucket)
    : Config(config)
    , Credentials(credentials)
{
    Bucket = bucket;
}

TS3ExternalStorageConfig::TS3ExternalStorageConfig(const NKikimrSchemeOp::TS3Settings& settings)
    : Config(ConfigFromSettings(settings))
    , Credentials(CredentialsFromSettings(settings))
    , StorageClass(ConvertStorageClass(settings.GetStorageClass()))
    , UseVirtualAddressing(settings.GetUseVirtualAddressing())
{
    Bucket = settings.GetBucket();
}

Aws::S3::Model::StorageClass TS3ExternalStorageConfig::ConvertStorageClass(const Ydb::Export::ExportToS3Settings::StorageClass storage) {
    switch (storage) {
        case Ydb::Export::ExportToS3Settings::STANDARD:
            return Aws::S3::Model::StorageClass::STANDARD;
        case Ydb::Export::ExportToS3Settings::STANDARD_IA:
            return Aws::S3::Model::StorageClass::STANDARD_IA;
        case Ydb::Export::ExportToS3Settings::REDUCED_REDUNDANCY:
            return Aws::S3::Model::StorageClass::REDUCED_REDUNDANCY;
        case Ydb::Export::ExportToS3Settings::ONEZONE_IA:
            return Aws::S3::Model::StorageClass::ONEZONE_IA;
        case Ydb::Export::ExportToS3Settings::INTELLIGENT_TIERING:
            return Aws::S3::Model::StorageClass::INTELLIGENT_TIERING;
        case Ydb::Export::ExportToS3Settings::GLACIER:
            return Aws::S3::Model::StorageClass::GLACIER;
        case Ydb::Export::ExportToS3Settings::DEEP_ARCHIVE:
            return Aws::S3::Model::StorageClass::DEEP_ARCHIVE;
        case Ydb::Export::ExportToS3Settings::OUTPOSTS:
            return Aws::S3::Model::StorageClass::OUTPOSTS;
        case Ydb::Export::ExportToS3Settings::STORAGE_CLASS_UNSPECIFIED:
            [[fallthrough]];
        default:
            return Aws::S3::Model::StorageClass::NOT_SET;
    }
}

}

#endif // KIKIMR_DISABLE_S3_OPS
