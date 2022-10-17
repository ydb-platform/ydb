#include "s3_storage.h"
#include "s3_storage_config.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/internal/AWSHttpResourceClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/utils/stream/ResponseStream.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/curl/include/curl/curl.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/log.h>
#include <util/string/cast.h>

#ifndef KIKIMR_DISABLE_S3_OPS
namespace NKikimr::NWrappers::NExternalStorage {

using namespace Aws;
using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace Aws::Utils::Stream;

namespace {

struct TCurlInitializer {
    TCurlInitializer() {
        curl_global_init(CURL_GLOBAL_ALL);
    }

    ~TCurlInitializer() {
        curl_global_cleanup();
    }
};

struct TApiInitializer {
    TApiInitializer() {
        Options.httpOptions.initAndCleanupCurl = false;
        InitAPI(Options);

        Internal::CleanupEC2MetadataClient(); // speeds up config construction
    }

    ~TApiInitializer() {
        ShutdownAPI(Options);
    }

private:
    SDKOptions Options;
};

class TApiOwner {
public:
    void Ref() {
        auto guard = Guard(Mutex);
        if (!RefCount++) {
            if (!CurlInitializer) {
                CurlInitializer.Reset(new TCurlInitializer);
            }
            ApiInitializer.Reset(new TApiInitializer);
        }
    }

    void UnRef() {
        auto guard = Guard(Mutex);
        if (!--RefCount) {
            ApiInitializer.Destroy();
        }
    }

private:
    ui64 RefCount = 0;
    TMutex Mutex;
    THolder<TCurlInitializer> CurlInitializer;
    THolder<TApiInitializer> ApiInitializer;
};

} // anonymous

TS3User::TS3User(const TS3User& /*baseObject*/) {
    Singleton<TApiOwner>()->Ref();
}

TS3User::TS3User(TS3User& /*baseObject*/) {
    Singleton<TApiOwner>()->Ref();
}

TS3User::TS3User() {
    Singleton<TApiOwner>()->Ref();
}

TS3User::~TS3User() {
    Singleton<TApiOwner>()->UnRef();
}

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
    config.enableTcpKeepAlive = true;
    //    config.lowSpeedLimit = 0;
    config.maxConnections = 5;
    config.caPath = "/etc/ssl/certs";

    switch (settings.GetScheme()) {
        case NKikimrSchemeOp::TS3Settings::HTTP:
            config.scheme = Aws::Http::Scheme::HTTP;
            break;
        case NKikimrSchemeOp::TS3Settings::HTTPS:
            config.scheme = Aws::Http::Scheme::HTTPS;
            break;
        default:
            Y_FAIL("Unknown scheme");
    }

    if (settings.HasRegion()) {
        config.region = settings.GetRegion();
    }

    if (settings.HasVerifySSL()) {
        config.verifySSL = settings.GetVerifySSL();
    }

    if (settings.HasProxyHost()) {
        config.proxyHost = settings.GetProxyHost();
        config.proxyPort = settings.GetProxyPort();

        switch (settings.GetProxyScheme()) {
            case NKikimrSchemeOp::TS3Settings::HTTP:
                config.proxyScheme = Aws::Http::Scheme::HTTP;
                break;
            case NKikimrSchemeOp::TS3Settings::HTTPS:
                config.proxyScheme = Aws::Http::Scheme::HTTPS;
                break;
            default:
                break;
        }
    }

    return config;
}

Aws::Client::ClientConfiguration TS3ExternalStorageConfig::ConfigFromSettings(const Ydb::Import::ImportFromS3Settings& settings) {
    Aws::Client::ClientConfiguration config;

    config.endpointOverride = settings.endpoint();
    config.verifySSL = false;
    config.connectTimeoutMs = 10000;
    config.maxConnections = 5;

    switch (settings.scheme()) {
        case Ydb::Import::ImportFromS3Settings::HTTP:
            config.scheme = Http::Scheme::HTTP;
            break;
        case Ydb::Import::ImportFromS3Settings::HTTPS:
            config.scheme = Http::Scheme::HTTPS;
            break;
        default:
            Y_FAIL("Unknown scheme");
    }

    return config;
}

Aws::Auth::AWSCredentials TS3ExternalStorageConfig::CredentialsFromSettings(const NKikimrSchemeOp::TS3Settings& settings) {
    return Aws::Auth::AWSCredentials(settings.GetAccessKey(), settings.GetSecretKey());
}

Aws::Auth::AWSCredentials TS3ExternalStorageConfig::CredentialsFromSettings(const Ydb::Import::ImportFromS3Settings& settings) {
    return Aws::Auth::AWSCredentials(settings.access_key(), settings.secret_key());
}

TString TS3ExternalStorageConfig::DoGetStorageId() const {
    return TString(Config.endpointOverride.data(), Config.endpointOverride.size());
}

IExternalStorageOperator::TPtr TS3ExternalStorageConfig::DoConstructStorageOperator() const {
    return std::make_shared<TS3ExternalStorage>(Config, Credentials);
}
}

#endif // KIKIMR_DISABLE_S3_OPS
