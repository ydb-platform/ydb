#include "aws.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>

#if !defined(_win32_)
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/curl/CurlHttpClient.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include <cstdint>
#include <exception>
#include <streambuf>

#include <util/system/compiler.h>
#endif

namespace NYdb::NConsoleClient {

const TString TCommandWithAwsCredentials::AwsCredentialsFile = "~/.aws/credentials";
const TString TCommandWithAwsCredentials::AwsDefaultProfileName = "default";

TString TCommandWithAwsCredentials::ReadIniKey(const TString& iniKey) {
    using namespace NIniConfig;

    const auto fileName = "AWS Credentials";
    const auto& profileName = AwsProfile.GetOrElse(AwsDefaultProfileName);

    try {
        if (!Config) {
            TString filePath = AwsCredentialsFile;
            const auto content = ReadFromFile(filePath, fileName);
            Config.ConstructInPlace(TConfig::ReadIni(content));
        }

        const auto& profiles = Config->Get<TDict>();
        if (!profiles.contains(profileName)) {
            throw yexception() << fileName << " file does not contain a profile '" << profileName << "'";
        }

        const auto& profile = profiles.At(profileName).Get<TDict>();
        if (!profile.contains(iniKey)) {
            throw yexception() << "Invalid profile '" << profileName << "' in " << fileName << " file";
        }

        return profile.At(iniKey).As<TString>();
    } catch (const TConfigError& ex) {
        throw yexception() << "Invalid " << fileName << " file: " << ex.what();
    }
}

#if defined(_win32_)
std::unique_ptr<IS3ClientWrapper> CreateS3ClientWrapper(const NImport::TImportFromS3Settings& settings) {
    throw yexception() << "AWS API is not supported for windows platform";
}

void InitAwsAPI() {
    throw yexception() << "AWS API is not supported for windows platform";
}

void ShutdownAwsAPI() {
    throw yexception() << "AWS API is not supported for windows platform";
}
#else

// Set while GetObject is streaming a body into the caller's checksum.
// An SDK retry would append a second body onto that checksum, so streaming
// reads are not retried here. List and Head keep the SDK retry policy.
// Validation retries the whole read and resets the checksum first.
thread_local bool TlStreamingGet = false;

class TStreamingRetryStrategy : public Aws::Client::DefaultRetryStrategy {
public:
    using Aws::Client::DefaultRetryStrategy::DefaultRetryStrategy;

    bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error, long attemptedRetries) const override {
        if (TlStreamingGet) {
            return false;
        }
        return Aws::Client::DefaultRetryStrategy::ShouldRetry(error, attemptedRetries);
    }
};

// The SDK forces HTTP/2, whose stream window starts at 64 KiB. Bulk reads of
// many large objects are faster as one long HTTP/1.1 response per worker.
class TBulkCurlHttpClient : public Aws::Http::CurlHttpClient {
public:
    using Aws::Http::CurlHttpClient::CurlHttpClient;

protected:
    void OverrideOptionsOnConnectionHandle(CURL* handle) const override {
        curl_easy_setopt(handle, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
        // Default receive buffer is 16 KiB. A larger buffer cuts write-callback
        // overhead on a wide link. Curl clamps this to its max read size.
        curl_easy_setopt(handle, CURLOPT_BUFFERSIZE, static_cast<long>(1 << 20));
    }
};

class TBulkHttpClientFactory : public Aws::Http::HttpClientFactory {
public:
    std::shared_ptr<Aws::Http::HttpClient> CreateHttpClient(const Aws::Client::ClientConfiguration& clientConfiguration) const override {
        return Aws::MakeShared<TBulkCurlHttpClient>("BulkS3", clientConfiguration);
    }

    std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(
        const Aws::String& uri,
        Aws::Http::HttpMethod method,
        const Aws::IOStreamFactory& streamFactory) const override
    {
        return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
    }

    std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(
        const Aws::Http::URI& uri,
        Aws::Http::HttpMethod method,
        const Aws::IOStreamFactory& streamFactory) const override
    {
        auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>("BulkS3", uri, method);
        request->SetResponseStreamFactory(streamFactory);
        return request;
    }

    void InitStaticState() override {
        Aws::Http::CurlHttpClient::InitGlobalState();
    }

    void CleanupStaticState() override {
        Aws::Http::CurlHttpClient::CleanupGlobalState();
    }
};

class TS3ClientWrapper : public IS3ClientWrapper {
public:
    TS3ClientWrapper(const NImport::TImportFromS3Settings& settings) 
        : Bucket(settings.Bucket_)
    {
        Aws::S3::S3ClientConfiguration config;
        config.endpointOverride = settings.Endpoint_;
        if (settings.Scheme_ == ES3Scheme::HTTP) {
            config.scheme = Aws::Http::Scheme::HTTP;
        } else if (settings.Scheme_ == ES3Scheme::HTTPS) {
            config.scheme = Aws::Http::Scheme::HTTPS;
        } else {
            throw TMisuseException() << "\"" << settings.Scheme_ << "\" scheme type is not supported";
        }
        config.useVirtualAddressing = settings.UseVirtualAddressing_;
        // One streaming GET holds one connection for the whole object. The SDK
        // default of 25 leaves the rest of the worker threads waiting on the
        // curl handle pool. The pool grows up to this cap as workers start.
        config.maxConnections = 512;
        // A burst of TLS handshakes can take longer than the SDK's 1s default.
        config.connectTimeoutMs = 10000;
        config.retryStrategy = Aws::MakeShared<TStreamingRetryStrategy>("BulkS3", 10L);

        Client = std::make_unique<Aws::S3::S3Client>(
            Aws::Auth::AWSCredentials(settings.AccessKey_, settings.SecretKey_),
            config,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            settings.UseVirtualAddressing_);
    }

    TListS3Result ListObjectKeys(const TString& prefix, const std::optional<TString>& token) override {
        auto request = Aws::S3::Model::ListObjectsV2Request()
            .WithBucket(Bucket)
            .WithPrefix(prefix);
        if (token) {
            request.WithContinuationToken(*token);
        }
        auto response = Client->ListObjectsV2(request);
        if (!response.IsSuccess()) {
            throw TMisuseException() << "ListObjectKeys error: " << response.GetError().GetMessage();
        }
        TListS3Result result;
        for (const auto& object : response.GetResult().GetContents()) {
            result.Keys.push_back(TString(object.GetKey()));
        }
        if (response.GetResult().GetIsTruncated()) {
            result.NextToken = TString(response.GetResult().GetNextContinuationToken());
        }
        return result;
    }

    bool ObjectExists(const TString& key) override {
        auto response = Client->HeadObject(Aws::S3::Model::HeadObjectRequest()
            .WithBucket(Bucket)
            .WithKey(key));
        if (response.IsSuccess()) {
            return true;
        }
        const auto& error = response.GetError();
        if (error.GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND
            || error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY
            || error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND)
        {
            return false;
        }
        throw TMisuseException() << "HeadObject error: " << error.GetMessage();
    }

    void GetObject(const TString& key, const std::function<void(TStringBuf)>& onChunk) override {
        // Many backup objects are already in flight, one per worker. Splitting
        // each of them into short ranges adds a request per chunk and keeps
        // most workers on a single sequential range once the extra-connection
        // budget is spent. One GET per object lets TCP ramp for the whole file.
        // Bytes are handed to the checksum as curl writes them, so an object is
        // not buffered whole.
        struct TGuard {
            TGuard() {
                TlStreamingGet = true;
            }
            ~TGuard() {
                TlStreamingGet = false;
            }
        } guard;
        Y_UNUSED(guard);

        struct TState {
            const std::function<void(TStringBuf)>& OnChunk;
            std::uint64_t Bytes = 0;
            std::exception_ptr Error;
            bool Failed = false;
        } state{onChunk, 0, nullptr, false};

        class TChunkStreamBuf : public std::streambuf {
        public:
            explicit TChunkStreamBuf(TState& state)
                : State(state)
            {
            }

        protected:
            std::streamsize xsputn(const char* s, std::streamsize n) override {
                if (n <= 0 || State.Failed) {
                    return n < 0 ? 0 : n;
                }
                try {
                    State.OnChunk(TStringBuf(s, static_cast<size_t>(n)));
                    State.Bytes += static_cast<std::uint64_t>(n);
                } catch (...) {
                    State.Error = std::current_exception();
                    State.Failed = true;
                }
                return n;
            }

            int_type overflow(int_type ch) override {
                if (traits_type::eq_int_type(ch, traits_type::eof())) {
                    return traits_type::not_eof(ch);
                }
                const char byte = traits_type::to_char_type(ch);
                xsputn(&byte, 1);
                return ch;
            }

        private:
            TState& State;
        };

        auto request = Aws::S3::Model::GetObjectRequest()
            .WithBucket(Bucket)
            .WithKey(key);
        request.SetResponseStreamFactory([&state]() -> Aws::IOStream* {
            return Aws::New<Aws::Utils::Stream::DefaultUnderlyingStream>(
                "BulkS3",
                Aws::MakeUnique<TChunkStreamBuf>("BulkS3", state));
        });

        // Keep reading after onChunk fails so an HTTP error body still produces
        // an S3 error. Further bytes are discarded by the stream buffer.
        auto response = Client->GetObject(request);
        if (!response.IsSuccess()) {
            throw TMisuseException() << "GetObject error: " << response.GetError().GetMessage();
        }
        if (state.Error) {
            std::rethrow_exception(state.Error);
        }
        const long long expected = response.GetResult().GetContentLength();
        if (expected < 0 || static_cast<unsigned long long>(expected) != state.Bytes) {
            throw TMisuseException() << "GetObject error: incomplete read of " << key;
        }
    }

private:
    std::unique_ptr<Aws::S3::S3Client> Client;
    const TString Bucket;
};

std::unique_ptr<IS3ClientWrapper> CreateS3ClientWrapper(const NImport::TImportFromS3Settings& settings) {
    return std::make_unique<TS3ClientWrapper>(settings);
}

void InitAwsAPI() {
    Aws::SDKOptions options;
    options.httpOptions.httpClientFactory_create_fn = [] {
        return Aws::MakeShared<TBulkHttpClientFactory>("BulkS3");
    };
    Aws::InitAPI(options);
}

void ShutdownAwsAPI() {
    Aws::ShutdownAPI(Aws::SDKOptions());
}
#endif

}
