#include "aws.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>

#if !defined(_win32_)
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <exception>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include <util/string/builder.h>
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

// Extra threads that download ranges of large objects. The calling thread is
// already one of the validation workers, so these are only the additional GETs.
class TRangeSlots {
public:
    static TRangeSlots& Instance() {
        static TRangeSlots slots;
        return slots;
    }

    unsigned Acquire(unsigned want) {
        std::lock_guard<std::mutex> lock(Mu);
        const unsigned grant = std::min(want, Capacity - Used);
        Used += grant;
        return grant;
    }

    void Release(unsigned count) {
        std::lock_guard<std::mutex> lock(Mu);
        Used -= count;
    }

    static constexpr unsigned Capacity = 64;

private:
    std::mutex Mu;
    unsigned Used = 0;
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
        // One GetObject holds one connection. Range reads of a large object need more
        // than the SDK default of 25, or extra threads wait on the curl handle pool.
        config.maxConnections = 128;

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
        // A single GET is one TCP stream. On a wide link that tops out around
        // several hundred MB/s while CPU and the NIC still have room. Objects
        // larger than one part are split into 16 MiB ranges and downloaded
        // concurrently; chunks are delivered in order, so checksums stay valid.
        constexpr long long PartBytes = 16ll << 20;
        const long long length = ContentLengthOrZero(key);
        if (length <= PartBytes) {
            ReadWholeObject(key, onChunk);
            return;
        }
        ReadObjectByRanges(key, length, PartBytes, onChunk);
    }

private:
    // Zero when size is unknown. The caller then uses one GET.
    long long ContentLengthOrZero(const TString& key) {
        auto response = Client->HeadObject(Aws::S3::Model::HeadObjectRequest()
            .WithBucket(Bucket)
            .WithKey(key));
        if (!response.IsSuccess()) {
            return 0;
        }
        return response.GetResult().GetContentLength();
    }

    void ReadWholeObject(const TString& key, const std::function<void(TStringBuf)>& onChunk) {
        auto response = Client->GetObject(Aws::S3::Model::GetObjectRequest()
            .WithBucket(Bucket)
            .WithKey(key));
        if (!response.IsSuccess()) {
            throw TMisuseException() << "GetObject error: " << response.GetError().GetMessage();
        }
        auto& body = response.GetResult().GetBody();
        TString buf;
        buf.resize(1 << 20);
        while (body) {
            body.read(buf.begin(), buf.size());
            const auto read = body.gcount();
            if (read > 0) {
                onChunk(TStringBuf(buf.data(), static_cast<size_t>(read)));
            }
            if (body.eof()) {
                break;
            }
            if (!body) {
                throw TMisuseException() << "GetObject error: failed to read object body";
            }
        }
    }

    TString DownloadRange(const TString& key, long long start, long long end) {
        const auto expected = static_cast<size_t>(end - start + 1);
        const TString range = TStringBuilder() << "bytes=" << start << "-" << end;
        TString lastError;
        for (int attempt = 1; attempt <= 3; ++attempt) {
            auto response = Client->GetObject(Aws::S3::Model::GetObjectRequest()
                .WithBucket(Bucket)
                .WithKey(key)
                .WithRange(range.c_str()));
            if (!response.IsSuccess()) {
                lastError = response.GetError().GetMessage();
                continue;
            }
            auto& body = response.GetResult().GetBody();
            TString data;
            data.resize(expected);
            body.read(data.begin(), expected);
            if (static_cast<size_t>(body.gcount()) != expected) {
                lastError = "short range read";
                continue;
            }
            return data;
        }
        throw TMisuseException() << "GetObject error: " << lastError;
    }

    void ReadObjectByRanges(
        const TString& key,
        long long length,
        long long partBytes,
        const std::function<void(TStringBuf)>& onChunk)
    {
        const size_t parts = static_cast<size_t>((length + partBytes - 1) / partBytes);
        constexpr unsigned kRangesPerObject = 8;
        const unsigned extra = TRangeSlots::Instance().Acquire(
            std::min<unsigned>(kRangesPerObject, static_cast<unsigned>(std::min<size_t>(parts, 8))));
        struct TReleaseSlots {
            unsigned Count = 0;
            ~TReleaseSlots() {
                if (Count != 0) {
                    TRangeSlots::Instance().Release(Count);
                }
            }
        } release{extra};

        if (extra == 0) {
            for (size_t index = 0; index < parts; ++index) {
                const long long start = static_cast<long long>(index) * partBytes;
                const long long end = std::min(length - 1, start + partBytes - 1);
                const TString data = DownloadRange(key, start, end);
                onChunk(data);
            }
            return;
        }

        const size_t window = extra;
        std::atomic<bool> failed{false};
        std::exception_ptr error;
        std::mutex mu;
        std::condition_variable cv;
        size_t nextToStart = 0;
        size_t nextToEmit = 0;
        std::map<size_t, TString> ready;

        auto download = [&] {
            while (!failed.load()) {
                size_t index = 0;
                {
                    std::unique_lock<std::mutex> lock(mu);
                    cv.wait(lock, [&] {
                        return failed.load()
                            || nextToStart >= parts
                            || (nextToStart < nextToEmit + window && ready.size() < window);
                    });
                    if (failed.load() || nextToStart >= parts) {
                        return;
                    }
                    index = nextToStart++;
                }
                cv.notify_all();
                try {
                    const long long start = static_cast<long long>(index) * partBytes;
                    const long long end = std::min(length - 1, start + partBytes - 1);
                    TString data = DownloadRange(key, start, end);
                    std::lock_guard<std::mutex> lock(mu);
                    ready.emplace(index, std::move(data));
                } catch (...) {
                    std::lock_guard<std::mutex> lock(mu);
                    if (!failed.exchange(true)) {
                        error = std::current_exception();
                    }
                }
                cv.notify_all();
            }
        };

        std::vector<std::thread> workers;
        workers.reserve(extra);
        for (unsigned i = 0; i < extra; ++i) {
            workers.emplace_back(download);
        }
        struct TJoinWorkers {
            std::vector<std::thread>& Workers;
            ~TJoinWorkers() {
                for (std::thread& worker : Workers) {
                    if (worker.joinable()) {
                        worker.join();
                    }
                }
            }
        } join{workers};

        try {
            while (nextToEmit < parts) {
                TString data;
                {
                    std::unique_lock<std::mutex> lock(mu);
                    cv.wait(lock, [&] {
                        return failed.load() || ready.contains(nextToEmit);
                    });
                    if (!ready.contains(nextToEmit)) {
                        break;
                    }
                    data = std::move(ready[nextToEmit]);
                    ready.erase(nextToEmit);
                    ++nextToEmit;
                }
                cv.notify_all();
                onChunk(data);
            }
        } catch (...) {
            failed.store(true);
            cv.notify_all();
            throw;
        }
        failed.store(true);
        cv.notify_all();
        for (std::thread& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        if (nextToEmit < parts) {
            if (error) {
                std::rethrow_exception(error);
            }
            throw TMisuseException() << "GetObject error: incomplete read of " << key;
        }
    }

    std::unique_ptr<Aws::S3::S3Client> Client;
    const TString Bucket;
};

std::unique_ptr<IS3ClientWrapper> CreateS3ClientWrapper(const NImport::TImportFromS3Settings& settings) {
    return std::make_unique<TS3ClientWrapper>(settings);
}

void InitAwsAPI() {
    Aws::InitAPI(Aws::SDKOptions());
}

void ShutdownAwsAPI() {
    Aws::ShutdownAPI(Aws::SDKOptions());
}
#endif

}
