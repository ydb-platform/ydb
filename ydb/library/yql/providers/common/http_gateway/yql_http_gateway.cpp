#include "yql_aws_signature.h"
#include "yql_dns_gateway.h"
#include "yql_http_gateway.h"

#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <ydb/library/yql/utils/log/log.h>

#include <thread>
#include <mutex>
#include <stack>
#include <queue>

#ifdef PROFILE_MEMORY_ALLOCATIONS
#include <ydb/library/actors/prof/tag.h>
#endif

namespace {

int curlTrace(CURL *handle, curl_infotype type,
    char *data, size_t size,
    void *userp) {
    Y_UNUSED(handle);
    Y_UNUSED(userp);
    Y_UNUSED(size);
    Y_UNUSED(data);

    TStringBuf buf(data, size);
    TStringBuilder sb;
    switch (type) {
    case CURLINFO_TEXT:
        sb << "== Info: " << buf;
        break;
    case CURLINFO_HEADER_OUT:
        sb << "=> Send header (" << size << " bytes):" << Endl << buf;
        break;
    case CURLINFO_HEADER_IN:
        sb << "<= Recv header (" << size << " bytes):" << buf;
        break;
    /*case CURLINFO_DATA_OUT:
        sb << "=> Send data (" << size << " bytes)" << Endl;
        break;
    case CURLINFO_SSL_DATA_OUT:
        sb << "=> Send SSL data (" << size << " bytes)" << Endl;
        break;
    case CURLINFO_DATA_IN:
        sb << "<= Recv data (" << size << " bytes)" << Endl;
        break;
    case CURLINFO_SSL_DATA_IN:
        sb << "<= Recv SSL data (" << size << " bytes)" << Endl;
        break;*/
    default:
        return 0;
    }

    Cerr << sb;

    return 0;
}
}

namespace NYql {
namespace {

struct TCurlInitConfig {
    ui64 RequestTimeout = 150;
    ui64 LowSpeedTime = 0;
    ui64 LowSpeedLimit = 0;
    ui64 ConnectionTimeout = 15;
    ui64 BytesPerSecondLimit = 0;
    ui64 BufferSize = CURL_MAX_WRITE_SIZE;
};

// some WinNT macros clash
#if defined(DELETE)
#undef DELETE
#endif

class TEasyCurl {
public:
    using TPtr = std::shared_ptr<TEasyCurl>;

    enum class EMethod {
        GET,
        POST,
        PUT,
        DELETE
    };

    TEasyCurl(
        const ::NMonitoring::TDynamicCounters::TCounterPtr& counter,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& downloadedBytes,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& uploadedBytes,
        TString url,
        IHTTPGateway::THeaders headers,
        EMethod method,
        size_t offset = 0ULL,
        size_t sizeLimit = 0,
        size_t bodySize = 0,
        const TCurlInitConfig& config = TCurlInitConfig(),
        TDNSGateway<>::TDNSConstCurlListPtr dnsCache = nullptr,
        TString data = {})
        : Headers(std::move(headers))
        , Method(method)
        , Offset(offset)
        , SizeLimit(sizeLimit)
        , BodySize(bodySize)
        , Counter(counter)
        , DownloadedBytes(downloadedBytes)
        , UploadedBytes(uploadedBytes)
        , Config(config)
        , ErrorBuffer(static_cast<size_t>(CURL_ERROR_SIZE), '\0')
        , DnsCache(dnsCache)
        , Url(url)
        , Data(std::move(data)) {
        InitHandles();
        Counter->Inc();
    }

    virtual ~TEasyCurl() {
        Counter->Dec();
        FreeHandles();
    }

    void InitHandles() {
        if (Handle) {
            return;
        }

        std::fill(ErrorBuffer.begin(), ErrorBuffer.end(), 0);

        Handle = curl_easy_init();
        switch (Method) {
            case EMethod::GET:
                break;
            case EMethod::POST:
                curl_easy_setopt(Handle, CURLOPT_POST, 1L);
                break;
            case EMethod::PUT:
                curl_easy_setopt(Handle, CURLOPT_UPLOAD, 1L);
                break;
            case EMethod::DELETE:
                curl_easy_setopt(Handle, CURLOPT_CUSTOMREQUEST, "DELETE");
                break;
        }

        // does nothing if CURLOPT_VERBOSE is not set to 1
        curl_easy_setopt(Handle, CURLOPT_DEBUGFUNCTION, curlTrace);
        // We can do this because we are using async DNS resolver (c-ares). https://curl.se/libcurl/c/CURLOPT_NOSIGNAL.html
        curl_easy_setopt(Handle, CURLOPT_NOSIGNAL, 1L);

        // for local debug only
        // will print tokens in HTTP headers
        // curl_easy_setopt(Handle, CURLOPT_VERBOSE, 1L);

        curl_easy_setopt(Handle, CURLOPT_URL, Url.c_str());
        curl_easy_setopt(Handle, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
        curl_easy_setopt(Handle, CURLOPT_USERAGENT, "YQ HTTP gateway");
        curl_easy_setopt(Handle, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(Handle, CURLOPT_CONNECTTIMEOUT, Config.ConnectionTimeout);
        curl_easy_setopt(Handle, CURLOPT_MAX_RECV_SPEED_LARGE, Config.BytesPerSecondLimit);
        curl_easy_setopt(Handle, CURLOPT_BUFFERSIZE, Config.BufferSize);
        curl_easy_setopt(Handle, CURLOPT_TIMEOUT, Config.RequestTimeout);
        curl_easy_setopt(Handle, CURLOPT_LOW_SPEED_TIME, Config.LowSpeedTime);
        curl_easy_setopt(Handle, CURLOPT_LOW_SPEED_LIMIT, Config.LowSpeedLimit);
        curl_easy_setopt(Handle, CURLOPT_ERRORBUFFER, ErrorBuffer.data());

        if (Headers.Options.CurlSignature) {
            if (Headers.Options.AwsSigV4) {
                curl_easy_setopt(Handle, CURLOPT_AWS_SIGV4, Headers.Options.AwsSigV4.c_str());
            }

            if (Headers.Options.UserPwd) {
                curl_easy_setopt(Handle, CURLOPT_USERPWD, Headers.Options.UserPwd.c_str());
            }
        } else if (Headers.Options.AwsSigV4 || Headers.Options.UserPwd) {
            TString method;
            switch (Method) {
                case EMethod::GET:
                    method = "GET";
                    break;
                case EMethod::POST:
                    method = "POST";
                    break;
                case EMethod::PUT:
                    method = "PUT";
                    break;
                case EMethod::DELETE:
                    method = "DELETE";
                    break;
            }

            TString contentType;
            for (const auto& field: Headers.Fields) {
                if (field.StartsWith("Content-Type:")) {
                    contentType = field.substr(strlen("Content-Type:"));
                }
            }
            TAwsSignature signature(method, Url, contentType, Data, Headers.Options.AwsSigV4, Headers.Options.UserPwd);
            Headers.Fields.push_back(TStringBuilder{} << "Authorization: " << signature.GetAuthorization());
            Headers.Fields.push_back(TStringBuilder{} << "x-amz-content-sha256: " << signature.GetXAmzContentSha256());
            Headers.Fields.push_back(TStringBuilder{} << "x-amz-date: " << signature.GetAmzDate());
        }

        if (DnsCache != nullptr) {
            curl_easy_setopt(Handle, CURLOPT_RESOLVE, DnsCache.get());
        }

        if (!Headers.Fields.empty()) {
            CurlHeaders = std::accumulate(
                Headers.Fields.cbegin(),
                Headers.Fields.cend(),
                CurlHeaders,
                std::bind(
                    &curl_slist_append,
                    std::placeholders::_1,
                    std::bind(&TString::c_str, std::placeholders::_2)));
            curl_easy_setopt(Handle, CURLOPT_HTTPHEADER, CurlHeaders);
        }

        curl_easy_setopt(Handle, CURLOPT_WRITEFUNCTION, &WriteMemoryCallback);
        curl_easy_setopt(Handle, CURLOPT_WRITEDATA, static_cast<void*>(this));

        if (Method == EMethod::PUT) {
            curl_easy_setopt(Handle, CURLOPT_HEADERFUNCTION, &WriteHeaderCallback);
            curl_easy_setopt(Handle, CURLOPT_HEADERDATA, static_cast<void*>(this));
            curl_easy_setopt(Handle, CURLOPT_INFILESIZE_LARGE, BodySize);
        }

        if (Method == EMethod::POST) {
            curl_easy_setopt(Handle, CURLOPT_POSTFIELDSIZE, BodySize);
        }

        if (BodySize) {
            curl_easy_setopt(Handle, CURLOPT_READFUNCTION, &ReadMemoryCallback);
            curl_easy_setopt(Handle, CURLOPT_READDATA, static_cast<void*>(this));
        }
        SkipTo(0ULL);
    }

    void FreeHandles() {
        if (Handle) {
            curl_easy_cleanup(Handle);
            Handle = nullptr;
        }
        if (Headers.Fields) {
            curl_slist_free_all(CurlHeaders);
            CurlHeaders = nullptr;
        }
    }

    CURL* GetHandle() const {
        return Handle;
    }

    EMethod GetMethod() const {
        return Method;
    }

    virtual void Fail(CURLcode result, const TIssue& error) = 0;
    virtual void Done(CURLcode result, long httpResponseCode) = 0;

    virtual size_t Write(void* contents, size_t size, size_t nmemb) = 0;
    virtual size_t WriteHeader(void* contents, size_t size, size_t nmemb) = 0;
    virtual size_t Read(char *buffer, size_t size, size_t nmemb) = 0;

    size_t GetSizeLimit() const { return SizeLimit; }
    TString GetDetailedErrorText() const { return ErrorBuffer.data(); }
protected:
    void SkipTo(size_t offset) const {
        if (offset || Offset || SizeLimit) {
            TStringBuilder byteRange;
            byteRange << Offset + offset << '-';
            if (SizeLimit)
                byteRange << Offset + SizeLimit - 1;
            curl_easy_setopt(Handle, CURLOPT_RANGE, byteRange.c_str());
        }
    }
private:
    static size_t
    WriteMemoryCallback(void* contents, size_t size, size_t nmemb, void* userp) {
        const auto self = static_cast<TEasyCurl*>(userp);
        const auto res = self->Write(contents, size, nmemb);
        self->DownloadedBytes->Add(res);
        return res;
    };

    static size_t
    WriteHeaderCallback(void* contents, size_t size, size_t nmemb, void* userp) {
        const auto self = static_cast<TEasyCurl*>(userp);
        const auto res = self->WriteHeader(contents, size, nmemb);
        return res;
    };

    static size_t
    ReadMemoryCallback(char *buffer, size_t size, size_t nmemb, void *userp) {
        const auto self = static_cast<TEasyCurl*>(userp);
        const auto res = self->Read(buffer, size, nmemb);
        self->UploadedBytes->Add(res);
        return res;
    };

    IHTTPGateway::THeaders Headers;
    const EMethod Method;
    const size_t Offset;
    const size_t SizeLimit;
    const size_t BodySize;
    CURL* Handle = nullptr;
    curl_slist* CurlHeaders = nullptr;
    const ::NMonitoring::TDynamicCounters::TCounterPtr Counter;
    const ::NMonitoring::TDynamicCounters::TCounterPtr DownloadedBytes;
    const ::NMonitoring::TDynamicCounters::TCounterPtr UploadedBytes;
    const TCurlInitConfig Config;
    std::vector<char> ErrorBuffer;
    TDNSGateway<>::TDNSConstCurlListPtr DnsCache;
public:
    TString Url;
    const TString Data;
};

class TEasyCurlBuffer : public TEasyCurl {
public:
    using TPtr = std::shared_ptr<TEasyCurlBuffer>;
    using TWeakPtr = std::weak_ptr<TEasyCurlBuffer>;

    TEasyCurlBuffer(
        const ::NMonitoring::TDynamicCounters::TCounterPtr& counter,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& downloadedBytes,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& uploadededBytes,
        TString url,
        EMethod method,
        TString data,
        IHTTPGateway::THeaders headers,
        size_t offset,
        size_t sizeLimit,
        IHTTPGateway::TOnResult callback,
        IHTTPGateway::TRetryPolicy::IRetryState::TPtr retryState,
        const TCurlInitConfig& config = TCurlInitConfig(),
        TDNSGateway<>::TDNSConstCurlListPtr dnsCache = nullptr)
        : TEasyCurl(
              counter,
              downloadedBytes,
              uploadededBytes,
              url,
              std::move(headers),
              method,
              offset,
              sizeLimit,
              data.size(),
              std::move(config),
              std::move(dnsCache),
              std::move(data))
        , Input(Data)
        , Output(Buffer)
        , HeaderOutput(Header)
        , RetryState(std::move(retryState)) {
        Output.Reserve(sizeLimit);
        Callbacks.emplace(std::move(callback));
    }

    static TPtr Make(
        const ::NMonitoring::TDynamicCounters::TCounterPtr& counter,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& downloadedBytes,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& uploadededBytes,
        TString url,
        EMethod method,
        TString data,
        IHTTPGateway::THeaders headers,
        size_t offset,
        size_t sizeLimit,
        IHTTPGateway::TOnResult callback,
        IHTTPGateway::TRetryPolicy::IRetryState::TPtr retryState,
        const TCurlInitConfig& config = TCurlInitConfig(),
        TDNSGateway<>::TDNSConstCurlListPtr dnsCache = nullptr) {
        return std::make_shared<TEasyCurlBuffer>(
            counter,
            downloadedBytes,
            uploadededBytes,
            std::move(url),
            method,
            std::move(data),
            std::move(headers),
            offset,
            sizeLimit,
            std::move(callback),
            std::move(retryState),
            std::move(config),
            std::move(dnsCache));
    }

    // return true if callback successfully added to this work
    bool AddCallback(IHTTPGateway::TOnResult callback) {
        const std::unique_lock lock(SyncCallbacks);
        if (Callbacks.empty())
            return false;
        Callbacks.emplace(std::move(callback));
        return true;
    }

    TMaybe<TDuration> GetNextRetryDelay(CURLcode curlResponseCode, long httpResponseCode) const {
        if (RetryState)
            return RetryState->GetNextRetryDelay(curlResponseCode, httpResponseCode);

        return {};
    }

    void Reset() {
        Buffer.clear();
        Header.clear();
        TStringOutput(Buffer).Swap(Output);
        TStringOutput(Header).Swap(HeaderOutput);
        TStringInput(Data).Swap(Input);
        FreeHandles();
        InitHandles();
    }
private:
    void Fail(CURLcode result, const TIssue& error) final  {
        TIssues issues{error};
        const std::unique_lock lock(SyncCallbacks);
        while (!Callbacks.empty()) {
            Callbacks.top()(IHTTPGateway::TResult(result, issues));
            Callbacks.pop();
        }
    }

    void Done(CURLcode result, long httpResponseCode) final {
        if (CURLE_OK != result)
            return Fail(result, TIssue( TStringBuilder{} << curl_easy_strerror(result) << ". Detailed: " << GetDetailedErrorText()));

        const std::unique_lock lock(SyncCallbacks);
        while (!Callbacks.empty()) {
            if (1U == Callbacks.size())
                Callbacks.top()(IHTTPGateway::TContent(std::move(Buffer), httpResponseCode, std::move(Header)));
            else
                Callbacks.top()(IHTTPGateway::TContent(Buffer, httpResponseCode, Header));
            Callbacks.pop();
        }
    }

    size_t Write(void* contents, size_t size, size_t nmemb) final {
        const auto realsize = size * nmemb;
        Output.Write(contents, realsize);
        return realsize;
    }

    size_t WriteHeader(void* contents, size_t size, size_t nmemb) final {
        const auto realsize = size * nmemb;
        HeaderOutput.Write(contents, realsize);
        return realsize;
    }

    size_t Read(char *buffer, size_t size, size_t nmemb) final {
        return Input.Read(buffer, size * nmemb);
    }

    TString Buffer, Header;
    TStringInput Input;
    TStringOutput Output, HeaderOutput;

    std::mutex SyncCallbacks;
    std::stack<IHTTPGateway::TOnResult> Callbacks;
    const IHTTPGateway::TRetryPolicy::IRetryState::TPtr RetryState;
};

class TEasyCurlStream : public TEasyCurl {
public:
    using TPtr = std::shared_ptr<TEasyCurlStream>;
    using TWeakPtr = std::weak_ptr<TEasyCurlStream>;

    TEasyCurlStream(
        const ::NMonitoring::TDynamicCounters::TCounterPtr& counter,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& downloadedBytes,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& uploadededBytes,
        TString url, IHTTPGateway::THeaders headers,
        size_t offset,
        size_t sizeLimit,
        IHTTPGateway::TOnDownloadStart onStart,
        IHTTPGateway::TOnNewDataPart onNewData,
        IHTTPGateway::TOnDownloadFinish onFinish,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& inflightCounter,
        const TCurlInitConfig& config = TCurlInitConfig(),
        TDNSGateway<>::TDNSConstCurlListPtr dnsCache = nullptr)
        : TEasyCurl(counter, downloadedBytes, uploadededBytes, url, std::move(headers), EMethod::GET, offset, sizeLimit, 0ULL, std::move(config), std::move(dnsCache))
        , OnStart(std::move(onStart))
        , OnNewData(std::move(onNewData))
        , OnFinish(std::move(onFinish))
        , Counter(std::make_shared<std::atomic_size_t>(0ULL))
        , InflightCounter(inflightCounter)
    {}

    static TPtr Make(
        const ::NMonitoring::TDynamicCounters::TCounterPtr& counter,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& downloadedBytes,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& uploadededBytes,
        TString url,
        IHTTPGateway::THeaders headers,
        size_t offset,
        size_t sizeLimit,
        IHTTPGateway::TOnDownloadStart onStart,
        IHTTPGateway::TOnNewDataPart onNewData,
        IHTTPGateway::TOnDownloadFinish onFinish,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& inflightCounter,
        const TCurlInitConfig& config = TCurlInitConfig(),
        TDNSGateway<>::TDNSConstCurlListPtr dnsCache = nullptr)
    {
        return std::make_shared<TEasyCurlStream>(counter, downloadedBytes, uploadededBytes, std::move(url), std::move(headers), offset, sizeLimit, std::move(onStart), std::move(onNewData), std::move(onFinish), inflightCounter, std::move(config), std::move(dnsCache));
    }

    enum class EAction : i8 {
        Drop = -2,
        Stop = -1,
        None = 0,
        Work = 1,
        Init = 2
    };

    EAction GetAction(size_t buffersSize) {
        if (!Started) {
            Started = true;
            return EAction::Init;
        }

        if (Cancelled) {
            return EAction::Drop;
        }
        if (buffersSize && Paused != Counter->load() >= buffersSize) {
            Paused = !Paused;
            return Paused ? EAction::Stop : EAction::Work;
        }

        return EAction::None;
    }

    void Cancel(TIssue issue) {
        Cancelled = true;
        OnFinish(CURLE_OK, TIssues{issue});
    }
private:
    void Fail(CURLcode result, const TIssue& error) final  {
        if (!Cancelled)
            OnFinish(result, TIssues{error});
    }

    void MaybeStart(CURLcode result, long httpResponseCode = 0) {
        if (!HttpResponseCode) {
            if (!httpResponseCode) {
                curl_easy_getinfo(GetHandle(), CURLINFO_RESPONSE_CODE, &httpResponseCode);
            }
            HttpResponseCode = httpResponseCode;
            OnStart(result, HttpResponseCode);
        }
    }

    void Done(CURLcode result, long httpResponseCode) final {
        MaybeStart(result, httpResponseCode);
        if (CURLE_OK != result) {
            return Fail(result, TIssue(TStringBuilder{} << "error: " << curl_easy_strerror(result) << " detailed: " << GetDetailedErrorText()));
        }
        if (!Cancelled)
            OnFinish(result, TIssues());
    }

    size_t Write(void* contents, size_t size, size_t nmemb) final {
        MaybeStart(CURLE_OK);
        const auto realsize = size * nmemb;
        if (!Cancelled)
            OnNewData(IHTTPGateway::TCountedContent(TString(static_cast<char*>(contents), realsize), Counter, InflightCounter));
        return realsize;
    }

    size_t WriteHeader(void*, size_t, size_t) final { return 0ULL; }
    size_t Read(char*, size_t, size_t) final { return 0ULL; }

    const IHTTPGateway::TOnDownloadStart OnStart;
    const IHTTPGateway::TOnNewDataPart OnNewData;
    const IHTTPGateway::TOnDownloadFinish OnFinish;

    const std::shared_ptr<std::atomic_size_t> Counter;
    const ::NMonitoring::TDynamicCounters::TCounterPtr InflightCounter;
    bool Started = false;
    bool Paused = false;
    bool Cancelled = false;
    long HttpResponseCode = 0L;
};

using TKeyType = std::tuple<TString, size_t, IHTTPGateway::THeaders, TString, IHTTPGateway::TRetryPolicy::TPtr>;

class TKeyHash {
public:
    TKeyHash() : Hash(), HashPtr() {}

    size_t operator()(const TKeyType& key) const {
        const auto& headers = std::get<2U>(key);
        auto initHash = CombineHashes(CombineHashes(Hash(std::get<0U>(key)), std::get<1U>(key)), Hash(std::get<3U>(key)));
        initHash = CombineHashes(HashPtr(std::get<4U>(key)), initHash);
        initHash = CombineHashes(Hash(headers.Options.UserPwd), initHash);
        initHash = CombineHashes(Hash(headers.Options.AwsSigV4), initHash);
        return std::accumulate(headers.Fields.cbegin(), headers.Fields.cend(), initHash,
            [this](size_t hash, const TString& item) { return CombineHashes(hash, Hash(item)); });
    }
private:
    const std::hash<TString> Hash;
    const std::hash<IHTTPGateway::TRetryPolicy::TPtr> HashPtr;
};

class THTTPMultiGateway : public IHTTPGateway {
friend class IHTTPGateway;
public:
    using TPtr = std::shared_ptr<THTTPMultiGateway>;
    using TWeakPtr = std::weak_ptr<THTTPMultiGateway>;

    explicit THTTPMultiGateway(
        const THttpGatewayConfig* httpGatewaysCfg,
        ::NMonitoring::TDynamicCounterPtr counters)
        : DnsGateway(httpGatewaysCfg ? httpGatewaysCfg->GetDnsResolverConfig(): TDnsResolverConfig{}, counters->GetSubgroup("subsystem", "dns_gateway"))
        , Counters(std::move(counters))
        , Rps(Counters->GetCounter("Requests", true))
        , InFlight(Counters->GetCounter("InFlight"))
        , InFlightStreams(Counters->GetCounter("InFlightStreams"))
        , MaxInFlight(Counters->GetCounter("MaxInFlight"))
        , AllocatedMemory(Counters->GetCounter("AllocatedMemory"))
        , MaxAllocatedMemory(Counters->GetCounter("MaxAllocatedMemory"))
        , OutputMemory(Counters->GetCounter("OutputMemory"))
        , PerformCycles(Counters->GetCounter("PerformCycles", true))
        , AwaitQueue(Counters->GetCounter("AwaitQueue"))
        , AwaitQueueTopSizeLimit(Counters->GetCounter("AwaitQueueTopSizeLimit"))
        , DownloadedBytes(Counters->GetCounter("DownloadedBytes", true))
        , UploadedBytes(Counters->GetCounter("UploadedBytes", true))
        , GroupForGET(Counters->GetSubgroup("method", "GET"))
        , GroupForPUT(Counters->GetSubgroup("method", "PUT"))
        , GroupForPOST(Counters->GetSubgroup("method", "POST"))
    {
        if (httpGatewaysCfg) {
            if (httpGatewaysCfg->HasMaxInFlightCount()) {
                MaxHandlers = httpGatewaysCfg->GetMaxInFlightCount();
            }
            MaxInFlight->Set(MaxHandlers);

            if (httpGatewaysCfg->HasMaxSimulatenousDownloadsSize()) {
                MaxSimulatenousDownloadsSize = httpGatewaysCfg->GetMaxSimulatenousDownloadsSize();
            }
            MaxAllocatedMemory->Set(MaxSimulatenousDownloadsSize);

            if (httpGatewaysCfg->HasBuffersSizePerStream()) {
                BuffersSizePerStream = httpGatewaysCfg->GetBuffersSizePerStream();
            }

            if (httpGatewaysCfg->HasRequestTimeoutSeconds()) {
                InitConfig.RequestTimeout = httpGatewaysCfg->GetRequestTimeoutSeconds();
            }

            if (httpGatewaysCfg->HasLowSpeedTimeSeconds()) {
                InitConfig.LowSpeedTime = httpGatewaysCfg->GetLowSpeedTimeSeconds();
            }

            if (httpGatewaysCfg->HasLowSpeedBytesLimit()) {
                InitConfig.LowSpeedLimit = httpGatewaysCfg->GetLowSpeedBytesLimit();
            }

            if (httpGatewaysCfg->HasConnectionTimeoutSeconds()) {
                InitConfig.ConnectionTimeout = httpGatewaysCfg->GetConnectionTimeoutSeconds();
            }

            if (httpGatewaysCfg->HasBytesPerSecondLimit()) {
                InitConfig.BytesPerSecondLimit = httpGatewaysCfg->GetBytesPerSecondLimit();
            }

            if (httpGatewaysCfg->HasDownloadBufferBytesLimit()) {
                InitConfig.BufferSize = httpGatewaysCfg->GetDownloadBufferBytesLimit();
            }
        }

        InitCurl();
    }

    ~THTTPMultiGateway() {
        curl_multi_wakeup(Handle);
        IsStopped = true;
        if (Thread.joinable()) {
            Thread.join();
        }
        UninitCurl();
    }

private:
    size_t MaxHandlers = 1024U;
    size_t MaxSimulatenousDownloadsSize = 8_GB;
    size_t BuffersSizePerStream = CURL_MAX_WRITE_SIZE << 3U;
    TCurlInitConfig InitConfig;

    void InitCurl() {
        const CURLcode globalInitResult = curl_global_init(CURL_GLOBAL_ALL);
        if (globalInitResult != CURLE_OK) {
           throw yexception() << "curl_global_init error " << int(globalInitResult) << ": " << curl_easy_strerror(globalInitResult) << Endl;
        }
        Handle = curl_multi_init();
        if (!Handle) {
            throw yexception() << "curl_multi_init error";
        }
    }

    void UninitCurl() {
        Y_ABORT_UNLESS(Handle);
        const CURLMcode multiCleanupResult = curl_multi_cleanup(Handle);
        if (multiCleanupResult != CURLM_OK) {
            Cerr << "curl_multi_cleanup error " << int(multiCleanupResult) << ": " << curl_multi_strerror(multiCleanupResult) << Endl;
        }
        curl_global_cleanup();
    }

    void Perform() {
#ifdef PROFILE_MEMORY_ALLOCATIONS
        NProfiling::SetThreadAllocTag(NProfiling::MakeTag("HTTP_PERFORM"));
#endif
        OutputSize.store(0ULL);

        for (size_t handlers = 0U; !IsStopped;) {
            handlers = FillHandlers();
            PerformCycles->Inc();
            OutputMemory->Set(OutputSize);

            int running = 0;
            if (const auto c = curl_multi_perform(Handle, &running); CURLM_OK != c) {
                Fail(c);
                break;
            }

            if (running < int(handlers)) {
                for (int messages = int(handlers) - running; messages;) {
                    if (const auto msg = curl_multi_info_read(Handle, &messages)) {
                        if(msg->msg == CURLMSG_DONE) {
                            Done(msg->easy_handle, msg->data.result);
                        }
                    }
                }
            } else {
                const int timeoutMs = 300;
                if (const auto c = curl_multi_poll(Handle, nullptr, 0, timeoutMs, nullptr); CURLM_OK != c) {
                    Fail(c);
                    break;
                }
            }
        }
    }

    size_t FillHandlers() {
        const std::unique_lock lock(Sync);
        for (auto it = Streams.cbegin(); Streams.cend() != it;) {
            if (const auto& stream = it->lock()) {
                const auto streamHandle = stream->GetHandle();
                switch (stream->GetAction(BuffersSizePerStream)) {
                    case TEasyCurlStream::EAction::Init:
                        curl_multi_add_handle(Handle, streamHandle);
                        break;
                    case TEasyCurlStream::EAction::Work:
                        curl_easy_pause(streamHandle, CURLPAUSE_RECV_CONT);
                        break;
                    case TEasyCurlStream::EAction::Stop:
                        curl_easy_pause(streamHandle, CURL_WRITEFUNC_PAUSE);
                        break;
                    case TEasyCurlStream::EAction::Drop:
                        curl_multi_remove_handle(Handle, streamHandle);
                        Allocated.erase(streamHandle);
                        break;
                    case TEasyCurlStream::EAction::None:
                        break;
                }
                ++it;
            } else
                it = Streams.erase(it);
        }

        while (!Delayed.empty() && Delayed.top().first <= TInstant::Now()) {
            Await.emplace(std::move(Delayed.top().second));
            Delayed.pop();
        }

        const ui64 topSizeLimit = Await.empty() ? 0 : Await.front()->GetSizeLimit();
        AwaitQueueTopSizeLimit->Set(topSizeLimit);
        while (!Await.empty() && Allocated.size() < MaxHandlers && AllocatedSize + Await.front()->GetSizeLimit() <= MaxSimulatenousDownloadsSize) {
            AllocatedSize += Await.front()->GetSizeLimit();
            const auto handle = Await.front()->GetHandle();
            Allocated.emplace(handle, std::move(Await.front()));
            Await.pop();
            curl_multi_add_handle(Handle, handle);
        }
        AwaitQueue->Set(Await.size());
        AllocatedMemory->Set(AllocatedSize);
        return Allocated.size();
    }

    void Done(CURL* handle, CURLcode result) {
        TEasyCurl::TPtr easy;
        long httpResponseCode = 0L;
        {
            const std::unique_lock lock(Sync);
            if (const auto it = Allocated.find(handle); Allocated.cend() != it) {
                easy = std::move(it->second);
                TString codeLabel;
                TString codeValue;
                if (CURLE_OK == result) {
                    curl_easy_getinfo(easy->GetHandle(), CURLINFO_RESPONSE_CODE, &httpResponseCode);
                    codeLabel = "code";
                    codeValue = ToString(httpResponseCode);
                } else {
                    codeLabel = "curl_code";
                    codeValue = ToString((int)result);
                }

                TIntrusivePtr<::NMonitoring::TDynamicCounters> group;
                switch (easy->GetMethod()) {
                case TEasyCurl::EMethod::GET:
                    group = GroupForGET->GetSubgroup(codeLabel, codeValue);
                    break;
                case TEasyCurl::EMethod::POST:
                    group = GroupForPOST->GetSubgroup(codeLabel, codeValue);
                    break;
                case TEasyCurl::EMethod::PUT:
                    group = GroupForPUT->GetSubgroup(codeLabel, codeValue);
                    break;
                default:
                    break;
                }

                if (group) {
                    group->GetCounter("count", true)->Inc();
                }

                if (auto buffer = std::dynamic_pointer_cast<TEasyCurlBuffer>(easy)) {
                    AllocatedSize -= buffer->GetSizeLimit();
                    if (const auto& nextRetryDelay = buffer->GetNextRetryDelay(result, httpResponseCode)) {
                        buffer->Reset();
                        Delayed.emplace(nextRetryDelay->ToDeadLine(), std::move(buffer));
                        easy.reset();
                    }
                }
                Allocated.erase(it);
            }
        }
        if (easy) {
            easy->Done(result, httpResponseCode);
        }
    }

    void Fail(CURLMcode result) {
        std::stack<TEasyCurl::TPtr> works;
        {
            const std::unique_lock lock(Sync);

            for (auto& item : Allocated) {
                works.emplace(std::move(item.second));
            }

            AllocatedSize = 0ULL;
            Allocated.clear();
        }

        const TIssue error(curl_multi_strerror(result));
        while (!works.empty()) {
            curl_multi_remove_handle(Handle, works.top()->GetHandle());
            works.top()->Fail(CURLE_OK, error);
            works.pop();
        }
    }

    void Upload(TString url, THeaders headers, TString body, TOnResult callback, bool put, TRetryPolicy::TPtr retryPolicy) final {
        Rps->Inc();

        const std::unique_lock lock(Sync);
        auto easy = TEasyCurlBuffer::Make(InFlight, DownloadedBytes, UploadedBytes, std::move(url), put ? TEasyCurl::EMethod::PUT : TEasyCurl::EMethod::POST, std::move(body), std::move(headers), 0U, 0U, std::move(callback), retryPolicy ? retryPolicy->CreateRetryState() : nullptr, InitConfig, DnsGateway.GetDNSCurlList());
        Await.emplace(std::move(easy));
        Wakeup(0U);
    }

    void Delete(TString url, THeaders headers, TOnResult callback, TRetryPolicy::TPtr retryPolicy) final {
        Rps->Inc();

        const std::unique_lock lock(Sync);
        auto easy = TEasyCurlBuffer::Make(InFlight, DownloadedBytes, UploadedBytes, std::move(url), TEasyCurl::EMethod::DELETE, 0, std::move(headers), 0U, 0U, std::move(callback), retryPolicy ? retryPolicy->CreateRetryState() : nullptr, InitConfig, DnsGateway.GetDNSCurlList());
        Await.emplace(std::move(easy));
        Wakeup(0U);
    }

    void Download(
        TString url,
        THeaders headers,
        size_t offset,
        size_t sizeLimit,
        TOnResult callback,
        TString data,
        TRetryPolicy::TPtr retryPolicy) final
    {
        Rps->Inc();
        if (sizeLimit > MaxSimulatenousDownloadsSize) {
            TIssue error(TStringBuilder() << "Too big file for downloading: size " << sizeLimit << ", but limit is " << MaxSimulatenousDownloadsSize);
            callback(TResult(CURLE_OK, TIssues{error}));
            return;
        }
        const std::unique_lock lock(Sync);
        auto easy = TEasyCurlBuffer::Make(InFlight, DownloadedBytes, UploadedBytes, std::move(url), TEasyCurl::EMethod::GET, std::move(data), std::move(headers), offset, sizeLimit, std::move(callback), retryPolicy ? retryPolicy->CreateRetryState() : nullptr, InitConfig, DnsGateway.GetDNSCurlList());
        Await.emplace(std::move(easy));
        Wakeup(sizeLimit);
    }

    TCancelHook Download(
        TString url,
        THeaders headers,
        size_t offset,
        size_t sizeLimit,
        TOnDownloadStart onStart,
        TOnNewDataPart onNewData,
        TOnDownloadFinish onFinish,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& inflightCounter) final
    {
        auto stream = TEasyCurlStream::Make(InFlightStreams, DownloadedBytes, UploadedBytes, std::move(url), std::move(headers), offset, sizeLimit, std::move(onStart), std::move(onNewData), std::move(onFinish), inflightCounter, InitConfig, DnsGateway.GetDNSCurlList());
        const std::unique_lock lock(Sync);
        const auto handle = stream->GetHandle();
        TEasyCurlStream::TWeakPtr weak = stream;
        Streams.emplace_back(stream);
        Allocated.emplace(handle, std::move(stream));
        Wakeup(0ULL);
        return [weak](TIssue issue) {
            if (const auto& stream = weak.lock())
                stream->Cancel(issue);
        };
    }

    ui64 GetBuffersSizePerStream() final {
        return BuffersSizePerStream;
    }

    void OnRetry(TEasyCurlBuffer::TPtr easy) {
        const std::unique_lock lock(Sync);
        const size_t sizeLimit = easy->GetSizeLimit();
        Await.emplace(std::move(easy));
        Wakeup(sizeLimit);
    }

    void Wakeup(size_t sizeLimit) {
        AwaitQueue->Set(Await.size());
        if (Allocated.size() < MaxHandlers && AllocatedSize + sizeLimit + OutputSize.load() <= MaxSimulatenousDownloadsSize) {
            curl_multi_wakeup(Handle);
        }
    }

    CURLM* GetHandle() const {
        return Handle;
    }

private:
    CURLM* Handle = nullptr;

    std::queue<TEasyCurlBuffer::TPtr> Await;
    std::vector<TEasyCurlStream::TWeakPtr> Streams;


    std::unordered_map<CURL*, TEasyCurl::TPtr> Allocated;
    std::priority_queue<std::pair<TInstant, TEasyCurlBuffer::TPtr>> Delayed;

    std::mutex Sync;
    std::thread Thread;
    std::atomic<bool> IsStopped = false;

    size_t AllocatedSize = 0ULL;
    static std::atomic_size_t OutputSize;

    static std::mutex CreateSync;
    static TWeakPtr Singleton;

    TDNSGateway<> DnsGateway;

    const ::NMonitoring::TDynamicCounterPtr Counters;
    const ::NMonitoring::TDynamicCounters::TCounterPtr Rps;
    const ::NMonitoring::TDynamicCounters::TCounterPtr InFlight;
    const ::NMonitoring::TDynamicCounters::TCounterPtr InFlightStreams;
    const ::NMonitoring::TDynamicCounters::TCounterPtr MaxInFlight;
    const ::NMonitoring::TDynamicCounters::TCounterPtr AllocatedMemory;
    const ::NMonitoring::TDynamicCounters::TCounterPtr MaxAllocatedMemory;
    const ::NMonitoring::TDynamicCounters::TCounterPtr OutputMemory;
    const ::NMonitoring::TDynamicCounters::TCounterPtr PerformCycles;
    const ::NMonitoring::TDynamicCounters::TCounterPtr AwaitQueue;
    const ::NMonitoring::TDynamicCounters::TCounterPtr AwaitQueueTopSizeLimit;
    const ::NMonitoring::TDynamicCounters::TCounterPtr DownloadedBytes;
    const ::NMonitoring::TDynamicCounters::TCounterPtr UploadedBytes;
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> GroupForGET;
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> GroupForPUT;
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> GroupForPOST;
};

std::atomic_size_t THTTPMultiGateway::OutputSize = 0ULL;
std::mutex THTTPMultiGateway::CreateSync;
THTTPMultiGateway::TWeakPtr THTTPMultiGateway::Singleton;

}

IHTTPGateway::TContentBase::TContentBase(TString&& data)
    : TString(std::move(data))
{
    if (!empty()) {
        THTTPMultiGateway::OutputSize.fetch_add(size());
    }
}

IHTTPGateway::TContentBase::TContentBase(const TString& data)
    : TString(data)
{
    if (!empty()) {
        THTTPMultiGateway::OutputSize.fetch_add(size());
    }
}

IHTTPGateway::TContentBase::~TContentBase()
{
    if (!empty()) {
        THTTPMultiGateway::OutputSize.fetch_sub(size());
    }
}

TString IHTTPGateway::TContentBase::Extract() {
    if (!empty()) {
        THTTPMultiGateway::OutputSize.fetch_sub(size());
    }
    return std::move(*this);
}

IHTTPGateway::TContent::TContent(TString&& data, long httpResponseCode, TString&& headers)
    : TContentBase(std::move(data))
    , Headers(std::move(headers))
    , HttpResponseCode(httpResponseCode)
{}

IHTTPGateway::TContent::TContent(const TString& data, long httpResponseCode, const TString& headers)
    : TContentBase(data)
    , Headers(headers)
    , HttpResponseCode(httpResponseCode)
{}

IHTTPGateway::TCountedContent::TCountedContent(TString&& data, const std::shared_ptr<std::atomic_size_t>& counter,
    const ::NMonitoring::TDynamicCounters::TCounterPtr& inflightCounter)
    : TContentBase(std::move(data)), Counter(counter), InflightCounter(inflightCounter)
{
    Counter->fetch_add(size());
    if (InflightCounter) {
        InflightCounter->Add(size());
    }
}

IHTTPGateway::TCountedContent::~TCountedContent()
{
    Counter->fetch_sub(size());
    if (InflightCounter) {
        InflightCounter->Sub(size());
    }
}

TString IHTTPGateway::TCountedContent::Extract() {
    Counter->fetch_sub(size());
    if (InflightCounter) {
        InflightCounter->Sub(size());
    }
    return TContentBase::Extract();
}

IHTTPGateway::TPtr
IHTTPGateway::Make(const THttpGatewayConfig* httpGatewaysCfg, ::NMonitoring::TDynamicCounterPtr counters) {
    const std::unique_lock lock(THTTPMultiGateway::CreateSync);
    if (const auto g = THTTPMultiGateway::Singleton.lock())
        return g;

    const auto gateway = std::make_shared<THTTPMultiGateway>(httpGatewaysCfg, std::move(counters));
    THTTPMultiGateway::Singleton = gateway;

    gateway->Thread = std::thread([self = gateway.get()] () {
        return self->Perform();
    });

    return gateway;
}

IHTTPGateway::THeaders IHTTPGateway::MakeYcHeaders(
    const TString& requestId,
    const TString& token,
    const TString& contentType,
    const TString& userPwd,
    const TString& awsSigV4) {

    IHTTPGateway::THeaders result{.Options{userPwd, awsSigV4}};

    if (requestId.empty()) {
        throw yexception() << "RequestId is mandatory";
    }
    result.Fields.push_back(TString("X-Request-ID:") + requestId);

    if (!token.empty()) {
        result.Fields.push_back(TString("X-YaCloud-SubjectToken:") + token);
    }

    if (!contentType.empty()) {
        result.Fields.push_back(TString("Content-Type:") + contentType);
    }

    return result;
}

}
