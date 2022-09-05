#include "yql_http_gateway.h"

#include <contrib/libs/curl/include/curl/curl.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>

#include <thread>
#include <mutex>
#include <stack>
#include <queue>

namespace {
int curlTrace(CURL *handle, curl_infotype type,
    char *data, size_t size,
    void *userp) {
    Y_UNUSED(handle);
    Y_UNUSED(userp);
    Y_UNUSED(size);
    Y_UNUSED(data);

    TStringBuf buf(data, size);
    switch (type) {
    case CURLINFO_TEXT:
        Cerr << "== Info: " << buf;
        break;
    case CURLINFO_HEADER_OUT:
        Cerr << "=> Send header (" << size << " bytes):" << Endl << buf;
        break;
    case CURLINFO_HEADER_IN:
        Cerr << "<= Recv header (" << size << " bytes):" << buf;
        break;
    default:
        return 0;

/*
    case CURLINFO_DATA_OUT:
        Cerr << "=> Send data (" << size << " bytes)" << Endl;
        break;
    case CURLINFO_SSL_DATA_OUT:
        Cerr << "=> Send SSL data (" << size << " bytes)" << Endl;
        break;
    case CURLINFO_DATA_IN:
        Cerr << "<= Recv data (" << size << " bytes)" << Endl;
        break;
    case CURLINFO_SSL_DATA_IN:
        Cerr << "<= Recv SSL data (" << size << " bytes)" << Endl;
        break;
*/
    }

    return 0;
}
}

namespace NYql {
namespace {

struct TCurlInitConfig {
    ui64 ConnectionTimeout = 0;
    ui64 BytesPerSecondLimit = 0;
    ui64 BufferSize = CURL_MAX_WRITE_SIZE;
};

class TEasyCurl {
public:
    using TPtr = std::shared_ptr<TEasyCurl>;


    enum class EMethod {
        GET,
        POST,
        PUT
    };

    TEasyCurl(const ::NMonitoring::TDynamicCounters::TCounterPtr& counter, const ::NMonitoring::TDynamicCounters::TCounterPtr& downloadedBytes, const ::NMonitoring::TDynamicCounters::TCounterPtr& uploadedBytes, TString url, IHTTPGateway::THeaders headers, EMethod method, size_t offset = 0ULL, size_t bodySize = 0, ui64 expectedSize = 0, const TCurlInitConfig& config = TCurlInitConfig())
        : Headers(headers), Method(method), Offset(offset), BodySize(bodySize), ExpectedSize(expectedSize), Counter(counter), DownloadedBytes(downloadedBytes), UploadedBytes(uploadedBytes), Config(config), Url(url)
    {
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
        }

        // does nothing if CURLOPT_VERBOSE is not set to 1
        curl_easy_setopt(Handle, CURLOPT_DEBUGFUNCTION, curlTrace);

        // for local debug only
        // will print tokens in HTTP headers
        // curl_easy_setopt(Handle, CURLOPT_VERBOSE, 1L);

        curl_easy_setopt(Handle, CURLOPT_URL, Url.c_str());
        curl_easy_setopt(Handle, CURLOPT_USERAGENT, "YQ HTTP gateway");
        curl_easy_setopt(Handle, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(Handle, CURLOPT_CONNECTTIMEOUT, Config.ConnectionTimeout);
        curl_easy_setopt(Handle, CURLOPT_MAX_RECV_SPEED_LARGE, Config.BytesPerSecondLimit);
        curl_easy_setopt(Handle, CURLOPT_BUFFERSIZE, Config.BufferSize);
        curl_easy_setopt(Handle, CURLOPT_LOW_SPEED_TIME, 20L);
        curl_easy_setopt(Handle, CURLOPT_LOW_SPEED_LIMIT, 1024L);

        if (!Headers.empty()) {
            CurlHeaders = std::accumulate(Headers.cbegin(), Headers.cend(), CurlHeaders,
                std::bind(&curl_slist_append, std::placeholders::_1, std::bind(&TString::c_str, std::placeholders::_2)));
            curl_easy_setopt(Handle, CURLOPT_HTTPHEADER, CurlHeaders);
        }
        TStringBuilder byteRange;
        byteRange << Offset << "-";
        if (ExpectedSize) {
            byteRange << Offset + ExpectedSize - 1;
        }
        curl_easy_setopt(Handle, CURLOPT_RANGE, byteRange.c_str());
        curl_easy_setopt(Handle, EMethod::PUT == Method ? CURLOPT_HEADERFUNCTION : CURLOPT_WRITEFUNCTION, &WriteMemoryCallback);
        curl_easy_setopt(Handle, EMethod::PUT == Method ? CURLOPT_HEADERDATA :CURLOPT_WRITEDATA, static_cast<void*>(this));
        if (Method == EMethod::POST) {
            curl_easy_setopt(Handle, CURLOPT_POSTFIELDSIZE, BodySize);
        }

        if (BodySize) {
            curl_easy_setopt(Handle, CURLOPT_READFUNCTION, &ReadMemoryCallback);
            curl_easy_setopt(Handle, CURLOPT_READDATA, static_cast<void*>(this));
        }
    }

    void FreeHandles() {
        if (Handle) {
            curl_easy_cleanup(Handle);
            Handle = nullptr;
        }
        if (Headers) {
            curl_slist_free_all(CurlHeaders);
            CurlHeaders = nullptr;
        }
    }

    CURL* GetHandle() const {
        return Handle;
    }

    virtual void Fail(const TIssue& error) = 0;
    virtual void Done(CURLcode result, long httpResponseCode) = 0;

    virtual size_t Write(void* contents, size_t size, size_t nmemb) = 0;
    virtual size_t Read(char *buffer, size_t size, size_t nmemb) = 0;
protected:
    void SkipTo(size_t offset) const {
        curl_easy_setopt(Handle, CURLOPT_RANGE,  (ToString(Offset + offset) += '-').c_str());
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
    ReadMemoryCallback(char *buffer, size_t size, size_t nmemb, void *userp) {
        const auto self = static_cast<TEasyCurl*>(userp);
        const auto res = self->Read(buffer, size, nmemb);
        self->UploadedBytes->Add(res);
        return res;
    };

    const IHTTPGateway::THeaders Headers;
    const EMethod Method;
    const size_t Offset;
    const size_t BodySize;
    const ui64 ExpectedSize;
    CURL* Handle = nullptr;
    curl_slist* CurlHeaders = nullptr;
    const ::NMonitoring::TDynamicCounters::TCounterPtr Counter;
    const ::NMonitoring::TDynamicCounters::TCounterPtr DownloadedBytes;
    const ::NMonitoring::TDynamicCounters::TCounterPtr UploadedBytes;
    const TCurlInitConfig Config;
public:
    TString Url;
};

class TEasyCurlBuffer : public TEasyCurl {
public:
    using TPtr = std::shared_ptr<TEasyCurlBuffer>;
    using TWeakPtr = std::weak_ptr<TEasyCurlBuffer>;

    TEasyCurlBuffer(const ::NMonitoring::TDynamicCounters::TCounterPtr&  counter, const ::NMonitoring::TDynamicCounters::TCounterPtr& downloadedBytes, const ::NMonitoring::TDynamicCounters::TCounterPtr& uploadededBytes, TString url, EMethod method, TString data, IHTTPGateway::THeaders headers, size_t offset, size_t expectedSize, IHTTPGateway::TOnResult callback, IRetryPolicy<long>::IRetryState::TPtr retryState, const TCurlInitConfig& config = TCurlInitConfig())
        : TEasyCurl(counter, downloadedBytes, uploadededBytes, url, headers, method, offset, data.size(), expectedSize, std::move(config)), ExpectedSize(expectedSize), Data(std::move(data)), Input(Data), Output(Buffer), RetryState(std::move(retryState))
    {
        Output.Reserve(ExpectedSize);
        Callbacks.emplace(std::move(callback));
    }

    static TPtr Make(const ::NMonitoring::TDynamicCounters::TCounterPtr&  counter, const ::NMonitoring::TDynamicCounters::TCounterPtr& downloadedBytes, const ::NMonitoring::TDynamicCounters::TCounterPtr& uploadededBytes, TString url, EMethod method, TString data, IHTTPGateway::THeaders headers, size_t offset, size_t expectedSize, IHTTPGateway::TOnResult callback, IRetryPolicy<long>::IRetryState::TPtr retryState, const TCurlInitConfig& config = TCurlInitConfig()) {
        return std::make_shared<TEasyCurlBuffer>(counter, downloadedBytes, uploadededBytes, std::move(url), method, std::move(data), std::move(headers), offset, expectedSize, std::move(callback), std::move(retryState), std::move(config));
    }

    size_t GetExpectedSize() const {
        return ExpectedSize;
    }

    // return true if callback successfully added to this work
    bool AddCallback(IHTTPGateway::TOnResult callback) {
        const std::unique_lock lock(SyncCallbacks);
        if (Callbacks.empty())
            return false;
        Callbacks.emplace(std::move(callback));
        return true;
    }

    TMaybe<TDuration> GetNextRetryDelay(long httpResponseCode) const {
        if (RetryState)
            return RetryState->GetNextRetryDelay(httpResponseCode);

        return {};
    }

    void Reset() {
        Buffer.clear();
        TStringOutput(Buffer).Swap(Output);
        Output.Reserve(ExpectedSize);
        TStringInput(Data).Swap(Input);
        FreeHandles();
        InitHandles();
    }
private:
    void Fail(const TIssue& error) final  {
        TIssues issues{error};
        const std::unique_lock lock(SyncCallbacks);
        while (!Callbacks.empty()) {
            Callbacks.top()(issues);
            Callbacks.pop();
        }
    }

    void Done(CURLcode result, long httpResponseCode) final {
        if (CURLE_OK != result)
            return Fail(TIssue(curl_easy_strerror(result)));

        const std::unique_lock lock(SyncCallbacks);
        while (!Callbacks.empty()) {
            if (1U == Callbacks.size())
                Callbacks.top()(IHTTPGateway::TContent(std::move(Buffer), httpResponseCode));
            else
                Callbacks.top()(IHTTPGateway::TContent(Buffer, httpResponseCode));
            Callbacks.pop();
        }
    }

    size_t Write(void* contents, size_t size, size_t nmemb) final {
        const auto realsize = size * nmemb;
        Output.Write(contents, realsize);
        return realsize;
    }

    size_t Read(char *buffer, size_t size, size_t nmemb) final {
        return Input.Read(buffer, size * nmemb);
    }

    const size_t ExpectedSize;
    const TString Data;
    TString Buffer;
    TStringInput Input;
    TStringOutput Output;

    std::mutex SyncCallbacks;
    std::stack<IHTTPGateway::TOnResult> Callbacks;
    const IRetryPolicy<long>::IRetryState::TPtr RetryState;
};

class TEasyCurlStream : public TEasyCurl {
public:
    using TPtr = std::shared_ptr<TEasyCurlStream>;
    using TWeakPtr = std::weak_ptr<TEasyCurlStream>;

    TEasyCurlStream(
        const ::NMonitoring::TDynamicCounters::TCounterPtr&  counter,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& downloadedBytes,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& uploadededBytes,
        TString url, IHTTPGateway::THeaders headers,
        size_t offset,
        IHTTPGateway::TOnDownloadStart onStart,
        IHTTPGateway::TOnNewDataPart onNewData,
        IHTTPGateway::TOnDownloadFinish onFinish,
        const TCurlInitConfig& config = TCurlInitConfig())
        : TEasyCurl(counter, downloadedBytes, uploadededBytes, url, headers, EMethod::GET, offset, 0, 0, std::move(config)), OnStart(std::move(onStart)), OnNewData(std::move(onNewData)), OnFinish(std::move(onFinish)), Counter(std::make_shared<std::atomic_size_t>(0ULL))
    {}

    static TPtr Make(
        const ::NMonitoring::TDynamicCounters::TCounterPtr&  counter,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& downloadedBytes,
        const ::NMonitoring::TDynamicCounters::TCounterPtr& uploadededBytes,
        TString url,
        IHTTPGateway::THeaders headers,
        size_t offset,
        IHTTPGateway::TOnDownloadStart onStart,
        IHTTPGateway::TOnNewDataPart onNewData,
        IHTTPGateway::TOnDownloadFinish onFinish,
        const TCurlInitConfig& config = TCurlInitConfig())
    {
        return std::make_shared<TEasyCurlStream>(counter, downloadedBytes, uploadededBytes, std::move(url), std::move(headers), offset, std::move(onStart), std::move(onNewData), std::move(onFinish), std::move(config));
    }

    enum class EAction : i8 {
        Drop = -2,
        Stop = -1,
        None = 0,
        Work = 1,
        StopAndDrop = Stop + Drop
    };

    EAction GetAction(size_t buffersSize) {
        if (Cancelled) {
            const auto ret = Working ? EAction::StopAndDrop : EAction::Drop;
            Working = false;
            return ret;
        }

        if (Working != Counter->load() < buffersSize) {
            if (Working = !Working)
                SkipTo(Position);
            return Working ? EAction::Work : EAction::Stop;
        }

        return EAction::None;
    }

    void Cancel(TIssue issue) {
        Cancelled = true;
        OnFinish(TIssues{issue});
    }
private:
    void Fail(const TIssue& error) final  {
        Working = false;
        return OnFinish(TIssues{error});
    }

    void Done(CURLcode result, long) final {
        if (CURLE_OK != result)
            return Fail(TIssue(curl_easy_strerror(result)));

        Working = false;
        return OnFinish(TIssues());
    }

    size_t Write(void* contents, size_t size, size_t nmemb) final {
        if (!StreamStarted) {
            StreamStarted = true;
            long httpResponseCode = 0L;
            curl_easy_getinfo(GetHandle(), CURLINFO_RESPONSE_CODE, &httpResponseCode);
            OnStart(httpResponseCode);
        }

        const auto realsize = size * nmemb;
        Position += realsize;
        OnNewData(IHTTPGateway::TCountedContent(TString(static_cast<char*>(contents), realsize), Counter));
        return realsize;
    }

    size_t Read(char*, size_t, size_t) final { return 0ULL; }

    const IHTTPGateway::TOnDownloadStart OnStart;
    const IHTTPGateway::TOnNewDataPart OnNewData;
    const IHTTPGateway::TOnDownloadFinish OnFinish;

    const std::shared_ptr<std::atomic_size_t> Counter;
    bool Working = false;
    size_t Position = 0ULL;
    bool Cancelled = false;
    bool StreamStarted = false;
};

using TKeyType = std::tuple<TString, size_t, IHTTPGateway::THeaders, TString, IRetryPolicy<long>::TPtr>;

class TKeyHash {
public:
    TKeyHash() : Hash(), HashPtr() {}

    size_t operator()(const TKeyType& key) const {
        const auto& headers = std::get<2U>(key);
        auto initHash = CombineHashes(CombineHashes(Hash(std::get<0U>(key)), std::get<1U>(key)), Hash(std::get<3U>(key)));
        initHash = CombineHashes(HashPtr(std::get<4U>(key)), initHash);
        return std::accumulate(headers.cbegin(), headers.cend(), initHash,
            [this](size_t hash, const TString& item) { return CombineHashes(hash, Hash(item)); });
    }
private:
    const std::hash<TString> Hash;
    const std::hash<IRetryPolicy<long>::TPtr> HashPtr;
};

class THTTPMultiGateway : public IHTTPGateway {
friend class IHTTPGateway;
public:
    using TPtr = std::shared_ptr<THTTPMultiGateway>;
    using TWeakPtr = std::weak_ptr<THTTPMultiGateway>;

    explicit THTTPMultiGateway(
        const THttpGatewayConfig* httpGatewaysCfg,
        ::NMonitoring::TDynamicCounterPtr counters)
        : Counters(std::move(counters))
        , Rps(Counters->GetCounter("Requests", true))
        , InFlight(Counters->GetCounter("InFlight"))
        , InFlightStreams(Counters->GetCounter("InFlightStreams"))
        , MaxInFlight(Counters->GetCounter("MaxInFlight"))
        , AllocatedMemory(Counters->GetCounter("AllocatedMemory"))
        , MaxAllocatedMemory(Counters->GetCounter("MaxAllocatedMemory"))
        , OutputMemory(Counters->GetCounter("OutputMemory"))
        , PerformCycles(Counters->GetCounter("PerformCycles", true))
        , AwaitQueue(Counters->GetCounter("AwaitQueue"))
        , AwaitQueueTopExpectedSize(Counters->GetCounter("AwaitQueueTopExpectedSize"))
        , DownloadedBytes(Counters->GetCounter("DownloadedBytes", true))
        , UploadedBytes(Counters->GetCounter("UploadedBytes", true))
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
    size_t BuffersSizePerStream = CURL_MAX_WRITE_SIZE << 1U;
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
        Y_VERIFY(Handle);
        const CURLMcode multiCleanupResult = curl_multi_cleanup(Handle);
        if (multiCleanupResult != CURLM_OK) {
            Cerr << "curl_multi_cleanup error " << int(multiCleanupResult) << ": " << curl_multi_strerror(multiCleanupResult) << Endl;
        }
        curl_global_cleanup();
    }

    void Perform() {
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
                switch (stream->GetAction(BuffersSizePerStream)) {
                    case TEasyCurlStream::EAction::Drop:
                        Allocated.erase(stream->GetHandle());
                        break;
                    case TEasyCurlStream::EAction::Work:
                        curl_multi_add_handle(Handle, stream->GetHandle());
                        break;
                    case TEasyCurlStream::EAction::Stop:
                        curl_multi_remove_handle(Handle, stream->GetHandle());
                        break;
                    case TEasyCurlStream::EAction::StopAndDrop:
                        curl_multi_remove_handle(Handle, stream->GetHandle());
                        Allocated.erase(stream->GetHandle());
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

        const ui64 topExpectedSize = Await.empty() ? 0 : Await.front()->GetExpectedSize();
        AwaitQueueTopExpectedSize->Set(topExpectedSize);
        while (!Await.empty() && Allocated.size() < MaxHandlers && AllocatedSize + Await.front()->GetExpectedSize() <= MaxSimulatenousDownloadsSize) {
            AllocatedSize += Await.front()->GetExpectedSize();
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
                if (CURLE_OK == result)
                    curl_easy_getinfo(easy->GetHandle(), CURLINFO_RESPONSE_CODE, &httpResponseCode);

                if (auto buffer = std::dynamic_pointer_cast<TEasyCurlBuffer>(easy)) {
                    AllocatedSize -= buffer->GetExpectedSize();
                    if (const auto& nextRetryDelay = buffer->GetNextRetryDelay(httpResponseCode)) {
                        buffer->Reset();
                        Delayed.emplace(nextRetryDelay->ToDeadLine(), std::move(buffer));
                        easy.reset();
                    }
                }
                Allocated.erase(it);
            }

            if (Await.empty() && Allocated.empty())
                Requests.clear();
        }
        if (easy) {
            easy->Done(result, httpResponseCode);
        }
    }

    void Fail(CURLMcode result) {
        std::stack<TEasyCurl::TPtr> works;
        {
            const std::unique_lock lock(Sync);

            for (const auto& item : Allocated) {
                works.emplace(std::move(item.second));
            }

            AllocatedSize = 0ULL;
            Allocated.clear();
            if (Await.empty())
                Requests.clear();
        }

        const TIssue error(curl_multi_strerror(result));
        while (!works.empty()) {
            curl_multi_remove_handle(Handle, works.top()->GetHandle());
            works.top()->Fail(error);
            works.pop();
        }
    }

    void Upload(TString url, THeaders headers, TString body, TOnResult callback, bool put, IRetryPolicy<long>::TPtr retryPolicy) final {
        Rps->Inc();

        const std::unique_lock lock(Sync);
        auto easy = TEasyCurlBuffer::Make(InFlight, DownloadedBytes, UploadedBytes, std::move(url), put ? TEasyCurl::EMethod::PUT : TEasyCurl::EMethod::POST, std::move(body), std::move(headers), 0U, 0U, std::move(callback), retryPolicy ? retryPolicy->CreateRetryState() : nullptr, InitConfig);
        Await.emplace(std::move(easy));
        Wakeup(0U);
    }

    void Download(
        TString url,
        THeaders headers,
        size_t expectedSize,
        TOnResult callback,
        TString data,
        IRetryPolicy<long>::TPtr retryPolicy) final
    {
        Rps->Inc();
        if (expectedSize > MaxSimulatenousDownloadsSize) {
            TIssue error(TStringBuilder() << "Too big file for downloading: size " << expectedSize << ", but limit is " << MaxSimulatenousDownloadsSize);
            callback(TIssues{error});
            return;
        }
        const std::unique_lock lock(Sync);
        auto& entry = Requests[TKeyType(url, 0U, headers, data, retryPolicy)];
        if (const auto& easy = entry.lock())
            if (easy->AddCallback(callback))
                return;

        auto easy = TEasyCurlBuffer::Make(InFlight, DownloadedBytes, UploadedBytes, std::move(url),  TEasyCurl::EMethod::GET, std::move(data), std::move(headers), 0U, expectedSize, std::move(callback), retryPolicy ? retryPolicy->CreateRetryState() : nullptr, InitConfig);
        entry = easy;
        Await.emplace(std::move(easy));
        Wakeup(expectedSize);
    }

    TCancelHook Download(
        TString url,
        THeaders headers,
        size_t offset,
        TOnDownloadStart onStart,
        TOnNewDataPart onNewData,
        TOnDownloadFinish onFinish) final
    {
        auto stream = TEasyCurlStream::Make(InFlightStreams, DownloadedBytes, UploadedBytes, std::move(url), std::move(headers), offset, std::move(onStart), std::move(onNewData), std::move(onFinish));
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

    void OnRetry(TEasyCurlBuffer::TPtr easy) {
        const std::unique_lock lock(Sync);
        const size_t expectedSize = easy->GetExpectedSize();
        Await.emplace(std::move(easy));
        Wakeup(expectedSize);
    }

    void Wakeup(size_t expectedSize) {
        AwaitQueue->Set(Await.size());
        if (Allocated.size() < MaxHandlers && AllocatedSize + expectedSize + OutputSize.load() <= MaxSimulatenousDownloadsSize) {
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
    std::unordered_map<TKeyType, TEasyCurlBuffer::TWeakPtr, TKeyHash> Requests;
    std::priority_queue<std::pair<TInstant, TEasyCurlBuffer::TPtr>> Delayed;

    std::mutex Sync;
    std::thread Thread;
    std::atomic<bool> IsStopped = false;

    size_t AllocatedSize = 0ULL;
    static std::atomic_size_t OutputSize;

    static std::mutex CreateSync;
    static TWeakPtr Singleton;

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
    const ::NMonitoring::TDynamicCounters::TCounterPtr AwaitQueueTopExpectedSize;
    const ::NMonitoring::TDynamicCounters::TCounterPtr DownloadedBytes;
    const ::NMonitoring::TDynamicCounters::TCounterPtr UploadedBytes;
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

IHTTPGateway::TContent::TContent(TString&& data, long httpResponseCode)
    : TContentBase(std::move(data))
    , HttpResponseCode(httpResponseCode)
{}

IHTTPGateway::TContent::TContent(const TString& data, long httpResponseCode)
    : TContentBase(data)
    , HttpResponseCode(httpResponseCode)
{}


IHTTPGateway::TCountedContent::TCountedContent(TString&& data, const std::shared_ptr<std::atomic_size_t>& counter)
    : TContentBase(std::move(data)), Counter(counter)
{
    Counter->fetch_add(size());
}

IHTTPGateway::TCountedContent::~TCountedContent()
{
    Counter->fetch_sub(size());
}

TString IHTTPGateway::TCountedContent::Extract() {
    Counter->fetch_sub(size());
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

}
