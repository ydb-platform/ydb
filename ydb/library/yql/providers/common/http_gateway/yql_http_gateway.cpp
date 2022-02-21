#include "yql_http_gateway.h"

#include <contrib/libs/curl/include/curl/curl.h>
#include <util/stream/str.h>
#include <util/generic/size_literals.h>

#include <thread>
#include <mutex>
#include <stack>
#include <queue>

namespace NYql {
namespace {

class TEasyCurl {
public:
    using TPtr = std::shared_ptr<TEasyCurl>;
    using TWeakPtr = std::weak_ptr<TEasyCurl>;

    TEasyCurl(TString url, TString data, IHTTPGateway::THeaders headers, std::size_t expectedSize, IHTTPGateway::TOnResult callback)
        : ExpectedSize(expectedSize), Handle(curl_easy_init()), Data(std::move(data)), Input(Data), Output(Buffer)
    {
        Output.Reserve(expectedSize);

        curl_easy_setopt(Handle, CURLOPT_URL, url.c_str());
        curl_easy_setopt(Handle, CURLOPT_POST, Data.empty() ?  0L : 1L);
        curl_easy_setopt(Handle, CURLOPT_WRITEFUNCTION, &WriteMemoryCallback);
        curl_easy_setopt(Handle, CURLOPT_WRITEDATA, static_cast<void*>(&Output));
        curl_easy_setopt(Handle, CURLOPT_USERAGENT, "YQ HTTP gateway");
        curl_easy_setopt(Handle, CURLOPT_SSL_VERIFYPEER, 0L);

        if (!headers.empty()) {
            Headers = std::accumulate(headers.cbegin(), headers.cend(), Headers,
                std::bind(&curl_slist_append, std::placeholders::_1, std::bind(&TString::c_str, std::placeholders::_2)));
            curl_easy_setopt(Handle, CURLOPT_HTTPHEADER, Headers);
        }

        if (!Data.empty()) {
            curl_easy_setopt(Handle, CURLOPT_READFUNCTION, &ReadMemoryCallback);
            curl_easy_setopt(Handle, CURLOPT_READDATA, static_cast<void*>(&Input));
        }

        Callbacks.emplace(std::move(callback));
    }

    ~TEasyCurl() {
        curl_easy_cleanup(Handle);
        if (Headers) {
            curl_slist_free_all(Headers);
        }
    }

    static TPtr Make(TString url, TString data, IHTTPGateway::THeaders headers, std::size_t expectedSize, IHTTPGateway::TOnResult callback) {
        return std::make_shared<TEasyCurl>(std::move(url), std::move(data), std::move(headers), expectedSize, std::move(callback));
    }

    std::size_t GetExpectedSize() const {
        return ExpectedSize;
    }

    CURL* GetHandle() const {
        return Handle;
    }

    void AddCallback(IHTTPGateway::TOnResult callback) {
        Callbacks.emplace(std::move(callback));
    }

    void Fail(TIssue error) {
        while (!Callbacks.empty()) {
            Callbacks.top()(TIssues{error});
            Callbacks.pop();
        }
    }

    void Done(CURLcode result) {
        if (CURLE_OK != result)
            return Fail(TIssue(curl_easy_strerror(result)));

        while (!Callbacks.empty()) {
            if (1U == Callbacks.size())
                Callbacks.top()(IHTTPGateway::TContent(std::move(Buffer)));
            else
                Callbacks.top()(IHTTPGateway::TContent(Buffer));
            Callbacks.pop();
        }
    }
private:
    static size_t
    WriteMemoryCallback(void* contents, size_t size, size_t nmemb, void* userp)
    {
        const auto realsize = size * nmemb;
        static_cast<TStringOutput*>(userp)->Write(contents, realsize);
        return realsize;
    };

    static size_t
    ReadMemoryCallback(char *buffer, size_t size, size_t nmemb, void *userp)
    {
        return static_cast<TStringInput*>(userp)->Read(buffer, size * nmemb);
    };

    const std::size_t ExpectedSize;
    CURL *const Handle;
    curl_slist* Headers = nullptr;
    const TString Data;
    TString Buffer;
    TStringInput Input;
    TStringOutput Output;
    std::stack<IHTTPGateway::TOnResult> Callbacks;
};

using TKeyType = std::tuple<TString, IHTTPGateway::THeaders, TString, IRetryPolicy<long>::TPtr>;

class TKeyHash {
public:
    TKeyHash() : Hash(), HashPtr() {}

    size_t operator()(const TKeyType& key) const {
        const auto& headers = std::get<1U>(key);
        auto initHash = CombineHashes(Hash(std::get<0U>(key)), Hash(std::get<2U>(key)));
        initHash = CombineHashes(HashPtr(std::get<3U>(key)), initHash);
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
        NMonitoring::TDynamicCounterPtr counters)
        : Counters(std::move(counters))
        , Rps(Counters->GetCounter("RPS", true))
        , InFlight(Counters->GetCounter("InFlight"))
        , AllocatedMemory(Counters->GetCounter("AllocatedMemory"))
    {
        if (!httpGatewaysCfg) {
            return;
        }
        if (httpGatewaysCfg->HasMaxInFlightCount()) {
            MaxHandlers = httpGatewaysCfg->GetMaxInFlightCount();
        }
        if (httpGatewaysCfg->HasMaxSimulatenousDownloadsSize()) {
            MaxSimulatenousDownloadsSize  = httpGatewaysCfg->GetMaxSimulatenousDownloadsSize();
        }

        TaskScheduler.Start();
    }

    ~THTTPMultiGateway() {
        if (Handle)
            curl_multi_wakeup(Handle);
        Thread.join();
    }
private:
    std::size_t MaxHandlers = 1024U;
    std::size_t MaxSimulatenousDownloadsSize = 8_GB;

    static void Perform(const TWeakPtr& weak) {
        OutputSize.store(0ULL);
        curl_global_init(CURL_GLOBAL_ALL);

        if (const auto handle = curl_multi_init()) {
            if (const auto& initHandle = weak.lock()) {
                initHandle->Handle = handle;
            }

            for (size_t handlers = 0U;;) {
                if (const auto& self = weak.lock())
                    handlers = self->FillHandlers();
                else
                    break;

                int running = 0;
                if (const auto c = curl_multi_perform(handle, &running); CURLM_OK != c) {
                    if (const auto& self = weak.lock()) {
                        self->Fail(c);
                    }
                    break;
                }

                if (running < int(handlers)) {
                    if (const auto& self = weak.lock()) {
                        for (int messages = int(handlers) - running; messages;) {
                            if (const auto msg = curl_multi_info_read(handle, &messages)) {
                                if(msg->msg == CURLMSG_DONE) {
                                    self->Done(msg->easy_handle, msg->data.result);
                                }
                            }
                        }
                    }
                } else {
                    if (const auto c = curl_multi_poll(handle, nullptr, 0, 1024, nullptr); CURLM_OK != c) {
                        if (const auto& self = weak.lock()) {
                            self->Fail(c);
                        }
                        break;
                    }
                }
            }

            curl_multi_cleanup(handle);
        }

        curl_global_cleanup();
    }

    size_t FillHandlers() {
        const std::unique_lock lock(Sync);
        while (!Await.empty() && Allocated.size() < MaxHandlers && AllocatedSize + Await.front()->GetExpectedSize() <= MaxSimulatenousDownloadsSize) {
            AllocatedSize += Await.front()->GetExpectedSize();
            const auto handle = Await.front()->GetHandle();
            Allocated.emplace(handle, std::move(Await.front()));
            Await.pop();
            curl_multi_add_handle(Handle, handle);
        }

        AllocatedMemory->Set(AllocatedSize);
        return Allocated.size();
    }

    void Done(CURL* handle, CURLcode result) {
        TEasyCurl::TPtr easy;
        {
            const std::unique_lock lock(Sync);
            if (const auto it = Allocated.find(handle); Allocated.cend() != it) {
                long httpResponseCode = 0;
                easy = std::move(it->second);
                curl_easy_getinfo(easy->GetHandle(), CURLINFO_RESPONSE_CODE, &httpResponseCode);

                if (const auto stateIt = Easy2RetryState.find(easy); stateIt != Easy2RetryState.end()) {
                    if (const auto& nextRetryDelay = stateIt->second->GetNextRetryDelay(httpResponseCode)) {
                        Y_VERIFY(TaskScheduler.Add(new THttpGatewayTask(easy, Singleton), *nextRetryDelay));
                    } else {
                        Easy2RetryState.erase(stateIt);
                    }
                }
                AllocatedSize -= easy->GetExpectedSize();
                Allocated.erase(it);
            }

            if (Await.empty() && Allocated.empty())
                Requests.clear();
        }
        if (easy) {
            InFlight->Dec();
            easy->Done(result);
        }
    }

    void Fail(CURLMcode result) {
        std::stack<TEasyCurl::TPtr> works;
        {
            const std::unique_lock lock(Sync);

            for (const auto& item : Allocated) {
                works.emplace(std::move(item.second));
                AllocatedSize -= works.top()->GetExpectedSize();
            }

            AllocatedSize = 0ULL;
            Allocated.clear();
            if (Await.empty())
                Requests.clear();
        }

        const TIssue error(curl_multi_strerror(result));
        InFlight->Sub(works.size());
        while (!works.empty()) {
            works.top()->Fail(error);
            works.pop();
        }
    }

    void Download(
        TString url,
        THeaders headers,
        std::size_t expectedSize,
        TOnResult callback,
        TString data,
        IRetryPolicy<long>::TPtr retryPolicy) final
    {
        Rps->Inc();
        InFlight->Inc();

        const std::unique_lock lock(Sync);
        auto& entry = Requests[TKeyType(url, headers, data, retryPolicy)];
        if (const auto& easy = entry.lock())
            return easy->AddCallback(std::move(callback));

        auto easy = TEasyCurl::Make(std::move(url), std::move(data), std::move(headers), expectedSize, std::move(callback));
        entry = easy;
        Easy2RetryState.emplace(easy, std::move(retryPolicy->CreateRetryState()));
        Await.emplace(std::move(easy));
        Wakeup(expectedSize);
    }

    void OnRetry(TEasyCurl::TPtr easy) {
        const std::unique_lock lock(Sync);
        const std::size_t expectedSize = easy->GetExpectedSize();
        Await.emplace(std::move(easy));
        Wakeup(expectedSize);
    }

    void Wakeup(std::size_t expectedSize) {
        if (Allocated.size() < MaxHandlers && AllocatedSize + expectedSize + OutputSize.load() <= MaxSimulatenousDownloadsSize) {
            curl_multi_wakeup(Handle);
        }
    }

    class THttpGatewayTask: public TTaskScheduler::IRepeatedTask {
    public:
        THttpGatewayTask(
            TEasyCurl::TPtr easy,
            THTTPMultiGateway::TWeakPtr gateway)
            : Easy(easy)
            , Gateway(gateway)
        {}

        bool Process() override {
            if (const auto g = Gateway.lock()) {
                Y_VERIFY(Easy);
                g->OnRetry(std::move(Easy));
            }
            return false;
        }
    private:
        TEasyCurl::TPtr Easy;
        THTTPMultiGateway::TWeakPtr Gateway;
    };

private:
    CURLM* Handle = nullptr;

    std::queue<TEasyCurl::TPtr> Await;

    std::unordered_map<CURL*, TEasyCurl::TPtr> Allocated;
    std::unordered_map<TKeyType, TEasyCurl::TWeakPtr, TKeyHash> Requests;
    std::unordered_map<TEasyCurl::TPtr, IRetryPolicy<long>::IRetryState::TPtr> Easy2RetryState;

    std::mutex Sync;
    std::thread Thread;

    std::size_t AllocatedSize = 0ULL;
    static std::atomic_size_t OutputSize;

    static std::mutex CreateSync;
    static TWeakPtr Singleton;

    const NMonitoring::TDynamicCounterPtr Counters;
    const NMonitoring::TDynamicCounters::TCounterPtr Rps;
    const NMonitoring::TDynamicCounters::TCounterPtr InFlight;
    const NMonitoring::TDynamicCounters::TCounterPtr AllocatedMemory;

    TTaskScheduler TaskScheduler;
};

std::atomic_size_t THTTPMultiGateway::OutputSize = 0ULL;
std::mutex THTTPMultiGateway::CreateSync;
THTTPMultiGateway::TWeakPtr THTTPMultiGateway::Singleton;

// Class that is not used in production (for testing only).
class THTTPEasyGateway : public IHTTPGateway, private std::enable_shared_from_this<THTTPEasyGateway> {
friend class IHTTPGateway;
public:
    using TPtr = std::shared_ptr<THTTPEasyGateway>;
    using TWeakPtr = std::weak_ptr<THTTPEasyGateway>;

    THTTPEasyGateway() {
        curl_global_init(CURL_GLOBAL_ALL);
    }

    ~THTTPEasyGateway() {
        for (auto& item : Requests)
            item.second.second.join();
        Requests.clear();
        curl_global_cleanup();
    }
private:
    static void Perform(const TWeakPtr& weak, const TEasyCurl::TPtr& easy) {
        const auto result = curl_easy_perform(easy->GetHandle());
        if (const auto& self = weak.lock())
            self->Done(easy, result);
        else
            easy->Done(result);
    }

    void Download(
        TString url,
        THeaders headers,
        std::size_t expectedSize,
        TOnResult callback,
        TString data,
        IRetryPolicy<long>::TPtr retryPolicy) final
    {
        const std::unique_lock lock(Sync);
        auto& entry = Requests[TKeyType(url, headers, data, std::move(retryPolicy))];
        if (const auto& easy = entry.first.lock())
            return easy->AddCallback(std::move(callback));
        auto easy = TEasyCurl::Make(std::move(url), std::move(data), std::move(headers), expectedSize, std::move(callback));
        entry = std::make_pair(TEasyCurl::TWeakPtr(easy), std::thread(&THTTPEasyGateway::Perform, weak_from_this(), easy));
    }

    void Done(const TEasyCurl::TPtr& easy, CURLcode result) {
        const std::unique_lock lock(Sync);
        easy->Done(result);
    }

    std::unordered_map<TKeyType, std::pair<TEasyCurl::TWeakPtr, std::thread>, TKeyHash> Requests;
    std::mutex Sync;
};

}

IHTTPGateway::TContent::TContent(TString&& data)
    : TString(std::move(data))
{
    if (!empty()) {
        THTTPMultiGateway::OutputSize.fetch_add(size());
    }
}

IHTTPGateway::TContent::TContent(const TString& data)
    : TString(data)
{
    if (!empty()) {
        THTTPMultiGateway::OutputSize.fetch_add(size());
    }
}

TString IHTTPGateway::TContent::Extract() {
    if (!empty()) {
        THTTPMultiGateway::OutputSize.fetch_sub(size());
    }
    return std::move(*this);
}

IHTTPGateway::TContent::~TContent()
{
    if (!empty()) {
        THTTPMultiGateway::OutputSize.fetch_sub(size());
    }
}

template<>
IHTTPGateway::TPtr
IHTTPGateway::Make<true>(const THttpGatewayConfig* httpGatewaysCfg, NMonitoring::TDynamicCounterPtr counters) {
    const std::unique_lock lock(THTTPMultiGateway::CreateSync);
    if (const auto g = THTTPMultiGateway::Singleton.lock())
        return g;

    const auto gateway = std::make_shared<THTTPMultiGateway>(httpGatewaysCfg, std::move(counters));
    THTTPMultiGateway::Singleton = gateway;
    gateway->Thread = std::thread(std::bind(&THTTPMultiGateway::Perform, THTTPMultiGateway::Singleton));
    return gateway;
}

template<>
IHTTPGateway::TPtr
IHTTPGateway::Make<false>(const THttpGatewayConfig*, NMonitoring::TDynamicCounterPtr) {
    return std::make_shared<THTTPEasyGateway>();
}

}
