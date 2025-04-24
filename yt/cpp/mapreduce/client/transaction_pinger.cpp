#include "transaction_pinger.h"

#include "transaction.h"

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/error_codes.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>
#include <yt/cpp/mapreduce/interface/tvm.h>

#include <yt/cpp/mapreduce/common/wait_proxy.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/http/requests.h>
#include <yt/cpp/mapreduce/http/retry_request.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/http.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/yt/threading/spin_lock.h>
#include <library/cpp/yt/assert/assert.h>

#include <util/datetime/base.h>
#include <util/random/random.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void CheckError(const TString& requestId, NHttp::IResponsePtr response)
{
    if (const auto* ytError = response->GetHeaders()->Find("X-YT-Error")) {
        TYtError error;
        error.ParseFrom(*ytError);

        TErrorResponse errorResponse(std::move(error), requestId);
        if (errorResponse.IsOk()) {
            return;
        }

        YT_LOG_ERROR("RSP %v - HTTP %v - %v",
            requestId,
            response->GetStatusCode(),
            errorResponse.AsStrBuf());

        ythrow errorResponse;
    }
}

void PingTx(NHttp::IClientPtr httpClient, const TPingableTransaction& tx)
{
    auto url = TString::Join("http://", tx.GetContext().ServerName, "/api/", tx.GetContext().Config->ApiVersion, "/ping_tx");
    auto headers = New<NHttp::THeaders>();
    auto requestId = CreateGuidAsString();

    headers->Add("Host", url);
    headers->Add("User-Agent", TProcessState::Get()->ClientVersion);

    if (const auto& serviceTicketAuth = tx.GetContext().ServiceTicketAuth) {
        const auto serviceTicket = serviceTicketAuth->Ptr->IssueServiceTicket();
        headers->Add("X-Ya-Service-Ticket", serviceTicket);
    } else if (const auto& token = tx.GetContext().Token; !token.empty()) {
        headers->Add("Authorization", "OAuth " + token);
    }

    headers->Add("Transfer-Encoding", "chunked");
    headers->Add("X-YT-Correlation-Id", requestId);
    headers->Add("X-YT-Header-Format", "<format=text>yson");
    headers->Add("Content-Encoding", "identity");
    headers->Add("Accept-Encoding", "identity");

    TNode node;
    node["transaction_id"] = GetGuidAsString(tx.GetId());
    auto strParams = NodeToYsonString(node);

    YT_LOG_DEBUG("REQ %v - sending request (HostName: %v; Method POST %v; X-YT-Parameters (sent in body): %v)",
        requestId,
        tx.GetContext().ServerName,
        url,
        strParams
    );

    auto response = NConcurrency::WaitFor(httpClient->Post(url, TSharedRef::FromString(strParams), headers)).ValueOrThrow();
    CheckError(requestId, response);

    YT_LOG_DEBUG("RSP %v - received response %v bytes. (%v)",
            requestId,
            response->ReadAll().size(),
            strParams);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TSharedTransactionPinger
    : public ITransactionPinger
{
public:
    TSharedTransactionPinger(NHttp::IClientPtr httpClient, int poolThreadCount)
        : PingerPool_(NConcurrency::CreateThreadPool(
            poolThreadCount, "tx_pinger_pool"))
        , HttpClient_(std::move(httpClient))
    { }

    ~TSharedTransactionPinger() override
    {
        PingerPool_->Shutdown();
    }

    ITransactionPingerPtr GetChildTxPinger() override
    {
        return this;
    }

    void RegisterTransaction(const TPingableTransaction& pingableTx) override
    {
        auto [minPingInterval, maxPingInterval] = pingableTx.GetPingInterval();
        auto pingInterval = (minPingInterval + maxPingInterval) / 2;
        double jitter = (maxPingInterval - pingInterval) / pingInterval;

        auto opts = NConcurrency::TPeriodicExecutorOptions{pingInterval, pingInterval, jitter};
        auto periodic = std::make_shared<NConcurrency::TPeriodicExecutorPtr>(nullptr);
        // Have to use weak_ptr in order to break reference cycle
        // This weak_ptr holds pointer to periodic, which will contain this lambda
        // Also we consider that lifetime of this lambda is no longer than lifetime of pingableTx
        // because every pingableTx have to call RemoveTransaction before it is destroyed
        auto pingRoutine = BIND([this, &pingableTx, periodic = std::weak_ptr{periodic}] {
            auto strong_ptr = periodic.lock();
            YT_VERIFY(strong_ptr);
            DoPingTransaction(pingableTx, *strong_ptr);
        });
        *periodic = New<NConcurrency::TPeriodicExecutor>(PingerPool_->GetInvoker(), pingRoutine, opts);
        (*periodic)->Start();

        auto guard = Guard(SpinLock_);
        YT_VERIFY(!Transactions_.contains(pingableTx.GetId()));
        Transactions_[pingableTx.GetId()] = std::move(periodic);
    }

    bool HasTransaction(const TPingableTransaction& pingableTx) override
    {
        auto guard = Guard(SpinLock_);
        return Transactions_.contains(pingableTx.GetId());
    }


    void RemoveTransaction(const TPingableTransaction& pingableTx) override
    {
        std::shared_ptr<NConcurrency::TPeriodicExecutorPtr> periodic;
        {
            auto guard = Guard(SpinLock_);

            auto it = Transactions_.find(pingableTx.GetId());

            YT_VERIFY(it != Transactions_.end());

            periodic = std::move(it->second);
            Transactions_.erase(it);
        }
        NConcurrency::WaitUntilSet((*periodic)->Stop());
    }

private:
    void DoPingTransaction(const TPingableTransaction& pingableTx,
                           NConcurrency::TPeriodicExecutorPtr periodic)
    {
        try {
            PingTx(HttpClient_, pingableTx);
        } catch (const std::exception& e) {
            if (auto* errorResponse = dynamic_cast<const TErrorResponse*>(&e)) {
                if (errorResponse->GetError().ContainsErrorCode(NYT::NClusterErrorCodes::NTransactionClient::NoSuchTransaction)) {
                    YT_UNUSED_FUTURE(periodic->Stop());
                } else if (errorResponse->GetError().ContainsErrorCode(NYT::NClusterErrorCodes::Timeout)) {
                    periodic->ScheduleOutOfBand();
                }
            }
        }
    }


private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<TTransactionId, std::shared_ptr<NConcurrency::TPeriodicExecutorPtr>> Transactions_;

    NConcurrency::IThreadPoolPtr PingerPool_;
    NHttp::IClientPtr HttpClient_;
};

////////////////////////////////////////////////////////////////////////////////

ITransactionPingerPtr CreateTransactionPinger(const TConfigPtr& config)
{
    YT_LOG_DEBUG("Using async transaction pinger");
    auto httpClientConfig = NYT::New<NHttp::TClientConfig>();
    httpClientConfig->MaxIdleConnections = 16;
    auto httpPoller = NConcurrency::CreateThreadPoolPoller(
        config->AsyncHttpClientThreads,
        "tx_http_client_poller");
    auto httpClient = NHttp::CreateClient(std::move(httpClientConfig), std::move(httpPoller));

    return MakeIntrusive<TSharedTransactionPinger>(
        std::move(httpClient),
        config->AsyncTxPingerPoolThreads);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
