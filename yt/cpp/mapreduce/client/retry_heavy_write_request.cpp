#include "retry_heavy_write_request.h"

#include "transaction.h"
#include "transaction_pinger.h"

#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/tvm.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/cpp/mapreduce/http/helpers.h>
#include <yt/cpp/mapreduce/http/http_client.h>
#include <yt/cpp/mapreduce/http/requests.h>
#include <yt/cpp/mapreduce/http/retry_request.h>

#include <util/stream/null.h>

namespace NYT {

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

void RetryHeavyWriteRequest(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const ITransactionPingerPtr& transactionPinger,
    const TClientContext& context,
    const TTransactionId& parentId,
    THttpHeader& header,
    std::function<THolder<IInputStream>()> streamMaker)
{
    int retryCount = context.Config->RetryCount;
    if (context.ServiceTicketAuth) {
        header.SetServiceTicket(context.ServiceTicketAuth->Ptr->IssueServiceTicket());
    } else {
        header.SetToken(context.Token);
    }

    if (context.ImpersonationUser) {
        header.SetImpersonationUser(*context.ImpersonationUser);
    }

    for (int attempt = 0; attempt < retryCount; ++attempt) {
        TPingableTransaction attemptTx(clientRetryPolicy, context, parentId, transactionPinger->GetChildTxPinger(), TStartTransactionOptions());

        auto input = streamMaker();
        TString requestId;

        try {
            auto hostName = GetProxyForHeavyRequest(context);
            requestId = CreateGuidAsString();

            UpdateHeaderForProxyIfNeed(hostName, context, header);

            header.AddTransactionId(attemptTx.GetId(), /* overwrite = */ true);
            header.SetRequestCompression(ToString(context.Config->ContentEncoding));

            auto request = context.HttpClient->StartRequest(
                GetFullUrlForProxy(hostName, context, header),
                requestId,
                header);
            TransferData(input.Get(), request->GetStream());
            request->Finish()->GetResponse();
        } catch (TErrorResponse& e) {
            YT_LOG_ERROR("RSP %v - attempt %v failed",
                requestId,
                attempt);

            if (!IsRetriable(e) || attempt + 1 == retryCount) {
                throw;
            }
            NDetail::TWaitProxy::Get()->Sleep(GetBackoffDuration(e, context.Config));
            continue;

        } catch (std::exception& e) {
            YT_LOG_ERROR("RSP %v - %v - attempt %v failed",
                requestId,
                e.what(),
                attempt);

            if (attempt + 1 == retryCount) {
                throw;
            }
            NDetail::TWaitProxy::Get()->Sleep(GetBackoffDuration(e, context.Config));
            continue;
        }

        attemptTx.Commit();
        return;
    }
}

////////////////////////////////////////////////////////////////////////////////

THeavyRequestRetrier::THeavyRequestRetrier(TParameters parameters)
    : Parameters_(std::move(parameters))
    , RequestRetryPolicy_(Parameters_.ClientRetryPolicy->CreatePolicyForGenericRequest())
    , StreamFactory_([] {
        return MakeHolder<TNullInput>();
    })
{
    Retry([] { });
}

THeavyRequestRetrier::~THeavyRequestRetrier() = default;

void THeavyRequestRetrier::Update(THeavyRequestRetrier::TStreamFactory streamFactory)
{
    StreamFactory_ = streamFactory;
    Retry([this] {
        auto stream = StreamFactory_();
        stream->Skip(Attempt_->Offset);
        auto transfered = stream->ReadAll(*Attempt_->Request->GetStream());
        Attempt_->Offset += transfered;
    });
}

void THeavyRequestRetrier::Finish()
{
    Retry([this] {
        Attempt_->Request->Finish()->GetResponse();
        Attempt_->Transaction->Commit();
        Attempt_.reset();
    });
}

void THeavyRequestRetrier::Retry(const std::function<void()> &function)
{
    while (true) {
        try {
            if (!Attempt_) {
                TryStartAttempt();
            }
            function();
            return;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR("RSP %v - attempt %v failed",
                Attempt_->RequestId,
                RequestRetryPolicy_->GetAttemptDescription());
            Attempt_.reset();

            TMaybe<TDuration> backoffDuration;
            if (const auto *errorResponse = dynamic_cast<const TErrorResponse *>(&ex)) {
                if (!IsRetriable(*errorResponse)) {
                    throw;
                }
                backoffDuration = RequestRetryPolicy_->OnRetriableError(*errorResponse);
            } else {
                if (!IsRetriable(ex)) {
                    throw;
                }
                backoffDuration = RequestRetryPolicy_->OnGenericError(ex);
            }

            if (!backoffDuration) {
                throw;
            }
            NDetail::TWaitProxy::Get()->Sleep(*backoffDuration);
        }
    }
}

void THeavyRequestRetrier::TryStartAttempt()
{
    Attempt_ = std::make_unique<TAttempt>();
    Attempt_->Transaction = std::make_unique<TPingableTransaction>(
        Parameters_.ClientRetryPolicy, Parameters_.Context,
        Parameters_.TransactionId,
        Parameters_.TransactionPinger->GetChildTxPinger(),
        TStartTransactionOptions());

    auto header = Parameters_.Header;
    if (Parameters_.Context.ServiceTicketAuth) {
        header.SetServiceTicket(Parameters_.Context.ServiceTicketAuth->Ptr->IssueServiceTicket());
    } else {
        header.SetToken(Parameters_.Context.Token);
    }

    if (Parameters_.Context.ImpersonationUser) {
        header.SetImpersonationUser(*Parameters_.Context.ImpersonationUser);
    }
    auto hostName = GetProxyForHeavyRequest(Parameters_.Context);
    Attempt_->RequestId = CreateGuidAsString();

    UpdateHeaderForProxyIfNeed(hostName, Parameters_.Context, header);

    header.AddTransactionId(Attempt_->Transaction->GetId(), /* overwrite = */ true);
    header.SetRequestCompression(ToString(Parameters_.Context.Config->ContentEncoding));

    Attempt_->Request = Parameters_.Context.HttpClient->StartRequest(
        GetFullUrlForProxy(hostName, Parameters_.Context, header),
        Attempt_->RequestId, header);

    auto stream = StreamFactory_();
    stream->ReadAll(*Attempt_->Request->GetStream());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
