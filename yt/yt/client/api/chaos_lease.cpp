#include "chaos_lease.h"

#include "connection.h"
#include "client.h"
#include "private.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/transaction_client/public.h>

namespace NYT::NApi {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TChaosLease
    : public virtual IPrerequisite
{
public:
    TChaosLease(
        IClientPtr client,
        NChaosClient::TChaosLeaseId id,
        TDuration timeout,
        bool pingAncestors,
        const NLogging::TLogger& logger);

    IClientPtr GetClient() const override;
    NPrerequisiteClient::TPrerequisiteId GetId() const override;
    TDuration GetTimeout() const override;

    TFuture<void> Ping(const TPrerequisitePingOptions& options = {}) override;
    TFuture<void> Abort(const TPrerequisiteAbortOptions& options = {}) override;

    void SubscribeAborted(const TAbortedHandler& handler) override;
    void UnsubscribeAborted(const TAbortedHandler& handler) override;

private:
    const IClientPtr Client_;
    const NChaosClient::TChaosLeaseId Id_;
    const TDuration Timeout_;
    const bool PingAncestors_;

    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TPromise<void> AbortPromise_;

    TSingleShotCallbackList<TAbortedHandlerSignature> Aborted_;
};

////////////////////////////////////////////////////////////////////////////////

TChaosLease::TChaosLease(
    IClientPtr client,
    NChaosClient::TChaosLeaseId id,
    TDuration timeout,
    bool pingAncestors,
    const NLogging::TLogger& logger)
    : Client_(std::move(client))
    , Id_(id)
    , Timeout_(timeout)
    , PingAncestors_(pingAncestors)
    , Logger(logger
        .WithTag("ChaosLeaseId", Id_)
        .WithTags(Client_->GetConnection()->GetLoggingTags()))
{ }

IClientPtr TChaosLease::GetClient() const
{
    return Client_;
}

NPrerequisiteClient::TPrerequisiteId TChaosLease::GetId() const
{
    return Id_;
}

TDuration TChaosLease::GetTimeout() const
{
    return Timeout_;
}

TFuture<void> TChaosLease::Ping(const TPrerequisitePingOptions& /*options*/)
{
    return Client_->PingChaosLease(GetId(), TChaosLeasePingOptions{
        .PingAncestors = PingAncestors_,
    }).Apply(
        BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<void>& resultOrError) {
            if (resultOrError.IsOK()) {
                YT_TLOG_DEBUG("Chaos lease pinged");
            } else if (resultOrError.FindMatching(NYTree::EErrorCode::ResolveError) ||
                resultOrError.FindMatching(NTransactionClient::EErrorCode::NoSuchTransaction))
            {
                // Hard error.
                YT_TLOG_DEBUG("Chaos lease has expired or was aborted");

                {
                    auto guard = Guard(SpinLock_);
                    if (!Aborted_.IsFired()) {
                        Aborted_.Fire(resultOrError);
                    }
                }

                THROW_ERROR(resultOrError);
            } else {
                // Soft error.
                YT_TLOG_DEBUG("Error pinging chaos lease")
                    .With(resultOrError);

                THROW_ERROR_EXCEPTION("Error pinging chaos lease %v",
                    GetId())
                    .With(resultOrError);
            }
        }));
}

TFuture<void> TChaosLease::Abort(const TPrerequisiteAbortOptions& options)
{
    {
        auto guard = Guard(SpinLock_);
        if (AbortPromise_) {
            return AbortPromise_.ToFuture();
        }

        AbortPromise_ = NewPromise<void>();
    }

    auto chaosLeasePath = FromObjectId(GetId());
    auto removeOptions = TRemoveNodeOptions{
        .Force = options.Force,
    };
    return Client_->RemoveNode(chaosLeasePath, removeOptions)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<void>& rspOrError) {
                {
                    auto guard = Guard(SpinLock_);

                    if (!AbortPromise_) {
                        YT_TLOG_DEBUG("Chaos lease is no longer aborting, abort response ignored")
                            .With(rspOrError);
                        return;
                    }

                    TError abortError;
                    if (rspOrError.IsOK()) {
                        YT_TLOG_DEBUG("Chaos lease aborted");
                    } else {
                        YT_TLOG_DEBUG("Error aborting chaos lease")
                            .With(rspOrError);

                        abortError = TError("Error aborting chaos lease %v",
                            GetId())
                            .With(rspOrError);
                    }

                    auto abortPromise = std::exchange(AbortPromise_, TPromise<void>());

                    guard.Release();

                    if (abortError.IsOK()) {
                        Aborted_.Fire(TError("Chaos lease aborted by user request"));
                    }

                    abortPromise.Set(std::move(abortError));
                }
        }));
}

void TChaosLease::SubscribeAborted(const TAbortedHandler& handler)
{
    Aborted_.Subscribe(handler);
}

void TChaosLease::UnsubscribeAborted(const TAbortedHandler& handler)
{
    Aborted_.Unsubscribe(handler);
}

////////////////////////////////////////////////////////////////////////////////

IPrerequisitePtr CreateChaosLease(
    IClientPtr client,
    NChaosClient::TChaosLeaseId id,
    TDuration timeout,
    bool pingAncestors,
    const NLogging::TLogger& logger)
{
    return New<TChaosLease>(
        std::move(client),
        id,
        timeout,
        pingAncestors,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
