#include "chaos_lease_base.h"

#include "connection.h"
#include "client.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NApi {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TChaosLeaseBase::TChaosLeaseBase(
    IClientPtr client,
    NRpc::IChannelPtr channel,
    NChaosClient::TChaosLeaseId id,
    TDuration timeout,
    bool pingAncestors,
    std::optional<TDuration> pingPeriod,
    const NLogging::TLogger& logger)
    : Client_(std::move(client))
    , Channel_(std::move(channel))
    , Id_(id)
    , Timeout_(timeout)
    , PingAncestors_(pingAncestors)
    , PingPeriod_(pingPeriod)
    , Logger(logger.WithTag("ChaosLeaseId: %v, %v",
        Id_,
        Client_->GetConnection()->GetLoggingTag()))
{ }

NApi::IClientPtr TChaosLeaseBase::GetClient() const
{
    return Client_;
}

NPrerequisiteClient::TPrerequisiteId TChaosLeaseBase::GetId() const
{
    return Id_;
}

TDuration TChaosLeaseBase::GetTimeout() const
{
    return Timeout_;
}

TFuture<void> TChaosLeaseBase::Ping(const TPrerequisitePingOptions& options)
{
    return DoPing(options).Apply(
        BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<void>& resultOrError) {
            if (resultOrError.IsOK()) {
                YT_LOG_DEBUG("Chaos lease pinged");
            } else if (resultOrError.FindMatching(NChaosClient::EErrorCode::ChaosLeaseNotKnown)) {
                // Hard error.
                YT_LOG_DEBUG("Chaos lease has expired or was aborted");

                {
                    auto guard = Guard(SpinLock_);
                    if (!Aborted_.IsFired()) {
                        Aborted_.Fire(resultOrError);
                    }
                }

                THROW_ERROR(resultOrError);
            } else {
                // Soft error.
                YT_LOG_DEBUG(resultOrError, "Error pinging chaos lease");

                THROW_ERROR_EXCEPTION("Error pinging chaos lease %v",
                    GetId())
                    << resultOrError;
            }
        }));
}

TFuture<void> TChaosLeaseBase::Abort(const TPrerequisiteAbortOptions& options)
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
                        YT_LOG_DEBUG(rspOrError, "Chaos lease is no longer aborting, abort response ignored");
                        return;
                    }

                    TError abortError;
                    if (rspOrError.IsOK()) {
                        YT_LOG_DEBUG("Chaos lease aborted");
                    } else {
                        YT_LOG_DEBUG(rspOrError, "Error aborting chaos lease");

                        abortError = TError("Error aborting chaos lease %v",
                            GetId())
                            << rspOrError;
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

void TChaosLeaseBase::SubscribeAborted(const TAbortedHandler& handler)
{
    Aborted_.Subscribe(handler);
}

void TChaosLeaseBase::UnsubscribeAborted(const TAbortedHandler& handler)
{
    Aborted_.Unsubscribe(handler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
