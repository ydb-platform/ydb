#include "update_limit_actor.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/rate_limiter/utils/path.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NFq {
namespace {

class TUpdateCloudRateLimitActor : public NActors::TActorBootstrapped<TUpdateCloudRateLimitActor> {
public:
    TUpdateCloudRateLimitActor(
        NActors::TActorId parent,
        TYdbConnectionPtr connection,
        const TString& coordinationNodePath,
        const TString& cloudId,
        ui64 limit,
        TYdbSdkRetryPolicy::TPtr retryPolicy,
        ui64 cookie)
    : Parent(parent)
    , Connection(std::move(connection))
    , CoordinationNodePath(coordinationNodePath)
    , CloudId(cloudId)
    , Limit(limit)
    , RetryPolicy(std::move(retryPolicy))
    , Cookie(cookie)
    {
    }

    void Bootstrap() {
        Become(&TUpdateCloudRateLimitActor::StateFunc);

        if (CloudId.empty()) {
            SendResponseAndPassAway(NYdb::TStatus(NYdb::EStatus::BAD_REQUEST, NYql::TIssues({NYql::TIssue("Cloud id is empty")})));
            return;
        }

        Register(
            MakeUpdateRateLimiterResourceActor(
                SelfId(),
                NKikimrServices::YQ_RATE_LIMITER,
                Connection,
                CoordinationNodePath,
                GetRateLimiterResourcePath(CloudId),
                Limit * 10,
                RetryPolicy
            )
        );
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvents::TEvSchemaUpdated, Handle);
        hFunc(TEvents::TEvSchemaCreated, Handle);
    );

    void Handle(TEvents::TEvSchemaUpdated::TPtr& ev) {
        if (ev->Get()->Result.IsSuccess() || !IsPathDoesNotExistError(ev->Get()->Result)) {
            SendResponseAndPassAway(std::move(ev->Get()->Result));
            return;
        }

        // Path doesn't exist. Create it with new quota value
        Register(
            MakeCreateRateLimiterResourceActor(
                SelfId(),
                NKikimrServices::YQ_RATE_LIMITER,
                Connection,
                CoordinationNodePath,
                GetRateLimiterResourcePath(CloudId),
                {Limit * 10}, // percent -> milliseconds
                RetryPolicy
            )
        );
    }

    void Handle(TEvents::TEvSchemaCreated::TPtr& ev) {
        SendResponseAndPassAway(std::move(ev->Get()->Result));
    }

    void SendResponseAndPassAway(NYdb::TStatus status) {
        Send(
            Parent,
            new TEvents::TEvSchemaUpdated(std::move(status)),
            0,
            Cookie
        );
        PassAway();
    }

private:
    const NActors::TActorId Parent;
    const TYdbConnectionPtr Connection;
    const TString CoordinationNodePath;
    const TString CloudId;
    const ui64 Limit;
    const TYdbSdkRetryPolicy::TPtr RetryPolicy;
    const ui64 Cookie;
};

} // anonymous namespace

NActors::IActor* MakeUpdateCloudRateLimitActor(
    NActors::TActorId parent,
    TYdbConnectionPtr connection,
    const TString& coordinationNodePath,
    const TString& cloudId,
    ui64 limit,
    TYdbSdkRetryPolicy::TPtr retryPolicy,
    ui64 cookie)
{
    return new TUpdateCloudRateLimitActor(
        parent,
        std::move(connection),
        coordinationNodePath,
        cloudId,
        limit,
        std::move(retryPolicy),
        cookie
    );
}

} // namespace NFq
