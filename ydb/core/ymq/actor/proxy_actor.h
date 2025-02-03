#pragma once
#include "defs.h"
#include "actor.h"
#include "error.h"
#include "events.h"
#include "log.h"
#include "serviceid.h"

#include <ydb/core/ymq/base/counters.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NSQS {

std::tuple<TString, TString, TString> ParseCloudSecurityToken(const TString& token);

class TProxyActor
    : public TActorBootstrapped<TProxyActor>
{
public:
    TProxyActor(const NKikimrClient::TSqsRequest& req, THolder<IReplyCallback> cb)
        : RequestId_(req.GetRequestId())
        , Request_(req)
        , Cb_(std::move(cb))
    {
        Y_ABORT_UNLESS(RequestId_);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_PROXY_ACTOR;
    }

    // Watches request type and returns true if this type assumes proxying request to other queue leader node.
    // So, TProxyActor must be created only if this function returns true.
    static bool NeedCreateProxyActor(const NKikimrClient::TSqsRequest& req);
    static bool NeedCreateProxyActor(EAction action);

    void Bootstrap();

private:
    STATEFN(StateFunc);

    void HandleConfiguration(TSqsEvents::TEvConfiguration::TPtr& ev);
    void HandleResponse(TSqsEvents::TEvProxySqsResponse::TPtr& ev);
    void HandleWakeup(TEvWakeup::TPtr& ev);

    void RequestConfiguration();

    void RetrieveUserAndQueueParameters();

    void SendReplyAndDie(const NKikimrClient::TSqsResponse& resp);
    void SendErrorAndDie(const TErrorClass& error, const TString& message = TString());
    static const TErrorClass& GetErrorClass(TSqsEvents::TEvProxySqsResponse::EProxyStatus proxyStatus);

private:
    const TString RequestId_;
    NKikimrClient::TSqsRequest Request_;
    TString QueueName_;
    TString UserName_;
    TString FolderId_;
    THolder<IReplyCallback> Cb_;
    bool ErrorResponse_ = false;
    TInstant StartTs_;
    TSchedulerCookieHolder TimeoutCookie_;

    TIntrusivePtr<TUserCounters> UserCounters_;
    TIntrusivePtr<TQueueCounters> QueueCounters_;
};

} // namespace NKikimr::NSQS
