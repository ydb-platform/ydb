#include "cfg.h"
#include "error.h"
#include "proxy_actor.h"

#include <ydb/core/ymq/base/action.h>
#include <ydb/core/protos/sqs.pb.h>
#include <ydb/core/ymq/base/counters.h>
#include <ydb/core/ymq/base/security.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/string/builder.h>
#include <util/system/defaults.h>


namespace NKikimr::NSQS {


#define SQS_SWITCH_REQUEST(request, default_case)       \
    SQS_SWITCH_REQUEST_CUSTOM(request, ENUMERATE_PROXY_ACTIONS, default_case)

TString SecurityPrint(const NKikimrClient::TSqsResponse& resp) {
    switch (resp.GetResponseCase()) {
        case NKikimrClient::TSqsResponse::kChangeMessageVisibility:
        case NKikimrClient::TSqsResponse::kCreateQueue:
        case NKikimrClient::TSqsResponse::kGetQueueAttributes:
        case NKikimrClient::TSqsResponse::kDeleteMessage:
        case NKikimrClient::TSqsResponse::kDeleteQueue:
        case NKikimrClient::TSqsResponse::kListQueues:
        case NKikimrClient::TSqsResponse::kPurgeQueue:
        case NKikimrClient::TSqsResponse::kSendMessage:
        case NKikimrClient::TSqsResponse::kSetQueueAttributes:
        case NKikimrClient::TSqsResponse::kGetQueueUrl:
        case NKikimrClient::TSqsResponse::kChangeMessageVisibilityBatch:
        case NKikimrClient::TSqsResponse::kDeleteMessageBatch:
        case NKikimrClient::TSqsResponse::kSendMessageBatch:
        case NKikimrClient::TSqsResponse::kCreateUser:
        case NKikimrClient::TSqsResponse::kDeleteUser:
        case NKikimrClient::TSqsResponse::kListUsers:
        case NKikimrClient::TSqsResponse::kModifyPermissions:
        case NKikimrClient::TSqsResponse::kListPermissions:
        case NKikimrClient::TSqsResponse::kDeleteQueueBatch:
        case NKikimrClient::TSqsResponse::kPurgeQueueBatch:
        case NKikimrClient::TSqsResponse::kGetQueueAttributesBatch:
        case NKikimrClient::TSqsResponse::kListDeadLetterSourceQueues:
        case NKikimrClient::TSqsResponse::kCountQueues:
        case NKikimrClient::TSqsResponse::kListQueueTags:
        case NKikimrClient::TSqsResponse::kTagQueue:
        case NKikimrClient::TSqsResponse::kUntagQueue: {
            return TStringBuilder() << resp;
        }
        case NKikimrClient::TSqsResponse::kReceiveMessage: {
            NKikimrClient::TSqsResponse respCopy = resp;
            for (auto& msg : *respCopy.MutableReceiveMessage()->MutableMessages()) {
                msg.SetData(TStringBuilder() << "[...user_data_" << msg.GetData().size() << "bytes" << "...]");
            }
            return TStringBuilder() << respCopy;
        }
        default: {
            return TStringBuilder() << "unsupported to print response with case=" << static_cast<ui64>(resp.GetResponseCase()) << "request=" << resp.GetRequestId();
        }
    }
    Y_ABORT_UNLESS(false);
}

std::tuple<TString, TString, TString> ParseCloudSecurityToken(const TString& token) {
    TStringBuf tokenBuf(token);
    TString userName = TString(tokenBuf.NextTok(':'));
    TString folderId = TString(tokenBuf.NextTok(':'));
    TString userSID = TString(tokenBuf.NextTok(':'));
    return {userName, folderId, userSID};
}

void TProxyActor::Bootstrap() {
    RetrieveUserAndQueueParameters();
    this->Become(&TProxyActor::StateFunc);

    StartTs_ = TActivationContext::Now();
    RLOG_SQS_DEBUG("Request proxy started");

    if (!UserName_ || !QueueName_) {
        RLOG_SQS_WARN("Validation error: No " << (!UserName_ ? "user name" : "queue name") << " in proxy actor");
        SendErrorAndDie(NErrors::INVALID_PARAMETER_VALUE, "Both account and queue name should be specified.");
        return;
    }

    const auto& cfg = Cfg();
    if (cfg.GetRequestTimeoutMs()) {
        TimeoutCookie_.Reset(ISchedulerCookie::Make2Way());
        this->Schedule(TDuration::MilliSeconds(cfg.GetRequestTimeoutMs()), new TEvWakeup(), TimeoutCookie_.Get());
    }

    RequestConfiguration();
}

void TProxyActor::HandleConfiguration(TSqsEvents::TEvConfiguration::TPtr& ev) {
    const TDuration confDuration = TActivationContext::Now() - StartTs_;
    RLOG_SQS_DEBUG("Get configuration duration: " << confDuration.MilliSeconds() << "ms");

    QueueCounters_ = std::move(ev->Get()->QueueCounters);
    UserCounters_ = std::move(ev->Get()->UserCounters);
    if (QueueCounters_) {
        auto* detailedCounters = QueueCounters_ ? QueueCounters_->GetDetailedCounters() : nullptr;
        COLLECT_HISTOGRAM_COUNTER(detailedCounters, GetConfiguration_Duration, confDuration.MilliSeconds());
    }

    if (ev->Get()->Throttled) {
        RLOG_SQS_ERROR("Attempt to get configuration was throttled");
        SendErrorAndDie(NErrors::THROTTLING_EXCEPTION, "Too many requests to nonexistent queue.");
        return;
    }

    if (ev->Get()->Fail) {
        RLOG_SQS_ERROR("Failed to get configuration");
        SendErrorAndDie(NErrors::INTERNAL_FAILURE, "Failed to get configuration.");
        return;
    }

    if (!ev->Get()->QueueExists) {
        SendErrorAndDie(NErrors::NON_EXISTENT_QUEUE);
        return;
    }

    Send(MakeSqsProxyServiceID(SelfId().NodeId()), MakeHolder<TSqsEvents::TEvProxySqsRequest>(Request_, UserName_, QueueName_));
}

STATEFN(TProxyActor::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TSqsEvents::TEvConfiguration, HandleConfiguration);
        hFunc(TSqsEvents::TEvProxySqsResponse, HandleResponse);
        hFunc(TEvWakeup, HandleWakeup);
    }
}

void TProxyActor::RequestConfiguration() {
    Send(MakeSqsServiceID(SelfId().NodeId()),
        MakeHolder<TSqsEvents::TEvGetConfiguration>(
            RequestId_,
            UserName_,
            QueueName_)
    );
}

void TProxyActor::SendReplyAndDie(const NKikimrClient::TSqsResponse& resp) {
    if (ErrorResponse_) {
        RLOG_SQS_WARN("Sending error reply from proxy actor: " << resp);
    } else {
        RLOG_SQS_DEBUG("Sending reply from proxy actor: " << SecurityPrint(resp));
    }
    Cb_->DoSendReply(resp);
    PassAway();
}

void TProxyActor::SendErrorAndDie(const TErrorClass& error, const TString& message) {
    ErrorResponse_ = true;
    auto* detailedCounters = UserCounters_ ? UserCounters_->GetDetailedCounters() : nullptr;
    if (detailedCounters) {
        detailedCounters->APIStatuses.AddError(error.ErrorCode);
    }
    NKikimrClient::TSqsResponse response;
#define SQS_REQUEST_CASE(action)                                    \
    MakeError(response.Y_CAT(Mutable, action)(), error, message);   \
    response.Y_CAT(Mutable, action)()->SetRequestId(RequestId_);

    SQS_SWITCH_REQUEST(Request_, Y_ABORT_UNLESS(false));

#undef SQS_REQUEST_CASE

    if (Cfg().GetYandexCloudMode()) {
        response.SetFolderId(FolderId_);
        response.SetIsFifo(false);
        response.SetResourceId(QueueName_);
    }

    SendReplyAndDie(response);
}

void TProxyActor::HandleResponse(TSqsEvents::TEvProxySqsResponse::TPtr& ev) {
    RLOG_SQS_TRACE("HandleResponse: " << ev->Get()->Record << ", status: " << ev->Get()->ProxyStatus);
    if (ev->Get()->ProxyStatus == TSqsEvents::TEvProxySqsResponse::EProxyStatus::OK) {
        SendReplyAndDie(ev->Get()->Record);
    } else {
        SendErrorAndDie(GetErrorClass(ev->Get()->ProxyStatus));
    }
}

void TProxyActor::HandleWakeup(TEvWakeup::TPtr&) {
    TString actionName;

#define SQS_REQUEST_CASE(action) actionName = Y_STRINGIZE(action);

    SQS_SWITCH_REQUEST(Request_, break;);

#undef SQS_REQUEST_CASE

    RLOG_SQS_ERROR("Proxy request timeout. User [" << UserName_ << "] Queue [" << QueueName_ << "] Action [" << actionName << "]");

    if (QueueCounters_) {
        INC_COUNTER_COUPLE(QueueCounters_, RequestTimeouts, request_timeouts_count_per_second);
    } else {
        auto rootCounters = TIntrusivePtrCntrCouple{
            GetSqsServiceCounters(AppData()->Counters, "core"),
            GetYmqPublicCounters(AppData()->Counters)
        };
        auto [userCountersCouple, queueCountersCouple] = GetUserAndQueueCounters(rootCounters, TQueuePath(Cfg().GetRoot(), UserName_, QueueName_));
        if (queueCountersCouple.SqsCounters) {
            queueCountersCouple.SqsCounters->GetCounter("RequestTimeouts", true)->Inc();
        }
//        if (queueCountersCouple.YmqCounters) {
//            queueCountersCouple.YmqCounters->GetCounter("request_timeouts_count_per_second", true)->Inc();
//        }
    }

    SendErrorAndDie(NErrors::TIMEOUT);
}

const TErrorClass& TProxyActor::GetErrorClass(TSqsEvents::TEvProxySqsResponse::EProxyStatus proxyStatus) {
    using EProxyStatus = TSqsEvents::TEvProxySqsResponse::EProxyStatus;
    switch (proxyStatus) {
    case EProxyStatus::LeaderResolvingError:
        return NErrors::LEADER_RESOLVING_ERROR;
    case EProxyStatus::SessionError:
        return NErrors::LEADER_SESSION_ERROR;
    case EProxyStatus::QueueDoesNotExist:
    case EProxyStatus::UserDoesNotExist:
        return NErrors::NON_EXISTENT_QUEUE;
    case EProxyStatus::Throttled:
        return NErrors::THROTTLING_EXCEPTION;
    default:
        return NErrors::INTERNAL_FAILURE;
    }
}

bool TProxyActor::NeedCreateProxyActor(const NKikimrClient::TSqsRequest& req) {
#define SQS_REQUEST_CASE(action) return true;

    SQS_SWITCH_REQUEST(req, return false)

#undef SQS_REQUEST_CASE
}

bool TProxyActor::NeedCreateProxyActor(EAction action) {
    return IsProxyAction(action);
}

void TProxyActor::RetrieveUserAndQueueParameters() {
    TString securityToken;
#define SQS_REQUEST_CASE(action)                                        \
    const auto& request = Request_.Y_CAT(Get, action)();                \
    UserName_ = request.GetAuth().GetUserName();                        \
    FolderId_ = request.GetAuth().GetFolderId();                        \
    QueueName_ = request.GetQueueName();                                \
    securityToken = ExtractSecurityToken(request);                      \

    SQS_SWITCH_REQUEST(Request_, throw TSQSException(NErrors::INVALID_ACTION) << "Incorrect request type")

    #undef SQS_REQUEST_CASE

    if (Cfg().GetYandexCloudMode() && !FolderId_) {
        auto items = ParseCloudSecurityToken(securityToken);
        UserName_ = std::get<0>(items);
        FolderId_ = std::get<1>(items);
    }
    RLOG_SQS_DEBUG("Proxy actor: used user_name='" << UserName_ << "', queue_name='" << QueueName_ << "', folder_id='" << FolderId_ << "'");
}

} // namespace NKikimr::NSQS
