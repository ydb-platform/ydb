#include "pinger.h"

#include <ydb/core/fq/libs/config/protos/pinger.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/private_client/internal_service.h>

#include <ydb/public/lib/fq/scope.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/protobuf/interop/cast.h>

#include <util/datetime/base.h>
#include <util/generic/utility.h>

#include <deque>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_PINGER, "QueryId: " << Id << ", Owner: " << OwnerId  << " " << stream)
#define LOG_W(stream) LOG_WARN_S (*TlsActivationContext, NKikimrServices::FQ_PINGER, "QueryId: " << Id << ", Owner: " << OwnerId  << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_PINGER, "QueryId: " << Id << ", Owner: " << OwnerId  << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_PINGER, "QueryId: " << Id << ", Owner: " << OwnerId  << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NYql;
using namespace NFq;

struct TEvPingResponse : public NActors::TEventLocal<TEvPingResponse, NActors::TEvents::TSystem::CallbackCompletion> {
    TPingTaskResult Result;
    FederatedQuery::QueryAction Action = FederatedQuery::QUERY_ACTION_UNSPECIFIED;

    explicit TEvPingResponse(TPingTaskResult&& result)
        : Result(std::move(result))
        , Action(Result.IsResultSet() ? Result.GetResult().action() : FederatedQuery::QUERY_ACTION_UNSPECIFIED)
    {
    }

    explicit TEvPingResponse(const TString& errorMessage)
        : TEvPingResponse(MakeResultFromErrorMessage(errorMessage))
    {
    }

private:
    static TPingTaskResult MakeResultFromErrorMessage(const TString& errorMessage) {
        NYql::TIssues issues;
        issues.AddIssue(errorMessage);
        return TPingTaskResult(NYdb::TStatus(NYdb::EStatus::INTERNAL_ERROR, std::move(issues)), nullptr);
    }
};

class TPingerActor : public NActors::TActorBootstrapped<TPingerActor> {
    class TRetryState {
    public:
        void Init(TInstant now, TInstant startLeaseTime, TDuration maxRetryTime) {
            StartRequestTime = now;
            StartLeaseTime = startLeaseTime;
            Delay = TDuration::Zero();
            RetriesCount = 0;
            MaxRetryTime = maxRetryTime;
        }

        void UpdateStartLeaseTime(TInstant startLeaseTime) {
            StartLeaseTime = startLeaseTime;
        }

        TMaybe<TDuration> GetNextDelay(TInstant now) {
            if (now >= StartLeaseTime + MaxRetryTime) {
                return Nothing();
            }

            const TDuration nextDelay = Delay; // The first delay is zero
            Delay = ClampVal(Delay * 2, MinDelay, MaxDelay);

            const TDuration randomizedNextDelay = nextDelay ? RandomizeDelay(nextDelay) : nextDelay;
            if (now + randomizedNextDelay < StartLeaseTime + MaxRetryTime) {
                ++RetriesCount;
                return randomizedNextDelay;
            }
            return Nothing();
        }

        TDuration GetRetryTime(TInstant now) const {
            return now - StartRequestTime;
        }

        size_t GetRetriesCount() const {
            return RetriesCount;
        }

        operator bool() const {
            return StartRequestTime != TInstant::Zero(); // State has been initialized.
        }

    private:
        static TDuration RandomizeDelay(TDuration baseDelay) {
            const TDuration::TValue half = baseDelay.GetValue() / 2;
            return TDuration::FromValue(half + RandomNumber<TDuration::TValue>(half));
        }

    private:
        TDuration Delay; // The first retry will be done instantly.
        TInstant StartLeaseTime;
        TInstant StartRequestTime;
        size_t RetriesCount = 0;

        TDuration MaxRetryTime;
        static constexpr TDuration MaxDelay = TDuration::Seconds(5);
        static constexpr TDuration MinDelay = TDuration::MilliSeconds(100); // from second retry
    };

    struct TForwardPingReqInfo {
        TForwardPingReqInfo(TEvents::TEvForwardPingRequest::TPtr&& ev)
            : Request(std::move(ev))
        {
        }

        TEvents::TEvForwardPingRequest::TPtr Request;
        bool Requested = false;
        TRetryState RetryState;
    };

    struct TConfig {
        NConfig::TPingerConfig Proto;
        TDuration PingPeriod = TDuration::Seconds(15);

        TConfig(const NConfig::TPingerConfig& config)
            : Proto(config)
        {
            if (Proto.GetPingPeriod()) {
                Y_ABORT_UNLESS(TDuration::TryParse(Proto.GetPingPeriod(), PingPeriod));
            }
        }
    };

public:
    TPingerActor(
        const TString& tenantName,
        const NYdb::NFq::TScope& scope,
        const TString& userId,
        const TString& id,
        const TString& ownerId,
        const TActorId parent,
        const NConfig::TPingerConfig& config,
        TInstant deadline,
        const ::NYql::NCommon::TServiceCounters& queryCounters,
        TInstant createdAt,
        bool replyToSender)
        : Config(config)
        , TenantName(tenantName)
        , Scope(scope)
        , UserId(userId)
        , Id(id)
        , OwnerId(ownerId)
        , Parent(parent)
        , Deadline(deadline)
        , QueryCounters(queryCounters)
        , CreatedAt(createdAt)
        , InternalServiceId(MakeInternalServiceActorId())
        , ReplyToSender(replyToSender)
    {
    }

    static constexpr char ActorName[] = "YQ_PINGER";

    void Bootstrap() {
        LOG_T("Start Pinger");
        StartLeaseTime = TActivationContext::Now(); // Not accurate value, but it allows us to retry the first unsuccessful ping request.
        ScheduleNextPing();
        Become(&TPingerActor::StateFunc);
    }

private:
    STRICT_STFUNC(
        StateFunc,
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway)
        hFunc(NActors::TEvents::TEvWakeup, Wakeup)
        hFunc(TEvInternalService::TEvPingTaskResponse, Handle)
        hFunc(TEvents::TEvForwardPingRequest, Handle)
    )

    void PassAway() override {
        LOG_T("Stop Pinger");
        NActors::TActorBootstrapped<TPingerActor>::PassAway();
    }

    void ScheduleNextPing() {
        if (!Finishing) {
            SchedulerCookieHolder.Reset(ISchedulerCookie::Make2Way());
            Schedule(Config.PingPeriod, new NActors::TEvents::TEvWakeup(ContinueLeaseWakeupTag), SchedulerCookieHolder.Get());
        }
    }

    void Wakeup(NActors::TEvents::TEvWakeup::TPtr& ev) {
        if (FatalError) {
            LOG_D("Got wakeup after fatal error. Ignore");
            return;
        }

        switch (ev->Get()->Tag) {
        case ContinueLeaseWakeupTag:
            WakeupContinueLease();
            break;
        case RetryContinueLeaseWakeupTag:
            WakeupRetryContinueLease();
            break;
        case RetryForwardPingRequestWakeupTag:
            WakeupRetryForwardPingRequest();
            break;
        default:
            Y_ABORT("Unknow wakeup tag: %lu", ev->Get()->Tag);
        }
    }

    void WakeupContinueLease() {
        SchedulerCookieHolder.Reset(nullptr);
        if (!Finishing) {
            Ping();
        }
    }

    void WakeupRetryContinueLease() {
        Ping(true);
    }

    void WakeupRetryForwardPingRequest() {
        Y_ABORT_UNLESS(!ForwardRequests.empty());
        auto& reqInfo = ForwardRequests.front();
        Y_ABORT_UNLESS(!reqInfo.Requested);
        ForwardPing(true);
    }

    void Handle(TEvents::TEvForwardPingRequest::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Cookie != ContinueLeaseRequestCookie);
        if (Finishing) {
            LOG_E("Attempt to send a ping request to the terminating actor " << ev->Get()->Request);
            Send(ReplyToSender ? ev->Sender : Parent, new TEvents::TEvForwardPingResponse(false, FederatedQuery::QueryAction::QUERY_ACTION_UNSPECIFIED), 0, ev->Cookie);
            return;
        }

        if (ev->Get()->Final) {
            Finishing = true;
            SchedulerCookieHolder.Reset(nullptr);
        }

        LOG_T("Forward ping request: " << ev->Get()->Request);
        if (FatalError) {
            if (Finishing) {
                LOG_D("Got final ping request after fatal error");
                PassAway();
            }
        } else {
            ForwardRequests.emplace_back(std::move(ev));
            ForwardPing();
        }
    }

    void SendQueryAction(FederatedQuery::QueryAction action) {
        if (!Finishing) {
            Send(Parent, new TEvents::TEvQueryActionResult(action));
        }
    }

    static bool Retryable(TEvInternalService::TEvPingTaskResponse::TPtr& ev) {
        if (ev->Get()->Status.IsTransportError()) {
            return true;
        }

        const NYdb::EStatus status = ev->Get()->Status.GetStatus();
        if (status == NYdb::EStatus::INTERNAL_ERROR
            || status == NYdb::EStatus::UNAVAILABLE
            || status == NYdb::EStatus::OVERLOADED
            || status == NYdb::EStatus::TIMEOUT
            || status == NYdb::EStatus::BAD_SESSION
            || status == NYdb::EStatus::SESSION_EXPIRED
            || status == NYdb::EStatus::SESSION_BUSY) {
            return true;
        }

        return false;
    }

    void Handle(TEvInternalService::TEvPingTaskResponse::TPtr& ev) {
        if (FatalError) {
            LOG_D("Got ping response after fatal error. Ignore");
            return;
        }

        const TInstant now = TActivationContext::Now();
        bool success = ev->Get()->Status.IsSuccess();
        bool retryable = !success && Retryable(ev);

        TString errorMessage;
        if (ev->Get()->Result.has_expired_at()) {
            TInstant expiredAt = NProtoInterop::CastFromProto(ev->Get()->Result.expired_at());
            if (expiredAt < now) {
                success = false;
                retryable = false;
                errorMessage += "Query ownership time is over: expired_at=" + expiredAt.ToString() + " < now=" + now.ToString();
            }
        }

        if (!success) {
            errorMessage += ev->Get()->Status.GetIssues().ToOneLineString();
        }

        const bool continueLeaseRequest = ev->Cookie == ContinueLeaseRequestCookie;
        TRetryState* retryState = nullptr;
        Y_ABORT_UNLESS(continueLeaseRequest || !ForwardRequests.empty());
        if (retryable) {
            if (continueLeaseRequest) {
                retryState = &RetryState;
            } else {
                retryState = &ForwardRequests.front().RetryState;
            }
            Y_ABORT_UNLESS(*retryState); // Initialized
        }

        if (continueLeaseRequest) {
            Y_ABORT_UNLESS(Requested);
            Requested = false;
        } else {
            Y_ABORT_UNLESS(ForwardRequests.front().Requested);
            ForwardRequests.front().Requested = false;
        }

        TMaybe<TDuration> retryAfter;
        if (retryable) {
            retryState->UpdateStartLeaseTime(StartLeaseTime);
            retryAfter = retryState->GetNextDelay(now);
        }

        if (success) {
            LOG_T("Ping response success: " << ev->Get()->Result);
            StartLeaseTime = now;
            auto action = ev->Get()->Result.action();
            if (action != FederatedQuery::QUERY_ACTION_UNSPECIFIED && !Finishing) {
                LOG_D("Query action: " << FederatedQuery::QueryAction_Name(action));
                SendQueryAction(action);
            }

            if (continueLeaseRequest) {
                ScheduleNextPing();
            } else {
                Send(ReplyToSender ? ForwardRequests.front().Request->Sender : Parent, new TEvents::TEvForwardPingResponse(true, action), 0, ev->Cookie);
                ForwardRequests.pop_front();

                // Process next forward ping request.
                if (!ForwardRequests.empty()) {
                    ForwardPing();
                }
            }
        } else if (retryAfter) {
            LOG_W("Ping response error: " << errorMessage << ". Retry after: " << *retryAfter);
            Schedule(*retryAfter, new NActors::TEvents::TEvWakeup(continueLeaseRequest ? RetryContinueLeaseWakeupTag : RetryForwardPingRequestWakeupTag));
        } else {
            TRetryState* retryStateForLogging = retryState;
            if (!retryStateForLogging) {
                retryStateForLogging = continueLeaseRequest ? &RetryState : &ForwardRequests.front().RetryState;
            }
            LOG_E("Ping response error: " << errorMessage << ". Retried " << retryStateForLogging->GetRetriesCount() << " times during " << retryStateForLogging->GetRetryTime(now));
            auto action = ev->Get()->Status.IsSuccess() ? ev->Get()->Result.action() : FederatedQuery::QUERY_ACTION_UNSPECIFIED;
            if (ReplyToSender) {
                for (const auto& forwardRequest: ForwardRequests) {
                    Send(forwardRequest.Request->Sender, new TEvents::TEvForwardPingResponse(false, action), 0, ev->Cookie);
                }
            } else {
                Send(Parent, new TEvents::TEvForwardPingResponse(false, action), 0, ev->Cookie);
            }
            FatalError = true;
            ForwardRequests.clear();
        }

        if (Finishing && ForwardRequests.empty() && !Requested) {
            LOG_D("Query finished");
            PassAway();
        }
    }

    void ForwardPing(bool retry = false) {
        Y_ABORT_UNLESS(!ForwardRequests.empty());
        auto& reqInfo = ForwardRequests.front();
        if (!reqInfo.Requested && (retry || !reqInfo.RetryState)) {
            reqInfo.Requested = true;
            Y_ABORT_UNLESS(!retry || reqInfo.RetryState);
            if (!retry && !reqInfo.RetryState) {
                reqInfo.RetryState.Init(TActivationContext::Now(), StartLeaseTime, Config.PingPeriod);
            }
            LOG_T((retry ? "Retry forward" : "Forward") << " request Private::PingTask");

            Ping(reqInfo.Request->Get()->Request, reqInfo.Request->Cookie);
        }
    }

    void Ping(bool retry = false) {
        LOG_T((retry ? "Retry request" : "Request") << " Private::PingTask");

        Y_ABORT_UNLESS(!Requested);
        Requested = true;

        if (!retry) {
            RetryState.Init(TActivationContext::Now(), StartLeaseTime, Config.PingPeriod);
        }
        Ping(Fq::Private::PingTaskRequest(), ContinueLeaseRequestCookie);
    }

    void Ping(Fq::Private::PingTaskRequest request, ui64 cookie) {
        QueryCounters.SetUptimePublicAndServiceCounter((TInstant::Now() - CreatedAt).Seconds());
        // Fill ids
        request.set_tenant(TenantName);
        request.set_scope(Scope.ToString());
        request.set_owner_id(OwnerId);
        request.mutable_query_id()->set_value(Id);
        *request.mutable_deadline() = NProtoInterop::CastToProto(Deadline);
        Send(InternalServiceId, new TEvInternalService::TEvPingTaskRequest(request), 0, cookie);
    }

    static constexpr ui64 ContinueLeaseRequestCookie = Max();

    enum : ui64 {
        ContinueLeaseWakeupTag,
        RetryContinueLeaseWakeupTag,
        RetryForwardPingRequestWakeupTag,
    };

    TConfig Config;

    const TString TenantName;
    const NYdb::NFq::TScope Scope;
    const TString UserId;
    const TString Id;
    const TString OwnerId;

    bool Requested = false;
    TInstant StartLeaseTime;
    TRetryState RetryState;
    const TActorId Parent;
    const TInstant Deadline;

    const ::NYql::NCommon::TServiceCounters QueryCounters;
    const TInstant CreatedAt;

    std::deque<TForwardPingReqInfo> ForwardRequests;
    bool Finishing = false;
    bool FatalError = false; // Nonretryable error from PingTask or all retries finished.

    TSchedulerCookieHolder SchedulerCookieHolder;
    TActorId InternalServiceId;
    bool ReplyToSender = false;
};

IActor* CreatePingerActor(
    const TString& tenantName,
    const NYdb::NFq::TScope& scope,
    const TString& userId,
    const TString& id,
    const TString& ownerId,
    const TActorId parent,
    const NConfig::TPingerConfig& config,
    TInstant deadline,
    const ::NYql::NCommon::TServiceCounters& queryCounters,
    TInstant createdAt,
    bool replyToSender)
{
    return new TPingerActor(
        tenantName,
        scope,
        userId,
        id,
        ownerId,
        parent,
        config,
        deadline,
        queryCounters,
        createdAt,
        replyToSender);
}

} /* NFq */
