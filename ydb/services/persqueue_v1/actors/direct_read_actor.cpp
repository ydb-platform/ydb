#include "direct_read_actor.h"

#include "helpers.h"
#include "read_init_auth_actor.h"
#include "read_session_actor.h"

#include <ydb/library/persqueue/topic_parser/counters.h>
#include <ydb/core/persqueue/dread_cache_service/caching_service.h>

#include <library/cpp/protobuf/util/repeated_field_utils.h>

#include <google/protobuf/util/time_util.h>

#include <util/string/join.h>
#include <util/string/strip.h>

#include <utility>

#define LOG_PREFIX "Direct read proxy " << ctx.SelfID.ToString() << ": " PQ_LOG_PREFIX

namespace NKikimr::NGRpcProxy::V1 {

using namespace NKikimrClient;
using namespace NMsgBusProxy;
using namespace PersQueue::V1;

TDirectReadSessionActor::TDirectReadSessionActor(
        TEvStreamReadRequest* request, const ui64 cookie,
        const TActorId& schemeCache, const TActorId& newSchemeCache,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
        const TMaybe<TString> clientDC,
        const NPersQueue::TTopicsListController& topicsHandler)
    : TRlHelpers({}, request, READ_BLOCK_SIZE, false, TDuration::Minutes(1))
    , Request(request)
    , Cookie(cookie)
    , ClientDC(clientDC.GetOrElse("other"))
    , StartTimestamp(TInstant::Now())
    , SchemeCache(schemeCache)
    , NewSchemeCache(newSchemeCache)
    , InitDone(false)
    , ForceACLCheck(false)
    , LastACLCheckTimestamp(TInstant::Zero())
    , Counters(counters)
    , TopicsHandler(topicsHandler)
{
    Y_ASSERT(Request);
}

void TDirectReadSessionActor::Bootstrap(const TActorContext& ctx) {
    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        ++(*GetServiceCounters(Counters, "pqproxy|readSession")
           ->GetNamedCounter("sensor", "DirectSessionsCreatedTotal", true));
    }

    Request->GetStreamCtx()->Attach(ctx.SelfID);
    if (!ReadFromStreamOrDie(ctx)) {
        return;
    }

    StartTime = ctx.Now();
    this->Become(&TDirectReadSessionActor::TThis::StateFunc);
}

void TDirectReadSessionActor::Handle(typename IContext::TEvNotifiedWhenDone::TPtr&, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, LOG_PREFIX << " grpc closed");
    Die(ctx);
}

bool TDirectReadSessionActor::ReadFromStreamOrDie(const TActorContext& ctx) {
    if (!Request->GetStreamCtx()->Read()) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, LOG_PREFIX << " grpc read failed at start");
        Die(ctx);
        return false;
    }
    return true;
}

void TDirectReadSessionActor::Handle(typename IContext::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
    auto& request = ev->Get()->Record;

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, LOG_PREFIX << " grpc read done"
        << ": success# " << ev->Get()->Success
        << ", data# " << request);

    if (!ev->Get()->Success) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, LOG_PREFIX << "grpc read failed");
        ctx.Send(ctx.SelfID, new TEvPQProxy::TEvDone());
        return;
    }

    switch (request.client_message_case()) {
        case TClientMessage::kInitRequest: {
            ctx.Send(ctx.SelfID, new TEvPQProxy::TEvInitDirectRead(request, Request->GetStreamCtx()->GetPeerName()));
            return;
        }

        case TClientMessage::kStartDirectReadPartitionSessionRequest: {
            const auto& req = request.start_direct_read_partition_session_request();

            ctx.Send(ctx.SelfID, new TEvPQProxy::TEvStartDirectRead(req.partition_session_id(), req.generation(), req.last_direct_read_id()));
            return (void)ReadFromStreamOrDie(ctx);
        }

        case TClientMessage::kUpdateTokenRequest: {
            if (const auto token = request.update_token_request().token()) { // TODO: refresh token here
                ctx.Send(ctx.SelfID, new TEvPQProxy::TEvAuth(token));
            }
            return (void)ReadFromStreamOrDie(ctx);
        }

        default: {
            return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "unsupported request");
        }
    }
}


bool TDirectReadSessionActor::WriteToStreamOrDie(const TActorContext& ctx, TServerMessage&& response, bool finish) {
    bool res = false;

    if (!finish) {
        res = Request->GetStreamCtx()->Write(std::move(response));
    } else {
        res = Request->GetStreamCtx()->WriteAndFinish(std::move(response), grpc::Status::OK);
    }

    if (!res) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, LOG_PREFIX << " grpc write failed at start");
        Die(ctx);
    }

    return res;
}


void TDirectReadSessionActor::Handle(typename IContext::TEvWriteFinished::TPtr& ev, const TActorContext& ctx) {
    if (!ev->Get()->Success) {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, LOG_PREFIX << " grpc write failed");
        return Die(ctx);
    }
}


void TDirectReadSessionActor::Die(const TActorContext& ctx) {
    if (AuthInitActor) {
        ctx.Send(AuthInitActor, new TEvents::TEvPoisonPill());
    }

    if (DirectSessionsActive) {
        --(*DirectSessionsActive);
    }

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, LOG_PREFIX << " proxy is DEAD");
    ctx.Send(GetPQReadServiceActorID(), new TEvPQProxy::TEvSessionDead(Cookie));
    ctx.Send(NPQ::MakePQDReadCacheServiceActorId(), new TEvPQProxy::TEvDirectReadDataSessionDead(Session));
    TRlHelpers::PassAway(SelfId());
    TActorBootstrapped<TDirectReadSessionActor>::Die(ctx);
}


void TDirectReadSessionActor::Handle(TEvPQProxy::TEvDone::TPtr&, const TActorContext&) {
    CloseSession(PersQueue::ErrorCode::OK, "reads done signal, closing everything");
}


void TDirectReadSessionActor::Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext&) {
    CloseSession(ev->Get()->ErrorCode, ev->Get()->Reason);
}

void TDirectReadSessionActor::Handle(TEvPQProxy::TEvAuth::TPtr& ev, const TActorContext& ctx) {
    const auto& auth = ev->Get()->Auth;
    if (!auth.empty() && auth != Auth) {
        Auth = auth;
        Request->RefreshToken(auth, ctx, ctx.SelfID);
    }
}


void TDirectReadSessionActor::Handle(TEvPQProxy::TEvStartDirectRead::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, LOG_PREFIX << " got StartDirectRead from client"
        << ": sessionId# " << Session
        << ", assignId# " << ev->Get()->AssignId
        << ", lastDirectReadId# " << ev->Get()->LastDirectReadId
        << ", generation# " << ev->Get()->Generation);

    ctx.Send(
        NPQ::MakePQDReadCacheServiceActorId(),
        new TEvPQProxy::TEvDirectReadDataSessionConnected(
            {Session, ev->Get()->AssignId},
            ev->Get()->Generation,
            ev->Get()->LastDirectReadId + 1
        )
    );
}

void TDirectReadSessionActor::Handle(TEvPQProxy::TEvDirectReadDataSessionConnectedResponse::TPtr& ev, const TActorContext& ctx) {
    TServerMessage result;
    result.set_status(Ydb::StatusIds::SUCCESS);
    result.mutable_start_direct_read_partition_session_response()->set_partition_session_id(ev->Get()->AssignId);
    result.mutable_start_direct_read_partition_session_response()->set_generation(ev->Get()->Generation);
    if (!WriteToStreamOrDie(ctx, std::move(result))) {
        return;
    }
}

void TDirectReadSessionActor::Handle(TEvPQProxy::TEvInitDirectRead::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, LOG_PREFIX << "got init request:" << ev->Get()->Request.DebugString());

    if (Initing) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "got second init request");
    }
    Initing = true;

    const auto& init = ev->Get()->Request.init_request();

    if (!init.topics_read_settings_size()) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "no topics in init request");
    }

    if (init.consumer().empty()) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "no consumer in init request");
    }

    ClientId = NPersQueue::ConvertNewConsumerName(init.consumer(), ctx);
    if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        ClientPath = init.consumer();
    } else {
        ClientPath = NPersQueue::StripLeadSlash(NPersQueue::MakeConsumerPath(init.consumer()));
    }

    Session = init.session_id();
    if (Session.empty()) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "no session id in init request");
    }
    PeerName = ev->Get()->PeerName;

    auto database = Request->GetDatabaseName().GetOrElse(TString());

    for (const auto& topic : init.topics_read_settings()) {
        const TString path = topic.path();
        if (path.empty()) {
            return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, "empty topic in init request");
        }

        TopicsToResolve.insert(path);
    }

    if (Request->GetSerializedToken().empty()) {
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            return CloseSession(PersQueue::ErrorCode::ACCESS_DENIED,
                "unauthenticated access is forbidden, please provide credentials");
        }
    } else {
        Y_ABORT_UNLESS(Request->GetYdbToken());
        Auth = *(Request->GetYdbToken());
        Token = new NACLib::TUserToken(Request->GetSerializedToken());
    }

    TopicsList = TopicsHandler.GetReadTopicsList(TopicsToResolve, true, database);

    if (!TopicsList.IsValid) {
        return CloseSession(PersQueue::ErrorCode::BAD_REQUEST, TopicsList.Reason);
    }

    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, LOG_PREFIX << " read init"
        << ": from# " << PeerName
        << ", request# " << ev->Get()->Request);

    if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
        SetupCounters();
    }

    RunAuthActor(ctx);
}


void TDirectReadSessionActor::SetupCounters() {
    if (DirectSessionsCreated) {
        return;
    }

    auto subGroup = GetServiceCounters(Counters, "pqproxy|readSession");
    subGroup = subGroup->GetSubgroup("Client", ClientId)->GetSubgroup("ConsumerPath", ClientPath);
    const TString name = "sensor";

    Errors = subGroup->GetExpiringNamedCounter(name, "Errors", true);
    DirectSessionsActive = subGroup->GetExpiringNamedCounter(name, "DirectSessionsActive", false);
    DirectSessionsCreated = subGroup->GetExpiringNamedCounter(name, "DirectSessionsCreated", true);

    ++(*DirectSessionsCreated);
    ++(*DirectSessionsActive);
}



void TDirectReadSessionActor::Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " auth ok"
        << ": topics# " << ev->Get()->TopicAndTablets.size()
        << ", initDone# " << InitDone);

    LastACLCheckTimestamp = ctx.Now();
    AuthInitActor = TActorId();


    if (!InitDone) {
        for (const auto& [name, t] : ev->Get()->TopicAndTablets) { // TODO: return something from Init and Auth Actor (Full Path - ?)

            if (!GetMeteringMode()) {
                SetMeteringMode(t.MeteringMode);
            } else if (*GetMeteringMode() != t.MeteringMode) {
                return CloseSession(PersQueue::ErrorCode::BAD_REQUEST,
                    "cannot read from topics with different metering modes");
            }
        }

        if (IsQuotaRequired()) {
            Y_ABORT_UNLESS(MaybeRequestQuota(1, EWakeupTag::RlInit, ctx));
        } else {
            InitSession(ctx);
        }

    } else {
        for (const auto& [name, t] : ev->Get()->TopicAndTablets) {
            if (t.MeteringMode != *GetMeteringMode()) {
                return CloseSession(PersQueue::ErrorCode::OVERLOAD, TStringBuilder()
                    << "metering mode of topic: " << name << " has been changed");
            }
        }
    }
}

void TDirectReadSessionActor::InitSession(const TActorContext& ctx) {
    // Successfully authenticated, send InitResponse, wait for StartDirectReadPartitionSession requests.
    TServerMessage result;
    result.set_status(Ydb::StatusIds::SUCCESS);
    result.mutable_init_response();
    if (!WriteToStreamOrDie(ctx, std::move(result))) {
        return;
    }

    InitDone = true;
    ReadFromStreamOrDie(ctx);
    ctx.Schedule(TDuration::Seconds(AppData(ctx)->PQConfig.GetACLRetryTimeoutSec()), new TEvents::TEvWakeup(EWakeupTag::RecheckAcl));
}


void TDirectReadSessionActor::CloseSession(PersQueue::ErrorCode::ErrorCode code, const TString& reason) {
    auto ctx = ActorContext();
    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, LOG_PREFIX << " Close session with reason: " << reason);
    if (code != PersQueue::ErrorCode::OK) {
        if (Errors) {
            ++(*Errors);
        } else if (!AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            ++(*GetServiceCounters(Counters, "pqproxy|readSession")->GetCounter("Errors", true));
        }

        TServerMessage result;
        result.set_status(ConvertPersQueueInternalCodeToStatus(code));
        FillIssue(result.add_issues(), code, reason);

        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " closed with error"
            << ": reason# " << reason);
        if (!WriteToStreamOrDie(ctx, std::move(result), true)) {
            return;
        }
    } else {
        LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " closed");
        if (!Request->GetStreamCtx()->Finish(grpc::Status::OK)) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " grpc double finish failed");
        }
    }
    Die(ctx);
}


void TDirectReadSessionActor::Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr& ev , const TActorContext& ctx) {
    if (ev->Get()->Authenticated && ev->Get()->InternalToken && !ev->Get()->InternalToken->GetSerializedToken().empty()) {
        Token = ev->Get()->InternalToken;
        ForceACLCheck = true;

        TServerMessage result;
        result.set_status(Ydb::StatusIds::SUCCESS);
        result.mutable_update_token_response();
        WriteToStreamOrDie(ctx, std::move(result));
    } else {
        if (ev->Get()->Retryable) {
            TServerMessage serverMessage;
            serverMessage.set_status(Ydb::StatusIds::UNAVAILABLE);
            Request->GetStreamCtx()->WriteAndFinish(std::move(serverMessage), grpc::Status::OK);
        } else {
            Request->RaiseIssues(ev->Get()->Issues);
            Request->ReplyUnauthenticated("refreshed token is invalid");
        }
        Die(ctx);
    }
}


void TDirectReadSessionActor::ProcessAnswer(TFormedDirectReadResponse::TPtr response, const TActorContext& ctx) {
    if (!WriteToStreamOrDie(ctx, std::move(*response->Response))) {
        return;
    }
}

void TDirectReadSessionActor::Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
    const auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
    OnWakeup(tag);

    switch (tag) {
        case EWakeupTag::RlInit:
            return InitSession(ctx);

        case EWakeupTag::RecheckAcl:
            return RecheckACL(ctx);

        case EWakeupTag::RlAllowed:
            if (auto counters = Request->GetCounters()) {
                counters->AddConsumedRequestUnits(PendingQuota->RequiredQuota);
            }

            ProcessAnswer(PendingQuota, ctx);

            if (!WaitingQuota.empty()) {
                PendingQuota = WaitingQuota.front();
                WaitingQuota.pop_front();
            } else {
                PendingQuota = nullptr;
            }
            if (PendingQuota) {
                auto res = MaybeRequestQuota(PendingQuota->RequiredQuota, EWakeupTag::RlAllowed, ctx);
                Y_ABORT_UNLESS(res);
            }

            break;

        case EWakeupTag::RlNoResource:
        case EWakeupTag::RlInitNoResource:
            if (PendingQuota) {
                auto res = MaybeRequestQuota(PendingQuota->RequiredQuota, EWakeupTag::RlAllowed, ctx);
                Y_ABORT_UNLESS(res);
            } else {
                return CloseSession(PersQueue::ErrorCode::OVERLOAD, "throughput limit exceeded");
            }
            break;
    }
}


void TDirectReadSessionActor::RecheckACL(const TActorContext& ctx) {
    const auto timeout = TDuration::Seconds(AppData(ctx)->PQConfig.GetACLRetryTimeoutSec());

    ctx.Schedule(timeout, new TEvents::TEvWakeup(EWakeupTag::RecheckAcl));

    const bool authTimedOut = (ctx.Now() - LastACLCheckTimestamp) > timeout;

    if (Token && !AuthInitActor && (ForceACLCheck || authTimedOut)) {
        ForceACLCheck = false;

        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, PQ_LOG_PREFIX << " checking auth because of timeout");
        RunAuthActor(ctx);
    }
}


void TDirectReadSessionActor::RunAuthActor(const TActorContext& ctx) {
    Y_ABORT_UNLESS(!AuthInitActor);
    AuthInitActor = ctx.Register(new TReadInitAndAuthActor(
        ctx, ctx.SelfID, ClientId, Cookie, Session, SchemeCache, NewSchemeCache, Counters, Token, TopicsList,
        TopicsHandler.GetLocalCluster()));
}

void TDirectReadSessionActor::HandleDestroyPartitionSession(TEvPQProxy::TEvDirectReadDestroyPartitionSession::TPtr& ev) {
    TServerMessage result;
    result.set_status(Ydb::StatusIds::SUCCESS);
    auto* stop = result.mutable_stop_direct_read_partition_session();
    stop->set_partition_session_id(ev->Get()->ReadKey.PartitionSessionId);
    result.set_status(ConvertPersQueueInternalCodeToStatus(ev->Get()->Code));
    FillIssue(stop->add_issues(), ev->Get()->Code, ev->Get()->Reason);
    WriteToStreamOrDie(ActorContext(), std::move(result));

}

void TDirectReadSessionActor::HandleSessionKilled(TEvPQProxy::TEvDirectReadCloseSession::TPtr& ev) {
    // ToDo: Close session uses other error code.
    CloseSession(ev->Get()->Code, ev->Get()->Reason);
}

void TDirectReadSessionActor::HandleGotData(TEvPQProxy::TEvDirectReadSendClientData::TPtr& ev) {
    auto formedResponse = MakeIntrusive<TFormedDirectReadResponse>();
    formedResponse->Response = std::move(ev->Get()->Message);
    ProcessAnswer(formedResponse, ActorContext());
}

}
