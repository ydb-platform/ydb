#include "kafka_read_session_proxy.h"
#include "kafka_read_session_utils.h"
#include "kafka_balancer_actor.h"

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>


namespace NKafka {

using namespace NKikimr::NSchemeCache;

namespace {

std::vector<TString> GetTopics(const TFetchRequestData& request, const TContext::TPtr& context) {
    std::vector<TString> result;
    for (const auto& topic: request.Topics) {
        result.push_back(NormalizePath(context->DatabasePath, topic.Topic.value()));
    }
    return result;
}

}


KafkaReadSessionProxyActor::KafkaReadSessionProxyActor(const TContext::TPtr context, ui64 cookie)
    : Context(context)
    , Cookie(cookie)
{
}

void KafkaReadSessionProxyActor::Bootstrap() {
    Become(&KafkaReadSessionProxyActor::StateWork);
    Y_UNUSED(Cookie);
}

template<typename TRequest>
void KafkaReadSessionProxyActor::DoHandle(TRequest& ev) {
    KAFKA_LOG_D("DoHandle");
    switch (Context->ReadSession.BalancingMode) {
        case EBalancingMode::Native:
            Register(new TKafkaBalancerActor(Context, 0, ev->Get()->CorrelationId, ev->Get()->Request));
            break;

        case EBalancingMode::Server:
            EnsureReadSessionActor();
            Forward(ev, ReadSessionActorId);
            break;
    }
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvJoinGroupRequest::TPtr& ev) {
    KAFKA_LOG_D("HandleOnWork<TEvKafka::TEvJoinGroupRequest>");
    Context->ReadSession.BalancingMode = Context->ReadSession.PendingBalancingMode.value_or(GetBalancingMode(*ev->Get()->Request));
    Context->ReadSession.PendingBalancingMode.reset();
    KAFKA_LOG_D("Balancing mode: " << Context->ReadSession.BalancingMode);

    DoHandle(ev);
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvSyncGroupRequest::TPtr& ev) {
    if (Context->ReadSession.PendingBalancingMode.has_value()) {
        KAFKA_LOG_D("Handle TEvKafka::TEvSyncGroupRequest with pending balancing mode");

        TSyncGroupResponseData::TPtr response = std::make_shared<TSyncGroupResponseData>();
        response->ErrorCode = EKafkaErrors::ILLEGAL_GENERATION;
        response->Assignment = "";

        Send(Context->ConnectionId, new TEvKafka::TEvResponse(ev->Get()->CorrelationId, response, EKafkaErrors::ILLEGAL_GENERATION));
        return;
    }

    DoHandle(ev);
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvHeartbeatRequest::TPtr& ev) {
    if (Context->ReadSession.PendingBalancingMode.has_value()) {
        KAFKA_LOG_D("Handle TEvKafka::TEvHeartbeatRequest with pending balancing mode");

        THeartbeatResponseData::TPtr response = std::make_shared<THeartbeatResponseData>();
        response->ErrorCode = EKafkaErrors::REBALANCE_IN_PROGRESS;

        Send(Context->ConnectionId, new TEvKafka::TEvResponse(ev->Get()->CorrelationId, response, EKafkaErrors::REBALANCE_IN_PROGRESS));
        return;
    }

    DoHandle(ev);
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvLeaveGroupRequest::TPtr& ev) {
    KAFKA_LOG_D("Handle TEvKafka::TEvLeaveGroupRequest");

    DoHandle(ev);
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvFetchRequest::TPtr& ev) {
    KAFKA_LOG_D("Handle TEvKafka::TEvFetchRequest");

    if (Context->ReadSession.BalancingMode == EBalancingMode::Server) {
        Register(CreateKafkaFetchActor(Context, ev->Get()->CorrelationId, ev->Get()->Request));
        return;
    }

    for (auto& topic : GetTopics(*ev->Get()->Request, Context)) {
        if (Topics.contains(topic)) {
            continue;
        }

        NewTopics.push_back(topic);
    }

    if (NewTopics.empty()) {
        Register(CreateKafkaFetchActor(Context, ev->Get()->CorrelationId, ev->Get()->Request));
        return;
    }

    KAFKA_LOG_D("Describe topics: " << JoinRange(", ", NewTopics.begin(), NewTopics.end()));

    Y_VERIFY(!PendingRequest.has_value());
    PendingRequest = ev;

    auto schemeRequest = std::make_unique<TSchemeCacheNavigate>(1);
    schemeRequest->DatabaseName = Context->DatabasePath;

    auto addEntry = [&](const TString& topic) {
        auto split = NKikimr::SplitPath(topic);

        schemeRequest->ResultSet.emplace_back();
        auto& entry = schemeRequest->ResultSet.back();
        entry.Path.insert(entry.Path.end(), split.begin(), split.end());
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        entry.SyncVersion = true;
        entry.ShowPrivatePath = true;
    };

    for (const auto& topic : NewTopics) {
        addEntry(topic);
        addEntry(TStringBuilder() << topic << "/streamImpl");
    }

    Send(NKikimr::MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeRequest.release()));
}

void KafkaReadSessionProxyActor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    KAFKA_LOG_D("Handle<TEvTxProxySchemeCache::TEvNavigateKeySetResult>");
    auto& result = ev->Get()->Request;

    for (size_t i = 0; i < result->ResultSet.size(); ++i) {
        const auto& entry = result->ResultSet[i];
        const auto& topic = NewTopics[i / 2];

        auto path = CanonizePath(NKikimr::JoinPath(entry.Path));
        switch (entry.Status) {
            case TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            case TSchemeCacheNavigate::EStatus::RootUnknown:
                if (i % 2 != 1) {
                    // TODO ERROR
                    continue;
                }
                continue;
            case TSchemeCacheNavigate::EStatus::Ok:
                break;
            default:
                // TODO ERROR
                continue;
        }
        if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
            if (i % 2 != 0) {
                // TODO ERROR
            }
            continue;
        }
        if (entry.Kind != TSchemeCacheNavigate::EKind::KindTopic) {
            // TODO ERROR
        }
        if (!entry.PQGroupInfo) {
            // TODO ERROR
        }
        if (!entry.PQGroupInfo->Description.HasBalancerTabletID() || entry.PQGroupInfo->Description.GetBalancerTabletID() == 0) {
            // TODO ERROR
        }

        ui64 readBalancerTabletId = entry.PQGroupInfo->Description.GetBalancerTabletID();
        Context->PipeCache->Prepare(NActors::TlsActivationContext->AsActorContext(), readBalancerTabletId);
        Topics[topic] = {
            .ReadBalancerTabletId = readBalancerTabletId
        };

        Context->PipeCache->Send(NActors::TlsActivationContext->AsActorContext(), readBalancerTabletId,
            new TEvPersQueue::TEvBalancingSubscribe(SelfId(), topic, Context->GroupId));
    }

    NewTopics.clear();
}

void KafkaReadSessionProxyActor::Handle(TEvPersQueue::TEvBalancingSubscribeNotify::TPtr& ev) {
    auto& record = ev->Get()->Record;
    KAFKA_LOG_D("Handle TEvPersQueue::TEvBalancingSubscribeNotify " << record.ShortDebugString());

    auto it = Topics.find(record.GetTopic());
    if (it == Topics.end()) {
        Y_VERIFY_DEBUG(it == Topics.end());
        return;
    }

    auto& topicInfo = it->second;
    if (topicInfo.ReadBalancerGeneration > record.GetGeneration()) {
        return;
    }
    if (topicInfo.ReadBalancerGeneration == record.GetGeneration() && topicInfo.ReadBalancerNotifyCookie >= record.GetCookie()) {
        return;
    }

    topicInfo.UsedServerBalancing = record.GetStatus() == NKikimrPQ::TEvBalancingSubscribeNotify::BALANCING;
    topicInfo.ReadBalancerGeneration = record.GetGeneration();
    topicInfo.ReadBalancerNotifyCookie = record.GetCookie();

    if (*topicInfo.UsedServerBalancing && Context->ReadSession.BalancingMode == EBalancingMode::Native) {
        Context->ReadSession.PendingBalancingMode = EBalancingMode::Server;
    }

    ProcessPendingRequestIfPossible();
}

void KafkaReadSessionProxyActor::ProcessPendingRequestIfPossible() {
    if (!PendingRequest.has_value()) {
        return;
    }

    auto fetchEv = PendingRequest.value();
    for (auto& topic : fetchEv->Get()->Request->Topics) {
        auto it = Topics.find(*topic.Topic);
        if (it == Topics.end()) {
            Y_VERIFY_DEBUG(it == Topics.end());
            return;
        }

        auto& topicInfo = it->second;
        if (!topicInfo.UsedServerBalancing.has_value()) {
            KAFKA_LOG_W("Topic " << *topic.Topic << " is not initialized");
            //return; TODO
        }
    }

    Register(CreateKafkaFetchActor(Context, fetchEv->Get()->CorrelationId, fetchEv->Get()->Request));

    PendingRequest.reset();
}

void KafkaReadSessionProxyActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
    if (Context->PipeCache->OnConnect(ev)) {
        return;
    }

    Reconnect(ev->Get()->TabletId);
}

void KafkaReadSessionProxyActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
    Context->PipeCache->OnDisconnect(ev);
    Reconnect(ev->Get()->TabletId);
}

void KafkaReadSessionProxyActor::Reconnect(ui64 tabletId) {
    KAFKA_LOG_I("Reconnecting the pipe to the tabletId " << tabletId);

    auto topicInfo = FindIf(Topics, [&](const auto& entry) {
        return entry.second.ReadBalancerTabletId == tabletId;
    });
    if (topicInfo == Topics.end()) {
        Y_VERIFY_DEBUG(topicInfo != Topics.end());
        return;
    }

    const auto& topic = topicInfo->first;

    Context->PipeCache->Prepare(NActors::TlsActivationContext->AsActorContext(), tabletId);
    Context->PipeCache->Send(NActors::TlsActivationContext->AsActorContext(), tabletId,
        new TEvPersQueue::TEvBalancingSubscribe(SelfId(), topic, Context->GroupId));
}

STFUNC(KafkaReadSessionProxyActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvKafka::TEvJoinGroupRequest, Handle);
        hFunc(TEvKafka::TEvSyncGroupRequest, Handle);
        hFunc(TEvKafka::TEvHeartbeatRequest, Handle);
        hFunc(TEvKafka::TEvLeaveGroupRequest, Handle);
        hFunc(TEvKafka::TEvFetchRequest, Handle);
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        hFunc(TEvPersQueue::TEvBalancingSubscribeNotify, Handle);
        hFunc(TEvTabletPipe::TEvClientConnected, Handle);
        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
    }
}

void KafkaReadSessionProxyActor::EnsureReadSessionActor() {
    if (!ReadSessionActorId) {
        ReadSessionActorId = Register(CreateKafkaReadSessionActor(Context, Cookie));
    }
}

void KafkaReadSessionProxyActor::PassAway() {
    if (ReadSessionActorId) {
        Send(ReadSessionActorId, new NActors::TEvents::TEvPoison());
    }
    for (auto& [_, topicInfo] : Topics) {
        Context->PipeCache->Close(TlsActivationContext->AsActorContext(), topicInfo.ReadBalancerTabletId);        
    }
    TBase::PassAway();
}

TActorId KafkaReadSessionProxyActor::CreatePipe(ui64 tabletId) {
    auto retryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
    retryPolicy.RetryLimitCount = 5;
    NTabletPipe::TClientConfig clientConfig(retryPolicy);

    return RegisterWithSameMailbox(
            NTabletPipe::CreateClient(TlsActivationContext->AsActorContext().SelfID, tabletId, clientConfig)
        );
}

IActor* CreateKafkaReadSessionProxyActor(const TContext::TPtr context, ui64 cookie) {
    return new KafkaReadSessionProxyActor(context, cookie);
}

}