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

template<bool handlePending, typename TRequest>
void KafkaReadSessionProxyActor::DoHandle(TRequest& ev, const TString& event) {
    if constexpr (handlePending) {
        if (Context->ReadSession.PendingBalancingMode.has_value()) {
            KAFKA_LOG_D("DoHandle " << event << " with pending balance mode");
            auto response = CreateChangeResponse(*ev->Get()->Request);
            Send(Context->ConnectionId, new TEvKafka::TEvResponse(ev->Get()->CorrelationId, response, EKafkaErrors::REBALANCE_IN_PROGRESS));
            return;
        }
    }

    KAFKA_LOG_D("DoHandle " << event);
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
    KAFKA_LOG_D("Handle TEvKafka::TEvJoinGroupRequest");
    Context->ReadSession.BalancingMode = Context->ReadSession.PendingBalancingMode.value_or(GetBalancingMode(*ev->Get()->Request));
    Context->ReadSession.PendingBalancingMode.reset();
    KAFKA_LOG_D("Balancing mode: " << Context->ReadSession.BalancingMode);

    DoHandle<false>(ev, "TEvKafka::TEvJoinGroupRequest");
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvSyncGroupRequest::TPtr& ev) {
    DoHandle<true>(ev, "TEvKafka::TEvSyncGroupRequest");
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvHeartbeatRequest::TPtr& ev) {
    DoHandle<true>(ev, "TEvKafka::TEvHeartbeatRequest");
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvLeaveGroupRequest::TPtr& ev) {
    DoHandle<false>(ev, "TEvKafka::TEvLeaveGroupRequest");
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvFetchRequest::TPtr& ev) {
    KAFKA_LOG_D("Handle TEvKafka::TEvFetchRequest");

    if (Context->ReadSession.BalancingMode == EBalancingMode::Server) {
        Register(CreateKafkaFetchActor(Context, ev->Get()->CorrelationId, ev->Get()->Request));
        return;
    }

    Y_VERIFY(!PendingRequest.has_value());
    PendingRequest = ev;

    for (auto& topic : GetTopics(*PendingRequest.value()->Get()->Request, Context)) {
        if (Topics.contains(topic)) {
            continue;
        }

        NewTopics.push_back(topic);
    }

    if (NewTopics.empty()) {
        return ProcessPendingRequestIfPossible();
    }

    KAFKA_LOG_D("Describe topics: " << JoinRange(", ", NewTopics.begin(), NewTopics.end()));

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
    KAFKA_LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult");
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
        ui64 cookie = 1;
        Topics[topic] = {
            .ReadBalancerTabletId = readBalancerTabletId,
            .SubscribeCookie = cookie
        };

        Subscribe(topic, readBalancerTabletId, cookie);
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
        KAFKA_LOG_D("Handle TEvPersQueue::TEvBalancingSubscribeNotify generation mismatch: "
            << topicInfo.ReadBalancerGeneration << " vs " << record.GetGeneration());
        return;
    }
    if (topicInfo.ReadBalancerGeneration == record.GetGeneration() && topicInfo.ReadBalancerNotifyCookie >= record.GetCookie()) {
        KAFKA_LOG_D("Handle TEvPersQueue::TEvBalancingSubscribeNotify cookie mismatch: "
            << topicInfo.ReadBalancerNotifyCookie << " vs " << record.GetCookie());
        return;
    }

    topicInfo.UsedServerBalancing = record.GetStatus() == NKikimrPQ::TEvBalancingSubscribeNotify::BALANCING;
    topicInfo.ReadBalancerGeneration = record.GetGeneration();
    topicInfo.ReadBalancerNotifyCookie = record.GetCookie();

    if (*topicInfo.UsedServerBalancing && Context->ReadSession.BalancingMode == EBalancingMode::Native) {
        KAFKA_LOG_D("Change balancing mode to server");
        Context->ReadSession.PendingBalancingMode = EBalancingMode::Server;
    }

    ProcessPendingRequestIfPossible();
}

void KafkaReadSessionProxyActor::ProcessPendingRequestIfPossible() {
    if (!PendingRequest.has_value()) {
        KAFKA_LOG_D("Pending request is not set");
        Y_VERIFY_DEBUG(PendingRequest.has_value());
        return;
    }

    auto fetchEv = PendingRequest.value();
    PendingRequest.reset();

    if (Context->ReadSession.PendingBalancingMode) {
        KAFKA_LOG_D("Handle TEvKafka::TEvFetchRequest with pending balancing mode");
        auto response = CreateChangeResponse(*fetchEv->Get()->Request);
        Send(Context->ConnectionId, new TEvKafka::TEvResponse(fetchEv->Get()->CorrelationId, response, EKafkaErrors::REBALANCE_IN_PROGRESS));
        return;
    }

    KAFKA_LOG_T("Creating the fetch actor");
    Register(CreateKafkaFetchActor(Context, fetchEv->Get()->CorrelationId, fetchEv->Get()->Request));

    PendingRequest.reset();
}

TSyncGroupResponseData::TPtr KafkaReadSessionProxyActor::CreateChangeResponse(TSyncGroupRequestData&) {
    TSyncGroupResponseData::TPtr response = std::make_shared<TSyncGroupResponseData>();
    response->ErrorCode = EKafkaErrors::ILLEGAL_GENERATION;
    response->Assignment = "";

    return response;
}

THeartbeatResponseData::TPtr KafkaReadSessionProxyActor::CreateChangeResponse(THeartbeatRequestData&) {
    THeartbeatResponseData::TPtr response = std::make_shared<THeartbeatResponseData>();
    response->ErrorCode = EKafkaErrors::REBALANCE_IN_PROGRESS;

    return response;
}

TFetchResponseData::TPtr KafkaReadSessionProxyActor::CreateChangeResponse(TFetchRequestData& request) {
    TFetchResponseData::TPtr response = std::make_shared<TFetchResponseData>();
    // Possible error code: OFFSET_OUT_OF_RANGE, TOPIC_AUTHORIZATION_FAILED, REPLICA_NOT_AVAILABLE, NOT_LEADER_OR_FOLLOWER,
    // FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH, UNKNOWN_TOPIC_OR_PARTITION, KAFKA_STORAGE_ERROR, UNSUPPORTED_COMPRESSION_TYPE,
    // CORRUPT_MESSAGE, UNKNOWN_TOPIC_ID, FETCH_SESSION_TOPIC_ID_ERROR, INCONSISTENT_TOPIC_ID, UNKNOWN_SERVER_ERROR.
    response->ErrorCode = EKafkaErrors::NONE_ERROR;
    response->Responses.resize(request.Topics.size());
    for (size_t i = 0; i < request.Topics.size(); ++i) {
        auto& sTopic = request.Topics[i];
        auto& rTopic = response->Responses[i];

        rTopic.Topic = std::move(sTopic.Topic.value());
        rTopic.Partitions.resize(sTopic.Partitions.size());
        for (size_t j = 0; j < sTopic.Partitions.size(); ++j) {
            auto& sPartition = sTopic.Partitions[j];
            auto& rPartition = rTopic.Partitions[j];

            rPartition.PartitionIndex = sPartition.Partition;
            rPartition.ErrorCode = EKafkaErrors::REBALANCE_IN_PROGRESS;
        }
    }

    return response;
}

void KafkaReadSessionProxyActor::Subscribe(const TString& topic, ui64 tabletId, const ui64 cookie) {
    auto ev = std::make_unique<TEvPersQueue::TEvBalancingSubscribe>(SelfId(), topic, Context->GroupId);
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev.release(), tabletId, true, cookie);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
}

void KafkaReadSessionProxyActor::Unsubscribe(const TString& topic, ui64 tabletId) {
    auto ev = std::make_unique<TEvPersQueue::TEvBalancingUnsubscribe>(SelfId(), topic, Context->GroupId);
    auto forward = std::make_unique<TEvPipeCache::TEvForward>(ev.release(), tabletId);
    Send(MakePipePerNodeCacheID(false), forward.release(), IEventHandle::FlagTrackDelivery);
}

void KafkaReadSessionProxyActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    auto tabletId = ev->Get()->TabletId;
    auto cookie = ev->Cookie;
    KAFKA_LOG_I("Reconnecting the pipe to the tabletId " << tabletId);

    auto topicInfo = FindIf(Topics, [&](const auto& entry) {
        return entry.second.ReadBalancerTabletId == tabletId;
    });
    if (topicInfo == Topics.end()) {
        Y_VERIFY_DEBUG(topicInfo != Topics.end());
        return;
    }

    if (topicInfo->second.SubscribeCookie != cookie) {
        return;
    }

    const auto& topic = topicInfo->first;
    Subscribe(topic, tabletId, ++topicInfo->second.SubscribeCookie);
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
        hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
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
    for (auto& [topicName, topicInfo] : Topics) {
        Unsubscribe(topicName, topicInfo.ReadBalancerTabletId);
    }
    Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));

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
