#include "kafka_read_session_proxy.h"
#include "kafka_read_session_utils.h"
#include "kafka_balancer_actor.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>


namespace NKafka {

using namespace NKikimr::NSchemeCache;

KafkaReadSessionProxyActor::KafkaReadSessionProxyActor(const TContext::TPtr context, ui64 cookie)
    : Context(context)
    , Cookie(cookie)
{
}

void KafkaReadSessionProxyActor::Bootstrap() {
    Become(&KafkaReadSessionProxyActor::StateWork);
}

template<typename TRequest>
void KafkaReadSessionProxyActor::HandleOnWork(TRequest& ev) {
    switch (Context->ReadSession.BalancingMode) {
        case EBalancingMode::Native:
            Register(new TKafkaBalancerActor(Context, 0, ev->Get()->CorrelationId, ev->Get()->Request));
            break;

        case EBalancingMode::Server:
            Forward(ev, ReadSessionActorId);
            break;
    }
}

template<>
void KafkaReadSessionProxyActor::HandleOnWork<TEvKafka::TEvJoinGroupRequest::TPtr>(TEvKafka::TEvJoinGroupRequest::TPtr& ev) {
    Context->ReadSession.BalancingMode = Context->ReadSession.PendingBalancingMode.value_or(GetBalancingMode(*ev->Get()->Request));
    HandleOnWork(ev);
}

namespace {

std::vector<TString> GetTopics(const TFetchRequestData& request, const TContext::TPtr& context) {
    std::vector<TString> result;
    for (const auto& topic: request.Topics) {
        result.push_back(NormalizePath(context->DatabasePath, topic.Topic.value()));
    }
    return result;
}

}

template<>
void KafkaReadSessionProxyActor::HandleOnWork<TEvKafka::TEvFetchRequest::TPtr>(TEvKafka::TEvFetchRequest::TPtr& ev) {
    if (Context->ReadSession.BalancingMode == EBalancingMode::Server) {
        Register(CreateKafkaFetchActor(Context, ev->Get()->CorrelationId, ev->Get()->Request));
        return;
    }
    std::vector<TString> newTopics;
    for (auto& topic : GetTopics(*ev->Get()->Request, Context)) {
        if (Topics.contains(topic)) {
            continue;
        }

        newTopics.push_back(topic);
    }

    if (newTopics.empty()) {
        Register(CreateKafkaFetchActor(Context, ev->Get()->CorrelationId, ev->Get()->Request));
        return;
    }

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

    for (const auto& topic : newTopics) {
        addEntry(topic);
        addEntry(TStringBuilder() << topic << "/streamImpl");
    }

    Send(NKikimr::MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeRequest.release()));
}

void KafkaReadSessionProxyActor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
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
        Topics[topic] = {
            .ReadBalancerTabletId = readBalancerTabletId,
            .PipeClient = CreatePipe(readBalancerTabletId)
        };
    }
}

STFUNC(KafkaReadSessionProxyActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvKafka::TEvJoinGroupRequest, HandleOnWork);
        hFunc(TEvKafka::TEvSyncGroupRequest, HandleOnWork);
        hFunc(TEvKafka::TEvHeartbeatRequest, HandleOnWork);
        hFunc(TEvKafka::TEvLeaveGroupRequest, HandleOnWork);
        hFunc(TEvKafka::TEvFetchRequest, HandleOnWork);
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
    }
}

void KafkaReadSessionProxyActor::PassAway() {
    if (ReadSessionActorId) {
        Send(ReadSessionActorId, new NActors::TEvents::TEvPoison());
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