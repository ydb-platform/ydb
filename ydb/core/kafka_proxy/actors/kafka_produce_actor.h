#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include "actors.h"

namespace NKafka {

using namespace NKikimr;
using namespace NKikimr::NPQ;
using namespace NKikimrClient;

//
// This actor handles write requests.
// Each request can contain data for writing to several topics, and in each topic to several partitions.
// When a request to write to an unknown topic arrives, the actor changes the state to Init until it receives
// information about all the topics needed to process the request.
//
// Requests are processed in parallel, but it is guaranteed that the recording order will be preserved.
// The order of responses to requests is also guaranteed.
//
// When the request begins to be processed, the actor enters the Accepting state. In this state, responses
// are expected from all TPartitionWriters confirming acceptance of the request (TEvWriteAccepted). After that,
// the actor switches back to the Work state. This guarantees the order of writing to each partition.
//
class TKafkaProduceActor: public NActors::TActorBootstrapped<TKafkaProduceActor> {
    struct TPendingRequest;

    enum ETopicStatus {
        OK,
        NOT_FOUND,
        UNAUTHORIZED
    };

public:
    TKafkaProduceActor(const TContext::TPtr context)
        : Context(context) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::KAFKA_PRODUCE_ACTOR; }

private:
    void PassAway() override;

    // Handlers for many StateFunc
    void Handle(TEvKafka::TEvWakeup::TPtr request, const TActorContext& ctx);
    void Handle(TEvPartitionWriter::TEvWriteResponse::TPtr request, const TActorContext& ctx);
    void Handle(TEvPartitionWriter::TEvInitResult::TPtr request, const TActorContext& ctx);
    void EnqueueRequest(TEvKafka::TEvProduceRequest::TPtr request, const TActorContext& ctx);
    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx);

    // StateInit - describe topics
    void HandleInit(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);

    STATEFN(StateInit) {
        LogEvent(*ev.Get());
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleInit);

            HFunc(TEvKafka::TEvProduceRequest, EnqueueRequest);
            HFunc(TEvPartitionWriter::TEvInitResult, Handle);
            HFunc(TEvPartitionWriter::TEvWriteResponse, Handle);

            HFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
            HFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);

            HFunc(TEvKafka::TEvWakeup, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    // StateWork - processing messages
    void Handle(TEvKafka::TEvProduceRequest::TPtr request, const TActorContext& ctx);

    STATEFN(StateWork) {
        LogEvent(*ev.Get());
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvKafka::TEvProduceRequest, Handle);
            HFunc(TEvPartitionWriter::TEvInitResult, Handle);
            HFunc(TEvPartitionWriter::TEvWriteResponse, Handle);

            HFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
            HFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);

            HFunc(TEvKafka::TEvWakeup, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    // StateAccepting - enqueue ProduceRequest parts to PartitionWriters
    // This guarantees the order of responses according order of request
    void HandleAccepting(TEvPartitionWriter::TEvWriteAccepted::TPtr request, const TActorContext& ctx);

    STATEFN(StateAccepting) {
        LogEvent(*ev.Get());
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPartitionWriter::TEvWriteAccepted, HandleAccepting);

            HFunc(TEvKafka::TEvProduceRequest, EnqueueRequest);
            HFunc(TEvPartitionWriter::TEvInitResult, Handle);
            HFunc(TEvPartitionWriter::TEvWriteResponse, Handle);

            HFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
            HFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);

            HFunc(TEvKafka::TEvWakeup, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }


    // Logic
    void ProcessRequests(const TActorContext& ctx);
    void ProcessRequest(std::shared_ptr<TPendingRequest> pendingRequest, const TActorContext& ctx);

    void SendResults(const TActorContext& ctx);

    size_t EnqueueInitialization();
    void ProcessInitializationRequests(const TActorContext& ctx);
    void CleanTopics(const TActorContext& ctx);
    void CleanWriters(const TActorContext& ctx);

    std::pair<ETopicStatus, TActorId> PartitionWriter(const TString& topicPath, ui32 partitionId, const TActorContext& ctx);

    TString LogPrefix();
    void LogEvent(IEventHandle& ev);
    void SendMetrics(const TString& topicName, size_t delta, const TString& name, const TActorContext& ctx);

private:
    const TContext::TPtr Context;
    TString SourceId;

    TString ClientDC;

    ui64 Cookie = 0;
    TDeque<TEvKafka::TEvProduceRequest::TPtr> Requests;

    struct TPendingRequest {
        using TPtr = std::shared_ptr<TPendingRequest>;

        TPendingRequest(TEvKafka::TEvProduceRequest::TPtr request)
            : Request(request) {
        }

        TEvKafka::TEvProduceRequest::TPtr Request;

        struct TPartitionResult {
            EKafkaErrors ErrorCode = EKafkaErrors::REQUEST_TIMED_OUT;
            TString ErrorMessage;
            TEvPartitionWriter::TEvWriteResponse::TPtr Value;
        };
        std::vector<TPartitionResult> Results;

        std::set<ui64> WaitAcceptingCookies;
        std::set<ui64> WaitResultCookies;

        TInstant StartTime;
    };
    TDeque<TPendingRequest::TPtr> PendingRequests;

    struct TCookieInfo {
        TString TopicPath;
        ui32 PartitionId;
        size_t Position;

        TPendingRequest::TPtr Request;
    };
    std::map<ui64, TCookieInfo> Cookies;

    std::set<TString> TopicsForInitialization;

    struct TTopicInfo {
        ETopicStatus Status = OK;
        TInstant ExpirationTime;

        NKikimrPQ::TPQTabletConfig::EMeteringMode MeteringMode;
        std::shared_ptr<IPartitionChooser> PartitionChooser;
    };
    std::map<TString, TTopicInfo> Topics;

    struct TWriterInfo {
        TActorId ActorId;
        TInstant LastAccessed;
    };
    // TopicPath -> PartitionId -> TPartitionWriter
    std::unordered_map<TString, std::unordered_map<ui32, TWriterInfo>> Writers;
};

}
