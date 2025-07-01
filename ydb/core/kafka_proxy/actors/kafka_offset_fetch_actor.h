#include "actors.h"
#include <ydb/core/kafka_proxy/kafka_events.h>
#include "../kqp_helper.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKafka {

const TString FETCH_ASSIGNMENTS = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroup AS Utf8;
    DECLARE $Database AS Utf8;

    SELECT assignment
    FROM `%s`
    VIEW PRIMARY KEY
    WHERE database = $Database
      AND consumer_group = $ConsumerGroup
)sql";

struct TopicEntities {
    std::shared_ptr<TSet<TString>> Consumers = std::make_shared<TSet<TString>>();
    std::shared_ptr<TSet<ui32>> Partitions = std::make_shared<TSet<ui32>>();
};

class TKafkaOffsetFetchActor: public NActors::TActorBootstrapped<TKafkaOffsetFetchActor> {

    using TBase = NActors::TActor<TKafkaOffsetFetchActor>;

public:
    TKafkaOffsetFetchActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TOffsetFetchRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , Message(message)
        , DatabasePath(context->DatabasePath) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvKafka::TEvCommitedOffsetsResponse, Handle);
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
        }
    }

    void Handle(TEvKafka::TEvCommitedOffsetsResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void ParseAssignments(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, std::vector<TConsumerProtocolAssignment>& assignments);
    void ExtractPartitions(const TString& group, const NKafka::TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics& topic);
    TOffsetFetchResponseData::TPtr GetOffsetFetchResponse();
    void ReplyError(const TActorContext& ctx);
    NYdb::TParamsBuilder BuildFetchAssignmentsParams(TString groupId);
    void Die(const TActorContext &ctx);

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TOffsetFetchRequestData> Message;
    std::unordered_map<TString, TopicEntities> TopicToEntities;
    std::unordered_map<TString, TAutoPtr<TEvKafka::TEvCommitedOffsetsResponse>> TopicsToResponses;
    std::unordered_map<TString, ui32> GroupIdToIndex;
    std::unordered_map<ui32, TString> CookieToGroupId;
    std::unique_ptr<NKafka::TKqpTxHelper> Kqp;

    ui32 InflyTopics = 0;
    ui32 WaitingGroupTopicsInfo = 0;
    ui32 KqpCookie = 0;
    std::queue<TKafkaString> GroupsToFetch;
    const TString DatabasePath;
    bool KqpSessionCreated = false;
};

} // NKafka
