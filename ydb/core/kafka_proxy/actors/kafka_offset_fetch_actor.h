#include "actors.h"
#include <ydb/core/kafka_proxy/kafka_events.h>
#include "../kqp_helper.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKafka {

const TString FETCH_ASSIGNMENTS = R"sql(
    --!syntax_v1
    DECLARE $ConsumerGroups AS List<Utf8>;
    DECLARE $Database AS Utf8;

    SELECT assignment, consumer_group
    FROM `%s`
    VIEW PRIMARY KEY
    WHERE database = $Database
      AND consumer_group IN $ConsumerGroups
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
    void ExtractPartitions(const TString& group, const NKafka::TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics& topic);
    void ParseGroupsAssignments(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, std::vector<std::pair<std::optional<TString>, TConsumerProtocolAssignment>>& assignments);
    TOffsetFetchResponseData::TPtr GetOffsetFetchResponse();
    NYdb::TParamsBuilder BuildFetchAssignmentsParams(const std::vector<std::optional<TString>>& groupIds);
    void ReplyError(const TActorContext& ctx);
    void Die(const TActorContext &ctx);

    TStringBuilder LogPrefix() const {
        return TStringBuilder() << "TKafkaOffsetFetchActor{GroupId=" << Message->GroupId.value() << ",DatabasePath=" << DatabasePath << "}: ";
    }

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
    std::vector<std::optional<TString>> GroupsToFetch;
    const TString DatabasePath;
    bool KqpSessionCreated = false;
};

} // NKafka
