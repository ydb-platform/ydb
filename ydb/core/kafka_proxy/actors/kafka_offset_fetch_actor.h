#include "actors.h"
#include <ydb/core/kafka_proxy/kafka_events.h>
#include "../kqp_helper.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/tx/replication/ydb_proxy/local_proxy/local_proxy.h>
#include <ydb/core/tx/replication/ydb_proxy/local_proxy/local_proxy_request.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/services/persqueue_v1/grpc_pq_schema.h>

#include <ydb/core/grpc_services/service_scheme.h>
#include <ydb/core/grpc_services/service_topic.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

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

struct ModifiedTopicInfo {
    TString TopicName;
    TopicEntities Entities;
};

struct TopicGroupRequest {
    TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics TopicRequest;
    TString GroupId;
};

struct TopicGroupIdAndPath {
    TString GroupId;
    TString TopicPath;

    bool operator==(const TopicGroupIdAndPath& topicGroupIdAndPath) const {
        return GroupId == topicGroupIdAndPath.GroupId && TopicPath == topicGroupIdAndPath.TopicPath;
    }
};

struct TStructHash { size_t operator()(const TopicGroupIdAndPath& alterTopicRequest) const { return CombineHashes(std::hash<TString>()(alterTopicRequest.GroupId), std::hash<TString>()(alterTopicRequest.TopicPath)); } };


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
            HFunc(NKikimr::NReplication::TEvYdbProxy::TEvAlterTopicResponse, Handle);
            HFunc(TEvKafka::TEvResponse, Handle);
        }
    }

    void Handle(TEvKafka::TEvCommitedOffsetsResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void Handle(NKikimr::NReplication::TEvYdbProxy::TEvAlterTopicResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(const TEvKafka::TEvResponse::TPtr& ev, const TActorContext& ctx);
    void ExtractPartitions(const TString& group, const NKafka::TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics& topic);
    void ParseGroupsAssignments(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, std::vector<std::pair<std::optional<TString>, TConsumerProtocolAssignment>>& assignments);
    void CreateConsumerGroupIfNecessary(const TString& topicName,
                                    const TString& topicPath,
                                    const TString& originalTopicName,
                                    const TString& groupId,
                                    const std::vector<TOffsetFetchResponsePartitions>& topicPartitions);
    bool CreateTopicIfNecessary(const TString& topicName,
                                const TString& originalTopicName,
                                const TString& groupId,
                                const TActorContext& ctx);
    TOffsetFetchResponseData::TPtr GetOffsetFetchResponse();
    TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics GetOffsetResponseForTopic(
                                    TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics const &requestTopic,
                                    TString groupId);
    NYdb::TParamsBuilder BuildFetchAssignmentsParams(const std::vector<std::optional<TString>>& groupIds);
    void FillMapWithGroupRequests();
    void ReplyError(const TActorContext& ctx);
    void Die(const TActorContext &ctx);

    TStringBuilder LogPrefix() const {
        return TStringBuilder() << "TKafkaOffsetFetchActor{GroupId=" << Message->GroupId.value() << ",DatabasePath=" << DatabasePath << "}: ";
    }

private:
    const TContext::TPtr Context;
    TContext::TPtr ContextForTopicCreation;
    const ui64 CorrelationId;
    const TMessagePtr<TOffsetFetchRequestData> Message;
    std::unordered_map<TString, TopicEntities> TopicToEntities;
    std::unordered_map<TString, TAutoPtr<TEvKafka::TEvCommitedOffsetsResponse>> TopicsToResponses;
    std::unordered_map<TString, ui32> GroupIdToIndex;
    std::unordered_map<ui32, TString> CookieToGroupId;
    std::unordered_map<ui32, ModifiedTopicInfo> AlterTopicInfo;
    std::unordered_map<TString, TopicGroupRequest> GroupRequests;
    std::unordered_map<TActorId, ModifiedTopicInfo> CreateTopicInfo;
    std::unordered_set<TopicGroupIdAndPath, TStructHash> ConsumerTopicAlterRequestAttempts;
    std::unordered_set<TString> TopicCreateRequestAttempts;
    std::unordered_set<TActorId> DependantActors;
    std::unique_ptr<NKafka::TKqpTxHelper> Kqp;

    ui32 InflyTopics = 0;
    ui32 WaitingGroupTopicsInfo = 0;
    ui32 KqpCookie = 0;
    ui32 AlterTopicCookie = 0;
    std::vector<std::optional<TString>> GroupsToFetch;
    const TString DatabasePath;
    bool KqpSessionCreated = false;
};

} // NKafka
