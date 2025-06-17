#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include "ydb/library/aclib/aclib.h"
#include <ydb/core/protos/kafka.pb.h>
#include <ydb/core/kafka_proxy/kqp_helper.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include "../kafka_consumer_members_metadata_initializers.h"


#include "actors.h"

namespace NKafka {


static const TString SELECT_DESCRIPTION_OF_GROUP_MEMBERS = R"sql(
    DECLARE $Database AS Utf8;
    DECLARE $GroupId AS Utf8;

    SELECT * FROM (
        SELECT
            `<consumer_members_table_name>`.*,
            DENSE_RANK() OVER (PARTITION BY consumer_group ORDER BY generation DESC) AS row_num
        FROM `<consumer_members_table_name>`
        WHERE database = $Database
    ) WHERE row_num = 1 AND consumer_group = $GroupId;
)sql";

static const TString SELECT_GROUP_INFO = R"sql(
    DECLARE $Database AS Utf8;
    DECLARE $GroupId AS Utf8;

    SELECT * FROM (
        SELECT
            `<consumer_groups_table_name>`.*,
            ROW_NUMBER() OVER (PARTITION BY consumer_group ORDER BY generation DESC) AS row_num
        FROM `<consumer_groups_table_name>`
        WHERE database = $Database
    ) WHERE row_num = 1 AND consumer_group = $GroupId;
)sql";

class TKafkaDescribeGroupsActor: public NActors::TActorBootstrapped<TKafkaDescribeGroupsActor> {

public:
    TKafkaDescribeGroupsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TDescribeGroupsRequestData>& message)
        : Context(context)
        , CorrelationId(correlationId)
        , DescribeGroupsRequestData(message)
        , DatabasePath(context->DatabasePath) {
    }


void Bootstrap(const NActors::TActorContext& ctx);


TStringBuilder LogPrefix() const {
    return TStringBuilder() << "KafkaDescribeGroupsActor{DatabasePath=" << DatabasePath << "}: ";
}

enum KqpRequestType {
    MEMBERS_DESCRIPTION = 0,
    GROUP_DESCRIPTION = 1,
};

struct TDescribeGroupsKqpQuery {
    TString GroupId;
    KqpRequestType KqpRequestTypeName;
    ui64 KqpCookie;
};

ui32 WAIT_REQUESTS_SECONDS = 20;

private:
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
        }
    }

    void StartKqpSession(const TActorContext& ctx);
    void SendToKqpDescribeGroupsMetadataRequest(const TActorContext& ctx);
    void HandleSelectResponse(const NKqp::TEvKqp::TEvQueryResponse& response, const TActorContext& ctx, ui64 responseCookie);
    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);
    TString GetYqlWithTableNames(const TString& templateStr, KqpRequestType requestName);
    int ParseGroupMembersMetadata(const NKqp::TEvKqp::TEvQueryResponse& response);
    void ParseGroupInfo(const NKqp::TEvKqp::TEvQueryResponse& response);
    NYdb::TParams BuildSelectParams(const TString& groupId);

    void SendFailResponse(EKafkaErrors errorCode, const std::optional<TString>& errorMessage);

    std::shared_ptr<TDescribeGroupsResponseData> BuildResponse();

    TMaybe<TString> GetErrorFromYdbResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev,
                                            const TActorContext& ctx);
    void Die(const TActorContext &ctx);
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TDescribeGroupsRequestData> DescribeGroupsRequestData;
    std::map<TString, TDescribeGroupsResponseData::TDescribedGroup> GroupIdToDescription;

    std::unique_ptr<TKqpTxHelper> Kqp;
    std::queue<TDescribeGroupsKqpQuery> KqpQueryQueue;

    const TString DatabasePath;

    TString KqpSessionId;
    ui64 KqpCookie = 0;
    ui64 PendingResponses = 0;
    std::map<ui64, std::pair<TString, KqpRequestType>> KqpCookieToGroupIdRequestType;

};

} // namespace NKafka
