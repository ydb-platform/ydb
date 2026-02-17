#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/core/kafka_proxy/kafka_transactions_coordinator.h>
#include <ydb/services/persqueue_v1/actors/schema_actors.h>
#include <ydb/core/kafka_proxy/kafka_transactional_producers_initializers.h>
#include "ydb/core/kafka_proxy/kafka_messages.h"
#include <ydb/core/kafka_proxy/kafka_consumer_groups_metadata_initializers.h>
#include <ydb/core/kqp/common/simple/services.h>

#include <util/generic/cast.h>
#include <regex>

#include "actors.h"
#include "kafka_describe_groups_actor.h"
#include "kafka_state_name_to_int.h"


namespace NKafka {

NActors::IActor* CreateKafkaDescribeGroupsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TDescribeGroupsRequestData>& message) {
    return new TKafkaDescribeGroupsActor(context, correlationId, message);
}

void TKafkaDescribeGroupsActor::Bootstrap(const NActors::TActorContext& ctx) {
    if (NKikimr::AppData()->FeatureFlags.GetEnableKafkaNativeBalancing()) {
        Kqp = std::make_unique<TKqpTxHelper>(Context->ResourceDatabasePath);
        if (Context->ResourceDatabasePath == AppData(ctx)->TenantName) {
            Kqp->SendInitTableRequest(ctx, NKikimr::NGRpcProxy::V1::TKafkaConsumerGroupsMetaInitManager::GetInstant());
            Kqp->SendInitTableRequest(ctx, NKikimr::NGRpcProxy::V1::TKafkaConsumerMembersMetaInitManager::GetInstant());
        } else {
            StartKqpSession(ctx);
        }
        Become(&TKafkaDescribeGroupsActor::StateWork);
        auto wakeup = std::make_unique<TEvents::TEvWakeup>();
        ctx.ActorSystem()->Schedule(
            TDuration::Seconds(WAIT_REQUESTS_SECONDS),
            new IEventHandle(SelfId(), SelfId(), wakeup.release())
        );
    } else {
        KAFKA_LOG_ERROR("No EnableKafkaNativeBalancing FeatureFlag set.");
        TDescribeGroupsResponseData groupsDescriptionResponseWithError;
        Send(Context->ConnectionId,
            new TEvKafka::TEvResponse(CorrelationId,
            std::make_shared<TDescribeGroupsResponseData>(std::move(groupsDescriptionResponseWithError)),
            EKafkaErrors::UNSUPPORTED_VERSION));
    }
}

void TKafkaDescribeGroupsActor::Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx) {
    KAFKA_LOG_D("Received TEvManagerPrepared. Sending create session request to KQP.");
    InitedTablesCount++;
    if (InitedTablesCount == TABLES_TO_INIT_COUNT) {
        StartKqpSession(ctx);
    }
}


void TKafkaDescribeGroupsActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
    KAFKA_LOG_D("KQP session created");
    if (!Kqp->HandleCreateSessionResponse(ev, ctx)) {
        SendFailResponse(EKafkaErrors::BROKER_NOT_AVAILABLE, "Failed to create KQP session");
        Die(ctx);
        return;
    }
    SendToKqpDescribeGroupsMetadataRequest(ctx);
}

void TKafkaDescribeGroupsActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    KAFKA_LOG_D("Received query response from KQP DescribeGroups request");
    if (auto error = GetErrorFromYdbResponse(ev)) {
        KAFKA_LOG_W(error);
        SendFailResponse(EKafkaErrors::BROKER_NOT_AVAILABLE, *error);
        Die(ctx);
        return;
    }
    HandleSelectResponse(*ev->Get(), ctx);
}

void TKafkaDescribeGroupsActor::Handle(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    KAFKA_LOG_W("Sending fail response because of request timeout " << WAIT_REQUESTS_SECONDS << " sec");
    Send(Context->ConnectionId,
        new TEvKafka::TEvResponse(CorrelationId, BuildResponse(), EKafkaErrors::REQUEST_TIMED_OUT));
    Die(ctx);
}

void TKafkaDescribeGroupsActor::Die(const TActorContext &ctx) {
    KAFKA_LOG_D("Dying.");
    if (Kqp) {
        Kqp->CloseKqpSession(ctx);
    }
    TBase::Die(ctx);
}

void TKafkaDescribeGroupsActor::StartKqpSession(const TActorContext& ctx) {
    KAFKA_LOG_D("Sending create session request to KQP for database " << DatabasePath);
    Kqp->SendCreateSessionRequest(ctx);
}

void TKafkaDescribeGroupsActor::HandleSelectResponse(const NKqp::TEvKqp::TEvQueryResponse& response, const TActorContext& ctx) {
    KAFKA_LOG_D("Handling Select Response for DescribeGroups. SELECT result size: " << response.Record.GetResponse().GetYdbResults().size());
    if (response.Record.GetResponse().GetYdbResults().size() != 2) {
        TString errorMessage = TStringBuilder() << "KQP returned wrong number of result sets on SELECT query. Expected 2, got " << response.Record.GetResponse().GetYdbResults().size() << ".";
        KAFKA_LOG_W(errorMessage);
        return;
    }
    ParseGroupDescriptionMetadata(response);
    auto responseDescribeGroups = BuildResponse();
    Send(Context->ConnectionId,
        new TEvKafka::TEvResponse(CorrelationId, responseDescribeGroups,
                                    EKafkaErrors::NONE_ERROR));
    Die(ctx);
}

void TKafkaDescribeGroupsActor::ParseGroupDescriptionMetadata(const NKqp::TEvKqp::TEvQueryResponse& response) {
    KAFKA_LOG_D("Parsing Groups metadata");
    ParseGroupMetadata(response);
    KAFKA_LOG_D("Parsing Members metadata");
    ParseMembersMetadata(response);
}

void TKafkaDescribeGroupsActor::FillInMemberMetadata(NKafka::TDescribeGroupsResponseData::TDescribedGroup& describedGroup,
        NKafka::TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember& groupMember,
        const TString& protoStr) {
    NKafka::TWorkerState workerState;
    groupMember.MemberMetadataStr = "";
    if (protoStr.empty() || workerState.ParseFromString(protoStr)) {
        const auto& protocols = workerState.protocols();
        TString protocol = *describedGroup.ProtocolData;
        for (const auto& p : protocols) {
            if (p.protocol_name() == protocol) {
                groupMember.MemberMetadataStr = TString(p.metadata());
                break;
            }
        }
    }
    groupMember.MemberMetadata = groupMember.MemberMetadataStr;
}

void TKafkaDescribeGroupsActor::ParseGroupMetadata(const NKqp::TEvKqp::TEvQueryResponse& response) {
    NYdb::TResultSetParser parserGroup(response.Record.GetResponse().GetYdbResults(REQUEST_GROUPS_QUERY_INDEX));
    while (parserGroup.TryNextRow()) {
        TString groupId = parserGroup.ColumnParser("consumer_group").GetUtf8().c_str();
        ui64 state = parserGroup.ColumnParser("state").GetUint64();
        TString protocolType = parserGroup.ColumnParser("protocol_type").GetUtf8().c_str();
        TString protocolData = parserGroup.ColumnParser("protocol").GetOptionalUtf8().value_or("").c_str();
        auto& describedGroup = GroupIdToDescription[groupId];
        describedGroup.ProtocolData = protocolData;
        describedGroup.GroupId = groupId;
        describedGroup.ProtocolType = protocolType;
        describedGroup.GroupState = NKafka::NConsumer::NumbersToStatesMapping.at(state);
    }
}

void TKafkaDescribeGroupsActor::ParseMembersMetadata(const NKqp::TEvKqp::TEvQueryResponse& response) {
    NYdb::TResultSetParser parserMembers(response.Record.GetResponse().GetYdbResults(REQUEST_MEMBERS_QUERY_INDEX));
    while (parserMembers.TryNextRow()) {
        TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember groupMember;
        TString groupId = parserMembers.ColumnParser("consumer_group").GetUtf8().c_str();
        TString memberId = parserMembers.ColumnParser("member_id").GetUtf8().c_str();
        TString groupInstanceId = parserMembers.ColumnParser("instance_id").GetOptionalUtf8().value_or("").c_str();

        TString protoStr = parserMembers.ColumnParser("worker_state_proto").GetOptionalString().value_or("");
        groupMember.GroupInstanceId = groupInstanceId;
        groupMember.MemberId = memberId;
        groupMember.MemberAssignmentStr = parserMembers.ColumnParser("assignment").GetOptionalString().value_or("");
        groupMember.MemberAssignment = TString(groupMember.MemberAssignmentStr);

        auto& describedGroup = GroupIdToDescription[groupId];
        FillInMemberMetadata(describedGroup, groupMember, protoStr);

        describedGroup.Members.push_back(std::move(groupMember));
        describedGroup.ErrorCode = EKafkaErrors::NONE_ERROR;
    }
}

NYdb::TParams TKafkaDescribeGroupsActor::BuildSelectParams() {
    NYdb::TParamsBuilder params;
    params.AddParam("$Database").Utf8(DatabasePath).Build();
    auto& groupIds = params.AddParam("$GroupIds").BeginList();

    KAFKA_LOG_D(TStringBuilder() << "Groups count: " << DescribeGroupsRequestData->Groups.size());

    for (auto& groupId: DescribeGroupsRequestData->Groups) {
        groupIds.AddListItem().Utf8(*groupId);
    }
    groupIds.EndList().Build();

    return params.Build();
}

TString TKafkaDescribeGroupsActor::GetYqlWithTableNames(const TString& templateStr) {
    TString templateWithCorrectTableNames = std::regex_replace(
        templateStr.c_str(),
        std::regex("<consumer_members_table_name>"),
        NKikimr::NGRpcProxy::V1::TKafkaConsumerMembersMetaInitManager::GetInstant()->FormPathToResourceTable(Context->ResourceDatabasePath).c_str()
    );

    templateWithCorrectTableNames = std::regex_replace(
        templateWithCorrectTableNames.c_str(),
        std::regex("<consumer_groups_table_name>"),
        NKikimr::NGRpcProxy::V1::TKafkaConsumerGroupsMetaInitManager::GetInstant()->FormPathToResourceTable(Context->ResourceDatabasePath).c_str()
    );

    return templateWithCorrectTableNames;
}

std::shared_ptr<TDescribeGroupsResponseData> TKafkaDescribeGroupsActor::BuildResponse() {
    TDescribeGroupsResponseData describeGroupsResponse;
    for (auto& groupId : DescribeGroupsRequestData->Groups) {
        if (GroupIdToDescription.find(groupId) != GroupIdToDescription.end()) {
            auto& groupDescription = GroupIdToDescription.at(*groupId);
            groupDescription.ErrorCode = EKafkaErrors::NONE_ERROR;
            describeGroupsResponse.Groups.push_back(std::move(groupDescription));
        } else {
            TDescribeGroupsResponseData::TDescribedGroup groupDescription;
            groupDescription.ErrorCode = EKafkaErrors::GROUP_ID_NOT_FOUND;
            groupDescription.GroupId = groupId;
            describeGroupsResponse.Groups.push_back(std::move(groupDescription));
        }
    }
    return std::make_shared<TDescribeGroupsResponseData>(std::move(describeGroupsResponse));
}

void TKafkaDescribeGroupsActor::SendToKqpDescribeGroupsMetadataRequest(const TActorContext& ctx) {
    if (DescribeGroupsRequestData->Groups.size() == 0) {
        auto response = BuildResponse();
        Send(Context->ConnectionId,
            new TEvKafka::TEvResponse(CorrelationId,
                                response,
                                EKafkaErrors::NONE_ERROR));
        Die(ctx);
    }
    Kqp->SendYqlRequest(GetYqlWithTableNames(SELECT_GROUPS_DESCRIPTION),
                                    BuildSelectParams(),
                                    KqpCookie,
                                    ctx,
                                    false);
}

void TKafkaDescribeGroupsActor::SendFailResponse(EKafkaErrors errorCode, const std::optional<TString>& errorMessage) {
    for (auto& groupId : DescribeGroupsRequestData->Groups) {
        GroupIdToDescription[*groupId].ErrorCode = errorCode;
    }
    if (errorMessage.has_value()) {
        KAFKA_LOG_W("Sending fail response with error code: " << errorCode << ". Reason:  " << errorMessage);
    } else {
        KAFKA_LOG_W("Sending fail response with error code: " << errorCode);
    }

    Send(Context->ConnectionId,
        new TEvKafka::TEvResponse(CorrelationId, BuildResponse(), errorCode));
}

TMaybe<TString> TKafkaDescribeGroupsActor::GetErrorFromYdbResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
    TStringBuilder builder = TStringBuilder() << "Received error on request to KQP. ErrorCode: " << ev->Get()->Record.GetYdbStatus();
    if (ev->Get()->Record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        return builder << "Unexpected YDB status in TEvQueryResponse. Expected YDB SUCCESS status, Actual: " << ev->Get()->Record.GetYdbStatus();
    } else {
        return {};
    }
}

} // namespace NKafka
