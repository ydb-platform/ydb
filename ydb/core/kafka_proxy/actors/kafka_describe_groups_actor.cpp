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


std::shared_ptr<TDescribeGroupsResponseData> TKafkaDescribeGroupsActor::BuildResponse() {
    TDescribeGroupsResponseData describeGroupsResponse;
    for (auto& groupIdToDescription : GroupIdToDescription) {
        TString groupId = groupIdToDescription.first;
        describeGroupsResponse.Groups.push_back(std::move(groupIdToDescription.second));
    }
    auto response = std::make_shared<TDescribeGroupsResponseData>(std::move(describeGroupsResponse));
    return response;
};

NActors::IActor* CreateKafkaDescribeGroupsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TDescribeGroupsRequestData>& message) {
    return new TKafkaDescribeGroupsActor(context, correlationId, message);
}

void TKafkaDescribeGroupsActor::StartKqpSession(const TActorContext& ctx) {
    Kqp = std::make_unique<TKqpTxHelper>(DatabasePath);
    KAFKA_LOG_D("Sending create session request to KQP for database " << DatabasePath);
    Kqp->SendCreateSessionRequest(ctx);
}

 void TKafkaDescribeGroupsActor::Die(const TActorContext &ctx) {
    KAFKA_LOG_D("Dying.");
    if (Kqp) {
        Kqp->CloseKqpSession(ctx);
    }
}

NYdb::TParams TKafkaDescribeGroupsActor::BuildSelectParams(const TString& groupId) {
    NYdb::TParamsBuilder params;
    params.AddParam("$Database").Utf8(DatabasePath).Build();
    params.AddParam("$GroupId").Utf8(groupId).Build();
    return params.Build();
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

TMaybe<TString> TKafkaDescribeGroupsActor::GetErrorFromYdbResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    TStringBuilder builder = TStringBuilder() << "Recieved error on request to KQP. ErrorCode: " << ev->Get()->Record.GetYdbStatus();
    if (KqpCookieToGroupIdRequestType.find(ev->Cookie) == KqpCookieToGroupIdRequestType.end()) {
        return builder << "Unexpected cookie met in TEvQueryResponse. Cookie met: " << ev->Cookie << ".";
    } else if (ev->Get()->Record.GetYdbStatus() == Ydb::StatusIds::SCHEME_ERROR) {
        if (KqpCookieToGroupIdRequestType[ev->Cookie].second == KqpRequestType::MEMBERS_DESCRIPTION) {
            Kqp->SendInitTableRequest(ctx, NKikimr::NGRpcProxy::V1::TKafkaConsumerMembersMetaInitManager::GetInstant());
            KAFKA_LOG_D("Created ConsumerMembersMeta table.");
        } else if (KqpCookieToGroupIdRequestType[ev->Cookie].second == KqpRequestType::GROUP_DESCRIPTION) {
            KAFKA_LOG_D("Created ConsumerGroupsMeta table.");
        }
        return builder << "Unexpected YDB status in TEvQueryResponse. Expected YDB SUCCESS status, Actual: " << ev->Get()->Record.GetYdbStatus();
    } else if (ev->Get()->Record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        return builder << "Unexpected YDB status in TEvQueryResponse. Expected YDB SUCCESS status, Actual: " << ev->Get()->Record.GetYdbStatus();
    } else {
        return {};
    }
}

void TKafkaDescribeGroupsActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    KAFKA_LOG_D("Received query response from KQP DescribeGroups request");
    if (auto error = GetErrorFromYdbResponse(ev, ctx)) {
        KAFKA_LOG_W(error);
        if (KqpCookieToGroupIdRequestType.find(ev->Cookie) != KqpCookieToGroupIdRequestType.end()) {
            PendingResponses--;
        }
        return;
    }
    HandleSelectResponse(*ev->Get(), ctx, ev->Cookie);
}

TString TKafkaDescribeGroupsActor::GetYqlWithTableNames(const TString& templateStr, KqpRequestType requestName) {
    TString templateWithCorrectTableName;
    if (requestName == MEMBERS_DESCRIPTION) {
        templateWithCorrectTableName = std::regex_replace(
            templateStr.c_str(),
            std::regex("<consumer_members_table_name>"),
            NKikimr::NGRpcProxy::V1::TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()
        );
    } else if (requestName == GROUP_DESCRIPTION) {
        templateWithCorrectTableName = std::regex_replace(
            templateStr.c_str(),
            std::regex("<consumer_groups_table_name>"),
            NKikimr::NGRpcProxy::V1::TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()
        );
    }
    return templateWithCorrectTableName;
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
    for (auto& groupId : DescribeGroupsRequestData->Groups) {
        if (groupId.has_value()) {
            KAFKA_LOG_W("Sending SELECT_DESCRIPTION_OF_GROUP_MEMBERS request to KQP for database " << DatabasePath << " for " << groupId << " group.");
            PendingResponses++;
            ++KqpCookie;
            KqpCookieToGroupIdRequestType[KqpCookie] = {*groupId, KqpRequestType::GROUP_DESCRIPTION};
            KqpQueryQueue.emplace(*groupId, KqpRequestType::GROUP_DESCRIPTION, KqpCookie);
            if (KqpQueryQueue.size() == 1) {
                KAFKA_LOG_W("Sending SELECT_GROUP_INFO request to KQP for database " << DatabasePath << " for " << groupId << " group.");
                Kqp->SendYqlRequest(GetYqlWithTableNames(SELECT_GROUP_INFO, KqpRequestType::GROUP_DESCRIPTION),
                                    BuildSelectParams(*groupId),
                                    KqpCookie,
                                    ctx,
                                    false);
            }
            ++KqpCookie;
            PendingResponses++;
            KqpCookieToGroupIdRequestType[KqpCookie] = {*groupId, KqpRequestType::MEMBERS_DESCRIPTION};
            KqpQueryQueue.emplace(*groupId, KqpRequestType::MEMBERS_DESCRIPTION, KqpCookie);
        }
    }
}

void TKafkaDescribeGroupsActor::Handle(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    KAFKA_LOG_W("Sending fail response because of request timeout " << WAIT_REQUESTS_SECONDS << " sec");
    Send(Context->ConnectionId,
        new TEvKafka::TEvResponse(CorrelationId, BuildResponse(), EKafkaErrors::REQUEST_TIMED_OUT));
    Die(ctx);
}

void TKafkaDescribeGroupsActor::Bootstrap(const NActors::TActorContext& ctx) {
    if (NKikimr::AppData()->FeatureFlags.GetEnableKafkaNativeBalancing()) {
        StartKqpSession(ctx);
        Become(&TKafkaDescribeGroupsActor::StateWork);
        auto wakeup = std::make_unique<TEvents::TEvWakeup>(1);
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

void TKafkaDescribeGroupsActor::SendFailResponse(EKafkaErrors errorCode, const std::optional<TString>& errorMessage = std::nullopt) {
    for (auto& groupId : DescribeGroupsRequestData->Groups) {
        if (groupId.has_value()) {
            GroupIdToDescription[*groupId].ErrorCode = errorCode;
        }
    }
    if (errorMessage.has_value()) {
        KAFKA_LOG_W("Sending fail response with error code: " << errorCode << ". Reason:  " << errorMessage);
    } else {
        KAFKA_LOG_W("Sending fail response with error code: " << errorCode);
    }

    Send(Context->ConnectionId,
        new TEvKafka::TEvResponse(CorrelationId, BuildResponse(), errorCode));
}

int TKafkaDescribeGroupsActor::ParseGroupMembersMetadata(const NKqp::TEvKqp::TEvQueryResponse& response) {
    KAFKA_LOG_D("Parsing KQP response");

    NYdb::TResultSetParser parser(response.Record.GetResponse().GetYdbResults(0));
    int addedMembersCount = 0;
    while (parser.TryNextRow()) {
        TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember groupMember;
        TString groupId = parser.ColumnParser("consumer_group").GetUtf8().c_str();
        TString memberId = parser.ColumnParser("member_id").GetUtf8().c_str();
        TString groupInstanceId = parser.ColumnParser("instance_id").GetOptionalUtf8().value_or("").c_str();
        TString assignment = parser.ColumnParser("assignment").GetOptionalString().value_or("");
        TString protoStr = parser.ColumnParser("worker_state_proto").GetOptionalString().value_or("");
        groupMember.GroupInstanceId = groupInstanceId;
        groupMember.MemberId = memberId;
        groupMember.MemberAssignment = assignment;
        NKafka::TWorkerState workerState;
        groupMember.MemberMetadata = "";
        if (protoStr.empty() || workerState.ParseFromString(protoStr)) {
            const auto& protocols = workerState.protocols();
            TString protocol = *GroupIdToDescription[groupId].ProtocolData;
            for (const auto& p : protocols) {
                if (p.protocol_name() == protocol) {
                    groupMember.MemberMetadata = p.metadata();
                    break;
                }
            }
        }
        GroupIdToDescription[groupId].Members.push_back(std::move(groupMember));
        addedMembersCount++;
    }
    return addedMembersCount;
}

void TKafkaDescribeGroupsActor::ParseGroupInfo(const NKqp::TEvKqp::TEvQueryResponse& response) {
    KAFKA_LOG_D("Parsing KQP response");

    NYdb::TResultSetParser parser(response.Record.GetResponse().GetYdbResults(0));
    while (parser.TryNextRow()) {
        TString groupId = parser.ColumnParser("consumer_group").GetUtf8().c_str();
        ui64 state = parser.ColumnParser("state").GetUint64();
        TString protocolType = parser.ColumnParser("protocol_type").GetUtf8().c_str();
        TString protocolData = parser.ColumnParser("protocol").GetOptionalUtf8().value_or("").c_str();
        GroupIdToDescription[groupId].ProtocolData = protocolData;
        GroupIdToDescription[groupId].GroupId = groupId;
        GroupIdToDescription[groupId].ProtocolType = protocolType;
        GroupIdToDescription[groupId].GroupState = NKafka::numbersToStatesMapping.at(state);
    }
}

void TKafkaDescribeGroupsActor::HandleSelectResponse(const NKqp::TEvKqp::TEvQueryResponse& response, const TActorContext& ctx, ui64 responseCookie) {
    KAFKA_LOG_D("Handling Select Response " << response.Record.GetResponse().GetYdbResults().size());
    if (response.Record.GetResponse().GetYdbResults().size() != 1) {
        TString errorMessage = TStringBuilder() << "KQP returned wrong number of result sets on SELECT query. Expected 1, got " << response.Record.GetResponse().GetYdbResults().size() << ".";
        KAFKA_LOG_W(errorMessage);
        TString groupId = KqpCookieToGroupIdRequestType[responseCookie].first;
        GroupIdToDescription[groupId].ErrorCode = EKafkaErrors::BROKER_NOT_AVAILABLE;
        PendingResponses--;
        return;
    }
    if (KqpCookieToGroupIdRequestType[responseCookie].second == MEMBERS_DESCRIPTION) {
        int addedGroupMembers = ParseGroupMembersMetadata(response);
        PendingResponses--;
        KAFKA_LOG_D("Write " << addedGroupMembers << " members description " << response.Record.GetResponse().GetYdbResults().size());
    } else {
        ParseGroupInfo(response);
        KAFKA_LOG_D("Write info for group" << KqpCookieToGroupIdRequestType[responseCookie].first);
        PendingResponses--;
    }
    KqpQueryQueue.pop();

    if (!KqpQueryQueue.empty()) {
        auto nextQuery = KqpQueryQueue.front();
        if (nextQuery.KqpRequestTypeName == KqpRequestType::MEMBERS_DESCRIPTION) {
            Kqp->SendYqlRequest(GetYqlWithTableNames(SELECT_DESCRIPTION_OF_GROUP_MEMBERS, KqpRequestType::MEMBERS_DESCRIPTION),
                                        BuildSelectParams(nextQuery.GroupId),
                                        nextQuery.KqpCookie,
                                        ctx,
                                        false);
        } else if (nextQuery.KqpRequestTypeName == KqpRequestType::GROUP_DESCRIPTION) {
            KAFKA_LOG_W("Sending SELECT_GROUP_INFO request to KQP for database " << DatabasePath);
                Kqp->SendYqlRequest(GetYqlWithTableNames(SELECT_GROUP_INFO, GROUP_DESCRIPTION),
                                    BuildSelectParams(nextQuery.GroupId),
                                    nextQuery.KqpCookie,
                                    ctx,
                                    false);
        }
    }
    if (PendingResponses == 0) {
      auto responseDescribeGroups = BuildResponse();
      Send(Context->ConnectionId,
           new TEvKafka::TEvResponse(CorrelationId, responseDescribeGroups,
                                     EKafkaErrors::NONE_ERROR));
      Die(ctx);
    }
}

} // namespace NKafka
