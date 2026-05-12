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
#include "kafka_list_groups_actor.h"
#include "kafka_state_name_to_int.h"


namespace NKafka {

NActors::IActor* CreateKafkaListGroupsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TListGroupsRequestData>& message) {
    return new TKafkaListGroupsActor(context, correlationId, message);
}

void TKafkaListGroupsActor::Bootstrap(const NActors::TActorContext& ctx) {
    Kqp = std::make_unique<TKqpTxHelper>(Context->ResourceDatabasePath);
    if (NKikimr::AppData()->FeatureFlags.GetEnableKafkaNativeBalancing()) {
        if (Context->ResourceDatabasePath == AppData(ctx)->TenantName) {
            Kqp->SendInitTableRequest(ctx, NKikimr::NGRpcProxy::V1::TKafkaConsumerGroupsMetaInitManager::GetInstant());
        } else {
            StartKqpSession(ctx);
        }
        Become(&TKafkaListGroupsActor::StateWork);
    } else {
        KAFKA_LOG_ERROR("No EnableKafkaNativeBalancing FeatureFlag set.");
        TListGroupsResponseData consumerGroupsResponseWithError;
        Send(Context->ConnectionId,
            new TEvKafka::TEvResponse(CorrelationId,
            std::make_shared<TListGroupsResponseData>(std::move(consumerGroupsResponseWithError)),
            EKafkaErrors::UNSUPPORTED_VERSION));
    }
}

void TKafkaListGroupsActor::Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx) {
    KAFKA_LOG_D("Received TEvManagerPrepared. Sending create session request to KQP.");
    StartKqpSession(ctx);
}

void TKafkaListGroupsActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
    KAFKA_LOG_D("KQP session created");
    if (!Kqp->HandleCreateSessionResponse(ev, ctx)) {
        SendFailResponse(EKafkaErrors::BROKER_NOT_AVAILABLE, "Failed to create KQP session");
        Die(ctx);
        return;
    }

    SendToKqpConsumerGroupsRequest(ctx);
}

void TKafkaListGroupsActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    KAFKA_LOG_D("Received query response from KQP ListGroups request");
    if (auto error = GetErrorFromYdbResponse(ev)) {
        KAFKA_LOG_W(error);
        SendFailResponse(EKafkaErrors::BROKER_NOT_AVAILABLE, error->data());
        Die(ctx);
        return;
    }

    HandleSelectResponse(*ev->Get(), ctx);
}

void TKafkaListGroupsActor::HandleSelectResponse(const NKqp::TEvKqp::TEvQueryResponse& response, const TActorContext& ctx) {
    KAFKA_LOG_D("Handling Select Response " << response.Record.GetResponse().GetYdbResults().size());
    if (response.Record.GetResponse().GetYdbResults().size() != 1) {
        TString errorMessage = TStringBuilder() << "KQP returned wrong number of result sets on SELECT query. Expected 1, got " << response.Record.GetResponse().GetYdbResults().size() << ".";
        KAFKA_LOG_W(errorMessage);
        SendFailResponse(EKafkaErrors::BROKER_NOT_AVAILABLE, errorMessage);
        Die(ctx);
        return;
    }

    TListGroupsResponseData consumerGroupsResponse = ParseGroupsMetadata(response);

    auto responseListGroups = BuildResponse(consumerGroupsResponse);
    Send(Context->ConnectionId,
        new TEvKafka::TEvResponse(CorrelationId, responseListGroups, EKafkaErrors::NONE_ERROR));
}

void TKafkaListGroupsActor::Die(const TActorContext &ctx) {
    KAFKA_LOG_D("Dying.");
    if (Kqp) {
        Kqp->CloseKqpSession(ctx);
    }
    TBase::Die(ctx);
}

void TKafkaListGroupsActor::StartKqpSession(const TActorContext& ctx) {
    KAFKA_LOG_D("Sending create session request to KQP for database " << DatabasePath);
    Kqp->SendCreateSessionRequest(ctx);
}

TListGroupsResponseData TKafkaListGroupsActor::ParseGroupsMetadata(const NKqp::TEvKqp::TEvQueryResponse& response) {
    KAFKA_LOG_D("Parsing KQP response");
    TListGroupsResponseData listGroupsResponse;

    NYdb::TResultSetParser parser(response.Record.GetResponse().GetYdbResults(0));
    while (parser.TryNextRow()) {
        TListGroupsResponseData::TListedGroup groupInfo;
        TString consumerName = parser.ColumnParser("consumer_group").GetUtf8().c_str();
        TString protocolType = parser.ColumnParser("protocol_type").GetUtf8().c_str();
        groupInfo.GroupId = consumerName;
        groupInfo.ProtocolType = protocolType;
        ui64 groupStateNumber = parser.ColumnParser("state").GetUint64();
        groupInfo.GroupState = NKafka::NConsumer::NumbersToStatesMapping.at(groupStateNumber);
        listGroupsResponse.Groups.push_back(groupInfo);
    }
    return listGroupsResponse;
}

TString TKafkaListGroupsActor::GetYqlWithTableNames(const TString& templateStr) {
    TString templateWithConsumerStateTable = std::regex_replace(
        templateStr.c_str(),
        std::regex("<consumer_state_table_name>"),
        NKikimr::NGRpcProxy::V1::TKafkaConsumerGroupsMetaInitManager::GetInstant()->FormPathToResourceTable(Context->ResourceDatabasePath).c_str()
    );
    return templateWithConsumerStateTable;
}

TMaybe<TString> TKafkaListGroupsActor::GetErrorFromYdbResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
    TStringBuilder builder = TStringBuilder() << "Received error on request to KQP. Last sent request: " << "SELECT" << ". Reason: ";
    if (ev->Cookie != KqpCookie) {
        return builder << "Unexpected cookie in TEvQueryResponse. Expected KQP Cookie: " << KqpCookie << ", Actual: " << ev->Cookie << ".";
    } else if (ev->Get()->Record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        return builder << "Unexpected YDB status in TEvQueryResponse. Expected YDB SUCCESS status, Actual: " << ev->Get()->Record.GetYdbStatus() << ".";
    } else {
        return {};
    }
}

NYdb::TParams TKafkaListGroupsActor::BuildSelectParams() {
    NYdb::TParamsBuilder params;
    params.AddParam("$Database").Utf8(DatabasePath).Build();
    if (ListGroupsRequestData->StatesFilter.size() > 0) {
        auto& statesFilterParams = params.AddParam("$StatesFilter").BeginList();
        for (auto& statesNumberFilter : ListGroupsRequestData->StatesFilter) {
            if (statesNumberFilter.has_value()) {
                statesFilterParams.AddListItem().Uint32(NKafka::NConsumer::StatesToNumbersMapping.at(*statesNumberFilter));
            }
        }
        statesFilterParams.EndList().Build();
    }
    return params.Build();
}

std::shared_ptr<TListGroupsResponseData> TKafkaListGroupsActor::BuildResponse(TListGroupsResponseData responseData) {
    auto response = std::make_shared<TListGroupsResponseData>(std::move(responseData));
    return response;
};

void TKafkaListGroupsActor::SendToKqpConsumerGroupsRequest(const TActorContext& ctx) {
    KAFKA_LOG_W("Sending select request to KQP for database " << DatabasePath);
    Kqp->SendYqlRequest(
    GetYqlWithTableNames(ListGroupsRequestData->StatesFilter.size() > 0 ?
                                    SELECT_GROUPS_WITH_FILTER :
                                    SELECT_GROUPS_NO_FILTER),
    BuildSelectParams(),
    KqpCookie,
    ctx,
    false
    );
}

void TKafkaListGroupsActor::SendFailResponse(EKafkaErrors errorCode, const std::optional<TString>& errorMessage = std::nullopt) {
    if (errorMessage.has_value()) {
        KAFKA_LOG_W("Sending fail response with error code: " << errorCode << ". Reason:  " << errorMessage);
    } else {
        KAFKA_LOG_W("Sending fail response with error code: " << errorCode);
    }

    TListGroupsResponseData consumerGroupsResponseWithError;
    consumerGroupsResponseWithError.ErrorCode = errorCode;

    Send(Context->ConnectionId,
        new TEvKafka::TEvResponse(CorrelationId, BuildResponse(consumerGroupsResponseWithError), errorCode));
}

} // namespace NKafka
