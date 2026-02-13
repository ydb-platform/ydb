#include "kafka_transaction_actor.h"
#include "kafka_transaction_actor_sql.h"
#include "txn_actor_response_builder.h"
#include <ydb/core/kafka_proxy/kafka_transactions_coordinator.h>
#include <ydb/core/kafka_proxy/kafka_transactional_producers_initializers.h>
#include <ydb/core/kafka_proxy/kafka_consumer_groups_metadata_initializers.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <util/generic/cast.h>
#include <regex>

#define VALIDATE_PRODUCER_IN_REQUEST(ErrorResponseType) \
if (!ProducerInRequestIsValid(ev->Get()->Request)) { \
    auto& kafkaRequest = ev->Get()->Request; \
    TString message = TStringBuilder() << "Received invalid request. Got: " \
            << "transactionalId=" << *kafkaRequest->TransactionalId \
            << ", producerId=" << kafkaRequest->ProducerId \
            << ", producerEpoch=" << kafkaRequest->ProducerEpoch; \
    SendFailResponse<ErrorResponseType>(ev, EKafkaErrors::UNKNOWN_SERVER_ERROR, message); \
    throw yexception() << message; \
} \

namespace NKafka {

    void TTransactionActor::Handle(TEvKafka::TEvAddPartitionsToTxnRequest::TPtr& ev, const TActorContext&){
        KAFKA_LOG_D("Received ADD_PARTITIONS_TO_TXN request");
        VALIDATE_PRODUCER_IN_REQUEST(TAddPartitionsToTxnResponseData);

        for (auto& topicInRequest : ev->Get()->Request->Topics) {
            for (auto& partitionInRequest : topicInRequest.Partitions) {
                PartitionsInTxn.emplace(GetFullTopicPath(*topicInRequest.Name), static_cast<ui32>(partitionInRequest));
            }
        }
        SendOkResponse<TAddPartitionsToTxnResponseData>(ev);
    }

    // Method does nothing. In Kafka it will add __consumer_offsets topic to transaction, but
    // in YDB Topics we store offsets in table and do not need this extra action.
    // Thus we can just ignore this request.
    void TTransactionActor::Handle(TEvKafka::TEvAddOffsetsToTxnRequest::TPtr& ev, const TActorContext&) {
        KAFKA_LOG_D("Received ADD_OFFSETS_TO_TXN request");
        VALIDATE_PRODUCER_IN_REQUEST(TAddOffsetsToTxnResponseData);
        SendOkResponse<TAddOffsetsToTxnResponseData>(ev);
    }

    void TTransactionActor::Handle(TEvKafka::TEvTxnOffsetCommitRequest::TPtr& ev, const TActorContext&) {
        KAFKA_LOG_D("Received TXN_OFFSET_COMMIT request");
        VALIDATE_PRODUCER_IN_REQUEST(TTxnOffsetCommitResponseData);

        // save offsets for future use
        for (auto& topicInRequest : ev->Get()->Request->Topics) {
            for (auto& partitionInRequest : topicInRequest.Partitions) {
                TTopicPartition topicPartition = {GetFullTopicPath(*topicInRequest.Name), static_cast<ui32>(partitionInRequest.PartitionIndex)};
                TPartitionCommit newCommit{
                    .Partition = partitionInRequest.PartitionIndex,
                    .Offset = partitionInRequest.CommittedOffset,
                    .ConsumerName = ev->Get()->Request->GroupId.value(),
                    .ConsumerGeneration = ev->Get()->Request->GenerationId
                };
                auto it = OffsetsToCommit.find(topicPartition);
                if (it == OffsetsToCommit.end()) {
                    OffsetsToCommit.emplace(std::make_pair(topicPartition, newCommit));
                } else {
                    it->second = newCommit;
                }
            }
        }
        SendOkResponse<TTxnOffsetCommitResponseData>(ev);
    }

    /*
    If abort, will send back ok.
    If commit, will do following sequence of calls and event handlers:
    1. Open KQP Session
    2. Handle(NKqp::TEvKqp::TEvCreateSessionResponse) - KQP session created
    3. Send Select request for current producer and consumers state, begin transaction
    4. Handle(NKqp::TEvKqp::TEvQueryResponse): Validate returned producer and consumers state. Return PRODUCER_FENCED if check failed
    5. Send Commit request with properly created KafkaApiOperations
    6. Handle(NKqp::TEvKqp::TEvQueryResponse): If everything committed successfully, return OK to the client
    7. Close KQP session, send to coordinator TEvTransactionActorDied, die.
    */
    void TTransactionActor::Handle(TEvKafka::TEvEndTxnRequest::TPtr& ev, const TActorContext& ctx) {
        KAFKA_LOG_D("Received END_TXN request");
        VALIDATE_PRODUCER_IN_REQUEST(TEndTxnResponseData);

        bool txnAborted = !ev->Get()->Request->Committed;
        if (CommitStarted) {
            return; // we just ignore second and subsequent requests
        } else if (txnAborted) {
            SendOkResponse<TEndTxnResponseData>(ev);
            Die(ctx);
        } else if (TxnExpired()) {
            SendFailResponse<TEndTxnResponseData>(ev, EKafkaErrors::PRODUCER_FENCED, "Transaction exceeded its timeout: " + std::to_string(TxnTimeoutMs));
            Die(ctx);
        } else {
            CommitStarted = true;
            EndTxnRequestPtr = std::move(ev);
            StartKqpSession(ctx);
        }
    }

    void TTransactionActor::Handle(TEvents::TEvPoison::TPtr&, const TActorContext& ctx) {
        Die(ctx);
    }

    void TTransactionActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        KAFKA_LOG_D("KQP session created");
        if (!Kqp->HandleCreateSessionResponse(ev, ctx)) {
            SendFailResponse<TEndTxnResponseData>(EndTxnRequestPtr, EKafkaErrors::BROKER_NOT_AVAILABLE, "Failed to create KQP session");
            Die(ctx);
            return;
        }

        KqpSessionId = ev->Get()->Record.GetResponse().GetSessionId();

        SendToKqpValidationRequests(ctx);
    }

    void TTransactionActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        KAFKA_LOG_D("Received query response from KQP for " << GetAsStr(LastSentToKqpRequest) << " request");
        if (auto error = GetErrorFromYdbResponse(ev)) {
            KAFKA_LOG_W(error);
            SendFailResponse<TEndTxnResponseData>(EndTxnRequestPtr, EKafkaErrors::BROKER_NOT_AVAILABLE, error->data());
            Die(ctx);
            return;
        }

        switch (LastSentToKqpRequest) {
            case EKafkaTxnKqpRequests::SELECT:
                HandleSelectResponse(*ev->Get(), ctx);
                break;
            case EKafkaTxnKqpRequests::ADD_KAFKA_OPERATIONS_TO_TXN:
                HandleAddKafkaOperationsResponse(ev->Get()->Record.GetResponse().GetTxMeta().id(), ctx);
                break;
            case EKafkaTxnKqpRequests::COMMIT:
                HandleCommitResponse(ctx);
                break;
            default:
                Y_FAIL("Unexpected KQP request");
        }
    }

    void TTransactionActor::StartKqpSession(const TActorContext& ctx) {
        Kqp = std::make_unique<TKqpTxHelper>(AppData(ctx)->TenantName);
        KAFKA_LOG_D("Sending create session request to KQP for database " << DatabasePath);
        Kqp->SendCreateSessionRequest(ctx);
    }

    void TTransactionActor::SendToKqpValidationRequests(const TActorContext& ctx) {
        KAFKA_LOG_D("Sending select request to KQP for database " << DatabasePath);
        Kqp->SendYqlRequest(
            GetYqlWithTablesNames(),
            BuildSelectParams(),
            ++KqpCookie,
            ctx,
            false
        );

        LastSentToKqpRequest = EKafkaTxnKqpRequests::SELECT;
    }

    void TTransactionActor::SendAddKafkaOperationsToTxRequest(const TString& kqpTransactionId) {
        auto request = BuildAddKafkaOperationsRequest(kqpTransactionId);

        Send(MakeKqpProxyID(SelfId().NodeId()), request.Release(), 0, ++KqpCookie);

        LastSentToKqpRequest = EKafkaTxnKqpRequests::ADD_KAFKA_OPERATIONS_TO_TXN;
    }

    // Response senders
    template<class ErrorResponseType, class EventType>
    void TTransactionActor::SendFailResponse(TAutoPtr<TEventHandle<EventType>>& evHandle, EKafkaErrors errorCode, const TString& errorMessage) {
        if (errorMessage) {
            KAFKA_LOG_W("Sending fail response with error code: " << errorCode << ". Reason:  " << errorMessage);
        } else {
            KAFKA_LOG_W("Sending fail response with error code: " << errorCode);
        }

        auto response = NKafkaTransactions::BuildResponse<ErrorResponseType>(evHandle->Get()->Request, errorCode);
        Send(evHandle->Get()->ConnectionId, new TEvKafka::TEvResponse(evHandle->Get()->CorrelationId, response, errorCode));
    }

    template<class ResponseType, class EventType>
    void TTransactionActor::SendOkResponse(TAutoPtr<TEventHandle<EventType>>& evHandle) {
        auto& kafkaRequest = evHandle->Get()->Request;
        KAFKA_LOG_D("Sending OK response to " << evHandle->Get()->ConnectionId << " with correlationId " << evHandle->Get()->CorrelationId << " and transactionalId " << TransactionalId);
        std::shared_ptr<ResponseType> response = NKafkaTransactions::BuildResponse<ResponseType>(kafkaRequest, EKafkaErrors::NONE_ERROR);
        Send(evHandle->Get()->ConnectionId, new TEvKafka::TEvResponse(evHandle->Get()->CorrelationId, response, EKafkaErrors::NONE_ERROR));
    }

    // helper methods
    void TTransactionActor::Die(const TActorContext &ctx) {
        KAFKA_LOG_D("Dying.");
        if (Kqp) {
            Kqp->CloseKqpSession(ctx);
        }
        Send(MakeTransactionsServiceID(SelfId().NodeId()), new TEvKafka::TEvTransactionActorDied(TransactionalId, ProducerInstanceId));
        TBase::Die(ctx);
    }

    bool TTransactionActor::TxnExpired() {
        return TAppData::TimeProvider->Now().MilliSeconds() - CreatedAt.MilliSeconds() > TxnTimeoutMs;
    }

    template<class EventType>
    bool TTransactionActor::ProducerInRequestIsValid(TMessagePtr<EventType> kafkaRequest) {
        return *kafkaRequest->TransactionalId == TransactionalId
            && kafkaRequest->ProducerId == ProducerInstanceId.Id
            && kafkaRequest->ProducerEpoch == ProducerInstanceId.Epoch;
    }

    TString TTransactionActor::GetFullTopicPath(const TString& topicName) {
        return NPersQueue::GetFullTopicPath(DatabasePath, topicName);
    }

    TString TTransactionActor::GetYqlWithTablesNames() {
        const TString& templateStr = OffsetsToCommit.empty() ?  NKafkaTransactionSql::SELECT_FOR_VALIDATION_WITHOUT_CONSUMERS : NKafkaTransactionSql::SELECT_FOR_VALIDATION_WITH_CONSUMERS;

        TString templateWithProducerStateTable = std::regex_replace(
            templateStr.c_str(),
            std::regex("<producer_state_table_name>"),
            NKikimr::NGRpcProxy::V1::TTransactionalProducersInitManager::GetInstant()->FormPathToResourceTable(ResourceDatabasePath).c_str()
        );

        if (!OffsetsToCommit.empty()) {
            return std::regex_replace(
                templateWithProducerStateTable.c_str(),
                std::regex("<consumer_state_table_name>"),
                NKikimr::NGRpcProxy::V1::TKafkaConsumerGroupsMetaInitManager::GetInstant()->FormPathToResourceTable(ResourceDatabasePath).c_str()
            );
        }

        return templateWithProducerStateTable;
    }

    NYdb::TParams TTransactionActor::BuildSelectParams() {
        NYdb::TParamsBuilder params;
        params.AddParam("$Database").Utf8(DatabasePath).Build();
        params.AddParam("$TransactionalId").Utf8(TransactionalId).Build();

        if (!OffsetsToCommit.empty()) {
            // select unique consumer group names
            std::unordered_set<TString> uniqueConsumerGroups;
            for (auto& [partition, commitDetails] : OffsetsToCommit) {
                uniqueConsumerGroups.emplace(commitDetails.ConsumerName);
            }

            // add unique consumer group names to request as params
            auto& consumerGroupsParamBuilder = params.AddParam("$ConsumerGroups").BeginList();
            for (auto& consumerGroupName : uniqueConsumerGroups) {
                consumerGroupsParamBuilder.AddListItem().Utf8(consumerGroupName);
            }
            consumerGroupsParamBuilder.EndList().Build();
        }

        return params.Build();
    }

    THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> TTransactionActor::BuildAddKafkaOperationsRequest(const TString& kqpTransactionId) {
        auto ev = MakeHolder<TEvKqp::TEvQueryRequest>();

        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_UNDEFINED);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_TOPIC);
        ev->Record.MutableRequest()->SetDatabase(DatabasePath);
        ev->Record.MutableRequest()->SetSessionId(KqpSessionId);

        ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(kqpTransactionId);

        ev->Record.MutableRequest()->SetUsePublicResponseDataFormat(true);

        auto* kafkaApiOperations = ev->Record.MutableRequest()->MutableKafkaApiOperations();
        kafkaApiOperations->set_producerid(ProducerInstanceId.Id);
        kafkaApiOperations->set_producerepoch(ProducerInstanceId.Epoch);

        for (auto& partition : PartitionsInTxn) {
            auto* partitionInRequest = kafkaApiOperations->AddPartitionsInTxn();
            partitionInRequest->set_topicpath(partition.TopicPath);
            partitionInRequest->set_partitionid(partition.PartitionId);
        }

        for (auto& [partition, offsetDetails] : OffsetsToCommit) {
            auto* offsetInRequest = kafkaApiOperations->AddOffsetsInTxn();
            offsetInRequest->set_topicpath(partition.TopicPath);
            offsetInRequest->set_partitionid(partition.PartitionId);
            offsetInRequest->set_consumername(offsetDetails.ConsumerName);
            offsetInRequest->set_offset(offsetDetails.Offset);
        }

        return ev;
    }

    void TTransactionActor::HandleSelectResponse(const NKqp::TEvKqp::TEvQueryResponse& response, const TActorContext& ctx) {
        // YDB should return exactly two result sets for two queries: to producer and consumer state tables
        int resultsSize = response.Record.GetResponse().GetYdbResults().size();
        int expectedResultsSize = OffsetsToCommit.empty() ? 1 : 2; // if there were no consumer in transactions we do not send request to consumer table
        if (expectedResultsSize != resultsSize) {
            TString error = TStringBuilder() << "KQP returned wrong number of result sets on SELECT query. Expected " << expectedResultsSize << ", got " << resultsSize << ".";
            KAFKA_LOG_W(error);
            SendFailResponse<TEndTxnResponseData>(EndTxnRequestPtr, EKafkaErrors::BROKER_NOT_AVAILABLE, error);
            Die(ctx);
            return;
        }

        // parse and validate producer state
        TMaybe<TProducerState> producerState;
        try {
            producerState = ParseProducerState(response);
        } catch (const yexception& y) {
            TString error = TStringBuilder() << "Error parsing producer state response from KQP. Reason: " << y.what();
            KAFKA_LOG_W(error);
            SendFailResponse<TEndTxnResponseData>(EndTxnRequestPtr, EKafkaErrors::BROKER_NOT_AVAILABLE, error);
            Die(ctx);
            return;
        }
        if (auto error = GetErrorInProducerState(producerState)) {
            KAFKA_LOG_W(error);
            SendFailResponse<TEndTxnResponseData>(EndTxnRequestPtr, EKafkaErrors::PRODUCER_FENCED, error->data());
            Die(ctx);
            return;
        }

        if (!OffsetsToCommit.empty()) {
            // parse and validate consumers
            std::unordered_map<TString, i32> consumerGenerationsByName = ParseConsumersGenerations(response);
            if (auto error = GetErrorInConsumersStates(consumerGenerationsByName)) {
                KAFKA_LOG_W(error);
                SendFailResponse<TEndTxnResponseData>(EndTxnRequestPtr, EKafkaErrors::PRODUCER_FENCED, error->data());
                Die(ctx);
                return;
            }
        }

        KAFKA_LOG_D("Validated producer and consumers states. Everything is alright, adding kafka operations to transaction.");
        auto kqpTxnId = response.Record.GetResponse().GetTxMeta().id();
        // finally everything is valid and we can add kafka operations to transaction and attempt to commit
        SendAddKafkaOperationsToTxRequest(kqpTxnId);
    }

    void TTransactionActor::HandleAddKafkaOperationsResponse(const TString& kqpTransactionId, const TActorContext& ctx) {
        KAFKA_LOG_D("Successfully added kafka operations to transaction. Committing transaction.");
        Kqp->SetTxId(kqpTransactionId);
        Kqp->CommitTx(++KqpCookie, ctx);
        LastSentToKqpRequest = EKafkaTxnKqpRequests::COMMIT;
    }

    void TTransactionActor::HandleCommitResponse(const TActorContext& ctx) {
        KAFKA_LOG_D("Successfully committed transaction. Sending ok and dying.");
        SendOkResponse<TEndTxnResponseData>(EndTxnRequestPtr);
        Die(ctx);
    }

    TMaybe<TString> TTransactionActor::GetErrorFromYdbResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        TStringBuilder builder = TStringBuilder() << "Received error on request to KQP. Last sent request: " << GetAsStr(LastSentToKqpRequest) << ". Reason: ";
        if (ev->Cookie != KqpCookie) {
            return builder << "Unexpected cookie in TEvQueryResponse. Expected KQP Cookie: " << KqpCookie << ", Actual: " << ev->Cookie << ".";
        } else if (ev->Get()->Record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            return builder << "Unexpected YDB status in TEvQueryResponse. Expected YDB SUCCESS status, Actual: " << ev->Get()->Record.GetYdbStatus() << ".";
        } else {
            return {};
        }
    }

    TMaybe<TProducerState> TTransactionActor::ParseProducerState(const NKqp::TEvKqp::TEvQueryResponse& response) {
        auto& resp = response.Record.GetResponse();

        NYdb::TResultSetParser parser(resp.GetYdbResults(NKafkaTransactionSql::PRODUCER_STATE_REQUEST_INDEX));

        // for this transactional id there is no rows
        if (parser.RowsCount() == 0) {
            return {};
        }
        // there are multiple rows for this transactional id. This is unexpected and should not happen
        else if (parser.RowsCount() > 1) {
            throw yexception() << "Request returned more than one row: " << resp.GetYdbResults().size();
        } else {
            parser.TryNextRow();

            TProducerState result;

            result.TransactionalId = parser.ColumnParser("transactional_id").GetUtf8();
            result.ProducerId = parser.ColumnParser("producer_id").GetInt64();
            result.ProducerEpoch = parser.ColumnParser("producer_epoch").GetInt16();

            return result;
        }
    }

    TMaybe<TString> TTransactionActor::GetErrorInProducerState(const TMaybe<TProducerState>& producerState) {
        if (!producerState) {
            return "No producer state found. May be it has expired";
        } else if (producerState->TransactionalId != TransactionalId || producerState->ProducerId != ProducerInstanceId.Id || producerState->ProducerEpoch != ProducerInstanceId.Epoch) {
            return TStringBuilder() << "Producer state in producers_state table is different from the one in this actor. Producer state in table: "
            << "transactionalId=" << producerState->TransactionalId
            << ", producerId=" << producerState->ProducerId
            << ", producerEpoch=" << producerState->ProducerEpoch;
        } else {
            return {};
        }
    }

    /**
    * Parses the response to extract consumer group generations.
    *
    * @param response The response object containing the result set from the YDB query.
    * @return A map where keys are consumer group names and values are their corresponding generations.
    */
    std::unordered_map<TString, i32> TTransactionActor::ParseConsumersGenerations(const NKqp::TEvKqp::TEvQueryResponse& response) {
        std::unordered_map<TString, i32> generationByConsumerName;

        NYdb::TResultSetParser parser(response.Record.GetResponse().GetYdbResults(NKafkaTransactionSql::CONSUMER_STATES_REQUEST_INDEX));
        while (parser.TryNextRow()) {
            TString consumerName = parser.ColumnParser("consumer_group").GetUtf8().c_str();
            i32 generation = IntegerCast<i32>(parser.ColumnParser("generation").GetUint64());
            generationByConsumerName.emplace(consumerName, generation);
        }

        return generationByConsumerName;
    }

    TMaybe<TString> TTransactionActor::GetErrorInConsumersStates(const std::unordered_map<TString, i32>& consumerGenerationByName) {
        TStringBuilder builder;
        bool foundError = false;
        for (auto& [topicPartition, offsetCommit] : OffsetsToCommit) {
            auto it = consumerGenerationByName.find(offsetCommit.ConsumerName);
            if (it != consumerGenerationByName.end()) {
                i32 consumerGenerationInTable = it->second;
                if (consumerGenerationInTable != offsetCommit.ConsumerGeneration) {
                    if (foundError) {
                        builder << ", ";
                    } else {
                        foundError = true;
                    }
                    builder << "Consumer " << offsetCommit.ConsumerName << " has different generation in table than requested in transaction. Generation in table: " << consumerGenerationInTable << ", Generation requested in txn: " << offsetCommit.ConsumerGeneration << ".";
                }
            } else {
                if (foundError) {
                    builder << ", ";
                } else {
                    foundError = true;
                }
                builder << "There is no consumer state in table for group " << offsetCommit.ConsumerName;
            }
        }

        if (foundError) {
            return builder;
        } else {
            return {};
        }
    }

    TString TTransactionActor::GetAsStr(EKafkaTxnKqpRequests request) {
        switch (request) {
        case SELECT:
            return "SELECT";
        case ADD_KAFKA_OPERATIONS_TO_TXN:
            return "ADD_KAFKA_OPERATIONS_TO_TXN";
        case COMMIT:
            return "COMMIT";
        case NO_REQUEST:
            return "NO_REQUEST";
        }
    }
} // namespace NKafka
