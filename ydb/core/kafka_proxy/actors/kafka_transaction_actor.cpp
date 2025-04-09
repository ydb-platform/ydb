#include "kafka_transaction_actor.h"
#include "kafka_transaction_actor_sql.cpp"
#include <ydb/core/kafka_proxy/kafka_transactions_coordinator.h>
#include <ydb/core/kafka_proxy/kafka_transactional_producers_initializers.h>
#include <ydb/core/kafka_proxy/kafka_consumer_groups_metadata_initializers.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <regex>

namespace NKafka {

    void TKafkaTransactionActor::Handle(TEvKafka::TEvAddPartitionsToTxnRequest::TPtr& ev, const TActorContext& ctx){
        KAFKA_LOG_D(TStringBuilder() << "Receieved ADD_PARTITIONS_TO_TXN request for transactionalId " << ev->Get()->Request->TransactionalId->c_str());
        if (!ProducerInRequestIsValid(ev->Get()->Request)) {
            SendInvalidTransactionActorStateResponse<TAddPartitionsToTxnResponseData>(ev);
            Die(ctx);
            return;
        }
        for (auto& topicInRequest : ev->Get()->Request->Topics) {
            for (auto& partitionInRequest : topicInRequest.Partitions) {
                PartitionsInTxn.insert({GetFullTopicPath(topicInRequest.Name->c_str()), partitionInRequest});
            }
        }
        SendOkResponse<TAddPartitionsToTxnResponseData>(ev);
    }

    void TKafkaTransactionActor::Handle(TEvKafka::TEvAddOffsetsToTxnRequest::TPtr& ev, const TActorContext& ctx) {
        KAFKA_LOG_D(TStringBuilder() << "Receieved ADD_OFFSETS_TO_TXN request for transactionalId " << ev->Get()->Request->TransactionalId->c_str());
        if (!ProducerInRequestIsValid(ev->Get()->Request)) {
            SendInvalidTransactionActorStateResponse<TAddPartitionsToTxnResponseData>(ev);
            Die(ctx);
            return;
        }
        SendOkResponse<TAddOffsetsToTxnResponseData>(ev);
    }

    void TKafkaTransactionActor::Handle(TEvKafka::TEvTxnOffsetCommitRequest::TPtr& ev, const TActorContext& ctx) {
        KAFKA_LOG_D(TStringBuilder() << "Receieved TXN_OFFSET_COMMIT request for transactionalId " << ev->Get()->Request->TransactionalId->c_str());
        if (!ProducerInRequestIsValid(ev->Get()->Request)) {
            SendInvalidTransactionActorStateResponse<TAddPartitionsToTxnResponseData>(ev);
            Die(ctx);
            return;
        }
        // save offsets for future use
        for (auto& topicInRequest : ev->Get()->Request->Topics) {
            for (auto& partitionInRequest : topicInRequest.Partitions) {
                TTopicPartition topicPartition = {GetFullTopicPath(topicInRequest.Name->c_str()), partitionInRequest.PartitionIndex};
                OffsetsToCommit[topicPartition] = TPartitionCommit{
                        .Partition = partitionInRequest.PartitionIndex,
                        .Offset = partitionInRequest.CommittedOffset,
                        .ConsumerName = ev->Get()->Request->GroupId->c_str(),
                        .ConsumerGeneration = ev->Get()->Request->GenerationId
                };
            }
        }
        SendOkResponse<TAddPartitionsToTxnResponseData>(ev);
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
    7. Die, close KQP session,
    */
    void TKafkaTransactionActor::Handle(TEvKafka::TEvEndTxnRequest::TPtr& ev, const TActorContext& ctx) {
        KAFKA_LOG_D(TStringBuilder() << "Receieved END_TXN request for transactionalId " << ev->Get()->Request->TransactionalId->c_str());
        if (!ProducerInRequestIsValid(ev->Get()->Request)) {
            SendInvalidTransactionActorStateResponse<TAddPartitionsToTxnResponseData>(ev);
            Die(ctx);
            return;
        }

        bool txnAborted = !ev->Get()->Request->Committed;
        if (txnAborted) {
            SendOkResponse<TEndTxnResponseData>(ev);
        } else {
            EndTxnRequestPtr = std::move(ev);
            StartKqpSession(ctx);
        }
    }

    void TKafkaTransactionActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        if (!Kqp->HandleCreateSessionResponse(ev, ctx)) {
            SendResponseFail<TEndTxnResponseData>(ev, EKafkaErrors::BROKER_NOT_AVAILABLE, "Failed to create KQP session");
            Die(ctx);
            return;
        }

        SendToKqpValidationRequests(ctx);
    }

    void TKafkaTransactionActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        if (auto error = GetErrorFromYdbResponse(ev)) {
            KAFKA_LOG_W(error);
            SendResponseFail<TEndTxnResponseData>(EndTxnRequestPtr, EKafkaErrors::BROKER_NOT_AVAILABLE, error->data());
            Die(ctx);
            return;
        }

        const auto& record = ev->Get()->Record;
        switch (LastSentToKqpRequest) {
            case EKafkaTxnKqpRequests::SELECT:
                HandleSelectResponse(record, ctx);
                break;
            case EKafkaTxnKqpRequests::COMMIT:
                HandleCommitResponse(ctx);
                break;
            default:
                Y_FAIL("Unexpected KQP request");
        }
    }

    void TKafkaTransactionActor::StartKqpSession(const TActorContext& ctx) {
        Kqp = std::make_unique<TKqpTxHelper>(DatabasePath);
        KAFKA_LOG_D("Sending create session request to KQP.");
        Kqp->SendCreateSessionRequest(ctx);
    }

    void TKafkaTransactionActor::SendToKqpValidationRequests(const TActorContext& ctx) {
        Kqp->SendYqlRequest(
            GetYqlWithTablesNames(NKafkaTransactionSql::SELECT_FOR_VALIDATION), 
            BuildSelectParams(), 
            ++KqpCookie, 
            ctx
        );
        
        LastSentToKqpRequest = EKafkaTxnKqpRequests::SELECT;
    }

    void TKafkaTransactionActor::SendCommitTxnRequest(const TActorContext& ctx) {
        auto request = BuildCommitTxnRequestToKqp();

        ctx.Send(MakeKqpProxyID(ctx.SelfID.NodeId()), request.Release(), 0, ++KqpCookie);
        
        LastSentToKqpRequest = EKafkaTxnKqpRequests::COMMIT;
    }
    
    // Response senders
    template<class ErrorResponseType, class EventType>
    void TKafkaTransactionActor::SendInvalidTransactionActorStateResponse(TAutoPtr<TEventHandle<EventType>>& evHandle) {
        auto kafkaRequest = evHandle->Get()->Request;
        KAFKA_LOG_CRIT(TStringBuilder() << "Recieved invalid request. Got: "
            << "transactionalId=" << kafkaRequest->TransactionalId->c_str() 
            << ", producerId=" << kafkaRequest->ProducerId 
            << ", producerEpoch=" << kafkaRequest->ProducerEpoch
        );
        std::shared_ptr<ErrorResponseType> response = ResponseBuilder.Build<ErrorResponseType>(kafkaRequest, EKafkaErrors::UNKNOWN_SERVER_ERROR);
        Send(evHandle->Get()->ConnectionId, new TEvKafka::TEvResponse(evHandle->Get()->CorrelationId, response, EKafkaErrors::UNKNOWN_SERVER_ERROR));
    }

    template<class ErrorResponseType, class EventType>
    void TKafkaTransactionActor::SendResponseFail(TAutoPtr<TEventHandle<EventType>>& evHandle, EKafkaErrors errorCode, const TString& errorMessage) {
        if (errorMessage) {
            KAFKA_LOG_W(TStringBuilder() << "Sending fail response with error code: " << errorCode << ". Reason:  " << errorMessage);
        }

        std::shared_ptr<ErrorResponseType> response = ResponseBuilder.Build<ErrorResponseType>(evHandle->Get()->Request, errorCode);
        Send(evHandle->Get()->ConnectionId, new TEvKafka::TEvResponse(evHandle->Get()->CorrelationId, response, errorCode));
    }

    template<class ResponseType, class EventType>
    void TKafkaTransactionActor::SendOkResponse(TAutoPtr<TEventHandle<EventType>>& evHandle) {
        auto kafkaRequest = evHandle->Get()->Request;
        KAFKA_LOG_D(TStringBuilder() << "Sending OK response to " << evHandle->Get()->ConnectionId << " with correlationId " << evHandle->Get()->CorrelationId << " and transactionalId " << TransactionalId);
        std::shared_ptr<ResponseType> response = ResponseBuilder.Build<ResponseType>(kafkaRequest, EKafkaErrors::NONE_ERROR);
        Send(evHandle->Get()->ConnectionId, new TEvKafka::TEvResponse(evHandle->Get()->CorrelationId, response, EKafkaErrors::NONE_ERROR));
    }

    // helper methods
    void TKafkaTransactionActor::Die(const TActorContext &ctx) {
        KAFKA_LOG_D(TStringBuilder() << "Dying. TransactionalId: " << TransactionalId << ", ProducerId: " << ProducerId << ", ProducerEpoch: " << ProducerEpoch);
        if (Kqp) {
            Kqp->CloseKqpSession(ctx);
        }
        Send(MakeKafkaTransactionsServiceID(), new TEvKafka::TEvTransactionActorDied(TransactionalId, ProducerId, ProducerEpoch));
        TBase::Die(ctx);
    }

    template<class EventType>
    bool TKafkaTransactionActor::ProducerInRequestIsValid(TMessagePtr<EventType> kafkaRequest) {
        return kafkaRequest->TransactionalId->c_str() == TransactionalId 
            && kafkaRequest->ProducerId == ProducerId 
            && kafkaRequest->ProducerEpoch == ProducerEpoch;
    }

    TString TKafkaTransactionActor::GetFullTopicPath(const TString& topicName) {
        return NPersQueue::GetFullTopicPath(DatabasePath, topicName);
    }

    TString TKafkaTransactionActor::GetYqlWithTablesNames(const TString& templateStr) {
        TString templateWithProducerStateTable = std::regex_replace(
            templateStr.c_str(),
            std::regex("<producer_state_table_name>"), 
            NKikimr::NGRpcProxy::V1::TTransactionalProducersInitManager::GetInstant()->GetStorageTablePath().c_str()
        );
        TString templateWithConsumerStateTable = std::regex_replace(
            templateWithProducerStateTable.c_str(),
            std::regex("<consumer_state_table_name>"), 
            NKikimr::NGRpcProxy::V1::TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()
        );

        return templateWithConsumerStateTable;
    }

    NYdb::TParams TKafkaTransactionActor::BuildSelectParams() {
        NYdb::TParamsBuilder params;
        params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
        params.AddParam("$TransactionalId").Utf8(TransactionalId).Build();
        
        // select unique consumer group names
        std::unordered_set<TString> uniqueConsumerGroups;
        for (auto& [partition, commitDetails] : OffsetsToCommit) {
            uniqueConsumerGroups.insert(commitDetails.ConsumerName);
        }

        // add unique consumer group names to request as params
        auto& consumerGroupsParamBuilder = params.AddParam("$ConsumerGroups").BeginList();
        for (auto& consumerGroupName : uniqueConsumerGroups) {
            consumerGroupsParamBuilder.AddListItem().Utf8(consumerGroupName).Build();
        }
        consumerGroupsParamBuilder.EndList().Build();

        return params.Build();
    }

    THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> TKafkaTransactionActor::BuildCommitTxnRequestToKqp() {
        auto ev = MakeHolder<TEvKqp::TEvQueryRequest>();

        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_UNDEFINED);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_TOPIC);
        ev->Record.MutableRequest()->SetDatabase(DatabasePath);
        ev->Record.MutableRequest()->SetSessionId(KqpSessionId);

        ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        ev->Record.MutableRequest()->MutableTxControl()->set_tx_id(KqpTxnId);

        ev->Record.MutableRequest()->SetUsePublicResponseDataFormat(true);

        auto* kafkaApiOperations = ev->Record.MutableRequest()->MutableKafkaApiOperations();
        kafkaApiOperations->set_transactionalid(TransactionalId);
        kafkaApiOperations->set_producerid(ProducerId);
        kafkaApiOperations->set_producerepoch(ProducerEpoch);

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
            offsetInRequest->set_consumergeneration(offsetDetails.ConsumerGeneration);
            offsetInRequest->set_offset(offsetDetails.Offset);
        }
        
        return ev;
    }

    void TKafkaTransactionActor::HandleSelectResponse(const NKikimrKqp::TEvQueryResponse& response, const TActorContext& ctx) {
        // YDB should return exactly two result sets for two queries: to producer and consumer state tables
        if (response.GetResponse().GetYdbResults().size() != 2) {
            TString error = TStringBuilder() << "KQP returned wrong number of result sets on SELECT query. Expected 2, got " << response.GetResponse().GetYdbResults().size() << ".";
            KAFKA_LOG_W(error);
            SendResponseFail<TEndTxnResponseData>(EndTxnRequestPtr, EKafkaErrors::BROKER_NOT_AVAILABLE, error);
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
            SendResponseFail<TEndTxnResponseData>(EndTxnRequestPtr, EKafkaErrors::BROKER_NOT_AVAILABLE, error);
            Die(ctx);
            return;
        }
        if (auto error = GetErrorInProducerState(producerState)) {
            KAFKA_LOG_W(error);
            SendResponseFail<TEndTxnResponseData>(EndTxnRequestPtr, EKafkaErrors::PRODUCER_FENCED, error->data());
            Die(ctx);
            return;
        }
        

        // parse and validate consumers
        std::unordered_map<TString, i32> consumerGenerationsByName = ParseConsumersGenerations(response);
        if (auto error = GetErrorInConsumersStates(consumerGenerationsByName)) {
            KAFKA_LOG_W(error);
            SendResponseFail<TEndTxnResponseData>(EndTxnRequestPtr, EKafkaErrors::PRODUCER_FENCED, error->data());
            Die(ctx);
            return;
        }

        KqpTxnId = response.GetResponse().GetTxMeta().id();
        // finally everything is valid and we can attempt to commit
        SendCommitTxnRequest(ctx);
    }

    void TKafkaTransactionActor::HandleCommitResponse(const TActorContext& ctx) {
        KAFKA_LOG_D("Successfully committed transaction. Sending ok and dying.");
        SendOkResponse<TEndTxnResponseData>(EndTxnRequestPtr);
        Die(ctx);
    }

    TMaybe<TString> TKafkaTransactionActor::GetErrorFromYdbResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        TStringBuilder builder = TStringBuilder() << "Recieved error on request to KQP. Last sent request: " << GetAsStr(LastSentToKqpRequest) << ". Reason: ";
        if (ev->Cookie != KqpCookie) {
            return builder << "Unexpected cookie in TEvQueryResponse. Expected KQP Cookie: " << KqpCookie << ", Actual: " << ev->Cookie << ".";
        } else if (ev->Get()->Record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            return builder << "Unexpected YDB status in TEvQueryResponse. Expected YDB SUCCESS status, Actual: " << ev->Get()->Record.GetYdbStatus() << ".";
        } else {
            return {};
        }
    }

    TMaybe<TProducerState> TKafkaTransactionActor::ParseProducerState(const NKikimrKqp::TEvQueryResponse& response) {
        auto& resp = response.GetResponse();

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

    TMaybe<TString> TKafkaTransactionActor::GetErrorInProducerState(const TMaybe<TProducerState>& producerState) {
        if (!producerState) {
            return "No producer state found. May be it has expired";
        } else if (producerState->TransactionalId != TransactionalId || producerState->ProducerId != ProducerId || producerState->ProducerEpoch != ProducerEpoch) {
            return TStringBuilder() << "Producer state in producers_state table is different from the one in this actor. Producer state in table: "
            << "transactionalId=" << producerState->TransactionalId 
            << ", producerId=" << producerState->ProducerId
            << ", producerEpoch=" << producerState->ProducerEpoch;
        } else {
            return {};
        }
    }

    std::unordered_map<TString, i32> TKafkaTransactionActor::ParseConsumersGenerations(const NKikimrKqp::TEvQueryResponse& response) {
        std::unordered_map<TString, i32> generationByConsumerName;
    
        NYdb::TResultSetParser parser(response.GetResponse().GetYdbResults(NKafkaTransactionSql::CONSUMER_STATES_REQUEST_INDEX));
        while (parser.TryNextRow()) {
            TString consumerName = parser.ColumnParser("consumer_group").GetUtf8().c_str();;
            i32 generation = (i32)parser.ColumnParser("generation").GetUint64();
            generationByConsumerName[consumerName] = generation;
        }
    
        return generationByConsumerName;
    }

    TMaybe<TString> TKafkaTransactionActor::GetErrorInConsumersStates(const std::unordered_map<TString, i32>& consumerGenerationByName) {
        TStringBuilder builder;
        bool foundError = false;
        for (auto& [topicPartition, offsetCommit] : OffsetsToCommit) {
            if (consumerGenerationByName.contains(offsetCommit.ConsumerName)) {
                i32 consumerGenerationInTable = consumerGenerationByName.at(offsetCommit.ConsumerName);
                if (consumerGenerationInTable != offsetCommit.ConsumerGeneration) {
                    foundError = true;
                    builder << "Consumer " << offsetCommit.ConsumerName << " has different generation in table than requested in transaction. Generation in table: " << consumerGenerationInTable << ", Generation requested in txn: " << offsetCommit.ConsumerGeneration << ".";
                }
            } else {
                foundError = true;
                builder << "There is no consumer state in table for group " << offsetCommit.ConsumerName;
            }
        }

        if (foundError) {
            return builder;
        } else {
            return {};
        }
    }

    TString TKafkaTransactionActor::GetAsStr(EKafkaTxnKqpRequests request) {
        switch (request) {
        case SELECT:
            return "SELECT";
        case COMMIT:
            return "SELECT";
        case NO_REQUEST:
            return "NO_REQUEST";
        }
    }
} // namespace NKafka