#include "kafka_transaction_actor.h"
#include "kafka_transaction_actor_sql.cpp"
#include <ydb/core/kafka_proxy/kafka_transactions_coordinator.h>
#include <ydb/core/kafka_proxy/kafka_transactional_producers_initializers.h>
#include <ydb/core/kafka_proxy/kafka_consumer_groups_metadata_initializers.h>

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
        if (auto error = GetErrorFromYdbResponse()) {
            KAFKA_LOG_W(error);
            SendResponseFail<TEndTxnResponseData>(ev, EKafkaErrors::BROKER_NOT_AVAILABLE, error);
            Die(ctx);
            return;
        }

        const auto& record = ev->Get()->Record;
        switch (LastSentToKqpRequest) {
            case EKafkaTxnKqpRequests::SELECT:
                HandleSelectResponse(record, ctx);
                break;
            case EKafkaTxnKqpRequests::COMMIT:
                HandleCommitResponse(record);
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
        // Kqp->SendYqlRequest(
        //     GetYqlWithTablesNames(NKafkaTransactionSql::SELECT_FOR_VALIDATION), 
        //     BuildSelectParams(), 
        //     ++KqpCookie, 
        //     ctx
        // );
        
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
    void TKafkaTransactionActor::SendResponseFail(TAutoPtr<TEventHandle<EventType>>& evHandle, EKafkaErrors errorCode, const TString& errorMessage = {}) {
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

    THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> TKafkaTransactionActor::BuildCommitTxnRequestToKqp() {
        
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
            templateWithProducerStateTable,
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

    void TKafkaTransactionActor::HandleSelectResponse(const NKikimrKqp::TEvQueryResponse& response, const TActorContext& ctx) {
        ValidateResponse(response);
        auto producerState = KqpResponseParser.ParseProducerState(response);
        ValidateProducerStateBeforeCommit(producerState);
        auto consumersStates = KqpResponseParser.ParseConsumersStates(response);
        ValidateConsumersStatesBeforeCommit(producerState);
        SendCommitTxnRequest(ctx);
    }

    void TKafkaTransactionActor::HandleCommitResponse(const NKikimrKqp::TEvQueryResponse& response) {
        SendOkResponse<TEndTxnResponseData>(EndTxnRequest); // ToDo: save EndTxnRequest in previous steps
    }

    template<class EventType>
    bool TKafkaTransactionActor::ProducerInRequestIsValid(TMessagePtr<EventType> kafkaRequest) {
        return kafkaRequest->TransactionalId->c_str() == TransactionalId 
            && kafkaRequest->ProducerId == ProducerId 
            && kafkaRequest->ProducerEpoch == ProducerEpoch;
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

    bool TKafkaTransactionActor::ResponseHasValidNumberOfResultsSets(const NKikimrKqp::TEvQueryResponse& response) {

    }

    TString TKafkaTransactionActor::GetErrorInProducerState(const TProducerState& producerState) {

    }

    TString TKafkaTransactionActor::GetErrorInConsumersState(const std::vector<TConsumerState>& consumerState) {

    }

    bool TKafkaTransactionActor::ValidateProducerStateBeforeCommit(const TProducerState& response) {

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