#include "kafka_transactions_coordinator.h"
#include "actors/kafka_transaction_actor.h"

namespace NKafka {
    // Handles new transactional_id+producer_id+producer_epoch: 
    // 1. validates that producer is not a zombie (in case of parallel init_producer_requests)
    // 2. saves transactional_id+producer_id+producer_epoch for validation of future transactional requests
    void TKafkaTransactionsCoordinator::Handle(TEvKafka::TEvSaveTxnProducerRequest::TPtr& ev, const TActorContext& ctx){
        auto request = ev->Get();

        if (ProducersByTransactionalId.contains(request->TransactionalId)) {
            auto currentProducerState = ProducersByTransactionalId[request->TransactionalId];
            auto newProducerState = TProducerState(request->ProducerId, request->ProducerEpoch);

            if (NewProducerStateIsOutdated(currentProducerState, newProducerState)) {
                TString message = GetProducerIsOutdatedError(request->TransactionalId, currentProducerState, newProducerState);
                ctx.Send(ev->Sender, new TEvKafka::TEvSaveTxnProducerResponse(TEvKafka::TEvSaveTxnProducerResponse::EStatus::PRODUCER_FENCED, message));
                return;
            } 

            currentProducerState.ProducerId = request->ProducerId;
            currentProducerState.ProducerEpoch = request->ProducerEpoch;
        } else {
            ProducersByTransactionalId[request->TransactionalId] = TProducerState(request->ProducerId, request->ProducerEpoch);
        }
        
        ctx.Send(ev->Sender, new TEvKafka::TEvSaveTxnProducerResponse(TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK, ""));
    };

    void TKafkaTransactionsCoordinator::Handle(TEvKafka::TEvAddPartitionsToTxnRequest::TPtr& ev, const TActorContext& ctx){
        HandleTransacationalRequest<TAddPartitionsToTxnResponseData>(ev, ctx);
    };

    void TKafkaTransactionsCoordinator::Handle(TEvKafka::TEvAddOffsetsToTxnRequest::TPtr& ev, const TActorContext& ctx){
        HandleTransacationalRequest<TAddOffsetsToTxnResponseData>(ev, ctx);
    };

    void TKafkaTransactionsCoordinator::Handle(TEvKafka::TEvTxnOffsetCommitRequest::TPtr& ev, const TActorContext& ctx) {
        HandleTransacationalRequest<TTxnOffsetCommitResponseData>(ev, ctx);
    };

    void TKafkaTransactionsCoordinator::Handle(TEvKafka::TEvEndTxnRequest::TPtr& ev, const TActorContext& ctx) {
        HandleTransacationalRequest<TEndTxnResponseData>(ev, ctx);
    };

    // Validates producer's id and epoch
    // If valid: proxies requests to the relevant TKafkaTransactionActor
    // If outdated or not initialized: returns PRODUCER_FENCED error
    template<class ErrorResponseType, class EventType>
    void TKafkaTransactionsCoordinator::HandleTransacationalRequest(TAutoPtr<TEventHandle<EventType>>& evHandle, const TActorContext& ctx) {
        auto ev = evHandle->Get();
        KAFKA_LOG_D(TStringBuilder() << "Receieved message for transactionalId " << ev->Request->TransactionalId->c_str() << " and ApiKey " << ev->Request->ApiKey());
        
        // create helper struct to simplify methods interaction
        auto txnRequest = TTransactionalRequest(
            ev->Request->TransactionalId->c_str(),
            TProducerState(ev->Request->ProducerId, ev->Request->ProducerEpoch),
            ev->CorrelationId,
            ev->ConnectionId
        );
        if (auto error = GetTxnRequestError(txnRequest)) {
            SendProducerFencedResponse<ErrorResponseType>(ev->Request, error->data(), txnRequest);
        } else {
            ForwardToTransactionActor(evHandle, ctx);
        }
    };

    template<class ErrorResponseType, class RequestType>
    void TKafkaTransactionsCoordinator::SendProducerFencedResponse(TMessagePtr<RequestType> kafkaRequest, const TString& error, const TTransactionalRequest& txnRequestDetails) {
        KAFKA_LOG_ERROR(error);
        auto response = BuildProducerFencedResponse<ErrorResponseType>(kafkaRequest);
        Send(txnRequestDetails.ConnectionId, new TEvKafka::TEvResponse(txnRequestDetails.CorrelationId, response, EKafkaErrors::PRODUCER_FENCED));
    };

    template<class EventType> 
    void TKafkaTransactionsCoordinator::ForwardToTransactionActor(TAutoPtr<TEventHandle<EventType>>& evHandle, const TActorContext& ctx) {
        auto ev = evHandle->Get();
        
        TActorId txnActorId;
        if (TxnActorByTransactionalId.contains(ev->Request->TransactionalId->c_str())) {
            txnActorId = TxnActorByTransactionalId[ev->Request->TransactionalId->c_str()];
        } else {
            txnActorId = ctx.Register(new TKafkaTransactionActor());
            TxnActorByTransactionalId[ev->Request->TransactionalId->c_str()] = txnActorId;
            KAFKA_LOG_D(TStringBuilder() << "Registered TKafkaTransactionActor with id " << txnActorId << " for transactionalId " << ev->Request->TransactionalId->c_str() << " and ApiKey " << ev->Request->ApiKey());
        }
        TAutoPtr<IEventHandle> tmpPtr = evHandle.Release();
        ctx.Forward(tmpPtr, txnActorId);
        KAFKA_LOG_D(TStringBuilder() << "Forwarded message to TKafkaTransactionActor with id " << txnActorId << " for transactionalId " << ev->Request->TransactionalId->c_str() << " and ApiKey " << ev->Request->ApiKey());
    };

    template<class ResponseType, class RequestType>
    std::shared_ptr<ResponseType> TKafkaTransactionsCoordinator::BuildProducerFencedResponse(TMessagePtr<RequestType> request) {
        Y_UNUSED(request); // used in other template functions
        auto response = std::make_shared<ResponseType>();
        response->ErrorCode = EKafkaErrors::PRODUCER_FENCED;
        return response;
    };

    template<>
    std::shared_ptr<TAddPartitionsToTxnResponseData> TKafkaTransactionsCoordinator::BuildProducerFencedResponse<TAddPartitionsToTxnResponseData, TAddPartitionsToTxnRequestData>(TMessagePtr<TAddPartitionsToTxnRequestData> request) {
        auto response = std::make_shared<TAddPartitionsToTxnResponseData>();
        std::vector<TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult> topicsResponse;
        topicsResponse.reserve(request->Topics.size());
        for (auto requestTopic : request->Topics) {
            TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult topicInResponse;
            topicInResponse.Name = requestTopic.Name;
            topicInResponse.Results.reserve(requestTopic.Partitions.size());
            for (auto requestPartition : requestTopic.Partitions) {
                TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult partitionInResponse;
                partitionInResponse.PartitionIndex = requestPartition;
                partitionInResponse.ErrorCode = EKafkaErrors::PRODUCER_FENCED;
                topicInResponse.Results.push_back(partitionInResponse);
            }
            topicsResponse.push_back(topicInResponse);
        }
        response->Results = topicsResponse;
        return response;
    };

    template<>
    std::shared_ptr<TTxnOffsetCommitResponseData> TKafkaTransactionsCoordinator::BuildProducerFencedResponse<TTxnOffsetCommitResponseData, TTxnOffsetCommitRequestData>(TMessagePtr<TTxnOffsetCommitRequestData> request) {
        auto response = std::make_shared<TTxnOffsetCommitResponseData>();
        std::vector<TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic> topicsResponse;
        topicsResponse.reserve(request->Topics.size());
        for (auto requestTopic : request->Topics) {
            TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic topicInResponse;
            topicInResponse.Name = requestTopic.Name;
            topicInResponse.Partitions.reserve(requestTopic.Partitions.size());
            for (auto requestPartition : requestTopic.Partitions) {
                TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition partitionInResponse;
                partitionInResponse.PartitionIndex = requestPartition.PartitionIndex;
                partitionInResponse.ErrorCode = EKafkaErrors::PRODUCER_FENCED;
                topicInResponse.Partitions.push_back(partitionInResponse);
            }
            topicsResponse.push_back(topicInResponse);
        }
        response->Topics = topicsResponse;
        return response;
    };

    std::shared_ptr<TAddPartitionsToTxnResponseData> TKafkaTransactionsCoordinator::BuildProducerFencedResponseForAddPartitions(TMessagePtr<TAddPartitionsToTxnRequestData> request) {
        auto response = std::make_shared<TAddPartitionsToTxnResponseData>();
        std::vector<TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult> topicsResponse;
        topicsResponse.reserve(request->Topics.size());
        for (auto requestTopic : request->Topics) {
            TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult topicInResponse;
            topicInResponse.Name = requestTopic.Name;
            topicInResponse.Results.reserve(requestTopic.Partitions.size());
            for (auto requestPartition : requestTopic.Partitions) {
                TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult partitionInResponse;
                partitionInResponse.PartitionIndex = requestPartition;
                partitionInResponse.ErrorCode = EKafkaErrors::PRODUCER_FENCED;
                topicInResponse.Results.push_back(partitionInResponse);
            }
            topicsResponse.push_back(topicInResponse);
        }
        response->Results = topicsResponse;
        return response;
    };

    std::shared_ptr<TTxnOffsetCommitResponseData> TKafkaTransactionsCoordinator::BuildProducerFencedResponseForTxnOffsetCommit(const TTxnOffsetCommitRequestData* const request) {
        auto response = std::make_shared<TTxnOffsetCommitResponseData>();
        std::vector<TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic> topicsResponse;
        topicsResponse.reserve(request->Topics.size());
        for (auto requestTopic : request->Topics) {
            TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic topicInResponse;
            topicInResponse.Name = requestTopic.Name;
            topicInResponse.Partitions.reserve(requestTopic.Partitions.size());
            for (auto requestPartition : requestTopic.Partitions) {
                TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition partitionInResponse;
                partitionInResponse.PartitionIndex = requestPartition.PartitionIndex;
                partitionInResponse.ErrorCode = EKafkaErrors::PRODUCER_FENCED;
                topicInResponse.Partitions.push_back(partitionInResponse);
            }
            topicsResponse.push_back(topicInResponse);
        }
        response->Topics = topicsResponse;
        return response;
    };

    bool TKafkaTransactionsCoordinator::NewProducerStateIsOutdated(const TProducerState& currentProducerState, const TProducerState& newProducerState) {
        bool producerIdAlreadyGreater = currentProducerState.ProducerId > newProducerState.ProducerId;
        bool producerIdsAreEqual = currentProducerState.ProducerId == newProducerState.ProducerId;
        bool epochAlreadyGreater = currentProducerState.ProducerEpoch > newProducerState.ProducerEpoch;
        return producerIdAlreadyGreater || (producerIdsAreEqual && epochAlreadyGreater);
    };

    TMaybe<TString> TKafkaTransactionsCoordinator::GetTxnRequestError(const TTransactionalRequest& request) {
        if (!ProducersByTransactionalId.contains(request.TransactionalId)) {
            return TStringBuilder() << "Producer with transactional id " << request.TransactionalId << " was not yet initailized.";
        } else if (NewProducerStateIsOutdated(ProducersByTransactionalId[request.TransactionalId], request.ProducerState)) {
            return GetProducerIsOutdatedError(request.TransactionalId, ProducersByTransactionalId[request.TransactionalId], request.ProducerState);
        } else {
            return {};
        }
    };

    TString TKafkaTransactionsCoordinator::GetProducerIsOutdatedError(const TString& transactionalId, const TProducerState& currentProducerState, const TProducerState& newProducerState) {
        return TStringBuilder() << "Producer with transactional id " << transactionalId <<
                    "is outdated. Current producer id is " << currentProducerState.ProducerId << 
                    " and producer epoch is " << currentProducerState.ProducerEpoch << ". Requested producer id is " << newProducerState.ProducerId << 
                    " and producer epoch is " << newProducerState.ProducerEpoch << ".";
    }

    NActors::IActor* CreateKafkaTransactionsCoordinator() {
        return new TKafkaTransactionsCoordinator();
    };


} // namespace NKafka