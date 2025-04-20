#include "kafka_transactions_coordinator.h"
#include "actors/kafka_transaction_actor.h"
#include "actors/txn_actor_response_builder.h"
#include <ydb/core/kqp/common/simple/services.h>

namespace NKafka {
    // Handles new transactional_id+producer_id+producer_epoch: 
    // 1. validates that producer is not a zombie (in case of parallel init_producer_requests)
    // 2. saves transactional_id+producer_id+producer_epoch for validation of future transactional requests
    void TKafkaTransactionsCoordinator::Handle(TEvKafka::TEvSaveTxnProducerRequest::TPtr& ev, const TActorContext& ctx){
        TEvKafka::TEvSaveTxnProducerRequest* request = ev->Get();

        auto it = ProducersByTransactionalId.find(request->TransactionalId);
        if (it != ProducersByTransactionalId.end()) {
            TProducerState& currentProducerState = it->second;
            TProducerState newProducerState = TProducerState(request->ProducerId, request->ProducerEpoch);

            if (NewProducerStateIsOutdated(currentProducerState, newProducerState)) {
                TString message = GetProducerIsOutdatedError(request->TransactionalId, currentProducerState, newProducerState);
                ctx.Send(ev->Sender, new TEvKafka::TEvSaveTxnProducerResponse(TEvKafka::TEvSaveTxnProducerResponse::EStatus::PRODUCER_FENCED, message));
                return;
            } 

            currentProducerState.Id = request->ProducerId;
            currentProducerState.Epoch = request->ProducerEpoch;
        } else {
            ProducersByTransactionalId[request->TransactionalId] = TProducerState(request->ProducerId, request->ProducerEpoch);
        }
        
        ctx.Send(ev->Sender, new TEvKafka::TEvSaveTxnProducerResponse(TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK, ""));
    };

    void TKafkaTransactionsCoordinator::Handle(TEvKafka::TEvAddPartitionsToTxnRequest::TPtr& ev, const TActorContext& ctx){
        HandleTransactionalRequest<TAddPartitionsToTxnResponseData>(ev, ctx);
    };

    void TKafkaTransactionsCoordinator::Handle(TEvKafka::TEvAddOffsetsToTxnRequest::TPtr& ev, const TActorContext& ctx){
        HandleTransactionalRequest<TAddOffsetsToTxnResponseData>(ev, ctx);
    };

    void TKafkaTransactionsCoordinator::Handle(TEvKafka::TEvTxnOffsetCommitRequest::TPtr& ev, const TActorContext& ctx) {
        HandleTransactionalRequest<TTxnOffsetCommitResponseData>(ev, ctx);
    };

    void TKafkaTransactionsCoordinator::Handle(TEvKafka::TEvEndTxnRequest::TPtr& ev, const TActorContext& ctx) {
        HandleTransactionalRequest<TEndTxnResponseData>(ev, ctx);
    };

    void TKafkaTransactionsCoordinator::Handle(TEvKafka::TEvTransactionActorDied::TPtr& ev, const TActorContext&) {
        auto it = ProducersByTransactionalId.find(ev->Get()->TransactionalId);
        TProducerState currentProducerState = ProducersByTransactionalId[ev->Get()->TransactionalId];

        if (it == ProducersByTransactionalId.end() || it->second != currentProducerState) {
            // new actor was already registered, we can just ignore this event
            KAFKA_LOG_D(TStringBuilder() << "Received TEvTransactionActorDied for transactionalId " << ev->Get()->TransactionalId << " but producer has already been reinitialized with new epoch or deleted. Ignoring this event");
        } else {
            KAFKA_LOG_D(TStringBuilder() << "Received TEvTransactionActorDied for transactionalId " << ev->Get()->TransactionalId 
                << " and producerId " << ev->Get()->ProducerId 
                << " and producerEpoch " << ev->Get()->ProducerEpoch
                << ". Erasing info about this actor.");
            // erase info about actor to prevent future zombie requests from this producer
            TxnActorByTransactionalId.erase(ev->Get()->TransactionalId);
        }
    };

    void TKafkaTransactionsCoordinator::Handle(TEvents::TEvPoison::TPtr&, const TActorContext& ctx) {
        KAFKA_LOG_D("Got poison pill, killing all transaction actors");
        for (auto& [transactionalId, txnActorId]: TxnActorByTransactionalId) {
            ctx.Send(txnActorId, new TEvents::TEvPoison());
            KAFKA_LOG_D(TStringBuilder() << "Sent poison pill to transaction actor for transactionalId " << transactionalId);
        }
        PassAway();
    };

    void TKafkaTransactionsCoordinator::PassAway() {
        KAFKA_LOG_D("Killing myself");
        TBase::PassAway();
    };

    // Validates producer's id and epoch
    // If valid: proxies requests to the relevant TKafkaTransactionActor
    // If outdated or not initialized: returns PRODUCER_FENCED error
    template<class ErrorResponseType, class EventType>
    void TKafkaTransactionsCoordinator::HandleTransactionalRequest(TAutoPtr<TEventHandle<EventType>>& evHandle, const TActorContext& ctx) {
        EventType* ev = evHandle->Get();
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
        KAFKA_LOG_W(error);
        std::shared_ptr<ErrorResponseType> response = NKafkaTransactions::BuildResponse<ErrorResponseType>(kafkaRequest, EKafkaErrors::PRODUCER_FENCED);
        Send(txnRequestDetails.ConnectionId, new TEvKafka::TEvResponse(txnRequestDetails.CorrelationId, response, EKafkaErrors::PRODUCER_FENCED));
    };

    template<class EventType> 
    void TKafkaTransactionsCoordinator::ForwardToTransactionActor(TAutoPtr<TEventHandle<EventType>>& evHandle, const TActorContext& ctx) {
        EventType* ev = evHandle->Get();
        
        TActorId txnActorId;
        if (TxnActorByTransactionalId.contains(ev->Request->TransactionalId->c_str())) {
            txnActorId = TxnActorByTransactionalId[ev->Request->TransactionalId->c_str()];
        } else {
            txnActorId = ctx.Register(new TKafkaTransactionActor(ev->Request->TransactionalId->c_str(), ev->Request->ProducerId, ev->Request->ProducerEpoch, ev->DatabasePath, NKikimr::NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ctx.SelfID));
            TxnActorByTransactionalId[ev->Request->TransactionalId->c_str()] = txnActorId;
            KAFKA_LOG_D(TStringBuilder() << "Registered TKafkaTransactionActor with id " << txnActorId << " for transactionalId " << ev->Request->TransactionalId->c_str() << " and ApiKey " << ev->Request->ApiKey());
        }
        TAutoPtr<IEventHandle> tmpPtr = evHandle.Release();
        ctx.Forward(tmpPtr, txnActorId);
        KAFKA_LOG_D(TStringBuilder() << "Forwarded message to TKafkaTransactionActor with id " << txnActorId << " for transactionalId " << ev->Request->TransactionalId->c_str() << " and ApiKey " << ev->Request->ApiKey());
    };

    bool TKafkaTransactionsCoordinator::NewProducerStateIsOutdated(const TProducerState& currentProducerState, const TProducerState& newProducerState) {
        bool producerIdAlreadyGreater = currentProducerState.Id > newProducerState.Id;
        bool producerIdsAreEqual = currentProducerState.Id == newProducerState.Id;
        bool epochAlreadyGreater = currentProducerState.Epoch > newProducerState.Epoch;
        return producerIdAlreadyGreater || (producerIdsAreEqual && epochAlreadyGreater);
    };

    TMaybe<TString> TKafkaTransactionsCoordinator::GetTxnRequestError(const TTransactionalRequest& request) {
        auto it = ProducersByTransactionalId.find(request.TransactionalId);

        if (it == ProducersByTransactionalId.end()) {
            return TStringBuilder() << "Producer with transactional id " << request.TransactionalId << " was not yet initailized.";
        } else if (NewProducerStateIsOutdated(it->second, request.ProducerState)) {
            return GetProducerIsOutdatedError(request.TransactionalId, it->second, request.ProducerState);
        } else {
            return {};
        }
    };

    TString TKafkaTransactionsCoordinator::GetProducerIsOutdatedError(const TString& transactionalId, const TProducerState& currentProducerState, const TProducerState& newProducerState) {
        return TStringBuilder() << "Producer with transactional id " << transactionalId <<
                    "is outdated. Current producer id is " << currentProducerState.Id << 
                    " and producer epoch is " << currentProducerState.Epoch << ". Requested producer id is " << newProducerState.Id << 
                    " and producer epoch is " << newProducerState.Epoch << ".";
    };
} // namespace NKafka