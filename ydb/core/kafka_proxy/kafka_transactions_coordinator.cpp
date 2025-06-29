#include "kafka_transactions_coordinator.h"
#include "actors/kafka_transaction_actor.h"
#include "actors/txn_actor_response_builder.h"
#include <ydb/core/kqp/common/simple/services.h>

namespace NKafka {
    // Handles new transactional_id+producer_id+producer_epoch: 
    // 1. validates that producer is not a zombie (in case of parallel init_producer_requests)
    // 2. saves transactional_id+producer_id+producer_epoch for validation of future transactional requests
    void TTransactionsCoordinator::Handle(TEvKafka::TEvSaveTxnProducerRequest::TPtr& ev, const TActorContext& ctx){
        TEvKafka::TEvSaveTxnProducerRequest* request = ev->Get();

        auto it = ProducersByTransactionalId.find(request->TransactionalId);
        if (it != ProducersByTransactionalId.end()) {
            TProducerInstanceId& currentProducerId = it->second.Id;
            const TProducerInstanceId& newProducerId = request->ProducerInstanceId;

            if (NewProducerStateIsOutdated(currentProducerId, newProducerId)) {
                TString message = GetProducerIsOutdatedError(request->TransactionalId, currentProducerId, newProducerId);
                ctx.Send(ev->Sender, new TEvKafka::TEvSaveTxnProducerResponse(TEvKafka::TEvSaveTxnProducerResponse::EStatus::PRODUCER_FENCED, message));
                return;
            } 

            it->second = {newProducerId, request->TxnTimeoutMs};
            DeleteTransactionActor(request->TransactionalId);
        } else {
            ProducersByTransactionalId.emplace(request->TransactionalId, TProducerInstance{request->ProducerInstanceId, request->TxnTimeoutMs});
        }
        
        ctx.Send(ev->Sender, new TEvKafka::TEvSaveTxnProducerResponse(TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK, ""));
    };

    void TTransactionsCoordinator::Handle(TEvKafka::TEvAddPartitionsToTxnRequest::TPtr& ev, const TActorContext& ctx){
        HandleTransactionalRequest<TAddPartitionsToTxnResponseData>(ev, ctx);
    };

    void TTransactionsCoordinator::Handle(TEvKafka::TEvAddOffsetsToTxnRequest::TPtr& ev, const TActorContext& ctx){
        HandleTransactionalRequest<TAddOffsetsToTxnResponseData>(ev, ctx);
    };

    void TTransactionsCoordinator::Handle(TEvKafka::TEvTxnOffsetCommitRequest::TPtr& ev, const TActorContext& ctx) {
        HandleTransactionalRequest<TTxnOffsetCommitResponseData>(ev, ctx);
    };

    void TTransactionsCoordinator::Handle(TEvKafka::TEvEndTxnRequest::TPtr& ev, const TActorContext& ctx) {
        HandleTransactionalRequest<TEndTxnResponseData>(ev, ctx);
    };

    void TTransactionsCoordinator::Handle(TEvKafka::TEvTransactionActorDied::TPtr& ev, const TActorContext&) {
        auto it = ProducersByTransactionalId.find(ev->Get()->TransactionalId);
        const TProducerInstanceId& deadActorProducerState = ev->Get()->ProducerState;

        if (it == ProducersByTransactionalId.end() || it->second.Id != deadActorProducerState) {
            // new actor was already registered, we can just ignore this event
            KAFKA_LOG_D("Received TEvTransactionActorDied for transactionalId " << ev->Get()->TransactionalId << " but producer has already been reinitialized with new epoch or deleted. Ignoring this event");
        } else {
            KAFKA_LOG_D("Received TEvTransactionActorDied for transactionalId " << ev->Get()->TransactionalId 
                << " and producerId " << ev->Get()->ProducerState.Id 
                << " and producerEpoch " << ev->Get()->ProducerState.Epoch
                << ". Erasing info about this actor.");
            // erase info about actor to prevent future zombie requests from this producer
            TxnActorByTransactionalId.erase(ev->Get()->TransactionalId);
        }
    };

    void TTransactionsCoordinator::Handle(TEvents::TEvPoison::TPtr&, const TActorContext& ctx) {
        KAFKA_LOG_D("Got poison pill, killing all transaction actors");
        for (auto& [transactionalId, txnActorId]: TxnActorByTransactionalId) {
            ctx.Send(txnActorId, new TEvents::TEvPoison());
            KAFKA_LOG_D("Sent poison pill to transaction actor for transactionalId " << transactionalId);
        }
        PassAway();
    };

    void TTransactionsCoordinator::PassAway() {
        KAFKA_LOG_D("Killing myself");
        TBase::PassAway();
    };

    // Validates producer's id and epoch
    // If valid: proxies requests to the relevant TTransactionActor
    // If outdated or not initialized: returns PRODUCER_FENCED error
    template<class ErrorResponseType, class EventType>
    void TTransactionsCoordinator::HandleTransactionalRequest(TAutoPtr<TEventHandle<EventType>>& evHandle, const TActorContext& ctx) {
        EventType* ev = evHandle->Get();
        KAFKA_LOG_D("Received message for transactionalId " << *ev->Request->TransactionalId << " and ApiKey " << ev->Request->ApiKey());
        
        // create helper struct to simplify methods interaction
        auto txnRequest = TTransactionalRequest(
            *ev->Request->TransactionalId,
            TProducerInstanceId(ev->Request->ProducerId, ev->Request->ProducerEpoch),
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
    void TTransactionsCoordinator::SendProducerFencedResponse(TMessagePtr<RequestType> kafkaRequest, const TString& error, const TTransactionalRequest& txnRequestDetails) {
        KAFKA_LOG_W(error);
        std::shared_ptr<ErrorResponseType> response = NKafkaTransactions::BuildResponse<ErrorResponseType>(kafkaRequest, EKafkaErrors::PRODUCER_FENCED);
        Send(txnRequestDetails.ConnectionId, new TEvKafka::TEvResponse(txnRequestDetails.CorrelationId, response, EKafkaErrors::PRODUCER_FENCED));
    };

    template<class EventType> 
    void TTransactionsCoordinator::ForwardToTransactionActor(TAutoPtr<TEventHandle<EventType>>& evHandle, const TActorContext& ctx) {
        EventType* ev = evHandle->Get();
        
        TActorId txnActorId;
        if (TxnActorByTransactionalId.contains(*ev->Request->TransactionalId)) {
            txnActorId = TxnActorByTransactionalId.at(*ev->Request->TransactionalId);
        } else {
            auto& producerInstance = ProducersByTransactionalId.at(*ev->Request->TransactionalId);
            txnActorId = ctx.Register(new TTransactionActor(*ev->Request->TransactionalId, {ev->Request->ProducerId, ev->Request->ProducerEpoch}, ev->DatabasePath, producerInstance.TxnTimeoutMs));
            TxnActorByTransactionalId[*ev->Request->TransactionalId] = txnActorId;
            KAFKA_LOG_D("Registered TTransactionActor with id " << txnActorId << " for transactionalId " << *ev->Request->TransactionalId << " and ApiKey " << ev->Request->ApiKey());
        }
        TAutoPtr<IEventHandle> tmpPtr = evHandle.Release();
        ctx.Forward(tmpPtr, txnActorId);
        KAFKA_LOG_D("Forwarded message to TTransactionActor with id " << txnActorId << " for transactionalId " << *ev->Request->TransactionalId << " and ApiKey " << ev->Request->ApiKey());
    };

    void TTransactionsCoordinator::DeleteTransactionActor(const TString& transactionalId) {
        auto it = TxnActorByTransactionalId.find(transactionalId);
        if (it != TxnActorByTransactionalId.end()) {
            Send(it->second, new TEvents::TEvPoison());
            TxnActorByTransactionalId.erase(it);
        } 
        // we ignore case when there is no actor, cause it means that no actor was ever created for this transactionalId
    }

    bool TTransactionsCoordinator::NewProducerStateIsOutdated(const TProducerInstanceId& currentProducerState, const TProducerInstanceId& newProducerState) {
        return currentProducerState > newProducerState;
    };

    TMaybe<TString> TTransactionsCoordinator::GetTxnRequestError(const TTransactionalRequest& request) {
        auto it = ProducersByTransactionalId.find(request.TransactionalId);

        if (it == ProducersByTransactionalId.end()) {
            return TStringBuilder() << "Producer with transactional id " << request.TransactionalId << " was not yet initailized.";
        } else if (NewProducerStateIsOutdated(it->second.Id, request.ProducerState)) {
            return GetProducerIsOutdatedError(request.TransactionalId, it->second.Id, request.ProducerState);
        } else {
            return {};
        }
    };

    TString TTransactionsCoordinator::GetProducerIsOutdatedError(const TString& transactionalId, const TProducerInstanceId& currentProducerState, const TProducerInstanceId& newProducerState) {
        return TStringBuilder() << "Producer with transactional id " << transactionalId <<
                    "is outdated. Current producer id is " << currentProducerState.Id << 
                    " and producer epoch is " << currentProducerState.Epoch << ". Requested producer id is " << newProducerState.Id << 
                    " and producer epoch is " << newProducerState.Epoch << ".";
    };
} // namespace NKafka