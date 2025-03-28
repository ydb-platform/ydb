#pragma once

#include "kafka_events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>


namespace NKafka {
    /* 
    This class serves as a proxy between Kafka SDK and TKafkaTransactionActor

    It validates that requester is not a zombie (by checking request's tranasactional_id+producer_id+producer_epoch)
    It does so by maintaining a set of the most relevant for this node tranasactional_id+producer_id+producer_epoch. 
    Recieves updates from init_producer_id_actors. 
    */
    class TKafkaTransactionsCoordinator : public NActors::TActorBootstrapped<TKafkaTransactionsCoordinator> {

        using TBase = NActors::TActorBootstrapped<TKafkaTransactionsCoordinator>;
    
        struct TProducerState {
            i64 ProducerId;
            i32 ProducerEpoch;
        };

        struct TTransactionalRequest {
            TString TransactionalId;
            TProducerState ProducerState;
            ui64 CorrelationId;
            TActorId ConnectionId;
        };

        public:
            void Bootstrap(const TActorContext&) {
                TBase::Become(&TKafkaTransactionsCoordinator::StateWork);
            }

            TStringBuilder LogPrefix() const {
                return TStringBuilder() << "KafkaTransactionsCoordinator";
            }
        private:
            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvKafka::TEvSaveTxnProducerRequest, Handle);
                    HFunc(TEvKafka::TEvAddPartitionsToTxnRequest, Handle);
                    HFunc(TEvKafka::TEvAddOffsetsToTxnRequest, Handle);
                    HFunc(TEvKafka::TEvTxnOffsetCommitRequest, Handle);
                    HFunc(TEvKafka::TEvEndTxnRequest, Handle);
                }
            }
            
            // Handles new transactional_id+producer_id+producer_epoch: saves for validation of future requests
            void Handle(TEvKafka::TEvSaveTxnProducerRequest::TPtr& ev, const TActorContext& ctx);
            
            // Proxies requests to the relevant TKafkaTransactionActor
            void Handle(TEvKafka::TEvAddPartitionsToTxnRequest::TPtr& ev, const TActorContext& ctx);
            void Handle(TEvKafka::TEvAddOffsetsToTxnRequest::TPtr& ev, const TActorContext& ctx);
            void Handle(TEvKafka::TEvTxnOffsetCommitRequest::TPtr& ev, const TActorContext& ctx);
            void Handle(TEvKafka::TEvEndTxnRequest::TPtr& ev, const TActorContext& ctx);

            template<class ErrorResponseType, class EventType>
            void HandleTransacationalRequest(TAutoPtr<TEventHandle<EventType>>& evHandle, const TActorContext& ctx);
            template<class ErrorResponseType, class RequestType>
            void SendProducerFencedResponse(TMessagePtr<RequestType> kafkaRequest, const TString& error, const TTransactionalRequest& request);
            template<class EventType> 
            void ForwardToTransactionActor(TAutoPtr<TEventHandle<EventType>>& evHandle, const TActorContext& ctx);

            template<class ResponseType, class RequestType>
            std::shared_ptr<ResponseType> BuildProducerFencedResponse(TMessagePtr<RequestType> request);
            std::shared_ptr<TAddPartitionsToTxnResponseData> BuildProducerFencedResponseForAddPartitions(TMessagePtr<TAddPartitionsToTxnRequestData> ev);
            std::shared_ptr<TTxnOffsetCommitResponseData> BuildProducerFencedResponseForTxnOffsetCommit(const TTxnOffsetCommitRequestData* const ev);

            bool NewProducerStateIsOutdated(const TProducerState& currentProducerState, const TProducerState& newProducerState);
            TMaybe<TString> GetTxnRequestError(const TTransactionalRequest& request);
            TString GetProducerIsOutdatedError(const TString& transactionalId, const TProducerState& currentProducerState, const TProducerState& newProducerState);

            std::unordered_map<TString, TProducerState> ProducersByTransactionalId;
            std::unordered_map<TString, TActorId> TxnActorByTransactionalId;
    };

    NActors::IActor* CreateKafkaTransactionsCoordinator();
    
} // namespace NKafka