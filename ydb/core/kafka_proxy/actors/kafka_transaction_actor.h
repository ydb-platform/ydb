#pragma once

#include "kafka_init_producer_id_actor.h"
#include <ydb/core/kafka_proxy/kafka_topic_partition.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/kafka_proxy/kafka_producer_instance_id.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/kafka_proxy/kqp_helper.h>

namespace NKafka {
    /* 
    This class is responsible for one kafka transaction.

    It accumulates transaction state (partitions in tx, offsets) and on commit submits transaction to KQP
    */
    class TTransactionActor : public NActors::TActor<TTransactionActor> {

        using TBase = NActors::TActor<TTransactionActor>;
        
        public:
            struct TPartitionCommit {
                i32 Partition; 
                i64 Offset;
                TString ConsumerName;
                i64 ConsumerGeneration;
            };

            enum EKafkaTxnKqpRequests : ui8 {
                NO_REQUEST = 0,
                
                // This request selects up-to-date producer and consumers states from relevant tables
                // After this request a check will happen, that no transaction details has expired.
                SELECT,
                // This requests just adds all transaction details (partitions, offsets) to KQP
                ADD_KAFKA_OPERATIONS_TO_TXN,
                // This request sends to KQP a command to commit transaction
                COMMIT
            };

            // we need to exlplicitly specify kqpActorId and txnCoordinatorActorId for unit tests
            TTransactionActor(const TString& transactionalId, const TProducerInstanceId& producerInstanceId, const TString& databasePath, ui64 txnTimeoutMs) : 
                TBase(&TTransactionActor::StateFunc),
                TransactionalId(transactionalId),
                ProducerInstanceId(producerInstanceId),
                DatabasePath(databasePath),
                TxnTimeoutMs(txnTimeoutMs),
                CreatedAt(TAppData::TimeProvider->Now()) {};

            TStringBuilder LogPrefix() const {
                return TStringBuilder() << "KafkaTransactionActor{TransactionalId=" << TransactionalId << "; ProducerId=" << ProducerInstanceId.Id << "; ProducerEpoch=" << ProducerInstanceId.Epoch << "}: ";
            }
        
        private:
            STFUNC(StateFunc) {
                try {
                    switch (ev->GetTypeRewrite()) {
                        HFunc(TEvKafka::TEvAddPartitionsToTxnRequest, Handle);
                        HFunc(TEvKafka::TEvAddOffsetsToTxnRequest, Handle);
                        HFunc(TEvKafka::TEvTxnOffsetCommitRequest, Handle);
                        HFunc(TEvKafka::TEvEndTxnRequest, Handle);
                        HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
                        HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
                        HFunc(TEvents::TEvPoison, Handle);
                    }
                } catch (const yexception& y) {
                    KAFKA_LOG_CRIT("Critical error happened. Reason: " << y.what());
                    if (EndTxnRequestPtr) {
                        SendFailResponse<TEndTxnResponseData>(EndTxnRequestPtr, EKafkaErrors::UNKNOWN_SERVER_ERROR, y.what());
                    }
                    Die(ActorContext());
                }
            }

            // Kafka API events
            void Handle(TEvKafka::TEvAddPartitionsToTxnRequest::TPtr& ev, const TActorContext& ctx);
            void Handle(TEvKafka::TEvAddOffsetsToTxnRequest::TPtr& ev, const TActorContext& ctx);
            void Handle(TEvKafka::TEvTxnOffsetCommitRequest::TPtr& ev, const TActorContext& ctx);
            void Handle(TEvKafka::TEvEndTxnRequest::TPtr& ev, const TActorContext& ctx);
            // KQP events
            void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);
            void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);
            
            // Poison pill
            void Handle(TEvents::TEvPoison::TPtr& ev, const TActorContext& ctx);

            // Transaction commit logic
            void StartKqpSession(const TActorContext& ctx);
            void SendToKqpValidationRequests(const TActorContext& ctx);
            void SendAddKafkaOperationsToTxRequest(const TString& kqpTransactionId);

            // Response senders
            template<class ErrorResponseType, class EventType>
            void SendFailResponse(TAutoPtr<TEventHandle<EventType>>& evHandle, EKafkaErrors errorCode, const TString& errorMessage = {});
            template<class ResponseType, class EventType>
            void SendOkResponse(TAutoPtr<TEventHandle<EventType>>& evHandle);

            // helper methods
            void Die(const TActorContext &ctx);
            bool TxnExpired();
            template<class EventType>
            bool ProducerInRequestIsValid(TMessagePtr<EventType> kafkaRequest);
            TString GetFullTopicPath(const TString& topicName);
            TString GetYqlWithTablesNames(const TString& templateStr);
            NYdb::TParams BuildSelectParams();
            THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> BuildAddKafkaOperationsRequest(const TString& kqpTransactionId);
            void HandleSelectResponse(const NKqp::TEvKqp::TEvQueryResponse& response, const TActorContext& ctx);
            void HandleAddKafkaOperationsResponse(const TString& kqpTransactionId, const TActorContext& ctx);
            void HandleCommitResponse(const TActorContext& ctx);
            TMaybe<TString> GetErrorFromYdbResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev);
            TMaybe<TProducerState> ParseProducerState(const NKqp::TEvKqp::TEvQueryResponse& response);
            TMaybe<TString> GetErrorInProducerState(const TMaybe<TProducerState>& producerState);
            std::unordered_map<TString, i32> ParseConsumersGenerations(const NKqp::TEvKqp::TEvQueryResponse& response);
            TMaybe<TString> GetErrorInConsumersStates(const std::unordered_map<TString, i32>& consumerGenerationByName);
            TString GetAsStr(EKafkaTxnKqpRequests request);

            // data from fields below will be sent to KQP on END_TXN request
            std::unordered_map<TTopicPartition, TPartitionCommit, TTopicPartitionHashFn> OffsetsToCommit = {};
            std::unordered_set<TTopicPartition, TTopicPartitionHashFn> PartitionsInTxn = {};
            const TString TransactionalId;
            const TProducerInstanceId ProducerInstanceId;

            // helper fields
            const TString DatabasePath;
            // This field need to preserve request details between several requests to KQP
            // In case something goes off road, we can always send error back to client
            TAutoPtr<TEventHandle<TEvKafka::TEvEndTxnRequest>> EndTxnRequestPtr;
            bool CommitStarted = false;
            ui64 TxnTimeoutMs;
            TInstant CreatedAt;

            // communication with KQP
            std::unique_ptr<TKqpTxHelper> Kqp;
            TString KqpSessionId;
            ui64 KqpCookie = 0;
            EKafkaTxnKqpRequests LastSentToKqpRequest;
    };
} // namespace NKafka