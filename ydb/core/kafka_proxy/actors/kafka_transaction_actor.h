#pragma once

#include "txn_actor_response_builder.h"
#include "kafka_init_producer_id_actor.h"
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/kafka_proxy/kqp_helper.h>

namespace NKafka {
    /* 
    This class is responsible for one kafka transaction.

    It accumulates transaction state (partitions in tx, offsets) and on commit submits transaction to KQP
    */
    class TKafkaTransactionActor : public NActors::TActor<TKafkaTransactionActor> {

        using TBase = NActors::TActor<TKafkaTransactionActor>;
        
        public:
            struct TTopicPartition {
                TString TopicPath;
                ui32 PartitionId;

                bool operator==(const TTopicPartition &other) const
                { return (TopicPath == other.TopicPath
                        && PartitionId == other.PartitionId);
                }
            };

            struct TopicPartitionHashFn {
                size_t operator()(const TTopicPartition& partition) const {
                    return std::hash<TString>()(partition.TopicPath) ^ std::hash<int64_t>()(partition.PartitionId);
                }
            };

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
                // This request sends to KQP a command to commit transaction
                // Both these requests happen in same transaction
                COMMIT 
            };

            // we need to exlplicitly specify kqpActorId and txnCoordinatorActorId for unit tests
            TKafkaTransactionActor(const TString& transactionalId, i64 producerId, i16 producerEpoch, const TString& DatabasePath, const TActorId& kqpActorId, const TActorId& txnCoordinatorActorId) : 
                TActor<TKafkaTransactionActor>(&TKafkaTransactionActor::StateFunc),
                TransactionalId(transactionalId),
                ProducerId(producerId),
                ProducerEpoch(producerEpoch),
                DatabasePath(DatabasePath),
                TxnCoordinatorActorId(txnCoordinatorActorId),
                KqpActorId(kqpActorId) {};

            TStringBuilder LogPrefix() const {
                return TStringBuilder() << "KafkaTransactionActor{TransactionalId=" << TransactionalId << "; ProducerId=" << ProducerId << "; ProducerEpoch=" << ProducerEpoch << "}: ";
            }
        
        private:
            STFUNC(StateFunc) {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvKafka::TEvAddPartitionsToTxnRequest, Handle);
                    HFunc(TEvKafka::TEvAddOffsetsToTxnRequest, Handle);
                    HFunc(TEvKafka::TEvTxnOffsetCommitRequest, Handle);
                    HFunc(TEvKafka::TEvEndTxnRequest, Handle);
                    HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
                    HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
                    HFunc(TEvents::TEvPoison, Handle);
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
            void SendCommitTxnRequest(const TString& kqpTransactionId);

            // Response senders
            template<class ErrorResponseType, class EventType>
            void SendInvalidTransactionActorStateResponse(TAutoPtr<TEventHandle<EventType>>& evHandle);
            template<class ErrorResponseType, class EventType>
            void SendResponseFail(TAutoPtr<TEventHandle<EventType>>& evHandle, EKafkaErrors errorCode, const TString& errorMessage = {});
            template<class ResponseType, class EventType>
            void SendOkResponse(TAutoPtr<TEventHandle<EventType>>& evHandle);

            // helper methods
            void Die(const TActorContext &ctx);
            template<class EventType>
            bool ProducerInRequestIsValid(TMessagePtr<EventType> kafkaRequest);
            TString GetFullTopicPath(const TString& topicName);
            TString GetYqlWithTablesNames(const TString& templateStr);
            NYdb::TParams BuildSelectParams();
            THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> BuildCommitTxnRequestToKqp(const TString& kqpTransactionId);
            void HandleSelectResponse(const NKikimrKqp::TEvQueryResponse& response, const TActorContext& ctx);
            void HandleCommitResponse(const TActorContext& ctx);
            TMaybe<TString> GetErrorFromYdbResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev);
            TMaybe<TProducerState> ParseProducerState(const NKikimrKqp::TEvQueryResponse& response);
            TMaybe<TString> GetErrorInProducerState(const TMaybe<TProducerState>& producerState);
            std::unordered_map<TString, i32> ParseConsumersGenerations(const NKikimrKqp::TEvQueryResponse& response);
            TMaybe<TString> GetErrorInConsumersStates(const std::unordered_map<TString, i32>& consumerGenerationByName);
            TString GetAsStr(EKafkaTxnKqpRequests request);

            // data from fields below will be sent to KQP on END_TXN request
            std::unordered_map<TTopicPartition, TPartitionCommit, TopicPartitionHashFn> OffsetsToCommit = {};
            std::unordered_set<TTopicPartition, TopicPartitionHashFn> PartitionsInTxn = {};
            const TString TransactionalId;
            const i64 ProducerId;
            const i16 ProducerEpoch;

            // helper fields
            const TString DatabasePath;
            NKafkaTransactions::TResponseBuilder ResponseBuilder;
            // This field need to preserve request details between several requests to KQP
            // In case something goes off road, we can always send error back to client
            TAutoPtr<TEventHandle<TEvKafka::TEvEndTxnRequest>> EndTxnRequestPtr;
            bool CommitStarted = false;
            const TActorId TxnCoordinatorActorId;

            // communication with KQP
            std::unique_ptr<NKafka::TKqpTxHelper> Kqp;
            TActorId KqpActorId;
            TString KqpSessionId;
            ui64 KqpCookie = 0;
            EKafkaTxnKqpRequests LastSentToKqpRequest;
    };
} // namespace NKafka