#pragma once

#include "txn_actor_response_builder.h"
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/kafka_proxy/kqp_helper.h>

namespace NKafka {
    /* 
    This class is responsible for one kafka transaction.

    It accumulates transaction state (partitions in tx, offsets) and on commit submits transaction to KQP
    */
    class TKafkaTransactionActor : public NActors::TActorBootstrapped<TKafkaTransactionActor> {

        using TBase = NActors::TActorBootstrapped<TKafkaTransactionActor>;
        
        public:
            struct TTopicPartition {
                TString TopicPath;
                i32 PartitionId;

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
                
                SELECT, // request to select producer and consumers in txn state. After this request a check will happen, that no participant has expired
                COMMIT // request commit txn to kqp
            };

            TKafkaTransactionActor(const TString& DatabasePath, const TString& transactionalId, i64 producerId, i16 producerEpoch) : 
                TransactionalId(transactionalId),
                ProducerId(producerId),
                ProducerEpoch(producerEpoch),
                DatabasePath(DatabasePath) {};

            void Bootstrap(const NActors::TActorContext&) {
                TBase::Become(&TKafkaTransactionActor::StateWork);
            }

            TStringBuilder LogPrefix() const {
                return TStringBuilder() << "KafkaTransactionActor{TransactionalId=" << TransactionalId << "; ProducerId=" << ProducerId << "; ProducerEpoch=" << ProducerEpoch << "}: ";
            }
        
        private:
            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvKafka::TEvAddPartitionsToTxnRequest, Handle);
                    HFunc(TEvKafka::TEvAddOffsetsToTxnRequest, Handle);
                    HFunc(TEvKafka::TEvTxnOffsetCommitRequest, Handle);
                    HFunc(TEvKafka::TEvEndTxnRequest, Handle);
                    HFunc(TEvents::TEvPoison, Handle);
                    // will be eimplemented in a future PR
                    // ToDo: add poison pill handler
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
            
            void Handle(TEvents::TEvPoison::TPtr& ev, const TActorContext& ctx);

            // Transaction commit logic
            void StartKqpSession(const TActorContext& ctx);
            void SendToKqpValidationRequests(const TActorContext& ctx);

            // Response senders
            template<class ErrorResponseType, class EventType>
            void SendInvalidTransactionActorStateResponse(TAutoPtr<TEventHandle<EventType>>& evHandle);
            template<class ErrorResponseType, class EventType>
            void SendResponseFail(TAutoPtr<TEventHandle<EventType>>& evHandle, EKafkaErrors errorCode, const TString& errorMessage = {});
            template<class ResponseType, class EventType>
            void SendOkResponse(TAutoPtr<TEventHandle<EventType>>& evHandle);

            // helper methods
            template<class EventType>
            bool ProducerInRequestIsValid(TMessagePtr<EventType> kafkaRequest);
            THolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest> BuildCommitTxnRequestToKqp();
            void Die(const TActorContext &ctx);
            TString GetFullTopicPath(const TString& topicName);
            TString GetYqlWithTablesNames(const TString& templateStr);
            NYdb::TParams BuildSelectParams();

            // data from fields below will be sent to KQP on END_TXN request
            std::unordered_map<TTopicPartition, TPartitionCommit, TopicPartitionHashFn> OffsetsToCommit = {};
            std::unordered_set<TTopicPartition, TopicPartitionHashFn> PartitionsInTxn = {};
            const TString TransactionalId;
            const i64 ProducerId;
            const i16 ProducerEpoch;

            // helper fields
            const TString DatabasePath;
            NKafkaTransactions::ResponseBuilder ResponseBuilder;

            // communication with KQP
            std::unique_ptr<NKafka::TKqpTxHelper> Kqp;
            TString KqpSessionId;
            ui64 KqpCookie = 0;
            EKafkaTxnKqpRequests LastSentToKqpRequest;
    };
} // namespace NKafka