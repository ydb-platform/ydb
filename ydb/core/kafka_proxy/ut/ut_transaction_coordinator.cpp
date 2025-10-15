#include <ydb/core/kafka_proxy/kafka_transactions_coordinator.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/kafka_proxy/kafka_producer_instance_id.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/fwd.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>

namespace {
    class TFixture : public NUnitTest::TBaseFixture {
        struct TopicPartitions {
            TString Topic;
            TVector<ui32> Partitions;
        };

        public:
            TMaybe<NKikimr::NPQ::TTestContext> Ctx;
            TActorId ActorId;
            const TString Database = "/Root/PQ";
            
            void SetUp(NUnitTest::TTestContext&) override {
                Ctx.ConstructInPlace();
                
                Ctx->Prepare();
                Ctx->Runtime->SetScheduledLimit(5'000);
                Ctx->Runtime->SetLogPriority(NKikimrServices::KAFKA_PROXY, NLog::PRI_DEBUG);
                ActorId = Ctx->Runtime->Register(new NKafka::TTransactionsCoordinator());
            }

            void TearDown(NUnitTest::TTestContext&) override  {
                Ctx->Finalize();
            }

            THolder<NKafka::TEvKafka::TEvSaveTxnProducerResponse> SaveTxnProducer(const TString& txnId, i64 producerId, i16 producerEpoch) {
                auto request = MakeHolder<NKafka::TEvKafka::TEvSaveTxnProducerRequest>(txnId, NKafka::TProducerInstanceId{producerId, producerEpoch}, 5000);
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, request.Release()));
                auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvSaveTxnProducerResponse>();
                UNIT_ASSERT(response != nullptr);
                return response;
            }

            void SendAddPartitionsToTxnRequest(ui64 correlationId, const TString& txnId, i64 producerId, i16 producerEpoch, std::vector<TopicPartitions> topicPartitions) {
                auto message = std::make_shared<NKafka::TAddPartitionsToTxnRequestData>();
                message->TransactionalId = txnId;
                message->ProducerId = producerId;
                message->ProducerEpoch = producerEpoch;
                for (const auto& tp : topicPartitions) {
                    NKafka::TAddPartitionsToTxnRequestData::TAddPartitionsToTxnTopic topic;
                    topic.Name = tp.Topic;
                    for (auto partitionIndex : tp.Partitions) {
                        topic.Partitions.push_back(partitionIndex);
                    }
                    message->Topics.push_back(topic);
                }
                auto event = MakeHolder<NKafka::TEvKafka::TEvAddPartitionsToTxnRequest>(correlationId, NKafka::TMessagePtr<NKafka::TAddPartitionsToTxnRequestData>({}, message), Ctx->Edge, Database);
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
            }

            void SendTxnOffsetCommitRequest(ui64 correlationId, const TString& txnId, i64 producerId, i16 producerEpoch, std::vector<TopicPartitions> topicPartitions) {
                auto message = std::make_shared<NKafka::TTxnOffsetCommitRequestData>();
                message->TransactionalId = txnId;
                message->ProducerId = producerId;
                message->ProducerEpoch = producerEpoch;
                for (const auto& tp : topicPartitions) {
                    NKafka::TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic topic;
                    topic.Name = tp.Topic;
                    for (auto partitionIndex : tp.Partitions) {
                        NKafka::TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition partition;
                        partition.PartitionIndex = partitionIndex;
                        topic.Partitions.push_back(partition);
                    }
                    message->Topics.push_back(topic);
                }
                auto event = MakeHolder<NKafka::TEvKafka::TEvTxnOffsetCommitRequest>(correlationId, NKafka::TMessagePtr<NKafka::TTxnOffsetCommitRequestData>({}, message), Ctx->Edge, Database);
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
            }

            void SendEndTxnRequest(ui64 correlationId, const TString& txnId, i64 producerId, i16 producerEpoch) {
                auto message = std::make_shared<NKafka::TEndTxnRequestData>();
                message->TransactionalId = txnId;
                message->ProducerId = producerId;
                message->ProducerEpoch = producerEpoch;
                auto event = MakeHolder<NKafka::TEvKafka::TEvEndTxnRequest>(correlationId, NKafka::TMessagePtr<NKafka::TEndTxnRequestData>({}, message), Ctx->Edge, Database);
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
            }
    };

    Y_UNIT_TEST_SUITE_F(TransactionCoordinatorActor, TFixture) {
        Y_UNIT_TEST(OnProducerInitializedEvent_ShouldRespondOkIfTxnProducerWasNotFound) {
            auto response = SaveTxnProducer("my-tn-producer-1", 123, 0);

            UNIT_ASSERT_EQUAL(response->Status, NKafka::TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK);
        }

        Y_UNIT_TEST(OnProducerInitializedEvent_ShouldRespondOkIfTxnProducerWasFoundButEpochIsOlder) {
            TString txnId = "my-tx-id";
            i64 producerId = 123;

            auto response1 = SaveTxnProducer(txnId, producerId, 0); // save old epoch
            auto response2 = SaveTxnProducer(txnId, producerId, 1); // save old epoch
            
            UNIT_ASSERT_EQUAL(response1->Status, NKafka::TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK);
            UNIT_ASSERT_EQUAL(response2->Status, NKafka::TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK);
        }

        // epoch overflown case
        Y_UNIT_TEST(OnProducerInitializedEvent_ShouldRespondOkIfNewEpochIsLessButProducerIdIsNew) {
            TString txnId = "my-tx-id";
            i64 producerId = 123;

            auto response1 = SaveTxnProducer(txnId, producerId, 10); // save old epoch
            auto response2 = SaveTxnProducer(txnId, producerId + 1, 1);
            
            UNIT_ASSERT_EQUAL(response1->Status, NKafka::TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK);
            UNIT_ASSERT_EQUAL(response2->Status, NKafka::TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK);
        }

        // two concurrent clients sequentially inited producer id case
        Y_UNIT_TEST(OnProducerInitializedEvent_ShouldRespondWithProducerFencedErrorIfNewEpochIsLessAndProducerIdIsTheSame) {
            TString txnId = "my-tx-id";
            i64 producerId = 123;

            auto response1 = SaveTxnProducer(txnId, producerId, 10);
            auto response2 = SaveTxnProducer(txnId, producerId, 9); // seсond request comes with stale epoch
            
            UNIT_ASSERT_EQUAL(response1->Status, NKafka::TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK);
            UNIT_ASSERT_EQUAL(response2->Status, NKafka::TEvKafka::TEvSaveTxnProducerResponse::EStatus::PRODUCER_FENCED);
        }

        Y_UNIT_TEST(OnAnyTransactionalRequest_ShouldSendBack_PRODUCER_FENCED_ErrorIfThereIsNoTransactionalIdInState) {
            ui64 correlationId = 123;
            // no producer_initialized event was send, thus actor knows nothing about any producer
            SendEndTxnRequest(correlationId, "my-tx-id", 1, 0);

            // will respond to edge, cause we provieded edge actorId as a connectionId in SendAddPartitionsToTxnRequest
            auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
            
            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
            UNIT_ASSERT_VALUES_EQUAL(response->CorrelationId, correlationId);
        }

        Y_UNIT_TEST(OnAnyTransactionalRequest_ShouldSendBack_PRODUCER_FENCED_ErrorIfProducerEpochExpired) {
            ui64 correlationId = 123;
            TString txnId = "my-tx-id";
            i64 producerId = 1;
            i16 producerEpoch = 0;
            auto saveResponse = SaveTxnProducer(txnId, producerId, producerEpoch + 1);
            UNIT_ASSERT_EQUAL(saveResponse->Status, NKafka::TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK);
            SendEndTxnRequest(correlationId, txnId, producerId, producerEpoch);

            // will respond to edge, cause we provieded edge actorId as a connectionId in SendAddPartitionsToTxnRequest
            auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
            
            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
            UNIT_ASSERT_VALUES_EQUAL(response->CorrelationId, correlationId);
        }

        Y_UNIT_TEST(OnAnyTransactionalRequest_ShouldForwardItToTheRelevantTransactionalIdActorIfProducerIsValid) {
            // send valid message
            ui64 correlationId = 123;
            TString txnId = "my-tx-id";
            i64 producerId = 1;
            i16 producerEpoch = 0;
            auto saveResponse = SaveTxnProducer(txnId, producerId, producerEpoch);
            UNIT_ASSERT_EQUAL(saveResponse->Status, NKafka::TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK);
            // observe TEvAddPartitionsToTxnRequest to a different actor
            bool seenEvent = false;
            ui32 eventCounter = 0;
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                if (auto* event = input->CastAsLocal<NKafka::TEvKafka::TEvEndTxnRequest>()) {
                    // there will be two events TEvEndTxnRequest:
                    // first: the one we dispatch in this test
                    // second: event forwarded by TTransactionCoordinatorActor
                    if (eventCounter == 1) {
                        UNIT_ASSERT_VALUES_EQUAL(event->Request->TransactionalId, txnId);
                        UNIT_ASSERT_VALUES_EQUAL(event->Request->ProducerId, producerId);
                        UNIT_ASSERT_VALUES_EQUAL(event->Request->ProducerEpoch, producerEpoch);
                        seenEvent = true;
                    } else {
                        eventCounter++;
                    }
                }

                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };
            Ctx->Runtime->SetObserverFunc(observer);

            SendEndTxnRequest(correlationId, txnId, producerId, producerEpoch);

            TDispatchOptions options;
            options.CustomFinalCondition = [&seenEvent]() {
                return seenEvent;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
        }

        Y_UNIT_TEST(OnAnyTransactionalRequest_ShouldForwardItToExistingTransactionActorIfProducerIsValid) {
            // send valid message
            ui64 correlationId = 123;
            TString txnId = "my-tx-id";
            i64 producerId = 1;
            i16 producerEpoch = 0;
            auto saveResponse = SaveTxnProducer(txnId, producerId, producerEpoch);
            UNIT_ASSERT_EQUAL(saveResponse->Status, NKafka::TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK);
            // observe TEvAddPartitionsToTxnRequest to a different actor
            bool seenEvent = false;
            ui32 eventCounter = 0;
            TActorId txnActorId;
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                if (auto* event = input->CastAsLocal<NKafka::TEvKafka::TEvEndTxnRequest>()) {
                    // There will be four events TEvEndTxnRequest. We need only two of them 
                    // with recipient not equal to our TTransactionCoordinatorActor id. 
                    // Those are event sent from TTransactionCoordinatorActor to TTransactionActor
                    if (input->Recipient != ActorId) {
                        if (eventCounter == 0) {
                            txnActorId = input->Recipient;
                            eventCounter++;
                        } else {
                            UNIT_ASSERT_VALUES_EQUAL(txnActorId, input->Recipient);
                            seenEvent = true;
                        }
                    } 
                }

                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };
            Ctx->Runtime->SetObserverFunc(observer);

            SendEndTxnRequest(correlationId, txnId, producerId, producerEpoch);
            SendEndTxnRequest(correlationId, txnId, producerId, producerEpoch);

            TDispatchOptions options;
            options.CustomFinalCondition = [&seenEvent]() {
                return seenEvent;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
        }

        Y_UNIT_TEST(OnSecondInitProducerId_ShouldSendPoisonPillToTxnActor) {
            // send valid message
            ui64 correlationId = 123;
            TString txnId = "my-tx-id";
            i64 producerId = 1;
            i16 producerEpoch = 0;
            SaveTxnProducer(txnId, producerId, producerEpoch);
            bool seenEvent = false;
            TActorId txnActorId;
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                if (auto* event = input->CastAsLocal<NKafka::TEvKafka::TEvEndTxnRequest>()) {
                    txnActorId = input->Recipient;
                } else if (auto* event = input->CastAsLocal<TEvents::TEvPoison>()) {
                    UNIT_ASSERT_VALUES_EQUAL(txnActorId, input->Recipient);
                    seenEvent = true;
                }

                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };
            Ctx->Runtime->SetObserverFunc(observer);

            // first request registers actor
            SendEndTxnRequest(correlationId, txnId, producerId, producerEpoch);
            // request to save producer with newer epoch should trigger poison pill to current txn actor
            SaveTxnProducer(txnId, producerId, producerEpoch + 1);

            TDispatchOptions options;
            options.CustomFinalCondition = [&seenEvent]() {
                return seenEvent;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
        }

        Y_UNIT_TEST(OnAddPartitions_ShouldSendBack_PRODUCER_FENCED_ErrorIfProducerIsNotInitialized) {
            ui64 correlationId = 123;
            SendAddPartitionsToTxnRequest(correlationId, "my-tx-id", 1, 0, {{"topic1", {0, 1}}, {"topic2", {0}}});

            // will respond to edge, cause we provieded edge actorId as a connectionId in SendAddPartitionsToTxnRequest
            auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
            
            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
            UNIT_ASSERT_EQUAL(response->Response->ApiKey(), NKafka::EApiKey::ADD_PARTITIONS_TO_TXN);
            const auto& message = static_cast<const NKafka::TAddPartitionsToTxnResponseData&>(*response->Response);
            UNIT_ASSERT_VALUES_EQUAL(response->CorrelationId, correlationId);
            UNIT_ASSERT_VALUES_EQUAL(message.Results.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(message.Results[0].Name, "topic1");
            UNIT_ASSERT_VALUES_EQUAL(message.Results[0].Results.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(message.Results[0].Results[0].PartitionIndex, 0);
            UNIT_ASSERT_EQUAL(message.Results[0].Results[0].ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
            UNIT_ASSERT_VALUES_EQUAL(message.Results[0].Results[1].PartitionIndex, 1);
            UNIT_ASSERT_EQUAL(message.Results[0].Results[1].ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
            UNIT_ASSERT_VALUES_EQUAL(message.Results[1].Name, "topic2");
            UNIT_ASSERT_VALUES_EQUAL(message.Results[1].Results.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(message.Results[1].Results[0].PartitionIndex, 0);
            UNIT_ASSERT_EQUAL(message.Results[1].Results[0].ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
        }

        Y_UNIT_TEST(OnTxnOffsetCommit_ShouldSendBack_PRODUCER_FENCED_ErrorIfProducerIsNotInitialized) {
            ui64 correlationId = 123;
            SendTxnOffsetCommitRequest(correlationId, "my-tx-id", 1, 0, {{"topic1", {0, 1}}, {"topic2", {0}}});

            // will respond to edge, cause we provieded edge actorId as a connectionId in SendAddPartitionsToTxnRequest
            auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
            
            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(response->CorrelationId, correlationId);
            UNIT_ASSERT_EQUAL(response->Response->ApiKey(), NKafka::EApiKey::TXN_OFFSET_COMMIT);
            const auto& message = static_cast<const NKafka::TTxnOffsetCommitResponseData&>(*response->Response);
            UNIT_ASSERT_VALUES_EQUAL(message.Topics.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[0].Name, "topic1");
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[0].Partitions.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[0].Partitions[0].PartitionIndex, 0);
            UNIT_ASSERT_EQUAL(message.Topics[0].Partitions[0].ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[0].Partitions[1].PartitionIndex, 1);
            UNIT_ASSERT_EQUAL(message.Topics[0].Partitions[1].ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[1].Name, "topic2");
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[1].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[1].Partitions[0].PartitionIndex, 0);
            UNIT_ASSERT_EQUAL(message.Topics[1].Partitions[0].ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
        }

        Y_UNIT_TEST(AfterSecondInitializationOldTxnRequestsShouldBeFenced) {
            ui64 correlationId = 123;
            TString txnId = "my-tx-id";
            i64 producerId = 1;
            i16 producerEpoch = 0;
            // first app initializes
            auto saveResponse1 = SaveTxnProducer(txnId, producerId, producerEpoch);
            UNIT_ASSERT_EQUAL(saveResponse1->Status, NKafka::TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK);
            // other app initializes with same transactional id
            auto saveResponse2 = SaveTxnProducer(txnId, producerId, producerEpoch + 1);
            UNIT_ASSERT_EQUAL(saveResponse2->Status, NKafka::TEvKafka::TEvSaveTxnProducerResponse::EStatus::OK);

            // first app sends txn request
            SendEndTxnRequest(correlationId, txnId, producerId, producerEpoch);
            // will respond to edge, cause we provieded edge actorId as a connectionId in SendAddPartitionsToTxnRequest
            auto firstResponse = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
            
            UNIT_ASSERT(firstResponse != nullptr);
            UNIT_ASSERT_EQUAL(firstResponse->ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
        }
    }
} // namespace
