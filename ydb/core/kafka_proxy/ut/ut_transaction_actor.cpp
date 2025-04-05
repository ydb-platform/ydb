#include <ydb/core/kafka_proxy/actors/kafka_transaction_actor.h>
#include <ydb/core/kafka_proxy/kafka_events.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/fwd.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/kqp/common/events/events.h>

namespace {
    class TTransactionActorFixture : public NUnitTest::TBaseFixture {
        public:
            struct TTopicPartitions {
                TString Topic;
                TVector<ui32> Partitions;
            };

            struct TPartitionCommit {
                ui32 Partition; 
                ui64 Offset;
                ui64 ConsumerEpoch;
            };

            struct QueryRequestMatcher {
                bool commitTx;
                bool beginTx;
                TVector<TTopicPartitions> topics;
                std::unordered_map<TString, std::vector<TPartitionCommit>> offsetsToCommitByTopic;
            };

            TMaybe<NKikimr::NPQ::TTestContext> Ctx;
            TActorId ActorId;
            const TString TransactionalId = "123"; // transactional id from kafka SDK
            const ui64 ProducerId = 1;
            const ui16 ProducerEpoch = 1;
            
            void SetUp(NUnitTest::TTestContext&) override {
                Ctx.ConstructInPlace();
                
                Ctx->Prepare();
                Ctx->Runtime->SetScheduledLimit(5'000);
                Ctx->Runtime->SetLogPriority(NKikimrServices::KAFKA_PROXY, NLog::PRI_DEBUG);
                ActorId = Ctx->Runtime->Register(new NKafka::TKafkaTransactionActor());
            }

            void TearDown(NUnitTest::TTestContext&) override  {
                Ctx->Finalize();
            }

            void SendAddPartitionsToTxnRequest(std::vector<TTopicPartitions> topicPartitions, ui64 correlationId = 0) {
                auto message = std::make_shared<NKafka::TAddPartitionsToTxnRequestData>();
                message->TransactionalId = TransactionalId;
                message->ProducerId = ProducerId;
                message->ProducerEpoch = ProducerEpoch;
                for (const auto& tp : topicPartitions) {
                    NKafka::TAddPartitionsToTxnRequestData::TAddPartitionsToTxnTopic topic;
                    topic.Name = tp.Topic;
                    for (auto partitionIndex : tp.Partitions) {
                        topic.Partitions.push_back(partitionIndex);
                    }
                    message->Topics.push_back(topic);
                }
                auto event = MakeHolder<NKafka::TEvKafka::TEvAddPartitionsToTxnRequest>(correlationId, NKafka::TMessagePtr<NKafka::TAddPartitionsToTxnRequestData>({}, message), Ctx->Edge);
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
            }

            void SendTxnOffsetCommitRequest(std::unordered_map<TString, std::vector<TPartitionCommit>> offsetsToCommitByTopic, ui64 correlationId = 0) {
                auto message = std::make_shared<NKafka::TTxnOffsetCommitRequestData>();
                message->TransactionalId = TransactionalId;
                message->ProducerId = ProducerId;
                message->ProducerEpoch = ProducerEpoch;
                for (auto& [topicName, commits]: offsetsToCommitByTopic) {
                    NKafka::TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic topic;
                    topic.Name = topicName;
                    for (const auto& commit : commits) {
                        NKafka::TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition partition;
                        partition.PartitionIndex = commit.Partition;
                        partition.CommittedOffset = commit.Offset;
                        partition.CommittedLeaderEpoch = commit.ConsumerEpoch;
                        topic.Partitions.push_back(partition);
                    }
                    message->Topics.push_back(topic);
                }
                auto event = MakeHolder<NKafka::TEvKafka::TEvTxnOffsetCommitRequest>(correlationId, NKafka::TMessagePtr<NKafka::TTxnOffsetCommitRequestData>({}, message), Ctx->Edge);
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
            }

            void SendEndTxnRequest(ui64 correlationId = 0) {
                auto message = std::make_shared<NKafka::TEndTxnRequestData>();
                message->TransactionalId = TransactionalId;
                message->ProducerId = ProducerId;
                message->ProducerEpoch = ProducerEpoch;
                auto event = MakeHolder<NKafka::TEvKafka::TEvEndTxnRequest>(correlationId, NKafka::TMessagePtr<NKafka::TEndTxnRequestData>({}, message), Ctx->Edge);
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
            }

            void AddObserverForRequestToKqpWithAssert(std::function<void(NKikimr::NKqp::TEvKqp::TEvQueryRequest)> callback) {
                auto observer = [&callback](TAutoPtr<IEventHandle>& input) {
                    if (auto* event = input->CastAsLocal<NKikimr::NKqp::TEvKqp::TEvQueryRequest>()) {
                        callback(event);
                    }

                    return TTestActorRuntimeBase::EEventAction::PROCESS;
                };
                Ctx->Runtime->SetObserverFunc(observer);
            }
    };

    Y_UNIT_TEST_SUITE_F(KafkaTransactionActor, TTransactionActorFixture) {
        Y_UNIT_TEST(OnAddPartitionsAndEndTxn_shouldSendTxnToKqpWithSpecifiedPartitions) {
            TVector<TTopicPartitions> topics = {{"topic1", {0, 1}}, {"topic2", {0}}};
            AddObserverForRequestToKqpWithAssert({true, true, topics, {}});

            SendAddPartitionsToTxnRequest(topics);
            SendEndTxnRequest();

            UNIT_ASSERT_C(false, "Not implemented yet");
        }

        Y_UNIT_TEST(OnAddPartitions_shouldReturnOkToSDK) {
            UNIT_ASSERT_C(false, "Not implemented yet");
        }

        Y_UNIT_TEST(OnDoubleAddPartitionsWithSamePartitionsAndEndTxn_shouldSendTxnToKqpWithOnceSpecifiedPartitions) {
            UNIT_ASSERT_C(false, "Not implemented yet");
        }

        Y_UNIT_TEST(OnDoubleAddPartitionsWithDifferentPartitionsAndEndTxn_shouldSendTxnToKqpWithAllSpecifiedPartitions) {
            UNIT_ASSERT_C(false, "Not implemented yet");
        }

        Y_UNIT_TEST(OnAddOffsetsToTxnAndEndTxn_shouldSendTxnToKqpWithSpecifiedOffsets) {
            UNIT_ASSERT_C(false, "Not implemented yet");
        }

        Y_UNIT_TEST(OnAddOffsetsToTxn_shouldReturnOkToSDK) {
            UNIT_ASSERT_C(false, "Not implemented yet");
        }

        Y_UNIT_TEST(OnTxnOffsetCommit_shouldReturnOkToSDK) {
            UNIT_ASSERT_C(false, "Not implemented yet");
        }

        Y_UNIT_TEST(OnPoisonPillAfterCommitStart_shouldSendAbortToKqp) {
            UNIT_ASSERT_C(false, "Not implemented yet");
        }

        Y_UNIT_TEST(OnAddOffsetsToTxnAndAddPartitionsAndEndTxn_shouldSendTxnToKqpCorrectTxn) {
            UNIT_ASSERT_C(false, "Not implemented yet");
        }

        Y_UNIT_TEST(OnEndTxnWithAbort_shouldSendOkAndDie) {
            UNIT_ASSERT_C(false, "Not implemented yet");
        }
    }
} // namespace