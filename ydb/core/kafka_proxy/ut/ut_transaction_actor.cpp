#include <ydb/core/kafka_proxy/actors/kafka_transaction_actor.h>
#include <ydb/core/kafka_proxy/kafka_events.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/fwd.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/kqp/common/events/events.h>

namespace {
    class TTransactionActorFixture : public NUnitTest::TBaseFixture {
        public:
            struct TTopicPartition {
                TString Topic;
                TVector<ui32> Partitions;
            };

            struct TPartitionCommit {
                ui32 Partition; 
                ui64 Offset;
                TString ConsumerName;
                ui64 ConsumerEpoch;
            };

            struct TQueryRequestMatcher {
                bool CommitTx;
                bool BeginTx;
                TVector<TTopicPartition> TopicPartitions;
                std::unordered_map<TString, std::vector<TPartitionCommit>> OffsetsToCommitByTopic;
            };

            using TopicPartition = TKafkaApiOperationsRequest::KafkaApiPartitionInTxn;

            struct TopicPartitionHashFn {
                size_t operator()(const TopicPartition& partition) const {
                    return std::hash<TString>()(partition.TopicPath) ^ std::hash<int64_t>()(partition.PartitionId);
                }
            };

            TMaybe<NKikimr::NPQ::TTestContext> Ctx;
            TActorId ActorId;
            const TString Database = "/Root/PQ";
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

            void AddObserverForRequestToKqpWithAssert(std::function<void(const NKikimr::NKqp::TEvKqp::TEvQueryRequest&)> callback) {
                auto observer = [&callback](TAutoPtr<IEventHandle>& input) {
                    if (auto* event = input->CastAsLocal<NKikimr::NKqp::TEvKqp::TEvQueryRequest>()) {
                        callback(*event);
                    }

                    return TTestActorRuntimeBase::EEventAction::PROCESS;
                };
                Ctx->Runtime->SetObserverFunc(observer);
            }

            void MatchQueryRequest(const NKikimr::NKqp::TEvKqp::TEvQueryRequest& request, const TQueryRequestMatcher& matcher) {
                UNIT_ASSERT_VALUES_EQUAL(request.Record.GetRequest().GetTxControl().begin_tx().has_serializable_read_write(), matcher.BeginTx);
                UNIT_ASSERT_VALUES_EQUAL(request.Record.GetRequest().GetTxControl().commit_tx(), matcher.CommitTx);
                UNIT_ASSERT_VALUES_EQUAL(request.Record.GetRequest().GetKafkaApiOperations().GetTransactionalId(), TransactionalId);
                UNIT_ASSERT_VALUES_EQUAL(request.Record.GetRequest().GetKafkaApiOperations().GetProducerId(), ProducerId);
                UNIT_ASSERT_VALUES_EQUAL(request.Record.GetRequest().GetKafkaApiOperations().GetProducerEpoch(), ProducerEpoch);
                
                MatchPartitionsInTxn(request, matcher);
                MatchOffsetsInTxn(request, matcher);
            }

        private:
            void MatchPartitionsInTxn(const NKikimr::NKqp::TEvKqp::TEvQueryRequest& request, const TQueryRequestMatcher& matcher) {
                UNIT_ASSERT_VALUES_EQUAL(request.Record.GetRequest().GetKafkaApiOperations().partitionsInTxn_size(), matcher.TopicPartitions.size());
                std::set<TopicPartition, TopicPartitionHashFn> paritionsInRequest;
                request.Record.GetRequest().GetKafkaApiOperations().partitionsInTxn().UnpackTo(&paritionsInRequest);

                for (ui32 i = 0; i < matcher.TopicPartitions.size(); i++) {
                    auto& topicPartition = matcher.TopicPartitions[i];
                    TString expectedTopicPath = GetExpectedTopicPath(topicPartition.Topic);
                    auto& partitionsInRequest = request.Record.GetRequest().GetTopicOperations().GetTopics().Get(i).Getpartitions();
                    UNIT_ASSERT_VALUES_EQUAL(partitionsInRequest.size(), topicPartition.Partitions.size());
                    for (i32 partition : topicPartition.Partitions) {
                        UNIT_ASSERT(paritionsInRequest.contains(TopicPartition{expectedTopicPath, partition});
                    }
                }
            }

            void MatchOffsetsInTxn(const NKikimr::NKqp::TEvKqp::TEvQueryRequest& request, const TQueryRequestMatcher& matcher) {
                std::unordered_map<TopicPartition, TKafkaApiOperationsRequest::KafkaApiOffsetInTxn> offsetsInRequest;
                for (auto& offsetInTxn : request.Record.GetRequest().GetKafkaApiOperations().OffsetsInTxn()) {
                    offsetsInRequest[TopicPartition{offsetInTxn.GetTopicPath(), offsetInTxn.GetPartitionId()}] = offsetInTxn;
                }

                for (auto& [topicName, partitionCommit]: matcher.OffsetsToCommitByTopic) {
                    UNIT_ASSERT(offsetsInRequest.contains(offsetsInRequest[TopicPartition{GetExpectedTopicPath(topicName), partitionCommit.Partition}]);
                    auto& offsetInRequest = offsetsInRequest[TopicPartition{GetExpectedTopicPath(topicName), partitionCommit.Partition}];
                    UNIT_ASSERT_VALUES_EQUAL(offsetInRequest.GetOffset(), partitionCommit.Offset);
                    UNIT_ASSERT_VALUES_EQUAL(offsetInRequest.GetConsumerName(), partitionCommit.ConsumerName);
                    UNIT_ASSERT_VALUES_EQUAL(offsetInRequest.GetConsumerGeneration(), partitionCommit.ConsumerEpoch);
                }
            }

            std::vector<TopicPartition> ConvertToTopicPartitionVector(const TVector<TTopicPartitions>& topicPartitionsList) {
                std::vector<TopicPartition> result;
                for (auto& topicPartitions : topicPartitionsList) {
                    for (auto& partition : topicPartitions.Partitions) {
                        result.push_back({topicPartitions.Topic, partition});
                    }
                }
                return result;
            }

            TString GetExpectedTopicPath(const TString& topicName) {
                return TStringBuilder() << Database << "/" << topicName;
            }
    };

    Y_UNIT_TEST_SUITE_F(KafkaTransactionActor, TTransactionActorFixture) {
        Y_UNIT_TEST(OnAddPartitionsAndEndTxn_shouldSendTxnToKqpWithSpecifiedPartitions) {
            TVector<TTopicPartitions> topics = {{"topic1", {0, 1}}, {"topic2", {0}}};
            bool seenEvent = false;
            AddObserverForRequestToKqpWithAssert([&](const NKikimr::NKqp::TEvKqp::TEvQueryRequest& request) {
                seenEvent = true;
                MatchQueryRequest(request, {true, true, topics});
            });

            SendAddPartitionsToTxnRequest(topics);
            SendEndTxnRequest();

            TDispatchOptions options;
            options.CustomFinalCondition = [&seenEvent]() {
                return seenEvent;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
        }

        Y_UNIT_TEST(OnAddPartitionsToTxn_shouldReturnOkToSDK) {
            TVector<TTopicPartitions> topics = {{"topic1", {0, 1}}, {"topic2", {0}}};
            ui64 correlationId = 123;

            SendAddPartitionsToTxnRequest(topics, correlationId);
            // will respond to edge, cause we provieded edge actorId as a connectionId in SendAddPartitionsToTxnRequest
            auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();

            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
            UNIT_ASSERT_EQUAL(response->Response->ApiKey(), NKafka::EApiKey::ADD_PARTITIONS_TO_TXN);
            const auto& message = static_cast<const NKafka::TAddPartitionsToTxnResponseData&>(*response->Response);
            UNIT_ASSERT_VALUES_EQUAL(response->CorrelationId, correlationId);
            UNIT_ASSERT_VALUES_EQUAL(message.Results.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(message.Results[0].Name, "topic1");
            UNIT_ASSERT_VALUES_EQUAL(message.Results[0].Results.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(message.Results[0].Results[0].PartitionIndex, 0);
            UNIT_ASSERT_EQUAL(message.Results[0].Results[0].ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(message.Results[0].Results[1].PartitionIndex, 1);
            UNIT_ASSERT_EQUAL(message.Results[0].Results[1].ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(message.Results[1].Name, "topic2");
            UNIT_ASSERT_VALUES_EQUAL(message.Results[1].Results.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(message.Results[1].Results[0].PartitionIndex, 0);
            UNIT_ASSERT_EQUAL(message.Results[1].Results[0].ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
        }

        Y_UNIT_TEST(OnDoubleAddPartitionsWithSamePartitionsAndEndTxn_shouldSendTxnToKqpWithOnceSpecifiedPartitions) {
            TVector<TTopicPartitions> topics = {{"topic1", {0}}};
            bool seenEvent = false;
            AddObserverForRequestToKqpWithAssert([&](const NKikimr::NKqp::TEvKqp::TEvQueryRequest& request) {
                seenEvent = true;
                MatchQueryRequest(request, {true, true, topics});
            });

            SendAddPartitionsToTxnRequest(topics);
            SendAddPartitionsToTxnRequest(topics);
            SendEndTxnRequest();

            TDispatchOptions options;
            options.CustomFinalCondition = [&seenEvent]() {
                return seenEvent;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
        }

        Y_UNIT_TEST(OnDoubleAddPartitionsWithDifferentPartitionsAndEndTxn_shouldSendTxnToKqpWithAllSpecifiedPartitions) {
            // implement after specifiying how many requests to KQP i should send
            UNIT_ASSERT_C(false, "Not implemented yet");
        }

        Y_UNIT_TEST(OnAddOffsetsToTxnAndEndTxn_shouldSendTxnToKqpWithSpecifiedOffsets) {
            // implement after specifiying how to commit offsets
            UNIT_ASSERT_C(false, "Not implemented yet");
        }

        Y_UNIT_TEST(OnTxnOffsetCommit_shouldReturnOkToSDK) {
            std::unordered_map<TString, std::vector<TPartitionCommit>> offsetsToCommitByTopic;
            offsetsToCommitByTopic["topic1"] = {{0, 0, 0}};
            offsetsToCommitByTopic["topic2"] = {{0, 10, 0}, {1, 5, 0}};
            ui64 correlationId = 123;

            SendTxnOffsetCommitRequest(offsetsToCommitByTopic, correlationId);
            // will respond to edge, cause we provieded edge actorId as a connectionId in SendAddPartitionsToTxnRequest
            auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();

            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
            UNIT_ASSERT_EQUAL(response->Response->ApiKey(), NKafka::EApiKey::TXN_OFFSET_COMMIT);
            const auto& message = static_cast<const NKafka::TTxnOffsetCommitResponseData&>(*response->Response);
            UNIT_ASSERT_VALUES_EQUAL(response->CorrelationId, correlationId);
            UNIT_ASSERT_VALUES_EQUAL(message.Topics.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[0].Name, "topic1");
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[0].Partitions[0].PartitionIndex, 0);
            UNIT_ASSERT_EQUAL(message.Topics[0].Partitions[0].ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[1].Name, "topic2");
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[1].Partitions.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[1].Partitions[0].PartitionIndex, 0);
            UNIT_ASSERT_EQUAL(message.Topics[1].Partitions[0].ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[1].Partitions.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(message.Topics[1].Partitions[1].PartitionIndex, 1);
            UNIT_ASSERT_EQUAL(message.Topics[1].Partitions[1].ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
        }

        Y_UNIT_TEST(OnAddOffsetsToTxnAndAddPartitionsAndEndTxn_shouldSendTxnToKqpCorrectTxn) {
            // implement after understanding how many requests i should send
            UNIT_ASSERT_C(false, "Not implemented yet");
        }

        Y_UNIT_TEST(OnEndTxnWithAbort_shouldSendOkAndDie) {
            auto request = std::make_shared<NKafka::TEndTxnRequestData>();
            request->Committed = false;
            ui64 correlationId = 123;
            auto event = MakeHolder<NKafka::TEvKafka::TEvEndTxnRequest>(correlationId, NKafka::TMessagePtr<NKafka::TEndTxnRequestData>({}, request), Ctx->Edge);
            Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));

            auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();

            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
            UNIT_ASSERT_EQUAL(response->Response->ApiKey(), NKafka::EApiKey::END_TXN);
            const auto& result = static_cast<const NKafka::TEndTxnResponseData&>(*response->Response);
            UNIT_ASSERT_VALUES_EQUAL(response->CorrelationId, correlationId);
            UNIT_ASSERT_VALUES_EQUAL(result.ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
        }
    }
} // namespace
