#include <ydb/core/kafka_proxy/actors/kafka_transaction_actor.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/kafka_proxy/kafka_transactions_coordinator.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/fwd.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>

namespace {
    using namespace NKafka;
    class TDummyKqpActor : public TActor<TDummyKqpActor> {
        public:
            TDummyKqpActor() : TActor<TDummyKqpActor>(&TDummyKqpActor::StateFunc) {}

            void SetValidationResponse(const TString& transactionalId, i64 producerId, i32 producerEpoch, const std::unordered_map<TString, i32>& consumerGenerations = {}) {
                TransactionalIdToReturn = transactionalId;
                ProducerIdToReturn = producerId;
                ProducerEpochToReturn = producerEpoch;
                ConsumerGenerationsToReturn = consumerGenerations;
            }

            void SetCommitResponse(bool success) {
                ReturnSuccessOnCommit = success;
            }

        private:
            STFUNC(StateFunc) {
                switch (ev->GetTypeRewrite()) {
                    HFunc(NKqp::TEvKqp::TEvCreateSessionRequest, Handle);
                    HFunc(NKqp::TEvKqp::TEvQueryRequest, Handle);
                }
            }

            void Handle(NKqp::TEvKqp::TEvCreateSessionRequest::TPtr& ev, const TActorContext& ctx) {
                auto response = MakeHolder<TEvKqp::TEvCreateSessionResponse>();
                response->Record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
                response->Record.MutableResponse()->SetSessionId("123");
                Send(new IEventHandle(ev->Sender, ctx.SelfID, response.Release(), 0, ev->Cookie));
            }

            void Handle(NKqp::TEvKqp::TEvQueryRequest::TPtr& ev, const TActorContext& ctx) {
                Cout << "Handling query request" << Endl;
                THolder<NKqp::TEvKqp::TEvQueryResponse> response;
                if (ev->Get()->Record.GetRequest().GetTxControl().commit_tx()) {
                    Cout << "Sending response on commit from dummy kqp" << Endl;
                    response = MakeSimpleSuccessResponse();
                } else if (ev->Get()->Record.GetRequest().HasKafkaApiOperations()) {
                    Cout << "Sending response on add kafka operations from dummy kqp" << Endl;
                    response = MakeSimpleSuccessResponse();
                } else {
                    Cout << "Sending response on select from dummy kqp" << Endl;
                    response = MakeResponseOnSelectFromKqp();
                }
                Send(new IEventHandle(
                    ev->Sender, 
                    ctx.SelfID, 
                    response.Release(),
                    0, 
                    ev->Cookie
                ));
            }

            THolder<NKqp::TEvKqp::TEvQueryResponse> MakeSimpleSuccessResponse() {
                auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
                NKikimrKqp::TEvQueryResponse record;
                if (ReturnSuccessOnCommit) {
                    record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
                } else {
                    record.SetYdbStatus(Ydb::StatusIds::ABORTED);
                }
                return response;
            }

            THolder<NKqp::TEvKqp::TEvQueryResponse> MakeResponseOnSelectFromKqp() {
                auto response = MakeHolder<NKqp::TEvKqp::TEvQueryResponse>();
                NKikimrKqp::TEvQueryResponse record;

                record.SetYdbStatus(Ydb::StatusIds::SUCCESS);
                auto* producerState = record.MutableResponse()->AddYdbResults();
                *producerState = CreateProducerStateResultsSet();
                auto* consumersState = record.MutableResponse()->AddYdbResults();
                *consumersState = CreateConsumersStatesResultSet();
                
                response->Record = record;
                return response;
            }

            Ydb::ResultSet CreateProducerStateResultsSet() {
                const std::string resultSetString = TStringBuilder() <<
                    "columns {\n"
                    "  name: \"transactional_id\"\n"
                    "  type {\n"
                    "    type_id: UTF8\n" 
                    "  }\n"
                    "}\n"
                    "columns {\n"
                    "  name: \"producer_id\"\n"
                    "  type {\n"
                    "    type_id: INT64\n"
                    "  }\n"
                    "}\n"
                    "columns {\n"
                    "  name: \"producer_epoch\"\n"
                    "  type {\n"
                    "    type_id: INT16\n"
                    "  }\n"
                    "}\n"
                    "rows {\n"
                    "  items {\n"
                    "    text_value: \"" << TransactionalIdToReturn << "\"\n"
                    "  }\n"
                    "  items {\n"
                    "    int64_value: " << ProducerIdToReturn << "\n"
                    "  }\n"
                    "  items {\n"
                    "    int32_value: " << ProducerEpochToReturn << "\n"
                    "  }\n"
                    "}\n";
                Ydb::ResultSet rsProto;
                google::protobuf::TextFormat::ParseFromString(NYdb::TStringType{resultSetString}, &rsProto);
                return rsProto;
            }

            Ydb::ResultSet CreateConsumersStatesResultSet() {
                TStringBuilder builder;
                builder << 
                    "columns {\n"
                    "  name: \"consumer_group\"\n"
                    "  type {\n"
                    "    type_id: UTF8\n"
                    "  }\n"
                    "}\n"
                    "columns {\n"
                    "  name: \"generation\"\n"
                    "  type {\n"
                    "    type_id: UINT64\n"
                    "  }\n"
                    "}\n";

                for (auto& [consumerName, generation] : ConsumerGenerationsToReturn) {
                    builder <<
                        "rows {\n"
                        "  items {\n"
                        "    text_value: \"" << consumerName << "\"\n"
                        "  }\n"
                        "  items {\n"
                        "    uint64_value: " << generation << "\n"
                        "  }\n"
                        "}\n";
                }
                Ydb::ResultSet rsProto;
                google::protobuf::TextFormat::ParseFromString(NYdb::TStringType{builder}, &rsProto);
                return rsProto;
            }

            TString TransactionalIdToReturn = "";
            i64 ProducerIdToReturn = 0;
            i32 ProducerEpochToReturn = 0;
            std::unordered_map<TString, i32> ConsumerGenerationsToReturn = {};
            bool ReturnSuccessOnCommit = true;
        };

    class TTransactionActorFixture : public NUnitTest::TBaseFixture {

        public:
            struct TTopicPartitions {
                TString Topic;
                TVector<ui32> Partitions;
            };

            struct TConsumerCommitMatcher {
                TString ConsumerName;
                i32 GenerationId;
                // map, where key - topicName, value - vector with pairs, 
                // where left - partition index, right - consumer offset for this partition
                std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> PartitionOffsetsByTopic;
            };

            struct TQueryRequestMatcher {
                TVector<TTopicPartitions> TopicPartitions;
                TVector<TConsumerCommitMatcher> ConsumerCommitMatchers;
            };

            struct TCommitRequest {
                TString ConsumerName;
                i32 GenerationId;
                // map, where key - topicName, value - vector with pairs, 
                // where left - partition index, right - consumer offset for this partition
                std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> PartitionOffsetsToCommitByTopic;
            };

            using KafkaApiOffsetInTxn = NKikimrKqp::TKafkaApiOperationsRequest::KafkaApiOffsetInTxn;

            TMaybe<NKikimr::NPQ::TTestContext> Ctx;
            TActorId ActorId;
            TDummyKqpActor* DummyKqpActor = nullptr;
            TActorId KqpActorId;
            const TString Database = "/Root/PQ";
            const TString TransactionalId = "123"; // transactional id from kafka SDK
            const i64 ProducerId = 1;
            const i32 ProducerEpoch = 1;
            ui32 QueryRequestsCounter = 0;
            
            void SetUp(NUnitTest::TTestContext&) override {
                Ctx.ConstructInPlace();
                
                Ctx->Prepare();
                Ctx->Runtime->SetScheduledLimit(5'000);
                Ctx->Runtime->DisableBreakOnStopCondition();
                Ctx->Runtime->SetLogPriority(NKikimrServices::KAFKA_PROXY, NLog::PRI_DEBUG);
                DummyKqpActor = new TDummyKqpActor();
                KqpActorId = Ctx->Runtime->Register(DummyKqpActor);
                Ctx->Runtime->RegisterService(MakeKqpProxyID(Ctx->Runtime->GetNodeId()), KqpActorId);
                Ctx->Runtime->RegisterService(MakeTransactionsServiceID(Ctx->Runtime->GetNodeId()), Ctx->Edge);
                ActorId = Ctx->Runtime->Register(new TTransactionActor(
                    TransactionalId,
                    {ProducerId, ProducerEpoch},
                    Database,
                    5000            
                ));
                DummyKqpActor->SetValidationResponse(TransactionalId, ProducerId, ProducerEpoch);
            }

            void TearDown(NUnitTest::TTestContext&) override  {
                QueryRequestsCounter = 0;
                Ctx->Finalize();
            }

            THolder<NKafka::TEvKafka::TEvResponse> SendAddPartitionsToTxnRequest(std::vector<TTopicPartitions> topicPartitions, ui64 correlationId = 0) {
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
                auto event = MakeHolder<NKafka::TEvKafka::TEvAddPartitionsToTxnRequest>(correlationId, NKafka::TMessagePtr<NKafka::TAddPartitionsToTxnRequestData>({}, message), Ctx->Edge, Database);

                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));

                return Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
            }

            THolder<NKafka::TEvKafka::TEvResponse> SendTxnOffsetCommitRequest(const TCommitRequest& commitRequest, ui64 correlationId = 0) {
                auto message = std::make_shared<NKafka::TTxnOffsetCommitRequestData>();
                message->TransactionalId = TransactionalId;
                message->ProducerId = ProducerId;
                message->ProducerEpoch = ProducerEpoch;
                message->GroupId = commitRequest.ConsumerName;
                message->GenerationId = commitRequest.GenerationId;
                for (auto& [topicName, commits]: commitRequest.PartitionOffsetsToCommitByTopic) {
                    NKafka::TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic topic;
                    topic.Name = topicName;
                    for (const auto& commit : commits) {
                        NKafka::TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition partition;
                        partition.PartitionIndex = commit.first;
                        partition.CommittedOffset = commit.second;
                        topic.Partitions.push_back(partition);
                    }
                    message->Topics.push_back(topic);
                }
                auto event = MakeHolder<NKafka::TEvKafka::TEvTxnOffsetCommitRequest>(correlationId, NKafka::TMessagePtr<NKafka::TTxnOffsetCommitRequestData>({}, message), Ctx->Edge, Database);

                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));

                return Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
            }

            THolder<NKafka::TEvKafka::TEvResponse> SendEndTxnRequest(bool commit = false, ui64 correlationId = 0) {
                auto message = std::make_shared<NKafka::TEndTxnRequestData>();
                message->TransactionalId = TransactionalId;
                message->ProducerId = ProducerId;
                message->ProducerEpoch = ProducerEpoch;
                message->Committed = commit;
                auto event = MakeHolder<NKafka::TEvKafka::TEvEndTxnRequest>(correlationId, NKafka::TMessagePtr<NKafka::TEndTxnRequestData>({}, message), Ctx->Edge, Database);
                
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));

                return Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
            }

            void SendPoisonPill() {
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, new NActors::TEvents::TEvPoison()));
            }

            // Arguments:
            // 1. callback - function, that will be called on recieving the response from KQP om commit request
            // 2. consumerGenerationsToReturnInValidationRequest - map of consumer name to its generation to ensure proper validation of consumer state by actor
            void AddObserverForAddOperationsRequest(std::function<void(const TEvKqp::TEvQueryRequest*)> callback, std::unordered_map<TString, i32> consumerGenerationsToReturnInValidationRequest = {}) {
                DummyKqpActor->SetValidationResponse(TransactionalId, ProducerId, ProducerEpoch, consumerGenerationsToReturnInValidationRequest);

                auto observer = [callback = std::move(callback), this](TAutoPtr<IEventHandle>& input) {
                    // handle query request
                    if (auto* event = input->CastAsLocal<TEvKqp::TEvQueryRequest>()) {
                        // first request is a validation request with select statements
                        // second request should be a commit request
                        if (QueryRequestsCounter == 0) {
                            QueryRequestsCounter++;
                        } else {
                            callback(event);
                        }
                    }

                    return TTestActorRuntimeBase::EEventAction::PROCESS;
                };

                Ctx->Runtime->SetObserverFunc(observer);
            }

            void MatchQueryRequest(const NKikimr::NKqp::TEvKqp::TEvQueryRequest* request, const TQueryRequestMatcher& matcher) {
                UNIT_ASSERT_VALUES_EQUAL(request->Record.GetRequest().GetTxControl().begin_tx().has_serializable_read_write(), false);
                UNIT_ASSERT_VALUES_EQUAL(request->Record.GetRequest().GetAction(), NKikimrKqp::QUERY_ACTION_TOPIC);
                UNIT_ASSERT_VALUES_EQUAL(request->Record.GetRequest().GetKafkaApiOperations().GetProducerId(), ProducerId);
                UNIT_ASSERT_VALUES_EQUAL(request->Record.GetRequest().GetKafkaApiOperations().GetProducerEpoch(), ProducerEpoch);
                
                MatchPartitionsInTxn(request, matcher);
                MatchOffsetsInTxn(request, matcher);
            }

        private:
            void MatchPartitionsInTxn(const NKikimr::NKqp::TEvKqp::TEvQueryRequest* request, const TQueryRequestMatcher& matcher) {
                auto& partitionsInRequest = request->Record.GetRequest().GetKafkaApiOperations().GetPartitionsInTxn();
                std::unordered_set<TTopicPartition, TTopicPartitionHashFn> paritionsInRequestSet;
                paritionsInRequestSet.reserve(partitionsInRequest.size());
                for (auto& partition : partitionsInRequest) {
                    paritionsInRequestSet.emplace(partition.GetTopicPath(), partition.GetPartitionId());
                };

                ui32 totalExpectedPartitions = 0;
                for (ui32 i = 0; i < matcher.TopicPartitions.size(); i++) {
                    auto& topicPartition = matcher.TopicPartitions[i];
                    TString expectedTopicPath = GetExpectedTopicPath(topicPartition.Topic);
                    for (auto partition : topicPartition.Partitions) {
                        totalExpectedPartitions++;
                        UNIT_ASSERT(paritionsInRequestSet.contains({expectedTopicPath, partition}));
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(request->Record.GetRequest().GetKafkaApiOperations().PartitionsInTxnSize(), totalExpectedPartitions);
            }

            void MatchOffsetsInTxn(const NKikimr::NKqp::TEvKqp::TEvQueryRequest* request, const TQueryRequestMatcher& matcher) {
                auto& offsetCommitsInRequest = request->Record.GetRequest().GetKafkaApiOperations().GetOffsetsInTxn();

                for (auto& consumerCommitMatcher : matcher.ConsumerCommitMatchers) {
                    for (auto& [topicName, partitionOffsets] : consumerCommitMatcher.PartitionOffsetsByTopic) {
                        for (auto& partitionOffset : partitionOffsets) {
                            auto it = std::find_if(offsetCommitsInRequest.begin(), offsetCommitsInRequest.end(), [&](const KafkaApiOffsetInTxn& offsetCommit) {
                                return offsetCommit.GetTopicPath() == GetExpectedTopicPath(topicName) 
                                    && offsetCommit.GetPartitionId() == partitionOffset.first
                                    && offsetCommit.GetConsumerName() == consumerCommitMatcher.ConsumerName;
                            });
    
                            UNIT_ASSERT_C(it != offsetCommitsInRequest.end(), TStringBuilder() << 
                                "Consumer commit for consuemr group " << consumerCommitMatcher.ConsumerName <<
                                " and topic partition " << topicName << "-" << partitionOffset.first <<
                                " not found in txn");
                            UNIT_ASSERT_VALUES_EQUAL(it->GetTopicPath(), GetExpectedTopicPath(topicName));
                            UNIT_ASSERT_VALUES_EQUAL(it->GetPartitionId(), partitionOffset.first);
                            UNIT_ASSERT_VALUES_EQUAL(it->GetOffset(), partitionOffset.second);
                            UNIT_ASSERT_VALUES_EQUAL(it->GetConsumerName(), consumerCommitMatcher.ConsumerName);
                        }
                    }
                }
            }

            TString GetExpectedTopicPath(const TString& topicName) {
                return TStringBuilder() << Database << "/" << topicName;
            }
    };

    Y_UNIT_TEST_SUITE_F(TransactionActor, TTransactionActorFixture) {
        Y_UNIT_TEST(OnAddPartitionsAndEndTxn_shouldSendTxnToKqpWithSpecifiedPartitions) {
            TVector<TTopicPartitions> topics = {{"topic1", {0, 1}}, {"topic2", {0}}};
            bool seenEvent = false;
            AddObserverForAddOperationsRequest([&](const NKikimr::NKqp::TEvKqp::TEvQueryRequest* request) {
                MatchQueryRequest(request, {topics, {}});
                seenEvent = true;
            });

            SendAddPartitionsToTxnRequest(topics);
            SendEndTxnRequest(true);

            TDispatchOptions options;
            options.CustomFinalCondition = [&seenEvent]() {
                return seenEvent;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
            UNIT_ASSERT(seenEvent);
        }

        Y_UNIT_TEST(OnAddPartitionsToTxn_shouldReturnOkToSDK) {
            TVector<TTopicPartitions> topics = {{"topic1", {0, 1}}, {"topic2", {0}}};
            ui64 correlationId = 123;

            auto response = SendAddPartitionsToTxnRequest(topics, correlationId);

            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
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

        Y_UNIT_TEST(OnDoubleAddPartitionsWithSamePartitionsAndEndTxn_shouldSendTxnToKqpKafkaOperationsWithOnceSpecifiedPartitions) {
            TVector<TTopicPartitions> topics = {{"topic1", {0}}};
            bool seenEvent = false;
            AddObserverForAddOperationsRequest([&](const NKikimr::NKqp::TEvKqp::TEvQueryRequest* request) {
                MatchQueryRequest(request, {topics, {}});
                seenEvent = true;
            });

            SendAddPartitionsToTxnRequest(topics);
            SendAddPartitionsToTxnRequest(topics);
            SendEndTxnRequest(true);

            TDispatchOptions options;
            options.CustomFinalCondition = [&seenEvent]() {
                return seenEvent;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
            UNIT_ASSERT(seenEvent);
        }

        Y_UNIT_TEST(OnDoubleAddPartitionsWithDifferentPartitionsAndEndTxn_shouldSendTxnToKqKafkaOperationspWithAllSpecifiedPartitions) {
            TVector<TTopicPartitions> topics1 = {{"topic1", {0}}};
            TVector<TTopicPartitions> topics2 = {{"topic2", {0}}};
            bool seenEvent = false;
            AddObserverForAddOperationsRequest([&](const NKikimr::NKqp::TEvKqp::TEvQueryRequest* request) {
                MatchQueryRequest(request, {{{"topic1", {0}}, {"topic2", {0}}}, {}});
                seenEvent = true;
            });

            SendAddPartitionsToTxnRequest(topics1);
            SendAddPartitionsToTxnRequest(topics2);
            SendEndTxnRequest(true);

            TDispatchOptions options;
            options.CustomFinalCondition = [&seenEvent]() {
                return seenEvent;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
            UNIT_ASSERT(seenEvent);
        }

        Y_UNIT_TEST(OnTxnOffsetCommit_shouldSendTxnToKqpWithSpecifiedOffsets) {
            TString consumerName = "my-consumer";
            i32 consumerGeneration = 0;
            std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> partitionOffsetsToCommitByTopic;
            partitionOffsetsToCommitByTopic["topic1"] = {{0, 0}};
            partitionOffsetsToCommitByTopic["topic2"] = {{0, 10}, {1, 5}};
            std::unordered_map<TString, i32> consumerGenerationByNameToReturnFromKqp;
            consumerGenerationByNameToReturnFromKqp[consumerName] = consumerGeneration;
            bool seenEvent = false;
            AddObserverForAddOperationsRequest([&](const NKikimr::NKqp::TEvKqp::TEvQueryRequest* request) {
                MatchQueryRequest(request, {{}, {{consumerName, consumerGeneration, partitionOffsetsToCommitByTopic}}});
                seenEvent = true;
            }, consumerGenerationByNameToReturnFromKqp);

            SendTxnOffsetCommitRequest({consumerName, consumerGeneration, partitionOffsetsToCommitByTopic});
            SendEndTxnRequest(true);

            TDispatchOptions options;
            options.CustomFinalCondition = [&seenEvent]() {
                return seenEvent;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
            UNIT_ASSERT(seenEvent);
        }

        Y_UNIT_TEST(OnDoubleTxnOffsetCommitRequest_shouldSendToKqpLatestGeneration) {
            TString consumerName = "my-consumer";
            i32 consumerGenerationFromTable = 1;
            std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> partitionOffsetsToCommitByTopic;
            partitionOffsetsToCommitByTopic["topic1"] = {{0, 0}};
            std::unordered_map<TString, i32> consumerGenerationByNameToReturnFromKqp;
            consumerGenerationByNameToReturnFromKqp[consumerName] = consumerGenerationFromTable;
            bool seenEvent = false;
            AddObserverForAddOperationsRequest([&](const NKikimr::NKqp::TEvKqp::TEvQueryRequest* request) {
                MatchQueryRequest(request, {{}, {{consumerName, consumerGenerationFromTable, partitionOffsetsToCommitByTopic}}});
                seenEvent = true;
            }, consumerGenerationByNameToReturnFromKqp);

            // first request with old generation
            SendTxnOffsetCommitRequest({consumerName, consumerGenerationFromTable - 1, partitionOffsetsToCommitByTopic});
            // second with actual generation
            SendTxnOffsetCommitRequest({consumerName, consumerGenerationFromTable, partitionOffsetsToCommitByTopic});
            SendEndTxnRequest(true);

            TDispatchOptions options;
            options.CustomFinalCondition = [&seenEvent]() {
                return seenEvent;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
            UNIT_ASSERT(seenEvent);
        }

        Y_UNIT_TEST(OnDoubleTxnOffsetCommitRequest_shouldSendToKqpLatestSpecifiedOffsets) {
            TString consumerName = "my-consumer";
            i32 consumerGeneration = 0;
            std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> firstRequestOffsetsByTopic;
            firstRequestOffsetsByTopic["topic1"] = {{0, 0}};
            std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> secondRequestOffsetsByTopic;
            secondRequestOffsetsByTopic["topic1"] = {{0, 1}};
            std::unordered_map<TString, i32> consumerGenerationByNameToReturnFromKqp;
            consumerGenerationByNameToReturnFromKqp[consumerName] = consumerGeneration;
            bool seenEvent = false;
            AddObserverForAddOperationsRequest([&](const NKikimr::NKqp::TEvKqp::TEvQueryRequest* request) {
                // we validate that to KQP are sent offsets from second request
                MatchQueryRequest(request, {{}, {{consumerName, consumerGeneration, secondRequestOffsetsByTopic}}});
                seenEvent = true;
            }, consumerGenerationByNameToReturnFromKqp);

            SendTxnOffsetCommitRequest({consumerName, consumerGeneration, firstRequestOffsetsByTopic});
            SendTxnOffsetCommitRequest({consumerName, consumerGeneration, secondRequestOffsetsByTopic});
            SendEndTxnRequest(true);

            TDispatchOptions options;
            options.CustomFinalCondition = [&seenEvent]() {
                return seenEvent;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
            UNIT_ASSERT(seenEvent);
        }

        Y_UNIT_TEST(OnTxnOffsetCommit_shouldReturnOkToSDK) {
            TString consumerName = "my-consumer";
            i32 consumerGeneration = 0;
            std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> partitionOffsetsToCommitByTopic;
            partitionOffsetsToCommitByTopic["topic1"] = {{0, 0}};
            partitionOffsetsToCommitByTopic["topic2"] = {{0, 10}, {1, 5}};
            ui64 correlationId = 123;

            auto response = SendTxnOffsetCommitRequest({consumerName, consumerGeneration, partitionOffsetsToCommitByTopic}, correlationId);

            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
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

        Y_UNIT_TEST(OnAddOffsetsToTxnAndAddPartitionsAndEndTxn_shouldSendToKqpCorrectAddKafkaOperationsRequest) {
            TVector<TTopicPartitions> topics = {{"topic3", {0}}};
            TString consumerName = "my-consumer";
            i32 consumerGeneration = 0;
            std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> partitionOffsetsToCommitByTopic;
            partitionOffsetsToCommitByTopic["topic1"] = {{0, 0}};
            partitionOffsetsToCommitByTopic["topic2"] = {{0, 10}, {1, 5}};
            std::unordered_map<TString, i32> consumerGenerationByNameToReturnFromKqp;
            consumerGenerationByNameToReturnFromKqp[consumerName] = consumerGeneration;
            bool seenEvent = false;
            AddObserverForAddOperationsRequest([&](const NKikimr::NKqp::TEvKqp::TEvQueryRequest* request) {
                MatchQueryRequest(request, {topics, {{consumerName, consumerGeneration, partitionOffsetsToCommitByTopic}}});
                seenEvent = true;
            }, consumerGenerationByNameToReturnFromKqp);

            SendAddPartitionsToTxnRequest(topics);
            SendTxnOffsetCommitRequest({consumerName, consumerGeneration, partitionOffsetsToCommitByTopic});
            SendEndTxnRequest(true);

            TDispatchOptions options;
            options.CustomFinalCondition = [&seenEvent]() {
                return seenEvent;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
            UNIT_ASSERT(seenEvent);
        }

        Y_UNIT_TEST(OnEndTxnWithAbort_shouldSendOkAndDie) {
            ui64 correlationId = 123;

            auto response = SendEndTxnRequest(false, correlationId);

            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
            UNIT_ASSERT_EQUAL(response->Response->ApiKey(), NKafka::EApiKey::END_TXN);
            const auto& result = static_cast<const NKafka::TEndTxnResponseData&>(*response->Response);
            UNIT_ASSERT_VALUES_EQUAL(response->CorrelationId, correlationId);
            UNIT_ASSERT_VALUES_EQUAL(result.ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
            auto txnActorDiedEvent = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvTransactionActorDied>();
            UNIT_ASSERT(txnActorDiedEvent != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(txnActorDiedEvent->TransactionalId, TransactionalId);
            UNIT_ASSERT_VALUES_EQUAL(txnActorDiedEvent->ProducerState.Id, ProducerId);
            UNIT_ASSERT_VALUES_EQUAL(txnActorDiedEvent->ProducerState.Epoch, ProducerEpoch);
        }

        Y_UNIT_TEST(OnEndTxnWithCommitAndAbortFromTxn_shouldReturnBROKER_NOT_AVAILABLE) {
            ui64 correlationId = 987;
            DummyKqpActor->SetCommitResponse(false);

            auto response = SendEndTxnRequest(true, correlationId);

            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::BROKER_NOT_AVAILABLE);
            UNIT_ASSERT_EQUAL(response->Response->ApiKey(), NKafka::EApiKey::END_TXN);
            const auto& result = static_cast<const NKafka::TEndTxnResponseData&>(*response->Response);
            UNIT_ASSERT_VALUES_EQUAL(response->CorrelationId, correlationId);
            UNIT_ASSERT_VALUES_EQUAL(result.ErrorCode, NKafka::EKafkaErrors::BROKER_NOT_AVAILABLE);
        }

        Y_UNIT_TEST(OnEndTxnWithCommitAndNoConsumerStateFound_shouldReturnINVALID_TXN_STATE) {
            std::unordered_map<TString, std::vector<std::pair<ui32, ui64>>> partitionOffsetsToCommitByTopic;
            partitionOffsetsToCommitByTopic["topic1"] = {{0, 0}};
            std::unordered_map<TString, i32> consumerGenerationByNameToReturnFromKqp; // empty
            DummyKqpActor->SetValidationResponse(TransactionalId, ProducerId, ProducerEpoch, consumerGenerationByNameToReturnFromKqp);

            SendTxnOffsetCommitRequest({"my-consumer", 0, partitionOffsetsToCommitByTopic});
            auto response = SendEndTxnRequest(true);

            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
            UNIT_ASSERT_EQUAL(response->Response->ApiKey(), NKafka::EApiKey::END_TXN);
            const auto& result = static_cast<const NKafka::TEndTxnResponseData&>(*response->Response);
            UNIT_ASSERT_VALUES_EQUAL(result.ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
        }

        Y_UNIT_TEST(OnEndTxnWithCommitAndExpiredProducerState_shouldReturnProducerFenced) {
            DummyKqpActor->SetValidationResponse(TransactionalId, ProducerId, ProducerEpoch + 1);

            SendAddPartitionsToTxnRequest({{"topic1", {0}}});
            auto response = SendEndTxnRequest(true);

            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
            UNIT_ASSERT_EQUAL(response->Response->ApiKey(), NKafka::EApiKey::END_TXN);
            const auto& result = static_cast<const NKafka::TEndTxnResponseData&>(*response->Response);
            UNIT_ASSERT_VALUES_EQUAL(result.ErrorCode, NKafka::EKafkaErrors::PRODUCER_FENCED);
        }

        Y_UNIT_TEST(OnDie_ShouldSendRemoveTransactionActorRequestToTxnCoordinator) {
            SendPoisonPill();

            auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvTransactionActorDied>();

            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(response->TransactionalId, TransactionalId);
            UNIT_ASSERT_VALUES_EQUAL(response->ProducerState.Id, ProducerId);
            UNIT_ASSERT_VALUES_EQUAL(response->ProducerState.Epoch, ProducerEpoch);
        }
    }
} // namespace