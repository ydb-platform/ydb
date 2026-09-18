#include <ydb/core/kafka_proxy/actors/kafka_produce_actor.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_records.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/aclib/aclib.h>

#include <limits>

namespace {
    using namespace NKafka;

    class TDummySchemeCacheActor : public TActor<TDummySchemeCacheActor> {
        public:
            enum EEv {
                EvReplyTopicNotFound = EventSpaceBegin(TEvents::ES_PRIVATE),
            };

            struct TEvReplyTopicNotFound : public TEventLocal<TEvReplyTopicNotFound, EvReplyTopicNotFound> {
            };

            TDummySchemeCacheActor(ui64 pqTabletId) :
                TActor<TDummySchemeCacheActor>(&TDummySchemeCacheActor::StateFunc),
                PqTabletId(pqTabletId) {}

            void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev, const TActorContext&) {
                auto response = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
                for (auto& requestedEntry : ev->Get()->Request->ResultSet) {
                    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
                    entry.Status = ReplyTopicNotFound
                        ? NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown
                        : NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
                    entry.Path = requestedEntry.Path;
                    auto groupInfo = std::make_unique<NKikimr::NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo>();
                    groupInfo->Description.MutablePQTabletConfig()->SetMeteringMode(NKikimrPQ::TPQTabletConfig_EMeteringMode_METERING_MODE_REQUEST_UNITS);
                    auto partitionDesc = groupInfo->Description.AddPartitions();
                    partitionDesc->SetPartitionId(0);
                    partitionDesc->SetTabletId(PqTabletId);
                    entry.PQGroupInfo = TIntrusivePtr<NKikimr::NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo>(groupInfo.release());
                    entry.SecurityObject = MakeIntrusive<NKikimr::TSecurityObject>("owner@builtin", TString{}, false);
                    entry.TableId = {};
                    response->ResultSet.push_back(entry);
                }
                Send(ev->Sender, MakeHolder<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(response.release()));
            }

            void Handle(TEvReplyTopicNotFound::TPtr&, const TActorContext&) {
                SetReplyTopicNotFound(true);
            }

        private:
            STFUNC(StateFunc) {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
                    HFunc(TEvReplyTopicNotFound, Handle);
                }
            }

            void SetReplyTopicNotFound(bool value) {
                ReplyTopicNotFound = value;
            }

            ui64 PqTabletId;
            bool ReplyTopicNotFound = false;
        };

    class TProduceActorFixture : public NUnitTest::TBaseFixture {

        public:
            TMaybe<NKikimr::NPQ::TTestContext> Ctx;
            TActorId ActorId;
            TContext::TPtr KafkaContext;
            const TString Database = "/Root/PQ";
            const TString TopicName = "topic"; // as specified in pq_ut_common
            const TString TopicPath = "/Root/PQ/my-topic";
            const NKikimrConfig::TKafkaProxyConfig KafkaConfig = {};
            const TString KeyToProduce = "record-key";
            const TString ValueToProduce = "record-value";
            TString TransactionalId = "123";

            void SetUp(NUnitTest::TTestContext&) override {
                Ctx.ConstructInPlace();

                Ctx->Prepare();
                PQTabletPrepare({.partitions=4}, {}, *Ctx);
                Ctx->Runtime->SetScheduledLimit(5'000);
                Ctx->Runtime->DisableBreakOnStopCondition();
                Ctx->Runtime->SetLogPriority(NKikimrServices::KAFKA_PROXY, NLog::PRI_TRACE);
                Ctx->Runtime->SetLogPriority(NKikimrServices::PQ_WRITE_PROXY, NLog::PRI_TRACE);
                Ctx->Runtime->SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_DEBUG);
                KafkaContext = std::make_shared<TContext>(KafkaConfig);
                KafkaContext->DatabasePath = "/Root/PQ";
                KafkaContext->ResourceDatabasePath = "/Root/PQ";
                KafkaContext->ConnectionId = Ctx->Edge;
                ActorId = Ctx->Runtime->Register(CreateKafkaProduceActor(KafkaContext));
                auto dummySchemeCacheId = Ctx->Runtime->Register(new TDummySchemeCacheActor(Ctx->TabletId));
                Ctx->Runtime->RegisterService(MakeSchemeCacheID(), dummySchemeCacheId);
            }

            void TearDown(NUnitTest::TTestContext&) override  {
                Ctx->Finalize();
            }

            void SendProduce(TMaybe<TString> transactionalId = {}, ui64 producerId = 0, ui16 producerEpoch = 0, i32 baseSequence = 0, i32 partitionIndex = 0, i64 baseTimestamp = 0, i64 timestampDelta = 0, ECompressionType compression = ECompressionType::NONE) {
                SendProduceToPartitions({partitionIndex}, transactionalId, producerId, producerEpoch, baseSequence, baseTimestamp, timestampDelta, compression);
            }

            void SendProduceToPartitions(
                    const std::vector<i32>& partitionIndexes,
                    TMaybe<TString> transactionalId = {},
                    ui64 producerId = 0,
                    ui16 producerEpoch = 0,
                    i32 baseSequence = 0,
                    i64 baseTimestamp = 0,
                    i64 timestampDelta = 0,
                    ECompressionType compression = ECompressionType::NONE) {
                auto message = std::make_shared<NKafka::TProduceRequestData>();
                if (transactionalId) {
                    message->TransactionalId = transactionalId->data();
                }
                NKafka::TProduceRequestData::TTopicProduceData topicData;
                topicData.Name = "my-topic";
                NKafka::TKafkaRecords records(std::in_place);
                records->ProducerId = producerId;
                records->ProducerEpoch = producerEpoch;

                records->BaseOffset = 3;
                records->BaseSequence = baseSequence;
                records->Magic = 2; // Current supported
                records->BaseTimestamp = baseTimestamp;
                records->Attributes = static_cast<i16>(compression);
                records->Records.resize(1);
                records->Records[0].TimestampDelta = timestampDelta;
                records->Records[0].Key = TKafkaRawBytes(KeyToProduce.data(), KeyToProduce.size());
                records->Records[0].Value = TKafkaRawBytes(ValueToProduce.data(), ValueToProduce.size());

                const TString serializedRecords = WriteKafkaRecordBatch(*records);
                auto recordsBuffer = std::make_shared<TBuffer>(serializedRecords.data(), serializedRecords.size());
                for (i32 partitionIndex : partitionIndexes) {
                    NKafka::TProduceRequestData::TTopicProduceData::TPartitionProduceData partitionData;
                    partitionData.Index = partitionIndex;
                    partitionData.Records = TKafkaRawBytes(recordsBuffer->data(), recordsBuffer->size());
                    topicData.PartitionData.push_back(partitionData);
                }
                message->TopicData.push_back(topicData);
                auto event = MakeHolder<TEvKafka::TEvProduceRequest>(0, NKafka::TMessagePtr<NKafka::TProduceRequestData>(recordsBuffer, message));
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
            }

            void AssertCorrectOptsInPartitionWriter(const TActorId& writerId, const TProducerInstanceId& producerInstanceId, const TMaybe<TString>& transactionalId) {
                const auto* writer = dynamic_cast<NKikimr::NPQ::TPartitionWriterOpts::IGetter*>(Ctx->Runtime->FindActor(writerId));
                UNIT_ASSERT(writer);
                const TPartitionWriterOpts& writerOpts = writer->GetOpts();

                UNIT_ASSERT_VALUES_EQUAL(*writerOpts.KafkaProducerInstanceId, producerInstanceId);
                if (transactionalId) {
                    UNIT_ASSERT_VALUES_EQUAL(*writerOpts.KafkaTransactionalId, *transactionalId);
                } else {
                    UNIT_ASSERT(transactionalId.Empty());
                }
            }

            THolder<TEvPersQueue::TEvResponse> CreateMissingSupPartitionErrorResponse(ui64 cookie) {
                auto event = MakeHolder<TEvPersQueue::TEvResponse>();
                NKikimrClient::TResponse record;
                record.SetErrorReason("expected test error");
                record.SetErrorCode(::NPersQueue::NErrorCode::EErrorCode::KAFKA_TRANSACTION_MISSING_SUPPORTIVE_PARTITION);
                record.MutablePartitionResponse()->SetCookie(cookie);
                event->Record = record;
                return event;
            }

            void SetSchemeCacheReplyTopicNotFound() {
                auto ev = std::make_unique<TDummySchemeCacheActor::TEvReplyTopicNotFound>();
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(MakeSchemeCacheID(), Ctx->Edge, ev.release()));
            }

            NSchemeCache::TDescribeResult::TPtr MakeDescribeResult(
                    const std::vector<ui32>& partitionIds,
                    bool withSelf,
                    const TString& owner = {},
                    const TString& effectiveAcl = {}) {
                NKikimrScheme::TEvDescribeSchemeResult proto;
                auto* pqGroup = proto.MutablePathDescription()->MutablePersQueueGroup();
                for (ui32 partitionId : partitionIds) {
                    auto* partition = pqGroup->AddPartitions();
                    partition->SetPartitionId(partitionId);
                    partition->SetTabletId(Ctx->TabletId);
                }
                if (withSelf) {
                    auto* self = proto.MutablePathDescription()->MutableSelf();
                    self->SetOwner(owner);
                    self->SetEffectiveACL(effectiveAcl);
                }
                return NSchemeCache::TDescribeResult::Create(std::move(proto));
            }

            void SendWatchNotifyUpdated(NSchemeCache::TDescribeResult::TCPtr result) {
                auto ev = MakeHolder<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(
                    0, TopicPath, TPathId{}, std::move(result));
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, ev.Release()));
            }

            void SendWatchNotifyDeleted() {
                auto ev = MakeHolder<TEvTxProxySchemeCache::TEvWatchNotifyDeleted>(0, TopicPath, TPathId{});
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, ev.Release()));
            }

            THolder<NKafka::TEvKafka::TEvResponse> GrabProduceResponse() {
                auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
                UNIT_ASSERT(response != nullptr);
                return response;
            }
        };

    Y_UNIT_TEST_SUITE_F(ProduceActor, TProduceActorFixture) {

        Y_UNIT_TEST(OnProduceWithTransactionalIdAndNewEpoch_shouldRegisterNewPartitionWriterAndSendPoisonPillToOld) {
            i64 producerId = 0;
            i32 producerEpoch = 0;
            TActorId writeRequestReceiver;
            TActorId poisonPillReceiver;
            ui32 writeRequestsCounter = 0;
            ui32 poisonPillCounter = 0;
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                Cout << input->ToString() << Endl;
                if (input->CastAsLocal<TEvPartitionWriter::TEvWriteRequest>()) {
                    writeRequestReceiver = input->Recipient;
                    writeRequestsCounter++;
                } else if (input->CastAsLocal<TEvents::TEvPoison>()) {
                    if (poisonPillCounter == 0) { // only first poison pill goes to writer
                        poisonPillReceiver = input->Recipient;
                        poisonPillCounter++;
                    } // we are not interested in all subsequent
                }

                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };
            Ctx->Runtime->SetObserverFunc(observer);

            // produce with initial producer epoch
            SendProduce(TransactionalId, producerId, producerEpoch);
            TDispatchOptions options;
            options.CustomFinalCondition = [&writeRequestsCounter]() {
                return writeRequestsCounter > 0;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
            TActorId firstPartitionWriterId = writeRequestReceiver;
            AssertCorrectOptsInPartitionWriter(firstPartitionWriterId, {producerId, producerEpoch}, TransactionalId);

            auto firstResponse = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(firstResponse->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);

            // produce with new epoch
            SendProduce(TransactionalId, producerId, producerEpoch + 1);

            // assert we registered new writer for new producer epoch
            TDispatchOptions options2;
            options2.CustomFinalCondition = [&writeRequestsCounter]() {
                return writeRequestsCounter > 1;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options2));
            TActorId secondPartitionWriterId = writeRequestReceiver;
            UNIT_ASSERT_VALUES_UNEQUAL(secondPartitionWriterId, firstPartitionWriterId);
            AssertCorrectOptsInPartitionWriter(secondPartitionWriterId, {producerId, producerEpoch + 1}, TransactionalId);

            // assert we send poison pill to old writer
            TDispatchOptions options3;
            options3.CustomFinalCondition = [&poisonPillCounter]() {
                return poisonPillCounter > 0;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options3));
            UNIT_ASSERT_VALUES_EQUAL(poisonPillReceiver, firstPartitionWriterId);
        }

        Y_UNIT_TEST(OnProduceWithTransactionalId_andLostMessagesError_shouldRecreatePartitionWriterAndRetryProduce) {
            i64 producerId = 1;
            i32 producerEpoch = 2;
            TActorId firstWriteRequestReceiver;
            TActorId poisonPillReceiver;
            TActorId secondWriteRequestReceiver;
            int writeRequestsCounter = 0;
            int poisonPillCounter = 0;
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                Cout << input->ToString() << Endl;
                if (input->CastAsLocal<TEvPartitionWriter::TEvWriteRequest>()) {
                    if (writeRequestsCounter == 0) {
                        firstWriteRequestReceiver = input->Recipient;
                        AssertCorrectOptsInPartitionWriter(firstWriteRequestReceiver, {producerId, producerEpoch}, TransactionalId);
                    } else if (writeRequestsCounter == 1) {
                        secondWriteRequestReceiver = input->Recipient;
                        AssertCorrectOptsInPartitionWriter(secondWriteRequestReceiver, {producerId, producerEpoch}, TransactionalId);
                    }
                    writeRequestsCounter++;
                } else if (auto* event = input->CastAsLocal<TEvPersQueue::TEvRequest>()) {
                    if (event->Record.GetPartitionRequest().HasCmdReserveBytes()) {
                        Ctx->Runtime->Send(new IEventHandle(firstWriteRequestReceiver, input->Sender, CreateMissingSupPartitionErrorResponse(event->Record.GetPartitionRequest().GetCookie()).Release()));
                        return TTestActorRuntimeBase::EEventAction::DROP;
                    }
                } else if (input->CastAsLocal<TEvents::TEvPoison>()) {
                    if (poisonPillCounter == 0) { // only first poison pill goes to writer
                        poisonPillReceiver = input->Recipient;
                        poisonPillCounter++;
                    } // we are not interested in all subsequent
                }

                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };
            Ctx->Runtime->SetObserverFunc(observer);

            SendProduce(TransactionalId, producerId, producerEpoch);

            TDispatchOptions options;
            options.CustomFinalCondition = [&writeRequestsCounter, &poisonPillCounter]() {
                return writeRequestsCounter > 1 && poisonPillCounter > 0;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

            UNIT_ASSERT_VALUES_UNEQUAL(firstWriteRequestReceiver, secondWriteRequestReceiver);
            UNIT_ASSERT_VALUES_EQUAL(firstWriteRequestReceiver, poisonPillReceiver);
        }

        Y_UNIT_TEST(OnSecondProduceWhileInFlight_ShouldNotSendResponse) {
            ui32 writeRequests = 0;
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                if (input->CastAsLocal<TEvPartitionWriter::TEvWriteRequest>()) {
                    ++writeRequests;
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };
            Ctx->Runtime->SetObserverFunc(observer);

            SendProduce();
            TDispatchOptions options;
            options.CustomFinalCondition = [&writeRequests]() {
                return writeRequests > 0;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

            SendProduce();
            auto unexpected = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>(TDuration::MilliSeconds(200));
            UNIT_ASSERT(!unexpected);
        }

        Y_UNIT_TEST(OnMissingSupportivePartitionRetryFail_ShouldRespondImmediately) {
            Ctx->Runtime->SetDispatchTimeout(TDuration::Seconds(5));

            struct TCapturedWrite {
                TActorId ProduceActor;
                ui64 Cookie;
            };
            std::vector<TCapturedWrite> writes;
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                if (auto* writeRequest = input->CastAsLocal<TEvPartitionWriter::TEvWriteRequest>()) {
                    writes.push_back({input->Sender, writeRequest->GetCookie()});
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };
            Ctx->Runtime->SetObserverFunc(observer);

            SendProduce(TransactionalId, 1, 2);
            TDispatchOptions options;
            options.CustomFinalCondition = [&writes]() {
                return !writes.empty();
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

            // Empty PQ group: THashChooser indexes by partition id, so {1} alone
            // is out of range. GetPartition(0) then returns null and Recreate fails locally.
            SendWatchNotifyUpdated(MakeDescribeResult({}, false));
            Ctx->Runtime->DispatchEvents(TDispatchOptions{}, TDuration::MilliSeconds(100));

            NKikimrClient::TResponse record;
            record.SetErrorCode(::NPersQueue::NErrorCode::EErrorCode::KAFKA_TRANSACTION_MISSING_SUPPORTIVE_PARTITION);
            record.MutablePartitionResponse()->SetCookie(writes[0].Cookie);
            auto ev = MakeHolder<TEvPartitionWriter::TEvWriteResponse>("", "", std::move(record));
            Ctx->Runtime->Send(new IEventHandle(writes[0].ProduceActor, Ctx->Edge, ev.Release()));

            auto response = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);
            UNIT_ASSERT_VALUES_EQUAL(
                std::dynamic_pointer_cast<NKafka::TProduceResponseData>(response->Response)->Responses[0].PartitionResponses[0].ErrorCode,
                NKafka::EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);
        }

        Y_UNIT_TEST(OnProduceWithoutTransactionalId_shouldNotKillOldWriter) {
            i64 producerId = 0;
            i32 producerEpoch = 0;
            TActorId writeRequestReceiver;
            ui32 writeRequestsCounter = 0;
            ui32 poisonPillCounter = 0;
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                if (input->CastAsLocal<TEvPartitionWriter::TEvWriteRequest>()) {
                    writeRequestReceiver = input->Recipient;
                    writeRequestsCounter++;
                } else if (input->CastAsLocal<TEvents::TEvPoison>()) {
                    poisonPillCounter++;
                }

                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };
            Ctx->Runtime->SetObserverFunc(observer);

            // produce with initial producer epoch
            SendProduce({}, producerId, producerEpoch);
            TDispatchOptions options;
            options.CustomFinalCondition = [&writeRequestsCounter]() {
                return writeRequestsCounter > 0;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
            TActorId firstPartitionWriterId = writeRequestReceiver;
            AssertCorrectOptsInPartitionWriter(firstPartitionWriterId, {producerId, producerEpoch}, {});

            auto firstResponse = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(firstResponse->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);

            // produce with new epoch
            SendProduce({}, producerId, producerEpoch + 1);

            // assert we didn't register new writer for new producer epoch
            TDispatchOptions options2;
            options2.CustomFinalCondition = [&writeRequestsCounter]() {
                return writeRequestsCounter > 1;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options2));
            TActorId secondPartitionWriterId = writeRequestReceiver;
            UNIT_ASSERT_VALUES_EQUAL(secondPartitionWriterId, firstPartitionWriterId);

            // assert we don't send poison pill to the writer
            TDispatchOptions options3;
            options3.CustomFinalCondition = [&poisonPillCounter]() {
                return poisonPillCounter > 0;
            };
            UNIT_ASSERT(!Ctx->Runtime->DispatchEvents(options3, TDuration::Seconds(2)));
        }

        Y_UNIT_TEST(OnWriteExpiredAndWakeUp_ShouldReturnREQUEST_TIMED_OUT) {
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                if (input->CastAsLocal<TEvPartitionWriter::TEvWriteRequest>()) {
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }

                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };
            Ctx->Runtime->SetObserverFunc(observer);

            SendProduce();

            Ctx->Runtime->AdvanceCurrentTime(TDuration::Seconds(31));

            Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, new TEvKafka::TEvWakeup()));

            auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();

            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::REQUEST_TIMED_OUT);
        }

        Y_UNIT_TEST(OnWriteExpired_CompletedPartitionKeepsNONE_ERROR) {
            SendProduce({}, 0, 0, 0, 0);
            auto first = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(first->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);

            SendWatchNotifyUpdated(MakeDescribeResult({0, 1}, false));

            ui32 writeResponses = 0;
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                if (auto* writeRequest = input->CastAsLocal<TEvPartitionWriter::TEvWriteRequest>()) {
                    if (writeRequest->Record.GetPartitionRequest().GetPartition() == 1) {
                        return TTestActorRuntimeBase::EEventAction::DROP;
                    }
                } else if (input->Recipient == ActorId && input->CastAsLocal<TEvPartitionWriter::TEvWriteResponse>()) {
                    ++writeResponses;
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };
            Ctx->Runtime->SetObserverFunc(observer);

            SendProduceToPartitions({0, 1});
            TDispatchOptions options;
            options.CustomFinalCondition = [&writeResponses]() {
                return writeResponses > 0;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

            Ctx->Runtime->AdvanceCurrentTime(TDuration::Seconds(31));
            Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, new TEvKafka::TEvWakeup()));

            auto response = GrabProduceResponse();
            const auto produceResponse = std::dynamic_pointer_cast<NKafka::TProduceResponseData>(response->Response);
            UNIT_ASSERT(produceResponse);
            UNIT_ASSERT_VALUES_EQUAL(produceResponse->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(produceResponse->Responses[0].PartitionResponses.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(produceResponse->Responses[0].PartitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(produceResponse->Responses[0].PartitionResponses[0].ErrorCode,
                NKafka::EKafkaErrors::NONE_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(produceResponse->Responses[0].PartitionResponses[1].Index, 1);
            UNIT_ASSERT_VALUES_EQUAL(produceResponse->Responses[0].PartitionResponses[1].ErrorCode,
                NKafka::EKafkaErrors::REQUEST_TIMED_OUT);
        }

        Y_UNIT_TEST(OnWriteExpired_TwoCompletedAndTwoTimedOutPartitions) {
            SendProduce({}, 0, 0, 0, 0);
            auto first = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(first->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);

            SendWatchNotifyUpdated(MakeDescribeResult({0, 1, 2, 3}, false));

            ui32 writeResponses = 0;
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                if (auto* writeRequest = input->CastAsLocal<TEvPartitionWriter::TEvWriteRequest>()) {
                    const ui32 partition = writeRequest->Record.GetPartitionRequest().GetPartition();
                    if (partition == 1 || partition == 3) {
                        return TTestActorRuntimeBase::EEventAction::DROP;
                    }
                } else if (input->Recipient == ActorId && input->CastAsLocal<TEvPartitionWriter::TEvWriteResponse>()) {
                    ++writeResponses;
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };
            Ctx->Runtime->SetObserverFunc(observer);

            SendProduceToPartitions({0, 1, 2, 3});
            TDispatchOptions options;
            options.CustomFinalCondition = [&writeResponses]() {
                return writeResponses >= 2;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

            Ctx->Runtime->AdvanceCurrentTime(TDuration::Seconds(31));
            Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, new TEvKafka::TEvWakeup()));

            auto response = GrabProduceResponse();
            const auto produceResponse = std::dynamic_pointer_cast<NKafka::TProduceResponseData>(response->Response);
            UNIT_ASSERT(produceResponse);
            UNIT_ASSERT_VALUES_EQUAL(produceResponse->Responses.size(), 1);
            const auto& partitionResponses = produceResponse->Responses[0].PartitionResponses;
            UNIT_ASSERT_VALUES_EQUAL(partitionResponses.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(partitionResponses[0].Index, 0);
            UNIT_ASSERT_VALUES_EQUAL(partitionResponses[0].ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(partitionResponses[1].Index, 1);
            UNIT_ASSERT_VALUES_EQUAL(partitionResponses[1].ErrorCode, NKafka::EKafkaErrors::REQUEST_TIMED_OUT);
            UNIT_ASSERT_VALUES_EQUAL(partitionResponses[2].Index, 2);
            UNIT_ASSERT_VALUES_EQUAL(partitionResponses[2].ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(partitionResponses[3].Index, 3);
            UNIT_ASSERT_VALUES_EQUAL(partitionResponses[3].ErrorCode, NKafka::EKafkaErrors::REQUEST_TIMED_OUT);
        }

        Y_UNIT_TEST(OnProduce_andPipeDisconnected) {
            i64 producerId = 1;
            i32 producerEpoch = 2;

            int writeRequestsCounter = 0;
            int poisonPillCounter = 0;

            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                if (input->CastAsLocal<TEvPartitionWriter::TEvWriteRequest>()) {
                    if (writeRequestsCounter++ == 0) {
                        auto r = std::make_unique<TEvPartitionWriter::TEvDisconnected>(TEvPartitionWriter::TEvWriteResponse::EErrorCode::InternalError);
                        Ctx->Runtime->Send(new IEventHandle(input->Sender, input->Recipient, r.release()));
                        return TTestActorRuntimeBase::EEventAction::DROP;
                    }
                } else if (input->CastAsLocal<TEvents::TEvPoison>()) {
                    poisonPillCounter++;
                }

                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };

            Ctx->Runtime->SetObserverFunc(observer);

            SendProduce({}, producerId, producerEpoch);

            auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::NOT_LEADER_OR_FOLLOWER);
            UNIT_ASSERT_VALUES_EQUAL(std::dynamic_pointer_cast<NKafka::TProduceResponseData>(response->Response)->Responses[0].PartitionResponses[0].ErrorCode,
                NKafka::EKafkaErrors::NOT_LEADER_OR_FOLLOWER);
        }

        Y_UNIT_TEST(OnWriteResponseError_ShouldFailRemainingCookiesOfSameWriter) {
            Ctx->Runtime->SetDispatchTimeout(TDuration::Seconds(5));

            struct TCapturedWrite {
                TActorId ProduceActor;
                ui64 Cookie;
            };
            std::vector<TCapturedWrite> writes;
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                if (auto* writeRequest = input->CastAsLocal<TEvPartitionWriter::TEvWriteRequest>()) {
                    writes.push_back({input->Sender, writeRequest->GetCookie()});
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };
            Ctx->Runtime->SetObserverFunc(observer);

            SendProduceToPartitions({0, 0}, {}, 1, 0, 1);

            TDispatchOptions options;
            options.CustomFinalCondition = [&writes]() {
                return writes.size() >= 2;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
            UNIT_ASSERT_VALUES_EQUAL(writes.size(), 2);

            NKikimrClient::TResponse record;
            record.MutablePartitionResponse()->SetCookie(writes[0].Cookie);
            auto ev = MakeHolder<TEvPartitionWriter::TEvWriteResponse>(
                "",
                "",
                TEvPartitionWriter::TEvWriteResponse::EErrorCode::InternalError,
                "test write error",
                std::move(record));
            Ctx->Runtime->Send(new IEventHandle(writes[0].ProduceActor, Ctx->Edge, ev.Release()));

            auto response = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::UNKNOWN_SERVER_ERROR);
            const auto produceResponse = std::dynamic_pointer_cast<NKafka::TProduceResponseData>(response->Response);
            UNIT_ASSERT(produceResponse);
            UNIT_ASSERT_VALUES_EQUAL(produceResponse->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(produceResponse->Responses[0].PartitionResponses.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(produceResponse->Responses[0].PartitionResponses[0].ErrorCode,
                NKafka::EKafkaErrors::UNKNOWN_SERVER_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(produceResponse->Responses[0].PartitionResponses[1].ErrorCode,
                NKafka::EKafkaErrors::UNKNOWN_SERVER_ERROR);
        }

        Y_UNIT_TEST(OnProduce_TimestampOverflowIsNotAParseError) {
            i32 sequence = 0;
            for (const bool batchingEnabled : {false, true}) {
                Ctx->Runtime->GetAppData().FeatureFlags.SetEnableTopicMessagesBatching(batchingEnabled);
                Ctx->Runtime->GetAppData().FeatureFlags.SetEnableTopicWriteOffsetDeltaInKeys(batchingEnabled);
                for (const auto compression : {ECompressionType::NONE, ECompressionType::GZIP, ECompressionType::ZSTD}) {
                    if (!batchingEnabled && compression != ECompressionType::NONE) {
                        continue;
                    }
                    for (const auto baseTimestamp : {std::numeric_limits<i64>::min(), std::numeric_limits<i64>::max()}) {
                        const i64 delta = baseTimestamp == std::numeric_limits<i64>::min() ? -1 : 1;
                        SendProduce({}, 1, 0, sequence++, 0, baseTimestamp, delta, compression);
                        auto response = GrabProduceResponse();
                        UNIT_ASSERT(response);
                        UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, EKafkaErrors::NONE_ERROR);
                        const auto produceResponse = std::dynamic_pointer_cast<TProduceResponseData>(response->Response);
                        UNIT_ASSERT(produceResponse);
                        UNIT_ASSERT_VALUES_EQUAL(produceResponse->Responses[0].PartitionResponses[0].ErrorCode,
                            EKafkaErrors::NONE_ERROR);
                    }
                }
            }
        }

        Y_UNIT_TEST(OnProduce_ManyRequests) {
            i64 producerId = 1;
            i32 producerEpoch = 2;

            SendProduce({}, producerId, producerEpoch, 1);
            {
                auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
                UNIT_ASSERT(response);
                UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
                UNIT_ASSERT_VALUES_EQUAL(std::dynamic_pointer_cast<NKafka::TProduceResponseData>(response->Response)->Responses[0].PartitionResponses[0].ErrorCode,
                    NKafka::EKafkaErrors::NONE_ERROR);
            }

            SendProduce({}, producerId, producerEpoch, 2);
            {
                auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
                UNIT_ASSERT(response);
                UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
                UNIT_ASSERT_VALUES_EQUAL(std::dynamic_pointer_cast<NKafka::TProduceResponseData>(response->Response)->Responses[0].PartitionResponses[0].ErrorCode,
                    NKafka::EKafkaErrors::NONE_ERROR);
            }
        }

        Y_UNIT_TEST(OnUnknownTopic_ShouldReturnUNKNOWN_TOPIC_OR_PARTITION) {
            const i64 producerId = 0;
            const i32 producerEpoch = 0;

            KafkaContext->Token.UserToken = new NACLib::TUserToken("user@builtin", TVector<TString>{});
            SetSchemeCacheReplyTopicNotFound();

            SendProduce(TransactionalId, producerId, producerEpoch + 0);
            auto response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);

            SendProduce(TransactionalId, producerId, producerEpoch + 1);
            response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);

            SendProduce(TransactionalId, producerId, producerEpoch + 2);
            response = Ctx->Runtime->GrabEdgeEvent<NKafka::TEvKafka::TEvResponse>();
            UNIT_ASSERT(response != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);
        }

        Y_UNIT_TEST(WatchNotifyUpdatedWithoutSelfDoesNotFailInFlightProduce) {
            KafkaContext->Token.UserToken = new NACLib::TUserToken("owner@builtin", TVector<TString>{});

            SendProduce();
            auto first = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(first->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);

            SendWatchNotifyUpdated(MakeDescribeResult({0}, false));
            SendProduce();
            auto second = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(second->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
        }

        Y_UNIT_TEST(WatchNotifyUpdatedWithEmptyAclDoesNotDenyAccess) {
            KafkaContext->Token.UserToken = new NACLib::TUserToken("owner@builtin", TVector<TString>{});

            SendProduce();
            auto first = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(first->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);

            SendWatchNotifyUpdated(MakeDescribeResult({0}, true, "owner@builtin", {}));
            SendProduce();
            auto second = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(second->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
        }

        Y_UNIT_TEST(WatchNotifyUpdatedWithBrokenAclDoesNotAbort) {
            KafkaContext->Token.UserToken = new NACLib::TUserToken("owner@builtin", TVector<TString>{});

            SendProduce();
            auto first = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(first->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);

            SendWatchNotifyUpdated(MakeDescribeResult({0}, true, "owner@builtin", TString("\xFF\xFF\xFF\x0F")));
            SendProduce();
            auto second = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(second->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
        }

        Y_UNIT_TEST(WatchNotifyUpdatedRebuildsPartitionChooser) {
            SendProduce({}, 0, 0, 0, 0);
            auto first = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(first->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);

            SendProduce({}, 0, 0, 0, 1);
            auto missing = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(missing->ErrorCode, NKafka::EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);

            SendWatchNotifyUpdated(MakeDescribeResult({0, 1}, false));
            SendProduce({}, 0, 0, 0, 1);
            auto added = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(added->ErrorCode, NKafka::EKafkaErrors::NONE_ERROR);
        }

        Y_UNIT_TEST(WatchNotifyDeletedFailsPendingWritesWithoutTimeout) {
            ui32 writeRequests = 0;
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                if (input->CastAsLocal<TEvPartitionWriter::TEvWriteRequest>()) {
                    ++writeRequests;
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };
            Ctx->Runtime->SetObserverFunc(observer);
            Ctx->Runtime->SetDispatchTimeout(TDuration::Seconds(5));

            SendProduce();
            TDispatchOptions options;
            options.CustomFinalCondition = [&writeRequests]() {
                return writeRequests > 0;
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

            SendWatchNotifyDeleted();
            auto response = GrabProduceResponse();
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, NKafka::EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);
        }
    }
} // anonymous namespace
