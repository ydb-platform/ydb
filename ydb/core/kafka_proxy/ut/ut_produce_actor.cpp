#include <ydb/core/kafka_proxy/actors/kafka_produce_actor.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/persqueue/writer/writer.cpp>

namespace {
    using namespace NKafka;

    class TDummySchemeCacheActor : public TActor<TDummySchemeCacheActor> {
        public:
            TDummySchemeCacheActor(ui64 pqTabletId) :
                TActor<TDummySchemeCacheActor>(&TDummySchemeCacheActor::StateFunc),
                PqTabletId(pqTabletId) {}

            void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev, const TActorContext&) {
                auto response = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
                for (auto& requestedEntry : ev->Get()->Request->ResultSet) {
                    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
                    entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
                    entry.Path = requestedEntry.Path;
                    auto groupInfo = std::make_unique<NKikimr::NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo>();
                    groupInfo->Description.MutablePQTabletConfig()->SetMeteringMode(NKikimrPQ::TPQTabletConfig_EMeteringMode_METERING_MODE_REQUEST_UNITS);
                    auto partitionDesc = groupInfo->Description.AddPartitions();
                    partitionDesc->SetPartitionId(0);
                    partitionDesc->SetTabletId(PqTabletId);
                    entry.PQGroupInfo = TIntrusivePtr<NKikimr::NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo>(groupInfo.release());
                    entry.TableId = {};
                    response->ResultSet.push_back(entry);
                }
                Send(ev->Sender, MakeHolder<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(response.release()));
            }

        private:
            STFUNC(StateFunc) {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
                }
            }

            ui64 PqTabletId;
        };

    class TProduceActorFixture : public NUnitTest::TBaseFixture {

        public:
            TMaybe<NKikimr::NPQ::TTestContext> Ctx;
            TActorId ActorId;
            const TString Database = "/Root/PQ";
            const TString TopicName = "topic"; // as specified in pq_ut_common
            const NKikimrConfig::TKafkaProxyConfig KafkaConfig = {};
            const TString KeyToProduce = "record-key";
            const TString ValueToProduce = "record-value";
            TString TransactionalId = "123";

            void SetUp(NUnitTest::TTestContext&) override {
                Ctx.ConstructInPlace();

                Ctx->Prepare();
                PQTabletPrepare({.partitions=1}, {}, *Ctx);
                Ctx->Runtime->SetScheduledLimit(5'000);
                Ctx->Runtime->DisableBreakOnStopCondition();
                Ctx->Runtime->SetLogPriority(NKikimrServices::KAFKA_PROXY, NLog::PRI_TRACE);
                Ctx->Runtime->SetLogPriority(NKikimrServices::PQ_WRITE_PROXY, NLog::PRI_TRACE);
                TContext::TPtr kafkaContext = std::make_shared<TContext>(KafkaConfig);
                kafkaContext->DatabasePath = "/Root/PQ";
                ActorId = Ctx->Runtime->Register(CreateKafkaProduceActor(kafkaContext));
                auto dummySchemeCacheId = Ctx->Runtime->Register(new TDummySchemeCacheActor(Ctx->TabletId));
                Ctx->Runtime->RegisterService(MakeSchemeCacheID(), dummySchemeCacheId);
            }

            void TearDown(NUnitTest::TTestContext&) override  {
                Ctx->Finalize();
            }

            void SendProduce(TMaybe<TString> transactionalId = {}, ui64 producerId = 0, ui16 producerEpoch = 0, i32 baseSequence = 0) {
                auto message = std::make_shared<NKafka::TProduceRequestData>();
                if (transactionalId) {
                    message->TransactionalId = transactionalId->data();
                }
                NKafka::TProduceRequestData::TTopicProduceData topicData;
                topicData.Name = "my-topic";
                NKafka::TProduceRequestData::TTopicProduceData::TPartitionProduceData partitionData;
                partitionData.Index = 0;
                NKafka::TKafkaRecords records(std::in_place);
                records->ProducerId = producerId;
                records->ProducerEpoch = producerEpoch;

                TKafkaRecordBatch batch;
                records->BaseOffset = 3;
                records->BaseSequence = baseSequence;
                records->Magic = 2; // Current supported
                records->Records.resize(1);
                records->Records[0].Key = TKafkaRawBytes(KeyToProduce.data(), KeyToProduce.size());
                records->Records[0].Value = TKafkaRawBytes(ValueToProduce.data(), ValueToProduce.size());

                partitionData.Records = records;
                topicData.PartitionData.push_back(partitionData);
                message->TopicData.push_back(topicData);
                auto event = MakeHolder<TEvKafka::TEvProduceRequest>(0, NKafka::TMessagePtr<NKafka::TProduceRequestData>({}, message));
                Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
            }

            void AssertCorrectOptsInPartitionWriter(const TActorId& writerId, const TProducerInstanceId& producerInstanceId, const TMaybe<TString>& transactionalId) {
                NKikimr::NPQ::TPartitionWriter* writer = dynamic_cast<NKikimr::NPQ::TPartitionWriter*>(Ctx->Runtime->FindActor(writerId));
                const TPartitionWriterOpts& writerOpts = writer->GetOpts();

                UNIT_ASSERT_VALUES_EQUAL(*writerOpts.KafkaProducerInstanceId, producerInstanceId);
                if (transactionalId) {
                    UNIT_ASSERT_VALUES_EQUAL(*writerOpts.KafkaTransactionalId, *transactionalId);
                } else {
                    UNIT_ASSERT(transactionalId.Empty());
                }
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
                if (auto* event = input->CastAsLocal<TEvPartitionWriter::TEvWriteRequest>()) {
                    writeRequestReceiver = input->Recipient;
                    writeRequestsCounter++;
                } else if (auto* event = input->CastAsLocal<TEvents::TEvPoison>()) {
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

        Y_UNIT_TEST(OnProduceWithoutTransactionalId_shouldNotKillOldWriter) {
            i64 producerId = 0;
            i32 producerEpoch = 0;
            TActorId writeRequestReceiver;
            ui32 writeRequestsCounter = 0;
            ui32 poisonPillCounter = 0;
            auto observer = [&](TAutoPtr<IEventHandle>& input) {
                if (auto* event = input->CastAsLocal<TEvPartitionWriter::TEvWriteRequest>()) {
                    writeRequestReceiver = input->Recipient;
                    writeRequestsCounter++;
                } else if (auto* event = input->CastAsLocal<TEvents::TEvPoison>()) {
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
    }
} // anonymous namespace
