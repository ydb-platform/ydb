#include <ydb/core/persqueue/pqtablet/batching/batch_processor.h>
#include <ydb/core/persqueue/pqtablet/batching/consumer_batch_processor.h>
#include <ydb/core/persqueue/public/codecs/kafka.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_messages_int.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_records.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/stream/str.h>

#include <limits>

namespace NKikimr::NPQ::NBatching {
namespace {

using TReadResult = NKikimrClient::TCmdReadResult::TResult;
using NActors::IEventBase;
using NActors::IEventHandle;
using NActors::TActorId;
using NActors::TEvents;
using NActors::TTestBasicRuntime;

NKafka::TKafkaRecord MakeKafkaRecord(
    i64 timestampDelta,
    i64 offsetDelta,
    TStringBuf key,
    TStringBuf value)
{
    NKafka::TKafkaRecord record;
    record.TimestampDelta = timestampDelta;
    record.OffsetDelta = offsetDelta;
    record.SetKey(TString{key});
    record.SetValue(TString{value});
    record.Length = record.Size(2)
        - NKafka::NPrivate::SizeOfVarint<NKafka::TKafkaRecord::LengthMeta::Type>(0);
    return record;
}

TString MakeKafkaBatchPayload(
    NKafka::ECompressionType compression = NKafka::ECompressionType::NONE,
    size_t recordCount = 2,
    size_t valueSize = 8)
{
    NKafka::TKafkaRecordBatch batch;
    batch.BaseOffset = 100;
    batch.Magic = 2;
    batch.Attributes = static_cast<NKafka::TKafkaRecordBatch::AttributesMeta::Type>(compression);
    batch.LastOffsetDelta = recordCount ? recordCount - 1 : 0;
    batch.BaseTimestamp = 1000;
    batch.MaxTimestamp = 1000 + recordCount;
    batch.ProducerId = 42;
    batch.ProducerEpoch = 3;
    batch.BaseSequence = 10;
    const TString value(valueSize, 'v');
    for (size_t i = 0; i < recordCount; ++i) {
        batch.Records.push_back(MakeKafkaRecord(
            static_cast<i64>(i),
            static_cast<i64>(i),
            TStringBuilder() << "k" << i,
            value));
    }
    batch.BatchLength = batch.Size(2)
        - sizeof(NKafka::TKafkaRecordBatch::BaseOffsetMeta::Type)
        - sizeof(NKafka::TKafkaRecordBatch::BatchLengthMeta::Type);
    return NKafka::WriteKafkaRecordBatch(batch);
}

TString SerializeDataChunk(NKikimrPQClient::TDataChunk chunk) {
    TString serialized;
    Y_ENSURE(chunk.SerializeToString(&serialized));
    return serialized;
}

TReadResult MakeKafkaBatchReadResult(TString payload, ui64 offset = 10) {
    NKikimrPQClient::TDataChunk chunk;
    chunk.SetChunkType(NKikimrPQClient::TDataChunk::REGULAR);
    chunk.SetCodec(KafkaBatchCodec());
    chunk.SetData(std::move(payload));
    chunk.SetSeqNo(100);

    TReadResult readResult;
    readResult.SetOffset(offset);
    readResult.SetSeqNo(100);
    readResult.SetLogicalMessageCount(2);
    readResult.SetIsBatch(true);
    readResult.SetData(SerializeDataChunk(std::move(chunk)));
    return readResult;
}

TReadResult MakeCorruptKafkaBatchReadResult(ui64 offset = 10, TString payload = "not-a-kafka-batch") {
    return MakeKafkaBatchReadResult(std::move(payload), offset);
}

TString MakeSnappyKafkaBatchPayload() {
    auto payload = MakeKafkaBatchPayload();
    // Kafka v2 attributes are int16 BE after baseOffset(8)+batchLength(4)+leaderEpoch(4)+magic(1)+crc(4).
    constexpr size_t attributesOffset = 21;
    UNIT_ASSERT(payload.size() > attributesOffset + 1);
    payload[attributesOffset] = 0;
    payload[attributesOffset + 1] = static_cast<char>(NKafka::ECompressionType::SNAPPY);
    return payload;
}

TReadResult MakePlainReadResult(
    ui64 offset,
    TStringBuf payload,
    bool isBatch = false,
    NKikimrPQClient::TDataChunk::EChunkType chunkType = NKikimrPQClient::TDataChunk::REGULAR,
    TVector<std::pair<TString, TString>> meta = {})
{
    NKikimrPQClient::TDataChunk chunk;
    chunk.SetChunkType(chunkType);
    chunk.SetCodec(NPersQueueCommon::RAW);
    if (!payload.empty()) {
        chunk.SetData(TString{payload});
    }
    for (const auto& [key, value] : meta) {
        auto* item = chunk.AddMessageMeta();
        item->set_key(key);
        item->set_value(value);
    }

    TReadResult readResult;
    readResult.SetOffset(offset);
    readResult.SetSeqNo(offset);
    readResult.SetLogicalMessageCount(1);
    readResult.SetIsBatch(isBatch);
    readResult.SetData(SerializeDataChunk(std::move(chunk)));
    return readResult;
}

THolder<TEvPQ::TEvProxyResponse> MakeProxyResponse(TVector<TReadResult> results) {
    auto event = MakeHolder<TEvPQ::TEvProxyResponse>(1, false);
    auto* cmdRead = event->Response->MutablePartitionResponse()->MutableCmdReadResult();
    for (auto& result : results) {
        cmdRead->AddResult()->Swap(&result);
    }
    return event;
}

TReadProcessingContext MakeReadContext(
    TActorId responseActor,
    TVector<TReadResult> results,
    const TString& user = "user",
    ui32 partitionId = 7,
    ui64 offset = 0,
    ui32 count = std::numeric_limits<ui32>::max(),
    ui64 lastOffset = 0)
{
    TReadProcessingContext context;
    context.User = user;
    context.PartitionId = partitionId;
    context.Destination = 11;
    context.Offset = offset;
    context.Count = count;
    context.LastOffset = lastOffset;
    context.ResponseActor = responseActor;
    context.Event.Reset(MakeProxyResponse(std::move(results)).Release());
    return context;
}

const NKikimrClient::TCmdReadResult& GetCmdReadResult(const TReadProcessingContext& context) {
    auto* proxy = dynamic_cast<TEvPQ::TEvProxyResponse*>(context.Event.Get());
    UNIT_ASSERT(proxy);
    UNIT_ASSERT(proxy->Response);
    UNIT_ASSERT(proxy->Response->HasPartitionResponse());
    UNIT_ASSERT(proxy->Response->GetPartitionResponse().HasCmdReadResult());
    return proxy->Response->GetPartitionResponse().GetCmdReadResult();
}

struct TEnv {
    TTestBasicRuntime Runtime;
    TActorId Tablet;
    TActorId Edge;

    explicit TEnv(bool restartOnUnhandledExceptions = false)
        : Runtime(1, false)
    {
        TAppPrepare app;
        app.FeatureFlags.SetEnableTabletRestartOnUnhandledExceptions(restartOnUnhandledExceptions);
        Runtime.Initialize(app.Unwrap());
        Tablet = Runtime.AllocateEdgeActor();
        Edge = Runtime.AllocateEdgeActor();
    }

    TActorId RegisterBatchProcessor() {
        return Runtime.Register(CreateBatchProcessor(42, Tablet));
    }

    TActorId RegisterConsumer(const TString& user = "user") {
        return Runtime.Register(CreateConsumerBatchProcessor(42, Tablet, user));
    }

    void Send(const TActorId& recipient, IEventBase* ev) {
        Runtime.Send(new IEventHandle(recipient, Edge, ev), 0, true);
    }

    void DispatchQuiet() {
        try {
            Runtime.DispatchEvents(NActors::TDispatchOptions(), TDuration::MilliSeconds(50));
        } catch (const NActors::TEmptyEventQueueException&) {
        }
    }

    template <typename TEvent>
    typename TEvent::TPtr Grab() {
        auto ev = Runtime.GrabEdgeEvent<TEvent>(Edge, TDuration::Seconds(10));
        UNIT_ASSERT(ev);
        return ev;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TConsumerBatchProcessorTest) {
    Y_UNIT_TEST(ProcessNonBatchMessages) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(env.Edge, {
            MakePlainReadResult(10, "a"),
            MakePlainReadResult(11, "b"),
        })));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetOffset(), 10u);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(1).GetOffset(), 11u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Context.User, "user");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Context.PartitionId, 7u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Context.Destination, 11u);
    }

    Y_UNIT_TEST(ProcessKafkaBatchCutsRecords) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakeKafkaBatchReadResult(MakeKafkaBatchPayload())},
            "user",
            7,
            10)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetOffset(), 10u);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(1).GetOffset(), 11u);
        UNIT_ASSERT(!results.Get(0).GetIsBatch());
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetLogicalMessageCount(), 1u);
    }

    Y_UNIT_TEST(SkipOffsetsOutsideWindow) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {
                MakePlainReadResult(9, "before"),
                MakePlainReadResult(10, "ok"),
                MakePlainReadResult(12, "after"),
            },
            "user",
            7,
            10,
            std::numeric_limits<ui32>::max(),
            12)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetOffset(), 10u);
    }

    Y_UNIT_TEST(StopAtCountOnNonBatchMessages) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {
                MakePlainReadResult(10, "first"),
                MakePlainReadResult(11, "second"),
            },
            "user",
            7,
            10,
            1)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetOffset(), 10u);
    }

    Y_UNIT_TEST(StopAtCountIncludingMidBatch) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {
                MakePlainReadResult(10, "first"),
                MakeKafkaBatchReadResult(MakeKafkaBatchPayload()),
                MakePlainReadResult(20, "tail"),
            },
            "user",
            7,
            10,
            2)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetOffset(), 10u);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(1).GetOffset(), 10u);
    }

    Y_UNIT_TEST(ZeroCountAddsAllNonBatchMessages) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {
                MakePlainReadResult(10, "a"),
                MakePlainReadResult(11, "b"),
            },
            "user",
            7,
            0,
            0)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        UNIT_ASSERT_VALUES_EQUAL(GetCmdReadResult(ev->Get()->Context).GetResult().size(), 2);
    }

    Y_UNIT_TEST(ZeroCountStopsAfterFirstKafkaBatch) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {
                MakeKafkaBatchReadResult(MakeKafkaBatchPayload()),
                MakePlainReadResult(20, "tail"),
            },
            "user",
            7,
            10,
            0)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetOffset(), 10u);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(1).GetOffset(), 11u);
    }

    Y_UNIT_TEST(ContinuesAfterKafkaBatchWhenCountNotReached) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {
                MakeKafkaBatchReadResult(MakeKafkaBatchPayload()),
                MakePlainReadResult(20, "tail"),
            },
            "user",
            7,
            10,
            4)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(2).GetOffset(), 20u);
    }

    Y_UNIT_TEST(LastOffsetSkipsRecordsInsideKafkaBatch) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakeKafkaBatchReadResult(MakeKafkaBatchPayload())},
            "user",
            7,
            10,
            std::numeric_limits<ui32>::max(),
            11)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetOffset(), 10u);
    }

    Y_UNIT_TEST(UnknownBatchCodecIsKeptAsOriginal) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        auto unknown = MakePlainReadResult(15, "gzip-batch", true);
        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {unknown, MakePlainReadResult(16, "next")},
            "user",
            7,
            15,
            1)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetOffset(), 15u);
        UNIT_ASSERT(results.Get(0).GetIsBatch());
    }

    Y_UNIT_TEST(SkipKafkaRecordsBeforeReadStartOffset) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakeKafkaBatchReadResult(MakeKafkaBatchPayload())},
            "user",
            7,
            11)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetOffset(), 11u);
    }

    Y_UNIT_TEST(CorruptKafkaBatchKeepsOriginalAndReplies) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakeCorruptKafkaBatchReadResult(10)},
            "user",
            7,
            10)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetOffset(), 10u);
        UNIT_ASSERT(results.Get(0).GetIsBatch());
        UNIT_ASSERT(env.Runtime.FindActor(actor));

        env.Send(actor, new TEvProcessBatch(MakeReadContext(env.Edge, {MakePlainReadResult(11, "ok")})));
        UNIT_ASSERT_VALUES_EQUAL(GetCmdReadResult(env.Grab<TEvProcessBatchResult>()->Get()->Context).GetResult().size(), 1);
    }

    Y_UNIT_TEST(UnsupportedSnappyKafkaBatchKeepsOriginalAndReplies) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakeCorruptKafkaBatchReadResult(10, MakeSnappyKafkaBatchPayload())},
            "user",
            7,
            10)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        UNIT_ASSERT(results.Get(0).GetIsBatch());
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetOffset(), 10u);
    }

    Y_UNIT_TEST(TruncatedKafkaBatchKeepsOriginalAndReplies) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        const auto truncated = MakeKafkaBatchPayload().substr(0, 8);
        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakeCorruptKafkaBatchReadResult(12, truncated)},
            "user",
            7,
            12)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        UNIT_ASSERT(results.Get(0).GetIsBatch());
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetOffset(), 12u);
    }

    Y_UNIT_TEST(CorruptKafkaBatchDoesNotPoisonTablet) {
        TEnv env(true);
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakeCorruptKafkaBatchReadResult()},
            "user",
            7,
            10)));

        UNIT_ASSERT(env.Grab<TEvProcessBatchResult>());
        UNIT_ASSERT(env.Runtime.FindActor(actor));

        env.Send(actor, new TEvProcessBatch(MakeReadContext(env.Edge, {MakePlainReadResult(11, "ok")})));
        UNIT_ASSERT_VALUES_EQUAL(GetCmdReadResult(env.Grab<TEvProcessBatchResult>()->Get()->Context).GetResult().size(), 1);
    }

    Y_UNIT_TEST(CorruptKafkaBatchInTheMiddleKeepsCutPrefixAndOriginal) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {
                MakeKafkaBatchReadResult(MakeKafkaBatchPayload(), 10),
                MakeCorruptKafkaBatchReadResult(20),
                MakePlainReadResult(30, "tail"),
            },
            "user",
            7,
            10)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        const auto& results = GetCmdReadResult(ev->Get()->Context).GetResult();
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(results.Get(0).GetOffset(), 10u);
        UNIT_ASSERT(!results.Get(0).GetIsBatch());
        UNIT_ASSERT_VALUES_EQUAL(results.Get(1).GetOffset(), 11u);
        UNIT_ASSERT(!results.Get(1).GetIsBatch());
        UNIT_ASSERT_VALUES_EQUAL(results.Get(2).GetOffset(), 20u);
        UNIT_ASSERT(results.Get(2).GetIsBatch());
        UNIT_ASSERT_VALUES_EQUAL(results.Get(3).GetOffset(), 30u);
        UNIT_ASSERT(!results.Get(3).GetIsBatch());
    }

    Y_UNIT_TEST(CorruptKafkaBatchKeysStillReplies) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        TBatchKeysProcessingContext keys;
        keys.PartitionId = 3;
        keys.ResponseActor = env.Edge;
        keys.Results = {
            MakePlainReadResult(3, "plain", false, NKikimrPQClient::TDataChunk::REGULAR, {
                {TString{MESSAGE_ATTRIBUTE_KEY}, "plain-key"},
            }),
            MakeCorruptKafkaBatchReadResult(10),
            MakeKafkaBatchReadResult(MakeKafkaBatchPayload(), 20),
        };

        env.Send(actor, new TEvProcessBatchKeys(std::move(keys)));

        const auto ev = env.Grab<TEvProcessBatchKeysResult>();
        const auto& offsetToKey = ev->Get()->OffsetToKey;
        UNIT_ASSERT_VALUES_EQUAL(offsetToKey.at(3u), "plain-key");
        UNIT_ASSERT(!offsetToKey.contains(10u));
        UNIT_ASSERT_VALUES_EQUAL(offsetToKey.at(20u), "k0");
        UNIT_ASSERT_VALUES_EQUAL(offsetToKey.at(21u), "k1");
        UNIT_ASSERT(env.Runtime.FindActor(actor));
    }

    Y_UNIT_TEST(CorruptKafkaBatchThroughBatchProcessorReplies) {
        TEnv env(true);
        const auto actor = env.RegisterBatchProcessor();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakeCorruptKafkaBatchReadResult()},
            "alice",
            7,
            10)));

        const auto ev = env.Grab<TEvProcessBatchResult>();
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Context.User, "alice");
        UNIT_ASSERT(GetCmdReadResult(ev->Get()->Context).GetResult().Get(0).GetIsBatch());
        UNIT_ASSERT(env.Runtime.FindActor(actor));

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakePlainReadResult(11, "ok")},
            "alice")));
        UNIT_ASSERT_VALUES_EQUAL(GetCmdReadResult(env.Grab<TEvProcessBatchResult>()->Get()->Context).GetResult().size(), 1);
    }

    Y_UNIT_TEST(ProcessBatchKeysCollectsPlainAndKafkaKeys) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        TReadResult emptyData;
        emptyData.SetOffset(1);

        TBatchKeysProcessingContext keys;
        keys.PartitionId = 3;
        keys.ResponseActor = env.Edge;
        keys.Results = {
            emptyData,
            MakePlainReadResult(2, "grow", false, NKikimrPQClient::TDataChunk::GROW, {{TString{MESSAGE_ATTRIBUTE_KEY}, "g"}}),
            MakePlainReadResult(3, "plain", false, NKikimrPQClient::TDataChunk::REGULAR, {
                {"other", "skip"},
                {TString{MESSAGE_ATTRIBUTE_KEY}, "plain-key"},
            }),
            MakePlainReadResult(4, "no-key", false),
            MakePlainReadResult(5, "unknown-batch", true),
            MakeKafkaBatchReadResult(MakeKafkaBatchPayload()),
        };

        env.Send(actor, new TEvProcessBatchKeys(std::move(keys)));

        const auto ev = env.Grab<TEvProcessBatchKeysResult>();
        const auto& offsetToKey = ev->Get()->OffsetToKey;
        UNIT_ASSERT(!offsetToKey.contains(1u));
        UNIT_ASSERT(!offsetToKey.contains(2u));
        UNIT_ASSERT_VALUES_EQUAL(offsetToKey.at(3u), "plain-key");
        UNIT_ASSERT_VALUES_EQUAL(offsetToKey.at(4u), "");
        UNIT_ASSERT(!offsetToKey.contains(5u));
        UNIT_ASSERT_VALUES_EQUAL(offsetToKey.at(10u), "k0");
        UNIT_ASSERT_VALUES_EQUAL(offsetToKey.at(11u), "k1");
    }

    Y_UNIT_TEST(UnexpectedEventDoesNotBreakProcessor) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvents::TEvPing());
        env.DispatchQuiet();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(env.Edge, {MakePlainReadResult(1, "ok")})));
        const auto ev = env.Grab<TEvProcessBatchResult>();
        UNIT_ASSERT_VALUES_EQUAL(GetCmdReadResult(ev->Get()->Context).GetResult().size(), 1);
    }

    Y_UNIT_TEST(WakeupFlushesCpuMetrics) {
        TEnv env;
        const auto actor = env.RegisterConsumer("metrics-user");

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakeKafkaBatchReadResult(MakeKafkaBatchPayload(NKafka::ECompressionType::GZIP, 32, 4096))},
            "metrics-user",
            9,
            10)));
        env.Grab<TEvProcessBatchResult>();

        env.Send(actor, new TEvents::TEvWakeup());
        auto metrics = env.Runtime.GrabEdgeEvent<TEvPQ::TEvConsumerBatchProcessorMetrics>(env.Tablet, TDuration::Seconds(10));
        UNIT_ASSERT(metrics);
        UNIT_ASSERT_VALUES_EQUAL(metrics->Get()->GetPartitionId(), 9u);
        UNIT_ASSERT_VALUES_EQUAL(metrics->Get()->User, "metrics-user");
        UNIT_ASSERT(metrics->Get()->CPUUsage > 0);
    }

    Y_UNIT_TEST(PoisonFlushesMetricsAndStops) {
        TEnv env;
        const auto actor = env.RegisterConsumer();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakeKafkaBatchReadResult(MakeKafkaBatchPayload(NKafka::ECompressionType::ZSTD, 16, 2048))},
            "user",
            4,
            10)));
        env.Grab<TEvProcessBatchResult>();

        env.Send(actor, new TEvents::TEvPoisonPill());
        env.DispatchQuiet();
        UNIT_ASSERT(!env.Runtime.FindActor(actor));
    }
}

Y_UNIT_TEST_SUITE(TBatchProcessorTest) {
    Y_UNIT_TEST(RoutesProcessBatchToPerUserProcessor) {
        TEnv env;
        const auto actor = env.RegisterBatchProcessor();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakePlainReadResult(10, "a")},
            "alice")));
        const auto first = env.Grab<TEvProcessBatchResult>();
        UNIT_ASSERT_VALUES_EQUAL(first->Get()->Context.User, "alice");

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakePlainReadResult(11, "b")},
            "alice")));
        UNIT_ASSERT_VALUES_EQUAL(env.Grab<TEvProcessBatchResult>()->Get()->Context.User, "alice");

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakePlainReadResult(12, "c")},
            "bob")));
        UNIT_ASSERT_VALUES_EQUAL(env.Grab<TEvProcessBatchResult>()->Get()->Context.User, "bob");
    }

    Y_UNIT_TEST(RoutesProcessBatchKeysToCompactificationWorker) {
        TEnv env;
        const auto actor = env.RegisterBatchProcessor();

        TBatchKeysProcessingContext keys;
        keys.PartitionId = 1;
        keys.ResponseActor = env.Edge;
        keys.Results = {MakePlainReadResult(8, "x", false, NKikimrPQClient::TDataChunk::REGULAR, {
            {TString{MESSAGE_ATTRIBUTE_KEY}, "ck"},
        })};

        env.Send(actor, new TEvProcessBatchKeys(std::move(keys)));
        const auto ev = env.Grab<TEvProcessBatchKeysResult>();
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->OffsetToKey.at(8u), "ck");
    }

    Y_UNIT_TEST(ConsumerRemovedPoisonsOnlyThatUser) {
        TEnv env;
        const auto actor = env.RegisterBatchProcessor();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakePlainReadResult(1, "a")},
            "alice")));
        env.Grab<TEvProcessBatchResult>();

        TBatchKeysProcessingContext keys;
        keys.PartitionId = 1;
        keys.ResponseActor = env.Edge;
        keys.Results = {MakePlainReadResult(2, "b", false, NKikimrPQClient::TDataChunk::REGULAR, {
            {TString{MESSAGE_ATTRIBUTE_KEY}, "k"},
        })};
        env.Send(actor, new TEvProcessBatchKeys(std::move(keys)));
        env.Grab<TEvProcessBatchKeysResult>();

        env.Send(actor, new TEvPQ::TEvConsumerRemoved("nobody"));
        env.DispatchQuiet();

        env.Send(actor, new TEvPQ::TEvConsumerRemoved("alice"));
        env.DispatchQuiet();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakePlainReadResult(3, "again")},
            "alice")));
        UNIT_ASSERT_VALUES_EQUAL(env.Grab<TEvProcessBatchResult>()->Get()->Context.User, "alice");

        TBatchKeysProcessingContext keysAgain;
        keysAgain.ResponseActor = env.Edge;
        keysAgain.Results = {MakePlainReadResult(4, "still", false, NKikimrPQClient::TDataChunk::REGULAR, {
            {TString{MESSAGE_ATTRIBUTE_KEY}, "k2"},
        })};
        env.Send(actor, new TEvProcessBatchKeys(std::move(keysAgain)));
        UNIT_ASSERT_VALUES_EQUAL(env.Grab<TEvProcessBatchKeysResult>()->Get()->OffsetToKey.at(4u), "k2");
    }

    Y_UNIT_TEST(UnexpectedEventIsIgnored) {
        TEnv env;
        const auto actor = env.RegisterBatchProcessor();

        env.Send(actor, new TEvents::TEvPing());
        env.DispatchQuiet();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(env.Edge, {MakePlainReadResult(1, "ok")}, "user")));
        UNIT_ASSERT_VALUES_EQUAL(GetCmdReadResult(env.Grab<TEvProcessBatchResult>()->Get()->Context).GetResult().size(), 1);
    }

    Y_UNIT_TEST(PoisonPillsChildProcessors) {
        TEnv env;
        const auto actor = env.RegisterBatchProcessor();

        env.Send(actor, new TEvProcessBatch(MakeReadContext(
            env.Edge,
            {MakePlainReadResult(1, "a")},
            "alice")));
        env.Grab<TEvProcessBatchResult>();

        env.Send(actor, new TEvents::TEvPoisonPill());
        env.DispatchQuiet();
        UNIT_ASSERT(!env.Runtime.FindActor(actor));
    }
}

} // namespace NKikimr::NPQ::NBatching
