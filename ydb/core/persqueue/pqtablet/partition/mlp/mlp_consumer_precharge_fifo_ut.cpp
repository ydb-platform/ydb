#include "mlp.h"
#include "mlp_storage.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/mlp/ut/common/common.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/mon.h>

#include <atomic>

namespace NKikimr::NPQ::NMLP {

Y_UNIT_TEST_SUITE(TMLPConsumerPrechargeFIFOTests) {

namespace {

// write long prefix with small number of groups and place unique groups at the tail
constexpr size_t HEAD_GROUPS = 10;
constexpr size_t HEAD_MESSAGES = 100000;
constexpr size_t TAIL_GROUPS = 5;

void WritePrechargeDataset(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topic) {
    WriteManyGroups(setup, topic, /*messageSize=*/1, HEAD_MESSAGES, HEAD_GROUPS);

    auto& runtime = setup->GetRuntime();
    std::vector<TWriterSettings::TMessage> messages;
    for (size_t i = 0; i < TAIL_GROUPS; ++i) {
        messages.push_back({
            .Index = i,
            .MessageBody = NUnitTest::RandomString(1),
            .MessageGroupId = TStringBuilder() << "unique_message_group_id_" << (100 + i),
        });
    }
    CreateWriterActor(runtime, TWriterSettings{
        .DatabasePath = "/Root",
        .TopicName = topic,
        .Messages = std::move(messages),
    });
    auto response = GetWriteResponse(runtime);
    UNIT_ASSERT_VALUES_EQUAL(response->DescribeStatus, NDescriber::EStatus::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(response->Messages.size(), TAIL_GROUPS);
}


constexpr ui64 PARTITION_TABLET_ID = 100;
constexpr const char* CONSUMER_NAME = "mlp-consumer";

NKikimrPQ::TPQTabletConfig MakeTopicConfig() {
    NKikimrPQ::TPQTabletConfig config;
    config.SetTopicName("topic");
    config.SetTopicPath("/Root/topic");

    auto* partition = config.AddAllPartitions();
    partition->SetPartitionId(0);
    partition->SetTabletId(PARTITION_TABLET_ID);
    partition->SetStatus(NKikimrPQ::ETopicPartitionStatus::Active);

    auto* consumer = config.AddConsumers();
    consumer->SetName(CONSUMER_NAME);
    consumer->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
    consumer->SetKeepMessageOrder(true);
    consumer->SetGeneration(1);
    return config;
}

NKikimrPQ::TPQTabletConfig::TConsumer MakeConsumerConfig() {
    NKikimrPQ::TPQTabletConfig::TConsumer consumer;
    consumer.SetName(CONSUMER_NAME);
    consumer.SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
    consumer.SetKeepMessageOrder(true);
    consumer.SetGeneration(1);
    return consumer;
}

// Builds a serialized snapshot with `messageCount` messages spread round-robin
// across `groupCount` groups, locking the heads of `lockedGroups` distinct groups.
TString BuildSnapshotBytes(size_t messageCount, size_t groupCount, size_t lockedGroups) {
    auto timeProvider = CreateDefaultTimeProvider();
    TStorage storage(timeProvider, TStorage::TStorageSettings{.KeepMessageOrder = true});
    const TInstant now = timeProvider->Now();
    for (size_t offset = 0; offset < messageCount; ++offset) {
        storage.AddMessage(offset, /*hasMessagegroup=*/true, /*messageGroupIdHash=*/offset % groupCount, now);
    }
    TStorage::TPosition position;
    for (size_t i = 0; i < lockedGroups; ++i) {
        auto locked = storage.Next(now + TDuration::Hours(1), position);
        UNIT_ASSERT(locked.has_value());
    }
    auto batch = storage.ExtractBatch();
    Y_UNUSED(batch);

    NKikimrPQ::TMLPStorageSnapshot snapshot;
    auto* configuration = snapshot.MutableConfiguration();
    configuration->SetConsumerName(CONSUMER_NAME);
    configuration->SetGeneration(1);
    storage.SerializeTo(snapshot);
    return snapshot.SerializeAsString();
}

THolder<TEvKeyValue::TEvResponse> MakeSnapshotKvResponse(ui64 cookie, const TString& snapshotBytes) {
    auto response = MakeHolder<TEvKeyValue::TEvResponse>();
    response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    response->Record.SetCookie(cookie);
    auto* readResult = response->Record.AddReadResult();
    readResult->SetStatus(NKikimrProto::OK);
    readResult->SetValue(snapshotBytes);
    response->Record.AddReadRangeResult()->SetStatus(NKikimrProto::NODATA);
    return response;
}

class TIgnorePipeCacheActor : public TActorBootstrapped<TIgnorePipeCacheActor> {
public:
    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    STRICT_STFUNC(StateWork,
        IgnoreFunc(TEvPipeCache::TEvForward);
        IgnoreFunc(TEvPipeCache::TEvUnlink);
    )
};

// Boots a consumer with the given read-ahead flag and a pre-built snapshot of
// messageCount messages over groupCount groups with lockedGroups heads locked,
// then returns the Count of the first CmdRead it emits, or nullopt if the fully
// buffered consumer decides not to fetch at all.
std::optional<ui64> GrabFirstFetchCount(float unlockedGroupsRatio, size_t messageCount, size_t groupCount, size_t lockedGroups) {
    TTestBasicRuntime runtime(1, false);
    runtime.Initialize(TAppPrepare().Unwrap());
    runtime.SetScheduledLimit(10000);
    runtime.GetAppData().PQConfig.SetMLPUnlockedGroupsRatio(unlockedGroupsRatio);

    auto pipeCache = runtime.Register(new TIgnorePipeCacheActor());
    runtime.EnableScheduleForActor(pipeCache);
    runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCache);

    auto tablet = runtime.AllocateEdgeActor();
    auto partition = runtime.AllocateEdgeActor();

    ::NMonitoring::TDynamicCounterPtr counters(new ::NMonitoring::TDynamicCounters());
    auto consumer = runtime.Register(CreateConsumerActor(
        "/Root",
        PARTITION_TABLET_ID,
        tablet,
        /*partitionId=*/0,
        partition,
        /*partitionGeneration=*/1,
        MakeTopicConfig(),
        MakeConsumerConfig(),
        TDuration::Hours(1),
        /*partitionEndOffset=*/messageCount + 1000,
        counters));
    runtime.EnableScheduleForActor(consumer);

    const TString snapshotBytes = BuildSnapshotBytes(messageCount, groupCount, lockedGroups);

    auto kvReq = runtime.GrabEdgeEvent<TEvKeyValue::TEvRequest>(TDuration::Seconds(10));
    UNIT_ASSERT(kvReq);
    runtime.Send(new IEventHandle(consumer, tablet, MakeSnapshotKvResponse(kvReq->Record.GetCookie(), snapshotBytes).Release()));

    auto readReq = runtime.GrabEdgeEvent<TEvPersQueue::TEvRequest>(TDuration::Seconds(3));
    if (!readReq) {
        return std::nullopt;
    }
    UNIT_ASSERT(readReq->Record.HasPartitionRequest());
    UNIT_ASSERT(readReq->Record.GetPartitionRequest().HasCmdRead());
    const auto& read = readReq->Record.GetPartitionRequest().GetCmdRead();
    UNIT_ASSERT(read.HasCount());
    return static_cast<ui64>(read.GetCount());
}

} // namespace

// EstimateFetchCountForNewGroups: exercised directly as a pure function.
Y_UNIT_TEST(MLPUnlockedGroupsEstimateFetchCountForNewGroups) {
    UNIT_ASSERT_VALUES_EQUAL(EstimateFetchCountForNewGroups(100000, 10, 1), 10000);
    UNIT_ASSERT_VALUES_EQUAL(EstimateFetchCountForNewGroups(100000, 10, 3), 30000);
    UNIT_ASSERT_VALUES_EQUAL(EstimateFetchCountForNewGroups(0, 0, 5), 5);
    UNIT_ASSERT_VALUES_EQUAL(EstimateFetchCountForNewGroups(100000, 10, 0), 0);
    UNIT_ASSERT_VALUES_EQUAL(EstimateFetchCountForNewGroups(5, 10, 2), 2);
    UNIT_ASSERT_VALUES_EQUAL(EstimateFetchCountForNewGroups(95, 10, 1), 10);
}

// With read-ahead disabled the fully buffered FIFO consumer does not fetch ahead
// at all, even with several groups locked.
Y_UNIT_TEST(MLPUnlockedGroupsDisabledDoesNotFetch) {
    const auto count = GrabFirstFetchCount(/*ratio=*/0.0f, HEAD_MESSAGES, HEAD_GROUPS, /*lockedGroups=*/6);
    UNIT_ASSERT_LE_C(count.value_or(0), /* default minMessages */ 100, TStringBuilder() << LabeledOutput(*count));
}

// ratio 0.5: 10 groups, 6 locked -> readable 4 < target ceil(5)=5 -> 1 missing
// group; density is 100000/10 = 10000 messages per group.
Y_UNIT_TEST(MLPUnlockedGroupsRatioFetchesEstimatedBatch) {
    const auto count = GrabFirstFetchCount(/*ratio=*/0.5f, HEAD_MESSAGES, HEAD_GROUPS, /*lockedGroups=*/6);
    UNIT_ASSERT(count);
    Cerr << ">>>>> first CmdRead count (ratio 0.5): " << *count << Endl;
    UNIT_ASSERT_VALUES_EQUAL(*count, 10000);
}

// ratio 1.0: readable 4 < target 10 -> 6 missing groups -> estimate 60000, capped
// by the free in-flight capacity (MaxMessages - 100000 = 20000).
Y_UNIT_TEST(MLPUnlockedGroupsEnabledFetchesMaxBatches) {
    const auto count = GrabFirstFetchCount(/*ratio=*/1.0f, HEAD_MESSAGES, HEAD_GROUPS, /*lockedGroups=*/6);
    UNIT_ASSERT(count);
    Cerr << ">>>>> first CmdRead count (enabled): " << *count << Endl;
    UNIT_ASSERT_VALUES_EQUAL(*count, 20000);
}


static size_t ReadDistinctGroupHeads(std::shared_ptr<TTopicSdkTestSetup>& setup) {
    const TInstant deadline = TDuration::Seconds(40).ToDeadLine();
    auto& runtime = setup->GetRuntime();
    const size_t expectedGroups = HEAD_GROUPS + TAIL_GROUPS; // 15
    TSet<TString> groups;
    for (size_t i = 0; TInstant::Now() < deadline; ++i) {
        CreateReaderActor(runtime, {
            .DatabasePath = "/Root",
            .TopicName = "/Root/topic1",
            .Consumer = "mlp-consumer",
            .WaitTime = TDuration::Seconds(2),
            .ProcessingTimeout = TDuration::Seconds(60),
            .MaxNumberOfMessage = static_cast<ui32>(expectedGroups),
        });
        auto response = GetReadResponse(runtime, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL_C(response->Status, Ydb::StatusIds::SUCCESS, response->ErrorDescription);
        for (const auto& message : response->Messages) {
            auto [it, ins] = groups.insert(message.MessageGroupId);
            UNIT_ASSERT_C(ins, LabeledOutput(message.MessageGroupId));
        }
        if (groups.size() == expectedGroups) {
            break;
        }
        Cerr << ">>>>> attempt " << i << ": read " << response->Messages.size() << " groups; totalGroups = " << groups.size() << Endl;
        Sleep(TDuration::MilliSeconds(1000));
    }
    return groups.size();
}

static void MLPUnlockedGroupsReadAllGroupsImpl(float ratio) {
    auto setup = CreateSetup();
    setup->GetRuntime().GetAppData().PQConfig.SetMLPUnlockedGroupsRatio(ratio);
    CreateTopic(setup, "/Root/topic1", "mlp-consumer", 1, /*keepMessagesOrder=*/true);
    WritePrechargeDataset(setup, "/Root/topic1");

    const size_t expectedGroups = HEAD_GROUPS + TAIL_GROUPS;
    UNIT_ASSERT_VALUES_EQUAL(ReadDistinctGroupHeads(setup), expectedGroups);
}

Y_UNIT_TEST(MLPUnlockedGroupsEnabledReadsAllGroups) {
    MLPUnlockedGroupsReadAllGroupsImpl(1.0f);
}

Y_UNIT_TEST(MLPUnlockedGroupsRatioReadsAllGroups) {
    MLPUnlockedGroupsReadAllGroupsImpl(0.5f);
}

}

} // namespace NKikimr::NPQ::NMLP
