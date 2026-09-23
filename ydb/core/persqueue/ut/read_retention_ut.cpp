#include <ydb/core/persqueue/dread_cache_service/caching_service.h>
#include <ydb/core/persqueue/pqtablet/common/constants.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

namespace {

constexpr ui32 LifetimeSec = 60;

void SetReadPriorRetention(TTestContext& tc, bool enabled) {
    for (ui32 nodeIdx = 0; nodeIdx < tc.Runtime->GetNodeCount(); ++nodeIdx) {
        tc.Runtime->GetAppData(nodeIdx).FeatureFlags.SetEnableTopicReadPriorRetention(enabled);
    }
}

struct TReadEnv {
    TTestContext Tc;
    TFinalizer Finalizer;

    explicit TReadEnv(bool readPriorRetention = false)
        : Finalizer(Tc)
    {
        Tc.Prepare();
        Tc.Runtime->SetScheduledLimit(200);
        Tc.Runtime->SetDispatchTimeout(TDuration::Seconds(1));
        SetReadPriorRetention(Tc, readPriorRetention);
        Tc.Runtime->GetAppData(0).PQConfig.MutableCompactionConfig()->SetBlobsCount(0);
    }
};

void PreparePartition(TTestContext& tc, const TTabletPreparationParameters& parameters, TConstArrayRef<TConsumerPreparationParameters> users) {
    PQTabletPrepare(parameters, users, *tc.Runtime, tc.TabletId, tc.Edge,
                    tc.NextPqConfigTxId++, tc.NextPqConfigPlanStep++);
}

void WriteMsg(TTestContext& tc, ui64 seqNo, bool isFirst) {
    CmdWrite(0, "sourceid0", {{seqNo, TStringBuilder() << "msg-" << seqNo}}, tc, false, {}, isFirst);
}

// Two messages: one written before the retention boundary and one after it.
// The gap is LifetimeSec + 2s, inside the 5s cleanup grace, so the old blob is still stored.
void WriteAroundRetention(TTestContext& tc) {
    WriteMsg(tc, 1, true);
    tc.Runtime->UpdateCurrentTime(tc.Runtime->GetCurrentTime() + TDuration::Seconds(LifetimeSec + 2));
    WriteMsg(tc, 2, false);
}

TVector<ui64> ReadOffsets(TTestContext& tc, const TString& user = "user", ui64 readTimestampMs = 0, ui32 partNo = 0) {
    TPQCmdReadSettings settings("", 0, 0, 10, Max<i32>(), 0, false, {}, 0, readTimestampMs, user);
    settings.PartNo = partNo;
    const auto result = CmdReadAndGetResult(settings, tc);
    TVector<ui64> offsets;
    offsets.reserve(result.ResultSize());
    for (const auto& message : result.GetResult()) {
        offsets.push_back(message.GetOffset());
    }
    return offsets;
}

TTabletPreparationParameters RetentionTopic() {
    return {.deleteTime = LifetimeSec, .partitions = 1, .AddDefaultConsumer = false};
}

void ExpectOnlyFresh(const TVector<ui64>& offsets) {
    UNIT_ASSERT_VALUES_EQUAL(offsets.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(offsets[0], 1u);
}

void ExpectBoth(const TVector<ui64>& offsets) {
    UNIT_ASSERT_VALUES_EQUAL(offsets.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(offsets[0], 0u);
    UNIT_ASSERT_VALUES_EQUAL(offsets[1], 1u);
}

} // namespace

Y_UNIT_TEST_SUITE(ReadInsideRetention) {

Y_UNIT_TEST(SkipsMessagesOlderThanRetention) {
    TReadEnv env;
    PreparePartition(env.Tc, RetentionTopic(), {TConsumerPreparationParameters{.Name = "user"}});
    WriteAroundRetention(env.Tc);
    ExpectOnlyFresh(ReadOffsets(env.Tc));
}

Y_UNIT_TEST(FlagAllowsReadingOlderThanRetention) {
    TReadEnv env(/*readPriorRetention=*/true);
    PreparePartition(env.Tc, RetentionTopic(), {TConsumerPreparationParameters{.Name = "user"}});
    WriteAroundRetention(env.Tc);
    ExpectBoth(ReadOffsets(env.Tc));
}

Y_UNIT_TEST(ImportantConsumerKeepsOldData) {
    TReadEnv env;
    PreparePartition(env.Tc, RetentionTopic(), {TConsumerPreparationParameters{.Name = "important", .Important = true}});
    WriteAroundRetention(env.Tc);
    ExpectBoth(ReadOffsets(env.Tc, "important"));
}

Y_UNIT_TEST(StorageLimitDisablesCutoff) {
    TReadEnv env;
    TTabletPreparationParameters parameters = RetentionTopic();
    parameters.storageLimitBytes = 1_MB;
    TConsumerPreparationParameters user{.Name = "user"};
    auto config = MakePQTabletConfig(parameters, {user}, *env.Tc.Runtime, 1);
    config.MutablePartitionConfig()->SetLifetimeSeconds(LifetimeSec);
    SendPQTabletConfig(*env.Tc.Runtime, env.Tc.TabletId, env.Tc.Edge, config,
                       env.Tc.NextPqConfigTxId++, env.Tc.NextPqConfigPlanStep++);
    WriteAroundRetention(env.Tc);
    ExpectBoth(ReadOffsets(env.Tc));
}

Y_UNIT_TEST(AvailabilityPeriodExtendsLag) {
    TReadEnv env;
    PreparePartition(env.Tc, RetentionTopic(), {TConsumerPreparationParameters{
        .Name = "user",
        .AvailabilityPeriodMs = TDuration::Hours(1).MilliSeconds(),
    }});
    WriteAroundRetention(env.Tc);
    ExpectBoth(ReadOffsets(env.Tc));
}

Y_UNIT_TEST(ExplicitReadFromIsRaisedToRetention) {
    TReadEnv env;
    PreparePartition(env.Tc, RetentionTopic(), {TConsumerPreparationParameters{.Name = "user"}});
    WriteAroundRetention(env.Tc);
    ExpectOnlyFresh(ReadOffsets(env.Tc, "user", /*readTimestampMs=*/1));
}

Y_UNIT_TEST(PartNoContinuationReturnsOldMessage) {
    TReadEnv env;
    PreparePartition(env.Tc, RetentionTopic(), {TConsumerPreparationParameters{.Name = "user"}});

    const TString cookie = CmdSetOwner(0, env.Tc).first;
    WritePartData(0, "sourceid0", -1, 1, 0, 2, 16, "aaaaaaaa", env.Tc, cookie, 0);
    {
        TAutoPtr<IEventHandle> handle;
        auto* result = env.Tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_EQUAL(result->Record.GetErrorCode(), NPersQueue::NErrorCode::OK);
    }
    WritePartData(0, "sourceid0", -1, 1, 1, 2, 16, "bbbbbbbb", env.Tc, cookie, 1);
    {
        TAutoPtr<IEventHandle> handle;
        auto* result = env.Tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_EQUAL(result->Record.GetErrorCode(), NPersQueue::NErrorCode::OK);
    }

    env.Tc.Runtime->UpdateCurrentTime(env.Tc.Runtime->GetCurrentTime() + TDuration::Seconds(LifetimeSec + 2));

    // Same shape as a read-proxy follow-up: PartNo > 0 bypasses the proxy and must not
    // recompute the retention floor, so the rest of an already-started message is returned.
    TPQCmdReadSettings settings("", 0, 0, 1, Max<i32>(), 0, false, {}, 0, 0, "user");
    settings.PartNo = 1;
    settings.RequestId = TMP_REQUEST_MARKER;
    const auto result = CmdReadAndGetResult(settings, env.Tc);
    UNIT_ASSERT_VALUES_EQUAL(result.ResultSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(result.GetResult(0).GetOffset(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(result.GetResult(0).GetData(), "bbbbbbbb");
    UNIT_ASSERT_VALUES_EQUAL(result.GetReadFromTimestampMs(), 0u);
}

Y_UNIT_TEST(DirectReadPastRetentionReturnsEmpty) {
    TReadEnv env;
    env.Tc.Runtime->RegisterService(
        MakePQDReadCacheServiceActorId(),
        env.Tc.Runtime->Register(CreatePQDReadCacheService(new NMonitoring::TDynamicCounters())));
    PreparePartition(env.Tc, RetentionTopic(), {TConsumerPreparationParameters{.Name = "user"}});

    WriteMsg(env.Tc, 1, true);
    WriteMsg(env.Tc, 2, false);
    WriteMsg(env.Tc, 3, false);
    env.Tc.Runtime->UpdateCurrentTime(env.Tc.Runtime->GetCurrentTime() + TDuration::Seconds(LifetimeSec + 2));

    const TString sessionId = "session1";
    TPQCmdSettings sessionSettings{0, "user", sessionId};
    sessionSettings.PartitionSessionId = 1;
    sessionSettings.KeepPipe = true;
    const TActorId pipe = CmdCreateSession(sessionSettings, env.Tc);

    TPQCmdReadSettings readSettings(sessionId, 0, 0, 10, Max<i32>(), 0);
    readSettings.User = "user";
    readSettings.PartitionSessionId = 1;
    readSettings.DirectReadId = 1;
    readSettings.Pipe = pipe;
    BeginCmdRead(readSettings, env.Tc);

    TAutoPtr<IEventHandle> handle;
    auto* result = env.Tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_C(result->Record.GetErrorCode() == NPersQueue::NErrorCode::OK, result->Record.DebugString());
    UNIT_ASSERT_C(result->Record.GetPartitionResponse().HasCmdPrepareReadResult(), result->Record.DebugString());
    const auto& prepared = result->Record.GetPartitionResponse().GetCmdPrepareReadResult();
    UNIT_ASSERT_VALUES_EQUAL(prepared.GetDirectReadId(), 1u);
    UNIT_ASSERT_C(!prepared.HasWriteTimestampMS(), result->Record.DebugString());
    UNIT_ASSERT_GE_C(prepared.GetReadOffset(), prepared.GetEndOffset(), result->Record.DebugString());
}

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr::NPQ
