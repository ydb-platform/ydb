#include <ydb/core/persqueue/dread_cache_service/caching_service.h>
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

TVector<ui64> ReadOffsets(TTestContext& tc, const TString& user = "user", ui64 readTimestampMs = 0) {
    TPQCmdReadSettings settings("", 0, 0, 10, Max<i32>(), 0, false, {}, 0, readTimestampMs, user);
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

ui64 CommittedWriteTimestampMs(TTestContext& tc, const TString& user) {
    THolder<TEvPersQueue::TEvRequest> request(new TEvPersQueue::TEvRequest);
    auto* req = request->Record.MutablePartitionRequest();
    req->SetPartition(0);
    req->MutableCmdGetClientOffset()->SetClientId(user);
    tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
    TAutoPtr<IEventHandle> handle;
    auto* result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_EQUAL(result->Record.GetErrorCode(), NPersQueue::NErrorCode::OK);
    const auto& resp = result->Record.GetPartitionResponse().GetCmdGetClientOffsetResult();
    return resp.HasWriteTimestampMS() ? resp.GetWriteTimestampMS() : 0;
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

Y_UNIT_TEST(MultipartMessagePastRetentionIsDropped) {
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
    WriteMsg(env.Tc, 2, false);

    // The whole message is past retention, including a tail that a follow-up would read with PartNo > 0.
    ExpectOnlyFresh(ReadOffsets(env.Tc));
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

// Both messages stay stored. The commit is moved back to the older one after it is past retention.
// The timestamp cache must keep that message's write time.
Y_UNIT_TEST(TimestampLookupUsesStoredCommitOffset) {
    TReadEnv env;
    PreparePartition(env.Tc, RetentionTopic(), {TConsumerPreparationParameters{.Name = "user"}});
    WriteMsg(env.Tc, 1, true);
    WriteMsg(env.Tc, 2, false);

    const auto written = CmdReadAndGetResult(TPQCmdReadSettings("", 0, 0, 10, Max<i32>(), 0, false, {}, 0, 0, "user"), env.Tc);
    UNIT_ASSERT_VALUES_EQUAL(written.ResultSize(), 2u);
    const ui64 atCommit = written.GetResult(0).GetWriteTimestampMS();
    UNIT_ASSERT(atCommit != written.GetResult(1).GetWriteTimestampMS());

    CmdSetOffset(0, "user", 1, false, env.Tc);
    env.Tc.Runtime->UpdateCurrentTime(env.Tc.Runtime->GetCurrentTime() + TDuration::Seconds(LifetimeSec + 2));
    PQGetPartInfo(0, 2, env.Tc);
    CmdSetOffset(0, "user", 0, false, env.Tc);

    ui64 observed = 0;
    for (int attempt = 0; attempt < 5 && observed != atCommit; ++attempt) {
        observed = CommittedWriteTimestampMs(env.Tc, "user");
    }
    UNIT_ASSERT_VALUES_EQUAL(observed, atCommit);
}

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr::NPQ
