#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace NTxUT;
using namespace NColumnShard;

namespace {

constexpr ui32 OldGroup = 2181038080;
constexpr ui32 NewGroup = 2181038081;

TIntrusivePtr<TTabletStorageInfo> MakeTabletInfo(const ui64 tabletId, const std::vector<std::pair<ui32, ui32>>& history) {
    auto info = MakeIntrusive<TTabletStorageInfo>();
    info->TabletID = tabletId;
    info->TabletType = TTabletTypes::ColumnShard;
    info->Channels.resize(5);
    for (ui64 channel = 0; channel < info->Channels.size(); ++channel) {
        info->Channels[channel].Channel = channel;
        info->Channels[channel].Type = TBlobStorageGroupType(BootGroupErasure);
        for (const auto& [fromGeneration, groupId] : history) {
            info->Channels[channel].History.emplace_back(fromGeneration, groupId);
        }
    }
    return info;
}

TActorId BootTablet(TTestBasicRuntime& runtime, const TIntrusivePtr<TTabletStorageInfo>& info) {
    auto setupInfo = MakeIntrusive<TTabletSetupInfo>(&CreateColumnShard, TMailboxType::Simple, ui32(0), TMailboxType::Simple, ui32(0));
    const TActorId actorId = runtime.Register(CreateTablet({}, info.Get(), setupInfo.Get(), 0), 0);
    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);
    // EvBoot only marks the start of boot; the shard stays in StateInit until its normalizers
    // finish, so drain the runtime before talking to it.
    runtime.DispatchEvents({}, TDuration::Seconds(1));
    return actorId;
}

// Portion data lives on channels 2+; channels 0 and 1 are the tablet log and the local database,
// which the executor's own vacuum leg moves.
std::vector<TLogoBlobID> LivePortionBlobs(const NFake::TProxyDS& proxy, const ui64 tabletId) {
    std::vector<TLogoBlobID> result;
    for (const auto& [id, blob] : proxy.AllMyBlobs()) {
        if (id.TabletID() == tabletId && id.Channel() >= 2 && !blob.DoNotKeep) {
            result.push_back(id);
        }
    }
    return result;
}

void RunMoveDataToCompletion(const bool ttlBackgroundDisabled) {
    TTestBasicRuntime runtime;
    runtime.SetScheduledLimit(10'000);
    TIntrusivePtr<NFake::TProxyDS> oldGroupProxy = new NFake::TProxyDS(TGroupId::FromValue(OldGroup));
    TIntrusivePtr<NFake::TProxyDS> newGroupProxy = new NFake::TProxyDS(TGroupId::FromValue(NewGroup));
    TTester::Setup(runtime,
        { new NFake::TProxyDS(TGroupId::FromValue(0)), oldGroupProxy, newGroupProxy, new NFake::TProxyDS(TGroupId::FromValue(Max<ui32>())) });
    // Without the flag TColumnShard hands TEvMoveData straight to the executor, which answers
    // Success after moving the log and the local database only.
    runtime.GetAppData().FeatureFlags.SetEnableColumnshardMoveData(true);
    auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
    if (ttlBackgroundDisabled) {
        controller->DisableBackground(NYDBTest::ICSController::EBackground::TTL);
    }

    const ui64 tabletId = TTestTxConfig::TxTablet0;
    const ui64 tableId = 1;

    const TActorId tabletActorId = BootTablet(runtime, MakeTabletInfo(tabletId, { { 0, OldGroup } }));
    TActorId sender = runtime.AllocateEdgeActor();

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);
    std::vector<ui64> writeIds;
    UNIT_ASSERT(WriteData(runtime, sender, tabletId, /*writeId=*/1, tableId, MakeTestBlob({ 0, 1000 }, table.Schema), table.Schema, &writeIds));
    planStep = ProposeCommit(runtime, sender, tabletId, /*txId=*/1, writeIds);
    PlanCommit(runtime, sender, tabletId, planStep, TSet<ui64>{ 1 });
    controller->WaitCompactions(TDuration::Seconds(10));

    const std::vector<TLogoBlobID> before = LivePortionBlobs(*oldGroupProxy, tabletId);
    UNIT_ASSERT_C(before.size(), "nothing was written into OldGroup - the test would pass vacuously");

    // Reassign the tablet one generation past everything written so far: those portions stay behind
    // in OldGroup, and from here the tablet writes to NewGroup.
    ui32 reassignedFrom = 0;
    for (const auto& id : before) {
        reassignedFrom = Max(reassignedFrom, id.Generation() + 1);
    }
    runtime.Send(new IEventHandle(tabletActorId, tabletActorId, new TKikimrEvents::TEvPoisonPill));
    BootTablet(runtime, MakeTabletInfo(tabletId, { { 0, OldGroup }, { reassignedFrom, NewGroup } }));
    UNIT_ASSERT_VALUES_EQUAL_C(LivePortionBlobs(*newGroupProxy, tabletId).size(), 0u, "no portion data may exist in the target group yet");

    runtime.SendToPipe(tabletId, sender, new TEvTablet::TEvMoveData(std::vector<ui32>{ OldGroup }), 0, GetPipeConfigWithRetries());

    // The completion gate is re-evaluated on the periodic wakeup, which this runtime does not
    // deliver on its own; drive it until the tablet answers.
    TEvTablet::TEvMoveDataResponse::TPtr response;
    for (ui32 i = 0; i < 100 && !response; ++i) {
        Wakeup(runtime, sender, tabletId);
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
        response = runtime.GrabEdgeEventIf<TEvTablet::TEvMoveDataResponse>(sender, [](const TEvTablet::TEvMoveDataResponse::TPtr&) {
            return true;
        }, TDuration::MilliSeconds(100));
    }
    UNIT_ASSERT_C(response, "no TEvMoveDataResponse: the move never drained OldGroup");
    UNIT_ASSERT_VALUES_EQUAL((int)response->Get()->Record.GetStatus(), (int)NKikimrTabletBase::TEvMoveDataResponse::Success);

    // Success has to mean the portions were rewritten, not just that the queues happened to be
    // empty: the only way data reaches the new group here is the move itself.
    UNIT_ASSERT_C(LivePortionBlobs(*newGroupProxy, tabletId).size(),
        "MoveData answered Success without rewriting any of the " << before.size() << " portion blobs out of the old group");
    UNIT_ASSERT_VALUES_EQUAL(ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(planStep.Val(), 1), table.Schema)->num_rows(), 1000);
}

}   // namespace

// The other tests in this suite each drive one piece - the group predicate, the completion-gate
// classifier, the request batcher - so nothing asserted that anything ever *calls* them. These run
// the whole chain: TEvMoveData -> selection -> accessor metadata -> rewrite -> response.
Y_UNIT_TEST_SUITE(TColumnShardMoveDataE2E) {
    Y_UNIT_TEST(MoveDataRewritesPortionsAndAnswersHive) {
        RunMoveDataToCompletion(/*ttlBackgroundDisabled=*/false);
    }

    // Rewrite tasks are extracted by the same loop TTL uses. Turning TTL off must not take
    // decommission down with it - the tablet would select portions and never move one.
    Y_UNIT_TEST(MoveDataCompletesWithTtlDisabled) {
        RunMoveDataToCompletion(/*ttlBackgroundDisabled=*/true);
    }
}

}   // namespace NKikimr
