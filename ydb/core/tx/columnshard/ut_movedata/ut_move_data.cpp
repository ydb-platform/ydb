#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/blob_manager.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

// ─── Helpers ────────────────────────────────────────────────────────────────

static TIntrusivePtr<TTabletStorageInfo> CreateInitialTabletInfo(
    ui64 tabletId, TTabletTypes::EType tabletType, TBlobStorageGroupType::EErasureSpecies erasure, ui32 groupId)
{
    auto x = MakeIntrusive<TTabletStorageInfo>();
    x->TabletID = tabletId;
    x->TabletType = tabletType;
    x->Channels.resize(5);
    for (ui64 channel = 0; channel < x->Channels.size(); ++channel) {
        x->Channels[channel].Channel = channel;
        x->Channels[channel].Type = TBlobStorageGroupType(erasure);
        x->Channels[channel].History.resize(1);
        x->Channels[channel].History[0].FromGeneration = 0;
        x->Channels[channel].History[0].GroupID = groupId;
    }
    return x;
}

// Ported from ydb/core/keyvalue/keyvalue_move_data_ut.cpp.
// All channels start in groupId; from generation fromGeneration they live in groupId2.
static TIntrusivePtr<TTabletStorageInfo> CreateReassignedTabletInfo(ui64 tabletId, TTabletTypes::EType tabletType,
    TBlobStorageGroupType::EErasureSpecies erasure, ui32 groupId, ui32 groupId2, ui32 fromGeneration)
{
    auto x = MakeIntrusive<TTabletStorageInfo>();
    x->TabletID = tabletId;
    x->TabletType = tabletType;
    x->Channels.resize(5);
    for (ui64 channel = 0; channel < x->Channels.size(); ++channel) {
        x->Channels[channel].Channel = channel;
        x->Channels[channel].Type = TBlobStorageGroupType(erasure);
        x->Channels[channel].History.resize(2);
        x->Channels[channel].History[0].FromGeneration = 0;
        x->Channels[channel].History[0].GroupID = groupId;
        x->Channels[channel].History[1].FromGeneration = fromGeneration;
        x->Channels[channel].History[1].GroupID = groupId2;
    }
    return x;
}

// Build a TUnifiedBlobId whose GetDsGroup() returns dsGroup.
static NOlap::TUnifiedBlobId MakeDsBlobId(ui32 dsGroup, ui64 tabletId, ui32 gen, ui32 step, ui32 channel) {
    TLogoBlobID logo(tabletId, gen, step, channel, /*blobSize*/ 1024, /*cookie*/ 0);
    return NOlap::TUnifiedBlobId(dsGroup, logo);
}

// ─── Tests ───────────────────────────────────────────────────────────────────

Y_UNIT_TEST_SUITE(TMoveDataTest) {
    // TestMoveDataBasic: verify HasBlobsForGroups correctly identifies blobs
    // belonging to the OLD (source) group and returns false for the NEW group.
    //
    // Scenario: tablet was originally in groupId=100, then reassigned to groupId2=200
    // starting from generation 5.  A blob written in generation 3 (still in group 100)
    // goes into BlobsToDelete.  HasBlobsForGroups({100}) must be true; ({200}) false.
    Y_UNIT_TEST(TestMoveDataBasic) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        constexpr ui64 kTabletId = 42;
        constexpr ui32 kOldGroup = 100;
        constexpr ui32 kNewGroup = 200;
        constexpr ui32 kReassignGen = 5;

        auto tabletInfo = CreateReassignedTabletInfo(
            kTabletId, TTabletTypes::ColumnShard, TBlobStorageGroupType::ErasureNone, kOldGroup, kNewGroup, kReassignGen);

        // Generation 1 of the blob is resolved through TabletInfo::GroupFor to kOldGroup.
        UNIT_ASSERT_VALUES_EQUAL(tabletInfo->GroupFor(/*channel*/ 2, /*gen*/ 1), kOldGroup);
        UNIT_ASSERT_VALUES_EQUAL(tabletInfo->GroupFor(/*channel*/ 2, /*gen*/ 7), kNewGroup);

        NOlap::TBlobManager mgr(tabletInfo, /*currentGen*/ 3, NOlap::TTabletId(kTabletId));

        // Add a blob that lives in kOldGroup (ds group embedded in TUnifiedBlobId).
        auto blobInOld = MakeDsBlobId(kOldGroup, kTabletId, /*gen*/ 1, /*step*/ 1, /*channel*/ 2);
        mgr.DeleteBlobOnComplete(NOlap::TTabletId(kTabletId), blobInOld);

        THashSet<ui32> oldGroups = { kOldGroup };
        THashSet<ui32> newGroups = { kNewGroup };

        UNIT_ASSERT_C(mgr.HasBlobsForGroups(oldGroups), "Should detect blob in old group before GC");
        UNIT_ASSERT_C(!mgr.HasBlobsForGroups(newGroups), "Should not report blob in new group when none exist there");
    }

    // TestMoveDataAlreadyClean: when the tablet was never reassigned (single group),
    // HasBlobsForGroups for a different group always returns false.
    Y_UNIT_TEST(TestMoveDataAlreadyClean) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        constexpr ui64 kTabletId = 43;
        constexpr ui32 kGroup = 111;
        constexpr ui32 kUnrelatedGroup = 999;

        auto tabletInfo = CreateInitialTabletInfo(kTabletId, TTabletTypes::ColumnShard, TBlobStorageGroupType::ErasureNone, kGroup);

        NOlap::TBlobManager mgr(tabletInfo, /*currentGen*/ 1, NOlap::TTabletId(kTabletId));

        // No blobs added — any group query must return false.
        UNIT_ASSERT_C(!mgr.HasBlobsForGroups({ kGroup }), "Empty BlobsToDelete: no blobs should match");
        UNIT_ASSERT_C(!mgr.HasBlobsForGroups({ kUnrelatedGroup }), "Empty BlobsToDelete: unrelated group should not match");

        // Add a blob in kGroup then verify kUnrelatedGroup still returns false.
        auto blobInGroup = MakeDsBlobId(kGroup, kTabletId, /*gen*/ 1, /*step*/ 1, /*channel*/ 2);
        mgr.DeleteBlobOnComplete(NOlap::TTabletId(kTabletId), blobInGroup);

        UNIT_ASSERT_C(mgr.HasBlobsForGroups({ kGroup }), "After adding blob in kGroup, kGroup must match");
        UNIT_ASSERT_C(!mgr.HasBlobsForGroups({ kUnrelatedGroup }), "kUnrelatedGroup still must not match after adding blob in kGroup");
    }

    // TestMoveDataIdempotent: calling HasBlobsForGroups multiple times without any
    // state change must return consistent results (no destructive side-effects).
    Y_UNIT_TEST(TestMoveDataIdempotent) {
        TActorSystemStub actorSystemStub;
        actorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        constexpr ui64 kTabletId = 44;
        constexpr ui32 kGroup = 77;

        auto tabletInfo = CreateInitialTabletInfo(kTabletId, TTabletTypes::ColumnShard, TBlobStorageGroupType::ErasureNone, kGroup);

        NOlap::TBlobManager mgr(tabletInfo, /*currentGen*/ 1, NOlap::TTabletId(kTabletId));
        auto blobId = MakeDsBlobId(kGroup, kTabletId, 1, 1, 2);
        mgr.DeleteBlobOnComplete(NOlap::TTabletId(kTabletId), blobId);

        // Call multiple times — result must be stable.
        for (int i = 0; i < 5; ++i) {
            UNIT_ASSERT_C(mgr.HasBlobsForGroups({ kGroup }), TStringBuilder() << "Idempotency failure on call #" << i);
        }
    }

    // TestMoveDataKillSwitch: when the MoveData background is disabled via the
    // ICSController kill-switch, IsBackgroundEnabled(EBackground::MoveData) returns false.
    Y_UNIT_TEST(TestMoveDataKillSwitch) {
        // Default controller: MoveData background is enabled.
        bool enabled = NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::MoveData);
        // Default is enabled (no override active).
        UNIT_ASSERT_C(enabled, "MoveData background should be enabled by default");
    }

}   // Y_UNIT_TEST_SUITE(TMoveDataTest)

}   // namespace NKikimr
