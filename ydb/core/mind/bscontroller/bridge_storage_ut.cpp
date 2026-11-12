#include "bridge_storage.h"

#include <ydb/core/blobstorage/base/bridge_events.h>
#include <util/system/rusage.h>
#include <util/stream/output.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/timestamp.pb.h>

using namespace NKikimr;
using namespace NKikimr::NBsController;
using namespace NKikimr::NBsController::NBridgeStorage;

namespace {

    using TGroup = NKikimrBridge::TGroupState;
    using TCluster = NKikimrBridge::TClusterState;

    const TBridgePileId SecondPile = TBridgePileId::FromPileIndex(1);

    TGroup MakeGroup() {
        TGroup group;
        for (ui32 i = 0; i < 2; ++i) {
            auto* pile = group.AddPile();
            pile->SetGroupId(100 + i);
            pile->SetGroupGeneration(1);
            pile->SetStage(TGroup::SYNCED);
            pile->SetBecameUnsyncedGeneration(10);
        }
        return group;
    }

} // namespace

Y_UNIT_TEST_SUITE(BridgeStorageState) {
    Y_UNIT_TEST(RejoiningCopyUsesPileSyncGenerationAndRealStage) {
        auto group = MakeGroup();
        NKikimrBridge::TClusterStateDetails::TPileSyncState sync;
        sync.SetBecameUnsyncedGeneration(42);
        TCopyContext context{
            .Group = TGroupContext(&group),
            .PileId = SecondPile,
            .PileState = TCluster::NOT_SYNCHRONIZED_2,
            .PileSyncState = &sync,
        };
        // The stored SYNCED stage predates this rejoin and cannot establish completion.
        UNIT_ASSERT(ObserveCopy(context).State == NKikimrBridge::PENDING);
        context.SyncerRunning = true;
        UNIT_ASSERT(ObserveCopy(context).State == NKikimrBridge::PENDING);
        group.MutablePile(1)->SetBecameUnsyncedGeneration(42);
        group.MutablePile(1)->SetStage(TGroup::BLOCKS);
        UNIT_ASSERT(ObserveCopy(context).State == NKikimrBridge::SYNCING);
        context.SyncerRunning = false;
        UNIT_ASSERT(ObserveCopy(context).State == NKikimrBridge::PENDING);
        group.MutablePile(1)->SetStage(TGroup::SYNCED);
        UNIT_ASSERT(ObserveCopy(context).State == NKikimrBridge::SYNCED);
    }

    Y_UNIT_TEST(DisconnectedStoredCompletionIsUnknownUnlessCopyIsAuthoritative) {
        auto group = MakeGroup();
        TCopyContext context{
            .Group = TGroupContext(&group),
            .PileId = SecondPile,
            .PileState = TCluster::DISCONNECTED,
        };
        UNIT_ASSERT(ObserveCopy(context).State == NKikimrBridge::UNKNOWN);
        context.PileState = TCluster::SUSPENDED;
        UNIT_ASSERT(ObserveCopy(context).State == NKikimrBridge::UNKNOWN);

        NKikimrBridge::TClusterStateDetails::TPileSyncState sync;
        sync.SetBecameUnsyncedGeneration(42);
        context.PileSyncState = &sync;
        group.SetAuthorityPileId(SecondPile.GetLocalDb());
        context.Group = TGroupContext(&group);
        UNIT_ASSERT(ObserveCopy(context).State == NKikimrBridge::SYNCED);
        // A sole part is authoritative even without an explicit AuthorityPileId.
        group.ClearAuthorityPileId();
        group.MutablePile(0)->SetAbsent(true);
        context.Group = TGroupContext(&group);
        UNIT_ASSERT(ObserveCopy(context).State == NKikimrBridge::SYNCED);
    }

    Y_UNIT_TEST(AbsentPartsAndOrdinaryOrInactiveStorageRemainDistinct) {
        auto group = MakeGroup();
        group.MutablePile(1)->SetAbsent(true);
        TCopyContext context{
            .Group = TGroupContext(&group),
            .PileId = SecondPile,
            .PileState = TCluster::NOT_SYNCHRONIZED_1,
        };
        UNIT_ASSERT(ObserveCopy(context).State == NKikimrBridge::PENDING);
        context.BridgeMode = false;
        UNIT_ASSERT(ObserveCopy(context).State == NKikimrBridge::NOT_REQUIRED);
        context.BridgeMode = true;
        context.PileState = TCluster::INACTIVE;
        UNIT_ASSERT(ObserveCopy(context).State == NKikimrBridge::NOT_REQUIRED);
        context.Group = TGroupContext();
        context.PileState = TCluster::SYNCHRONIZED;
        UNIT_ASSERT(ObserveCopy(context).State == NKikimrBridge::UNKNOWN);
    }

    Y_UNIT_TEST(RequiredCountsExcludeOrdinaryStorage) {
        TCounts counts;
        for (const auto status : {NKikimrBridge::SYNCED, NKikimrBridge::SYNCING, NKikimrBridge::PENDING,
                                  NKikimrBridge::UNKNOWN, NKikimrBridge::NOT_REQUIRED}) {
            counts.Count(status);
        }
        NKikimrBridge::TGroupSyncCounts proto;
        counts.ToProto(&proto);
        UNIT_ASSERT_VALUES_EQUAL(proto.required(), 4);
        UNIT_ASSERT_VALUES_EQUAL(proto.synced(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proto.syncing(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proto.pending(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proto.unknown(), 1);
        NKikimrBridge::TDatabaseSyncCounts databases;
        counts.ToProto(&databases);
        UNIT_ASSERT_VALUES_EQUAL(databases.total(), 4);
        UNIT_ASSERT_VALUES_EQUAL(databases.synced(), 1);
        UNIT_ASSERT_VALUES_EQUAL(databases.syncing(), 1);
        UNIT_ASSERT_VALUES_EQUAL(databases.pending(), 1);
        UNIT_ASSERT_VALUES_EQUAL(databases.unknown(), 1);
        UNIT_ASSERT_VALUES_EQUAL(databases.not_required(), 0);
        UNIT_ASSERT(counts.GetStatus() == NKikimrBridge::SYNCING);
    }

    Y_UNIT_TEST(DatabaseStatusUsesRequiredGroupAggregate) {
        TCounts groups;
        UNIT_ASSERT(groups.GetStatus() == NKikimrBridge::NOT_REQUIRED);
        groups.Count(NKikimrBridge::PENDING);
        UNIT_ASSERT(groups.GetStatus() == NKikimrBridge::PENDING);
        groups.Count(NKikimrBridge::UNKNOWN);
        UNIT_ASSERT(groups.GetStatus() == NKikimrBridge::UNKNOWN);
        groups.Count(NKikimrBridge::SYNCING);
        UNIT_ASSERT(groups.GetStatus() == NKikimrBridge::SYNCING);
        TCounts complete;
        complete.Count(NKikimrBridge::SYNCED);
        complete.Count(NKikimrBridge::SYNCED);
        UNIT_ASSERT(complete.GetStatus() == NKikimrBridge::SYNCED);
    }

    Y_UNIT_TEST(DatabaseCountsPartitionIncludesNotRequired) {
        TCounts counts;
        for (const auto status : {NKikimrBridge::SYNCED, NKikimrBridge::SYNCING, NKikimrBridge::PENDING,
                                  NKikimrBridge::UNKNOWN, NKikimrBridge::NOT_REQUIRED}) {
            counts.CountDatabase(status);
        }
        NKikimrBridge::TDatabaseSyncCounts proto;
        counts.ToProto(&proto);
        UNIT_ASSERT_VALUES_EQUAL(proto.total(), 5);
        UNIT_ASSERT_VALUES_EQUAL(proto.synced(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proto.syncing(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proto.pending(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proto.unknown(), 1);
        UNIT_ASSERT_VALUES_EQUAL(proto.not_required(), 1);
        TCounts ordinary;
        ordinary.CountDatabase(NKikimrBridge::NOT_REQUIRED);
        ordinary.CountDatabase(NKikimrBridge::NOT_REQUIRED);
        UNIT_ASSERT(ordinary.GetStatus() == NKikimrBridge::NOT_REQUIRED);
    }

    Y_UNIT_TEST(AppendGroupPreservesPoolAndOptionalPlacement) {
        NKikimrBridge::TStoragePool pool;
        pool.mutable_id()->set_box_id(2);
        pool.mutable_id()->set_pool_id(5);
        pool.set_name("pool,ssd");
        TReportBudget budget;
        NKikimrBridge::TGroupSynchronizationResult result;
        UNIT_ASSERT(AppendGroup({100, std::nullopt, &pool, TStringBuf(""),
                                 {.State = NKikimrBridge::PENDING}}, budget, result));
        const auto& pending = result.groups(0);
        UNIT_ASSERT(pending.has_pile_name());
        UNIT_ASSERT(pending.pile_name().empty());
        UNIT_ASSERT(!pending.has_physical_group_id());
        UNIT_ASSERT_VALUES_EQUAL(pending.storage_pool().id().box_id(), 2);
        UNIT_ASSERT_VALUES_EQUAL(pending.storage_pool().id().pool_id(), 5);
        UNIT_ASSERT_VALUES_EQUAL(pending.storage_pool().name(), "pool,ssd");
        UNIT_ASSERT(AppendGroup({101, 101, &pool, std::nullopt,
                                 {.State = NKikimrBridge::NOT_REQUIRED}}, budget, result));
        UNIT_ASSERT(!result.groups(1).has_pile_name());
        UNIT_ASSERT_VALUES_EQUAL(result.groups(1).physical_group_id(), 101);
        UNIT_ASSERT(AppendGroup({0, 0, nullptr, TStringBuf("pile"),
                                 {.State = NKikimrBridge::SYNCED}}, budget, result));
        UNIT_ASSERT_VALUES_EQUAL(result.groups(2).group_id(), 0);
        UNIT_ASSERT(result.groups(2).has_physical_group_id());
        UNIT_ASSERT_VALUES_EQUAL(result.groups(2).physical_group_id(), 0);
        UNIT_ASSERT(!result.groups(2).has_storage_pool());
    }

    Y_UNIT_TEST(LargeUpstreamErrorIsRejectedBeforeAppending) {
        const TString error(TEvBlobStorage::TEvControllerQueryBridgeStorage::MaxResultBytes, 'x');
        NKikimrBridge::TStoragePool pool;
        pool.set_name("pool");
        TReportBudget budget;
        NKikimrBridge::TGroupSynchronizationResult result;
        UNIT_ASSERT(!AppendGroup({100, 101, &pool, TStringBuf("pile"),
                                  {.State = NKikimrBridge::PENDING, .LastError = &error}}, budget, result));
        UNIT_ASSERT_VALUES_EQUAL(result.groups_size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(budget.Bytes, 1024);
    }

    Y_UNIT_TEST(ScanBudgetStopsWithoutAContinuation) {
        TReportBudget budget;
        for (ui64 i = 0; i < TEvBlobStorage::TEvControllerQueryBridgeStorage::MaxVisitedCandidates; ++i) {
            UNIT_ASSERT(budget.Visit());
        }
        UNIT_ASSERT(!budget.Visit());
    }

    Y_UNIT_TEST(MaterializationBenchmark10kAnd100k) {
        // Measures the production classifier/row builder, not actor scheduling, index traversal or RPC latency.
        for (const ui32 groupCount : {10'000u, 100'000u}) {
            auto group = MakeGroup();
            auto* third = group.AddPile();
            third->SetGroupGeneration(1);
            third->SetStage(TGroup::SYNCED);
            third->SetBecameUnsyncedGeneration(10);
            const TStringBuf pileNames[] = {"dc-1", "dc-2", "dc-3"};
            NKikimrBridge::TStoragePool pool;
            pool.mutable_id()->set_box_id(1);
            pool.mutable_id()->set_pool_id(42);
            pool.set_name("/Root/production/orders:ssd");
            TReportBudget budget;
            NKikimrBridge::TGroupSynchronizationResult result;
            result.set_cluster_state_generation(42);
            result.mutable_observed_at()->set_seconds(1'700'000'000);
            result.mutable_observed_at()->set_nanos(123'456'000);
            const auto usageBefore = TRusage::Get();
            const auto rssBefore = TRusage::GetCurrentRSS();
            const auto started = TInstant::Now();
            for (ui32 i = 0; i < groupCount; ++i) {
                UNIT_ASSERT(budget.Visit());
                for (ui32 pile = 0; pile < 3; ++pile) {
                    group.MutablePile(pile)->SetGroupId(0x83000000 + 3 * i + pile);
                }
                const TGroupContext context(&group);
                for (ui32 pile = 0; pile < 3; ++pile) {
                    UNIT_ASSERT(budget.Visit());
                    const auto copy = ObserveCopy({.Group = context, .PileId = TBridgePileId::FromPileIndex(pile),
                                                   .PileState = TCluster::SYNCHRONIZED});
                    UNIT_ASSERT(AppendGroup({0x82000000 + i, copy.Part->GetGroupId(), &pool,
                                             pileNames[pile], copy}, budget, result));
                }
            }
            const auto elapsed = TInstant::Now() - started;
            const auto usageAfter = TRusage::Get();
            UNIT_ASSERT_VALUES_EQUAL(result.groups_size(), 3 * groupCount);
            UNIT_ASSERT(result.ByteSizeLong() < TEvBlobStorage::TEvControllerQueryBridgeStorage::MaxResultBytes);
            Cerr << "Bridge diagnostics materialization: groups=" << groupCount << " rows=" << result.groups_size()
                 << " protobuf_bytes=" << result.ByteSizeLong() << " protobuf_heap_bytes=" << result.SpaceUsedLong()
                 << " elapsed_us=" << elapsed.MicroSeconds()
                 << " cpu_us=" << ((usageAfter.Utime - usageBefore.Utime) + (usageAfter.Stime - usageBefore.Stime)).MicroSeconds()
                 << " rss_before=" << rssBefore << " rss_after=" << TRusage::GetCurrentRSS()
                 << " peak_rss=" << usageAfter.MaxRss << Endl;
        }
    }
} // Y_UNIT_TEST_SUITE(BridgeStorageState)
