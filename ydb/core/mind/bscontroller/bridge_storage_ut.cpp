#include "bridge_storage.h"

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NBsController;
using namespace NKikimr::NBsController::NBridgeStorage;

namespace {

    using TGroup = NKikimrBridge::TGroupState;
    using TStorageGroup = NKikimrBridge::TStorageGroup;
    using TDatabase = NKikimrBridge::TStorageDatabase;
    using TQuery = NKikimrBridge::TStorageQuery;
    using TCluster = NKikimrBridge::TClusterState;

    const TBridgePileId FirstPile = TBridgePileId::FromPileIndex(0);
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

    TPoolSummary MakePool(TBoxStoragePoolId id, TString name, TPathId owner, EMode mode) {
        TPoolSummary pool{.Id = id, .Name = std::move(name), .Owner = owner, .Mode = mode, .Copies = {}};
        pool.Copies[FirstPile];
        pool.Copies[SecondPile];
        return pool;
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
        UNIT_ASSERT(ObserveCopy(context).State == TStorageGroup::PENDING);
        context.SyncerRunning = true;
        UNIT_ASSERT(ObserveCopy(context).State == TStorageGroup::PENDING);
        group.MutablePile(1)->SetBecameUnsyncedGeneration(42);
        group.MutablePile(1)->SetStage(TGroup::BLOCKS);
        UNIT_ASSERT(ObserveCopy(context).State == TStorageGroup::SYNCING);
        context.SyncerRunning = false;
        UNIT_ASSERT(ObserveCopy(context).State == TStorageGroup::PENDING);
        group.MutablePile(1)->SetStage(TGroup::SYNCED);
        UNIT_ASSERT(ObserveCopy(context).State == TStorageGroup::SYNCED);
    }

    Y_UNIT_TEST(DisconnectedStoredCompletionIsUnknownUnlessCopyIsAuthoritative) {
        auto group = MakeGroup();
        TCopyContext context{
            .Group = TGroupContext(&group),
            .PileId = SecondPile,
            .PileState = TCluster::DISCONNECTED,
        };
        UNIT_ASSERT(ObserveCopy(context).State == TStorageGroup::UNKNOWN);
        context.PileState = TCluster::SUSPENDED;
        UNIT_ASSERT(ObserveCopy(context).State == TStorageGroup::UNKNOWN);

        NKikimrBridge::TClusterStateDetails::TPileSyncState sync;
        sync.SetBecameUnsyncedGeneration(42);
        context.PileSyncState = &sync;
        group.SetAuthorityPileId(SecondPile.GetLocalDb());
        context.Group = TGroupContext(&group);
        UNIT_ASSERT(ObserveCopy(context).State == TStorageGroup::SYNCED);
        // A sole part is authoritative even without an explicit AuthorityPileId.
        group.ClearAuthorityPileId();
        group.MutablePile(0)->SetAbsent(true);
        context.Group = TGroupContext(&group);
        UNIT_ASSERT(ObserveCopy(context).State == TStorageGroup::SYNCED);
    }

    Y_UNIT_TEST(AbsentPartsAndOrdinaryOrInactiveStorageRemainDistinct) {
        auto group = MakeGroup();
        group.MutablePile(1)->SetAbsent(true);
        TCopyContext context{
            .Group = TGroupContext(&group),
            .PileId = SecondPile,
            .PileState = TCluster::NOT_SYNCHRONIZED_1,
        };
        UNIT_ASSERT(ObserveCopy(context).State == TStorageGroup::PENDING);
        context.BridgeMode = false;
        UNIT_ASSERT(ObserveCopy(context).State == TStorageGroup::NOT_REQUIRED);
        context.BridgeMode = true;
        context.PileState = TCluster::INACTIVE;
        UNIT_ASSERT(ObserveCopy(context).State == TStorageGroup::NOT_REQUIRED);
        context.Group = TGroupContext();
        context.PileState = TCluster::SYNCHRONIZED;
        UNIT_ASSERT(ObserveCopy(context).State == TStorageGroup::UNKNOWN);
    }

    Y_UNIT_TEST(OwnerAggregationPreservesPoolIdentityAndSharesFilterRules) {
        const TPathId sharedOwner(1000, 5);
        auto bridge = MakePool({1, 1}, "ssd, fast", sharedOwner, TDatabase::BRIDGED);
        bridge.Copies[FirstPile].Groups.Count(TStorageGroup::SYNCED);
        bridge.Copies[SecondPile].Groups.Count(TStorageGroup::SYNCING);
        bridge.Copies[SecondPile].LastError = "previous attempt";
        bridge.Copies[SecondPile].LastErrorTimestamp = TInstant::MicroSeconds(123);
        auto ordinary = MakePool({1, 2}, "hdd", sharedOwner, TDatabase::ORDINARY);
        TPoolSummaries pools;
        pools.emplace(bridge.Id, bridge);
        pools.emplace(ordinary.Id, ordinary);
        const auto all = BuildDatabaseSummaries(pools);
        UNIT_ASSERT_VALUES_EQUAL(all.size(), 2);
        const auto& row = all.at({{sharedOwner, {}}, SecondPile});
        UNIT_ASSERT(row.Mode == TDatabase::MIXED);
        UNIT_ASSERT_VALUES_EQUAL(row.StoragePools.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(row.StoragePools[0], "ssd, fast");
        UNIT_ASSERT_VALUES_EQUAL(row.StoragePools[1], "hdd");
        UNIT_ASSERT_VALUES_EQUAL(row.Copy.Groups.Total, 1);
        UNIT_ASSERT_VALUES_EQUAL(row.Copy.Groups.Running, 1);
        UNIT_ASSERT_VALUES_EQUAL(row.Copy.LastError, "previous attempt");

        const std::set<TBoxStoragePoolId> ordinaryOnly{{1, 2}};
        const auto filtered = BuildDatabaseSummaries(pools, &ordinaryOnly);
        UNIT_ASSERT_VALUES_EQUAL(filtered.size(), 1);
        const auto& ordinaryRow = filtered.at({{sharedOwner, {}}, {}});
        UNIT_ASSERT(ordinaryRow.Mode == TDatabase::ORDINARY);
        UNIT_ASSERT_VALUES_EQUAL(ordinaryRow.StoragePools.size(), 1);
        UNIT_ASSERT(ordinaryRow.Copy.Groups.State() == TStorageGroup::NOT_REQUIRED);
        UNIT_ASSERT_VALUES_EQUAL(ordinaryRow.Copy.Groups.Total, 0);

        const std::set<TBoxStoragePoolId> allPools{{1, 1}, {1, 2}};
        const auto fullFilter = BuildDatabaseSummaries(pools, &allPools);
        UNIT_ASSERT_VALUES_EQUAL(fullFilter.size(), all.size());
        UNIT_ASSERT_VALUES_EQUAL(fullFilter.at({{sharedOwner, {}}, SecondPile}).Copy.Groups.Running, 1);
    }

    Y_UNIT_TEST(UnknownOwnersStaySeparatedAndZeroGroupPoolsRemainVisible) {
        TPoolSummaries pools;
        pools.emplace(TBoxStoragePoolId{1, 1}, MakePool({1, 1}, "first", TPathId(0, 0), TDatabase::ORDINARY));
        pools.emplace(TBoxStoragePoolId{1, 2}, MakePool({1, 2}, "second", TPathId(0, 0), TDatabase::ORDINARY));
        const auto rows = BuildDatabaseSummaries(pools);
        UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rows.at({{TPathId(0, 0), {1, 1}}, {}}).StoragePools[0], "first");
        UNIT_ASSERT_VALUES_EQUAL(rows.at({{TPathId(0, 0), {1, 2}}, {}}).StoragePools[0], "second");
    }

    Y_UNIT_TEST(TypedCursorsAreCompactBoundToQueryAndGeneration) {
        TQuery query;
        query.SetView(TQuery::DATABASES);
        query.SetPileName(TString(800, 'a'));
        query.SetPageSize(1);
        const TDatabaseKey key{{TPathId(1000, 5), {}}, SecondPile};
        const TString encoded = MakePageToken(query, 42, key);
        UNIT_ASSERT(encoded.size() < 1024);
        query.SetPageToken(encoded);
        NKikimrBridge::TStoragePageToken parsed;
        UNIT_ASSERT(ParsePageToken(query, 42, parsed));
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetDatabase().GetPileId(), SecondPile.GetLocalDb());
        UNIT_ASSERT(!ParsePageToken(query, 43, parsed));
        query.SetPageSize(100);
        UNIT_ASSERT(ParsePageToken(query, 42, parsed));
        query.SetNotSynced(true);
        UNIT_ASSERT(!ParsePageToken(query, 42, parsed));
        query.SetNotSynced(false);

        // Even a token with the correct hash must have the view's cursor type.
        NKikimrBridge::TStoragePageToken wrongType;
        wrongType.SetGeneration(42);
        wrongType.SetQueryHash(QueryHash(query));
        wrongType.MutableGroup()->SetStoragePoolId(1);
        wrongType.MutableGroup()->SetGroupId(100);
        query.SetPageToken(Base64Encode(wrongType.SerializeAsString()));
        UNIT_ASSERT(!ParsePageToken(query, 42, parsed));
        query.SetPageToken("not a token");
        UNIT_ASSERT(!ParsePageToken(query, 42, parsed));
        query.SetPageToken(TString(1025, 'a'));
        UNIT_ASSERT(!ParsePageToken(query, 42, parsed));
    }

    Y_UNIT_TEST(GroupCursorDistinguishesPartialAndWholeGroupPosition) {
        TQuery query;
        query.SetView(TQuery::GROUPS);
        query.SetStoragePool("pool");
        NKikimrBridge::TStoragePageToken::TGroupCursor position;
        position.SetBoxId(1);
        position.SetStoragePoolId(2);
        position.SetGroupId(100);
        position.SetLastPileId(FirstPile.GetLocalDb());
        query.SetPageToken(MakePageToken(query, 42, position));
        NKikimrBridge::TStoragePageToken parsed;
        UNIT_ASSERT(ParsePageToken(query, 42, parsed));
        UNIT_ASSERT(parsed.GetGroup().HasLastPileId());
        position.ClearLastPileId();
        query.SetPageToken(MakePageToken(query, 42, position));
        UNIT_ASSERT(ParsePageToken(query, 42, parsed));
        UNIT_ASSERT(!parsed.GetGroup().HasLastPileId());
        position.SetLastPileId(0);
        query.SetPageToken(MakePageToken(query, 42, position));
        UNIT_ASSERT(!ParsePageToken(query, 42, parsed));
    }
} // Y_UNIT_TEST_SUITE(BridgeStorageState)
