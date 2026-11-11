#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/base/bridge_events.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/core/blobstorage/vdisk/scrub/scrub_actor.h>

#include <set>

using namespace NBridgeTest;

namespace {

    // switch BridgeMode of the storage pool of its current generation
    NKikimrBlobStorage::TConfigResponse ChangeStoragePoolBridgeMode(TEnvironmentSetup& env, ui64 storagePoolId,
            bool bridgeMode) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableChangeStoragePoolBridgeMode();
        cmd->SetBoxId(1);
        cmd->SetStoragePoolId(storagePoolId);
        cmd->SetBridgeMode(bridgeMode);
        cmd->SetItemConfigGeneration(ReadStoragePool(env, storagePoolId).GetItemConfigGeneration());
        return env.Invoke(request);
    }

    // piles of 9 nodes, each pile being a datacenter of its own; non-uniform storage is enabled
    TEnvironmentSetup::TSettings MakePileSettings(ui32 numPiles, bool enableDeepScrubbing = false) {
        return {
            .NodeCount = 9 * numPiles,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .PrepareRuntime = [](TTestActorSystem& runtime) {
                runtime.GetAppDataBridgeConfig().SetEnableNonUniformStorage(true);
            },
            .LocationGenerator = [](ui32 nodeId) {
                NActorsInterconnect::TNodeLocation location;
                const ui32 pile = (nodeId - 1) / 9 + 1;
                location.SetDataCenter(TStringBuilder() << "dc_" << pile);
                location.SetBridgePileName(TStringBuilder() << "pile_" << pile);
                location.SetRack(TStringBuilder() << "rack_" << nodeId);
                location.SetUnit(TStringBuilder() << "unit_" << nodeId);
                return TNodeLocation(location);
            },
            .SelfManagementConfig = true,
            .EnableDeepScrubbing = enableDeepScrubbing,
            .NumPiles = numPiles,
            .AutomaticBootstrap = true,
        };
    }

    TEnvironmentSetup::TSettings MakeTwoPileSettings() {
        return MakePileSettings(2);
    }

    constexpr ui32 FirstNodeOfPile[] = {1, 10};

    // ordinary pool with the settings of pool 1, its groups reside in the datacenter of the designated pile
    void CreateOrdinaryPool(TEnvironmentSetup& env, ui64 storagePoolId, ui32 numGroups, ui32 pileIndex,
            ui32 numFailDomains = 0) {
        auto pool = ReadStoragePool(env, 1);
        pool.SetStoragePoolId(storagePoolId);
        pool.SetName(TStringBuilder() << "pool_" << storagePoolId);
        pool.SetKind(pool.GetName());
        pool.ClearItemConfigGeneration();
        pool.SetBridgeMode(false);
        pool.SetNumGroups(numGroups);
        if (numFailDomains) {
            auto *geometry = pool.MutableGeometry();
            geometry->SetNumFailRealms(1);
            geometry->SetNumFailDomainsPerFailRealm(numFailDomains);
            geometry->SetNumVDisksPerFailDomain(1);
        }
        UNIT_ASSERT(pool.PDiskFilterSize());
        pool.MutablePDiskFilter(0)->AddProperty()->MutableLocation()->SetDataCenter(TStringBuilder() << "dc_"
            << pileIndex + 1);
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.Sim(TDuration::Seconds(30));
    }

    // let the pool use PDisks of any datacenter
    void DropLocationFilter(TEnvironmentSetup& env, ui64 storagePoolId) {
        auto pool = ReadStoragePool(env, storagePoolId);
        for (auto& filter : *pool.MutablePDiskFilter()) {
            auto *properties = filter.MutableProperty();
            for (int i = properties->size() - 1; i >= 0; --i) {
                if (properties->Get(i).HasLocation()) {
                    properties->DeleteSubrange(i, 1);
                }
            }
        }
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    // make the pool placed in the single datacenter bridged one: this fails, as its parts can't be placed in other
    // piles, and nothing changes; then the pool is allowed to use PDisks of any datacenter
    void MakeLocatedPoolBridged(TEnvironmentSetup& env, ui64 storagePoolId, const std::vector<ui32>& groups) {
        auto response = ChangeStoragePoolBridgeMode(env, storagePoolId, true);
        UNIT_ASSERT_C(!response.GetSuccess(), "parts can't be placed outside the datacenter of the pool");
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "Group fit error");
        UNIT_ASSERT(!ReadStoragePool(env, storagePoolId).GetBridgeMode());
        for (ui32 groupId : groups) {
            UNIT_ASSERT(!GetGroupInfo(env, groupId).HasBridgeGroupState());
        }
        DropLocationFilter(env, storagePoolId);
        response = ChangeStoragePoolBridgeMode(env, storagePoolId, true);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    // groups of the pool as tablets see them: ordinary groups, inline piles and bridge proxies, but not other parts
    std::vector<ui32> GetLogicalPoolGroups(TEnvironmentSetup& env, ui64 storagePoolId) {
        std::vector<ui32> res;
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto *group : GetPoolGroups(baseConfig, storagePoolId)) {
            const auto info = GetGroupInfo(env, group->GetGroupId());
            if (!info.HasBridgePileId() || info.GetBridgeProxyGroupId() == group->GetGroupId()) {
                res.push_back(group->GetGroupId());
            }
        }
        return res;
    }

    bool HasGroup(const NKikimrBlobStorage::TBaseConfig& baseConfig, ui32 groupId) {
        for (const auto& group : baseConfig.GetGroup()) {
            if (group.GetGroupId() == groupId) {
                return true;
            }
        }
        return false;
    }

    // wait for a full scrub cycle of every VDisk
    void WaitForScrubCycles(TEnvironmentSetup& env, const std::vector<TActorId>& vdiskActorIds) {
        for (const TActorId& vdiskActorId : vdiskActorIds) {
            const ui32 nodeId = vdiskActorId.NodeId();
            const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
            const TInstant deadline = env.Runtime->GetClock() + TDuration::Minutes(5);
            for (;;) {
                env.Runtime->Send(new IEventHandle(TEvBlobStorage::EvScrubAwait, 0, vdiskActorId, edge, nullptr,
                    RandomNumber<ui64>()), nodeId);
                auto ev = env.WaitForEdgeActorEvent<TEvScrubNotify>(edge, false, deadline);
                UNIT_ASSERT_C(ev, "no successful scrub cycle of the VDisk with ActorId# " << vdiskActorId);
                if (ev->Get()->Success) {
                    break;
                }
            }
        }
    }

    // the group has synced parts in both piles and relies on none of them in particular
    bool IsFullyBridged(const NKikimrBlobStorage::TGroupInfo& info) {
        const auto& state = info.GetBridgeGroupState();
        size_t numSynced = 0;
        NBridge::ForEachGroupPile(state, [&](TBridgePileId, const NKikimrBridge::TGroupState::TPile& pile) {
            numSynced += pile.GetStage() == NKikimrBridge::TGroupState::SYNCED;
        });
        return !state.GetAuthorityPileId() && numSynced == 2;
    }

    // the group has the only part, which is in the designated pile
    bool HasOnlyPart(TEnvironmentSetup& env, const NKikimrBlobStorage::TGroupInfo& info, ui32 pileIndex) {
        const auto& state = info.GetBridgeGroupState();
        size_t numParts = 0;
        NBridge::ForEachGroupPile(state, [&](TBridgePileId, const NKikimrBridge::TGroupState::TPile&) { ++numParts; });
        const auto *pile = NBridge::GetGroupPile(state, TBridgePileId::FromPileIndex(pileIndex));
        if (numParts != 1 || !pile || state.GetAuthorityPileId()) {
            return false;
        }
        // VDisks of the part reside in the pile
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& vslot : baseConfig.GetVSlot()) {
            if (vslot.GetGroupId() == pile->GetGroupId()) {
                const ui32 nodeId = vslot.GetVSlotId().GetNodeId();
                UNIT_ASSERT(FirstNodeOfPile[pileIndex] <= nodeId && nodeId < FirstNodeOfPile[pileIndex] + 9);
            }
        }
        return true;
    }

    // wait until the node warden of the node knows the pile state
    void WaitForLocalPileState(TEnvironmentSetup& env, ui32 nodeId, ui32 pileIndex,
            NKikimrBridge::TClusterState::EPileState state) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), edge,
            new TEvNodeWardenQueryStorageConfig(true)), nodeId);
        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
        for (;;) {
            auto response = env.WaitForEdgeActorEvent<TEvNodeWardenStorageConfig>(edge, false, deadline);
            UNIT_ASSERT_C(response, "timed out waiting for the local pile state");
            if (response->Get()->Config->GetClusterState().GetPerPileState(pileIndex) == state) {
                break;
            }
        }
        env.Runtime->Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, MakeBlobStorageNodeWardenID(nodeId), edge,
            nullptr, 0), nodeId);
    }

    NKikimrProto::EReplyStatus TryPut(TEnvironmentSetup& env, ui32 groupId, ui32 nodeId, const TLogoBlobID& id,
            const TString& data) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data,
                env.Runtime->GetClock() + TDuration::Seconds(30)));
        });
        return env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false)->Get()->Status;
    }

    // settings of pools of the kind 'pool_2' for the second pile
    void AddPoolPileOverride(YAML::Node& config, const TString& erasure, ui32 numFailDomains) {
        YAML::Node rule;
        rule["pile_name"].push_back("pile_2");
        rule["kind"].push_back("pool_2");
        rule["static_group_override"] = false;
        rule["erasure_species"] = erasure;
        rule["geometry"] = YAML::Node(YAML::NodeType::Map);
        if (numFailDomains) {
            auto geometry = rule["geometry"];
            geometry["num_fail_realms"] = 1;
            geometry["num_fail_domains_per_fail_realm"] = numFailDomains;
            geometry["num_vdisks_per_fail_domain"] = 1;
        }
        config["blob_storage_config"]["storage_pool_pile_overrides"].push_back(rule);
    }

    void SetPoolPileOverride(TEnvironmentSetup& env, const TString& erasure, ui32 numFailDomains) {
        const auto result = ReplaceMainConfig(env, [&](YAML::Node& config) {
            AddPoolPileOverride(config, erasure, numFailDomains);
        });
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            result.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
    }

    void CheckIncompatiblePoolPileOverride(const TString& erasure, ui32 baseNumFailDomains,
            ui32 overrideNumFailDomains) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        CreateOrdinaryPool(env, 2, 1, 0, baseNumFailDomains);
        CreateOrdinaryPool(env, 3, 1, 1, baseNumFailDomains);
        const ui32 firstGroupId = GetLogicalPoolGroups(env, 2).front();
        const ui32 secondGroupId = GetLogicalPoolGroups(env, 3).front();
        UNIT_ASSERT_LT(firstGroupId, secondGroupId);
        DropLocationFilter(env, 2);

        NKikimrBlobStorage::TConfigRequest request;
        auto *move = request.AddCommand()->MutableMoveGroups();
        move->SetBoxId(1);
        move->SetOriginStoragePoolId(3);
        move->SetTargetStoragePoolId(2);
        move->SetOriginStoragePoolGeneration(ReadStoragePool(env, 3).GetItemConfigGeneration());
        move->SetTargetStoragePoolGeneration(ReadStoragePool(env, 2).GetItemConfigGeneration());
        const auto moved = env.Invoke(request);
        UNIT_ASSERT_C(moved.GetSuccess(), moved.GetErrorDescription());

        TDataChecker checker(env);
        checker.Write(firstGroupId);
        checker.Write(secondGroupId);
        SetPoolPileOverride(env, erasure, overrideNumFailDomains);

        const auto poolBefore = ReadStoragePool(env, 2);
        const auto baseBefore = env.FetchBaseConfig();
        const auto firstBefore = GetGroupInfo(env, firstGroupId);
        const auto secondBefore = GetGroupInfo(env, secondGroupId);
        const auto response = ChangeStoragePoolBridgeMode(env, 2, true);
        UNIT_ASSERT_C(!response.GetSuccess(), "conversion accepted an incompatible pile override");
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "erasure or topology");
        UNIT_ASSERT_VALUES_EQUAL(ReadStoragePool(env, 2).SerializeAsString(), poolBefore.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(GetGroupInfo(env, firstGroupId).SerializeAsString(), firstBefore.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(GetGroupInfo(env, secondGroupId).SerializeAsString(), secondBefore.SerializeAsString());

        const auto baseAfter = env.FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(baseAfter.GroupSize(), baseBefore.GroupSize());
        UNIT_ASSERT_VALUES_EQUAL(baseAfter.VSlotSize(), baseBefore.VSlotSize());
        for (const auto& before : baseBefore.GetGroup()) {
            const auto& after = FindGroup(baseAfter, before.GetGroupId());
            UNIT_ASSERT_VALUES_EQUAL(after.GetGroupGeneration(), before.GetGroupGeneration());
            UNIT_ASSERT_VALUES_EQUAL(after.GetIsProxyGroup(), before.GetIsProxyGroup());
            UNIT_ASSERT_VALUES_EQUAL(after.VSlotIdSize(), before.VSlotIdSize());
            for (size_t i = 0; i < before.VSlotIdSize(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(after.GetVSlotId(i).SerializeAsString(), before.GetVSlotId(i).SerializeAsString());
            }
        }
        checker.CheckAll();
    }

}

namespace {
    NKikimrBridge::TStorageState QueryStorageDiagnostics(TEnvironmentSetup& env, const NKikimrBridge::TStorageQuery& query) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        auto request = std::make_unique<TEvBlobStorage::TEvControllerQueryBridgeStorage>();
        request->Record.CopyFrom(query);
        env.Runtime->SendToPipe(env.TabletId, edge, request.release(), 0,
            TTestActorSystem::GetPipeConfigWithRetries());
        auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerQueryBridgeStorageResult>(edge, false);
        return response->Get()->Record;
    }
}

Y_UNIT_TEST_SUITE(BridgePoolMode) {

    Y_UNIT_TEST(RejectIncompatiblePileOverrideErasure) {
        CheckIncompatiblePoolPileOverride("none", 8, 0);
    }

    Y_UNIT_TEST(RejectIncompatiblePileOverrideLargerTopology) {
        CheckIncompatiblePoolPileOverride("block-4-2", 8, 9);
    }

    Y_UNIT_TEST(RejectIncompatiblePileOverrideSmallerTopology) {
        CheckIncompatiblePoolPileOverride("block-4-2", 9, 8);
    }

    Y_UNIT_TEST(CompatiblePileOverrideAllowsConversionAndRepair) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        CreateOrdinaryPool(env, 2, 1, 1);
        const ui32 groupId = GetLogicalPoolGroups(env, 2).front();
        DropLocationFilter(env, 2);
        TDataChecker checker(env);
        checker.Write(groupId);
        SetPoolPileOverride(env, "block-4-2", 8);

        const auto poolBefore = ReadStoragePool(env, 2);
        const auto response = ChangeStoragePoolBridgeMode(env, 2, true);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        WaitUntil(env, [&] { return IsFullyBridged(GetGroupInfo(env, groupId)); }, "group fully bridged");
        const auto info = GetGroupInfo(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL(info.GetBridgeProxyGroupId(), groupId);
        UNIT_ASSERT_VALUES_EQUAL(info.GetErasureSpecies(), TBlobStorageGroupType::Erasure4Plus2Block);
        UNIT_ASSERT_C(TBridgePileId::FromProto(&info, &NKikimrBlobStorage::TGroupInfo::GetBridgePileId) ==
            TBridgePileId::FromPileIndex(1), info.ShortDebugString());

        auto expectedPool = poolBefore;
        expectedPool.SetBridgeMode(true);
        expectedPool.SetItemConfigGeneration(poolBefore.GetItemConfigGeneration() + 1);
        UNIT_ASSERT_VALUES_EQUAL(ReadStoragePool(env, 2).SerializeAsString(), expectedPool.SerializeAsString());
        NKikimrBlobStorage::TConfigRequest redefine;
        redefine.AddCommand()->MutableDefineStoragePool()->CopyFrom(ReadStoragePool(env, 2));
        const auto redefined = env.Invoke(redefine);
        UNIT_ASSERT_C(redefined.GetSuccess(), redefined.GetErrorDescription());

        ReassignOneDisk(env, groupId);
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(GetGroupInfo(env, groupId).GetErasureSpecies(),
            TBlobStorageGroupType::Erasure4Plus2Block);
        checker.Write(groupId);
        checker.CheckAll();
    }

    Y_UNIT_TEST(StorageDiagnosticsReportNonBridgeClusterWithoutRefreshingError) {
        TEnvironmentSetup env({.NodeCount = 1});
        NKikimrBridge::TStorageQuery query;
        query.SetView(NKikimrBridge::TStorageQuery::PILES);
        const auto report = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(report.GetStatus() == Ydb::StatusIds::NOT_FOUND, report.DebugString());
        UNIT_ASSERT(!report.HasPiles());
        UNIT_ASSERT_VALUES_EQUAL(report.GetErrorMessage(), "Bridge cluster state is not configured");
    }

    Y_UNIT_TEST(StorageDiagnosticsPileFilterDistinguishesEmptyAndNamedOrdinaryPlacement) {
        TEnvironmentSetup env({
            .NodeCount = 18,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .ConfigPreprocessor = [](ui32, TNodeWardenConfig& config) {
                // The environment's YAML builder emits an empty name as null; preserve the actual empty string.
                auto yaml = YAML::Load(config.StartupConfigYaml);
                auto main = yaml["config"];
                main["bridge_config"]["piles"][0]["name"] = "";
                for (auto host : main["hosts"]) {
                    auto location = host["location"];
                    if (location["bridge_pile_name"].IsNull()) {
                        location["bridge_pile_name"] = "";
                    }
                }
                config.StartupConfigYaml = TString(YAML::Dump(yaml));
            },
            .PrepareRuntime = [](TTestActorSystem& runtime) {
                auto& bridge = runtime.GetAppDataBridgeConfig();
                bridge.SetEnableNonUniformStorage(true);
                bridge.MutablePiles(0)->SetName("");
                bridge.MutablePiles(1)->SetName("b");
            },
            .LocationGenerator = [](ui32 nodeId) {
                NActorsInterconnect::TNodeLocation location;
                const ui32 pile = (nodeId - 1) / 9 + 1;
                location.SetDataCenter(TStringBuilder() << "dc_" << pile);
                location.SetBridgePileName(pile == 1 ? "" : "b");
                location.SetRack(TStringBuilder() << "rack_" << nodeId);
                location.SetUnit(TStringBuilder() << "unit_" << nodeId);
                return TNodeLocation(location);
            },
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        });
        env.CreatePool(2);
        CreateOrdinaryPool(env, 2, 1, 1);
        const auto groups = GetLogicalPoolGroups(env, 1);
        WaitUntil(env, [&] {
            return std::ranges::all_of(groups, [&](ui32 id) { return IsFullyBridged(GetGroupInfo(env, id)); });
        }, "groups synchronized with an empty pile name");
        NKikimrBridge::TStorageQuery query;
        query.SetView(NKikimrBridge::TStorageQuery::GROUPS);
        query.SetStoragePool(ReadStoragePool(env, 1).GetName());
        const auto all = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(all.GetStatus() == Ydb::StatusIds::SUCCESS, all.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(all.GetGroups().groups_size(), 2 * groups.size());
        query.SetPileName("b");
        const auto named = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(named.GetStatus() == Ydb::StatusIds::SUCCESS, named.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(named.GetGroups().groups_size(), groups.size());
        for (const auto& row : named.GetGroups().groups()) {
            UNIT_ASSERT_VALUES_EQUAL(row.pile_name(), "b");
        }
        query.SetPileName("");
        const auto empty = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(empty.GetStatus() == Ydb::StatusIds::SUCCESS, empty.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(empty.GetGroups().groups_size(), groups.size());
        for (const auto& row : empty.GetGroups().groups()) {
            UNIT_ASSERT(row.has_pile_name());
            UNIT_ASSERT(row.pile_name().empty());
            UNIT_ASSERT(row.has_physical_group_id());
        }
        query.SetStoragePool("pool_2");
        UNIT_ASSERT_VALUES_EQUAL(QueryStorageDiagnostics(env, query).GetGroups().groups_size(), 0);
        query.ClearPileName();
        const auto ordinary = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_VALUES_EQUAL(ordinary.GetGroups().groups_size(), 1);
        UNIT_ASSERT(ordinary.GetGroups().groups(0).has_pile_name());
        UNIT_ASSERT_VALUES_EQUAL(ordinary.GetGroups().groups(0).pile_name(), "b");
        UNIT_ASSERT(ordinary.GetGroups().groups(0).sync_status() == NKikimrBridge::NOT_REQUIRED);
        query.SetPileName("b");
        UNIT_ASSERT_VALUES_EQUAL(QueryStorageDiagnostics(env, query).GetGroups().groups_size(), 1);

        const auto converted = ChangeStoragePoolBridgeMode(env, 1, false);
        UNIT_ASSERT_C(converted.GetSuccess(), converted.GetErrorDescription());
        WaitUntil(env, [&] {
            return std::ranges::all_of(groups, [&](ui32 id) { return HasOnlyPart(env, GetGroupInfo(env, id), 0); });
        }, "ordinary groups retain the empty-named primary pile");
        query.SetStoragePool(ReadStoragePool(env, 1).GetName());
        query.SetPileName("b");
        UNIT_ASSERT_VALUES_EQUAL(QueryStorageDiagnostics(env, query).GetGroups().groups_size(), 0);
        query.SetPileName("");
        const auto retained = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_VALUES_EQUAL(retained.GetGroups().groups_size(), groups.size());
        for (const auto& row : retained.GetGroups().groups()) {
            UNIT_ASSERT(row.has_pile_name());
            UNIT_ASSERT(row.pile_name().empty());
            UNIT_ASSERT(row.sync_status() == NKikimrBridge::NOT_REQUIRED);
        }
        query.SetView(NKikimrBridge::TStorageQuery::DATABASES);
        const auto emptyDatabase = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_VALUES_EQUAL(emptyDatabase.GetDatabases().databases_size(), 1);
        UNIT_ASSERT(emptyDatabase.GetDatabases().databases(0).has_pile_name());
        UNIT_ASSERT(emptyDatabase.GetDatabases().databases(0).pile_name().empty());
        UNIT_ASSERT(emptyDatabase.GetDatabases().databases(0).storage_mode() == NKikimrBridge::ORDINARY);
        UNIT_ASSERT_VALUES_EQUAL(emptyDatabase.GetDatabases().databases(0).groups().required(), 0);
        query.SetPileName("b");
        const auto otherPile = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(otherPile.GetStatus() == Ydb::StatusIds::SUCCESS, otherPile.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(otherPile.GetDatabases().databases_size(), 0);
        query.ClearPileName();
        const auto database = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_VALUES_EQUAL(database.GetDatabases().databases_size(), 1);
        UNIT_ASSERT(database.GetDatabases().databases(0).has_pile_name());
        UNIT_ASSERT(database.GetDatabases().databases(0).pile_name().empty());
    }

    Y_UNIT_TEST(StorageDiagnosticsReturnCompleteListsAndQualifiedPools) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool(2);
        CreateOrdinaryPool(env, 2, 1, 1);
        const auto logical = GetLogicalPoolGroups(env, 1);
        UNIT_ASSERT_VALUES_EQUAL(logical.size(), 2);
        WaitUntil(env, [&] {
            return std::ranges::all_of(logical, [&](ui32 id) { return IsFullyBridged(GetGroupInfo(env, id)); });
        }, "groups synchronized before full-list diagnostics");
        NKikimrBridge::TStorageQuery query;
        query.SetView(NKikimrBridge::TStorageQuery::PILES);
        const auto overview = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(overview.GetStatus() == Ydb::StatusIds::SUCCESS, overview.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(overview.GetPiles().piles_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(overview.GetPiles().unattributed_storage_pools(), 2);
        UNIT_ASSERT_VALUES_EQUAL(overview.GetPiles().cluster_state_generation(), QueryStorageConfig(env).GetClusterState().GetGeneration());
        UNIT_ASSERT(overview.GetPiles().has_observed_at());
        for (const auto& row : overview.GetPiles().piles()) {
            UNIT_ASSERT_VALUES_EQUAL(row.databases().total(), 0); // pools have no owner metadata yet
            UNIT_ASSERT_VALUES_EQUAL(row.databases().synced(), 0);
            UNIT_ASSERT_VALUES_EQUAL(row.databases().unknown(), 0); // an unattributed pool is not an unknown database
        }
        query.SetView(NKikimrBridge::TStorageQuery::GROUPS);
        query.SetStoragePool(ReadStoragePool(env, 1).GetName());
        const auto groups = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(groups.GetStatus() == Ydb::StatusIds::SUCCESS, groups.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(groups.GetGroups().groups_size(), 2 * logical.size());
        std::set<std::pair<ui32, TString>> identities;
        for (const auto& row : groups.GetGroups().groups()) {
            UNIT_ASSERT(identities.emplace(row.group_id(), row.pile_name()).second);
            UNIT_ASSERT_VALUES_EQUAL(row.storage_pool().id().box_id(), 1);
            UNIT_ASSERT_VALUES_EQUAL(row.storage_pool().id().pool_id(), 1);
            UNIT_ASSERT(row.sync_status() == NKikimrBridge::SYNCED);
            const auto info = GetGroupInfo(env, row.group_id());
            const auto* part = NBridge::GetGroupPile(info.GetBridgeGroupState(),
                TBridgePileId::FromPileIndex(row.pile_name() == "pile_1" ? 0 : 1));
            UNIT_ASSERT(part);
            UNIT_ASSERT_VALUES_EQUAL(row.physical_group_id(), part->GetGroupId());
        }
        query.AddStatuses(NKikimrBridge::UNKNOWN);
        UNIT_ASSERT_VALUES_EQUAL(QueryStorageDiagnostics(env, query).GetGroups().groups_size(), 0);
        query.AddStatuses(NKikimrBridge::SYNCED);
        query.AddStatuses(NKikimrBridge::UNKNOWN);
        UNIT_ASSERT_VALUES_EQUAL(QueryStorageDiagnostics(env, query).GetGroups().groups_size(), 2 * logical.size());
        query.ClearStatuses();
        query.AddStatuses(NKikimrBridge::SYNCHRONIZATION_STATUS_UNSPECIFIED);
        const auto invalid = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT(invalid.GetStatus() == Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT(!invalid.HasGroups());
        query.ClearStatuses();
        query.MutableStatuses()->Add(1000);
        UNIT_ASSERT(QueryStorageDiagnostics(env, query).GetStatus() == Ydb::StatusIds::BAD_REQUEST);
        query.ClearStatuses();

        query.Clear();
        query.SetView(NKikimrBridge::TStorageQuery::DATABASES);
        const auto databases = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(databases.GetStatus() == Ydb::StatusIds::SUCCESS, databases.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(databases.GetDatabases().databases_size(), 3);
        query.MutableStoragePoolId()->set_box_id(1);
        query.MutableStoragePoolId()->set_pool_id(2);
        const auto ordinary = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_VALUES_EQUAL(ordinary.GetDatabases().databases_size(), 1);
        const auto& row = ordinary.GetDatabases().databases(0);
        UNIT_ASSERT(row.storage_mode() == NKikimrBridge::ORDINARY);
        UNIT_ASSERT(row.has_pile_name());
        UNIT_ASSERT_VALUES_EQUAL(row.pile_name(), "pile_2");
        UNIT_ASSERT_VALUES_EQUAL(row.groups().required(), 0);
        UNIT_ASSERT_VALUES_EQUAL(row.storage_pools(0).id().pool_id(), 2);
        query.SetPileName("pile_1");
        const auto otherPile = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(otherPile.GetStatus() == Ydb::StatusIds::SUCCESS, otherPile.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(otherPile.GetDatabases().databases_size(), 0);
        query.SetPileName("pile_2");
        UNIT_ASSERT_VALUES_EQUAL(QueryStorageDiagnostics(env, query).GetDatabases().databases_size(), 1);
        query.ClearPileName();
        query.SetNotSynced(true);
        UNIT_ASSERT_VALUES_EQUAL(QueryStorageDiagnostics(env, query).GetDatabases().databases_size(), 0);
        query.SetNotSynced(false);

        query.ClearStoragePoolId();
        query.SetFilterPools(true);
        query.AddPoolNames(ReadStoragePool(env, 1).GetName());
        query.AddPoolNames(ReadStoragePool(env, 2).GetName());
        query.SetStorageMode(NKikimrBridge::MIXED);
        const auto selectedDatabase = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(selectedDatabase.GetStatus() == Ydb::StatusIds::SUCCESS, selectedDatabase.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(selectedDatabase.GetDatabases().databases_size(), 2);
        for (const auto& database : selectedDatabase.GetDatabases().databases()) {
            UNIT_ASSERT(database.storage_mode() == NKikimrBridge::MIXED);
            UNIT_ASSERT_VALUES_EQUAL(database.storage_pools_size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(database.groups().required(), logical.size());
        }
        query.Clear();

        auto duplicate = ReadStoragePool(env, 2);
        duplicate.SetName(ReadStoragePool(env, 1).GetName());
        NKikimrBlobStorage::TConfigRequest rename;
        rename.AddCommand()->MutableDefineStoragePool()->CopyFrom(duplicate);
        UNIT_ASSERT(env.Invoke(rename).GetSuccess());
        query.ClearStoragePoolId();
        query.SetView(NKikimrBridge::TStorageQuery::GROUPS);
        query.SetStoragePool(duplicate.GetName());
        UNIT_ASSERT(QueryStorageDiagnostics(env, query).GetStatus() == Ydb::StatusIds::PRECONDITION_FAILED);
        query.ClearStoragePool();
        query.MutableStoragePoolId()->set_box_id(1);
        query.MutableStoragePoolId()->set_pool_id(2);
        const auto qualified = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(qualified.GetStatus() == Ydb::StatusIds::SUCCESS, qualified.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(qualified.GetGroups().groups_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(qualified.GetGroups().groups(0).storage_pool().name(), duplicate.GetName());
        UNIT_ASSERT_VALUES_EQUAL(qualified.GetGroups().groups(0).storage_pool().id().pool_id(), 2);
        UNIT_ASSERT(qualified.GetGroups().groups(0).has_pile_name());
        UNIT_ASSERT_VALUES_EQUAL(qualified.GetGroups().groups(0).pile_name(), "pile_2");
        UNIT_ASSERT(qualified.GetGroups().groups(0).sync_status() == NKikimrBridge::NOT_REQUIRED);
        query.MutableStoragePoolId()->set_pool_id(0);
        UNIT_ASSERT(QueryStorageDiagnostics(env, query).GetStatus() == Ydb::StatusIds::BAD_REQUEST);
        query.MutableStoragePoolId()->set_pool_id(10000);
        UNIT_ASSERT(QueryStorageDiagnostics(env, query).GetStatus() == Ydb::StatusIds::NOT_FOUND);

        NKikimrBlobStorage::TConfigRequest setOwner;
        for (ui64 id : {1, 2}) {
            auto pool = ReadStoragePool(env, id);
            pool.MutableScopeId()->SetX1(1000);
            pool.MutableScopeId()->SetX2(5);
            setOwner.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        }
        const auto scoped = env.Invoke(setOwner);
        UNIT_ASSERT_C(scoped.GetSuccess(), scoped.GetErrorDescription());
        query.Clear();
        query.SetView(NKikimrBridge::TStorageQuery::DATABASES);
        const auto mixed = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(mixed.GetStatus() == Ydb::StatusIds::SUCCESS, mixed.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(mixed.GetDatabases().databases_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(mixed.OwnersSize(), 2);
        for (const auto& annotation : mixed.GetOwners()) {
            UNIT_ASSERT_VALUES_EQUAL(annotation.GetOwnerSchemeShardId(), 1000);
            UNIT_ASSERT_VALUES_EQUAL(annotation.GetOwnerPathId(), 5);
            const auto& ownerRow = mixed.GetDatabases().databases(annotation.GetRowIndex());
            UNIT_ASSERT(ownerRow.storage_mode() == NKikimrBridge::MIXED);
            UNIT_ASSERT_VALUES_EQUAL(ownerRow.storage_pools_size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(ownerRow.groups().required(), logical.size());
        }
        query.SetView(NKikimrBridge::TStorageQuery::PILES);
        query.ClearStorageMode();
        const auto mixedOverview = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(mixedOverview.GetStatus() == Ydb::StatusIds::SUCCESS, mixedOverview.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(mixedOverview.GetPiles().unattributed_storage_pools(), 0);
        for (const auto& row : mixedOverview.GetPiles().piles()) {
            UNIT_ASSERT_VALUES_EQUAL(row.databases().total(), 1); // two groups and two pools, one storage owner
            UNIT_ASSERT_VALUES_EQUAL(row.databases().synced(), 1);
            UNIT_ASSERT_VALUES_EQUAL(row.databases().not_required(), 0);
        }

        auto ordinaryPool = ReadStoragePool(env, 2);
        ordinaryPool.MutableScopeId()->SetX2(6);
        NKikimrBlobStorage::TConfigRequest separateOwner;
        separateOwner.AddCommand()->MutableDefineStoragePool()->CopyFrom(ordinaryPool);
        const auto separated = env.Invoke(separateOwner);
        UNIT_ASSERT_C(separated.GetSuccess(), separated.GetErrorDescription());
        const auto separateOverview = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(separateOverview.GetStatus() == Ydb::StatusIds::SUCCESS, separateOverview.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(separateOverview.GetPiles().unattributed_storage_pools(), 0);
        for (const auto& row : separateOverview.GetPiles().piles()) {
            UNIT_ASSERT(row.has_pile_name());
            UNIT_ASSERT_VALUES_EQUAL(row.databases().total(), row.pile_name() == "pile_2" ? 2 : 1);
            UNIT_ASSERT_VALUES_EQUAL(row.databases().synced(), 1);
            UNIT_ASSERT_VALUES_EQUAL(row.databases().not_required(), row.pile_name() == "pile_2" ? 1 : 0);
        }

        auto emptyPool = ReadStoragePool(env, 2);
        emptyPool.SetStoragePoolId(3);
        emptyPool.SetName("empty-ordinary-pool");
        emptyPool.SetNumGroups(0);
        emptyPool.ClearItemConfigGeneration();
        emptyPool.MutableScopeId()->SetX2(7);
        NKikimrBlobStorage::TConfigRequest createEmpty;
        createEmpty.AddCommand()->MutableDefineStoragePool()->CopyFrom(emptyPool);
        const auto createdEmpty = env.Invoke(createEmpty);
        UNIT_ASSERT_C(createdEmpty.GetSuccess(), createdEmpty.GetErrorDescription());
        const auto withUnplaced = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(withUnplaced.GetStatus() == Ydb::StatusIds::SUCCESS, withUnplaced.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(withUnplaced.GetPiles().piles_size(), 3);
        bool foundUnplaced = false;
        for (const auto& row : withUnplaced.GetPiles().piles()) {
            if (!row.has_pile_name()) {
                foundUnplaced = true;
                UNIT_ASSERT_VALUES_EQUAL(row.databases().total(), 1);
                UNIT_ASSERT_VALUES_EQUAL(row.databases().not_required(), 1);
            }
        }
        UNIT_ASSERT(foundUnplaced);

        const auto staticConfig = QueryStorageConfig(env);
        const NKikimrBlobStorage::TGroupInfo* staticLogical = nullptr;
        for (const auto& group : staticConfig.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            if (group.HasBridgeGroupState()) {
                staticLogical = &group;
                break;
            }
        }
        UNIT_ASSERT(staticLogical);
        UNIT_ASSERT_VALUES_EQUAL(staticLogical->GetGroupID(), 0);
        query.Clear();
        query.SetView(NKikimrBridge::TStorageQuery::GROUPS);
        const auto allGroups = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(allGroups.GetStatus() == Ydb::StatusIds::SUCCESS, allGroups.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(allGroups.GetGroups().groups_size(), 2 * logical.size() + 3);
        size_t withoutPool = 0;
        for (const auto& row : allGroups.GetGroups().groups()) {
            if (!row.has_storage_pool()) {
                ++withoutPool;
                UNIT_ASSERT_VALUES_EQUAL(row.group_id(), 0);
                UNIT_ASSERT(row.has_pile_name());
                UNIT_ASSERT(row.sync_status() == NKikimrBridge::SYNCED);
                const auto* part = NBridge::GetGroupPile(staticLogical->GetBridgeGroupState(),
                    TBridgePileId::FromPileIndex(row.pile_name() == "pile_1" ? 0 : 1));
                UNIT_ASSERT(part);
                UNIT_ASSERT_VALUES_EQUAL(row.physical_group_id(), part->GetGroupId());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(withoutPool, 2);
        query.SetPileName("pile_2");
        const auto onePile = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(onePile.GetStatus() == Ydb::StatusIds::SUCCESS, onePile.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(onePile.GetGroups().groups_size(), logical.size() + 2);
        for (const auto& row : onePile.GetGroups().groups()) {
            UNIT_ASSERT_VALUES_EQUAL(row.pile_name(), "pile_2");
        }
        query.ClearPileName();
        query.AddStatuses(NKikimrBridge::NOT_REQUIRED);
        const auto notRequired = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(notRequired.GetStatus() == Ydb::StatusIds::SUCCESS, notRequired.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(notRequired.GetGroups().groups_size(), 1);
        UNIT_ASSERT(notRequired.GetGroups().groups(0).has_storage_pool());

        query.ClearStatuses();
        query.MutableStoragePoolId()->set_box_id(1);
        query.MutableStoragePoolId()->set_pool_id(1);
        const auto scopedGroups = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(scopedGroups.GetStatus() == Ydb::StatusIds::SUCCESS, scopedGroups.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(scopedGroups.GetGroups().groups_size(), 2 * logical.size());
        for (const auto& row : scopedGroups.GetGroups().groups()) {
            UNIT_ASSERT(row.has_storage_pool());
        }
        query.ClearStoragePoolId();
        query.SetFilterPools(true);
        query.AddPoolNames(emptyPool.GetName());
        const auto emptyDatabaseGroups = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT_C(emptyDatabaseGroups.GetStatus() == Ydb::StatusIds::SUCCESS, emptyDatabaseGroups.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(emptyDatabaseGroups.GetGroups().groups_size(), 0);
        query.SetFilterPools(false);
        query.ClearPoolNames();
        query.SetStoragePool("");
        const auto emptyScope = QueryStorageDiagnostics(env, query);
        UNIT_ASSERT(emptyScope.GetStatus() == Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT(!emptyScope.HasGroups());
    }

    // ordinary groups residing in single piles become inline piles, get their parts in the other pile, which are synced
    // while the groups are written to, and then the groups don't rely on their original parts anymore
    Y_UNIT_TEST(SyncersAreLimited) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        CreateOrdinaryPool(env, 2, 4, 1);
        const auto groups = GetLogicalPoolGroups(env, 2);
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 4);
        SetBridgeSyncMaxSyncers(env, 2);

        TSyncerTracker tracker;
        tracker.Hold = true;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            return tracker.OnEvent(nodeId, ev);
        };
        MakeLocatedPoolBridged(env, 2, groups);
        WaitUntil(env, [&] { return tracker.GetNumRunning() >= 2; }, "syncers running");
        env.Sim(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetNumRunning(), 2);
        tracker.RunUntil(env, 2, "groups fully bridged", [&] {
            return std::ranges::all_of(groups, [&](ui32 groupId) {
                return IsFullyBridged(GetGroupInfo(env, groupId));
            });
        });
        env.Runtime->FilterFunction = {};
    }

    // the pool is being made bridged in synchronized pile 1, and its syncers wait for the only slot; when pile 2 gets back,
    // the slot goes to it
    Y_UNIT_TEST(RejoiningPileGoesAheadOfWaitingSyncers) {
        TSyncerTracker tracker;
        TEnvironmentSetup env(MakePileSettings(3));
        env.CreatePool();
        CreateOrdinaryPool(env, 2, 4, 0);
        const auto groups = GetLogicalPoolGroups(env, 2);
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 4);
        SwitchBridgePileState(env, 2, NKikimrBridge::TClusterState::DISCONNECTED);
        WaitForPileState(env, 2, NKikimrBridge::TClusterState::DISCONNECTED);
        SetBridgeSyncMaxSyncers(env, 1);

        tracker.Hold = true;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            return tracker.OnEvent(nodeId, ev);
        };
        MakeLocatedPoolBridged(env, 2, groups);
        WaitUntil(env, [&] { return tracker.GetNumRunning() >= 1; }, "syncer running");
        env.Sim(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetNumRunning(), 1);
        constexpr ui32 firstNodeOfPile2 = 19;
        UNIT_ASSERT_LT(tracker.Started.back().first, firstNodeOfPile2);

        SwitchBridgePileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
        env.Sim(TDuration::Seconds(10)); // parts of pile 2 are reset by now, and the static group is synced
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetNumRunning(), 1);

        // the part of the group next in the queue may be the one of pile 2 anyway, so the check is that no syncer of pile 1
        // starts while any part of pile 2 is left
        const size_t numStarted = tracker.Started.size();
        tracker.RunUntil(env, 1, "syncer of pile 1 started", [&] {
            return std::any_of(tracker.Started.begin() + numStarted, tracker.Started.end(), [&](const auto& started) {
                return started.first < firstNodeOfPile2;
            });
        });
        for (ui32 groupId : groups) {
            const auto info = GetGroupInfo(env, groupId);
            const auto *pile = NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(2));
            UNIT_ASSERT_C(pile && pile->GetStage() == NKikimrBridge::TGroupState::SYNCED, info.ShortDebugString());
        }
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(MakeBridgedInTwoPileCluster) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        CreateOrdinaryPool(env, 2, 2, 1);
        TDataChecker checker(env);
        const auto groups = GetLogicalPoolGroups(env, 2);
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        for (ui32 groupId : groups) {
            UNIT_ASSERT(!GetGroupInfo(env, groupId).HasBridgeGroupState());
            checker.Write(groupId);
        }

        MakeLocatedPoolBridged(env, 2, groups);
        for (ui32 groupId : groups) {
            const auto info = GetGroupInfo(env, groupId);
            UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeProxyGroupId(), groupId, info.ShortDebugString());
            const auto& state = info.GetBridgeGroupState();
            UNIT_ASSERT_C(TBridgePileId::FromProto(&state, &NKikimrBridge::TGroupState::GetAuthorityPileId) ==
                TBridgePileId::FromPileIndex(1), info.ShortDebugString());
            UNIT_ASSERT_C(NBridge::GetGroupPile(state, TBridgePileId::FromPileIndex(0)), info.ShortDebugString());
        }

        for (ui32 i = 0; i < 3; ++i) {
            for (ui32 groupId : groups) {
                checker.Write(groupId, FirstNodeOfPile[i % 2]);
            }
            checker.CheckAll();
            env.Sim(TDuration::Seconds(1));
        }
        WaitUntil(env, [&] {
            return std::ranges::all_of(groups, [&](ui32 groupId) {
                return IsFullyBridged(GetGroupInfo(env, groupId));
            });
        }, "groups fully bridged");

        for (ui32 groupId : groups) {
            checker.Write(groupId);
        }
        checker.CheckAll();
        for (ui32 pileIndex : {0, 1}) {
            CheckPileData(env, checker, pileIndex, FirstNodeOfPile[pileIndex]);
        }
        for (ui32 nodeId : {2u, 11u}) {
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checker.CheckAll(nodeId);
        }
    }

    // the VDisks of a group made bridged keep running as its inline pile; their deep scrubbing checks this part from
    // then on, fenced with its generation, instead of checking every part of the group through its bridge proxy
    Y_UNIT_TEST(DeepScrubbingAfterMakingBridged) {
        TEnvironmentSetup env(MakePileSettings(2, true));
        env.CreatePool();
        CreateOrdinaryPool(env, 2, 1, 1);
        const auto groups = GetLogicalPoolGroups(env, 2);
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const ui32 groupId = groups.front();
        TDataChecker checker(env);
        for (ui32 i = 0; i < 10; ++i) {
            checker.Write(groupId);
        }
        std::vector<TActorId> vdiskActorIds;
        const auto info = env.GetGroupInfo(groupId);
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            vdiskActorIds.push_back(info->GetActorId(i));
            env.CompactVDisk(info->GetActorId(i)); // the scrub checks the blobs it finds in SSTs
        }

        env.SetScrubPeriodicity(TDuration::Seconds(1));

        MakeLocatedPoolBridged(env, 2, groups);
        UNIT_ASSERT_VALUES_EQUAL(GetGroupInfo(env, groupId).GetBridgeProxyGroupId(), groupId);
        WaitForScrubCycles(env, vdiskActorIds); // the cycles that have started before the change finish

        // checks of the blobs written to the group: the bridge proxy fences the checks it sends to the parts, so an
        // unfenced one can come only from the scrub of the inline pile itself
        const ui64 tabletId = std::get<1>(checker.GetWritten().front()).TabletID();
        ui32 numFenced = 0;
        ui32 numUnfenced = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvCheckIntegrity) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvCheckIntegrity>();
                if (msg->Id.TabletID() == tabletId) {
                    ++(msg->ForceGroupGeneration ? numFenced : numUnfenced);
                }
            }
            return true;
        };
        WaitForScrubCycles(env, vdiskActorIds);
        env.Runtime->FilterFunction = {};

        UNIT_ASSERT_VALUES_EQUAL(numUnfenced, 0);
        UNIT_ASSERT_C(numFenced, "no blob checked by deep scrubbing");
        checker.CheckAll();
    }

    // the group being made bridged one relies on its original part solely: when the pile of the part goes out, the
    // group is not written to (the new part already getting writes would take them alone otherwise), and the part keeps
    // its synced state when the pile gets back, so that the new part gets synced from it afterwards
    Y_UNIT_TEST(OriginalPileOutWhileMakingBridged) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        CreateOrdinaryPool(env, 2, 1, 1);
        const auto groups = GetLogicalPoolGroups(env, 2);
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const ui32 groupId = groups.front();
        const ui32 bridgedGroupId = GetLogicalPoolGroups(env, 1).front();
        TDataChecker checker(env);
        checker.Write(groupId);
        checker.Write(bridgedGroupId);

        // the new part is synced up to the stage when it gets every write, and no further
        NKikimrBlobStorage::TGroupInfo latestInfo; // as distributed by BSC
        bool hold = true;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> held;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerNodeServiceSetUpdate) {
                const auto& record = ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
                for (const auto& group : record.GetServiceSet().GetGroups()) {
                    if (group.GetGroupID() == groupId &&
                            latestInfo.GetGroupGeneration() <= group.GetGroupGeneration()) {
                        latestInfo.CopyFrom(group);
                    }
                }
            }
            if (hold && ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType) {
                const auto *msg = ev->Get<NStorage::TEvNodeWardenNotifySyncerFinished>();
                if (msg->BridgeProxyGroupId.GetRawId() == groupId) {
                    const auto *pile = NBridge::FindGroupPileByGroupId(latestInfo.GetBridgeGroupState(),
                        msg->TargetGroupId);
                    if (pile && pile->GetStage() == NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP_DATA) {
                        held.emplace_back(nodeId, std::move(ev));
                        return false;
                    }
                }
            }
            return true;
        };

        MakeLocatedPoolBridged(env, 2, groups);
        WaitUntil(env, [&] { return !held.empty(); }, "new part synced up to data stage", TDuration::Seconds(1), 120);
        checker.Write(groupId);
        checker.CheckAll();

        // the pile of the original part goes out
        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        WaitForLocalPileState(env, 1, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        const TString data = "data written while the original part is out";
        const TLogoBlobID id(100501, 1, 1, 0, data.size(), 0);
        UNIT_ASSERT_UNEQUAL(TryPut(env, groupId, 1, id, data), NKikimrProto::OK);
        checker.Write(bridgedGroupId); // the bridged group keeps working

        // the pile gets back, the part it has is still synced
        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        {
            const auto info = GetGroupInfo(env, groupId);
            const auto& state = info.GetBridgeGroupState();
            const auto *pile = NBridge::GetGroupPile(state, TBridgePileId::FromPileIndex(1));
            UNIT_ASSERT_C(pile && pile->GetStage() == NKikimrBridge::TGroupState::SYNCED, info.ShortDebugString());
            UNIT_ASSERT_C(TBridgePileId::FromProto(&state, &NKikimrBridge::TGroupState::GetAuthorityPileId) ==
                TBridgePileId::FromPileIndex(1), info.ShortDebugString());
        }
        checker.Write(groupId);
        checker.CheckAll();

        // the new part gets synced
        hold = false;
        for (auto& [nodeId, ev] : held) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        WaitUntil(env, [&] { return IsFullyBridged(GetGroupInfo(env, groupId)); }, "group fully bridged");
        checker.Write(groupId);
        checker.CheckAll();
        for (ui32 pileIndex : {0, 1}) {
            CheckPileData(env, checker, pileIndex, FirstNodeOfPile[pileIndex]);
        }
    }

    // bridged groups keep their ids and their parts in the primary pile, other parts are deleted while the groups are
    // written to; new groups are ordinary ones; then the pool is made bridged again
    Y_UNIT_TEST(MakeOrdinaryAndBack) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool(2);
        env.Sim(TDuration::Seconds(30));
        const auto groups = GetLogicalPoolGroups(env, 1);
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        TDataChecker checker(env);
        std::vector<ui32> droppedParts;
        for (ui32 groupId : groups) {
            checker.Write(groupId);
            droppedParts.push_back(GetPileGroups(env, groupId).at(1));
        }

        auto response = ChangeStoragePoolBridgeMode(env, 1, false);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        UNIT_ASSERT(!ReadStoragePool(env, 1).GetBridgeMode());
        for (ui32 groupId : groups) {
            const auto info = GetGroupInfo(env, groupId);
            const auto& state = info.GetBridgeGroupState();
            UNIT_ASSERT_C(TBridgePileId::FromProto(&state, &NKikimrBridge::TGroupState::GetAuthorityPileId) ==
                TBridgePileId::FromPileIndex(0), info.ShortDebugString());
        }

        // writes keep succeeding while the parts go away
        for (ui32 i = 0; i < 15; ++i) {
            for (ui32 groupId : groups) {
                checker.Write(groupId, FirstNodeOfPile[i % 2]);
            }
            env.Sim(TDuration::Seconds(5));
        }
        WaitUntil(env, [&] {
            return std::ranges::all_of(groups, [&](ui32 groupId) {
                return HasOnlyPart(env, GetGroupInfo(env, groupId), 0);
            });
        }, "groups with the only part");
        const auto baseConfig = env.FetchBaseConfig();
        for (ui32 groupId : droppedParts) {
            UNIT_ASSERT_C(!HasGroup(baseConfig, groupId), "part not deleted GroupId# " << groupId);
        }
        checker.CheckAll();

        // new group of the pool is ordinary one
        SetNumGroups(env, 1, 3);
        env.Sim(TDuration::Seconds(30));
        const auto allGroups = GetLogicalPoolGroups(env, 1);
        UNIT_ASSERT_VALUES_EQUAL(allGroups.size(), 3);
        for (ui32 groupId : allGroups) {
            if (std::ranges::find(groups, groupId) == groups.end()) {
                UNIT_ASSERT(!GetGroupInfo(env, groupId).HasBridgeGroupState());
                checker.Write(groupId);
            }
        }
        for (ui32 nodeId : {2u, 11u}) {
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checker.CheckAll(nodeId);
        }

        // the pool becomes bridged again: groups get their parts in the other pile anew
        response = ChangeStoragePoolBridgeMode(env, 1, true);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        for (ui32 groupId : allGroups) {
            checker.Write(groupId);
        }
        WaitUntil(env, [&] {
            return std::ranges::all_of(allGroups, [&](ui32 groupId) {
                return IsFullyBridged(GetGroupInfo(env, groupId));
            });
        }, "groups fully bridged");
        checker.CheckAll();
        for (ui32 pileIndex : {0, 1}) {
            CheckPileData(env, checker, pileIndex, FirstNodeOfPile[pileIndex]);
        }
    }

    // the part dropped by the pool made ordinary stops getting requests for the whole delay before it is deleted, even
    // when the pool has been made bridged and ordinary again in between
    Y_UNIT_TEST(RepeatedModeChanges) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = GetLogicalPoolGroups(env, 1).front();
        const ui32 partId = GetPileGroups(env, groupId).at(1);
        TDataChecker checker(env);
        checker.Write(groupId);

        auto change = [&](bool bridgeMode) {
            const auto response = ChangeStoragePoolBridgeMode(env, 1, bridgeMode);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        };
        auto getPartStage = [&] {
            const auto info = GetGroupInfo(env, groupId);
            const auto *pile = NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(1));
            UNIT_ASSERT_C(pile, info.ShortDebugString());
            return pile->GetStage();
        };

        // the part stops getting requests, and the pool is made bridged again before the part is deleted
        change(false);
        WaitUntil(env, [&] { return getPartStage() == NKikimrBridge::TGroupState::BLOCKS; }, "part stopped",
            TDuration::Seconds(1), 30);
        change(true);
        WaitUntil(env, [&] { return IsFullyBridged(GetGroupInfo(env, groupId)); }, "group fully bridged");
        UNIT_ASSERT_VALUES_EQUAL(GetPileGroups(env, groupId).at(1), partId); // the same part has been synced
        env.Sim(TDuration::Seconds(40)); // longer than the delay since the part stopped for the first time
        checker.Write(groupId);

        // the part is dropped again: it is still there before the delay passes since it has stopped this time
        change(false);
        env.Sim(TDuration::Seconds(15));
        UNIT_ASSERT_C(HasGroup(env.FetchBaseConfig(), partId), "part deleted before the delay passed");
        checker.Write(groupId);
        checker.CheckAll();
        WaitUntil(env, [&] { return !HasGroup(env.FetchBaseConfig(), partId); }, "part deleted");
        UNIT_ASSERT(HasOnlyPart(env, GetGroupInfo(env, groupId), 0));
        checker.Write(groupId);
        checker.CheckAll();
    }

    // the group with the only part in the pile can't lose it along with the pile, and the part keeps its data and state
    // when the pile gets back after being out
    Y_UNIT_TEST(OnlyPartPileOut) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = GetLogicalPoolGroups(env, 1).front();
        TDataChecker checker(env);
        checker.Write(groupId);

        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableChangeStoragePoolBridgeMode();
        cmd->SetBoxId(1);
        cmd->SetStoragePoolId(1);
        cmd->SetBridgeMode(false);
        cmd->SetRetainPileName("pile_3");
        cmd->SetItemConfigGeneration(ReadStoragePool(env, 1).GetItemConfigGeneration());
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(!response.GetSuccess(), "unknown pile must be rejected");
        cmd->SetRetainPileName("pile_2");
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        WaitUntil(env, [&] { return HasOnlyPart(env, GetGroupInfo(env, groupId), 1); }, "group with the only part");
        checker.Write(groupId);
        checker.CheckAll();

        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        WaitForLocalPileState(env, 1, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        const TString data = "data written while the only part is out";
        UNIT_ASSERT_UNEQUAL(TryPut(env, groupId, 1, TLogoBlobID(100501, 1, 1, 0, data.size(), 0), data),
            NKikimrProto::OK);
        ExpectError(RemovePile(env, "pile_2"), "relies on its part in the pile solely");

        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        UNIT_ASSERT(HasOnlyPart(env, GetGroupInfo(env, groupId), 1));
        checker.Write(groupId);
        checker.CheckAll();
    }

    // the pool kept ordinary at conversion is made bridged later: its groups become inline piles keeping their ids and
    // data, and new groups are bridged ones; the other ordinary pool stays as it is
    Y_UNIT_TEST(MakeBridgedInSinglePileCluster) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker, 1, true);
        CreateAnotherPool(env, 2);
        env.Sim(TDuration::Seconds(30));
        const auto baseConfig = env.FetchBaseConfig(); // groups are referenced by pointers
        const auto poolGroups = GetPoolGroups(baseConfig, 2);
        UNIT_ASSERT_VALUES_EQUAL(poolGroups.size(), 1);
        const ui32 otherGroupId = poolGroups.front()->GetGroupId();
        for (ui32 id : {groupId, otherGroupId}) {
            checker.Write(id);
        }

        ui32 numInnerPuts = 0; // puts issued by the bridge proxy to the DS proxy of the inline pile
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                if (auto *msg = ev->Get<TEvBlobStorage::TEvPut>(); msg->ForceGroupGeneration &&
                        msg->Id.TabletID() == 100500) {
                    ++numInnerPuts;
                }
            }
            return true;
        };

        auto getGeneration = [&](ui32 id) {
            return FindGroup(env.FetchBaseConfig(), id).GetGroupGeneration();
        };
        const ui32 generation = getGeneration(groupId);
        const ui32 otherGeneration = getGeneration(otherGroupId);

        // the generation of the pool fences concurrent changes
        NKikimrBlobStorage::TConfigRequest request;
        auto *cmd = request.AddCommand()->MutableChangeStoragePoolBridgeMode();
        cmd->SetBoxId(1);
        cmd->SetStoragePoolId(1);
        cmd->SetBridgeMode(true);
        cmd->SetItemConfigGeneration(ReadStoragePool(env, 1).GetItemConfigGeneration() + 1);
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(!response.GetSuccess(), "stale generation must be rejected");

        response = ChangeStoragePoolBridgeMode(env, 1, true);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT(ReadStoragePool(env, 1).GetBridgeMode());
        UNIT_ASSERT(!ReadStoragePool(env, 2).GetBridgeMode());

        const auto info = GetGroupInfo(env, groupId);
        UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeProxyGroupId(), groupId, info.ShortDebugString());
        UNIT_ASSERT_C(TBridgePileId::FromProto(&info, &NKikimrBlobStorage::TGroupInfo::GetBridgePileId) ==
            TBridgePileId::FromPileIndex(0), info.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(info.GetGroupGeneration(), generation + 1);
        UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeGroupState().PileSize(), 1, info.ShortDebugString());
        UNIT_ASSERT_EQUAL(info.GetBridgeGroupState().GetPile(0).GetStage(), NKikimrBridge::TGroupState::SYNCED);
        const auto otherInfo = GetGroupInfo(env, otherGroupId);
        UNIT_ASSERT_C(!otherInfo.HasBridgeGroupState(), otherInfo.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(otherInfo.GetGroupGeneration(), otherGeneration);

        // the same mode is accepted without changes
        response = ChangeStoragePoolBridgeMode(env, 1, true);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        UNIT_ASSERT_VALUES_EQUAL(getGeneration(groupId), generation + 1);

        // the pool is redefined with its new mode or without one, but its mode can't be changed that way
        auto pool = ReadStoragePool(env, 1);
        pool.SetBridgeMode(false);
        request.Clear();
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        response = env.Invoke(request);
        UNIT_ASSERT_C(!response.GetSuccess(), "mode change by redefinition must be rejected");
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "can't change storage pool BridgeMode");
        pool.ClearBridgeMode();
        pool.SetNumGroups(2);
        request.Clear();
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.Sim(TDuration::Seconds(30));
        std::optional<ui32> newGroupId;
        {
            const auto current = env.FetchBaseConfig();
            const auto poolGroups = GetPoolGroups(current, 1);
            UNIT_ASSERT_VALUES_EQUAL(poolGroups.size(), 2); // new group is born inline pile, without separate parts
            for (const auto *group : poolGroups) {
                if (group->GetGroupId() != groupId) {
                    UNIT_ASSERT(!newGroupId);
                    newGroupId = group->GetGroupId();
                }
            }
        }
        UNIT_ASSERT(newGroupId);
        CheckInlinePileGroup(env, *newGroupId);
        checker.Write(*newGroupId);

        auto checkAll = [&](ui32 nodeId) {
            const ui32 prev = numInnerPuts;
            checker.Write(groupId, nodeId);
            UNIT_ASSERT_VALUES_EQUAL(numInnerPuts, prev + 1);
            checker.Write(otherGroupId, nodeId);
            UNIT_ASSERT_VALUES_EQUAL(numInnerPuts, prev + 1);
            checker.CheckAll(nodeId);
        };
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            checkAll(nodeId);
        }

        // the converted group keeps working through disk reassignment and restarts
        ReassignOneDisk(env, groupId);
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT_VALUES_EQUAL(GetGroupInfo(env, groupId).GetBridgeGroupState().GetPile(0).GetGroupGeneration(),
            getGeneration(groupId));
        checkAll(1);
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            Cerr << "*** restarting node " << nodeId << Endl;
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checkAll(nodeId);
        }
    }

    // BSC validates the proposed pile override while no group conflicts with it, but such a group gets created before
    // distconf commits the config. BSC doesn't adopt the override then: the group stays manageable under the settings it
    // was created with, and the override takes effect when the conflicting group is gone.
    Y_UNIT_TEST(StalePileOverrideIsNotApplied) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));

        auto createPool = [&] {
            auto pool = ReadStoragePool(env, 1);
            pool.SetStoragePoolId(2);
            pool.SetName("pool_2");
            pool.SetKind("pool_2");
            pool.ClearItemConfigGeneration();
            pool.SetBridgeMode(true);
            pool.SetNumGroups(1);
            NKikimrBlobStorage::TConfigRequest request;
            request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        };
        // the part of the group in the pile: its group id and erasure
        auto getPart = [&](ui32 groupId, ui32 pileIndex) {
            const auto info = GetGroupInfo(env, groupId);
            const auto *part = NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(pileIndex));
            UNIT_ASSERT_C(part, info.ShortDebugString());
            return std::make_pair(part->GetGroupId(), GetGroupInfo(env, part->GetGroupId()).GetErasureSpecies());
        };
        auto checkPoolErasure = [&](size_t numGroups, TBlobStorageGroupType::EErasureSpecies secondPileErasure) {
            const auto groups = GetLogicalPoolGroups(env, 2);
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), numGroups);
            for (ui32 groupId : groups) {
                UNIT_ASSERT_VALUES_EQUAL(getPart(groupId, 0).second, TBlobStorageGroupType::Erasure4Plus2Block);
                UNIT_ASSERT_VALUES_EQUAL(getPart(groupId, 1).second, secondPileErasure);
            }
        };

        // propose the override and hold the answer of BSC, which has just found nothing against it
        std::unique_ptr<IEventHandle> held;
        ui32 heldNodeId = 0;
        bool hold = true;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (hold && !held && ev->Type == TEvBlobStorage::EvControllerDistconfResponse) {
                held = std::move(ev);
                heldNodeId = nodeId;
                return false;
            }
            return true;
        };
        const TString yaml = MakeNextMainConfigYaml(env, [](YAML::Node& config) { AddPoolPileOverride(config, "none", 0); });
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        {
            auto ev = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
            auto *cmd = ev->Record.MutableReplaceStorageConfig();
            cmd->SetYAML(yaml);
            cmd->SetSkipConsoleValidation(true);
            env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), edge, ev.release()), 1);
        }
        WaitUntil(env, [&] { return static_cast<bool>(held); }, "answer of BSC", TDuration::MilliSeconds(10), 3000);

        // the pool of the kind gets its group under the settings still in effect
        createPool();

        // distconf commits the override BSC has agreed to
        hold = false;
        env.Runtime->Send(held.release(), heldNodeId);
        {
            auto res = env.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(edge, false);
            const auto& record = res->Get()->Record;
            UNIT_ASSERT_EQUAL_C(record.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
                record.GetErrorReason());
        }
        env.Runtime->FilterFunction = {};
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(QueryStorageConfig(env).GetBlobStorageConfig().StoragePoolPileOverridesSize(), 1);

        // BSC keeps managing the group: its disks can be replaced, and the pool can be redefined and grown under the
        // same settings, including after the restart of BSC
        checkPoolErasure(1, TBlobStorageGroupType::Erasure4Plus2Block);
        const ui32 groupId = GetLogicalPoolGroups(env, 2).front();
        TDataChecker checker(env);
        checker.Write(groupId);
        ReassignOneDisk(env, getPart(groupId, 1).first);
        env.Sim(TDuration::Seconds(30));
        {
            NKikimrBlobStorage::TConfigRequest request;
            request.AddCommand()->MutableDefineStoragePool()->CopyFrom(ReadStoragePool(env, 2));
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }
        env.RebootTablet(env.TabletId);
        env.Sim(TDuration::Seconds(30));
        SetNumGroups(env, 2, 2);
        env.Sim(TDuration::Seconds(30));
        checkPoolErasure(2, TBlobStorageGroupType::Erasure4Plus2Block);
        checker.CheckAll();

        // the override can't be made effective while the groups are there; it is adopted as soon as they are gone
        {
            const auto pool = ReadStoragePool(env, 2);
            NKikimrBlobStorage::TConfigRequest request;
            auto *cmd = request.AddCommand()->MutableDeleteStoragePool();
            cmd->SetBoxId(pool.GetBoxId());
            cmd->SetStoragePoolId(pool.GetStoragePoolId());
            cmd->SetItemConfigGeneration(pool.GetItemConfigGeneration());
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }
        env.Sim(TDuration::Seconds(30));
        createPool();
        env.Sim(TDuration::Seconds(30));
        checkPoolErasure(1, TBlobStorageGroupType::ErasureNone);

        // and it stays adopted
        env.RebootTablet(env.TabletId);
        env.Sim(TDuration::Seconds(30));
        SetNumGroups(env, 2, 2);
        env.Sim(TDuration::Seconds(30));
        checkPoolErasure(2, TBlobStorageGroupType::ErasureNone);
    }

    // BridgeMode of pools may be changed only in the cluster supporting both kinds of them
    Y_UNIT_TEST(RequiresNonUniformStorage) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);
        const auto response = ChangeStoragePoolBridgeMode(env, 1, false);
        UNIT_ASSERT_C(!response.GetSuccess(), "mode change must be rejected");
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "enable_non_uniform_storage");
        checker.Write(groupId);
        checker.CheckAll();
    }

}
