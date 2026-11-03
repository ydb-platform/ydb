#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/base/bridge_events.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>

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

    // two piles of 9 nodes, each pile being a datacenter of its own; non-uniform storage is enabled
    TEnvironmentSetup::TSettings MakeTwoPileSettings() {
        return {
            .NodeCount = 18,
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
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        };
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

    void SetPoolPileOverride(TEnvironmentSetup& env, const TString& erasure, ui32 numFailDomains) {
        const auto result = ReplaceMainConfig(env, [&](YAML::Node& config) {
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

    Y_UNIT_TEST(StorageDiagnosticsPileFilterDistinguishesEmptyNameFromUnplacedOrdinary) {
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
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        WaitUntil(env, [&] {
            return std::ranges::all_of(groups, [&](ui32 groupId) {
                return IsFullyBridged(GetGroupInfo(env, groupId));
            });
        }, "groups synchronized before querying diagnostics with an empty pile name");
        env.Sim(TDuration::Seconds(6));

        const auto config = QueryStorageConfig(env);
        const auto& registry = config.GetClusterStateDetails().GetPileRegistry();
        UNIT_ASSERT(registry.GetPiles(0).HasName());
        UNIT_ASSERT_VALUES_EQUAL(registry.GetPiles(0).GetName(), "");
        UNIT_ASSERT_VALUES_EQUAL(registry.GetPiles(1).GetName(), "b");
        UNIT_ASSERT(TBridgePileId::FromProto(&config.GetClusterState(), &NKikimrBridge::TClusterState::GetPrimaryPile) ==
            TBridgePileId::FromPileIndex(0));

        auto queryStorage = [&](const NKikimrBridge::TStorageQuery& query) {
            const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            auto request = std::make_unique<TEvBlobStorage::TEvControllerQueryBridgeStorage>();
            request->Record.CopyFrom(query);
            env.Runtime->SendToPipe(env.TabletId, edge, request.release(), 0,
                TTestActorSystem::GetPipeConfigWithRetries());
            auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerQueryBridgeStorageResult>(edge, false);
            UNIT_ASSERT_C(response->Get()->Record.GetUnavailableReason().empty(), response->Get()->Record.DebugString());
            return response->Get()->Record;
        };

        NKikimrBridge::TStorageQuery query;
        query.SetView(NKikimrBridge::TStorageQuery::GROUPS);
        query.SetStoragePool(ReadStoragePool(env, 1).GetName());
        const auto allCopies = queryStorage(query);
        UNIT_ASSERT_VALUES_EQUAL(allCopies.GroupsSize(), groups.size() * 2);
        UNIT_ASSERT_VALUES_EQUAL(std::ranges::count_if(allCopies.GetGroups(), [](const auto& row) {
            return row.GetPileName().empty();
        }), groups.size());

        query.SetPileName("b");
        query.SetPageSize(1);
        std::set<ui32> selectedGroups;
        do {
            const auto page = queryStorage(query);
            UNIT_ASSERT_VALUES_EQUAL(page.GroupsSize(), 1);
            const auto& row = page.GetGroups(0);
            UNIT_ASSERT_VALUES_EQUAL(row.GetPileName(), "b");
            UNIT_ASSERT(row.GetState() == NKikimrBridge::TStorageGroup::SYNCED);
            UNIT_ASSERT(selectedGroups.insert(row.GetGroupId()).second);
            UNIT_ASSERT(selectedGroups.size() <= groups.size());
            const auto info = GetGroupInfo(env, row.GetGroupId());
            const auto* part = NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(1));
            UNIT_ASSERT(part);
            UNIT_ASSERT_VALUES_EQUAL(row.GetPartGroupId(), part->GetGroupId());
            query.SetPageToken(page.GetNextPageToken());
        } while (!query.GetPageToken().empty());
        UNIT_ASSERT_VALUES_EQUAL(selectedGroups.size(), groups.size());

        query.SetStoragePool("pool_2");
        const auto unplaced = queryStorage(query);
        UNIT_ASSERT_VALUES_EQUAL(unplaced.GroupsSize(), 1);
        UNIT_ASSERT(unplaced.GetGroups(0).GetPileName().empty());
        UNIT_ASSERT(unplaced.GetGroups(0).GetState() == NKikimrBridge::TStorageGroup::NOT_REQUIRED);
        UNIT_ASSERT(unplaced.GetNextPageToken().empty());

        // Ordinary groups retaining a real copy in the empty-named primary pile still obey the filter.
        const auto ordinary = ChangeStoragePoolBridgeMode(env, 1, false);
        UNIT_ASSERT_C(ordinary.GetSuccess(), ordinary.GetErrorDescription());
        WaitUntil(env, [&] {
            return std::ranges::all_of(groups, [&](ui32 groupId) {
                return HasOnlyPart(env, GetGroupInfo(env, groupId), 0);
            });
        }, "ordinary groups retain the copy in the empty-named primary pile");
        env.Sim(TDuration::Seconds(6));
        query.SetStoragePool(ReadStoragePool(env, 1).GetName());
        const auto retainedOutsideFilter = queryStorage(query);
        UNIT_ASSERT_VALUES_EQUAL(retainedOutsideFilter.GroupsSize(), 0);
        UNIT_ASSERT(retainedOutsideFilter.GetNextPageToken().empty());
        query.ClearPileName();
        query.ClearPageSize();
        const auto retained = queryStorage(query);
        UNIT_ASSERT_VALUES_EQUAL(retained.GroupsSize(), groups.size());
        for (const auto& row : retained.GetGroups()) {
            UNIT_ASSERT(row.GetPileName().empty());
            UNIT_ASSERT(row.GetState() == NKikimrBridge::TStorageGroup::NOT_REQUIRED);
        }
    }

    Y_UNIT_TEST(StorageDiagnosticsCountLogicalGroupsAndPaginateFilteredCopies) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool(2);
        CreateOrdinaryPool(env, 2, 1, 1);
        const auto groups = GetLogicalPoolGroups(env, 1);
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        WaitUntil(env, [&] {
            return std::ranges::all_of(groups, [&](ui32 groupId) {
                return IsFullyBridged(GetGroupInfo(env, groupId));
            });
        }, "groups synchronized before querying diagnostics");
        env.Sim(TDuration::Seconds(6)); // refresh the BSC diagnostic aggregates

        auto queryStorage = [&](const NKikimrBridge::TStorageQuery& query) {
            const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            auto request = std::make_unique<TEvBlobStorage::TEvControllerQueryBridgeStorage>();
            request->Record.CopyFrom(query);
            env.Runtime->SendToPipe(env.TabletId, edge, request.release(), 0,
                TTestActorSystem::GetPipeConfigWithRetries());
            auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerQueryBridgeStorageResult>(edge, false);
            return response->Get()->Record;
        };

        NKikimrBridge::TStorageQuery query;
        query.SetView(NKikimrBridge::TStorageQuery::PILES);
        const auto piles = queryStorage(query);
        UNIT_ASSERT_C(piles.GetUnavailableReason().empty(), piles.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(piles.PilesSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(piles.GetGeneration(), QueryStorageConfig(env).GetClusterState().GetGeneration());
        UNIT_ASSERT(piles.GetCollectedAtUs());
        for (const auto& pile : piles.GetPiles()) {
            // Two logical bridge groups in each pile; their physical parts and the ordinary group are not added.
            UNIT_ASSERT_VALUES_EQUAL_C(pile.GetUserGroups().GetTotal(), groups.size(), pile.DebugString());
            UNIT_ASSERT_VALUES_EQUAL_C(pile.GetUserGroups().GetSynced(), groups.size(), pile.DebugString());
            UNIT_ASSERT(pile.GetSystemGroups().GetTotal());
        }

        query.Clear();
        query.SetView(NKikimrBridge::TStorageQuery::GROUPS);
        query.SetStoragePool(ReadStoragePool(env, 1).GetName());
        query.SetPageSize(1);
        std::set<std::pair<ui32, TString>> copies;
        std::set<TString> tokens;
        TString firstToken;
        size_t pages = 0;
        do {
            const auto page = queryStorage(query);
            UNIT_ASSERT_C(page.GetUnavailableReason().empty(), page.DebugString());
            UNIT_ASSERT_VALUES_EQUAL_C(page.GroupsSize(), 1, page.DebugString());
            UNIT_ASSERT_C(++pages <= groups.size() * 2, "pagination did not terminate");
            const auto& row = page.GetGroups(0);
            UNIT_ASSERT_VALUES_EQUAL(row.GetPoolName(), query.GetStoragePool());
            UNIT_ASSERT(std::find(groups.begin(), groups.end(), row.GetGroupId()) != groups.end());
            UNIT_ASSERT(row.GetState() == NKikimrBridge::TStorageGroup::SYNCED);
            UNIT_ASSERT(copies.emplace(row.GetGroupId(), row.GetPileName()).second);
            UNIT_ASSERT(row.GetPileName() == "pile_1" || row.GetPileName() == "pile_2");
            const auto info = GetGroupInfo(env, row.GetGroupId());
            const auto* part = NBridge::GetGroupPile(info.GetBridgeGroupState(),
                TBridgePileId::FromPileIndex(row.GetPileName() == "pile_1" ? 0 : 1));
            UNIT_ASSERT(part);
            UNIT_ASSERT_VALUES_EQUAL(row.GetPartGroupId(), part->GetGroupId());
            query.SetPageToken(page.GetNextPageToken());
            if (!query.GetPageToken().empty()) {
                UNIT_ASSERT(tokens.insert(query.GetPageToken()).second);
                if (firstToken.empty()) {
                    firstToken = query.GetPageToken();
                }
            }
        } while (!query.GetPageToken().empty());
        UNIT_ASSERT_VALUES_EQUAL(copies.size(), groups.size() * 2);
        UNIT_ASSERT_VALUES_EQUAL(pages, groups.size() * 2); // the final nonempty page has no continuation token

        query.SetPageToken(firstToken);
        query.SetPileName("pile_1");
        const auto changedFilter = queryStorage(query);
        UNIT_ASSERT_STRING_CONTAINS(changedFilter.GetUnavailableReason(), "Invalid page token");
        UNIT_ASSERT_VALUES_EQUAL(changedFilter.GroupsSize(), 0);
        query.ClearPageToken();
        query.ClearPileName();

        query.SetNotSynced(true);
        const auto synced = queryStorage(query);
        UNIT_ASSERT_C(synced.GetUnavailableReason().empty(), synced.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(synced.GroupsSize(), 0);
        UNIT_ASSERT(synced.GetNextPageToken().empty());

        query.Clear();
        query.SetView(NKikimrBridge::TStorageQuery::DATABASES);
        query.SetFilterPools(true);
        query.AddPoolNames(ReadStoragePool(env, 1).GetName());
        query.SetPileName("pile_1");
        query.SetPageSize(1);
        const auto database = queryStorage(query);
        UNIT_ASSERT_C(database.GetUnavailableReason().empty(), database.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(database.DatabasesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(database.GetDatabases(0).GetPileName(), "pile_1");
        UNIT_ASSERT_VALUES_EQUAL(database.GetDatabases(0).StoragePoolsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(database.GetDatabases(0).GetStoragePools(0), ReadStoragePool(env, 1).GetName());
        UNIT_ASSERT(database.GetNextPageToken().empty()); // the other pile is outside the selected view

        query.AddPoolNames("missing_pool");
        const auto missingPool = queryStorage(query);
        UNIT_ASSERT_STRING_CONTAINS(missingPool.GetUnavailableReason(), "must exist");
        UNIT_ASSERT_VALUES_EQUAL(missingPool.DatabasesSize(), 0);

        query.Clear();
        query.SetView(NKikimrBridge::TStorageQuery::DATABASES);
        query.SetPageSize(1);
        std::set<std::pair<TString, TString>> databaseCopies;
        do {
            const auto page = queryStorage(query);
            UNIT_ASSERT_C(page.GetUnavailableReason().empty(), page.DebugString());
            UNIT_ASSERT_VALUES_EQUAL_C(page.DatabasesSize(), 1, page.DebugString());
            const auto& databaseRow = page.GetDatabases(0);
            UNIT_ASSERT_VALUES_EQUAL(databaseRow.StoragePoolsSize(), 1);
            UNIT_ASSERT(databaseCopies.emplace(databaseRow.GetStoragePools(0), databaseRow.GetPileName()).second);
            UNIT_ASSERT(databaseCopies.size() <= 3);
            query.SetPageToken(page.GetNextPageToken());
        } while (!query.GetPageToken().empty());
        UNIT_ASSERT_VALUES_EQUAL(databaseCopies.size(), 3); // bridge copies and one pile-independent ordinary row

        query.Clear();
        query.SetView(NKikimrBridge::TStorageQuery::DATABASES);
        query.SetStoragePool("pool_2");
        const auto ordinary = queryStorage(query);
        UNIT_ASSERT_C(ordinary.GetUnavailableReason().empty(), ordinary.DebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(ordinary.DatabasesSize(), 1, ordinary.DebugString());
        const auto& row = ordinary.GetDatabases(0);
        UNIT_ASSERT_VALUES_EQUAL(row.GetStoragePools(0), "pool_2");
        UNIT_ASSERT(row.GetState() == NKikimrBridge::TStorageGroup::NOT_REQUIRED);
        UNIT_ASSERT(row.GetMode() == NKikimrBridge::TStorageDatabase::ORDINARY);
        UNIT_ASSERT_VALUES_EQUAL(row.GetGroups().GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(row.GetOwnerSchemeShardId(), 0); // the test pool has no database metadata
        UNIT_ASSERT_VALUES_EQUAL(row.GetOwnerPathId(), 0);
        query.SetNotSynced(true);
        const auto excluded = queryStorage(query);
        UNIT_ASSERT_C(excluded.GetUnavailableReason().empty(), excluded.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(excluded.DatabasesSize(), 0);
        UNIT_ASSERT(excluded.GetNextPageToken().empty());

        query.Clear();
        query.SetView(NKikimrBridge::TStorageQuery::GROUPS);
        query.SetStoragePool("pool_2");
        query.SetPileName("pile_2");
        const auto ordinaryGroups = queryStorage(query);
        UNIT_ASSERT_C(ordinaryGroups.GetUnavailableReason().empty(), ordinaryGroups.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(ordinaryGroups.GroupsSize(), 1);
        UNIT_ASSERT(ordinaryGroups.GetGroups(0).GetState() == NKikimrBridge::TStorageGroup::NOT_REQUIRED);
        UNIT_ASSERT(ordinaryGroups.GetGroups(0).GetPileName().empty()); // no per-pile bridge copy is implied

        query.SetPileName("missing_pile");
        const auto missingPile = queryStorage(query);
        UNIT_ASSERT_VALUES_EQUAL(missingPile.GetUnavailableReason(), "Pile not found");
        query.ClearPileName();
        query.ClearStoragePool();
        const auto unscoped = queryStorage(query);
        UNIT_ASSERT_STRING_CONTAINS(unscoped.GetUnavailableReason(), "require a database or storage pool");
    }

    // ordinary groups residing in single piles become inline piles, get their parts in the other pile, which are synced
    // while the groups are written to, and then the groups don't rely on their original parts anymore
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
            for (const auto *group : GetPoolGroups(current, 1)) {
                if (group->GetIsProxyGroup()) {
                    UNIT_ASSERT(!newGroupId);
                    newGroupId = group->GetGroupId();
                }
            }
        }
        UNIT_ASSERT(newGroupId);
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
