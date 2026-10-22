#include "bridge_config_yaml.h"
#include <ydb/core/blobstorage/nodewarden/distconf.h>

using namespace NBridgeTest;

namespace {
    TEnvironmentSetup::TSettings MixedSettings(bool enabled = true, ui32 numPiles = 3) {
        return {
            .NodeCount = 9 * numPiles,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .PrepareRuntime = [enabled](TTestActorSystem& runtime) {
                runtime.GetAppDataBridgeConfig().SetEnableNonUniformStorage(enabled);
            },
            .LocationGenerator = [](ui32 nodeId) {
                NActorsInterconnect::TNodeLocation location;
                const ui32 dc = (nodeId - 1) / 9 + 1;
                location.SetDataCenter(TStringBuilder() << "dc_" << dc);
                location.SetBridgePileName(TStringBuilder() << "pile_" << dc);
                location.SetRack(TStringBuilder() << "rack_" << nodeId);
                location.SetUnit(TStringBuilder() << "unit_" << nodeId);
                return TNodeLocation(location);
            },
            .SelfManagementConfig = true,
            .NumPiles = numPiles,
            .AutomaticBootstrap = true,
        };
    }

    NKikimrBlobStorage::TDefineStoragePool ReadPool(TEnvironmentSetup& env, ui64 id) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *read = request.AddCommand()->MutableReadStoragePool();
        read->SetBoxId(1);
        read->AddStoragePoolId(id);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(0).StoragePoolSize(), 1);
        return response.GetStatus(0).GetStoragePool(0);
    }

    NKikimrBlobStorage::TConfigResponse DefinePool(TEnvironmentSetup& env,
            const NKikimrBlobStorage::TDefineStoragePool& pool) {
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        return env.Invoke(request);
    }

    NKikimrBlobStorage::TDefineStoragePool NewPool(TEnvironmentSetup& env, ui64 id, bool bridgeMode) {
        auto pool = ReadPool(env, 1);
        pool.SetStoragePoolId(id);
        pool.SetName(TStringBuilder() << "pool_" << id);
        pool.SetKind(pool.GetName());
        pool.ClearItemConfigGeneration();
        pool.SetBridgeMode(bridgeMode);
        pool.SetErasureSpecies(bridgeMode ? "block-4-2" : "mirror-3-dc");
        return pool;
    }

    ui32 OrdinaryGroup(TEnvironmentSetup& env) {
        const auto config = env.FetchBaseConfig();
        const auto groups = GetPoolGroups(config, 2);
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const auto& group = *groups.front();
        const auto info = GetGroupInfo(env, group.GetGroupId());
        UNIT_ASSERT(!info.HasBridgeGroupState());
        UNIT_ASSERT(!info.HasBridgePileId());
        UNIT_ASSERT(!info.HasBridgeProxyGroupId());
        UNIT_ASSERT_VALUES_EQUAL(group.GetErasureSpecies(), "mirror-3-dc");
        std::set<ui32> dcs;
        for (const auto& slot : group.GetVSlotId()) {
            dcs.insert((slot.GetNodeId() - 1) / 9);
        }
        UNIT_ASSERT_VALUES_EQUAL(group.VSlotIdSize(), 9);
        UNIT_ASSERT_VALUES_EQUAL(dcs.size(), 3);
        return group.GetGroupId();
    }

    void WaitForOrdinaryGroupReady(TEnvironmentSetup& env, ui32 groupId) {
        WaitUntil(env, [&] {
            const auto config = env.FetchBaseConfig();
            ui32 ready = 0;
            for (const auto& slot : config.GetVSlot()) {
                if (slot.GetGroupId() == groupId && slot.GetReady()) {
                    ++ready;
                }
            }
            return ready == 9;
        }, "ordinary VDisks ready");
    }

    void TestOrdinarySingleDatacenterErasure(bool erasureNone) {
        TEnvironmentSetup env(MixedSettings(true, 2));
        env.CreatePool();
        auto pool = NewPool(env, 2, false);
        const TString erasure = erasureNone ? "none" : "block-4-2";
        pool.SetErasureSpecies(erasure);
        auto response = DefinePool(env, pool);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        TDataChecker checker(env);
        auto checkGroups = [&](ui32 expectedCount) {
            const auto config = env.FetchBaseConfig();
            const auto groups = GetPoolGroups(config, 2);
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), expectedCount);
            for (const auto* group : groups) {
                const auto info = GetGroupInfo(env, group->GetGroupId());
                UNIT_ASSERT(!info.HasBridgeGroupState());
                UNIT_ASSERT(!info.HasBridgePileId());
                UNIT_ASSERT(!info.HasBridgeProxyGroupId());
                UNIT_ASSERT_VALUES_EQUAL(group->GetErasureSpecies(), erasure);
                UNIT_ASSERT_VALUES_EQUAL(group->VSlotIdSize(), erasureNone ? 1 : 8);
                std::set<ui32> datacenters;
                for (const auto& slot : group->GetVSlotId()) {
                    datacenters.insert((slot.GetNodeId() - 1) / 9);
                }
                UNIT_ASSERT_VALUES_EQUAL(datacenters.size(), 1);
                checker.Write(group->GetGroupId());
            }
            checker.CheckAll();
        };
        checkGroups(1);
        env.RestartNode(1);
        checker.CheckAll();
        pool = ReadPool(env, 2);
        UNIT_ASSERT(pool.HasBridgeMode() && !pool.GetBridgeMode());
        pool.ClearBridgeMode();
        pool.SetNumGroups(2);
        response = DefinePool(env, pool);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        pool = ReadPool(env, 2);
        UNIT_ASSERT(pool.HasBridgeMode() && !pool.GetBridgeMode());
        checkGroups(2);
    }

}

Y_UNIT_TEST_SUITE(BridgeMixedStorage) {
    Y_UNIT_TEST(PoolPolicyAndDataSurviveRestart) {
        TEnvironmentSetup env(MixedSettings());
        env.CreatePool();
        UNIT_ASSERT(ReadPool(env, 1).GetBridgeMode());
        for (const auto [id, mode] : {std::pair{2u, false}, std::pair{3u, true}}) {
            const auto response = DefinePool(env, NewPool(env, id, mode));
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }
        const ui32 ordinary = OrdinaryGroup(env);
        const ui32 bridged = env.GetGroups().front();
        TDataChecker checker(env);
        checker.Write(ordinary);
        checker.Write(bridged);

        WaitForOrdinaryGroupReady(env, ordinary);

        const auto before = env.FetchBaseConfig();
        for (const auto& slot : before.GetVSlot()) {
            if (slot.GetGroupId() != ordinary) {
                continue;
            }
            NKikimrBlobStorage::TConfigRequest request;
            auto *reassign = request.AddCommand()->MutableReassignGroupDisk();
            reassign->SetGroupId(ordinary);
            reassign->SetGroupGeneration(slot.GetGroupGeneration());
            reassign->SetFailRealmIdx(slot.GetFailRealmIdx());
            reassign->SetFailDomainIdx(slot.GetFailDomainIdx());
            reassign->SetVDiskIdx(slot.GetVDiskIdx());
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            break;
        }
        UNIT_ASSERT_VALUES_EQUAL(OrdinaryGroup(env), ordinary);
        checker.CheckAll();

        auto pool = ReadPool(env, 2);
        UNIT_ASSERT(pool.HasBridgeMode() && !pool.GetBridgeMode());
        pool.ClearBridgeMode();
        auto response = DefinePool(env, pool);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        pool = ReadPool(env, 2);
        UNIT_ASSERT(pool.HasBridgeMode() && !pool.GetBridgeMode());
        pool.SetBridgeMode(true);
        response = DefinePool(env, pool);
        UNIT_ASSERT(!response.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "can't change storage pool BridgeMode");

        env.RestartNode(1);
        UNIT_ASSERT(ReadPool(env, 1).GetBridgeMode());
        pool = ReadPool(env, 2);
        UNIT_ASSERT(pool.HasBridgeMode() && !pool.GetBridgeMode());
        pool = ReadPool(env, 3);
        UNIT_ASSERT(pool.HasBridgeMode() && pool.GetBridgeMode());
        UNIT_ASSERT_VALUES_EQUAL(OrdinaryGroup(env), ordinary);
        checker.CheckAll();

        // Root/static storage stays bridged regardless of ordinary tenant pools.
        const auto config = QueryStorageConfig(env);
        UNIT_ASSERT(config.GetClusterStateDetails().GetNonUniformStorageEnabled());
        for (const auto& group : config.GetBlobStorageConfig().GetServiceSet().GetGroups()) {
            UNIT_ASSERT(group.HasBridgeGroupState() || group.HasBridgePileId());
        }

        pool = ReadPool(env, 2);
        pool.SetNumGroups(2);
        response = DefinePool(env, pool);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        const auto grown = env.FetchBaseConfig();
        const auto groups = GetPoolGroups(grown, 2);
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        for (const auto *group : groups) {
            UNIT_ASSERT(!GetGroupInfo(env, group->GetGroupId()).HasBridgeGroupState());
            checker.Write(group->GetGroupId());
        }
        checker.CheckAll();
    }

    Y_UNIT_TEST(ResolvedModeRoundtripInUnbridgedCluster) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        env.CreatePool();
        auto pool = ReadPool(env, 1);
        UNIT_ASSERT(pool.HasBridgeMode() && !pool.GetBridgeMode());
        auto response = DefinePool(env, pool);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        pool = ReadPool(env, 1);
        pool.ClearBridgeMode();
        response = DefinePool(env, pool);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        pool = ReadPool(env, 1);
        UNIT_ASSERT(pool.HasBridgeMode() && !pool.GetBridgeMode());
        pool.SetBridgeMode(true);
        response = DefinePool(env, pool);
        UNIT_ASSERT(!response.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "can't change storage pool BridgeMode");

        pool = NewPool(env, 2, true);
        response = DefinePool(env, pool);
        UNIT_ASSERT(!response.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "requires a bridged cluster");
        pool.SetBridgeMode(false);
        response = DefinePool(env, pool);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        env.RestartNode(1);
        pool = ReadPool(env, 2);
        UNIT_ASSERT(pool.HasBridgeMode() && !pool.GetBridgeMode());
    }

    Y_UNIT_TEST(OrdinaryBlock42PoolInTwoDatacenters) {
        TestOrdinarySingleDatacenterErasure(false);
    }

    Y_UNIT_TEST(OrdinaryNonePoolInTwoDatacenters) {
        TestOrdinarySingleDatacenterErasure(true);
    }

    Y_UNIT_TEST(ActivationAndValidation) {
        TEnvironmentSetup env(MixedSettings(false));
        env.CreatePool();
        // Returning the resolved mode must not make read/redefine depend on feature activation.
        auto unchanged = DefinePool(env, ReadPool(env, 1));
        UNIT_ASSERT_C(unchanged.GetSuccess(), unchanged.GetErrorDescription());
        auto explicitDefault = DefinePool(env, NewPool(env, 3, true));
        UNIT_ASSERT_C(explicitDefault.GetSuccess(), explicitDefault.GetErrorDescription());
        auto pool = NewPool(env, 2, false);
        auto response = DefinePool(env, pool);
        UNIT_ASSERT(!response.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "enable_non_uniform_storage");

        const auto activation = ReplaceMainConfig(env, [](YAML::Node& config) {
            config["bridge_config"]["enable_non_uniform_storage"] = true;
        });
        UNIT_ASSERT_EQUAL_C(activation.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            activation.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        const auto config = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(NStorage::GetRequiredBridgeFeatureLevel(config),
            ui32(NStorage::BridgeFeatureLevelNonUniformStorage));
        response = DefinePool(env, pool);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        OrdinaryGroup(env);

        NKikimrBlobStorage::TConfigRequest request;
        auto *move = request.AddCommand()->MutableMoveGroups();
        move->SetBoxId(1);
        move->SetOriginStoragePoolId(2);
        move->SetTargetStoragePoolId(1);
        move->SetOriginStoragePoolGeneration(ReadPool(env, 2).GetItemConfigGeneration());
        move->SetTargetStoragePoolGeneration(ReadPool(env, 1).GetItemConfigGeneration());
        response = env.Invoke(request);
        UNIT_ASSERT(!response.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "different BridgeMode");

        const auto disable = ReplaceMainConfig(env, [](YAML::Node& config) {
            config["bridge_config"]["enable_non_uniform_storage"] = false;
        });
        UNIT_ASSERT_UNEQUAL(disable.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
        UNIT_ASSERT_STRING_CONTAINS(disable.GetErrorReason(), "can't be disabled");
    }

    Y_UNIT_TEST(SharedDisconnectionPreservesOrdinaryQuorum) {
        TEnvironmentSetup env(MixedSettings());
        env.CreatePool();
        const auto response = DefinePool(env, NewPool(env, 2, false));
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        const ui32 ordinary = OrdinaryGroup(env);
        const ui32 bridged = env.GetGroups().front();
        TDataChecker checker(env);
        checker.Write(ordinary);
        checker.Write(bridged);

        auto disconnect = [&](ui32 pile) {
            // Like BridgeFailover, pause the test bootstrapper's immediate retries
            // while distconf commits the state that makes static storage available again.
            std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> pendingDeaths;
            env.Runtime->FilterFunction = [&](ui32 destination, std::unique_ptr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvTablet::TEvTabletDead::EventType
                        && event->Get<TEvTablet::TEvTabletDead>()->TabletID == env.TabletId) {
                    pendingDeaths.emplace_back(destination, std::move(event));
                    return false;
                }
                return true;
            };
            for (ui32 node = pile * 9 + 1; node <= (pile + 1) * 9; ++node) {
                env.StopNode(node);
            }
            SwitchBridgePileState(env, pile, NKikimrBridge::TClusterState::DISCONNECTED);
            env.Sim(TDuration::Seconds(10));
            env.Runtime->FilterFunction = {};
            for (auto& [destination, event] : pendingDeaths) {
                env.Runtime->Send(event.release(), destination);
            }
        };

        disconnect(2);
        const auto removal = InvokeDistconf(env, [](auto& record) {
            record.MutableRemoveBridgePile()->SetPileName("pile_3");
        });
        UNIT_ASSERT_EQUAL(removal.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR);
        UNIT_ASSERT_STRING_CONTAINS(removal.GetErrorReason(), "its data would be lost");
        checker.Write(ordinary);
        checker.Write(bridged);
        checker.CheckAll();

        disconnect(1);
        checker.Write(bridged);
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TString data = "ordinary write without quorum";
        const TLogoBlobID id(100501, 1, 1, 0, data.size(), 0);
        const TInstant deadline = env.Runtime->GetClock() + TDuration::Seconds(5);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, ordinary, new TEvBlobStorage::TEvPut(id, data, deadline));
        });
        auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false,
            deadline + TDuration::Seconds(10));
        UNIT_ASSERT(result);
        UNIT_ASSERT_UNEQUAL_C(result->Get()->Status, NKikimrProto::OK, result->Get()->ToString());

        // Bring back the original disks, including the stale pile; only acknowledged writes are checked.
        for (ui32 node = 10; node <= 27; ++node) {
            env.StartNode(node);
        }
        TString lastConfigError;
        TString lastSyncerError;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRootResult::EventType) {
                const auto& record = event->Get<NStorage::TEvNodeConfigInvokeOnRootResult>()->Record;
                if (record.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
                    lastConfigError = record.ShortDebugString();
                }
            } else if (event->GetTypeRewrite() == TEvBlobStorage::TEvControllerUpdateSyncerState::EventType) {
                for (const auto& syncer : event->Get<TEvBlobStorage::TEvControllerUpdateSyncerState>()->Record.GetSyncers()) {
                    if (syncer.HasErrorReason()) {
                        lastSyncerError = syncer.ShortDebugString();
                    }
                }
            }
            return true;
        };
        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        SwitchBridgePileState(env, 2, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitUntil(env, [&] {
            const auto state = QueryStorageConfig(env).GetClusterState();
            return state.GetPerPileState(1) == NKikimrBridge::TClusterState::SYNCHRONIZED
                && state.GetPerPileState(2) == NKikimrBridge::TClusterState::SYNCHRONIZED;
        }, "rejoined piles synchronized", [&] {
            const auto config = QueryStorageConfig(env);
            return TStringBuilder() << config.GetClusterState().ShortDebugString()
                << " details# " << config.GetClusterStateDetails().ShortDebugString()
                << " lastError# " << lastConfigError << " syncerError# " << lastSyncerError
                << " group# " << GetGroupInfo(env, bridged).ShortDebugString();
        });
        env.Runtime->FilterFunction = {};
        WaitForOrdinaryGroupReady(env, ordinary);
        checker.CheckAll();
        checker.Write(ordinary);
        UNIT_ASSERT_VALUES_EQUAL(OrdinaryGroup(env), ordinary);
    }

    Y_UNIT_TEST(Mirror3dcPoolRequiresThreeDatacenters) {
        TEnvironmentSetup env(MixedSettings(true, 2));
        env.CreatePool();
        const auto response = DefinePool(env, NewPool(env, 2, false));
        UNIT_ASSERT(!response.GetSuccess());
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "failed to allocate group");
    }
}
