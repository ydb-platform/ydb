#include "bridge_pile_ops.h"

using namespace NBridgeTest;

namespace {

    TEnvironmentSetup::TSettings MakeStaticModeSettings() {
        return {
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .PrepareRuntime = [](TTestActorSystem& runtime) {
                runtime.GetAppDataBridgeConfig().SetEnableNonUniformStorage(true);
            },
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
            .InitialConfiguredNodeCount = 16,
        };
    }

    size_t CountParts(const NKikimrBlobStorage::TGroupInfo& info) {
        size_t result = 0;
        NBridge::ForEachGroupPile(info.GetBridgeGroupState(), [&](TBridgePileId, const auto&) {
            ++result;
        });
        return result;
    }

    void CheckPartCount(TEnvironmentSetup& env, size_t expected) {
        const auto info = GetGroupInfo(env, 0);
        UNIT_ASSERT_VALUES_EQUAL_C(CountParts(info), expected, info.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeGroupState().GetAuthorityPileId(), 0, info.ShortDebugString());
        NBridge::ForEachGroupPile(info.GetBridgeGroupState(), [&](TBridgePileId, const auto& pile) {
            UNIT_ASSERT_EQUAL(pile.GetStage(), NKikimrBridge::TGroupState::SYNCED);
        });
    }

    void WaitForPartCount(TEnvironmentSetup& env, size_t expected) {
        WaitUntil(env, [&] {
            const auto info = GetGroupInfo(env, 0);
            return CountParts(info) == expected && !info.GetBridgeGroupState().GetAuthorityPileId() &&
                std::ranges::all_of(info.GetBridgeGroupState().GetPile(), [](const auto& pile) {
                    return pile.GetAbsent() || pile.GetStage() == NKikimrBridge::TGroupState::SYNCED;
                });
        }, "static group mode transition");
    }

    void ChangeMode(TEnvironmentSetup& env, bool bridged, const TString& retainPile = {}) {
        const auto result = SetStaticGroupBridgeMode(env, bridged, retainPile);
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            result.GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(NBridge::IsStaticGroupBridged(QueryStorageConfig(env).GetClusterStateDetails()), bridged);
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgeStaticGroupMode) {

    Y_UNIT_TEST(RoundTripWithRetainedPile) {
        TEnvironmentSetup env(MakeStaticModeSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        CheckPartCount(env, 2);

        TDataChecker checker(env);
        checker.Write(0);
        const auto before = GetGroupInfo(env, 0);
        const auto *secondPart = NBridge::GetGroupPile(before.GetBridgeGroupState(), TBridgePileId::FromPileIndex(1));
        UNIT_ASSERT(secondPart);
        const ui32 secondPartId = secondPart->GetGroupId();

        ChangeMode(env, false, "pile_2");
        for (ui32 i = 0; i < 3; ++i) {
            checker.Write(0, i % 2 ? 9 : 1);
            checker.CheckAll();
            env.Sim(TDuration::Seconds(5));
        }
        WaitForPartCount(env, 1);
        const auto ordinary = GetGroupInfo(env, 0);
        const auto *retained = NBridge::GetGroupPile(ordinary.GetBridgeGroupState(), TBridgePileId::FromPileIndex(1));
        UNIT_ASSERT_C(retained && retained->GetGroupId() == secondPartId, ordinary.ShortDebugString());
        UNIT_ASSERT(!NBridge::GetGroupPile(ordinary.GetBridgeGroupState(), TBridgePileId::FromPileIndex(0)));
        CheckPileData(env, checker, 1, 9);

        ChangeMode(env, true);
        WaitForPartCount(env, 2);
        const auto bridged = GetGroupInfo(env, 0);
        const auto *restored = NBridge::GetGroupPile(bridged.GetBridgeGroupState(), TBridgePileId::FromPileIndex(0));
        UNIT_ASSERT_C(restored && restored->GetGroupId() != secondPartId, bridged.ShortDebugString());
        UNIT_ASSERT_GT(QueryStorageConfig(env).GetClusterStateDetails().GetNextStaticGroupId(), secondPartId);
        for (ui32 nodeId : {1u, 9u}) {
            checker.Write(0, nodeId);
            checker.CheckAll(nodeId);
        }
        CheckPileData(env, checker, 0, 1);
        CheckPileData(env, checker, 1, 9);
    }

    Y_UNIT_TEST(ReversalReusesPartsAndOrdinaryGroupsSkipNewPiles) {
        TEnvironmentSetup env(MakeStaticModeSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        CheckPartCount(env, 2);
        TDataChecker checker(env);
        checker.Write(0);
        const auto before = GetGroupInfo(env, 0);
        const ui32 secondPartId = NBridge::GetGroupPile(before.GetBridgeGroupState(),
            TBridgePileId::FromPileIndex(1))->GetGroupId();

        ChangeMode(env, false, "pile_1");
        ChangeMode(env, true); // reverse before the cleanup delay expires
        WaitForPartCount(env, 2);
        const auto reused = GetGroupInfo(env, 0);
        UNIT_ASSERT_VALUES_EQUAL(NBridge::GetGroupPile(reused.GetBridgeGroupState(),
            TBridgePileId::FromPileIndex(1))->GetGroupId(), secondPartId);
        checker.CheckAll();

        ChangeMode(env, false, "pile_1");
        WaitForPartCount(env, 1);
        auto res = AddThirdPileToConfig(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        StartPileNodes(env, "pile_3", FirstThirdPileNodeId, LastThirdPileNodeId);
        AddPileAndWait(env, "pile_3");
        const auto withThirdPile = GetGroupInfo(env, 0);
        UNIT_ASSERT_VALUES_EQUAL(CountParts(withThirdPile), 1);
        UNIT_ASSERT(!NBridge::GetGroupPile(withThirdPile.GetBridgeGroupState(), TBridgePileId::FromPileIndex(2)));
        checker.Write(0);
        checker.CheckAll();

        SwitchBridgePileState(env, 2, NKikimrBridge::TClusterState::DISCONNECTED);
        RemovePileAndWait(env, "pile_3");
        ChangeMode(env, true);
        WaitForPartCount(env, 2);
        checker.Write(0);
        checker.CheckAll();
    }

    Y_UNIT_TEST(AuthorityPileOutWhileMakingBridged) {
        TEnvironmentSetup env(MakeStaticModeSettings());
        const ui32 dynamicGroupId = CreatePoolAndGetGroup(env);
        env.Sim(TDuration::Seconds(30));
        CheckPartCount(env, 2);
        TDataChecker checker(env);
        checker.Write(0);
        checker.Write(dynamicGroupId);

        ChangeMode(env, false, "pile_2");
        ChangeMode(env, true);

        // Let the new part reach the stage when it receives every write, then hold its next stage transition.
        NKikimrBlobStorage::TGroupInfo latestInfo;
        bool hold = true;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> held;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType) {
                const auto& record = ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
                if (record.HasUpdateBridgeGroupInfo() && record.GetUpdateBridgeGroupInfo().GetGroupId() == 0) {
                    latestInfo.CopyFrom(record.GetUpdateBridgeGroupInfo().GetBridgeGroupInfo());
                }
            }
            if (hold && ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType) {
                const auto *msg = ev->Get<NStorage::TEvNodeWardenNotifySyncerFinished>();
                if (msg->BridgeProxyGroupId.GetRawId() == 0) {
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
        WaitUntil(env, [&] { return !held.empty(); }, "static part synced up to data stage", TDuration::Seconds(1), 120);
        checker.Write(0);
        checker.CheckAll();

        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        const TString data = "static write while authority pile is out";
        const TLogoBlobID id(100501, 1, 1, 0, data.size(), 0);
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, 0, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        });
        const auto putResult = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_UNEQUAL(putResult->Get()->Status, NKikimrProto::OK);
        checker.Write(dynamicGroupId); // bridged dynamic storage keeps working
        ExpectError(RemovePile(env, "pile_2"), "relies on its part in the pile solely");
        {
            const auto info = GetGroupInfo(env, 0);
            const auto *authority = NBridge::GetGroupPile(info.GetBridgeGroupState(), TBridgePileId::FromPileIndex(1));
            UNIT_ASSERT_C(authority && authority->GetStage() == NKikimrBridge::TGroupState::SYNCED,
                info.ShortDebugString());
        }

        SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
        {
            const auto info = GetGroupInfo(env, 0);
            const auto& state = info.GetBridgeGroupState();
            const auto *authority = NBridge::GetGroupPile(state, TBridgePileId::FromPileIndex(1));
            UNIT_ASSERT_C(authority && authority->GetStage() == NKikimrBridge::TGroupState::SYNCED,
                info.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL(TBridgePileId::FromProto(&state,
                &NKikimrBridge::TGroupState::GetAuthorityPileId), TBridgePileId::FromPileIndex(1));
        }
        checker.Write(0);
        checker.CheckAll();

        hold = false;
        for (auto& [nodeId, ev] : held) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        WaitForPartCount(env, 2);
        for (ui32 pileIndex : {0, 1}) {
            CheckPileData(env, checker, pileIndex, pileIndex ? 9 : 1);
        }

        ChangeMode(env, false, "pile_2");
        WaitForPartCount(env, 1);
        checker.Write(0);
        checker.CheckAll();
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(ConversionCanSkipStaticGroups) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        TDataChecker checker(env);
        checker.Write(groupId);
        checker.Write(0);
        const auto before = QueryStorageConfig(env);
        const auto generation = before.GetBlobStorageConfig().GetServiceSet().GetGroups(0).GetGroupGeneration();

        env.Runtime->GetAppDataBridgeConfig().SetEnableNonUniformStorage(true);
        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);
        const auto result = ConvertToBridge(env, false, true);
        UNIT_ASSERT_EQUAL_C(result.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            result.GetErrorReason());
        env.Sim(TDuration::Minutes(1));
        const auto converted = QueryStorageConfig(env);
        UNIT_ASSERT(!converted.GetClusterStateDetails().GetBscConversionPending());
        UNIT_ASSERT(converted.GetClusterStateDetails().GetNonUniformStorageEnabled());
        UNIT_ASSERT(!NBridge::IsStaticGroupBridged(converted.GetClusterStateDetails()));
        const auto& ordinary = converted.GetBlobStorageConfig().GetServiceSet().GetGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(ordinary.GetGroupGeneration(), generation);
        UNIT_ASSERT(!ordinary.HasBridgeGroupState());
        UNIT_ASSERT(!ordinary.HasBridgePileId());
        UNIT_ASSERT(!ordinary.HasBridgeProxyGroupId());
        for (const auto *pb : {&converted.GetStateStorageConfig(), &converted.GetStateStorageBoardConfig(),
                &converted.GetSchemeBoardConfig()}) {
            for (const auto& ringGroup : pb->GetRingGroups()) {
                UNIT_ASSERT(ringGroup.HasBridgePileId());
            }
        }
        checker.CheckAll();

        ChangeMode(env, true);
        CheckInlinePileGroup(env, 0);
        checker.Write(0);
        checker.CheckAll();
        ChangeMode(env, false);
        WaitForPartCount(env, 1);
        checker.Write(0);
        checker.CheckAll();
    }

}
