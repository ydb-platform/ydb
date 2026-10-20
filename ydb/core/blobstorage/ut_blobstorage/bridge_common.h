#pragma once

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>

// helpers shared by bridge tests
namespace NBridgeTest {

    // nodes of natively bridged cluster: 8 nodes per pile
    inline TNodeLocation GetBridgedLocation(ui32 nodeId) {
        NActorsInterconnect::TNodeLocation location;
        location.SetBridgePileName(TStringBuilder() << "pile_" << (nodeId - 1) / 8 + 1);
        location.SetDataCenter("my_dc");
        location.SetRack(TStringBuilder() << "rack_" << nodeId);
        location.SetUnit(TStringBuilder() << "unit_" << nodeId);
        return TNodeLocation(location);
    }

    // nodes of the cluster being converted have no pile names in their locations
    inline TNodeLocation GetUnbridgedLocation(ui32 nodeId) {
        NActorsInterconnect::TNodeLocation location;
        location.SetDataCenter("my_dc");
        location.SetRack(TStringBuilder() << "rack_" << nodeId);
        location.SetUnit(TStringBuilder() << "unit_" << nodeId);
        return TNodeLocation(location);
    }

    // settings of the cluster to be converted into bridged one
    inline TEnvironmentSetup::TSettings MakeUnbridgedSettings(bool setupHive = false) {
        return {
            .NodeCount = 9, // one spare node to make disk reassignment possible
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetUnbridgedLocation,
            .SetupHive = setupHive,
            .SelfManagementConfig = true,
            .AutomaticBootstrap = true,
        };
    }

    // check that the read result contains exactly one blob with the expected data
    inline void CheckGetResult(const TEvBlobStorage::TEvGetResult& m, const TString& data) {
        UNIT_ASSERT_VALUES_EQUAL_C(m.Status, NKikimrProto::OK, m.ToString());
        UNIT_ASSERT_VALUES_EQUAL(m.ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL_C(m.Responses[0].Status, NKikimrProto::OK, m.ToString());
        UNIT_ASSERT_VALUES_EQUAL(m.Responses[0].Buffer.ConvertToString(), data);
    }

    inline const NKikimrBlobStorage::TBaseConfig::TGroup& FindGroup(const NKikimrBlobStorage::TBaseConfig& baseConfig,
            ui32 groupId) {
        for (const auto& group : baseConfig.GetGroup()) {
            if (group.GetGroupId() == groupId) {
                return group;
            }
        }
        UNIT_FAIL("group not found GroupId# " << groupId);
        Y_UNREACHABLE();
    }

    class TDataChecker {
        TEnvironmentSetup& Env;
        ui32 Step = 1;
        std::vector<std::tuple<ui32, TLogoBlobID, TString>> Written;

    public:
        TDataChecker(TEnvironmentSetup& env)
            : Env(env)
        {}

        void Write(ui32 groupId, ui32 nodeId = 1) {
            const TActorId edge = Env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
            SendPut(groupId, edge);
            auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        }

        // send put without waiting for its result, which is to be checked by the caller
        void SendPut(ui32 groupId, TActorId edge) {
            const TString data = TStringBuilder() << "data for step " << Step;
            const TLogoBlobID id(100500, 1, Step++, 0, data.size(), 0);
            Env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            });
            Written.emplace_back(groupId, id, data);
        }

        const std::vector<std::tuple<ui32, TLogoBlobID, TString>>& GetWritten() const {
            return Written;
        }

        void CheckAll(ui32 nodeId = 1) {
            const TActorId edge = Env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
            for (const auto& [groupId, id, data] : Written) {
                Env.Runtime->WrapInActorContext(edge, [&] {
                    SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                        NKikimrBlobStorage::FastRead));
                });
                CheckGetResult(*Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false)->Get(), data);
            }
        }
    };

    // make nodes (all of them by default) bridge-capable with single pile by restarting them one by one
    inline void RollingRestartWithSinglePileBridgeConfig(TEnvironmentSetup& env, TDataChecker& checker, ui32 groupId,
            std::optional<std::vector<ui32>> nodeIds = std::nullopt) {
        auto& bridgeConfig = env.Runtime->GetAppDataBridgeConfig();
        if (!bridgeConfig.PilesSize()) {
            bridgeConfig.AddPiles()->SetName("pile_1");
        }
        UNIT_ASSERT_VALUES_EQUAL(bridgeConfig.PilesSize(), 1);

        if (!nodeIds) {
            nodeIds.emplace();
            for (ui32 nodeId : env.Runtime->GetNodes()) {
                if (nodeId <= env.ConfiguredNodeCount) { // the rest are not started yet
                    nodeIds->push_back(nodeId);
                }
            }
        }
        for (ui32 nodeId : *nodeIds) {
            Cerr << "*** restarting node " << nodeId << " with single-pile bridge config" << Endl;
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checker.Write(groupId);
            checker.CheckAll();
        }
    }

    inline NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult InvokeDistconf(TEnvironmentSetup& env,
            const std::function<void(NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot&)>& fill, ui32 nodeId = 1,
            TInstant deadline = TInstant::Max()) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        auto ev = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        fill(ev->Record);
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), edge, ev.release()), nodeId);
        auto res = env.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(edge, false, deadline);
        UNIT_ASSERT_C(res, "Timed out waiting for a bridge configuration command");
        return res->Get()->Record;
    }

    inline NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult ConvertToBridge(TEnvironmentSetup& env) {
        return InvokeDistconf(env, [](auto& record) { record.MutableConvertToBridge(); });
    }

    inline NKikimrBlobStorage::TStorageConfig QueryStorageConfig(TEnvironmentSetup& env,
            TInstant deadline = TInstant::Max()) {
        const auto res = InvokeDistconf(env, [](auto& record) { record.MutableQueryConfig(); }, 1, deadline);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        return res.GetQueryConfig().GetConfig();
    }

    inline void SwitchBridgePileState(TEnvironmentSetup& env, ui32 pileIndex,
            NKikimrBridge::TClusterState::EPileState state) {
        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
        for (;;) {
            const auto config = QueryStorageConfig(env, deadline);
            const auto response = InvokeDistconf(env, [&](auto& record) {
                auto *next = record.MutableSwitchBridgeClusterState()->MutableNewClusterState();
                next->CopyFrom(config.GetClusterState());
                next->SetPerPileState(pileIndex, state);
                next->SetGeneration(next->GetGeneration() + 1);
            }, 1, deadline);
            if (response.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
                return;
            }
            // Synchronization can advance another pile between QueryConfig and the command.
            const auto current = QueryStorageConfig(env, deadline).GetClusterState();
            UNIT_ASSERT_C(current.GetGeneration() > config.GetClusterState().GetGeneration()
                && env.Runtime->GetClock() < deadline,
                response.DebugString() << " before# " << config.GetClusterState().ShortDebugString()
                    << " current# " << current.ShortDebugString());
        }
    }

    inline std::vector<const NKikimrBlobStorage::TBaseConfig::TGroup*> GetPoolGroups(
            const NKikimrBlobStorage::TBaseConfig& baseConfig, ui64 storagePoolId) {
        std::vector<const NKikimrBlobStorage::TBaseConfig::TGroup*> res;
        for (const auto& group : baseConfig.GetGroup()) {
            if (group.GetStoragePoolId() == storagePoolId) {
                res.push_back(&group);
            }
        }
        return res;
    }

    // define new storage pool with the same settings as the existing one
    inline void CreateAnotherPool(TEnvironmentSetup& env, ui64 storagePoolId) {
        NKikimrBlobStorage::TConfigRequest request;
        auto *read = request.AddCommand()->MutableReadStoragePool();
        read->SetBoxId(1);
        read->AddStoragePoolId(1);
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        UNIT_ASSERT_VALUES_EQUAL(response.StatusSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(0).StoragePoolSize(), 1);

        NKikimrBlobStorage::TDefineStoragePool pool = response.GetStatus(0).GetStoragePool(0);
        pool.SetStoragePoolId(storagePoolId);
        pool.SetName(TStringBuilder() << "pool_" << storagePoolId);
        pool.SetKind(pool.GetName());
        pool.SetNumGroups(1);
        pool.ClearItemConfigGeneration();

        request.Clear();
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    // keeps the latest group info distributed by BS_CONTROLLER to nodes; Filter is to be called from the filter function
    // of the test runtime
    class TGroupInfoTracker {
        THashMap<ui32, NKikimrBlobStorage::TGroupInfo> Groups;

    public:
        bool Filter(ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvControllerNodeServiceSetUpdate) {
                const auto& record = ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
                for (const auto& group : record.GetServiceSet().GetGroups()) {
                    auto& info = Groups[group.GetGroupID()];
                    if (info.GetGroupGeneration() <= group.GetGroupGeneration()) {
                        info.CopyFrom(group); // the update of the same generation may change decommit status
                    }
                }
            }
            return true;
        }

        const NKikimrBlobStorage::TGroupInfo& Get(ui32 groupId) const {
            const auto it = Groups.find(groupId);
            UNIT_ASSERT_C(it != Groups.end(), "group info not distributed GroupId# " << groupId);
            return it->second;
        }
    };

    // wait until the condition holds, checking it every step of simulated time
    inline void WaitUntil(TEnvironmentSetup& env, const std::function<bool()>& condition, const TString& what,
            TDuration step = TDuration::Seconds(10), ui32 numSteps = 60) {
        for (ui32 i = 0; i < numSteps; ++i) {
            if (condition()) {
                return;
            }
            env.Sim(step);
        }
        UNIT_ASSERT_C(condition(), what << " not reached");
    }

    // request the change of the pile state in the next cluster state generation
    inline NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult TrySwitchPileState(TEnvironmentSetup& env, ui32 pileIndex,
            NKikimrBridge::TClusterState::EPileState state) {
        auto clusterState = QueryStorageConfig(env).GetClusterState();
        clusterState.SetPerPileState(pileIndex, state);
        clusterState.SetGeneration(clusterState.GetGeneration() + 1);
        return InvokeDistconf(env, [&](auto& record) {
            record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(clusterState);
        });
    }

    inline void SwitchPileState(TEnvironmentSetup& env, ui32 pileIndex, NKikimrBridge::TClusterState::EPileState state) {
        const auto res = TrySwitchPileState(env, pileIndex, state);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
    }

    inline NKikimrBridge::TClusterState::EPileState GetPileState(TEnvironmentSetup& env, ui32 pileIndex) {
        return QueryStorageConfig(env).GetClusterState().GetPerPileState(pileIndex);
    }

    inline void WaitForPileState(TEnvironmentSetup& env, ui32 pileIndex, NKikimrBridge::TClusterState::EPileState state) {
        WaitUntil(env, [&] { return GetPileState(env, pileIndex) == state; },
            TStringBuilder() << "pile state " << NKikimrBridge::TClusterState::EPileState_Name(state));
    }

    // group info as BSC distributes it to nodes
    inline NKikimrBlobStorage::TGroupInfo GetGroupInfo(TEnvironmentSetup& env, ui32 groupId) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        auto request = std::make_unique<TEvBlobStorage::TEvControllerGetGroup>();
        request->Record.AddGroupIDs(groupId);
        // cookie Max<ui64>() marks the request not coming from NodeWarden
        env.Runtime->SendToPipe(env.TabletId, edge, request.release(), Max<ui64>(),
            TTestActorSystem::GetPipeConfigWithRetries());
        auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(edge, false);
        for (const auto& group : response->Get()->Record.GetServiceSet().GetGroups()) {
            if (group.GetGroupID() == groupId) {
                return group;
            }
        }
        UNIT_FAIL("group not found GroupId# " << groupId);
        Y_UNREACHABLE();
    }

    // create storage pool with the designated number of groups and wait for them to start; returns the first group id
    inline ui32 CreatePoolAndGetGroup(TEnvironmentSetup& env, ui32 numGroups = 1) {
        env.CreatePool(numGroups);
        env.Sim(TDuration::Seconds(30));
        const auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), numGroups);
        return groups.front();
    }

    // create cluster with dynamic groups and convert it into single-pile bridged one; returns the first group id, which
    // some data is written to
    inline ui32 ConvertCluster(TEnvironmentSetup& env, TDataChecker& checker, ui32 numGroups = 1) {
        const ui32 groupId = CreatePoolAndGetGroup(env, numGroups);

        checker.Write(groupId);
        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);

        const auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT(!QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());
        return groupId;
    }

} // NBridgeTest
