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
            const TString data = TStringBuilder() << "data for step " << Step;
            const TLogoBlobID id(100500, 1, Step++, 0, data.size(), 0);
            Env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            });
            auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
            Written.emplace_back(groupId, id, data);
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
            const auto& nodes = env.Runtime->GetNodes();
            nodeIds.emplace(nodes.begin(), nodes.end());
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
            const std::function<void(NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot&)>& fill, ui32 nodeId = 1) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        auto ev = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
        fill(ev->Record);
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), edge, ev.release()), nodeId);
        auto res = env.WaitForEdgeActorEvent<NStorage::TEvNodeConfigInvokeOnRootResult>(edge, false);
        return res->Get()->Record;
    }

    inline NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult ConvertToBridge(TEnvironmentSetup& env) {
        return InvokeDistconf(env, [](auto& record) { record.MutableConvertToBridge(); });
    }

    inline NKikimrBlobStorage::TStorageConfig QueryStorageConfig(TEnvironmentSetup& env) {
        const auto res = InvokeDistconf(env, [](auto& record) { record.MutableQueryConfig(); });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        return res.GetQueryConfig().GetConfig();
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

    // create cluster with dynamic groups and convert it into single-pile bridged one; returns the first group id, which
    // some data is written to
    inline ui32 ConvertCluster(TEnvironmentSetup& env, TDataChecker& checker, ui32 numGroups = 1) {
        env.CreatePool(numGroups);
        env.Sim(TDuration::Seconds(30));

        const auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), numGroups);
        const ui32 groupId = groups.front();

        checker.Write(groupId);
        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);

        const auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT(!QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());
        return groupId;
    }

} // NBridgeTest
