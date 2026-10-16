#pragma once

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>

// helpers shared by bridge tests
namespace NBridgeTest {

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

        void CheckAll(ui32 nodeId = 1) {
            const TActorId edge = Env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
            for (const auto& [groupId, id, data] : Written) {
                Env.Runtime->WrapInActorContext(edge, [&] {
                    SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                        NKikimrBlobStorage::FastRead));
                });
                auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
                auto& m = *res->Get();
                UNIT_ASSERT_VALUES_EQUAL_C(m.Status, NKikimrProto::OK, m.ToString());
                UNIT_ASSERT_VALUES_EQUAL(m.ResponseSz, 1);
                UNIT_ASSERT_VALUES_EQUAL_C(m.Responses[0].Status, NKikimrProto::OK, m.ToString());
                UNIT_ASSERT_VALUES_EQUAL(m.Responses[0].Buffer.ConvertToString(), data);
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

} // NBridgeTest
