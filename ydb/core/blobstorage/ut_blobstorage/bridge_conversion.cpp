#include "bridge_config_yaml.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/core/protos/blob_depot_config.pb.h>

using namespace NBridgeTest;

Y_UNIT_TEST_SUITE(BridgeConversion) {

    Y_UNIT_TEST(RollingRestartIntoBridgeCapable) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);

        TDataChecker checker(env);
        checker.Write(groupId);
        checker.CheckAll();

        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);

        // the cluster is not converted yet, so newly created groups must be ordinary ones and the placement must work
        // in spite of PDisks being bound to the single pile
        CreateAnotherPool(env, 2);
        env.Sim(TDuration::Seconds(30));
        const auto baseConfig = env.FetchBaseConfig();
        const auto poolGroups = GetPoolGroups(baseConfig, 2);
        UNIT_ASSERT_VALUES_EQUAL(poolGroups.size(), 1);
        for (const auto *group : poolGroups) {
            UNIT_ASSERT_C(!group->GetIsProxyGroup(), "unexpected bridge proxy group# " << group->GetGroupId());
            UNIT_ASSERT_C(group->VSlotIdSize(), "unexpected group without VDisks# " << group->GetGroupId());
            checker.Write(group->GetGroupId());
        }
        checker.CheckAll();

        // reassign one disk of the original group to check that self-heal machinery is able to place it
        ReassignOneDisk(env, groupId);
        env.Sim(TDuration::Minutes(1));
        checker.Write(groupId);
        checker.CheckAll();

        // and restart the nodes once more to ensure that bridge-capable cluster restarts correctly
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            Cerr << "*** restarting bridge-capable node " << nodeId << Endl;
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checker.Write(groupId);
            checker.CheckAll();
        }
    }

    // Conversion of the group into inline pile one is emulated by patching group info delivered to node wardens, so this
    // test checks only the proxy machinery: bridge proxy must be put in front of the running DS proxy and route requests
    // of the inline pile into it, and newly started proxies must be set up the same way.
    Y_UNIT_TEST(InlinePileProxy) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);

        TDataChecker checker(env);
        checker.Write(groupId);
        checker.CheckAll();

        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);

        bool converted = false;
        ui32 numInnerPuts = 0; // puts issued by the bridge proxy to the DS proxy of the inline pile
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvControllerNodeServiceSetUpdate:
                    if (converted) {
                        auto& record = ev->Get<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()->Record;
                        for (auto& group : *record.MutableServiceSet()->MutableGroups()) {
                            if (group.GetGroupID() == groupId && group.GetEntityStatus() != NKikimrBlobStorage::DESTROY) {
                                NBridge::MakeInlinePile(&group);
                            }
                        }
                    }
                    break;

                case TEvBlobStorage::EvUpdateGroupInfo:
                    if (auto *msg = ev->Get<TEvBlobStorage::TEvUpdateGroupInfo>(); converted &&
                            msg->GroupId.GetRawId() == groupId && msg->GroupInfo) {
                        NBridge::MakeInlinePile(&*msg->GroupInfo);
                    }
                    break;

                case TEvBlobStorage::EvNodeWardenQueryCacheResult:
                    if (auto *msg = ev->Get<NStorage::TEvNodeWardenQueryCacheResult>(); converted &&
                            msg->Key == Sprintf("G%08" PRIx32, groupId) && msg->GenerationValue) {
                        auto& [generation, value] = *msg->GenerationValue;
                        NKikimrBlobStorage::TGroupInfo group;
                        UNIT_ASSERT(group.ParseFromString(value));
                        NBridge::MakeInlinePile(&group);
                        UNIT_ASSERT(group.SerializeToString(&value));
                    }
                    break;

                case TEvBlobStorage::EvPut:
                    if (auto *msg = ev->Get<TEvBlobStorage::TEvPut>(); msg->ForceGroupGeneration &&
                            msg->Id.TabletID() == 100500) {
                        ++numInnerPuts;
                    }
                    break;
            }
            return true;
        };

        auto checkThroughBridgeProxy = [&](ui32 nodeId) {
            const ui32 prev = numInnerPuts;
            checker.Write(groupId, nodeId);
            UNIT_ASSERT_VALUES_EQUAL(numInnerPuts, prev + 1);
            checker.CheckAll(nodeId);
        };

        // bump group generation to make BSC distribute the (patched) group info to running proxies and VDisks
        converted = true;
        ReassignOneDisk(env, groupId);
        env.Sim(TDuration::Minutes(1));
        checkThroughBridgeProxy(1);

        // newly started proxies must also be bridged ones
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            Cerr << "*** restarting node " << nodeId << " with inline pile group" << Endl;
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checkThroughBridgeProxy(nodeId);
        }
    }

    // puts batched by the DS proxy before the bridge proxy is put in front of it must not be processed by the DS proxy
    // itself with the inline pile view, as they have no forced generation
    void BatchedPutDuringConversion(ui32 numPuts) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);

        TDataChecker checker(env);
        checker.Write(groupId);
        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);

        const ui32 nodeId = 1;
        const TActorId dsProxyId = env.Runtime->GetNode(nodeId)->ActorSystem->LookupLocalService(
            MakeBlobStorageProxyID(groupId));
        UNIT_ASSERT(dsProxyId);

        // hold the event making DS proxy stop batching puts until it gets the inline pile view of the group
        bool holdStopBatching = true;
        bool inlineViewConfigured = false;
        std::vector<std::unique_ptr<IEventHandle>> held;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() != dsProxyId) {
                return true;
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvConfigureProxy) {
                const auto& info = ev->Get<TEvBlobStorage::TEvConfigureProxy>()->Info;
                if (info && info->IsInlinePileView() && info->GroupID == TGroupId::FromValue(groupId)) {
                    UNIT_ASSERT(info->Group);
                    UNIT_ASSERT(info->Group->HasBridgeGroupState());
                    inlineViewConfigured = true;
                }
            } else if (holdStopBatching && ev->Sender == dsProxyId && ev->GetTypeName().Contains("TEvStopBatchingPutRequests")) {
                held.push_back(std::move(ev));
                return false;
            }
            return true;
        };

        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        for (ui32 i = 0; i < numPuts; ++i) {
            checker.SendPut(groupId, edge);
        }
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(held.size(), 1);

        const auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        const TInstant deadline = env.Runtime->GetClock() + TDuration::Minutes(1);
        env.Runtime->Sim([&] { return !inlineViewConfigured && env.Runtime->GetClock() <= deadline; });
        UNIT_ASSERT(inlineViewConfigured);
        env.Sim(TDuration::Seconds(5)); // let DS proxy return to working state with the inline pile view

        holdStopBatching = false;
        for (auto& ev : held) {
            env.Runtime->Send(ev.release(), nodeId);
        }
        for (ui32 i = 0; i < numPuts; ++i) {
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        }

        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT(!QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());
        checker.Write(groupId);
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            checker.CheckAll(nodeId);
        }
    }

    Y_UNIT_TEST(BatchedPutDuringConversion) {
        BatchedPutDuringConversion(1);
    }

    Y_UNIT_TEST(BatchOfPutsDuringConversion) {
        BatchedPutDuringConversion(3);
    }

    // static node not reporting its bridge capability (being disconnected) may still run without BridgeConfig, so it
    // must block conversion
    // put started by DS proxy before conversion races with VDisks having new group generation and gets restarted
    // through node warden to the service of the group, which is the bridge proxy by then
    Y_UNIT_TEST(PutRestartedAfterConversion) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);

        TDataChecker checker(env);
        checker.Write(groupId);
        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);

        // hold VDisk writes of the put until the group is converted
        const ui64 tabletId = 100502;
        bool hold = true;
        std::vector<std::unique_ptr<IEventHandle>> held;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (hold && ev->GetTypeRewrite() == TEvBlobStorage::EvVPut &&
                    LogoBlobIDFromLogoBlobID(ev->Get<TEvBlobStorage::TEvVPut>()->Record.GetBlobID()).TabletID() == tabletId) {
                held.push_back(std::move(ev));
                return false;
            }
            return true;
        };

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TString data = "data of put restarted after conversion";
        const TLogoBlobID id(tabletId, 1, 1, 0, data.size(), 0);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        });
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(!held.empty());

        const auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(5));

        // VDisks have new generation now, so they reply RACE to these writes
        hold = false;
        for (auto& ev : held) {
            const ui32 nodeId = ev->Sender.NodeId();
            env.Runtime->Send(ev.release(), nodeId);
        }
        auto putRes = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(putRes->Get()->Status, NKikimrProto::OK, putRes->Get()->ToString());

        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead));
        });
        auto getRes = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(getRes->Get()->Status, NKikimrProto::OK, getRes->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(getRes->Get()->Responses[0].Buffer.ConvertToString(), data);

        checker.Write(groupId);
        checker.CheckAll();
    }

    Y_UNIT_TEST(AbsentStaticNodeBlocksConversion) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);

        TDataChecker checker(env);
        checker.Write(groupId);

        // all nodes but the last one are prepared; the last one goes away while still running old settings
        const auto nodes = env.Runtime->GetNodes();
        std::vector<ui32> nodeIds(nodes.begin(), nodes.end());
        const ui32 absentNodeId = nodeIds.back();
        nodeIds.pop_back();
        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId, nodeIds);
        env.StopNode(absentNodeId);
        env.Sim(TDuration::Seconds(30));

        auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR,
            res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "did not report");
        UNIT_ASSERT(!QueryStorageConfig(env).HasClusterState());
        checker.Write(groupId);

        // the node returns with new settings, so the cluster may be converted now
        env.StartNode(absentNodeId);
        env.Sim(TDuration::Seconds(30));
        res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT(!QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());
        checker.Write(groupId, absentNodeId);
        checker.CheckAll(absentNodeId);
        checker.CheckAll();
    }

    // node that runs without BridgeConfig after the conversion (e.g. dynamic one missed by capability check) must not
    // crash when serving bridged group, it just can't serve such groups until restarted with BridgeConfig
    Y_UNIT_TEST(NodeWithoutBridgeConfigAfterConversion) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);

        TDataChecker checker(env);
        checker.Write(groupId);
        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);

        const auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT(!QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());

        // restart one node without bridge config
        const ui32 nodeId = 2;
        auto& bridgeConfig = env.Runtime->GetAppDataBridgeConfig();
        const NKikimrConfig::TBridgeConfig saved = bridgeConfig;
        bridgeConfig.Clear();
        env.RestartNode(nodeId);
        bridgeConfig = saved;
        env.Sim(TDuration::Seconds(30));

        const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
        const TString data = "data written through not prepared node";
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(TLogoBlobID(100501, 1, 1, 0, data.size(), 0), data,
                TInstant::Max()));
        });
        auto putRes = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(putRes->Get()->Status, NKikimrProto::ERROR, putRes->Get()->ToString());
        UNIT_ASSERT_STRING_CONTAINS(putRes->Get()->ErrorReason, "BridgeConfig");

        // other nodes keep working
        checker.Write(groupId);
        checker.CheckAll();

        // and the node is fine after being restarted with bridge config
        env.RestartNode(nodeId);
        env.Sim(TDuration::Seconds(30));
        checker.Write(groupId, nodeId);
        checker.CheckAll(nodeId);
    }

    // Hive restarted on bridge-capable node must keep nodes registered before (and whose Local actors survived Hive
    // restart) eligible for running tablets, as they implicitly belong to the only pile
    Y_UNIT_TEST(HiveKeepsUntouchedNodesEligible) {
        TEnvironmentSetup env(MakeUnbridgedSettings(true));
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));

        // restart only the node hosting Hive with bridge config; Locals on other nodes stay the same
        const ui32 hiveNodeId = env.Settings.ControllerNodeId;
        env.Runtime->GetAppDataBridgeConfig().AddPiles()->SetName("pile_1");
        env.RestartNode(hiveNodeId);
        env.Sim(TDuration::Seconds(30));

        // create tablet that may run only on one of untouched nodes
        const ui32 targetNodeId = 5;
        UNIT_ASSERT_VALUES_UNEQUAL(targetNodeId, hiveNodeId);
        const TActorId edge = env.Runtime->AllocateEdgeActor(hiveNodeId, __FILE__, __LINE__);
        const TActorId clientId = env.Runtime->Register(NTabletPipe::CreateClient(edge, MakeDefaultHiveID(),
            NTabletPipe::TClientRetryPolicy::WithRetries()), edge.NodeId());
        {
            auto res = env.WaitForEdgeActorEvent<TEvTabletPipe::TEvClientConnected>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }
        env.Runtime->WrapInActorContext(edge, [&] {
            auto ev = std::make_unique<TEvHive::TEvCreateTablet>();
            auto& record = ev->Record;
            record.SetOwner(1);
            record.SetOwnerIdx(1);
            record.SetTabletType(TTabletTypes::BlobDepot);
            record.AddAllowedNodeIDs(targetNodeId);
            for (ui32 i = 0; i < 3; ++i) {
                record.AddBindedChannels()->SetStoragePoolName(env.StoragePoolName);
            }
            NTabletPipe::SendData(edge, clientId, ev.release());
        });
        {
            auto res = env.WaitForEdgeActorEvent<TEvHive::TEvCreateTabletReply>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Record.GetStatus(), NKikimrProto::OK, res->Get()->Record.ShortDebugString());
        }
        auto res = env.WaitForEdgeActorEvent<TEvHive::TEvTabletCreationResult>(edge, false,
            env.Runtime->GetClock() + TDuration::Minutes(1));
        UNIT_ASSERT_C(res, "tablet constrained to untouched node did not start");
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.GetStatus(), NKikimrProto::OK);
    }

    // requests to the service of the converted group which the bridge proxy has to handle in a special way
    Y_UNIT_TEST(RequestsToInlinePile) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);

        ui32 generation = 0;
        const auto baseConfig = env.FetchBaseConfig();
        for (const auto& group : baseConfig.GetGroup()) {
            if (group.GetGroupId() == groupId) {
                generation = group.GetGroupGeneration();
            }
        }
        UNIT_ASSERT(generation > 1);

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        auto send = [&](IEventBase *ev) {
            env.Runtime->WrapInActorContext(edge, [&] { SendToBSProxy(edge, groupId, ev); });
        };

        // BlobDepot agent attaches execution relay to requests to its backing groups and expects it back
        const TString data = "data with execution relay";
        const TLogoBlobID id(100501, 1, 1, 0, data.size(), 0);
        {
            auto relay = std::make_shared<TEvBlobStorage::TExecutionRelay>();
            auto ev = std::make_unique<TEvBlobStorage::TEvPut>(id, data, TInstant::Max());
            ev->ExecutionRelay = relay;
            send(ev.release());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
            UNIT_ASSERT(res->Get()->ExecutionRelay == relay);
        }
        {
            auto relay = std::make_shared<TEvBlobStorage::TExecutionRelay>();
            auto ev = std::make_unique<TEvBlobStorage::TEvGet>(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead);
            ev->ExecutionRelay = relay;
            send(ev.release());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Buffer.ConvertToString(), data);
            UNIT_ASSERT(res->Get()->ExecutionRelay == relay);
        }

        // BlobDepot read path checks integrity of blobs in its backing groups
        {
            auto relay = std::make_shared<TEvBlobStorage::TExecutionRelay>();
            auto ev = std::make_unique<TEvBlobStorage::TEvCheckIntegrity>(id, TInstant::Max(), NKikimrBlobStorage::FastRead);
            ev->ExecutionRelay = relay;
            send(ev.release());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCheckIntegrityResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
            UNIT_ASSERT_EQUAL_C(res->Get()->PlacementStatus, TEvBlobStorage::TEvCheckIntegrityResult::PS_OK,
                res->Get()->ToString());
            UNIT_ASSERT(res->Get()->ExecutionRelay == relay);
        }

        // replication sends phantom checks to the physical group with its own generation
        {
            auto ev = std::make_unique<TEvBlobStorage::TEvGet>(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::AsyncRead);
            ev->PhantomCheck = true;
            ev->ForceGroupGeneration = generation;
            send(ev.release());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
        }

        // and the generation of such request is a fence which must not be replaced
        {
            const TString data = "data with stale generation";
            auto ev = std::make_unique<TEvBlobStorage::TEvPut>(TLogoBlobID(100501, 1, 2, 0, data.size(), 0), data,
                TInstant::Max());
            ev->ForceGroupGeneration = generation - 1;
            send(ev.release());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::RACE, res->Get()->ToString());
        }

        checker.Write(groupId);
        checker.CheckAll();
    }

    // converted groups still have their VDisks laid out by the erasure of the pool, so it can't be changed
    Y_UNIT_TEST(PoolErasureOfConvertedGroups) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        TDataChecker checker(env);
        ConvertCluster(env, checker);

        NKikimrBlobStorage::TConfigRequest request;
        auto *read = request.AddCommand()->MutableReadStoragePool();
        read->SetBoxId(1);
        read->AddStoragePoolId(1);
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        NKikimrBlobStorage::TDefineStoragePool pool = response.GetStatus(0).GetStoragePool(0);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetErasureSpecies(), "block-4-2");
        pool.SetErasureSpecies("none");

        request.Clear();
        request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
        response = env.Invoke(request);
        UNIT_ASSERT_C(!response.GetSuccess(), "erasure change of pool with converted groups must be rejected");
        UNIT_ASSERT_STRING_CONTAINS(response.GetErrorDescription(), "different erasure species");
    }

    // BSC has to retry finishing conversion when distconf fails to clear the flag, as nothing else may trigger it on a
    // quiet cluster
    Y_UNIT_TEST(ConversionFinishRetried) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);

        TDataChecker checker(env);
        checker.Write(groupId);
        RollingRestartWithSinglePileBridgeConfig(env, checker, groupId);

        // intercept the first notification from BSC, so that it does not reach distconf
        std::unique_ptr<IEventHandle> held;
        ui32 numNotifications = 0;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType &&
                    ev->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record.HasNotifyBscConversionFinished() &&
                    !numNotifications++) {
                held = std::move(ev);
                return false;
            }
            return true;
        };

        const auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        const TInstant deadline = env.Runtime->GetClock() + TDuration::Minutes(1);
        env.Runtime->Sim([&] { return !held && env.Runtime->GetClock() <= deadline; });
        UNIT_ASSERT(held);
        UNIT_ASSERT(QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());

        // answer it with an error, which is not retried by the invocation itself (like loss of quorum during propose)
        auto result = std::make_unique<NStorage::TEvNodeConfigInvokeOnRootResult>();
        result->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR);
        result->Record.SetErrorReason("injected failure");
        env.Runtime->Send(new IEventHandle(held->Sender, held->Recipient, result.release(), 0, held->Cookie),
            held->Sender.NodeId());

        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT_GE(numNotifications, 2);
        UNIT_ASSERT(!QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());

        checker.Write(groupId);
        checker.CheckAll();
    }

    Y_UNIT_TEST(ConvertStaticGroup) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const ui32 staticGroupId = 0;

        TDataChecker checker(env);
        checker.Write(groupId);
        checker.Write(staticGroupId);
        checker.CheckAll();

        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

        // nodes are not bridge-capable yet
        auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());

        // and some of them still are not
        RollingRestartWithSinglePileBridgeConfig(env, checker, staticGroupId, std::vector<ui32>{1, 2, 3, 4, 5});
        res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT(!QueryStorageConfig(env).HasClusterState());

        RollingRestartWithSinglePileBridgeConfig(env, checker, staticGroupId, std::vector<ui32>{6, 7, 8, 9});
        const auto before = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(before.GetBlobStorageConfig().GetServiceSet().GroupsSize(), 1);
        const auto& staticGroupBefore = before.GetBlobStorageConfig().GetServiceSet().GetGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(staticGroupBefore.GetGroupID(), staticGroupId);

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

        res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(30));

        // check the resulting config
        const auto after = QueryStorageConfig(env);
        UNIT_ASSERT(after.HasClusterState());
        UNIT_ASSERT_VALUES_EQUAL(after.GetClusterState().PerPileStateSize(), 1);
        UNIT_ASSERT(!after.GetClusterStateDetails().GetBscConversionPending()); // BSC has already converted its groups
        const auto& registry = after.GetClusterStateDetails().GetPileRegistry(); // the only pile is the implicit one
        UNIT_ASSERT_VALUES_EQUAL(registry.PilesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(registry.GetPiles(0).GetName(), "pile_1");
        UNIT_ASSERT_EQUAL(TBridgePileId::FromProto(&registry, &NKikimrBridge::TPileRegistry::GetImplicitPileId),
            TBridgePileId::FromPileIndex(0));
        UNIT_ASSERT_VALUES_EQUAL(after.GetBlobStorageConfig().GetServiceSet().GroupsSize(), 1);
        const auto& staticGroup = after.GetBlobStorageConfig().GetServiceSet().GetGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(staticGroup.GetGroupID(), staticGroupId);
        UNIT_ASSERT_VALUES_EQUAL(staticGroup.GetGroupGeneration(), staticGroupBefore.GetGroupGeneration() + 1);
        UNIT_ASSERT_VALUES_EQUAL(staticGroup.GetBridgeProxyGroupId(), staticGroupId);
        UNIT_ASSERT_VALUES_EQUAL(staticGroup.GetBridgeGroupState().PileSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(staticGroup.GetBridgeGroupState().GetPile(0).GetGroupId(), staticGroupId);
        for (const auto *pb : {&after.GetStateStorageConfig(), &after.GetStateStorageBoardConfig(),
                &after.GetSchemeBoardConfig()}) {
            UNIT_ASSERT(pb->RingGroupsSize());
            for (const auto& ringGroup : pb->GetRingGroups()) {
                UNIT_ASSERT(ringGroup.HasBridgePileId());
            }
        }

        // conversion can't be repeated
        res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());

        // static group is served by the bridge proxy, old data is still readable
        auto checkStaticGroup = [&](ui32 nodeId) {
            const ui32 prev = numInnerPuts;
            checker.Write(staticGroupId, nodeId);
            UNIT_ASSERT_VALUES_EQUAL(numInnerPuts, prev + 1);
            checker.Write(groupId, nodeId);
            checker.CheckAll(nodeId);
        };
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            checkStaticGroup(nodeId);
        }

        // move one of the static group's VDisks; inline pile generation must follow the group's one
        {
            const auto& vdisk = after.GetBlobStorageConfig().GetServiceSet().GetVDisks(0);
            res = InvokeDistconf(env, [&](auto& record) {
                record.MutableReassignGroupDisk()->MutableVDiskId()->CopyFrom(vdisk.GetVDiskID());
            });
            UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
            env.Sim(TDuration::Minutes(1));
            const auto reassigned = QueryStorageConfig(env);
            const auto& group = reassigned.GetBlobStorageConfig().GetServiceSet().GetGroups(0);
            UNIT_ASSERT_VALUES_EQUAL(group.GetGroupGeneration(), staticGroup.GetGroupGeneration() + 1);
            UNIT_ASSERT_VALUES_EQUAL(group.GetBridgeGroupState().GetPile(0).GetGroupGeneration(),
                group.GetGroupGeneration());
            checkStaticGroup(1);
        }

        // the cluster must restart correctly in its new state
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            Cerr << "*** restarting node " << nodeId << " of converted cluster" << Endl;
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checkStaticGroup(nodeId);
        }
    }

    Y_UNIT_TEST(ConvertDynamicGroups) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        env.CreatePool();
        CreateAnotherPool(env, 2);
        env.Sim(TDuration::Seconds(30));

        std::vector<ui32> groups;
        const auto initialBaseConfig = env.FetchBaseConfig();
        for (ui64 storagePoolId : {1, 2}) {
            const auto poolGroups = GetPoolGroups(initialBaseConfig, storagePoolId);
            UNIT_ASSERT_VALUES_EQUAL(poolGroups.size(), 1);
            groups.push_back(poolGroups.front()->GetGroupId());
        }

        TDataChecker checker(env);
        for (ui32 groupId : groups) {
            checker.Write(groupId);
        }
        checker.CheckAll();

        RollingRestartWithSinglePileBridgeConfig(env, checker, groups.front());

        ui32 numInnerPuts = 0; // puts issued by the bridge proxy to the DS proxy of the pile
        TGroupInfoTracker groupInfos; // latest group infos sent by BSC
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                if (auto *msg = ev->Get<TEvBlobStorage::TEvPut>(); msg->ForceGroupGeneration &&
                        msg->Id.TabletID() == 100500) {
                    ++numInnerPuts;
                }
            }
            return groupInfos.Filter(nodeId, ev);
        };

        const auto baseConfigBefore = env.FetchBaseConfig();
        auto getGroupGeneration = [](const NKikimrBlobStorage::TBaseConfig& baseConfig, ui32 groupId) {
            for (const auto& group : baseConfig.GetGroup()) {
                if (group.GetGroupId() == groupId) {
                    return group.GetGroupGeneration();
                }
            }
            UNIT_FAIL("group not found GroupId# " << groupId);
            return 0u;
        };

        const auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Minutes(1));

        // BSC must have converted its groups and reported it to distconf
        UNIT_ASSERT(!QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());

        // groups keep their ids and VDisks, and become inline piles
        const auto baseConfig = env.FetchBaseConfig();
        UNIT_ASSERT(env.GetGroups() == std::vector<ui32>{groups.front()}); // still listable
        for (ui32 groupId : groups) {
            const ui32 generation = getGroupGeneration(baseConfig, groupId);
            UNIT_ASSERT_VALUES_EQUAL(generation, getGroupGeneration(baseConfigBefore, groupId) + 1);
            const auto& info = groupInfos.Get(groupId);
            UNIT_ASSERT_VALUES_EQUAL_C(info.GetGroupGeneration(), generation, info.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeProxyGroupId(), groupId, info.ShortDebugString());
            UNIT_ASSERT_C(TBridgePileId::FromProto(&info, &NKikimrBlobStorage::TGroupInfo::GetBridgePileId) ==
                TBridgePileId::FromPileIndex(0), info.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeGroupState().PileSize(), 1, info.ShortDebugString());
            const auto& pile = info.GetBridgeGroupState().GetPile(0);
            UNIT_ASSERT_VALUES_EQUAL_C(pile.GetGroupId(), groupId, info.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL_C(pile.GetGroupGeneration(), generation, info.ShortDebugString());
            UNIT_ASSERT_C(info.RingsSize(), info.ShortDebugString());
        }

        // data is served through the bridge proxy now, old data is still readable
        auto checkGroups = [&](ui32 nodeId) {
            for (ui32 groupId : groups) {
                const ui32 prev = numInnerPuts;
                checker.Write(groupId, nodeId);
                UNIT_ASSERT_VALUES_EQUAL(numInnerPuts, prev + 1);
            }
            checker.CheckAll(nodeId);
        };
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            checkGroups(nodeId);
        }

        // self-heal machinery must work for converted groups; pile generation follows the group's one
        ReassignOneDisk(env, groups.front());
        env.Sim(TDuration::Minutes(1));
        {
            const auto& info = groupInfos.Get(groups.front());
            UNIT_ASSERT_VALUES_EQUAL(info.GetGroupGeneration(),
                getGroupGeneration(env.FetchBaseConfig(), groups.front()));
            UNIT_ASSERT_VALUES_EQUAL(info.GetBridgeGroupState().GetPile(0).GetGroupGeneration(), info.GetGroupGeneration());
        }
        checkGroups(1);

        // new groups are bridged ones, both in new and existing pools
        CreateAnotherPool(env, 3);
        SetNumGroups(env, 2, 2);
        env.Sim(TDuration::Seconds(30));
        const auto newBaseConfig = env.FetchBaseConfig();
        for (const auto& [storagePoolId, numGroups] : {std::make_pair(2, 3), std::make_pair(3, 2)}) {
            // existing pool contains inline pile and new proxy group with its pile group
            const auto poolGroups = GetPoolGroups(newBaseConfig, storagePoolId);
            UNIT_ASSERT_VALUES_EQUAL(poolGroups.size(), numGroups);
            ui32 newGroupId = 0;
            for (const auto *group : poolGroups) {
                if (group->GetIsProxyGroup()) {
                    UNIT_ASSERT(!newGroupId);
                    newGroupId = group->GetGroupId();
                }
            }
            UNIT_ASSERT(newGroupId);
            checker.Write(newGroupId);
        }
        checker.CheckAll();

        // the cluster must restart correctly in its new state
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            Cerr << "*** restarting node " << nodeId << " of converted cluster" << Endl;
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checkGroups(nodeId);
        }
    }

    // conversion of existing pools and their groups may be skipped, which activates non-uniform storage along with the
    // conversion; static groups become inline piles anyway
    Y_UNIT_TEST(SkipStoragePoolConversion) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        env.CreatePool();
        CreateAnotherPool(env, 2);
        env.Sim(TDuration::Seconds(30));
        std::vector<ui32> groups;
        const auto initialBaseConfig = env.FetchBaseConfig();
        for (ui64 storagePoolId : {1, 2}) {
            for (const auto *group : GetPoolGroups(initialBaseConfig, storagePoolId)) {
                groups.push_back(group->GetGroupId());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        const ui32 staticGroupId = 0;

        TDataChecker checker(env);
        for (ui32 groupId : {groups[0], groups[1], staticGroupId}) {
            checker.Write(groupId);
        }

        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        env.Runtime->GetAppDataBridgeConfig().SetEnableNonUniformStorage(true);
        RollingRestartWithSinglePileBridgeConfig(env, checker, groups.front());

        // the activation can't go along with the conversion of BSC groups
        auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::ERROR, res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "only after BSC conversion completes");
        UNIT_ASSERT(!QueryStorageConfig(env).HasClusterState());

        auto getGeneration = [&](ui32 groupId) {
            return FindGroup(env.FetchBaseConfig(), groupId).GetGroupGeneration();
        };
        std::vector<ui32> generations;
        for (ui32 groupId : groups) {
            generations.push_back(getGeneration(groupId));
        }

        res = ConvertToBridge(env, true);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), TResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Minutes(1));

        const auto config = QueryStorageConfig(env);
        UNIT_ASSERT(config.HasClusterState());
        UNIT_ASSERT(!config.GetClusterStateDetails().GetBscConversionPending());
        UNIT_ASSERT(config.GetClusterStateDetails().GetNonUniformStorageEnabled());
        const auto& staticGroup = config.GetBlobStorageConfig().GetServiceSet().GetGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(staticGroup.GetBridgeProxyGroupId(), staticGroupId);

        // groups of BSC are left intact, pools stay ordinary ones
        for (size_t i = 0; i < groups.size(); ++i) {
            const auto info = GetGroupInfo(env, groups[i]);
            UNIT_ASSERT_C(!info.HasBridgeGroupState() && !info.HasBridgeProxyGroupId(), info.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL(getGeneration(groups[i]), generations[i]);
        }
        NKikimrBlobStorage::TConfigRequest request;
        auto *read = request.AddCommand()->MutableReadStoragePool();
        read->SetBoxId(1);
        auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(0).StoragePoolSize(), 2);
        for (const auto& pool : response.GetStatus(0).GetStoragePool()) {
            UNIT_ASSERT_C(pool.HasBridgeMode() && !pool.GetBridgeMode(), pool.ShortDebugString());
        }

        // new pools are bridged ones unless they are requested to be ordinary
        NKikimrBlobStorage::TDefineStoragePool pool = response.GetStatus(0).GetStoragePool(0);
        pool.ClearItemConfigGeneration();
        pool.SetNumGroups(1);
        for (const auto& [storagePoolId, bridgeMode] : {std::make_pair(3, std::optional<bool>()),
                std::make_pair(4, std::optional<bool>(false))}) {
            pool.SetStoragePoolId(storagePoolId);
            pool.SetName(TStringBuilder() << "pool_" << storagePoolId);
            pool.SetKind(pool.GetName());
            if (bridgeMode) {
                pool.SetBridgeMode(*bridgeMode);
            } else {
                pool.ClearBridgeMode();
            }
            request.Clear();
            request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
            response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }
        env.Sim(TDuration::Seconds(30));
        const auto baseConfig = env.FetchBaseConfig(); // groups are referenced by pointers
        for (const auto& [storagePoolId, numGroups] : {std::make_pair(3, 2), std::make_pair(4, 1)}) {
            const auto poolGroups = GetPoolGroups(baseConfig, storagePoolId);
            UNIT_ASSERT_VALUES_EQUAL(poolGroups.size(), numGroups); // bridged one has proxy and part in the pile
            for (const auto *group : poolGroups) {
                if (group->GetIsProxyGroup() || numGroups == 1) {
                    checker.Write(group->GetGroupId());
                }
            }
        }

        auto checkAll = [&](ui32 nodeId) {
            for (ui32 groupId : {groups[0], groups[1], staticGroupId}) {
                checker.Write(groupId, nodeId);
            }
            checker.CheckAll(nodeId);
        };
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            checkAll(nodeId);
        }

        // ordinary groups keep working through disk reassignment and restarts of the converted cluster
        ReassignOneDisk(env, groups.front());
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT_VALUES_EQUAL(getGeneration(groups.front()), generations.front() + 1);
        checkAll(1);
        for (ui32 nodeId : env.Runtime->GetNodes()) {
            Cerr << "*** restarting node " << nodeId << " of converted cluster" << Endl;
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checkAll(nodeId);
        }
    }

    // YAML config of the converted cluster can't lose its bridge config, as nodes restarted with such config would run
    // without one; the config of this test environment has no bridge config, nodes get it from their startup settings
    Y_UNIT_TEST(BridgeConfigCantBeDropped) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);

        auto res = ReplaceMainConfig(env, [](YAML::Node& /*config*/) {});
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR,
            res.GetErrorReason());
        UNIT_ASSERT_STRING_CONTAINS(res.GetErrorReason(), "bridged cluster can't run without bridge config");

        // the config with the only pile of the cluster is accepted
        res = ReplaceMainConfig(env, [](YAML::Node& config) {
            YAML::Node pile;
            pile["name"] = "pile_1";
            config["bridge_config"]["piles"].push_back(pile);
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            res.GetErrorReason());
        env.Sim(TDuration::Seconds(10));
        checker.Write(groupId);
        checker.CheckAll();
    }

    // single ring group of state storage must stay in RingGroups form for bridged config, legacy Ring isn't allowed there
    Y_UNIT_TEST(ReconfigStateStorageAfterConversion) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        TDataChecker checker(env);
        const ui32 groupId = ConvertCluster(env, checker);

        const auto before = QueryStorageConfig(env);
        UNIT_ASSERT_VALUES_EQUAL(before.GetStateStorageConfig().RingGroupsSize(), 1);
        const auto res = InvokeDistconf(env, [&](auto& record) {
            auto *cmd = record.MutableReconfigStateStorage();
            cmd->MutableStateStorageConfig()->CopyFrom(before.GetStateStorageConfig());
            cmd->MutableStateStorageBoardConfig()->CopyFrom(before.GetStateStorageBoardConfig());
            cmd->MutableSchemeBoardConfig()->CopyFrom(before.GetSchemeBoardConfig());
        });
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Seconds(30));

        const auto after = QueryStorageConfig(env);
        UNIT_ASSERT_LT(before.GetGeneration(), after.GetGeneration());
        for (const auto *pb : {&after.GetStateStorageConfig(), &after.GetStateStorageBoardConfig(),
                &after.GetSchemeBoardConfig()}) {
            UNIT_ASSERT(!pb->HasRing());
            UNIT_ASSERT_VALUES_EQUAL(pb->RingGroupsSize(), 1);
            UNIT_ASSERT(pb->GetRingGroups(0).HasBridgePileId());
        }
        checker.Write(groupId);
        checker.CheckAll();
    }

    // group being decommitted is not converted, and BSC conversion is not finished until the decommission is over;
    // test environment has no SchemeCache, so decommission never goes beyond PENDING state, which is enough to block
    // the conversion: the group has both physical and virtual parts; then decommission is cancelled and the group
    // becomes ordinary one again, so it gets converted and the conversion finishes
    Y_UNIT_TEST(DecommitBlocksConversion) {
        TEnvironmentSetup env(MakeUnbridgedSettings());
        env.CreatePool();
        CreateAnotherPool(env, 2);
        env.Sim(TDuration::Seconds(30));

        std::vector<ui32> groups;
        const auto initialBaseConfig = env.FetchBaseConfig();
        for (ui64 storagePoolId : {1, 2}) {
            const auto poolGroups = GetPoolGroups(initialBaseConfig, storagePoolId);
            UNIT_ASSERT_VALUES_EQUAL(poolGroups.size(), 1);
            groups.push_back(poolGroups.front()->GetGroupId());
        }
        const ui32 decommittedGroupId = groups[0];
        const ui32 regularGroupId = groups[1];

        TDataChecker checker(env);
        for (ui32 groupId : groups) {
            checker.Write(groupId);
        }
        checker.CheckAll();

        TGroupInfoTracker groupInfos; // latest group infos sent by BSC
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            return groupInfos.Filter(nodeId, ev);
        };

        auto checkDecommitStatus = [&](NKikimrBlobStorage::TGroupDecommitStatus::E expected) {
            const auto baseConfig = env.FetchBaseConfig();
            for (const auto& group : baseConfig.GetGroup()) {
                if (group.GetGroupId() == decommittedGroupId) {
                    const auto status = group.GetVirtualGroupInfo().GetDecommitStatus();
                    UNIT_ASSERT_C(status == expected, NKikimrBlobStorage::TGroupDecommitStatus::E_Name(status));
                    return;
                }
            }
            UNIT_FAIL("group not found");
        };

        {
            NKikimrBlobStorage::TConfigRequest request;
            auto *cmd = request.AddCommand()->MutableDecommitGroups();
            cmd->AddGroupIds(decommittedGroupId);
            cmd->SetDatabase(TStringBuilder() << '/' << env.Runtime->GetDomainsInfo()->GetDomain()->Name);
            for (auto kind : {NKikimrBlobDepot::TChannelKind::System, NKikimrBlobDepot::TChannelKind::Data}) {
                auto *prof = cmd->AddChannelProfiles();
                prof->SetStoragePoolName("pool_2");
                prof->SetChannelKind(kind);
                prof->SetCount(2);
            }
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }
        env.Sim(TDuration::Seconds(10));
        checkDecommitStatus(NKikimrBlobStorage::TGroupDecommitStatus::PENDING);
        checker.Write(decommittedGroupId);
        checker.CheckAll();

        RollingRestartWithSinglePileBridgeConfig(env, checker, regularGroupId);

        const auto res = ConvertToBridge(env);
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        env.Sim(TDuration::Minutes(1));

        // decommitted group is not converted, while the other one is, and conversion is still pending
        checkDecommitStatus(NKikimrBlobStorage::TGroupDecommitStatus::PENDING);
        UNIT_ASSERT(QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());
        const auto& decommitted = groupInfos.Get(decommittedGroupId);
        UNIT_ASSERT_C(!decommitted.HasBridgeGroupState(), decommitted.ShortDebugString());
        const auto& regular = groupInfos.Get(regularGroupId);
        UNIT_ASSERT_VALUES_EQUAL_C(regular.GetBridgeProxyGroupId(), regularGroupId, regular.ShortDebugString());
        for (ui32 groupId : groups) {
            checker.Write(groupId);
        }
        checker.CheckAll();

        // cancel the decommission; the group becomes ordinary one and gets converted, finishing the conversion
        {
            NKikimrBlobStorage::TConfigRequest request;
            request.AddCommand()->MutableCancelVirtualGroup()->SetGroupId(decommittedGroupId);
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }
        env.Sim(TDuration::Minutes(1));
        checkDecommitStatus(NKikimrBlobStorage::TGroupDecommitStatus::NONE);
        UNIT_ASSERT(!QueryStorageConfig(env).GetClusterStateDetails().GetBscConversionPending());
        const auto& info = groupInfos.Get(decommittedGroupId);
        UNIT_ASSERT_VALUES_EQUAL_C(info.GetBridgeProxyGroupId(), decommittedGroupId, info.ShortDebugString());
        UNIT_ASSERT_C(!info.HasBlobDepotId(), info.ShortDebugString());
        for (ui32 groupId : groups) {
            checker.Write(groupId);
        }
        checker.CheckAll();
    }

}
