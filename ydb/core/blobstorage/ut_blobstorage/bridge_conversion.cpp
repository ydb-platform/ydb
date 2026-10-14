#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>

namespace {

    // nodes of the cluster being converted have no pile names in their locations
    TNodeLocation GetLocation(ui32 nodeId) {
        NActorsInterconnect::TNodeLocation location;
        location.SetDataCenter("my_dc");
        location.SetRack(TStringBuilder() << "rack_" << nodeId);
        location.SetUnit(TStringBuilder() << "unit_" << nodeId);
        return TNodeLocation(location);
    }

    TEnvironmentSetup::TSettings MakeSettings() {
        return {
            .NodeCount = 9, // one spare node to make disk reassignment possible
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetLocation,
            .SelfManagementConfig = true,
            .AutomaticBootstrap = true,
        };
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

    // make all nodes bridge-capable with single pile by restarting them one by one
    void RollingRestartWithSinglePileBridgeConfig(TEnvironmentSetup& env, TDataChecker& checker, ui32 groupId) {
        auto& bridgeConfig = env.Runtime->GetAppDataBridgeConfig();
        UNIT_ASSERT_VALUES_EQUAL(bridgeConfig.PilesSize(), 0);
        bridgeConfig.AddPiles()->SetName("pile_1");

        for (ui32 nodeId : env.Runtime->GetNodes()) {
            Cerr << "*** restarting node " << nodeId << " with single-pile bridge config" << Endl;
            env.RestartNode(nodeId);
            env.Sim(TDuration::Seconds(30));
            checker.Write(groupId);
            checker.CheckAll();
        }
    }

    // define new storage pool with the same settings as the existing one
    void CreateAnotherPool(TEnvironmentSetup& env, ui64 storagePoolId) {
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

    // move one of the group's VDisks to another PDisk; this also bumps the group generation
    void ReassignOneDisk(TEnvironmentSetup& env, ui32 groupId) {
        const auto baseConfig = env.FetchBaseConfig();
        const NKikimrBlobStorage::TBaseConfig::TVSlot *target = nullptr;
        for (const auto& vslot : baseConfig.GetVSlot()) {
            if (vslot.GetGroupId() == groupId) {
                target = &vslot;
                break;
            }
        }
        UNIT_ASSERT(target);

        NKikimrBlobStorage::TConfigRequest request;
        auto *reassign = request.AddCommand()->MutableReassignGroupDisk();
        reassign->SetGroupId(groupId);
        reassign->SetGroupGeneration(target->GetGroupGeneration());
        reassign->SetFailRealmIdx(target->GetFailRealmIdx());
        reassign->SetFailDomainIdx(target->GetFailDomainIdx());
        reassign->SetVDiskIdx(target->GetVDiskIdx());
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    }

    // turn the group into an inline pile one: it becomes bridged group with single pile being the group itself
    void MakeInlinePile(NKikimrBlobStorage::TGroupInfo *group) {
        const auto groupId = TGroupId::FromProto(group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
        auto *state = group->MutableBridgeGroupState();
        state->ClearPile();
        auto *pile = state->AddPile();
        groupId.CopyToProto(pile, &NKikimrBridge::TGroupState::TPile::SetGroupId);
        pile->SetGroupGeneration(group->GetGroupGeneration());
        pile->SetStage(NKikimrBridge::TGroupState::SYNCED);
        groupId.CopyToProto(group, &NKikimrBlobStorage::TGroupInfo::SetBridgeProxyGroupId);
        TBridgePileId::FromPileIndex(0).CopyToProto(group, &NKikimrBlobStorage::TGroupInfo::SetBridgePileId);
    }

    std::vector<const NKikimrBlobStorage::TBaseConfig::TGroup*> GetPoolGroups(
            const NKikimrBlobStorage::TBaseConfig& baseConfig, ui64 storagePoolId) {
        std::vector<const NKikimrBlobStorage::TBaseConfig::TGroup*> res;
        for (const auto& group : baseConfig.GetGroup()) {
            if (group.GetStoragePoolId() == storagePoolId) {
                res.push_back(&group);
            }
        }
        return res;
    }

}

Y_UNIT_TEST_SUITE(BridgeConversion) {

    Y_UNIT_TEST(RollingRestartIntoBridgeCapable) {
        TEnvironmentSetup env(MakeSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));

        const auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const ui32 groupId = groups.front();

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
        TEnvironmentSetup env(MakeSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));

        const auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const ui32 groupId = groups.front();

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
                                MakeInlinePile(&group);
                            }
                        }
                    }
                    break;

                case TEvBlobStorage::EvUpdateGroupInfo:
                    if (auto *msg = ev->Get<TEvBlobStorage::TEvUpdateGroupInfo>(); converted &&
                            msg->GroupId.GetRawId() == groupId && msg->GroupInfo) {
                        MakeInlinePile(&*msg->GroupInfo);
                    }
                    break;

                case TEvBlobStorage::EvNodeWardenQueryCacheResult:
                    if (auto *msg = ev->Get<NStorage::TEvNodeWardenQueryCacheResult>(); converted &&
                            msg->Key == Sprintf("G%08" PRIx32, groupId) && msg->GenerationValue) {
                        auto& [generation, value] = *msg->GenerationValue;
                        NKikimrBlobStorage::TGroupInfo group;
                        UNIT_ASSERT(group.ParseFromString(value));
                        MakeInlinePile(&group);
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

}
