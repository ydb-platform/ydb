#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <util/system/compat.h>

using namespace NKikimr;
using namespace NActors;

void SetupLogging(TTestBasicRuntime& runtime) {
    runtime.SetLogPriority(NKikimrServices::BS_CONTROLLER, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_DEBUG);
}

void SetupServices(TTestBasicRuntime& runtime) {
    TAppPrepare app;

    app.ClearDomainsAndHive();
    auto dom = TDomainsInfo::TDomain::ConstructEmptyDomain("dom-1", 0);
    app.AddDomain(dom.Release());

    TTempDir temp;
    ui64 pdiskSize = 32ULL << 30;
    ui64 chunkSize = 32ULL << 20;
    TVector<TString> paths(runtime.GetNodeCount());
    TVector<ui64> guids(runtime.GetNodeCount());
    TVector<TIntrusivePtr<NPDisk::TSectorMap>> sectorMaps(runtime.GetNodeCount());

    for (ui32 i = 0; i < runtime.GetNodeCount(); ++i) {
        paths[i] = TStringBuilder() << "SectorMap:" << temp() << "node-" << i << ".dat";
        guids[i] = 0x9E3779B97F4A7C15ull + i;
        auto sectorMap = sectorMaps[i] = MakeIntrusive<NPDisk::TSectorMap>(pdiskSize);
        if (i == 0) {
            // Only format static pdisks, other should format themselves on startup
            FormatPDisk(paths[i], 0, 4096, chunkSize, guids[i], 0x1234567890 + 1, 0x4567890123 + 1, 0x7890123456 + 1,
                NPDisk::YdbDefaultPDiskSequence, TString(), false, false, sectorMap, false);
        } else {
            sectorMap->ZeroInit(1_MB / NKikimr::NPDisk::NSectorMap::SECTOR_SIZE);
        }
    }

    // per-node NodeWarden configurations; node 0 has the static group and the BS_CONTROLLER tablet
    THashMap<ui32, NKikimrBlobStorage::TNodeWardenServiceSet> configs;
    for (ui32 i = 0; i < runtime.GetNodeCount(); ++i) {
        auto& cfg = configs[i];
        cfg.AddAvailabilityDomains(0);
    }

    auto& st = configs[0];
    auto& staticVdisk = *st.AddVDisks();
    auto& staticPDisk = *st.AddPDisks();
    staticPDisk.SetNodeID(runtime.GetNodeId(0));
    staticPDisk.SetPDiskID(1);
    staticPDisk.SetPath(paths[0]);
    staticPDisk.SetPDiskGuid(guids[0]);
    staticPDisk.SetPDiskCategory(0);
    staticPDisk.MutablePDiskConfig()->SetExpectedSlotCount(2);
    auto& id = *staticVdisk.MutableVDiskID();
    id.SetGroupID(0);
    id.SetGroupGeneration(1);
    id.SetRing(0);
    id.SetDomain(0);
    id.SetVDisk(0);
    auto& loc = *staticVdisk.MutableVDiskLocation();
    loc.SetNodeID(staticPDisk.GetNodeID());
    loc.SetPDiskID(staticPDisk.GetPDiskID());
    loc.SetVDiskSlotID(1);
    loc.SetPDiskGuid(staticPDisk.GetPDiskGuid());
    staticVdisk.SetVDiskKind(NKikimrBlobStorage::TVDiskKind::Default);
    auto& g = *st.AddGroups();
    g.SetGroupID(0);
    g.SetGroupGeneration(1);
    g.SetErasureSpecies(0);
    auto& r = *g.AddRings();
    auto& d = *r.AddFailDomains();
    auto& l = *d.AddVDiskLocations();
    l.CopyFrom(loc);
    app.BSConf = st;

    for (ui32 i = 0; i < runtime.GetNodeCount(); ++i) {
        SetupStateStorage(runtime, i);
        auto config = MakeIntrusive<TNodeWardenConfig>(new TStrandedPDiskServiceFactory(runtime));
        config->SectorMaps[paths[i]] = sectorMaps[i];
        config->BlobStorageConfig.MutableServiceSet()->CopyFrom(configs[i]);
        SetupBSNodeWarden(runtime, i, config);
        SetupTabletResolver(runtime, i);
        SetupNodeWhiteboard(runtime, i);
    }

    runtime.Initialize(app.Unwrap());

    CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController), &CreateFlatBsController);

    // setup box and storage pool for testing
    {
        TActorId edge = runtime.AllocateEdgeActor();

        runtime.Send(new IEventHandle(GetNameserviceActorId(), edge, new TEvInterconnect::TEvListNodes));
        TAutoPtr<IEventHandle> handleNodesInfo;
        auto nodesInfo = runtime.GrabEdgeEventRethrow<TEvInterconnect::TEvNodesInfo>(handleNodesInfo);

        auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto *r = ev->Record.MutableRequest();

        THashMap<ui32, ui64> hostConfigIdByNodeId;
        for (ui32 i = 0; i < runtime.GetNodeCount(); ++i) {
            auto *hc = r->AddCommand()->MutableDefineHostConfig();
            const ui64 hostConfigId = i + 1;
            hc->SetHostConfigId(hostConfigId);
            auto *d = hc->AddDrive();
            d->SetPath(paths[i]);
            d->SetType(NKikimrBlobStorage::ROT);
            d->MutablePDiskConfig()->SetExpectedSlotCount(2);
            hostConfigIdByNodeId.emplace(runtime.GetNodeId(i), hostConfigId);
        }

        auto *db = r->AddCommand()->MutableDefineBox();
        db->SetBoxId(1);
        for (const auto& nodeInfo : nodesInfo->Nodes) {
            auto it = hostConfigIdByNodeId.find(nodeInfo.NodeId);
            if (it == hostConfigIdByNodeId.end()) {
                continue;
            }
            auto *h = db->AddHost();
            auto *hk = h->MutableKey();
            hk->SetFqdn(nodeInfo.Host);
            hk->SetIcPort(nodeInfo.Port);
            h->SetHostConfigId(it->second);
        }
        auto *ds = r->AddCommand()->MutableDefineStoragePool();
        ds->SetBoxId(db->GetBoxId());
        ds->SetStoragePoolId(1);
        ds->SetErasureSpecies("none");
        ds->SetVDiskKind("Default");
        ds->SetNumGroups(1);
        ds->AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::ROT);

        runtime.SendToPipe(MakeBSControllerID(), edge, ev.release());
        auto resp = runtime.GrabEdgeEvent<TEvBlobStorage::TEvControllerConfigResponse>(edge);
        const auto& record = resp->Get()->Record;
        UNIT_ASSERT(record.GetResponse().GetSuccess());
    }
}

void Setup(TTestBasicRuntime& runtime) {
    SetupLogging(runtime);
    SetupServices(runtime);
}

NKikimrBlobStorage::TBaseConfig QueryBaseConfig(TTestBasicRuntime& runtime) {
    TActorId edge = runtime.AllocateEdgeActor();
    auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
    ev->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
    runtime.SendToPipe(MakeBSControllerID(), edge, ev.release());
    auto resp = runtime.GrabEdgeEvent<TEvBlobStorage::TEvControllerConfigResponse>(edge);
    const auto& response = resp->Get()->Record.GetResponse();
    UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
    UNIT_ASSERT_VALUES_EQUAL(response.StatusSize(), 1);
    return response.GetStatus(0).GetBaseConfig();
}

void ReassignGroupDisk(TTestBasicRuntime& runtime, ui64 groupId, ui32 groupGeneration,
        ui32 targetNodeId, ui32 targetPDiskId) {
    TActorId edge = runtime.AllocateEdgeActor();
    auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
    auto *request = ev->Record.MutableRequest();
    request->SetIgnoreGroupFailModelChecks(true);
    request->SetIgnoreDegradedGroupsChecks(true);
    request->SetIgnoreDisintegratedGroupsChecks(true);
    auto *cmd = request->AddCommand()->MutableReassignGroupDisk();
    cmd->SetGroupId(groupId);
    cmd->SetGroupGeneration(groupGeneration);
    cmd->SetFailRealmIdx(0);
    cmd->SetFailDomainIdx(0);
    cmd->SetVDiskIdx(0);
    auto *targetPDisk = cmd->MutableTargetPDiskId();
    targetPDisk->SetNodeId(targetNodeId);
    targetPDisk->SetPDiskId(targetPDiskId);

    runtime.SendToPipe(MakeBSControllerID(), edge, ev.release());
    auto resp = runtime.GrabEdgeEvent<TEvBlobStorage::TEvControllerConfigResponse>(edge);
    const auto& response = resp->Get()->Record.GetResponse();
    UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
}

TMaybe<ui32> QueryNodeWardenGroupGeneration(TTestBasicRuntime& runtime, ui32 nodeIndex, ui32 nodeId, ui32 groupId,
        TDuration simTimeout = TDuration::Seconds(1)) {
    const TActorId edge = runtime.AllocateEdgeActor(nodeIndex);
    runtime.Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), edge, new TEvNodeWardenQueryGroupInfo(groupId)), nodeIndex);
    auto resp = runtime.GrabEdgeEvent<TEvNodeWardenGroupInfo>(edge, simTimeout);
    if (!resp) {
        return Nothing();
    }
    if (resp->Get()->Record.HasGroup()) {
        return resp->Get()->Record.GetGroup().GetGroupGeneration();
    }
    return Nothing();
}

void WaitForNodeWardenGroupGeneration(TTestBasicRuntime& runtime, ui32 nodeIndex, ui32 nodeId, ui32 groupId,
        ui32 expectedGeneration) {
    TDuration simTimeout = TDuration::Seconds(10);
    TDuration queryTimeout = TDuration::MilliSeconds(50);
    const TInstant deadline = runtime.GetCurrentTime() + simTimeout;
    TMaybe<ui32> generation;
    while (runtime.GetCurrentTime() < deadline) {
        generation = QueryNodeWardenGroupGeneration(runtime, nodeIndex, nodeId, groupId, queryTimeout);
        if (generation && *generation == expectedGeneration) {
            return;
        }
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
    }

    generation = QueryNodeWardenGroupGeneration(runtime, nodeIndex, nodeId, groupId, queryTimeout);
    UNIT_ASSERT_C(false, TStringBuilder() << "Timeout while waiting for owner local group info update"
        << " NodeId# " << nodeId
        << " GroupId# " << groupId
        << " ExpectedGeneration# " << expectedGeneration
        << " ActualGeneration# " << (generation ? TStringBuilder() << *generation : TString("<none>")));
}

Y_UNIT_TEST_SUITE(NodeWardenDsProxyConfigRetrieval) {

    Y_UNIT_TEST(Disconnect) {
        TTestBasicRuntime runtime(1);

        const ui32 groupId = 0x80000000;
        const ui64 tabletId = MakeBSControllerID();
        bool allowConfiguring = false;

        TActorId nodeWardenId;
        TTestActorRuntimeBase::TRegistrationObserver prevReg = runtime.SetRegistrationObserverFunc(
                [&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
            if (IActor *actor = runtime.FindActor(actorId); dynamic_cast<NKikimr::NStorage::TNodeWarden*>(actor)) {
                UNIT_ASSERT(!nodeWardenId);
                nodeWardenId = actorId;
                runtime.EnableScheduleForActor(actorId);
                Cerr << "Caught NodeWarden registration actorId# " << actorId << Endl;
            }
            return prevReg(runtime, parentId, actorId);
        });

        TActorId clientId;
        TTestActorRuntimeBase::TEventObserver prev = runtime.SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& ev) {
            if (auto *msg = ev->CastAsLocal<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>()) {
                for (const auto& group : msg->Record.GetServiceSet().GetGroups()) {
                    if (group.GetGroupID() == groupId && !allowConfiguring) {
                        return TTestActorRuntimeBase::EEventAction::DROP;
                    }
                }
            } else if (auto *msg = ev->CastAsLocal<TEvTabletPipe::TEvClientConnected>()) {
                if (ev->Recipient == nodeWardenId && msg->TabletId == tabletId && msg->Status == NKikimrProto::OK) {
                    Cerr << "Pipe connected clientId# " << msg->ClientId << Endl;
                    UNIT_ASSERT(!clientId);
                    clientId = msg->ClientId;
                }
            } else if (auto *msg = ev->CastAsLocal<TEvTabletPipe::TEvClientDestroyed>()) {
                if (ev->Recipient == nodeWardenId && msg->TabletId == tabletId) {
                    Cerr << "Pipe disconnected clientId# " << msg->ClientId << Endl;
                    UNIT_ASSERT_VALUES_EQUAL(clientId, msg->ClientId);
                    clientId = {};
                }
            }
            return prev(ev);
        });

        Setup(runtime);

        // wait for pipe to establish
        TDispatchOptions opts;
        opts.CustomFinalCondition = [&] { return !!clientId; };
        Cerr << "=== Waiting for pipe to establish ===" << Endl;
        runtime.DispatchEvents(opts);

        // trigger event
        const TActorId recip = MakeBlobStorageProxyID(groupId);
        const TActorId sender = runtime.AllocateEdgeActor(0);
        const TActorId warden = MakeBlobStorageNodeWardenID(runtime.GetNodeId(0));
        Cerr << "=== Breaking pipe ===" << Endl;
        runtime.Send(new IEventHandle(TEvents::TSystem::Poison, 0, clientId, TActorId(), {}, 0));
        Cerr << "=== Sending put ===" << Endl;
        runtime.Send(new IEventHandle(recip, sender, new TEvBlobStorage::TEvPut(TLogoBlobID(1, 1, 1, 1, 1, 1), "1", TInstant::Max()),
            IEventHandle::FlagForwardOnNondelivery, 0, &warden), 0, true);
        allowConfiguring = true;
        auto res = runtime.GrabEdgeEvent<TEvBlobStorage::TEvPutResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
    }

    Y_UNIT_TEST(LocalGroupInfoUpdatesAfterMovingOut) {
        // Regression scenario:
        // 1) a dynamic group is initially local for owner node;
        // 2) its VDisk is moved away from owner, owner still has last GroupInfo;
        // 3) owner nodewarden reconnects to BSC while proxy is not started locally;
        // 4) VDisk is moved again between non-owner nodes.
        // Original owner must continue receiving fresh generation updates after each reconnect.
        TTestBasicRuntime runtime(3);

        ui32 ownerNodeId = 0;
        ui32 ownerNodeIndex = 0;
        ui32 groupId = 0;
        ui32 oldGeneration = 0;
        ui32 newGeneration = 0;

        THashMap<TActorId, TActorId> connectedPipeClientByWarden;
        THashMap<ui32, TActorId> nodeWardenActorIdByNodeId;

        TTestActorRuntimeBase::TRegistrationObserver prevReg = runtime.SetRegistrationObserverFunc(
                [&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
            if (IActor *actor = runtime.FindActor(actorId); dynamic_cast<NKikimr::NStorage::TNodeWarden*>(actor)) {
                nodeWardenActorIdByNodeId[actorId.NodeId()] = actorId;
                runtime.EnableScheduleForActor(actorId);
            }
            return prevReg(runtime, parentId, actorId);
        });

        TTestActorRuntimeBase::TEventObserver prev = runtime.SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& ev) {
            if (auto *msg = ev->CastAsLocal<TEvTabletPipe::TEvClientConnected>()) {
                if (msg->TabletId == MakeBSControllerID() && msg->Status == NKikimrProto::OK) {
                    connectedPipeClientByWarden[ev->Recipient] = msg->ClientId;
                }
            }
            return prev(ev);
        });

        Setup(runtime);

        auto findNodeIndex = [&](ui32 nodeId) {
            for (ui32 i = 0; i < runtime.GetNodeCount(); ++i) {
                if (runtime.GetNodeId(i) == nodeId) {
                    return i;
                }
            }
            Y_ABORT("Node index not found");
        };

        auto baseConfig = QueryBaseConfig(runtime);

        bool foundGroup = false;
        for (const auto& group : baseConfig.GetGroup()) {
            if (TGroupID(group.GetGroupId()).ConfigurationType() != EGroupConfigurationType::Dynamic) {
                continue;
            }
            ui32 nodeId = 0;
            for (const auto& vslot : baseConfig.GetVSlot()) {
                if (vslot.GetGroupId() == group.GetGroupId() && vslot.HasVSlotId()) {
                    nodeId = vslot.GetVSlotId().GetNodeId();
                    break;
                }
            }
            if (nodeId) {
                groupId = group.GetGroupId();
                oldGeneration = group.GetGroupGeneration();
                ownerNodeId = nodeId;
                ownerNodeIndex = findNodeIndex(ownerNodeId);
                foundGroup = true;
                break;
            }
        }
        UNIT_ASSERT_C(foundGroup, "Failed to locate target dynamic group");

        runtime.WaitFor("owner node warden registration and initial BSC pipe connection", [&] {
            return nodeWardenActorIdByNodeId.contains(ownerNodeId) &&
                connectedPipeClientByWarden.contains(nodeWardenActorIdByNodeId.at(ownerNodeId));
        }, TDuration::Seconds(1));

        auto ownerGeneration = QueryNodeWardenGroupGeneration(runtime, ownerNodeIndex, ownerNodeId, groupId);
        UNIT_ASSERT_C(ownerGeneration, "Owner node warden has no group info");
        UNIT_ASSERT_VALUES_EQUAL(*ownerGeneration, oldGeneration);

        // Move the only group VDisk between non-owner nodes and bump group generation.
        // Owner node is expected to keep receiving generation updates via BSC subscription.
        auto moveVDisk = [&]() {
            auto baseConfig = QueryBaseConfig(runtime);

            ui32 currentNodeId = 0;

            for (const auto& vslot : baseConfig.GetVSlot()) {
                if (vslot.GetGroupId() == groupId) {
                    currentNodeId = vslot.GetVSlotId().GetNodeId();
                    break;
                }
            }

            ui32 destinationNodeId = 0;
            ui32 destinationPDiskId = 0;
            for (const auto& pdisk : baseConfig.GetPDisk()) {
                // Don't move to original node at all
                // And move to any other node.
                if (pdisk.GetNodeId() != ownerNodeId && pdisk.GetNodeId() != currentNodeId) {
                    destinationNodeId = pdisk.GetNodeId();
                    destinationPDiskId = pdisk.GetPDiskId();
                    break;
                }
            }
            UNIT_ASSERT_C(destinationNodeId && destinationPDiskId, "Failed to locate destination PDisk");
            // Move VDisk from the initial node to trigger group info change
            ReassignGroupDisk(runtime, groupId, oldGeneration, destinationNodeId, destinationPDiskId);
            baseConfig = QueryBaseConfig(runtime);
            for (const auto& group : baseConfig.GetGroup()) {
                if (group.GetGroupId() == groupId) {
                    newGeneration = group.GetGroupGeneration();
                    break;
                }
            }
            UNIT_ASSERT_C(newGeneration > oldGeneration, "Expected generation bump after reassign");
            oldGeneration = newGeneration;
        };

        moveVDisk();

        WaitForNodeWardenGroupGeneration(runtime, ownerNodeIndex, ownerNodeId, groupId, newGeneration);

        // Reconnect owner nodewarden several times while the group is already moved out from it.
        // Then move the VDisk again and verify owner still gets fresh generation updates.
        for (ui8 i = 0; i < 3; ++i) {
            // Force pipe disconnect to trigger RegisterNode
            const TActorId ownerWardenActorId = nodeWardenActorIdByNodeId.at(ownerNodeId);
            const TActorId ownerClientId = connectedPipeClientByWarden.at(ownerWardenActorId);
            runtime.Send(new IEventHandle(ownerWardenActorId, runtime.AllocateEdgeActor(ownerNodeIndex),
                new TEvTabletPipe::TEvClientDestroyed(MakeBSControllerID(), ownerClientId, {})), ownerNodeIndex, true);

            runtime.WaitFor("owner pipe reconnect after ClientDestroyed", [&] {
                return connectedPipeClientByWarden.contains(ownerWardenActorId) &&
                    connectedPipeClientByWarden.at(ownerWardenActorId) != ownerClientId;
            }, TDuration::Seconds(1));

            moveVDisk();

            WaitForNodeWardenGroupGeneration(runtime, ownerNodeIndex, ownerNodeId, groupId, newGeneration);
        }
    }

}
