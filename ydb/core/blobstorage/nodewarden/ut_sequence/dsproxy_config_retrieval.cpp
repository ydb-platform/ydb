#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
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
    TString path = "SectorMap:" + temp() + "static.dat";
    ui64 pdiskSize = 32ULL << 30;
    ui64 chunkSize = 32ULL << 20;
    ui64 guid = RandomNumber<ui64>();
    auto sectorMap = MakeIntrusive<NPDisk::TSectorMap>(pdiskSize);
    FormatPDisk(path, 0, 4096, chunkSize, guid, 0x1234567890 + 1, 0x4567890123 + 1, 0x7890123456 + 1,
        NPDisk::YdbDefaultPDiskSequence, TString(), false, false, sectorMap, false);

    // per-node NodeWarden configurations; node 0 has the static group and the BS_CONTROLLER tablet
    THashMap<ui32, NKikimrBlobStorage::TNodeWardenServiceSet> configs;
    auto& st = configs[0];
    auto& pdisk = *st.AddPDisks();
    pdisk.SetNodeID(runtime.GetNodeId(0));
    pdisk.SetPDiskID(1);
    pdisk.SetPath(path);
    pdisk.SetPDiskGuid(guid);
    pdisk.SetPDiskCategory(0);
    pdisk.MutablePDiskConfig()->SetExpectedSlotCount(2);
    auto& vdisk = *st.AddVDisks();
    auto& id = *vdisk.MutableVDiskID();
    id.SetGroupID(0);
    id.SetGroupGeneration(1);
    id.SetRing(0);
    id.SetDomain(0);
    id.SetVDisk(0);
    auto& loc = *vdisk.MutableVDiskLocation();
    loc.SetNodeID(pdisk.GetNodeID());
    loc.SetPDiskID(pdisk.GetPDiskID());
    loc.SetVDiskSlotID(1);
    loc.SetPDiskGuid(pdisk.GetPDiskGuid());
    vdisk.SetVDiskKind(NKikimrBlobStorage::TVDiskKind::Default);
    auto& g = *st.AddGroups();
    g.SetGroupID(0);
    g.SetGroupGeneration(1);
    g.SetErasureSpecies(0);
    auto& r = *g.AddRings();
    auto& d = *r.AddFailDomains();
    auto& l = *d.AddVDiskLocations();
    l.CopyFrom(loc);
    st.AddAvailabilityDomains(0);
    app.BSConf = st;

    for (ui32 i = 0; i < runtime.GetNodeCount(); ++i) {
        SetupStateStorage(runtime, i);
        auto config = MakeIntrusive<TNodeWardenConfig>(new TStrandedPDiskServiceFactory(runtime));
        config->SectorMaps[path] = sectorMap;
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

        ui32 nodeId = runtime.GetNodeId(0);
        Y_ABORT_UNLESS(nodesInfo->Nodes[0].NodeId == nodeId);
        auto& nodeInfo = nodesInfo->Nodes[0];

        auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto *r = ev->Record.MutableRequest();
        auto *hc = r->AddCommand()->MutableDefineHostConfig();
        hc->SetHostConfigId(1);
        auto *d = hc->AddDrive();
        d->SetPath(pdisk.GetPath());
        d->SetType(NKikimrBlobStorage::ROT);
        d->MutablePDiskConfig()->SetExpectedSlotCount(2);
        auto *db = r->AddCommand()->MutableDefineBox();
        db->SetBoxId(1);
        auto *h = db->AddHost();
        auto *hk = h->MutableKey();
        hk->SetFqdn(nodeInfo.Host);
        hk->SetIcPort(nodeInfo.Port);
        h->SetHostConfigId(hc->GetHostConfigId());
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

}
