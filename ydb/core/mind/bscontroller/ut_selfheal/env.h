#pragma once

#include "defs.h"

#include "node_warden_mock.h"
#include "timer_actor.h"
#include "events.h"
#include <ydb/core/base/blobstorage_common.h>

struct TEnvironmentSetup {
    std::unique_ptr<TTestActorSystem> Runtime;
    const ui32 NodeCount;
    const ui32 Domain = 0;
    const ui64 TabletId = MakeBSControllerID();
    const TDuration Timeout = TDuration::Seconds(30);
    const ui32 GroupId = 0;
    const ui32 NodeId = 1;
    ui64 NextHostConfigId = 1;
    TActorId TimerActor;

    const std::function<TNodeLocation(ui32)> LocationGenerator;

    TEnvironmentSetup(ui32 nodeCount, std::function<TNodeLocation(ui32)> locationGenerator = {})
        : NodeCount(nodeCount)
        , LocationGenerator(locationGenerator)
    {
        Initialize();
    }

    ~TEnvironmentSetup() {
        Cleanup();
    }

    std::unique_ptr<TTestActorSystem> MakeRuntime() {
        auto domainsInfo = MakeIntrusive<TDomainsInfo>();
        domainsInfo->AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dom", Domain).Release());
        return std::make_unique<TTestActorSystem>(NodeCount, NLog::PRI_ERROR, domainsInfo);
    }

    void Initialize() {
        Runtime = MakeRuntime();
        TimerActor = Runtime->Register(new TTimerActor, NodeId);
        SetupLogging();
        Runtime->Start();
        if (LocationGenerator) {
            Runtime->SetupTabletRuntime(LocationGenerator);
        } else {
            Runtime->SetupTabletRuntime();
        }
        SetupStorage();
        SetupTablet();
    }

    void Cleanup() {
        Runtime->Stop();
        Runtime.reset();
    }

    template<typename TEvent>
    TAutoPtr<TEventHandle<TEvent>> WaitForEdgeActorEvent(const TActorId& actorId, bool termOnCapture = true) {
        for (;;) {
            auto ev = Runtime->WaitForEdgeActorEvent({actorId});
            if (ev->GetTypeRewrite() == TEvent::EventType) {
                TAutoPtr<TEventHandle<TEvent>> res = reinterpret_cast<TEventHandle<TEvent>*>(ev.release());
                if (termOnCapture) {
                    Runtime->DestroyActor(actorId);
                }
                return res;
            }
        }
    }

    NKikimrBlobStorage::TConfigResponse Invoke(const NKikimrBlobStorage::TConfigRequest& request) {
        const TActorId self = Runtime->AllocateEdgeActor(NodeId);
        auto ev = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        ev->Record.MutableRequest()->CopyFrom(request);
        Runtime->SendToPipe(TabletId, self, ev.Release(), NodeId, TTestActorSystem::GetPipeConfigWithRetries());
        auto response = WaitForEdgeActorEvent<TEvBlobStorage::TEvControllerConfigResponse>(self);
        return response->Get()->Record.GetResponse();
    }

    std::map<ui32, std::tuple<TString, i32>> GetNodeMap() {
        const TActorId edge = Runtime->AllocateEdgeActor(NodeId);
        Runtime->Send(new IEventHandle(GetNameserviceActorId(), edge, new TEvInterconnect::TEvListNodes), NodeId);
        auto response = WaitForEdgeActorEvent<TEvInterconnect::TEvNodesInfo>(edge);
        std::map<ui32, std::tuple<TString, i32>> res;
        for (const auto& nodeInfo : response->Get()->Nodes) {
            res.emplace(nodeInfo.NodeId, std::tie(nodeInfo.Host, nodeInfo.Port));
        }
        return res;
    }

    struct TDrive {
        TString Path;
        NKikimrBlobStorage::EPDiskType Type = NKikimrBlobStorage::ROT;
        bool SharedWithOs = false, ReadCentric = false;
        ui64 Kind = 0;
    };

    struct TNodeRange {
        ui32 From;
        ui32 To;
    };

    void DefineBox(ui64 boxId, const TVector<TDrive>& drives, const TVector<TNodeRange>& nodes,
            NKikimrBlobStorage::TConfigRequest *request, const ui64 generation = 0) {
        auto *hostcfg = request->AddCommand()->MutableDefineHostConfig();
        hostcfg->SetHostConfigId(NextHostConfigId);
        for (const auto& d : drives) {
            auto *pb = hostcfg->AddDrive();
            pb->SetPath(d.Path);
            pb->SetType(d.Type);
            pb->SetSharedWithOs(d.SharedWithOs);
            pb->SetReadCentric(d.ReadCentric);
            pb->SetKind(d.Kind);
        }

        auto *box = request->AddCommand()->MutableDefineBox();
        box->SetBoxId(boxId);
        box->SetName(TStringBuilder() << "BoxId# " << boxId);
        box->SetItemConfigGeneration(generation);
        for (const auto& range : nodes) {
            for (ui32 nodeId = range.From; nodeId <= range.To; ++nodeId) {
                auto *host = box->AddHost();
                host->SetHostConfigId(NextHostConfigId);
                auto *key = host->MutableKey();
                key->SetNodeId(nodeId);
            }
        }

        ++NextHostConfigId;
    }

    void DefineStoragePool(ui64 boxId, ui64 storagePoolId, ui32 numGroups,
            std::optional<NKikimrBlobStorage::EPDiskType> pdiskType, std::optional<bool> sharedWithOs,
            NKikimrBlobStorage::TConfigRequest *request, const TString& erasure = "block-4-2",
            const ui64 generation = 0) {
        auto *cmd = request->AddCommand()->MutableDefineStoragePool();
        cmd->SetBoxId(boxId);
        cmd->SetStoragePoolId(storagePoolId);
        cmd->SetName(TStringBuilder() << "BoxId# " << boxId << " StoragePoolId# " << storagePoolId);
        cmd->SetErasureSpecies(erasure);
        cmd->SetVDiskKind("Default");
        cmd->SetNumGroups(numGroups);
        cmd->SetItemConfigGeneration(generation);
        auto *filter = cmd->AddPDiskFilter();
        if (pdiskType) {
            filter->AddProperty()->SetType(*pdiskType);
        }
        if (sharedWithOs) {
            filter->AddProperty()->SetSharedWithOs(*sharedWithOs);
        }
    }

    void QueryBaseConfig(NKikimrBlobStorage::TConfigRequest *request) {
        request->AddCommand()->MutableQueryBaseConfig();
    }

    void UpdateDriveStatus(const TPDiskId& pdiskId, NKikimrBlobStorage::EDriveStatus status,
            NKikimrBlobStorage::TConfigRequest *request) {
        auto *cmd = request->AddCommand()->MutableUpdateDriveStatus();
        cmd->MutableHostKey()->SetNodeId(pdiskId.NodeId);
        cmd->SetPDiskId(pdiskId.PDiskId);
        cmd->SetStatus(status);
    }

    void SetupLogging() {
//        Runtime->SetLogPriority(NKikimrServices::BS_CONTROLLER, NLog::PRI_DEBUG);
        Runtime->SetLogPriority(NKikimrServices::BS_SELFHEAL, NLog::PRI_DEBUG);
        Runtime->SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_DEBUG);

        auto prio = NLog::PRI_ERROR;
        Runtime->SetLogPriority(NKikimrServices::TABLET_MAIN, prio);
        Runtime->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, prio);
        Runtime->SetLogPriority(NKikimrServices::PIPE_CLIENT, prio);
        Runtime->SetLogPriority(NKikimrServices::PIPE_SERVER, prio);
        Runtime->SetLogPriority(NKikimrServices::TABLET_RESOLVER, prio);
        Runtime->SetLogPriority(NKikimrServices::STATESTORAGE, prio);
        Runtime->SetLogPriority(NKikimrServices::BOOTSTRAPPER, prio);
    }

    void SetupStorage() {
        const TActorId proxyId = MakeBlobStorageProxyID(GroupId);
        Runtime->RegisterService(proxyId, Runtime->Register(CreateBlobStorageGroupProxyMockActor(TGroupId::FromValue(GroupId)), NodeId));

        for (ui32 nodeId : Runtime->GetNodes()) {
            const TActorId wardenId = Runtime->Register(new TNodeWardenMock(nodeId, TabletId), nodeId);
            Runtime->RegisterService(MakeBlobStorageNodeWardenID(nodeId), wardenId);
        }
    }

    void SetupTablet() {
        Runtime->CreateTestBootstrapper(
            TTestActorSystem::CreateTestTabletInfo(TabletId, TTabletTypes::BSController, TErasureType::ErasureNone, GroupId, 4),
            &CreateFlatBsController,
            NodeId);

        bool working = true;
        Runtime->Sim([&] { return working; }, [&](IEventHandle& event) { working = event.GetTypeRewrite() != TEvTablet::EvBoot; });
    }

    void Sim(TDuration duration) {
        const auto end = Runtime->GetClock() + duration;
        Runtime->Sim([&] { return Runtime->GetClock() <= end; });
    }

    void WaitForNodeWardensToConnect() {
        std::vector<TActorId> edges;
        for (ui32 nodeId : Runtime->GetNodes()) {
            const TActorId wardenId = MakeBlobStorageNodeWardenID(nodeId);
            const TActorId edge = Runtime->AllocateEdgeActor(nodeId);
            Runtime->Send(new IEventHandle(wardenId, edge, new TEvCheckState(EState::CONNECTED)), nodeId);
            edges.push_back(edge);
        }
        for (TActorId edge : edges) {
            WaitForEdgeActorEvent<TEvDone>(edge);
        }
        Ctest << "All node wardens are connected to BSC" << Endl;
    }

    void Wait(TDuration timeout) {
        const TActorId edge = Runtime->AllocateEdgeActor(NodeId);
        Runtime->Send(new IEventHandle(TimerActor, edge, new TEvArmTimer(timeout)), NodeId);
        WaitForEdgeActorEvent<TEvents::TEvWakeup>(edge);
    }

};
