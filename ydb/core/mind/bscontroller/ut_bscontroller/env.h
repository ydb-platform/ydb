#pragma once

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/dsproxy/mock/dsproxy_mock.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/mind/bscontroller/impl.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr::NBsController {

class TInitialEventsFilter : TNonCopyable {
public:
    TTestActorRuntime::TEventFilter Prepare() {
        return [](TTestActorRuntimeBase& /*runtime*/, TAutoPtr<IEventHandle>& /*event*/) {
            return false;
        };
    }
};

struct TEnvironmentSetup {
    THolder<TTestBasicRuntime> Runtime;
    const ui32 NodeCount;
    const ui32 DataCenterCount;
    const ui32 Domain = 0;
    const ui64 TabletId = MakeBSControllerID();
    const TVector<ui64> TabletIds = {TabletId};
    const TDuration Timeout = TDuration::Seconds(30);
    const ui32 GroupId = 0;
    const ui32 NodeId = 0;
    ui64 NextHostConfigId = 1;
    TInitialEventsFilter InitialEventsFilter;

    using TNodeRecord = std::tuple<TString, i32, ui32>;
    using TPDiskDefinition = std::tuple<TString, NKikimrBlobStorage::EPDiskType, bool, bool, ui64>;
    using TPDiskRecord = std::tuple<ui32, TPDiskDefinition>;

    TSet<TPDiskRecord> ExpectedPDisks;

    TPDiskRecord ParsePDiskRecord(const NKikimrBlobStorage::TBaseConfig::TPDisk& pdisk) {
        return std::make_tuple(
            pdisk.GetNodeId(),
            std::make_tuple(
                pdisk.GetPath(),
                pdisk.GetType(),
                pdisk.GetSharedWithOs(),
                pdisk.GetReadCentric(),
                pdisk.GetKind()));
    }

    TSet<TPDiskRecord> ParsePDisks(const NKikimrBlobStorage::TBaseConfig& config) {
        TSet<TPDiskRecord> res;
        for (const NKikimrBlobStorage::TBaseConfig::TPDisk& pdisk : config.GetPDisk()) {
            res.insert(ParsePDiskRecord(pdisk));
        }
        return res;
    }

    TEnvironmentSetup(ui32 nodeCount, ui32 dataCenterCount)
        : NodeCount(nodeCount)
        , DataCenterCount(dataCenterCount)
    {
    }

    void Prepare(const TString& /*dispatchName*/, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone) {
        outActiveZone = false;
        SetupRuntime(NodeCount, DataCenterCount);
        SetupLogging();
        SetupStorage();
        setup(*Runtime);
        SetupTablet();
    }

    TTestActorRuntime::TEventFilter PrepareInitialEventsFilter() {
        return InitialEventsFilter.Prepare();
    }

    NKikimrBlobStorage::TConfigResponse Invoke(const NKikimrBlobStorage::TConfigRequest& request, bool selfHeal = false) {
        const TActorId self = Runtime->AllocateEdgeActor();
        auto ev = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        ev->Record.MutableRequest()->CopyFrom(request);
        ev->SelfHeal = selfHeal;
        Runtime->SendToPipe(TabletId, self, ev.Release(), NodeId, GetPipeConfigWithRetries());
        auto response = Runtime->GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(self);
        return response->Get()->Record.GetResponse();
    }

    void RegisterNode() {
        for (ui32 i = 1; i <= NodeCount; ++i) {
            ui32 nodeIndex;
            for (nodeIndex = 0; nodeIndex < Runtime->GetNodeCount(); ++nodeIndex) {
                if (Runtime->GetNodeId(nodeIndex) == i) {
                    break;
                }
            }
            if (nodeIndex != Runtime->GetNodeCount()) {
                const TActorId self = Runtime->AllocateEdgeActor(nodeIndex);
                auto ev = MakeHolder<TEvBlobStorage::TEvControllerRegisterNode>(i, TVector<ui32>{}, TVector<ui32>{}, TVector<NPDisk::TDriveData>{});
                Runtime->SendToPipe(TabletId, self, ev.Release(), nodeIndex, GetPipeConfigWithRetries());
                auto response = Runtime->GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(self);
            }
        }
    }


    NKikimrBlobStorage::TEvControllerSelectGroupsResult SelectGroups(const NKikimrBlobStorage::TEvControllerSelectGroups& request) {
        const TActorId self = Runtime->AllocateEdgeActor();
        auto ev = MakeHolder<TEvBlobStorage::TEvControllerSelectGroups>();
        ev->Record.MergeFrom(request);
        Runtime->SendToPipe(TabletId, self, ev.Release(), NodeId, GetPipeConfigWithRetries());
        auto response = Runtime->GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerSelectGroupsResult>(self);
        return response->Get()->Record;
    }

    TVector<std::tuple<TString, i32, ui32>> GetNodes() {
        const TActorId edge = Runtime->AllocateEdgeActor();
        Runtime->Send(new IEventHandle(GetNameserviceActorId(), edge, new TEvInterconnect::TEvListNodes));
        auto response = Runtime->GrabEdgeEventRethrow<TEvInterconnect::TEvNodesInfo>(edge);
        TVector<std::tuple<TString, i32, ui32>> res;
        for (const auto& nodeInfo : response->Get()->Nodes) {
            res.emplace_back(nodeInfo.Host, nodeInfo.Port, nodeInfo.NodeId);
        }
        return res;
    }

    void DefineBox(ui64 boxId, const TString& name, const TVector<TPDiskDefinition>& pdisks,
                   const TVector<TNodeRecord>& nodes, NKikimrBlobStorage::TConfigRequest& request,
                   const ui64 generation = 0) {
        auto& hostcfg = *request.AddCommand()->MutableDefineHostConfig();
        hostcfg.SetHostConfigId(NextHostConfigId++);
        for (const auto& pdisk : pdisks) {
            TString path;
            NKikimrBlobStorage::EPDiskType type;
            bool sharedWithOs, readCentric;
            ui64 kind;
            std::tie(path, type, sharedWithOs, readCentric, kind) = pdisk;
            auto& drive = *hostcfg.AddDrive();
            drive.SetPath(path);
            drive.SetType(type);
            drive.SetSharedWithOs(sharedWithOs);
            drive.SetReadCentric(readCentric);
            drive.SetKind(kind);
        }

        auto& box = *request.AddCommand()->MutableDefineBox();
        box.SetBoxId(boxId);
        box.SetName(name);
        box.SetItemConfigGeneration(generation);
        for (const auto& node : nodes) {
            TString fqdn;
            i32 icPort;
            ui32 nodeId;
            std::tie(fqdn, icPort, nodeId) = node;

            auto& host = *box.AddHost();
            host.SetHostConfigId(hostcfg.GetHostConfigId());
            auto& key = *host.MutableKey();
            key.SetFqdn(fqdn);
            key.SetIcPort(icPort);

            for (const auto& pdisk : pdisks) {
                ExpectedPDisks.emplace(nodeId, pdisk);
            }
        }
    }

    void DefineStoragePool(ui64 boxId, ui64 storagePoolId, const TString& name, ui32 numGroups,
                           TMaybe<NKikimrBlobStorage::EPDiskType> pdiskType, TMaybe<bool> sharedWithOs,
                           NKikimrBlobStorage::TConfigRequest& request, const TString& erasure = "block-4-2",
                           const ui64 generation = 0) {
        auto& cmd = *request.AddCommand()->MutableDefineStoragePool();
        cmd.SetBoxId(boxId);
        cmd.SetStoragePoolId(storagePoolId);
        cmd.SetName(name);
        cmd.SetErasureSpecies(erasure);
        cmd.SetVDiskKind("Default");
        cmd.SetNumGroups(numGroups);
        cmd.SetItemConfigGeneration(generation);
        if (pdiskType || sharedWithOs) {
            auto& filter = *cmd.AddPDiskFilter();
            if (pdiskType) {
                filter.AddProperty()->SetType(*pdiskType);
            }
            if (sharedWithOs) {
                filter.AddProperty()->SetSharedWithOs(*sharedWithOs);
            }
        }
    }

    void SetupRuntime(ui32 nodeCount, ui32 dataCenterCount) {
        Runtime = MakeHolder<TTestBasicRuntime>(nodeCount, dataCenterCount);

        TAppPrepare app;
        app.AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dc-1").Release());
        for (ui32 i = 0; i < nodeCount; ++i) {
            SetupStateStorage(*Runtime, i, true);
            SetupTabletResolver(*Runtime, i);
        }
        Runtime->Initialize(app.Unwrap());
    }

    void SetupLogging() {
        Runtime->SetLogPriority(NKikimrServices::BS_CONTROLLER, NLog::PRI_DEBUG);

        auto prio = NLog::PRI_ERROR;
        Runtime->SetLogPriority(NKikimrServices::TABLET_MAIN, prio);
        Runtime->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, prio);
        Runtime->SetLogPriority(NKikimrServices::BS_NODE, prio);
        Runtime->SetLogPriority(NKikimrServices::PIPE_CLIENT, prio);
        Runtime->SetLogPriority(NKikimrServices::PIPE_SERVER, prio);
        Runtime->SetLogPriority(NKikimrServices::TABLET_RESOLVER, prio);
        Runtime->SetLogPriority(NKikimrServices::STATESTORAGE, prio);
        Runtime->SetLogPriority(NKikimrServices::BOOTSTRAPPER, prio);
    }

    void DisableLogging() {
        Runtime->SetLogPriority(NKikimrServices::BS_CONTROLLER, NLog::PRI_ERROR);
    }

    void SetupStorage() {
        const TActorId proxyId = MakeBlobStorageProxyID(GroupId);
        Runtime->RegisterService(proxyId, Runtime->Register(CreateBlobStorageGroupProxyMockActor(TGroupId::FromValue(GroupId)), NodeId), NodeId);

        class TMock : public TActor<TMock> {
        public:
            TMock()
                : TActor(&TThis::StateFunc)
            {}

            void Handle(TEvNodeWardenQueryStorageConfig::TPtr ev) {
                Send(ev->Sender, new TEvNodeWardenStorageConfig(std::make_shared<NKikimrBlobStorage::TStorageConfig>(),
                    false, nullptr));
            }

            STATEFN(StateFunc) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvNodeWardenQueryStorageConfig, Handle);
                }
            }
        };

        Runtime->RegisterService(MakeBlobStorageNodeWardenID(Runtime->GetNodeId(NodeId)), Runtime->Register(new TMock, NodeId), NodeId);
    }

    void SetupTablet() {
        const TActorId bootstrapper = CreateTestBootstrapper(*Runtime,
                                                       CreateTestTabletInfo(TabletId, TTabletTypes::BSController, TErasureType::ErasureNone, GroupId),
                                                       &CreateFlatBsController, NodeId);
        Runtime->EnableScheduleForActor(bootstrapper);
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTablet::EvBoot);
            Runtime->DispatchEvents(options);
        }
    }

    void Finalize() {
        Runtime.Reset();
        ExpectedPDisks.clear();
    }
};

class TFinalizer {
    TEnvironmentSetup& Env;

public:
    TFinalizer(TEnvironmentSetup& env)
        : Env(env)
    {
    }

    ~TFinalizer() {
        Env.Finalize();
    }
};

} // namespace NKikimr::NBsController
