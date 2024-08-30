#include <ydb/core/base/tablet.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/dsproxy/mock/dsproxy_mock.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/mind/bscontroller/indir.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/mind/bscontroller/ut_helpers.h>
#include <ydb/core/protos/blobstorage_config.pb.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <util/datetime/cputimer.h>
#include <util/random/random.h>

#include <google/protobuf/text_format.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NBsController;

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

    NKikimrBlobStorage::TConfigResponse Invoke(const NKikimrBlobStorage::TConfigRequest& request) {
        const TActorId self = Runtime->AllocateEdgeActor();
        auto ev = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        ev->Record.MutableRequest()->CopyFrom(request);
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
                Send(ev->Sender, new TEvNodeWardenStorageConfig(NKikimrBlobStorage::TStorageConfig(), nullptr));
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

Y_UNIT_TEST_SUITE(BsControllerConfig) {
    Y_UNIT_TEST(Basic) {
        TEnvironmentSetup env(10, 1);
        RunTestWithReboots(env.TabletIds, [&] { return env.PrepareInitialEventsFilter(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone) {
                TFinalizer finalizer(env);
                env.Prepare(dispatchName, setup, outActiveZone);

                NKikimrBlobStorage::TConfigRequest request;
                NKikimrBlobStorage::TConfigResponse response = env.Invoke(request);
                UNIT_ASSERT(response.GetSuccess()); });
    }

    Y_UNIT_TEST(PDiskCreate) {
        TEnvironmentSetup env(10, 1);
        RunTestWithReboots(env.TabletIds, [&] { return env.PrepareInitialEventsFilter(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone) {
                TFinalizer finalizer(env);
                env.Prepare(dispatchName, setup, outActiveZone);

                NKikimrBlobStorage::TConfigRequest request;
                env.DefineBox(1, "test box", {
                        {"/dev/disk1", NKikimrBlobStorage::ROT, false, false, 0},
                        {"/dev/disk2", NKikimrBlobStorage::ROT, true,  false, 0},
                        {"/dev/disk3", NKikimrBlobStorage::SSD, false, false, 0},
                    }, env.GetNodes(), request);

                size_t baseConfigIndex = request.CommandSize();
                request.AddCommand()->MutableQueryBaseConfig();

                NKikimrBlobStorage::TConfigResponse response = env.Invoke(request);
                UNIT_ASSERT(response.GetSuccess());
                UNIT_ASSERT(env.ParsePDisks(response.GetStatus(baseConfigIndex).GetBaseConfig()) == env.ExpectedPDisks); });
    }

    Y_UNIT_TEST(ManyPDisksRestarts) {
        int nodes = 100;
        TEnvironmentSetup env(nodes, 1);
        RunTestWithReboots(env.TabletIds, [&] { return env.PrepareInitialEventsFilter(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone) {
                TFinalizer finalizer(env);
                env.Prepare(dispatchName, setup, outActiveZone);

                NKikimrBlobStorage::TConfigRequest request;
                TVector<TEnvironmentSetup::TPDiskDefinition> disks;

                int numRotDisks = 8;
                int numSsdDisks = 8;
                for (int i = 0; i < numRotDisks + numSsdDisks; ++i) {
                    TString path = TStringBuilder() << "/dev/disk" << i;
                    disks.emplace_back(path, i < numRotDisks ? NKikimrBlobStorage::ROT : NKikimrBlobStorage::SSD, false, false, 0);
                }
                env.DefineBox(1, "test box", disks, env.GetNodes(), request);

                env.DefineStoragePool(1, 1, "first storage pool", nodes * numRotDisks, NKikimrBlobStorage::ROT, {}, request);
                env.DefineStoragePool(1, 2, "first storage pool", nodes * numSsdDisks, NKikimrBlobStorage::SSD, {}, request);

                size_t baseConfigIndex = request.CommandSize();
                request.AddCommand()->MutableQueryBaseConfig();

                NKikimrBlobStorage::TConfigResponse response = env.Invoke(request);
                UNIT_ASSERT(response.GetSuccess());
                UNIT_ASSERT(env.ParsePDisks(response.GetStatus(baseConfigIndex).GetBaseConfig()) == env.ExpectedPDisks);
                env.RegisterNode();
        });
    }

    Y_UNIT_TEST(ExtendByCreatingSeparateBox) {
        const ui32 numNodes = 50;
        const ui32 numNodes1 = 20;
        const ui32 numNodes2 = numNodes - numNodes1;
        const ui32 numGroups1 = numNodes1 * 3;
        const ui32 numGroups2 = numNodes2 * 4;
        TEnvironmentSetup env(numNodes, 1);
        RunTestWithReboots(env.TabletIds, [&] { return env.PrepareInitialEventsFilter(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone) {
                TFinalizer finalizer(env);
                env.Prepare(dispatchName, setup, outActiveZone);

                TVector<TEnvironmentSetup::TNodeRecord> nodes1, nodes2;
                for (const auto& node : env.GetNodes()) {
                    (nodes1.size() < numNodes1 ? nodes1 : nodes2).push_back(node);
                }

                TSet<ui32> nodeIds1, nodeIds2;
                for (const auto& item : nodes1) {
                    nodeIds1.insert(std::get<2>(item));
                }
                for (const auto& item : nodes2) {
                    nodeIds2.insert(std::get<2>(item));
                }

                NKikimrBlobStorage::TConfigRequest request;
                env.DefineBox(1, "first box", {
                        {"/dev/disk1", NKikimrBlobStorage::ROT, false, false, 0},
                        {"/dev/disk2", NKikimrBlobStorage::ROT, true,  false, 0},
                        {"/dev/disk3", NKikimrBlobStorage::SSD, false, false, 0},
                    }, nodes1, request);
                env.DefineStoragePool(1, 1, "first storage pool", numGroups1, NKikimrBlobStorage::ROT, {}, request);

                size_t baseConfigIndex = request.CommandSize();
                request.AddCommand()->MutableQueryBaseConfig();

                NKikimrBlobStorage::TConfigResponse response = env.Invoke(request);
                UNIT_ASSERT(response.GetSuccess());

                TMap<ui32, ui32> groups1;
                {
                    const auto& baseConfig = response.GetStatus(baseConfigIndex).GetBaseConfig();
                    UNIT_ASSERT(env.ParsePDisks(baseConfig) == env.ExpectedPDisks);
                    for (const auto& vslot : baseConfig.GetVSlot()) {
                        UNIT_ASSERT(vslot.HasVSlotId());
                        UNIT_ASSERT(nodeIds1.count(vslot.GetVSlotId().GetNodeId()));
                        ++groups1[vslot.GetGroupId()];
                    }
                    UNIT_ASSERT_EQUAL(groups1.size(), numGroups1);
                    for (const auto& kv : groups1) {
                        UNIT_ASSERT_EQUAL(kv.second, 8);
                    }
                }

                request.Clear();
                env.DefineBox(2, "second box", {
                        {"/dev/disk1", NKikimrBlobStorage::ROT, false, false, 0},
                        {"/dev/disk2", NKikimrBlobStorage::ROT, true,  false, 0},
                        {"/dev/disk3", NKikimrBlobStorage::SSD, false, false, 0},
                    }, nodes2, request);
                env.DefineStoragePool(2, 1, "second storage pool", numGroups2, NKikimrBlobStorage::ROT, {}, request);

                baseConfigIndex = request.CommandSize();
                request.AddCommand()->MutableQueryBaseConfig();

                response = env.Invoke(request);
                UNIT_ASSERT(response.GetSuccess());

                TMap<ui32, ui32> groups2;
                {
                    const auto& baseConfig = response.GetStatus(baseConfigIndex).GetBaseConfig();
                    UNIT_ASSERT(env.ParsePDisks(baseConfig) == env.ExpectedPDisks);
                    for (const auto& vslot : baseConfig.GetVSlot()) {
                        UNIT_ASSERT(vslot.HasVSlotId());
                        if (groups1.count(vslot.GetGroupId())) {
                            UNIT_ASSERT(nodeIds1.count(vslot.GetVSlotId().GetNodeId()));
                        } else {
                            UNIT_ASSERT(nodeIds2.count(vslot.GetVSlotId().GetNodeId()));
                            ++groups2[vslot.GetGroupId()];
                        }
                    }
                    UNIT_ASSERT_EQUAL(groups2.size(), numGroups2);
                    for (const auto& kv : groups2) {
                        UNIT_ASSERT_EQUAL(kv.second, 8);
                    }
                } });
    }

    Y_UNIT_TEST(ExtendBoxAndStoragePool) {
        const ui32 totalNumNodes = 60;
        const ui32 originNumNodes = 50;
        const ui32 originNumGroups = originNumNodes * 3;
        const ui32 resultNumGroups = totalNumNodes * 3;
        TEnvironmentSetup env(totalNumNodes, 1);
        RunTestWithReboots(env.TabletIds, [&] { return env.PrepareInitialEventsFilter(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone) {
                TFinalizer finalizer(env);
                env.Prepare(dispatchName, setup, outActiveZone);

                TVector<TEnvironmentSetup::TNodeRecord> nodes = env.GetNodes(), part1, part2;

                for (auto &node : nodes) {
                    (part1.size() < originNumNodes ? part1 : part2).push_back(node);
                }

                // creating box of originNumNodes nodes
                NKikimrBlobStorage::TConfigRequest request;
                env.DefineBox(1, "first box", {
                        {"/dev/disk1", NKikimrBlobStorage::ROT, false, false, 0},
                        {"/dev/disk2", NKikimrBlobStorage::ROT, true,  false, 0},
                        {"/dev/disk3", NKikimrBlobStorage::SSD, false, false, 0},
                    }, part1, request);

                // creating storage pool of originNumGroups groups
                env.DefineStoragePool(1, 1, "first storage pool", originNumGroups, NKikimrBlobStorage::ROT, {}, request);

                // executing request
                size_t baseConfigIndex = request.CommandSize();
                request.AddCommand()->MutableQueryBaseConfig();
                NKikimrBlobStorage::TConfigResponse response = env.Invoke(request);
                UNIT_ASSERT(response.GetSuccess());

                // extending count of nodes in box: from originNumNodes to totalNumNodes
                request.Clear();
                env.DefineBox(1, "first box", {
                        {"/dev/disk1", NKikimrBlobStorage::ROT, false, false, 0},
                        {"/dev/disk2", NKikimrBlobStorage::ROT, true,  false, 0},
                        {"/dev/disk3", NKikimrBlobStorage::SSD, false, false, 0},
                    }, nodes, request, 1);

                // saving node ids
                TSet<ui32> nodeIds;
                for (const auto& item : nodes) {
                    nodeIds.insert(std::get<2>(item));
                }

                // extending count of groups in box: from originNumGroups to resultNumGroups
                env.DefineStoragePool(1, 1, "first storage pool", resultNumGroups, NKikimrBlobStorage::ROT, {},
                        request, "block-4-2", 1);

                // executing extention request
                baseConfigIndex = request.CommandSize();
                request.AddCommand()->MutableQueryBaseConfig();
                response = env.Invoke(request);
                UNIT_ASSERT(response.GetSuccess());

                // checking consequence
                TMap<ui32, ui32> groups;
                {
                    const auto& baseConfig = response.GetStatus(baseConfigIndex).GetBaseConfig();
                    UNIT_ASSERT(env.ParsePDisks(baseConfig) == env.ExpectedPDisks);
                    for (const auto& vslot : baseConfig.GetVSlot()) {
                        UNIT_ASSERT(vslot.HasVSlotId());
                        UNIT_ASSERT(nodeIds.count(vslot.GetVSlotId().GetNodeId()));
                        ++groups[vslot.GetGroupId()];
                    }
                    UNIT_ASSERT_EQUAL(groups.size(), resultNumGroups);
                    for (const auto& kv : groups) {
                        UNIT_ASSERT_EQUAL(kv.second, 8);
                    }
                } });
    }

    Y_UNIT_TEST(DeleteStoragePool) {
        const ui32 numNodes = 50;
        const ui32 numGroups = 50;
        TEnvironmentSetup env(numNodes, 1);
        RunTestWithReboots(env.TabletIds, [&] { return env.PrepareInitialEventsFilter(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone) {
            TFinalizer finalizer(env);
            env.Prepare(dispatchName, setup, outActiveZone);

            NKikimrBlobStorage::TConfigRequest request;
            NKikimrBlobStorage::TConfigResponse response;

            env.DefineBox(1, "box", {
                    {"/dev/disk1", NKikimrBlobStorage::ROT, false, false, 0},
                    {"/dev/disk2", NKikimrBlobStorage::ROT, true,  false, 0},
                    {"/dev/disk3", NKikimrBlobStorage::SSD, false, false, 0},
                }, env.GetNodes(), request);
            response = env.Invoke(request);
            UNIT_ASSERT(response.GetSuccess());
            request.Clear();

            env.DefineStoragePool(1, 1, "storage pool 1", numGroups, NKikimrBlobStorage::ROT, {}, request);
            response = env.Invoke(request);
            UNIT_ASSERT(response.GetSuccess());
            request.Clear();

            env.DefineStoragePool(1, 2, "storage pool 2", numGroups, NKikimrBlobStorage::SSD, {}, request);
            auto *m = request.AddCommand()->MutableDeleteStoragePool();
            m->SetBoxId(1);
            m->SetStoragePoolId(2);
            m->SetItemConfigGeneration(1);
            response = env.Invoke(request);
            UNIT_ASSERT(response.GetSuccess());
            request.Clear();

            m = request.AddCommand()->MutableDeleteStoragePool();
            m->SetBoxId(1);
            m->SetStoragePoolId(1);
            m->SetItemConfigGeneration(1);
            request.AddCommand()->MutableQueryBaseConfig();
            response = env.Invoke(request);
            UNIT_ASSERT(response.GetSuccess());
            const auto& baseConfig = response.GetStatus(1).GetBaseConfig();
            UNIT_ASSERT(baseConfig.GetGroup().empty());
            UNIT_ASSERT(baseConfig.GetVSlot().empty());
        });
    }

    Y_UNIT_TEST(ReassignGroupDisk) {
        const ui32 numNodes = 12;
        const ui32 numGroups = 8;
        TEnvironmentSetup env(numNodes, 1);

        RunTestWithReboots(env.TabletIds, [&] { return env.PrepareInitialEventsFilter(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone) {
            TFinalizer finalizer(env);
            env.Prepare(dispatchName, setup, outActiveZone);

            NKikimrBlobStorage::TConfigRequest request;
            NKikimrBlobStorage::TConfigResponse response;

            auto invoke = [&] {
                response = env.Invoke(std::exchange(request, {}));
                auto fmt = [&] {
                    google::protobuf::TextFormat::Printer p;
                    p.SetSingleLineMode(true);
                    TString s;
                    p.PrintToString(response, &s);
                    return s;
                };
                Cerr << Sprintf("Response# %s", fmt().data());
            };

            env.DefineBox(1, "box", {{"/dev/disk", NKikimrBlobStorage::ROT, false, false, 0}}, env.GetNodes(), request);
            env.DefineStoragePool(1, 1, "storage pool", numGroups, NKikimrBlobStorage::ROT, {}, request);

            invoke();

            auto *cmd = request.AddCommand()->MutableUpdateDriveStatus();
            cmd->MutableHostKey()->SetNodeId(1);
            cmd->SetPath("/dev/disk");
            cmd->SetStatus(NKikimrBlobStorage::INACTIVE);

            invoke();

            auto *cmd2 = request.AddCommand()->MutableReassignGroupDisk();
            cmd2->SetGroupId(2147483649);
            cmd2->SetGroupGeneration(1);
            cmd2->SetFailDomainIdx(3);
        });
    }

    Y_UNIT_TEST(MergeBoxes) {
        const ui32 numNodes = 50;
        const ui32 numNodes1 = 20;
        const ui32 numNodes2 = numNodes - numNodes1;
        const ui32 numGroups1 = numNodes1 * 3;
        const ui32 numGroups2 = numNodes2 * 4;
        TEnvironmentSetup env(numNodes, 1);
        RunTestWithReboots(env.TabletIds, [&] { return env.PrepareInitialEventsFilter(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone) {
                TFinalizer finalizer(env);
                env.Prepare(dispatchName, setup, outActiveZone);

                TVector<TEnvironmentSetup::TNodeRecord> nodes1, nodes2;
                for (const auto& node : env.GetNodes()) {
                    (nodes1.size() < numNodes1 ? nodes1 : nodes2).push_back(node);
                }

                TSet<ui32> nodeIds1, nodeIds2;
                for (const auto& item : nodes1) {
                    nodeIds1.insert(std::get<2>(item));
                }
                for (const auto& item : nodes2) {
                    nodeIds2.insert(std::get<2>(item));
                }

                NKikimrBlobStorage::TConfigRequest request;
                env.DefineBox(1, "first box", {
                        {"/dev/disk1", NKikimrBlobStorage::ROT, false, false, 0},
                        {"/dev/disk2", NKikimrBlobStorage::ROT, true,  false, 0},
                        {"/dev/disk3", NKikimrBlobStorage::SSD, false, false, 0},
                    }, nodes1, request);
                env.DefineStoragePool(1, 1, "first storage pool", numGroups1, NKikimrBlobStorage::ROT, {}, request);

                NKikimrBlobStorage::TConfigResponse response = env.Invoke(request);
                UNIT_ASSERT(response.GetSuccess());

                request.Clear();
                env.DefineBox(2, "second box", {
                        {"/dev/disk1", NKikimrBlobStorage::ROT, false, false, 0},
                        {"/dev/disk2", NKikimrBlobStorage::ROT, true,  false, 0},
                        {"/dev/disk3", NKikimrBlobStorage::SSD, false, false, 0},
                    }, nodes2, request);
                env.DefineStoragePool(2, 1, "second storage pool", numGroups2, NKikimrBlobStorage::ROT, {}, request);

                response = env.Invoke(request);
                UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

                ////////////////////////////////////////////////////////////////////////////////////////////////////////
                // merge boxes

                request.Clear();
                auto& cmd = *request.AddCommand()->MutableMergeBoxes();
                cmd.SetOriginBoxId(2);
                cmd.SetOriginBoxGeneration(1);
                cmd.SetTargetBoxId(1);
                cmd.SetTargetBoxGeneration(1);
                auto& item = *cmd.AddStoragePoolIdMap();
                item.SetOriginStoragePoolId(1);
                item.SetTargetStoragePoolId(2);

                response = env.Invoke(request);
                UNIT_ASSERT(response.GetSuccess());

                ////////////////////////////////////////////////////////////////////////////////////////////////////////
                // validate result

                request.Clear();
                {
                    auto& cmd = *request.AddCommand()->MutableReadBox();
                    cmd.AddBoxId(1);
                }

                request.AddCommand()->MutableQueryBaseConfig();

                response = env.Invoke(request);
                UNIT_ASSERT(response.GetSuccess());

                UNIT_ASSERT(response.GetStatus(0).GetSuccess());

                // check box nodes
                TSet<std::tuple<TString, i32>> nodes;
                for (const auto& node : env.GetNodes()) {
                    nodes.emplace(std::get<0>(node), std::get<1>(node));
                }
                UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(0).BoxSize(), 1);
                for (const auto& host : response.GetStatus(0).GetBox(0).GetHost()) {
                    const auto& key = host.GetKey();
                    const ui32 erased = nodes.erase(std::make_tuple(key.GetFqdn(), key.GetIcPort()));
                    UNIT_ASSERT_VALUES_EQUAL(erased, 1);
                }
                UNIT_ASSERT(nodes.empty());

                // validate base config
                ui32 num1 = 0, num2 = 0;
                {
                    const auto& baseConfig = response.GetStatus(1).GetBaseConfig();
                    for (const auto& pdisk : baseConfig.GetPDisk()) {
                        UNIT_ASSERT_VALUES_EQUAL(pdisk.GetBoxId(), 1);
                    }
                    for (const auto& group : baseConfig.GetGroup()) {
                        UNIT_ASSERT_VALUES_EQUAL(group.GetBoxId(), 1);
                        const ui64 storagePoolId = group.GetStoragePoolId();
                        UNIT_ASSERT(storagePoolId == 1 || storagePoolId == 2);
                        ++(storagePoolId == 1 ? num1 : num2);
                    }
                    UNIT_ASSERT_VALUES_EQUAL(num1, numGroups1);
                    UNIT_ASSERT_VALUES_EQUAL(num2, numGroups2);
                }

        });
    }

    Y_UNIT_TEST(MoveGroups) {
        const ui32 numNodes = 50;
        const ui32 numGroups1 = 100;
        const ui32 numGroups2 = 50;
        TEnvironmentSetup env(numNodes, 1);
        RunTestWithReboots(env.TabletIds, [&] { return env.PrepareInitialEventsFilter(); }, [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone) {
            TFinalizer finalizer(env);
            env.Prepare(dispatchName, setup, outActiveZone);

            NKikimrBlobStorage::TConfigRequest request;
            env.DefineBox(1, "first box", {
                    {"/dev/disk1", NKikimrBlobStorage::ROT, false, false, 0},
                    {"/dev/disk2", NKikimrBlobStorage::ROT, true,  false, 0},
                    {"/dev/disk3", NKikimrBlobStorage::SSD, false, false, 0},
                }, env.GetNodes(), request);
            env.DefineStoragePool(1, 1, "first storage pool", numGroups1, NKikimrBlobStorage::ROT, {}, request);
            env.DefineStoragePool(1, 2, "second storage pool", numGroups2, NKikimrBlobStorage::SSD, {}, request);

            NKikimrBlobStorage::TConfigResponse response = env.Invoke(request);
            UNIT_ASSERT(response.GetSuccess());

            auto check = [](const auto& proto) {
                google::protobuf::TextFormat::Printer p;
                p.SetSingleLineMode(true);
                TString buffer;
                p.PrintToString(proto, &buffer);
                UNIT_ASSERT_C(proto.GetSuccess(), buffer);
            };

            auto getGroupMapping = [&] {
                request.Clear();
                request.AddCommand()->MutableQueryBaseConfig();
                response = env.Invoke(request);
                check(response);
                const auto& base = response.GetStatus(0);
                check(base);
                const auto& config = base.GetBaseConfig();
                TMap<std::pair<ui64, ui64>, TSet<ui32>> groups;
                for (const auto& group : config.GetGroup()) {
                    groups[std::make_pair(group.GetBoxId(), group.GetStoragePoolId())].insert(group.GetGroupId());
                }
                return groups;
            };

            auto reference = getGroupMapping();

            auto& a = reference[std::make_pair(1, 1)];
            auto& b = reference[std::make_pair(1, 2)];

            UNIT_ASSERT_VALUES_EQUAL(a.size(), numGroups1);
            UNIT_ASSERT_VALUES_EQUAL(b.size(), numGroups2);

            request.Clear();
            auto *pb = request.AddCommand()->MutableMoveGroups();
            pb->SetBoxId(1);
            pb->SetOriginStoragePoolId(2);
            pb->SetOriginStoragePoolGeneration(1);
            pb->SetTargetStoragePoolId(1);
            pb->SetTargetStoragePoolGeneration(1);
            pb->AddExplicitGroupId(*b.begin());
            response = env.Invoke(request);
            check(response);
            a.insert(*b.begin());
            b.erase(b.begin());
            UNIT_ASSERT_VALUES_EQUAL(reference, getGroupMapping());

            request.Clear();
            pb = request.AddCommand()->MutableMoveGroups();
            pb->SetBoxId(1);
            pb->SetOriginStoragePoolId(2);
            pb->SetOriginStoragePoolGeneration(2);
            pb->SetTargetStoragePoolId(1);
            pb->SetTargetStoragePoolGeneration(2);
            pb->AddExplicitGroupId(*b.begin());
            response = env.Invoke(request);
            check(response);
            a.insert(*b.begin());
            b.erase(b.begin());
            UNIT_ASSERT_VALUES_EQUAL(reference, getGroupMapping());

            request.Clear();
            pb = request.AddCommand()->MutableMoveGroups();
            pb->SetBoxId(1);
            pb->SetOriginStoragePoolId(2);
            pb->SetOriginStoragePoolGeneration(3);
            pb->SetTargetStoragePoolId(1);
            pb->SetTargetStoragePoolGeneration(3);
            response = env.Invoke(request);
            check(response);
            a.merge(std::move(b));
            reference.erase(std::make_pair(1, 2));
            UNIT_ASSERT_VALUES_EQUAL(reference, getGroupMapping());
        });
    }

    Y_UNIT_TEST(SelectAllGroups) {
        const int numGroups = 80;
        TEnvironmentSetup env(10, 1);
        bool activeZone;
        env.Prepare("", [](TTestActorRuntime&) {}, activeZone);
        env.DisableLogging();

        NKikimrBlobStorage::TConfigRequest request;
        env.DefineBox(1, "test box", {
                                         {"/dev/disk1", NKikimrBlobStorage::ROT, false, false, 0},
                                         {"/dev/disk2", NKikimrBlobStorage::ROT, false, false, 0},
                                         {"/dev/disk3", NKikimrBlobStorage::ROT, false, false, 0},
                                         {"/dev/disk4", NKikimrBlobStorage::ROT, false, false, 0},
                                         {"/dev/disk5", NKikimrBlobStorage::ROT, false, false, 0},
                                         {"/dev/disk6", NKikimrBlobStorage::ROT, false, false, 0},
                                         {"/dev/disk7", NKikimrBlobStorage::ROT, false, false, 0},
                                         {"/dev/disk8", NKikimrBlobStorage::ROT, false, false, 0},
                                     },
                      env.GetNodes(), request);

        env.DefineStoragePool(1, 1, "test pool", numGroups, NKikimrBlobStorage::ROT, false, request);

        NKikimrBlobStorage::TConfigResponse response = env.Invoke(request);
        UNIT_ASSERT(response.GetSuccess());

        {
            NKikimrBlobStorage::TEvControllerSelectGroups request;
            request.SetReturnAllMatchingGroups(true);

            auto* p = request.AddGroupParameters();
            p->SetErasureSpecies(TBlobStorageGroupType::Erasure4Plus2Block);
            p->SetDesiredPDiskCategory(0);
            p->SetDesiredVDiskCategory(0);

            NKikimrBlobStorage::TEvControllerSelectGroupsResult res = env.SelectGroups(request);
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res.MatchingGroupsSize(), 1);
            THashSet<ui32> groupIds;
            for (const auto& item : res.GetMatchingGroups(0).GetGroups()) {
                UNIT_ASSERT(groupIds.insert(item.GetGroupID()).second);
            }
            UNIT_ASSERT_VALUES_EQUAL(groupIds.size(), numGroups);
        }
    }

    Y_UNIT_TEST(AddDriveSerial) {
        TEnvironmentSetup env(10, 1);
        auto test = [&] (const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone) {
            TFinalizer finalizer(env);
            env.Prepare(dispatchName, setup, outActiveZone);

            for (int i = 0; i < 3; ++i) {
                NKikimrBlobStorage::TConfigRequest request;
                auto pb = request.AddCommand()->MutableAddDriveSerial();
                pb->SetSerial("SN_123");
                pb->SetBoxId(1);
                NKikimrBlobStorage::TConfigResponse response = env.Invoke(request);
                UNIT_ASSERT(!response.GetSuccess());
                UNIT_ASSERT(response.StatusSize() == 1);
                UNIT_ASSERT(!response.GetStatus(0).GetSuccess());
                UNIT_ASSERT(response.GetStatus(0).GetErrorDescription());
            }
        };
        RunTestWithReboots(env.TabletIds, [&] { return env.PrepareInitialEventsFilter(); }, test);
    }

    Y_UNIT_TEST(AddDriveSerialMassive) {
        TEnvironmentSetup env(10, 1);
        auto test = [&] (const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone) {
            TFinalizer finalizer(env);
            env.Prepare(dispatchName, setup, outActiveZone);

            const size_t disksCount = 10;
            for (size_t i = 0; i < disksCount; ++i) {
                NKikimrBlobStorage::TConfigRequest request;
                auto pb = request.AddCommand()->MutableAddDriveSerial();
                pb->SetSerial(TStringBuilder() << "SN_" << i);
                pb->SetBoxId(1);
                NKikimrBlobStorage::TConfigResponse response = env.Invoke(request);
                UNIT_ASSERT(!response.GetSuccess());
                UNIT_ASSERT(response.StatusSize() == 1);
                UNIT_ASSERT(!response.GetStatus(0).GetSuccess());
                UNIT_ASSERT(response.GetStatus(0).GetErrorDescription());
            }
            for (size_t i = 0; i < disksCount; ++i) {
                NKikimrBlobStorage::TConfigRequest request;
                auto pb = request.AddCommand()->MutableRemoveDriveSerial();
                pb->SetSerial(TStringBuilder() << "SN_" << i);
                NKikimrBlobStorage::TConfigResponse response = env.Invoke(request);
                UNIT_ASSERT(!response.GetSuccess());
                UNIT_ASSERT(response.StatusSize() == 1);
                UNIT_ASSERT(!response.GetStatus(0).GetSuccess());
                UNIT_ASSERT(response.GetStatus(0).GetErrorDescription());
            }
        };
        RunTestWithReboots(env.TabletIds, [&] { return env.PrepareInitialEventsFilter(); }, test);
    }

    Y_UNIT_TEST(OverlayMap) {
        for (ui32 iter = 0; iter < 100; ++iter) {
            struct TItem {
                ui32 Value;

                TItem() = default;
                TItem(TItem&&) = default;
                TItem(const TItem&) = default;

                TItem(ui32 value)
                    : Value(value)
                {}

                bool operator ==(const TItem& other) const {
                    return Value == other.Value;
                }

                void OnClone(const THolder<TItem>&) {}
                void OnCommit(...) {}
                void OnRollback(...) {}
            };

            TMap<ui32, THolder<TItem>> base, reference;

            for (ui32 i = 0; i < 1000; ++i) {
                ui32 index = RandomNumber<ui32>(1000);
                base[index] = MakeHolder<TItem>(i);
                reference[index] = MakeHolder<TItem>(i);
                Ctest << "initial " << index << " -> " << i << Endl;
            }

            TOverlayMap<ui32, TItem> overlay(base);

            for (ui32 i = 0; i < 100; ++i) {
                bool deleteSomething = reference && RandomNumber<ui32>(100) < 20;
                if (deleteSomething) {
                    const ui32 index = RandomNumber(reference.size());
                    auto it = reference.begin();
                    std::advance(it, index);
                    Ctest << "deleting " << it->first << Endl;
                    overlay.DeleteExistingEntry(it->first);
                    reference.erase(it);
                } else {
                    const ui32 index = RandomNumber<ui32>(1000);
                    const ui32 value = RandomNumber<ui32>();
                    if (reference.count(index)) {
                        Ctest << "updating " << index << " -> " << value << Endl;
                        TItem *valp = overlay.FindForUpdate(index);
                        Y_ABORT_UNLESS(valp);
                        valp->Value = value;
                    } else {
                        Ctest << "inserting " << index << " -> " << value << Endl;
                        overlay.ConstructInplaceNewEntry(index, value);
                    }
                    reference[index] = MakeHolder<TItem>(value);
                }
            }

            TMap<ui32, TItem> generated;
            overlay.ForEach([&](ui32 key, const TItem& value) {
                const bool inserted = generated.emplace(key, value).second;
                UNIT_ASSERT(inserted);
            });

            auto downgradeMap = [](const TMap<ui32, THolder<TItem>>& m) {
                TMap<ui32, TItem> res;
                for (auto&& [key, value] : m) {
                    UNIT_ASSERT(value);
                    res.emplace(key, *value);
                }
                return res;
            };

            UNIT_ASSERT_EQUAL(generated, downgradeMap(reference));

            overlay.Commit();

            UNIT_ASSERT_EQUAL(downgradeMap(base), downgradeMap(reference));
        }
    }

    struct TBeta;

    struct TAlpha : TIndirectReferable<TAlpha> {
        const unsigned Key;
        unsigned Value = 0;
        TMap<unsigned, TIndirectReferable<TBeta>::TPtr> BetaRefs;

        TAlpha(unsigned key) : Key(key) {}
        TAlpha(const TAlpha&) = default;

        void OnCommit();
    };

    struct TBeta : TIndirectReferable<TBeta> {
        const unsigned Key;
        unsigned Value = 0;
        TMap<unsigned, TIndirectReferable<TAlpha>::TPtr> AlphaRefs;

        TBeta(unsigned key) : Key(key) {}
        TBeta(const TBeta&) = default;

        void OnCommit();
    };

    void TAlpha::OnCommit() {
        for (const auto& [key, value] : BetaRefs) {
            value.Mutable().AlphaRefs[Key] = this;
        }
    }

    void TBeta::OnCommit() {
        for (const auto& [key, value] : AlphaRefs) {
            value.Mutable().BetaRefs[Key] = this;
        }
    }

    Y_UNIT_TEST(OverlayMapCrossReferences) {
        struct TState {
            TOverlayMap<unsigned, TAlpha> Alphas;
            TOverlayMap<unsigned, TBeta> Betas;

            TState(TMap<unsigned, THolder<TAlpha>>& alphas, TMap<unsigned, THolder<TBeta>>& betas)
                : Alphas(alphas)
                , Betas(betas)
            {}
        };

        for (int iter = 0; iter < 100; ++iter) {
            Ctest << "Next iteration\n";
            const unsigned num = 1000;
            TMap<unsigned, THolder<TAlpha>> alphas;
            TMap<unsigned, THolder<TBeta>> betas;
            for (unsigned key = 0; key < num; ++key) {
                alphas.emplace(key, MakeHolder<TAlpha>(key));
                betas.emplace(key, MakeHolder<TBeta>(key));
                Ctest << Sprintf("Alpha[%u]# %p\n", key, alphas[key].Get());
                Ctest << Sprintf("Beta[%u]# %p\n", key, alphas[key].Get());
            }
            for (int i = 0; i < 1000; ++i) {
                const unsigned a = RandomNumber(num);
                const unsigned b = RandomNumber(num);
                alphas[a]->BetaRefs[b] = betas[b].Get();
                betas[b]->AlphaRefs[a] = alphas[a].Get();
            }

            TState state(alphas, betas);

            THashSet<unsigned> alphaKeys, betaKeys;
            for (int i = 0; i < 100; ++i) {
                alphaKeys.insert(RandomNumber(num));
                betaKeys.insert(RandomNumber(num));
            }

            for (unsigned key : alphaKeys) {
                Ctest << Sprintf("Alphas.FindForUpdate Key# %u\n", key);
                ++state.Alphas.FindForUpdate(key)->Value;
            }
            for (unsigned key : betaKeys) {
                Ctest << Sprintf("Betas.FindForUpdate Key# %u\n", key);
                ++state.Betas.FindForUpdate(key)->Value;
            }

            for (unsigned key = 0; key < num; ++key) {
                UNIT_ASSERT_VALUES_EQUAL(key, state.Alphas.Find(key)->Key);
                UNIT_ASSERT_VALUES_EQUAL(key, state.Betas.Find(key)->Key);

                const TAlpha *alpha = state.Alphas.Find(key);
                UNIT_ASSERT_VALUES_EQUAL(alpha->Value, alphaKeys.count(key));
                for (const auto& [key, value] : alpha->BetaRefs) {
                    UNIT_ASSERT_VALUES_EQUAL(Sprintf("%p", (const TBeta*)value), Sprintf("%p", state.Betas.Find(key)));
                }

                const TBeta *beta = state.Betas.Find(key);
                UNIT_ASSERT_VALUES_EQUAL(beta->Value, betaKeys.count(key));
                for (const auto& [key, value] : beta->AlphaRefs) {
                    UNIT_ASSERT_VALUES_EQUAL(Sprintf("%p", (const TAlpha*)value), Sprintf("%p", state.Alphas.Find(key)));
                }
            }

            state.Alphas.Commit();
            state.Betas.Commit();

            for (unsigned key = 0; key < num; ++key) {
                const TAlpha& alpha = *alphas.at(key);
                UNIT_ASSERT_VALUES_EQUAL(alpha.Value, alphaKeys.count(key));
                for (const auto& [key, value] : alpha.BetaRefs) {
                    UNIT_ASSERT_VALUES_EQUAL((const TBeta*)value, betas.at(key).Get());
                }

                const TBeta& beta = *betas.at(key);
                UNIT_ASSERT_VALUES_EQUAL(beta.Value, betaKeys.count(key));
                for (const auto& [key, value] : beta.AlphaRefs) {
                    UNIT_ASSERT_VALUES_EQUAL((const TAlpha*)value, alphas.at(key).Get());
                }
            }
        }
    }
}
