#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/blobstorage/base/infer_pdisk_slot_count_settings.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_test_peer.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/library/pdisk_io/aio.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_http_request.h>
#include <ydb/core/blobstorage/vdisk/localrecovery/localrecovery_public.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/core/cms/console/console.h>

#include <ydb/library/pdisk_io/sector_map.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/util/random.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#if defined(__linux__)
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_test_peer.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/blobstorage/ddisk/ddisk_actor_test_peer.h>
#include <ydb/library/pdisk_io/uring_router_test_peer.h>
#include <ydb/library/pdisk_io/uring_test_support.h>
#include <sys/file.h>
#include <atomic>
#endif
#include <functional>
#include <optional>

const bool STRAND_PDISK = true;
#ifndef NDEBUG
const bool ENABLE_DETAILED_HIVE_LOG = true;
#else
const bool ENABLE_DETAILED_HIVE_LOG = false;
#endif


namespace NKikimr {

namespace NBlobStorageNodeWardenTest{

#define ENABLE_FORKED_TESTS 0
#if ENABLE_FORKED_TESTS
#    define CUSTOM_UNIT_TEST(a) SIMPLE_UNIT_FORKED_TEST(a)
#else
#define CUSTOM_UNIT_TEST(a) Y_UNIT_TEST(a)
#endif //ENABLE_FORKED_TESTS

#define VERBOSE_COUT(str) \
do { \
    if (IsVerbose) { \
        Cerr << (TStringBuilder() << str << Endl); \
    } \
} while(false)

#define LOW_VERBOSE_COUT(str) \
do { \
    if (IsLowVerbose) { \
        Cerr << (TStringBuilder() << str << Endl); \
    } \
} while(false)


static bool IsVerbose = true;

static yexception LastException;

constexpr ui32 DOMAIN_ID = 1;

using TAppPreprocessor = std::function<void(TAppPrepare&)>;
using TNodeWardenConfigPreprocessor = std::function<void(ui32, TNodeWardenConfig&)>;

using namespace NActors;

void RegisterSharedControl(THotSwap<TControl>& icbControl, TAtomicBase defaultValue,
        TAtomicBase lowerBound, TAtomicBase upperBound, TAtomicBase currentValue) {
    TControlWrapper control(defaultValue, lowerBound, upperBound);
    TControlBoard::RegisterSharedControl(control, icbControl);
    TControlBoard::SetValue(currentValue, icbControl);
}

void FormatPDiskRandomKeys(TString path, ui32 diskSize, ui32 chunkSize, ui64 guid, bool isGuidValid,
        TIntrusivePtr<NPDisk::TSectorMap> sectorMap, bool enableSmallDiskOptimization) {
    NPDisk::TKey chunkKey;
    NPDisk::TKey logKey;
    NPDisk::TKey sysLogKey;
    SafeEntropyPoolRead(&chunkKey, sizeof(NKikimr::NPDisk::TKey));
    SafeEntropyPoolRead(&logKey, sizeof(NKikimr::NPDisk::TKey));
    SafeEntropyPoolRead(&sysLogKey, sizeof(NKikimr::NPDisk::TKey));

    if (!isGuidValid) {
        SafeEntropyPoolRead(&guid, sizeof(guid));
    }

    TFormatOptions options;
    options.SectorMap = sectorMap;
    options.EnableSmallDiskOptimization = enableSmallDiskOptimization;

    NKikimr::FormatPDisk(path, diskSize, 4 << 10, chunkSize,
            guid, chunkKey, logKey,
            sysLogKey, NPDisk::YdbDefaultPDiskSequence, "Test", options);
}

void SetupLogging(TTestActorRuntime& runtime) {
    NActors::NLog::EPriority priority = ENABLE_DETAILED_HIVE_LOG ? NLog::PRI_DEBUG : NLog::PRI_ERROR;
    NActors::NLog::EPriority otherPriority = NLog::PRI_ERROR;

    runtime.SetLogPriority(NKikimrServices::BS_NODE, priority);
    runtime.SetLogPriority(NKikimrServices::BS_CONTROLLER, priority);
    runtime.SetLogPriority(NKikimrServices::BS_PDISK, otherPriority);
    runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, otherPriority);
    runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, otherPriority);
    runtime.SetLogPriority(NKikimrServices::BS_PROXY, otherPriority);
    runtime.SetLogPriority(NKikimrServices::PIPE_CLIENT, otherPriority);
    runtime.SetLogPriority(NKikimrServices::TABLET_RESOLVER, otherPriority);

    runtime.SetLogPriority(NKikimrServices::BS_SKELETON, otherPriority);
    runtime.SetLogPriority(NKikimrServices::BS_SYNCJOB, otherPriority);
    runtime.SetLogPriority(NKikimrServices::BS_SYNCER, otherPriority);
}

void SetupServices(TTestActorRuntime &runtime, TString extraPath, TIntrusivePtr<NPDisk::TSectorMap> extraSectorMap,
        TAppPreprocessor appPreprocessor = {}, TNodeWardenConfigPreprocessor nodeWardenConfigPreprocessor = {}) {
    const ui32 domainsNum = 1;
    const ui32 disksInDomain = 1;

    TAppPrepare app;

    {
        TString baseDir = runtime.GetTempDir();
        TString keyfile = Sprintf("%s/key.txt", baseDir.data());

        TFileOutput file(keyfile);
        file << "some data";
        app.SetKeyForNode(keyfile, 0);
    }

    { // setup domain info
        app.ClearDomainsAndHive();
        auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds("dc-1", 1, 0,
                                                                                  100500,
                                                                                  TVector<ui64>{},
                                                                                  TVector<ui64>{},
                                                                                  TVector<ui64>{},
                                                                                  DefaultPoolKinds(2));
        app.AddDomain(domain.Release());
        app.AddHive(MakeDefaultHiveID());
    }

    SetupChannelProfiles(app);
    if (appPreprocessor) {
        appPreprocessor(app);
    }

    if (false) { // setup channel profiles
        TIntrusivePtr<TChannelProfiles> channelProfiles = new TChannelProfiles;
        channelProfiles->Profiles.emplace_back();
        TChannelProfiles::TProfile &profile = channelProfiles->Profiles.back();
        for (ui32 channelIdx = 0; channelIdx < 3; ++channelIdx) {
            profile.Channels.push_back(
                TChannelProfiles::TProfile::TChannel(TBlobStorageGroupType::Erasure4Plus2Block, 0,
                    NKikimrBlobStorage::TVDiskKind::Default));
        }
        app.SetChannels(std::move(channelProfiles));
    }

    ui32 groupId = TGroupID(EGroupConfigurationType::Static, DOMAIN_ID, 0).GetRaw();
    for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
        SetupStateStorage(runtime, nodeIndex);

        TStringStream str;
        str << "AvailabilityDomains: " << DOMAIN_ID << Endl;
        str << "PDisks { NodeID: $Node1 PDiskID: 0 PDiskGuid: 1 Path: \"pdisk0.dat\"}" << Endl;
        str << "" << Endl;
        str << "VDisks {" << Endl;
        str << "    VDiskID { GroupID: " << groupId << " GroupGeneration: 1 Ring: 0 Domain: 0 VDisk: 0 }" << Endl;
        str << "    VDiskLocation { NodeID: $Node1 PDiskID: 0 PDiskGuid: 1 VDiskSlotID: 0 }" << Endl;
        str << "}" << Endl;
        str << "VDisks {" << Endl;
        str << "    VDiskID { GroupID: " << groupId << " GroupGeneration: 1 Ring: 0 Domain: 1 VDisk: 0 }" << Endl;
        str << "    VDiskLocation { NodeID: $Node1 PDiskID: 0 PDiskGuid: 1 VDiskSlotID: 1 }" << Endl;
        str << "}" << Endl;
        str << "VDisks {" << Endl;
        str << "    VDiskID { GroupID: " << groupId << " GroupGeneration: 1 Ring: 0 Domain: 2 VDisk: 0 }" << Endl;
        str << "    VDiskLocation { NodeID: $Node1 PDiskID: 0 PDiskGuid: 1 VDiskSlotID: 2 }" << Endl;
        str << "}" << Endl;
        str << "VDisks {" << Endl;
        str << "    VDiskID { GroupID: " << groupId << " GroupGeneration: 1 Ring: 0 Domain: 3 VDisk: 0 }" << Endl;
        str << "    VDiskLocation { NodeID: $Node1 PDiskID: 0 PDiskGuid: 1 VDiskSlotID: 3 }" << Endl;
        str << "}" << Endl;
        str << "VDisks {" << Endl;
        str << "    VDiskID { GroupID: " << groupId << " GroupGeneration: 1 Ring: 0 Domain: 4 VDisk: 0 }" << Endl;
        str << "    VDiskLocation { NodeID: $Node1 PDiskID: 0 PDiskGuid: 1 VDiskSlotID: 4 }" << Endl;
        str << "}" << Endl;
        str << "VDisks {" << Endl;
        str << "    VDiskID { GroupID: " << groupId << " GroupGeneration: 1 Ring: 0 Domain: 5 VDisk: 0 }" << Endl;
        str << "    VDiskLocation { NodeID: $Node1 PDiskID: 0 PDiskGuid: 1 VDiskSlotID: 5 }" << Endl;
        str << "}" << Endl;
        str << "VDisks {" << Endl;
        str << "    VDiskID { GroupID: " << groupId << " GroupGeneration: 1 Ring: 0 Domain: 6 VDisk: 0 }" << Endl;
        str << "    VDiskLocation { NodeID: $Node1 PDiskID: 0 PDiskGuid: 1 VDiskSlotID: 6 }" << Endl;
        str << "}" << Endl;
        str << "VDisks {" << Endl;
        str << "    VDiskID { GroupID: " << groupId << " GroupGeneration: 1 Ring: 0 Domain: 7 VDisk: 0 }" << Endl;
        str << "    VDiskLocation { NodeID: $Node1 PDiskID: 0 PDiskGuid: 1 VDiskSlotID: 7 }" << Endl;
        str << "}" << Endl;
        str << "" << Endl;
        str << "Groups {" << Endl;
        str << "    GroupID: " << groupId << Endl;
        str << "    GroupGeneration: 1 " << Endl;
        str << "    ErasureSpecies: 4 " << Endl;// Block42
        str << "    Rings {" << Endl;
        str << "        FailDomains {" << Endl;
        str << "            VDiskLocations { NodeID: $Node1 PDiskID: 0 VDiskSlotID: 0 PDiskGuid: 1 }" << Endl;
        str << "        }" << Endl;
        str << "        FailDomains {" << Endl;
        str << "            VDiskLocations { NodeID: $Node1 PDiskID: 0 VDiskSlotID: 1 PDiskGuid: 1 }" << Endl;
        str << "        }" << Endl;
        str << "        FailDomains {" << Endl;
        str << "            VDiskLocations { NodeID: $Node1 PDiskID: 0 VDiskSlotID: 2 PDiskGuid: 1 }" << Endl;
        str << "        }" << Endl;
        str << "        FailDomains {" << Endl;
        str << "            VDiskLocations { NodeID: $Node1 PDiskID: 0 VDiskSlotID: 3 PDiskGuid: 1 }" << Endl;
        str << "        }" << Endl;
        str << "        FailDomains {" << Endl;
        str << "            VDiskLocations { NodeID: $Node1 PDiskID: 0 VDiskSlotID: 4 PDiskGuid: 1 }" << Endl;
        str << "        }" << Endl;
        str << "        FailDomains {" << Endl;
        str << "            VDiskLocations { NodeID: $Node1 PDiskID: 0 VDiskSlotID: 5 PDiskGuid: 1 }" << Endl;
        str << "        }" << Endl;
        str << "        FailDomains {" << Endl;
        str << "            VDiskLocations { NodeID: $Node1 PDiskID: 0 VDiskSlotID: 6 PDiskGuid: 1 }" << Endl;
        str << "        }" << Endl;
        str << "        FailDomains {" << Endl;
        str << "            VDiskLocations { NodeID: $Node1 PDiskID: 0 VDiskSlotID: 7 PDiskGuid: 1 }" << Endl;
        str << "        }" << Endl;
        str << "    }" << Endl;
        str << "}";
        TString staticConfig(str.Str());

        SubstGlobal(staticConfig, "$Node1", Sprintf("%" PRIu32, runtime.GetNodeId(0)));

        TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig(new TNodeWardenConfig());
//            nodeWardenConfig->Monitoring = monitoring;
        google::protobuf::TextFormat::ParseFromString(staticConfig, nodeWardenConfig->BlobStorageConfig->MutableServiceSet());

        if (nodeIndex == 0) {
            nodeWardenConfig->SectorMaps[extraPath] = extraSectorMap;
            ObtainTenantKey(&nodeWardenConfig->TenantKey, app.Keys[0]);
            ObtainStaticKey(&nodeWardenConfig->StaticKey);

            TString baseDir = runtime.GetTempDir();

            TIntrusivePtr<NPDisk::TSectorMap> sectorMap(new NPDisk::TSectorMap());
            sectorMap->ForceSize(64ull << 30ull);


            TString pDiskPath0 = TStringBuilder() << "SectorMap:" << baseDir << "pdisk_map";
            nodeWardenConfig->BlobStorageConfig->MutableServiceSet()->MutablePDisks(0)->SetPath(pDiskPath0);
            nodeWardenConfig->SectorMaps[pDiskPath0] = sectorMap;

            ui64 pDiskGuid = 1;
            static ui64 iteration = 0;
            ++iteration;
            TFormatOptions options;
            options.SectorMap = sectorMap;
            options.EnableSmallDiskOptimization = false;
            ::NKikimr::FormatPDisk(pDiskPath0, 0, 4 << 10, 32u << 20u, pDiskGuid,
                0x1234567890 + iteration, 0x4567890123 + iteration, 0x7890123456 + iteration,
                NPDisk::YdbDefaultPDiskSequence, "", options);


            // Magic path from testlib, do not change it
            TString pDiskPath1 = TStringBuilder() << baseDir << "pdisk_1.dat";
            TIntrusivePtr<NPDisk::TSectorMap> sectorMap1(new NPDisk::TSectorMap());
            sectorMap1->ForceSize(64ull << 30ull);
            sectorMap1->ZeroInit(32);
            nodeWardenConfig->SectorMaps[pDiskPath1] = sectorMap1;
        }

        if (nodeWardenConfigPreprocessor) {
            nodeWardenConfigPreprocessor(nodeIndex, *nodeWardenConfig);
        }

        SetupBSNodeWarden(runtime, nodeIndex, nodeWardenConfig.Release());
        SetupTabletResolver(runtime, nodeIndex);
    }

    SetupPDiskSubsystem(&runtime, STRAND_PDISK);
    runtime.Initialize(app.Unwrap());

    for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
        TActorId localActor = runtime.GetLocalServiceId(
            MakeBlobStorageNodeWardenID(runtime.GetNodeId(nodeIndex)), nodeIndex);
        runtime.EnableScheduleForActor(localActor, true);
    }

    if (!runtime.IsRealThreads()) {
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(
            TEvBlobStorage::EvLocalRecoveryDone, domainsNum * disksInDomain));
        runtime.DispatchEvents(options);
    }

    CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeBSControllerID(),
        TTabletTypes::BSController, TBlobStorageGroupType::ErasureMirror3dc, groupId),
        &CreateFlatBsController);

    SetupBoxAndStoragePool(runtime, runtime.AllocateEdgeActor());
}

void Setup(TTestActorRuntime &runtime, TString extraPath, TIntrusivePtr<NPDisk::TSectorMap> extraSectorMap,
        TAppPreprocessor appPreprocessor = {}, TNodeWardenConfigPreprocessor nodeWardenConfigPreprocessor = {}) {
    SetupLogging(runtime);
    SetupServices(runtime, extraPath, extraSectorMap,
        std::move(appPreprocessor), std::move(nodeWardenConfigPreprocessor));
//    runtime.SetLogPriority(NKikimrServices::BS_CONTROLLER, NLog::PRI_DEBUG);
//    runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::BS_PROXY, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::BS_PROXY_PUT, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::BS_PROXY_BLOCK, NLog::PRI_DEBUG);
//    runtime.SetLogPriority(NKikimrServices::BS_PDISK, NLog::PRI_DEBUG);
//    runtime.SetLogPriority(NKikimrServices::BS_QUEUE, NLog::PRI_DEBUG);
}

Y_UNIT_TEST_SUITE(TBlobStorageWardenTest) {
    ui64 GetBsc(TTestActorRuntime& /*runtime*/) {
        return MakeBSControllerID();
    }

    ui32 CreatePDisk(TTestActorRuntime &runtime, ui32 nodeIdx, TString path, ui64 guid, ui32 pdiskId, ui64 pDiskCategory,
            const NKikimrBlobStorage::TPDiskConfig* pdiskConfig = nullptr, TActorId nodeWarden = {}) {
        VERBOSE_COUT(" Creating pdisk");

        ui32 nodeId = runtime.GetNodeId(nodeIdx);
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, nodeId);
        auto& record = ev->Record;
        auto *pdisk = record.MutableServiceSet()->AddPDisks();
        pdisk->SetNodeID(nodeId);
        pdisk->SetPDiskID(pdiskId);
        pdisk->SetPath(path);
        pdisk->SetPDiskGuid(guid);
        pdisk->SetPDiskCategory(pDiskCategory);
        pdisk->SetEntityStatus(NKikimrBlobStorage::CREATE);
        if (pdiskConfig) {
            pdisk->MutablePDiskConfig()->CopyFrom(*pdiskConfig);
        }

        if (!nodeWarden) {
            nodeWarden = MakeBlobStorageNodeWardenID(nodeId);
        }
        runtime.Send(new IEventHandle(nodeWarden, TActorId(), ev.release()));

        return pdiskId;
    }

    void DestroyAllPDisks(TTestActorRuntime &runtime, ui32 nodeIdx, TActorId nodeWarden = {}) {
        VERBOSE_COUT(" Destroying all pdisks");

        ui32 nodeId = runtime.GetNodeId(nodeIdx);
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>(NKikimrProto::OK, nodeId);
        auto& record = ev->Record;
        record.SetComprehensive(true);
        record.MutableServiceSet()->ClearPDisks();

        if (!nodeWarden) {
            nodeWarden = MakeBlobStorageNodeWardenID(nodeId);
        }
        runtime.Send(new IEventHandle(nodeWarden, TActorId(), ev.release()));
    }

    void Put(TTestActorRuntime &runtime, TActorId &sender, ui32 groupId, TLogoBlobID logoBlobId, TString data, NKikimrProto::EReplyStatus expectAnsver = NKikimrProto::OK) {
        VERBOSE_COUT(" Sending TEvPut");
        TActorId proxy = MakeBlobStorageProxyID(groupId);
        ui32 nodeId = sender.NodeId();
        TActorId nodeWarden = MakeBlobStorageNodeWardenID(nodeId);
        ui64 cookie = 6543210;
        runtime.Send(new IEventHandle(proxy, sender,
            new TEvBlobStorage::TEvPut(logoBlobId, data, TInstant::Max()),
            IEventHandle::FlagForwardOnNondelivery, cookie, &nodeWarden), sender.NodeId() - runtime.GetNodeId(0));

        TAutoPtr<IEventHandle> handle;
        auto putResult = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvPutResult>(handle);
        UNIT_ASSERT(putResult);
        UNIT_ASSERT_C(putResult->Status == expectAnsver,
                "Status# " << NKikimrProto::EReplyStatus_Name(putResult->Status));
        UNIT_ASSERT_EQUAL(handle->Cookie, cookie);
    }

    void CreateStoragePool(TTestBasicRuntime& runtime, TString name, TString kind) {
        NKikimrBlobStorage::TDefineStoragePool storagePool = runtime.GetAppData().DomainsInfo->GetDomain()->StoragePoolTypes.at(kind);

        TActorId edge = runtime.AllocateEdgeActor();
        auto request = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        Y_ABORT_UNLESS(storagePool.GetKind() == kind);
        storagePool.ClearStoragePoolId();
        storagePool.SetName(name);
        storagePool.SetNumGroups(1);
        storagePool.SetEncryptionMode(1);
        request->Record.MutableRequest()->AddCommand()->MutableDefineStoragePool()->CopyFrom(storagePool);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        runtime.SendToPipe(MakeBSControllerID(), edge, request.release(), 0, pipeConfig);

        auto reply = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetResponse().GetSuccess(), true);
    }

    ui32 GetGroupFromPool(TTestBasicRuntime& runtime, TString poolName) {
        TActorId edge = runtime.AllocateEdgeActor();
        auto selectGroups = std::make_unique<TEvBlobStorage::TEvControllerSelectGroups>();
        auto *record = &selectGroups->Record;
        record->SetReturnAllMatchingGroups(true);
        auto* groupParams = record->AddGroupParameters();
        groupParams->MutableStoragePoolSpecifier()->SetName(poolName);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        runtime.SendToPipe(MakeBSControllerID(), edge, selectGroups.release(), 0, pipeConfig);

        auto reply = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerSelectGroupsResult>(edge);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), NKikimrProto::OK);
        return reply->Get()->Record.GetMatchingGroups(0).GetGroups(0).GetGroupID();
    }

    void SendToBsProxy(TTestBasicRuntime& runtime, TActorId sender, ui32 groupId, IEventBase *ev, ui64 cookie = 0) {
        auto flags = NActors::IEventHandle::FlagTrackDelivery
                    | NActors::IEventHandle::FlagForwardOnNondelivery;

        TActorId recipient = MakeBlobStorageProxyID(groupId);
        TActorId nodeWarden = MakeBlobStorageNodeWardenID(sender.NodeId());
        return runtime.Send(new IEventHandle(recipient, sender, ev,
            flags, cookie, &nodeWarden, {}), sender.NodeId() - runtime.GetNodeId(0));
    }

    NKikimrBlobStorage::TDefineStoragePool DescribeStoragePool(TTestBasicRuntime& runtime, const TString& name) {
        TActorId edge = runtime.AllocateEdgeActor();
        auto selectGroups = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto* request = selectGroups->Record.MutableRequest();
        auto* readPool = request->AddCommand()->MutableReadStoragePool();
        readPool->SetBoxId(1);
        readPool->AddName(name);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        runtime.SendToPipe(MakeBSControllerID(), edge, selectGroups.release(), 0, pipeConfig);

        auto reply = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetResponse().GetSuccess(), true);
        return reply->Get()->Record.GetResponse().GetStatus(0).GetStoragePool(0);
    }

    void RemoveStoragePool(TTestBasicRuntime& runtime, const NKikimrBlobStorage::TDefineStoragePool& storagePool) {
        TActorId edge = runtime.AllocateEdgeActor();
        auto selectGroups = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto* request = selectGroups->Record.MutableRequest();
        auto* deletePool = request->AddCommand()->MutableDeleteStoragePool();
        deletePool->SetBoxId(1);
        deletePool->SetStoragePoolId(storagePool.GetStoragePoolId());
        deletePool->SetItemConfigGeneration(storagePool.GetItemConfigGeneration());

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        runtime.SendToPipe(MakeBSControllerID(), edge, selectGroups.release(), 0, pipeConfig);

        auto reply = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetResponse().GetSuccess(), true);
    }

    struct TBlockUpdates {
        TTestBasicRuntime* Runtime;
        TTestActorRuntime::TEventObserver PrevObserver = nullptr;



        TBlockUpdates(TTestBasicRuntime& runtime)
        : Runtime(&runtime)
        {
            TTestActorRuntime::TEventObserver observer = [=] (TAutoPtr<IEventHandle>& event) -> TTestActorRuntime::EEventAction {
                if (event->GetTypeRewrite() == TEvBlobStorage::EvControllerNodeServiceSetUpdate) {
                    return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };
            PrevObserver = Runtime->SetObserverFunc(observer);
        }

        ~TBlockUpdates() {
            Runtime->SetObserverFunc(PrevObserver);
        }
    };

    CUSTOM_UNIT_TEST(TestSyncLogLimitControlsPassedToVDiskConfig) {
        TTestBasicRuntime runtime(1, false);

        constexpr ui64 expectedSyncLogMaxDiskAmount = 96_MB;
        constexpr ui64 expectedSyncLogMaxMemAmount = 7_MB;

        ui32 observedConfigs = 0;
        TVector<TString> mismatches;

        auto appPreprocessor = [&](TAppPrepare& app) {
            app.InitIcb(runtime.GetNodeCount());
            for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
                RegisterSharedControl(app.Icb[nodeIndex]->VDiskControls.SyncLogMaxDiskAmount,
                    0, 0, 1ull << 40, expectedSyncLogMaxDiskAmount);
                RegisterSharedControl(app.Icb[nodeIndex]->VDiskControls.SyncLogMaxMemAmount,
                    64ull << 20, 0, 1ull << 30, expectedSyncLogMaxMemAmount);
            }
        };

        auto nodeWardenConfigPreprocessor = [&](ui32, TNodeWardenConfig& config) {
            config.VDiskConfigPreprocessor = [&](TVDiskConfig& vdiskConfig) {
                ++observedConfigs;
                if (vdiskConfig.SyncLogMaxDiskAmount != expectedSyncLogMaxDiskAmount ||
                        vdiskConfig.SyncLogMaxMemAmount != expectedSyncLogMaxMemAmount) {
                    mismatches.push_back(TStringBuilder()
                        << "{SyncLogMaxDiskAmount# " << vdiskConfig.SyncLogMaxDiskAmount
                        << " SyncLogMaxMemAmount# " << vdiskConfig.SyncLogMaxMemAmount
                        << "}");
                }
            };
        };

        Setup(runtime, "", nullptr, std::move(appPreprocessor), std::move(nodeWardenConfigPreprocessor));

        UNIT_ASSERT_C(observedConfigs,
            "VDiskConfigPreprocessor was not called; NodeWarden did not create local VDisk configs");
        UNIT_ASSERT_C(mismatches.empty(),
            "NodeWarden did not pass SyncLog immediate controls to TVDiskConfig"
            << " expectedSyncLogMaxDiskAmount# " << expectedSyncLogMaxDiskAmount
            << " expectedSyncLogMaxMemAmount# " << expectedSyncLogMaxMemAmount
            << " mismatches# " << FormatList(mismatches));
    }

    void BlockGroup(TTestBasicRuntime& runtime, TActorId sender, ui64 tabletId, ui32 groupId, ui32 generation, bool isMonitored,
            NKikimrProto::EReplyStatus expectAnsver = NKikimrProto::EReplyStatus::OK) {
        auto request = std::make_unique<TEvBlobStorage::TEvBlock>(tabletId, generation, TInstant::Max());
        request->IsMonitored = isMonitored;
        SendToBsProxy(runtime, sender, groupId, request.release());
        auto reply = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvBlockResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Status, expectAnsver);
    }

    void CollectGroup(TTestBasicRuntime& runtime, TActorId sender, ui64 tabletId, ui32 groupId, bool isMonitored,
            NKikimrProto::EReplyStatus expectAnsver = NKikimrProto::EReplyStatus::OK) {
        auto request = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(tabletId, Max<ui32>(), Max<ui32>(), ui32(0),
                                                                     true, Max<ui32>(), Max<ui32>(),
                                                                     nullptr, nullptr, TInstant::Max(),
                                                                     true, TWriteSource::Unknown, true);
        request->IsMonitored = isMonitored;
        SendToBsProxy(runtime, sender, groupId, request.release());
        auto reply = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvCollectGarbageResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Status, expectAnsver);
    }

    CUSTOM_UNIT_TEST(TestDeleteStoragePool) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, "", nullptr);

        auto sender0 = runtime.AllocateEdgeActor(0);

        CreateStoragePool(runtime, "test_storage", "pool-kind-1");
        ui32 groupId = GetGroupFromPool(runtime, "test_storage");

        ui64 tabletId = 1234;
        ui32 generation = 1;
        BlockGroup(runtime, sender0, tabletId, groupId, generation, true);
        BlockGroup(runtime, sender0, tabletId, groupId, generation, true, NKikimrProto::EReplyStatus::ALREADY);
        BlockGroup(runtime, sender0, tabletId, groupId, generation-1, true, NKikimrProto::EReplyStatus::ALREADY);

        auto describePool = DescribeStoragePool(runtime, "test_storage");
        {
            TBlockUpdates bloker(runtime);
            RemoveStoragePool(runtime, describePool);

            ++generation;
            BlockGroup(runtime, sender0, tabletId, groupId, generation++, true);
        }

        ++generation;
        BlockGroup(runtime, sender0, tabletId, groupId, generation++, true);

        RebootTablet(runtime, MakeBSControllerID(), sender0, sender0.NodeId() - runtime.GetNodeId(0));

        ++generation;
        BlockGroup(runtime, sender0, tabletId, groupId, generation++, true, NKikimrProto::EReplyStatus::NO_GROUP);
    }

    CUSTOM_UNIT_TEST(TestFilterBadSerials) {
        TTestActorSystem runtime(1);
        runtime.SetupNodeSubSystems = [](ui32, TActorSystemSetup* setup) {
            setup->RegisterSubSystem<IPDiskSubsystem>(CreatePDiskSubsystem());
        };
        runtime.Start();

        TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig(new TNodeWardenConfig());

        IActor* ac = CreateBSNodeWarden(nodeWardenConfig.Release());

        TActorId nodeWarden = runtime.Register(ac, 1);

        runtime.WrapInActorContext(nodeWarden, [](IActor* wardenActor) {
            auto vectorsEqual = [](const TVector<TString>& vec1, const TVector<TString>& vec2) {
                TVector<TString> sortedVec1 = vec1;
                TVector<TString> sortedVec2 = vec2;

                std::sort(sortedVec1.begin(), sortedVec1.end());
                std::sort(sortedVec2.begin(), sortedVec2.end());

                return sortedVec1 == sortedVec2;
            };

            auto checkHasOnlyGoodDrive = [](TVector<NPDisk::TDriveData>& drives) {
                UNIT_ASSERT_EQUAL(1, drives.size());
                UNIT_ASSERT_EQUAL(drives[0].Path, "/good1");
            };

            NStorage::TNodeWarden& warden = *dynamic_cast<NStorage::TNodeWarden*>(wardenActor);

            NPDisk::TDriveData goodDrive1;
            goodDrive1.Path = "/good1";
            goodDrive1.SerialNumber = "FOOBAR";

            NPDisk::TDriveData goodDrive2;
            goodDrive2.Path = "/good2";
            goodDrive2.SerialNumber = "BARFOO";

            NPDisk::TDriveData badDrive1;
            badDrive1.Path = "/bad1";
            char s[] = {50, 51, 52, -128, 0}; // Non-ASCII character -128.
            badDrive1.SerialNumber = TString(s);

            NPDisk::TDriveData badDrive2;
            badDrive2.Path = "/bad2";
            badDrive2.SerialNumber = "NOT\tGOOD"; // Non-printable character \t.

            NPDisk::TDriveData badDrive3;
            badDrive3.Path = "/bad3";
            badDrive3.SerialNumber = TString(101, 'F'); // Size exceeds 100.

            NPDisk::TDriveData badDrive4;
            badDrive4.Path = "/bad4";
            badDrive4.SerialNumber = "NOTGOODEITHER";
            badDrive4.SerialNumber[5] = '\0'; // Unexpected null-terminator.

            TStringStream details;

            // Check for zero drives.
            {
                TVector<NPDisk::TDriveData> drives;
                warden.RemoveDrivesWithBadSerialsAndReport(drives, details);

                UNIT_ASSERT_EQUAL(0, drives.size());
                UNIT_ASSERT_EQUAL(0, warden.DrivePathCounterKeys().size());
            }

            // If a drive is not present in a subsequent call, then it is removed from a counters map.
            // We check both serial number validator and also that counters are removed for missing drives.
            {
                TVector<NPDisk::TDriveData> drives = {goodDrive1, badDrive1};
                warden.RemoveDrivesWithBadSerialsAndReport(drives, details);

                checkHasOnlyGoodDrive(drives);
                UNIT_ASSERT(vectorsEqual(warden.DrivePathCounterKeys(), {"/good1", "/bad1"}));
            }
            {
                TVector<NPDisk::TDriveData> drives = {goodDrive1, badDrive2};
                warden.RemoveDrivesWithBadSerialsAndReport(drives, details);

                checkHasOnlyGoodDrive(drives);
                UNIT_ASSERT(vectorsEqual(warden.DrivePathCounterKeys(), {"/good1", "/bad2"}));
            }
            {
                TVector<NPDisk::TDriveData> drives = {goodDrive1, badDrive3};
                warden.RemoveDrivesWithBadSerialsAndReport(drives, details);

                checkHasOnlyGoodDrive(drives);
                UNIT_ASSERT(vectorsEqual(warden.DrivePathCounterKeys(), {"/good1", "/bad3"}));
            }
            {
                TVector<NPDisk::TDriveData> drives = {goodDrive1, badDrive4};
                warden.RemoveDrivesWithBadSerialsAndReport(drives, details);

                checkHasOnlyGoodDrive(drives);
                UNIT_ASSERT(vectorsEqual(warden.DrivePathCounterKeys(), {"/good1", "/bad4"}));
            }
            {
                TVector<NPDisk::TDriveData> drives = {goodDrive1, goodDrive2, badDrive4};
                warden.RemoveDrivesWithBadSerialsAndReport(drives, details);

                UNIT_ASSERT_EQUAL(2, drives.size());
                UNIT_ASSERT(vectorsEqual(warden.DrivePathCounterKeys(), {"/good1", "/good2", "/bad4"}));
            }
            // Check that good drives can also be removed from counters map.
            {
                TVector<NPDisk::TDriveData> drives = {goodDrive1, badDrive4};
                warden.RemoveDrivesWithBadSerialsAndReport(drives, details);

                checkHasOnlyGoodDrive(drives);
                UNIT_ASSERT(vectorsEqual(warden.DrivePathCounterKeys(), {"/good1", "/bad4"}));
            }
            // Check that everything is removed if there are no drives.
            {
                TVector<NPDisk::TDriveData> drives;
                warden.RemoveDrivesWithBadSerialsAndReport(drives, details);

                UNIT_ASSERT_EQUAL(0, drives.size());
                UNIT_ASSERT_EQUAL(0, warden.DrivePathCounterKeys().size());
            }
        });
    }

    CUSTOM_UNIT_TEST(TestStopAggregatorRemovesReportedStats) {
        TTestActorSystem runtime(1);
        runtime.SetupNodeSubSystems = [](ui32, TActorSystemSetup* setup) {
            setup->RegisterSubSystem<IPDiskSubsystem>(CreatePDiskSubsystem());
        };
        runtime.Start();

        TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig(
            new TNodeWardenConfig());
        const TActorId nodeWarden = runtime.Register(CreateBSNodeWarden(nodeWardenConfig.Release()), 1);

        runtime.WrapInActorContext(nodeWarden, [](IActor* wardenActor) {
            auto& warden = *dynamic_cast<NStorage::TNodeWarden*>(wardenActor);
            const TActorId vdiskServiceId = MakeBlobStorageVDiskID(1, 2, 3);

            warden.RunningVDiskServiceIds.insert(vdiskServiceId);
            warden.PerAggregatorInfo.emplace(vdiskServiceId, NStorage::TNodeWarden::TAggregatorInfo{42, {}});

            warden.StopAggregator(vdiskServiceId);

            UNIT_ASSERT(!warden.RunningVDiskServiceIds.contains(vdiskServiceId));
            UNIT_ASSERT(!warden.PerAggregatorInfo.contains(vdiskServiceId));
        });
    }

    CUSTOM_UNIT_TEST(TestSendToInvalidGroupId) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, "", nullptr);

        auto sender = runtime.AllocateEdgeActor(0);

        CreateStoragePool(runtime, "test_storage", "pool-kind-1");
        ui32 groupId = Max<ui32>();

        ui64 tabletId = 1234;
        ui32 generation = 1;
        BlockGroup(runtime, sender, tabletId, groupId, generation, true, NKikimrProto::ERROR);
        Put(runtime, sender, groupId, TLogoBlobID(tabletId, generation, 0, 0, 5, 0), "hello",
                NKikimrProto::EReplyStatus::ERROR);
        CollectGroup(runtime, sender, tabletId, groupId, true, NKikimrProto::EReplyStatus::ERROR);
    }

    CUSTOM_UNIT_TEST(TestBlockEncriptedGroup) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, "", nullptr);

        auto sender0 = runtime.AllocateEdgeActor(0);
        auto sender1 = runtime.AllocateEdgeActor(1);

        CreateStoragePool(runtime, "test_storage", "pool-kind-1");
        ui32 groupId = GetGroupFromPool(runtime, "test_storage");

        ui64 tabletId = 1234;
        ui32 generation = 1;
        BlockGroup(runtime, sender0, tabletId, groupId, generation, true);

        Put(runtime, sender0, groupId, TLogoBlobID(tabletId, generation, 0, 0, 5, 0), "hello", NKikimrProto::EReplyStatus::BLOCKED);
        Put(runtime, sender0, groupId, TLogoBlobID(tabletId, generation+1, 0, 0, 5, 0), "hello");

        BlockGroup(runtime, sender1, tabletId, groupId, generation+2, true);
        Put(runtime, sender1, groupId, TLogoBlobID(tabletId, generation+2, 0, 0, 10, 0), "hellohello", NKikimrProto::EReplyStatus::ERROR);
        Put(runtime, sender1, groupId, TLogoBlobID(tabletId, generation+3, 0, 0, 10, 0), "hellohello", NKikimrProto::EReplyStatus::ERROR);

        Put(runtime, sender0, groupId, TLogoBlobID(tabletId, generation+1, 0, 0, 11, 0), "hello_again", NKikimrProto::EReplyStatus::BLOCKED);

        CollectGroup(runtime, sender1, tabletId, groupId, true);
    }

    void AssertMonitoringExists(TTestBasicRuntime& runtime, ui32 nodeIdx, TString groupName) {
        auto rootStats = runtime.GetDynamicCounters(nodeIdx);
        auto stats = GetServiceCounters(rootStats, "dsproxy_percentile")->GetSubgroup("blobstorageproxy", groupName);
        auto responseStats = stats->GetSubgroup("subsystem", "response");
        auto putTabletStats = responseStats->GetSubgroup("event", "putTabletLog");

        UNIT_ASSERT_UNEQUAL(responseStats->FindSubgroup("event", "putTabletLogAll"), nullptr);
        UNIT_ASSERT_UNEQUAL(responseStats->FindSubgroup("event", "putAsyncBlob"), nullptr);
        UNIT_ASSERT_UNEQUAL(responseStats->FindSubgroup("event", "putUserData"), nullptr);
        UNIT_ASSERT_UNEQUAL(responseStats->FindSubgroup("event", "get"), nullptr);
        UNIT_ASSERT_UNEQUAL(responseStats->FindSubgroup("event", "block"), nullptr);
        UNIT_ASSERT_UNEQUAL(responseStats->FindSubgroup("event", "discover"), nullptr);
        UNIT_ASSERT_UNEQUAL(responseStats->FindSubgroup("event", "indexRestoreGet"), nullptr);
        UNIT_ASSERT_UNEQUAL(responseStats->FindSubgroup("event", "range"), nullptr);

        UNIT_ASSERT_UNEQUAL(putTabletStats->FindSubgroup("size", "256"), nullptr);
        UNIT_ASSERT_UNEQUAL(putTabletStats->FindSubgroup("size", "512"), nullptr);
    }

    void AssertMonitoringDoesNotExist(TTestBasicRuntime& runtime, ui32 nodeIdx, TString groupName) {
        auto rootStats = runtime.GetDynamicCounters(nodeIdx);
        auto stats = GetServiceCounters(rootStats, "dsproxy_percentile")->GetSubgroup("blobstorageproxy", groupName);
        auto responseStats = stats->GetSubgroup("subsystem", "response");
        auto putTabletStats = responseStats->GetSubgroup("event", "putTabletLog");

        UNIT_ASSERT_VALUES_EQUAL(responseStats->FindSubgroup("event", "putTabletLogAll"), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(responseStats->FindSubgroup("event", "putAsyncBlob"), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(responseStats->FindSubgroup("event", "putUserData"), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(responseStats->FindSubgroup("event", "get"), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(responseStats->FindSubgroup("event", "discover"), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(responseStats->FindSubgroup("event", "indexRestoreGet"), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(responseStats->FindSubgroup("event", "range"), nullptr);

        UNIT_ASSERT_VALUES_EQUAL(putTabletStats->FindSubgroup("size", "256"), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(putTabletStats->FindSubgroup("size", "512"), nullptr);

        // always send BlockResponseTime
        UNIT_ASSERT_UNEQUAL(responseStats->FindSubgroup("event", "block"), nullptr);
    }

    CUSTOM_UNIT_TEST(TestLimitedKeylessGroupThenNoMonitoring) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, "", nullptr);

        auto sender0 = runtime.AllocateEdgeActor(0);
        auto sender1 = runtime.AllocateEdgeActor(1);

        CreateStoragePool(runtime, "test_storage", "pool-kind-1");

        ui32 generation = 1;
        ui64 tabletId = 1234;
        ui32 groupId = GetGroupFromPool(runtime, "test_storage");
        TString name = Sprintf("%09" PRIu32, groupId);

        BlockGroup(runtime, sender0, tabletId, groupId, generation, true);

        Put(runtime, sender0, groupId, TLogoBlobID(tabletId, generation, 0, 0, 5, 0), "hello", NKikimrProto::EReplyStatus::BLOCKED);
        Put(runtime, sender0, groupId, TLogoBlobID(tabletId, generation+1, 0, 0, 5, 0), "hello");

        BlockGroup(runtime, sender1, tabletId, groupId, generation+2, true);
        Put(runtime, sender1, groupId, TLogoBlobID(tabletId, generation+2, 0, 0, 10, 0), "hellohello", NKikimrProto::EReplyStatus::ERROR);
        Put(runtime, sender1, groupId, TLogoBlobID(tabletId, generation+3, 0, 0, 10, 0), "hellohello", NKikimrProto::EReplyStatus::ERROR);

        Put(runtime, sender0, groupId, TLogoBlobID(tabletId, generation+1, 0, 0, 11, 0), "hello_again", NKikimrProto::EReplyStatus::BLOCKED);

        CollectGroup(runtime, sender1, tabletId, groupId, true);

        AssertMonitoringDoesNotExist(runtime, 1, name); // expect IsLimitedKeyLess on node 1
    }

    CUSTOM_UNIT_TEST(TestUnmonitoredEventsThenNoMonitorings) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, "", nullptr);

        auto sender0 = runtime.AllocateEdgeActor(0);

        CreateStoragePool(runtime, "test_storage", "pool-kind-1");

        ui32 generation = 1;
        ui64 tabletId = 1234;
        ui32 groupId = GetGroupFromPool(runtime, "test_storage");
        TString name = Sprintf("%09" PRIu32, groupId);

        BlockGroup(runtime, sender0, tabletId, groupId, generation, false);
        CollectGroup(runtime, sender0, tabletId, groupId, false);

        AssertMonitoringDoesNotExist(runtime, 0, name);

        BlockGroup(runtime, sender0, tabletId, groupId, generation + 2, true);
        AssertMonitoringExists(runtime, 0, name);

        BlockGroup(runtime, sender0, tabletId, groupId, generation + 3, false);
        AssertMonitoringExists(runtime, 0, name); // it cannot disappear
    }

    CUSTOM_UNIT_TEST(TestSendUsefulMonitoring) {
        TTestBasicRuntime runtime(2, false);
        Setup(runtime, "", nullptr);

        auto sender0 = runtime.AllocateEdgeActor(0);
        auto sender1 = runtime.AllocateEdgeActor(1);

        CreateStoragePool(runtime, "test_storage", "pool-kind-1");

        ui32 generation = 1;
        ui64 tabletId = 1234;
        ui32 groupId = GetGroupFromPool(runtime, "test_storage");
        TString name = Sprintf("%09" PRIu32, groupId);

        Put(runtime, sender0, groupId, TLogoBlobID(tabletId, generation+1, 0, 0, 5, 0), "hello");
        CollectGroup(runtime, sender1, tabletId, groupId, true);

        AssertMonitoringExists(runtime, 0, name);
    }

    CUSTOM_UNIT_TEST(TestGivenPDiskFormatedWithGuid1AndCreatedWithGuid2WhenYardInitThenError) {
        TTestBasicRuntime runtime(1, false);
        TString pdiskPath = "SectorMap:TestGivenPDiskFormatedWithGuid1AndCreatedWithGuid2WhenYardInitThenError";
        TIntrusivePtr<NPDisk::TSectorMap> sectorMap(new NPDisk::TSectorMap(32ull << 30));
        Setup(runtime, pdiskPath, sectorMap);
        TActorId edge = runtime.AllocateEdgeActor();

        ui64 guid1 = 1;
        ui64 guid2 = 2;
        SafeEntropyPoolRead(&guid1, sizeof(guid1));
        SafeEntropyPoolRead(&guid2, sizeof(guid2));
        UNIT_ASSERT_VALUES_UNEQUAL(guid1, guid2);

        VERBOSE_COUT(" Formatting PDisk with guid1 " << guid1);
        FormatPDiskRandomKeys("", sectorMap->DeviceSize, 32 << 20, guid1, true, sectorMap, false);

        VERBOSE_COUT(" Creating PDisk with guid2 " << guid2);
        ui32 pdiskId = CreatePDisk(runtime, 0, pdiskPath, guid2, 1001, 0);
        runtime.SimulateSleep(TDuration::Seconds(1));

        VERBOSE_COUT(" Verify that PDisk returns ERROR");
        ui32 nodeId = runtime.GetNodeId(0);
        TActorId pdiskActor = MakeBlobStoragePDiskID(nodeId, pdiskId);
        TVDiskID vdiskId;
        runtime.Send(new IEventHandle(pdiskActor, edge, new NPDisk::TEvYardInit(1, vdiskId, guid1)), 0);
        auto initResult = runtime.GrabEdgeEventRethrow<NPDisk::TEvYardInitResult>(edge, TDuration::Seconds(1));
        UNIT_ASSERT(initResult && initResult->Get());
        auto record = initResult->Get();
        VERBOSE_COUT(" YardInitResult: " << record->ToString());

        UNIT_ASSERT(record->Status == NKikimrProto::CORRUPTED);
        UNIT_ASSERT(record->ErrorReason.Contains("PDisk is in StateError"));
        UNIT_ASSERT(record->ErrorReason.Contains("guid error"));
        UNIT_ASSERT(record->ErrorReason.Contains(TStringBuilder() << guid1));
        UNIT_ASSERT(record->ErrorReason.Contains(TStringBuilder() << guid2));
    }

    void TestHttpMonForPath(const TString& path) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, "", nullptr);
        auto edge = runtime.AllocateEdgeActor(0);
        TActorId nodeWarden = MakeBlobStorageNodeWardenID(edge.NodeId());
        THttpRequestMock HttpRequest;
        NMonitoring::TMonService2HttpRequest monService2HttpRequest(nullptr, &HttpRequest, nullptr, nullptr, path,
                nullptr);
        runtime.Send(new IEventHandle(nodeWarden, edge, new NMon::TEvHttpInfo(monService2HttpRequest)), 0);
        auto httpInfoRes = runtime.GrabEdgeEventRethrow<NMon::TEvHttpInfoRes>(edge, TDuration::Seconds(1));
        UNIT_ASSERT(httpInfoRes && httpInfoRes->Get());
        TStringStream out;
        httpInfoRes->Get()->Output(out);
        UNIT_ASSERT(out.Size());
    }

    CUSTOM_UNIT_TEST(TestHttpMonPage) {
        TestHttpMonForPath("");
        TestHttpMonForPath("/json/groups");
    }

    void TestObtainPDiskKey(TString pin1, TString pin2) {
        std::unique_ptr<TTempDir> tmp(new TTempDir());
        TString keyfile = Sprintf("%s/key.txt", (*tmp)().data());
        {
            TFileOutput file(keyfile);
            file << "some data";
        }

        NKikimrProto::TKeyConfig keyConfig;
        NKikimrProto::TKeyRecord* keyRecord = keyConfig.AddKeys();
        keyRecord->SetContainerPath(keyfile);
        keyRecord->SetPin(pin1);
        keyRecord->SetId("Key");
        keyRecord->SetVersion(1);

        NPDisk::TMainKey mainKey1;
        UNIT_ASSERT(ObtainPDiskKey(&mainKey1, keyConfig));

        keyRecord->SetPin(pin2);
        NPDisk::TMainKey mainKey2;
        UNIT_ASSERT(ObtainPDiskKey(&mainKey2, keyConfig));

        UNIT_ASSERT_VALUES_EQUAL(mainKey1.Keys.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(mainKey2.Keys.size(), 1);

        if (pin1 == pin2) {
            UNIT_ASSERT_VALUES_EQUAL(mainKey1.Keys[0], mainKey2.Keys[0]);
        } else {
            UNIT_ASSERT_VALUES_UNEQUAL(mainKey1.Keys[0], mainKey2.Keys[0]);
        }
    }

    CUSTOM_UNIT_TEST(ObtainPDiskKeySamePin) {
        TestObtainPDiskKey("pin", "pin");
    }

    // TODO (serg-belyakov): Fix conversion from TEncryption key to PDisk's TKey
    // CUSTOM_UNIT_TEST(ObtainPDiskKeyDifferentPin) {
    //    TestObtainPDiskKey("pin1", "pin2");
    // }

    void TestObtainTenantKey(TString pin1, TString pin2) {
        std::unique_ptr<TTempDir> tmp(new TTempDir());
        TString keyfile = Sprintf("%s/key.txt", (*tmp)().data());
        {
            TFileOutput file(keyfile);
            file << "some data";
        }

        NKikimrProto::TKeyConfig keyConfig;
        NKikimrProto::TKeyRecord* keyRecord = keyConfig.AddKeys();
        keyRecord->SetContainerPath(keyfile);
        keyRecord->SetPin(pin1);
        keyRecord->SetId("Key");
        keyRecord->SetVersion(1);

        TEncryptionKey key1;
        UNIT_ASSERT(ObtainTenantKey(&key1, keyConfig));

        keyRecord->SetPin(pin2);
        TEncryptionKey key2;
        UNIT_ASSERT(ObtainTenantKey(&key2, keyConfig));

        if (pin1 == pin2) {
            UNIT_ASSERT(key1.Key == key2.Key);
        } else {
            UNIT_ASSERT(!(key1.Key == key2.Key));
        }
    }

    CUSTOM_UNIT_TEST(ObtainTenantKeySamePin) {
        TestObtainTenantKey("pin", "pin");
    }

    CUSTOM_UNIT_TEST(ObtainTenantKeyDifferentPin) {
        TestObtainTenantKey("pin1", "pin2");
    }

    Y_UNIT_TEST(TestReceivedPDiskRestartNotAllowed) {
        TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
        runtime.SetupNodeSubSystems = [](ui32, TActorSystemSetup* setup) {
            setup->RegisterSubSystem<IPDiskSubsystem>(CreatePDiskSubsystem());
        };
        runtime.Start();

        ui32 nodeId = 1;
        ui32 pdiskId = 1337;
        ui64 cookie = 555;

        auto &appData = runtime.GetNode(1)->AppData;
        appData->DomainsInfo->AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dom", 1).Release());

        TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig(new TNodeWardenConfig());

        IActor* ac = CreateBSNodeWarden(nodeWardenConfig.Release());

        TActorId nodeWarden = runtime.Register(ac, nodeId);

        auto fakeBSC = runtime.AllocateEdgeActor(nodeId);

        TActorId pdiskActorId = runtime.AllocateEdgeActor(nodeId);
        TActorId pdiskServiceId = MakeBlobStoragePDiskID(nodeId, pdiskId);

        runtime.RegisterService(pdiskServiceId, pdiskActorId);

        runtime.Send(new IEventHandle(nodeWarden, pdiskActorId, new TEvBlobStorage::TEvAskWardenRestartPDisk(pdiskId, false), 0, cookie), nodeId);

        auto responseEvent = new TEvBlobStorage::TEvControllerConfigResponse();

        auto res = responseEvent->Record.MutableResponse();
        res->SetSuccess(false);
        res->SetErrorDescription("Fake error");
        runtime.Send(new IEventHandle(nodeWarden, fakeBSC, responseEvent, 0, 1), nodeId);

        auto evPtr = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvAskWardenRestartPDiskResult>(pdiskActorId);
        auto restartPDiskEv = evPtr->Get();

        UNIT_ASSERT(!restartPDiskEv->RestartAllowed);
        UNIT_ASSERT_STRINGS_EQUAL("Fake error", restartPDiskEv->Details);

        UNIT_ASSERT_EQUAL(pdiskId, restartPDiskEv->PDiskId);
    }

    void TestInferPDiskSlotCount(ui64 driveSize, ui64 unitSizeInBytes, ui32 maxSlots,
            ui32 expectedSlotCount, ui32 expectedSlotSizeInUnits, double expectedRelativeError = 0) {
        TIntrusivePtr<TPDiskConfig> pdiskConfig = new TPDiskConfig("fake_drive", 0, 0, 0);

        NStorage::TNodeWarden::InferPDiskSlotCount(pdiskConfig, driveSize, unitSizeInBytes, maxSlots);

        double unitSizeCalculated = double(driveSize) / pdiskConfig->ExpectedSlotCount / pdiskConfig->SlotSizeInUnits;
        double unitSizeRelativeError =  (unitSizeCalculated - unitSizeInBytes) / unitSizeInBytes;

        VERBOSE_COUT(""
            << " driveSize# " << driveSize
            << " unitSizeInBytes# " << unitSizeInBytes
            << " maxSlots# " << maxSlots
            << " ->"
            << " ExpectedSlotCount# " << pdiskConfig->ExpectedSlotCount
            << " SlotSizeInUnits# " << pdiskConfig->SlotSizeInUnits
            << " relativeError# " << unitSizeRelativeError
        );

        if (expectedSlotCount) {
            UNIT_ASSERT_VALUES_EQUAL(pdiskConfig->ExpectedSlotCount, expectedSlotCount);
        }
        if (expectedSlotSizeInUnits) {
            UNIT_ASSERT_VALUES_EQUAL(pdiskConfig->SlotSizeInUnits, expectedSlotSizeInUnits);
        }

        if (expectedRelativeError > 0) {
            UNIT_ASSERT_LE_C(abs(unitSizeRelativeError), expectedRelativeError,
                TStringBuilder() << "abs(" << unitSizeRelativeError << ") <= " << expectedRelativeError
            );
        }
    }

    CUSTOM_UNIT_TEST(TestInferPDiskSlotCountPureFunction) {
        TestInferPDiskSlotCount(7900, 1000, 16, 8, 1u, 0.0125);
        TestInferPDiskSlotCount(8000, 1000, 16, 8, 1u, std::numeric_limits<double>::epsilon());
        TestInferPDiskSlotCount(8100, 1000, 16, 8, 1u, 0.0125);
        TestInferPDiskSlotCount(16000, 1000, 16, 16, 1u, std::numeric_limits<double>::epsilon());
        TestInferPDiskSlotCount(24000, 1000, 16, 12, 2u, std::numeric_limits<double>::epsilon());
        TestInferPDiskSlotCount(31000, 1000, 16, 16, 2u, 0.032);
        TestInferPDiskSlotCount(50000, 1000, 16, 13, 4u, 0.039);
        TestInferPDiskSlotCount(50000, 100, 16, 16, 32u, 0.024);
        TestInferPDiskSlotCount(18000, 200, 16, 11, 8u, 0.023);
        TestInferPDiskSlotCount(200, 1000, 16, 1, 1u, 0.8);
        TestInferPDiskSlotCount(999, 1000, 16, 1, 1u, 0.001);
        TestInferPDiskSlotCount(1499, 1000, 16, 1, 1u, 0.499);
        TestInferPDiskSlotCount(1500, 1000, 16, 2, 1u, 0.25);

        for (ui32 maxSlots = 1; maxSlots <= 24; maxSlots++) {
            for (ui64 i = 1; i <= 1024; i++) {
                // In all cases the relative error doesn't exceed 1/maxSlots
                TestInferPDiskSlotCount(i, 1, maxSlots, 0, 0, 1./maxSlots);
            }
        }

        const size_t c_160GB = 160'000'000'000;
        const size_t c_200GB = 200'000'000'000;
        const size_t c_2000GB = 2000'000'000'000;

        // Some real-world examples
        TestInferPDiskSlotCount(1919'366'987'776, c_200GB, 16, 10, 1u, 0.041); // "Micron_5200_MTFDDAK1T9TDD"
        TestInferPDiskSlotCount(3199'243'124'736, c_200GB, 16, 16, 1u, 0.001); // "SAMSUNG MZWLR3T8HBLS-00007"
        TestInferPDiskSlotCount(6400'161'873'920, c_200GB, 16, 16, 2u, 0.001); // "INTEL SSDPE2KE064T8"
        TestInferPDiskSlotCount(6398'611'030'016, c_200GB, 16, 16, 2u, 0.001); // "INTEL SSDPF2KX076T1"
        TestInferPDiskSlotCount(17999'117'418'496, c_2000GB, 16, 9, 1u, 0.001); // "WDC  WUH721818ALE6L4"

        // Another real-world case
        TestInferPDiskSlotCount(3199'556'648'960, c_160GB, 24, 20, 1u, 0.001);
        TestInferPDiskSlotCount(6399'968'935'936, c_160GB, 24, 20, 2u, 0.001);
        TestInferPDiskSlotCount(17999'117'418'496, c_2000GB, 24, 9, 1u, 0.001);
    }

    void CheckInferredPDiskSettings(TTestBasicRuntime& runtime, TActorId fakeWhiteboard,
            TActorId fakeNodeWarden, ui32 pdiskId, ui32 expectedSlotCount, ui32 expectedSlotSizeInUnits,
            std::optional<ui64> expectedSlotSize = std::nullopt,
            TDuration simTimeout = TDuration::Seconds(10)) {
        const int maxAttempts = 10;
        for (int attempt = 1; attempt <= maxAttempts; ++attempt) {
            // Check EvPDiskStateUpdate sent from PDiskActor to Whiteboard
            const auto ev = runtime.GrabEdgeEventRethrow<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateUpdate>(fakeWhiteboard, simTimeout);
            VERBOSE_COUT(" Got TEvPDiskStateUpdate# " << ev->ToString());

            NKikimrWhiteboard::TPDiskStateInfo pdiskInfo = ev->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(pdiskInfo.GetPDiskId(), pdiskId);
            if (pdiskInfo.GetState() != NKikimrBlobStorage::TPDiskState::Normal) {
                UNIT_ASSERT_LT_C(attempt, maxAttempts, "last attempt failed");
                continue;
            }
            UNIT_ASSERT(pdiskInfo.HasExpectedSlotCount());
            UNIT_ASSERT(pdiskInfo.HasSlotSizeInUnits());
            UNIT_ASSERT(pdiskInfo.HasAvailableSize());
            UNIT_ASSERT(pdiskInfo.HasTotalSize());
            UNIT_ASSERT_VALUES_EQUAL(pdiskInfo.GetExpectedSlotCount(), expectedSlotCount);
            UNIT_ASSERT_VALUES_EQUAL(pdiskInfo.GetSlotSizeInUnits(), expectedSlotSizeInUnits);
            // the field is always present in whiteboard updates; 0 means 'not set'
            UNIT_ASSERT(pdiskInfo.HasExpectedSlotSize());
            UNIT_ASSERT_VALUES_EQUAL(pdiskInfo.GetExpectedSlotSize(), expectedSlotSize.value_or(0));
            UNIT_ASSERT(pdiskInfo.HasPDiskUsage());
            UNIT_ASSERT_VALUES_EQUAL(pdiskInfo.GetPDiskUsage(), 0.0);
            UNIT_ASSERT(pdiskInfo.HasPDiskCapacityAlert());
            UNIT_ASSERT_VALUES_EQUAL(pdiskInfo.GetPDiskCapacityAlert(), NKikimrBlobStorage::TPDiskSpaceColor::GREEN);
            break;
        }

        for (int attempt = 1; attempt <= maxAttempts; ++attempt) {
            // Check EvControllerUpdateDiskStatus sent from PDiskActor to NodeWarden
            const auto ev = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerUpdateDiskStatus>(fakeNodeWarden, simTimeout);
            VERBOSE_COUT(" Got TEvControllerUpdateDiskStatus# " << ev->ToString());

            NKikimrBlobStorage::TEvControllerUpdateDiskStatus diskStatus = ev->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(diskStatus.PDisksMetricsSize(), 1);

            const NKikimrBlobStorage::TPDiskMetrics &metrics = diskStatus.GetPDisksMetrics(0);
            UNIT_ASSERT_VALUES_EQUAL(metrics.GetPDiskId(), pdiskId);
            if (metrics.GetState() != NKikimrBlobStorage::TPDiskState::Normal) {
                UNIT_ASSERT_LT_C(attempt, maxAttempts, "last attempt failed");
                continue;
            }
            // metrics are replaced as a whole on the receiving side, so zero values are
            // reported by omitting the field
            UNIT_ASSERT_VALUES_EQUAL(metrics.HasSlotCount(), expectedSlotCount != 0);
            UNIT_ASSERT(metrics.HasSlotSizeInUnits());
            UNIT_ASSERT_VALUES_EQUAL(metrics.GetSlotCount(), expectedSlotCount);
            UNIT_ASSERT_VALUES_EQUAL(metrics.GetSlotSizeInUnits(), expectedSlotSizeInUnits);
            if (expectedSlotSize) {
                UNIT_ASSERT(metrics.HasExpectedSlotSize());
                UNIT_ASSERT_VALUES_EQUAL(metrics.GetExpectedSlotSize(), *expectedSlotSize);
            } else {
                UNIT_ASSERT(!metrics.HasExpectedSlotSize());
            }
            UNIT_ASSERT(metrics.HasPDiskUsage());
            UNIT_ASSERT_VALUES_EQUAL(metrics.GetPDiskUsage(), 0.0);
            UNIT_ASSERT(metrics.HasPDiskCapacityAlert());
            UNIT_ASSERT_VALUES_EQUAL(metrics.GetPDiskCapacityAlert(), NKikimrBlobStorage::TPDiskSpaceColor::GREEN);
            break;
        }
    }

    TActorId SetupNodeWardenOnly(TTestBasicRuntime& runtime) {
        // Setup logging
        SetupLogging(runtime);
        runtime.SetLogPriority(NKikimrServices::BS_PDISK, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_DEBUG);

        // Initialize runtime
        TAppPrepare app;
        app.AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dc-1").Release());
        app.AddHive(0);
        SetupPDiskSubsystem(&runtime, false);
        runtime.Initialize(app.Unwrap());

        // Setup BSNodeWarden
        TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig(new TNodeWardenConfig());
        IActor* nodeWardenActor = CreateBSNodeWarden(nodeWardenConfig.Release());
        TActorId realNodeWarden = runtime.Register(nodeWardenActor, 0);
        runtime.EnableScheduleForActor(realNodeWarden, true);

        // Communication scheme:
        //                                      .-> fakeNodeWarden -.
        // test -> realNodeWarden -> realPDsik -                     -> test
        //                                      `-> fakeWhiteboard -`
        // Now give it some time to bootstrap
        runtime.SimulateSleep(TDuration::Seconds(10));
        return realNodeWarden;
    }

    class TNoReplyPDiskActor : public TActorBootstrapped<TNoReplyPDiskActor> {
        const TActorId Observer;

    public:
        TNoReplyPDiskActor(TActorId observer)
            : Observer(observer)
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }

        void Handle(NPDisk::TEvSlay::TPtr ev) {
            const auto *msg = ev->Get();
            Send(Observer, new NPDisk::TEvSlay(msg->VDiskId, msg->SlayOwnerRound, msg->PDiskId, msg->VSlotId));
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NPDisk::TEvSlay, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        )
    };

    class TNoReplyPDiskSubsystem : public IPDiskSubsystem {
        TActorId Observer;

    public:
        void SetObserver(TActorId observer) {
            Observer = observer;
        }

        void Start(const TActorContext& ctx, ui32 pdiskId, const TIntrusivePtr<TPDiskConfig>&,
                const NPDisk::TMainKey&, ui32 poolId, ui32 nodeId) override {
            Y_ABORT_UNLESS(Observer);
            const TActorId actorId = ctx.Register(new TNoReplyPDiskActor(Observer), TMailboxType::HTSwap, poolId);
            ctx.ActorSystem()->RegisterLocalService(MakeBlobStoragePDiskID(nodeId, pdiskId), actorId);
        }
    };

    TActorId SetupNodeWardenForSlayTest(TTestActorSystem& runtime) {
        if (!runtime.SetupNodeSubSystems) {
            runtime.SetupNodeSubSystems = [](ui32, TActorSystemSetup* setup) {
                setup->RegisterSubSystem<IPDiskSubsystem>(CreatePDiskSubsystem());
            };
        }
        runtime.Start();

        auto& appData = *runtime.GetNode(1)->AppData;
        appData.DomainsInfo->AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dom", 1).Release());
        appData.DynamicNameserviceConfig = new TDynamicNameserviceConfig();

        TIntrusivePtr<TNodeWardenConfig> config(new TNodeWardenConfig());
        ObtainStaticKey(&config->StaticKey);
        const TActorId nodeWardenId = runtime.Register(CreateBSNodeWarden(config.Release()), 1);
        runtime.RegisterService(MakeBlobStorageNodeWardenID(1), nodeWardenId);
        runtime.WrapInActorContext(nodeWardenId, [](IActor *actor) {
            dynamic_cast<NStorage::TNodeWarden*>(actor)->Bootstrap();
        });
        return nodeWardenId;
    }

    void SetSlayTestVDisk(NStorage::TNodeWarden::TVDiskRecord& record, ui32 nodeId, ui32 pdiskId,
            ui32 vdiskSlotId, const TVDiskID& vdiskId) {
        auto *location = record.Config.MutableVDiskLocation();
        location->SetNodeID(nodeId);
        location->SetPDiskID(pdiskId);
        location->SetVDiskSlotID(vdiskSlotId);
        VDiskIDFromVDiskID(vdiskId, record.Config.MutableVDiskID());
    }

    CUSTOM_UNIT_TEST(TestSlayCompletesWhenPDiskIsDestroyedBeforeReply) {
        TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
        runtime.SetupNodeSubSystems = [](ui32, TActorSystemSetup* setup) {
            setup->RegisterSubSystem<IPDiskSubsystem>(std::make_unique<TNoReplyPDiskSubsystem>());
        };
        const TActorId nodeWardenId = SetupNodeWardenForSlayTest(runtime);
        const ui32 nodeId = 1;
        const ui32 pdiskId = 2007;
        const ui32 vdiskSlotId = 10;
        const NStorage::TNodeWarden::TVSlotId vslotId(nodeId, pdiskId, vdiskSlotId);
        const TVDiskID vdiskId(100507, 15, 0, 0, 0);
        const TActorId slayObserver = runtime.AllocateEdgeActor(nodeId);
        runtime.WrapInActorContext(nodeWardenId, [slayObserver](IActor*) {
            auto* subsystem = TActivationContext::ActorSystem()->GetSubSystem<IPDiskSubsystem>();
            dynamic_cast<TNoReplyPDiskSubsystem*>(subsystem)->SetObserver(slayObserver);
        });

        NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk pdisk;
        pdisk.SetNodeID(nodeId);
        pdisk.SetPDiskID(pdiskId);
        pdisk.SetPath("slay-test-pdisk");
        pdisk.SetPDiskGuid(1);
        pdisk.SetPDiskCategory(0);

        runtime.WrapInActorContext(nodeWardenId, [&](IActor *actor) {
            auto& nodeWarden = *dynamic_cast<NStorage::TNodeWarden*>(actor);
            nodeWarden.StartLocalPDisk(pdisk, false);

            NStorage::TNodeWarden::TVDiskRecord record;
            SetSlayTestVDisk(record, nodeId, pdiskId, vdiskSlotId, vdiskId);
            nodeWarden.Slay(record, NStorage::TNodeWarden::ESlayAction::DESTROY);
            UNIT_ASSERT(nodeWarden.SlayInFlight.contains(vslotId));
        });

        auto slay = runtime.WaitForEdgeActorEvent<NPDisk::TEvSlay>(slayObserver, false);
        UNIT_ASSERT_VALUES_EQUAL(slay->Get()->VDiskId, vdiskId);

        runtime.WrapInActorContext(nodeWardenId, [&](IActor *actor) {
            auto& nodeWarden = *dynamic_cast<NStorage::TNodeWarden*>(actor);
            const auto it = nodeWarden.SlayInFlight.find(vslotId);
            UNIT_ASSERT(it != nodeWarden.SlayInFlight.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second.Round, slay->Get()->SlayOwnerRound);

            nodeWarden.DestroyLocalPDisk(pdiskId);

            UNIT_ASSERT(!nodeWarden.SlayInFlight.contains(vslotId));
        });

        const TActorId probeActor = runtime.AllocateEdgeActor(nodeId);
        runtime.Send(new IEventHandle(nodeWardenId, MakeBlobStoragePDiskID(nodeId, pdiskId),
            new NPDisk::TEvSlayResult(NKikimrProto::OK, 0, vdiskId, slay->Get()->SlayOwnerRound,
                pdiskId, vdiskSlotId, {})), nodeId);
        runtime.Send(new IEventHandle(probeActor, nodeWardenId, new TEvents::TEvWakeup), nodeId);
        runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(probeActor);

        runtime.WrapInActorContext(nodeWardenId, [&](IActor *actor) {
            auto& nodeWarden = *dynamic_cast<NStorage::TNodeWarden*>(actor);
            UNIT_ASSERT(!nodeWarden.SlayInFlight.contains(vslotId));
        });
    }

    CUSTOM_UNIT_TEST(TestUnconfirmedSlayIsRetriedByTimer) {
        TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
        const TActorId nodeWardenId = SetupNodeWardenForSlayTest(runtime);
        const ui32 nodeId = 1;
        const ui32 pdiskId = 2004;
        const ui32 vdiskSlotId = 7;
        const NStorage::TNodeWarden::TVSlotId vslotId(nodeId, pdiskId, vdiskSlotId);
        const TVDiskID vdiskId(100504, 12, 0, 0, 0);
        const TActorId pdiskActor = runtime.AllocateEdgeActor(nodeId);
        runtime.RegisterService(MakeBlobStoragePDiskID(nodeId, pdiskId), pdiskActor);

        runtime.WrapInActorContext(nodeWardenId, [&](IActor *actor) {
            auto& nodeWarden = *dynamic_cast<NStorage::TNodeWarden*>(actor);
            auto [it, inserted] = nodeWarden.SlayInFlight.emplace(vslotId,
                NStorage::TNodeWarden::TSlayInFlight{vdiskId, NStorage::TNodeWarden::ESlayAction::WIPE});
            UNIT_ASSERT(inserted);
            nodeWarden.IssueSlay(vslotId, it->second);
        });

        auto first = runtime.WaitForEdgeActorEvent<NPDisk::TEvSlay>(pdiskActor, false);
        auto retry = runtime.WaitForEdgeActorEvent<NPDisk::TEvSlay>(pdiskActor, false);
        UNIT_ASSERT_VALUES_EQUAL(retry->Get()->VDiskId, vdiskId);
        UNIT_ASSERT_UNEQUAL(retry->Get()->SlayOwnerRound, first->Get()->SlayOwnerRound);

        runtime.WrapInActorContext(nodeWardenId, [&](IActor *actor) {
            auto& nodeWarden = *dynamic_cast<NStorage::TNodeWarden*>(actor);
            nodeWarden.SlayInFlight.erase(vslotId);
        });
    }

    CUSTOM_UNIT_TEST(TestNotReadySlayRetryMakesInsuranceTimerStale) {
        TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
        const TActorId nodeWardenId = SetupNodeWardenForSlayTest(runtime);
        const ui32 nodeId = 1;
        const ui32 pdiskId = 2005;
        const ui32 vdiskSlotId = 8;
        const NStorage::TNodeWarden::TVSlotId vslotId(nodeId, pdiskId, vdiskSlotId);
        const TVDiskID vdiskId(100505, 13, 0, 0, 0);
        const TActorId pdiskActor = runtime.AllocateEdgeActor(nodeId);
        const TActorId probeActor = runtime.AllocateEdgeActor(nodeId);
        runtime.RegisterService(MakeBlobStoragePDiskID(nodeId, pdiskId), pdiskActor);

        runtime.WrapInActorContext(nodeWardenId, [&](IActor *actor) {
            auto& nodeWarden = *dynamic_cast<NStorage::TNodeWarden*>(actor);
            auto [it, inserted] = nodeWarden.SlayInFlight.emplace(vslotId,
                NStorage::TNodeWarden::TSlayInFlight{vdiskId, NStorage::TNodeWarden::ESlayAction::WIPE});
            UNIT_ASSERT(inserted);
            nodeWarden.IssueSlay(vslotId, it->second);
        });

        auto first = runtime.WaitForEdgeActorEvent<NPDisk::TEvSlay>(pdiskActor, false);
        runtime.Send(new IEventHandle(nodeWardenId, pdiskActor,
            new NPDisk::TEvSlayResult(NKikimrProto::NOTREADY, 0, vdiskId,
                first->Get()->SlayOwnerRound, pdiskId, vdiskSlotId, "PDisk is initializing")), nodeId);

        auto retry = runtime.WaitForEdgeActorEvent<NPDisk::TEvSlay>(pdiskActor, false);
        UNIT_ASSERT_UNEQUAL(retry->Get()->SlayOwnerRound, first->Get()->SlayOwnerRound);

        runtime.Schedule(TDuration::Seconds(5),
            new IEventHandle(probeActor, nodeWardenId, new TEvents::TEvWakeup), nullptr, nodeId);
        auto next = runtime.WaitForEdgeActorEvent({pdiskActor, probeActor});
        UNIT_ASSERT_VALUES_EQUAL(next->GetTypeRewrite(), TEvents::TEvWakeup::EventType);

        runtime.WrapInActorContext(nodeWardenId, [&](IActor *actor) {
            auto& nodeWarden = *dynamic_cast<NStorage::TNodeWarden*>(actor);
            const auto it = nodeWarden.SlayInFlight.find(vslotId);
            UNIT_ASSERT(it != nodeWarden.SlayInFlight.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second.Round, retry->Get()->SlayOwnerRound);
            nodeWarden.SlayInFlight.erase(it);
        });
    }

    CUSTOM_UNIT_TEST(TestSlayRetryKeepsOperationIdentity) {
        TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
        const TActorId nodeWardenId = SetupNodeWardenForSlayTest(runtime);
        const ui32 nodeId = 1;
        const ui32 pdiskId = 2001;
        const ui32 vdiskSlotId = 3;
        const TActorId pdiskActor = runtime.AllocateEdgeActor(nodeId);
        runtime.RegisterService(MakeBlobStoragePDiskID(nodeId, pdiskId), pdiskActor);

        const TVDiskID initialVDiskId(100500, 7, 0, 0, 0);
        const TVDiskID destroyVDiskId(100500, 8, 0, 0, 0);
        runtime.WrapInActorContext(nodeWardenId, [&](IActor *actor) {
            auto& nodeWarden = *dynamic_cast<NStorage::TNodeWarden*>(actor);
            NStorage::TNodeWarden::TVDiskRecord record;
            SetSlayTestVDisk(record, nodeId, pdiskId, vdiskSlotId, initialVDiskId);
            nodeWarden.Slay(record, NStorage::TNodeWarden::ESlayAction::WIPE);

            SetSlayTestVDisk(record, nodeId, pdiskId, vdiskSlotId, destroyVDiskId);
            nodeWarden.Slay(record, NStorage::TNodeWarden::ESlayAction::DESTROY);

            const auto it = nodeWarden.SlayInFlight.find({nodeId, pdiskId, vdiskSlotId});
            UNIT_ASSERT(it != nodeWarden.SlayInFlight.end());
            UNIT_ASSERT(it->second.Action == NStorage::TNodeWarden::ESlayAction::DESTROY);
            UNIT_ASSERT_VALUES_EQUAL(it->second.VDiskId, destroyVDiskId);
        });

        auto first = runtime.WaitForEdgeActorEvent<NPDisk::TEvSlay>(pdiskActor, false);
        UNIT_ASSERT_VALUES_EQUAL(first->Get()->VDiskId, initialVDiskId);

        auto retry = runtime.WaitForEdgeActorEvent<NPDisk::TEvSlay>(pdiskActor, false);
        UNIT_ASSERT_VALUES_EQUAL(retry->Get()->VDiskId, destroyVDiskId);
        UNIT_ASSERT_UNEQUAL(retry->Get()->SlayOwnerRound, first->Get()->SlayOwnerRound);
    }

    CUSTOM_UNIT_TEST(TestPDiskRestartWaitsForConcreteDDisksAndCoalescesChanges) {
        using TWarden = NStorage::TNodeWarden;
        TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
        const auto wardenId = SetupNodeWardenForSlayTest(runtime);
        const ui32 pdiskId = 2090;
        const auto pdiskActor = runtime.AllocateEdgeActor(1);
        runtime.RegisterService(MakeBlobStoragePDiskID(1, pdiskId), pdiskActor);
        const auto first = runtime.AllocateEdgeActor(1);
        const auto second = runtime.AllocateEdgeActor(1);
        const TWarden::TVSlotId slot(1, pdiskId, 1);
        ui32 permissions = 0;
        runtime.FilterEnqueue = [&](ui32, std::unique_ptr<IEventHandle>& ev, ISchedulerCookie*, TInstant) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvAskWardenRestartPDiskResult::EventType) {
                ++permissions;
            }
            return true;
        };
        runtime.WrapInActorContext(wardenId, [&](IActor* actor) {
            auto& warden = *dynamic_cast<TWarden*>(actor);
            auto& pdisk = NStorage::TNodeWardenTestPeer::AddPDisk(warden, pdiskId);
            pdisk.SetNodeID(1);
            pdisk.SetPDiskID(pdiskId);
            pdisk.SetPath("/tmp/scripted-restart-device");
            pdisk.SetPDiskGuid(123);
            warden.DDiskActors.emplace(first, slot);
            warden.DDiskActors.emplace(second, TWarden::TVSlotId(1, pdiskId, 2));
            warden.VDiskIdByActor.emplace(first, slot);
            warden.VDiskIdByActor.emplace(second, TWarden::TVSlotId(1, pdiskId, 2));
            auto& record = warden.LocalVDisks[slot];
            SetSlayTestVDisk(record, 1, pdiskId, 1, TVDiskID(100590, 1, 0, 0, 0));
            record.RuntimeData.emplace();
            record.RuntimeData->GroupInfo = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone);
            record.RuntimeData->ActorId = first;
            record.RuntimeData->DDisk = true;
            warden.DoRestartLocalPDisk(pdisk);
            UNIT_ASSERT(record.ShutdownPending);
            warden.PoisonLocalVDisk(record);
            UNIT_ASSERT(record.ShutdownPending);
            warden.StartLocalVDiskActor(record);
            UNIT_ASSERT(!record.RuntimeData);
            auto& restart = warden.PDiskRestartInFlight.at(pdiskId);
            UNIT_ASSERT_VALUES_EQUAL(restart.WaitingFor.size(), 2);
            UNIT_ASSERT(restart.Phase == TWarden::TPDiskRestart::EPhase::WaitingForDDisks);
            auto incoming = pdisk;
            incoming.MutablePDiskConfig()->SetExpectedSlotCount(42);
            warden.DoRestartLocalPDisk(incoming);
            warden.OnPDiskRestartFinished(pdiskId, NKikimrProto::OK);
            UNIT_ASSERT(restart.Phase == TWarden::TPDiskRestart::EPhase::WaitingForDDisks);
            UNIT_ASSERT_VALUES_EQUAL(restart.WaitingFor.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(permissions, 0);
            UNIT_ASSERT(!restart.RequiresAnotherRestart);
            // Deleted slots still fence PDisk restart; the second was already stopping.
            warden.LocalVDisks.erase(slot);
        });
        runtime.WaitForEdgeActorEvent<TEvents::TEvPoison>(first, false);
        runtime.WaitForEdgeActorEvent<TEvents::TEvPoison>(second, false);
        runtime.Send(new IEventHandle(wardenId, first, new TEvents::TEvGone()), 1);
        bool firstGone = false;
        runtime.Sim([&] {
            runtime.WrapInActorContext(wardenId, [&](IActor* actor) {
                auto& warden = *dynamic_cast<TWarden*>(actor);
                firstGone = !warden.DDiskActors.contains(first);
            });
            return !firstGone;
        });
        runtime.WrapInActorContext(wardenId, [&](IActor* actor) {
            auto& warden = *dynamic_cast<TWarden*>(actor);
            UNIT_ASSERT_VALUES_EQUAL(warden.PDiskRestartInFlight.at(pdiskId).WaitingFor.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(permissions, 0);
        });
        runtime.Send(new IEventHandle(wardenId, first, new TEvents::TEvGone()), 1);
        runtime.Send(new IEventHandle(wardenId, second, new TEvents::TEvGone()), 1);
        auto forwarded = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvAskWardenRestartPDiskResult>(pdiskActor, false);
        UNIT_ASSERT_VALUES_EQUAL(forwarded->Get()->Config->ExpectedSlotCount, 42);
        runtime.WrapInActorContext(wardenId, [&](IActor* actor) {
            auto& warden = *dynamic_cast<TWarden*>(actor);
            auto& restart = warden.PDiskRestartInFlight.at(pdiskId);
            UNIT_ASSERT(restart.Phase == TWarden::TPDiskRestart::EPhase::RestartSent);
            UNIT_ASSERT(restart.WaitingFor.empty());
            auto& recreated = warden.LocalVDisks[slot];
            SetSlayTestVDisk(recreated, 1, pdiskId, 1, TVDiskID(100590, 1, 0, 0, 0));
            warden.StartLocalVDiskActor(recreated);
            UNIT_ASSERT(!recreated.RuntimeData);
            UNIT_ASSERT_VALUES_EQUAL(permissions, 1);
            auto incoming = NStorage::TNodeWardenTestPeer::GetPDisk(warden, pdiskId);
            incoming.MutablePDiskConfig()->SetExpectedSlotCount(43);
            warden.DoRestartLocalPDisk(incoming);
            UNIT_ASSERT(restart.RequiresAnotherRestart);
            warden.OnPDiskRestartFinished(pdiskId, NKikimrProto::OK);
            UNIT_ASSERT(!recreated.RuntimeData);
        });
        auto next = runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvAskWardenRestartPDiskResult>(pdiskActor, false);
        UNIT_ASSERT_VALUES_EQUAL(next->Get()->Config->ExpectedSlotCount, 43);
        UNIT_ASSERT_VALUES_EQUAL(permissions, 2);
        runtime.Send(new IEventHandle(wardenId, second, new TEvents::TEvGone()), 1);
        const auto barrier = runtime.AllocateEdgeActor(1);
        runtime.Send(new IEventHandle(barrier, {}, new TEvents::TEvWakeup()), 1);
        runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(barrier, false);
        UNIT_ASSERT_VALUES_EQUAL(permissions, 2);
        runtime.FilterEnqueue = {};
    }

    CUSTOM_UNIT_TEST(TestPDiskRestartImmediateHandoffAndRemovalCancelsDrain) {
        using TWarden = NStorage::TNodeWarden;
        TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
        const auto wardenId = SetupNodeWardenForSlayTest(runtime);
        for (const bool hasDDisk : {false, true}) {
            const ui32 pdiskId = 2091 + hasDDisk;
            const auto pdiskActor = runtime.AllocateEdgeActor(1);
            const auto ddisk = runtime.AllocateEdgeActor(1);
            runtime.RegisterService(MakeBlobStoragePDiskID(1, pdiskId), pdiskActor);
            runtime.WrapInActorContext(wardenId, [&](IActor* actor) {
                auto& warden = *dynamic_cast<TWarden*>(actor);
                auto& pdisk = NStorage::TNodeWardenTestPeer::AddPDisk(warden, pdiskId);
                pdisk.SetNodeID(1);
                pdisk.SetPDiskID(pdiskId);
                pdisk.SetPath(TStringBuilder() << "/tmp/scripted-pdisk-" << pdiskId);
                pdisk.SetPDiskGuid(pdiskId);
                NStorage::TNodeWardenTestPeer::TrackPath(warden, pdiskId);
                if (hasDDisk) {
                    warden.DDiskActors.emplace(ddisk, TWarden::TVSlotId(1, pdiskId, 1));
                }
                warden.DoRestartLocalPDisk(pdisk);
                const auto& restart = warden.PDiskRestartInFlight.at(pdiskId);
                UNIT_ASSERT(restart.Phase == (hasDDisk ? TWarden::TPDiskRestart::EPhase::WaitingForDDisks
                    : TWarden::TPDiskRestart::EPhase::RestartSent));
                if (hasDDisk) {
                    warden.DestroyLocalPDisk(pdiskId);
                    UNIT_ASSERT(!warden.PDiskRestartInFlight.contains(pdiskId));
                }
            });
            if (hasDDisk) {
                runtime.WaitForEdgeActorEvent<TEvents::TEvPoison>(ddisk, false);
                runtime.WaitForEdgeActorEvent<TEvents::TEvPoison>(pdiskActor, false);
                runtime.Send(new IEventHandle(wardenId, ddisk, new TEvents::TEvGone()), 1);
            } else {
                runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvAskWardenRestartPDiskResult>(pdiskActor, false);
            }
        }
    }

    CUSTOM_UNIT_TEST(TestRestartDrainReminderGenerationAndPhaseGates) {
        using TWarden = NStorage::TNodeWarden;
        using TReminder = NStorage::TNodeWardenTestPeer::TRestartDrainReminder;
        TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
        const auto wardenId = SetupNodeWardenForSlayTest(runtime);
        const ui32 pdiskId = 2094;
        const auto pdiskActor = runtime.AllocateEdgeActor(1);
        const auto ddisk = runtime.AllocateEdgeActor(1);
        const auto clockEdge = runtime.AllocateEdgeActor(1);
        runtime.RegisterService(MakeBlobStoragePDiskID(1, pdiskId), pdiskActor);
        std::vector<std::pair<ui64, TInstant>> reminders;
        ui32 permissions = 0;
        runtime.FilterEnqueue = [&](ui32, std::unique_ptr<IEventHandle>& ev, ISchedulerCookie*, TInstant at) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvAskWardenRestartPDiskResult::EventType) {
                ++permissions;
            }
            if (ev->GetTypeRewrite() == TReminder::EventType && at > runtime.GetClock()) {
                reminders.emplace_back(ev->Get<TReminder>()->Generation, at);
                return false;
            }
            return true;
        };
        runtime.WrapInActorContext(wardenId, [&](IActor* actor) {
            auto& warden = *static_cast<TWarden*>(actor);
            auto& pdisk = NStorage::TNodeWardenTestPeer::AddPDisk(warden, pdiskId);
            pdisk.SetNodeID(1);
            pdisk.SetPDiskID(pdiskId);
            pdisk.SetPath("/tmp/reminder-pdisk");
            pdisk.SetPDiskGuid(123);
            NStorage::TNodeWardenTestPeer::TrackPath(warden, pdiskId);
            warden.DDiskActors.emplace(ddisk, TWarden::TVSlotId(1, pdiskId, 1));
            warden.DoRestartLocalPDisk(pdisk);
        });
        runtime.WaitForEdgeActorEvent<TEvents::TEvPoison>(ddisk, false);
        UNIT_ASSERT_VALUES_EQUAL(reminders.size(), 1);
        const auto generation = reminders.front().first;
        for (ui32 count = 1; count <= 2; ++count) {
            const auto due = reminders.back().second;
            runtime.Schedule(due, new IEventHandle(clockEdge, {}, new TEvents::TEvWakeup()), nullptr, 1);
            runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(clockEdge, false);
            runtime.Send(new IEventHandle(wardenId, {}, new TReminder(pdiskId, generation)), 1);
            runtime.Send(new IEventHandle(clockEdge, {}, new TEvents::TEvWakeup()), 1);
            runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(clockEdge, false);
            UNIT_ASSERT_VALUES_EQUAL(reminders.size(), count + 1);
            UNIT_ASSERT_VALUES_EQUAL(reminders.back().first, generation);
            UNIT_ASSERT_VALUES_EQUAL(reminders.back().second, due + TDuration::Seconds(30));
        }
        runtime.Send(new IEventHandle(wardenId, ddisk, new TEvents::TEvGone()), 1);
        runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvAskWardenRestartPDiskResult>(pdiskActor, false);
        auto deliverStale = [&] {
            runtime.Send(new IEventHandle(wardenId, {}, new TReminder(pdiskId, generation)), 1);
            runtime.Send(new IEventHandle(clockEdge, {}, new TEvents::TEvWakeup()), 1);
            runtime.WaitForEdgeActorEvent<TEvents::TEvWakeup>(clockEdge, false);
        };
        deliverStale();
        UNIT_ASSERT_VALUES_EQUAL(reminders.size(), 3);
        runtime.WrapInActorContext(wardenId, [&](IActor* actor) {
            auto& warden = *static_cast<TWarden*>(actor);
            warden.OnPDiskRestartFinished(pdiskId, NKikimrProto::OK);
            warden.DDiskActors.emplace(ddisk, TWarden::TVSlotId(1, pdiskId, 1));
            auto incoming = NStorage::TNodeWardenTestPeer::GetPDisk(warden, pdiskId);
            warden.DoRestartLocalPDisk(incoming);
        });
        runtime.WaitForEdgeActorEvent<TEvents::TEvPoison>(ddisk, false);
        UNIT_ASSERT_VALUES_EQUAL(reminders.size(), 4);
        UNIT_ASSERT_UNEQUAL(reminders.back().first, generation);
        deliverStale();
        UNIT_ASSERT_VALUES_EQUAL(reminders.size(), 4);
        runtime.WrapInActorContext(wardenId, [&](IActor* actor) {
            static_cast<TWarden*>(actor)->DestroyLocalPDisk(pdiskId);
        });
        runtime.WaitForEdgeActorEvent<TEvents::TEvPoison>(pdiskActor, false);
        runtime.Send(new IEventHandle(wardenId, ddisk, new TEvents::TEvGone()), 1);
        runtime.Send(new IEventHandle(wardenId, {}, new TReminder(pdiskId, reminders.back().first)), 1);
        deliverStale();
        UNIT_ASSERT_VALUES_EQUAL(reminders.size(), 4);
        bool restartExists = true;
        runtime.WrapInActorContext(wardenId, [&](IActor* actor) {
            restartExists = static_cast<TWarden*>(actor)->PDiskRestartInFlight.contains(pdiskId);
        });
        UNIT_ASSERT(!restartExists);
        UNIT_ASSERT_VALUES_EQUAL(permissions, 1);
        runtime.FilterEnqueue = {};
    }

    CUSTOM_UNIT_TEST(TestGoneFromOldIncarnationDoesNotClearReplacementShutdown) {
        using TWarden = NStorage::TNodeWarden;
        TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
        const auto wardenId = SetupNodeWardenForSlayTest(runtime);
        const auto oldActor = runtime.AllocateEdgeActor(1);
        const auto replacement = runtime.AllocateEdgeActor(1);
        const TWarden::TVSlotId slot(1, 2093, 1);
        runtime.WrapInActorContext(wardenId, [&](IActor* actor) {
            auto& warden = *dynamic_cast<TWarden*>(actor);
            warden.DDiskActors.emplace(oldActor, slot);
            warden.VDiskIdByActor.emplace(oldActor, slot);
            auto& record = warden.LocalVDisks[slot];
            record.ShutdownPending = true;
            record.ShutdownActorId = replacement;
        });
        runtime.Send(new IEventHandle(wardenId, oldActor, new TEvents::TEvGone()), 1);
        bool removed = false;
        runtime.Sim([&] {
            runtime.WrapInActorContext(wardenId, [&](IActor* actor) {
                auto& warden = *dynamic_cast<TWarden*>(actor);
                removed = !warden.VDiskIdByActor.contains(oldActor);
                UNIT_ASSERT(warden.LocalVDisks.at(slot).ShutdownPending);
                UNIT_ASSERT_VALUES_EQUAL(warden.LocalVDisks.at(slot).ShutdownActorId, replacement);
            });
            return !removed;
        });
    }

    CUSTOM_UNIT_TEST(TestSlayIsReplayedAfterPDiskRestartWithoutLocalVDisk) {
        TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
        const TActorId nodeWardenId = SetupNodeWardenForSlayTest(runtime);
        const ui32 nodeId = 1;
        const ui32 pdiskId = 2002;
        const ui32 vdiskSlotId = 4;
        const TActorId pdiskActor = runtime.AllocateEdgeActor(nodeId);
        runtime.RegisterService(MakeBlobStoragePDiskID(nodeId, pdiskId), pdiskActor);

        const TVDiskID vdiskId(100501, 9, 0, 0, 0);
        runtime.WrapInActorContext(nodeWardenId, [&](IActor *actor) {
            auto& nodeWarden = *dynamic_cast<NStorage::TNodeWarden*>(actor);
            NStorage::TNodeWarden::TVDiskRecord record;
            SetSlayTestVDisk(record, nodeId, pdiskId, vdiskSlotId, vdiskId);
            nodeWarden.Slay(record, NStorage::TNodeWarden::ESlayAction::DESTROY);
        });

        auto first = runtime.WaitForEdgeActorEvent<NPDisk::TEvSlay>(pdiskActor, false);

        runtime.WrapInActorContext(nodeWardenId, [&](IActor *actor) {
            auto& nodeWarden = *dynamic_cast<NStorage::TNodeWarden*>(actor);
            nodeWarden.PDiskRestartInFlight[pdiskId].Phase = NStorage::TNodeWarden::TPDiskRestart::EPhase::RestartSent;
            nodeWarden.OnPDiskRestartFinished(pdiskId, NKikimrProto::OK);
        });

        auto replay = runtime.WaitForEdgeActorEvent<NPDisk::TEvSlay>(pdiskActor, false);
        UNIT_ASSERT_VALUES_EQUAL(replay->Get()->VDiskId, vdiskId);
        UNIT_ASSERT_UNEQUAL(replay->Get()->SlayOwnerRound, first->Get()->SlayOwnerRound);
    }

    CUSTOM_UNIT_TEST(TestVDiskWithSlayInFlightIsNotStartedAfterFailedPDiskRestart) {
        TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
        const TActorId nodeWardenId = SetupNodeWardenForSlayTest(runtime);
        const ui32 nodeId = 1;
        const ui32 pdiskId = 2006;
        const ui32 vdiskSlotId = 9;
        const NStorage::TNodeWarden::TVSlotId vslotId(nodeId, pdiskId, vdiskSlotId);
        const TVDiskID vdiskId(100506, 14, 0, 0, 0);

        runtime.WrapInActorContext(nodeWardenId, [&](IActor *actor) {
            auto& nodeWarden = *dynamic_cast<NStorage::TNodeWarden*>(actor);
            auto& record = nodeWarden.LocalVDisks[vslotId];
            SetSlayTestVDisk(record, nodeId, pdiskId, vdiskSlotId, vdiskId);
            nodeWarden.SlayInFlight.emplace(vslotId,
                NStorage::TNodeWarden::TSlayInFlight{vdiskId, NStorage::TNodeWarden::ESlayAction::WIPE});
            nodeWarden.PDiskRestartInFlight[pdiskId].Phase = NStorage::TNodeWarden::TPDiskRestart::EPhase::RestartSent;

            nodeWarden.OnPDiskRestartFinished(pdiskId, NKikimrProto::ERROR);

            UNIT_ASSERT(!record.RuntimeData);
            UNIT_ASSERT(nodeWarden.SlayInFlight.contains(vslotId));
            UNIT_ASSERT(!nodeWarden.PDiskRestartInFlight.contains(pdiskId));
        });
    }

    CUSTOM_UNIT_TEST(TestSlayCompletesWhenPDiskIsAlreadyMissing) {
        TTestActorSystem runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>());
        const TActorId nodeWardenId = SetupNodeWardenForSlayTest(runtime);
        const ui32 nodeId = 1;
        const ui32 pdiskId = 2003;
        const ui32 destroyVDiskSlotId = 5;
        const ui32 wipeVDiskSlotId = 6;
        const TVDiskID destroyVDiskId(100502, 10, 0, 0, 0);
        const TVDiskID wipeVDiskId(100503, 11, 0, 0, 0);
        const NStorage::TNodeWarden::TVSlotId destroyVSlotId(nodeId, pdiskId, destroyVDiskSlotId);
        const NStorage::TNodeWarden::TVSlotId wipeVSlotId(nodeId, pdiskId, wipeVDiskSlotId);

        NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk destroyVDisk;
        auto *location = destroyVDisk.MutableVDiskLocation();
        location->SetNodeID(nodeId);
        location->SetPDiskID(pdiskId);
        location->SetVDiskSlotID(destroyVDiskSlotId);
        VDiskIDFromVDiskID(destroyVDiskId, destroyVDisk.MutableVDiskID());
        destroyVDisk.SetEntityStatus(NKikimrBlobStorage::EEntityStatus::DESTROY);

        NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk wipeVDisk;
        location = wipeVDisk.MutableVDiskLocation();
        location->SetNodeID(nodeId);
        location->SetPDiskID(pdiskId);
        location->SetVDiskSlotID(wipeVDiskSlotId);
        VDiskIDFromVDiskID(wipeVDiskId, wipeVDisk.MutableVDiskID());
        wipeVDisk.SetDoWipe(true);

        runtime.WrapInActorContext(nodeWardenId, [&](IActor *actor) {
            auto& nodeWarden = *dynamic_cast<NStorage::TNodeWarden*>(actor);
            nodeWarden.ApplyLocalVDiskInfo(destroyVDisk);
            UNIT_ASSERT(!nodeWarden.LocalVDisks.contains(destroyVSlotId));
            UNIT_ASSERT(!nodeWarden.SlayInFlight.contains(destroyVSlotId));

            nodeWarden.LocalVDisks[wipeVSlotId].UnderlyingPDiskDestroyed = true;
            nodeWarden.ApplyLocalVDiskInfo(wipeVDisk);
            UNIT_ASSERT(nodeWarden.LocalVDisks.contains(wipeVSlotId));
            UNIT_ASSERT(!nodeWarden.SlayInFlight.contains(wipeVSlotId));
            UNIT_ASSERT(!nodeWarden.LocalVDisks.at(wipeVSlotId).UnderlyingPDiskDestroyed);
        });
    }

    void UpdateInferPDiskSlotCountSettings(TTestBasicRuntime& runtime, TActorId realNodeWarden,
            ui64 unitSize, ui32 maxSlots, bool preferInferredSettings) {
        auto request = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        auto& record = request->Record;
        auto* blobStorageConfig = record.MutableConfig()->MutableBlobStorageConfig();
        auto* inferSettings = blobStorageConfig->MutableInferPDiskSlotCountSettings();
        auto* inferRotSettings = inferSettings->MutableRot();

        inferRotSettings->SetUnitSize(unitSize);
        inferRotSettings->SetMaxSlots(maxSlots);
        inferRotSettings->SetPreferInferredSettingsOverExplicit(preferInferredSettings);

        TActorId sender = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(realNodeWarden, sender, request.release()));

        auto response = runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvConfigNotificationResponse>(sender);
        Y_UNUSED(response);
    }

    void UpdateInferPDiskSlotCountFromSlotSizeSettings(TTestBasicRuntime& runtime, TActorId realNodeWarden,
            ui64 slotSize, ui32 maxSlots, bool preferInferredSettings = false) {
        auto request = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        auto& record = request->Record;
        auto* blobStorageConfig = record.MutableConfig()->MutableBlobStorageConfig();
        auto* inferSettings = blobStorageConfig->MutableInferPDiskSlotCountSettings();
        auto* inferRotSettings = inferSettings->MutableRot();

        inferRotSettings->SetSlotSize(slotSize);
        inferRotSettings->SetMaxSlots(maxSlots);
        inferRotSettings->SetPreferInferredSettingsOverExplicit(preferInferredSettings);

        TActorId sender = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(realNodeWarden, sender, request.release()));

        auto response = runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvConfigNotificationResponse>(sender);
        Y_UNUSED(response);
    }

    CUSTOM_UNIT_TEST(TestInferPDiskSlotCountSettingsSlotSizeValidation) {
        NKikimrBlobStorage::TInferPDiskSlotCountSettings settings;
        settings.MutableRot()->SetSlotSize(600ull << 30);
        auto error = ValidateInferPDiskSlotCountSettings(
            settings, "BlobStorageConfig->InferPDiskSlotCountSettings");
        UNIT_ASSERT(error);
        UNIT_ASSERT_C(error->Contains("MaxSlots is mandatory with SlotSize or UnitSize"), *error);

        settings.MutableRot()->SetMaxSlots(16);
        UNIT_ASSERT(!ValidateInferPDiskSlotCountSettings(
            settings, "BlobStorageConfig->InferPDiskSlotCountSettings"));

        settings.MutableRot()->SetUnitSize(100_GB);
        error = ValidateInferPDiskSlotCountSettings(
            settings, "BlobStorageConfig->InferPDiskSlotCountSettings");
        UNIT_ASSERT(error);
        UNIT_ASSERT_C(error->Contains("SlotSize is mutually exclusive with UnitSize"), *error);
    }

    CUSTOM_UNIT_TEST(TestInferPDiskSlotCountExplicitConfig) {
        TTestBasicRuntime runtime(1, false);
        TActorId realNodeWarden = SetupNodeWardenOnly(runtime);
        UpdateInferPDiskSlotCountSettings(runtime, realNodeWarden,
            100_GB, 16, false);

        const ui32 nodeId = runtime.GetNodeId(0);
        const ui32 pdiskId = 1001;
        const TString pdiskPath = "SectorMap:TestInferPDiskSlotCountExplicitConfig:2400";

        TActorId fakeNodeWarden = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageNodeWardenID(nodeId), fakeNodeWarden);
        TActorId fakeWhiteboard = runtime.AllocateEdgeActor();
        runtime.RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId), fakeWhiteboard);

        VERBOSE_COUT("- Test case 1 - create PDisk");
        NKikimrBlobStorage::TPDiskConfig pdiskConfig;
        pdiskConfig.SetExpectedSlotCount(13);
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 13, 0u);

        VERBOSE_COUT("- Test case 2 - enable PreferInferredSettingsOverExplicit");
        UpdateInferPDiskSlotCountSettings(runtime, realNodeWarden,
            100_GB, 16, true);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 12, 2u);

        VERBOSE_COUT("- Test case 3 - update InferPDiskSlotCountSettings");
        UpdateInferPDiskSlotCountSettings(runtime, realNodeWarden,
            50_GB, 9, true);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 6, 8u);

        VERBOSE_COUT("- Test case 4 - remove InferPDiskSlotCountSettings");
        auto request = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        {
            auto& record = request->Record;
            record.MutableConfig()->MutableBlobStorageConfig();
            TActorId sender = runtime.AllocateEdgeActor();
            runtime.Send(new IEventHandle(realNodeWarden, sender, request.release()));
            runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvConfigNotificationResponse>(sender);
        }
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 13, 0u);
    }

    CUSTOM_UNIT_TEST(TestInferPDiskSlotCountFromSlotSizeWithRealNodeWarden) {
        TTestBasicRuntime runtime(1, false);
        TActorId realNodeWarden = SetupNodeWardenOnly(runtime);
        const ui64 expectedSlotSize = 600ull << 30;
        UpdateInferPDiskSlotCountFromSlotSizeSettings(runtime, realNodeWarden, expectedSlotSize, 64);

        const ui32 nodeId = runtime.GetNodeId(0);
        const ui32 pdiskId = 1006;
        const TString pdiskPath = "SectorMap:TestInferPDiskSlotCountFromSlotSizeWithRealNodeWarden:2400";

        TActorId fakeNodeWarden = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageNodeWardenID(nodeId), fakeNodeWarden);
        TActorId fakeWhiteboard = runtime.AllocateEdgeActor();
        runtime.RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId), fakeWhiteboard);

        NKikimrBlobStorage::TPDiskConfig pdiskConfig;
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 4, 0u, expectedSlotSize);

        const ui64 updatedExpectedSlotSize = 800ull << 30;
        UpdateInferPDiskSlotCountFromSlotSizeSettings(runtime, realNodeWarden, updatedExpectedSlotSize, 64);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 3, 0u, updatedExpectedSlotSize);

        UpdateInferPDiskSlotCountFromSlotSizeSettings(runtime, realNodeWarden, updatedExpectedSlotSize, 2);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 2, 0u, updatedExpectedSlotSize);

        pdiskConfig.SetExpectedSlotCount(17);
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 17, 0u);

        UpdateInferPDiskSlotCountFromSlotSizeSettings(runtime, realNodeWarden, updatedExpectedSlotSize, 64, true);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 3, 0u, updatedExpectedSlotSize);
    }

    CUSTOM_UNIT_TEST(TestExpectedSlotSizeCalculatesSlotCount) {
        TTestBasicRuntime runtime(1, false);
        TActorId realNodeWarden = SetupNodeWardenOnly(runtime);

        const ui32 nodeId = runtime.GetNodeId(0);
        const ui32 pdiskId = 1003;
        const TString pdiskPath = "SectorMap:TestExpectedSlotSizeCalculatesSlotCount:2400";
        const ui64 expectedSlotSize = 600ull << 30;

        TActorId fakeNodeWarden = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageNodeWardenID(nodeId), fakeNodeWarden);
        TActorId fakeWhiteboard = runtime.AllocateEdgeActor();
        runtime.RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId), fakeWhiteboard);

        NKikimrBlobStorage::TPDiskConfig pdiskConfig;
        pdiskConfig.SetExpectedSlotSize(expectedSlotSize);
        pdiskConfig.SetMaxSlots(8);
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 4, 0u, expectedSlotSize);

        const ui64 updatedExpectedSlotSize = 800ull << 30;
        pdiskConfig.SetExpectedSlotSize(updatedExpectedSlotSize);
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 3, 0u, updatedExpectedSlotSize);

        const ui64 smallExpectedSlotSize = 1ull << 30;
        pdiskConfig.SetExpectedSlotSize(smallExpectedSlotSize);
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 8u, 0u, smallExpectedSlotSize);
    }

    CUSTOM_UNIT_TEST(TestExpectedSlotSettingsTransitions) {
        TTestBasicRuntime runtime(1, false);
        TActorId realNodeWarden = SetupNodeWardenOnly(runtime);

        const ui32 nodeId = runtime.GetNodeId(0);
        const ui32 pdiskId = 1004;
        const TString pdiskPath = "SectorMap:TestExpectedSlotSettingsTransitions:2400";
        const ui64 expectedSlotSize = 600ull << 30;

        TActorId fakeNodeWarden = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageNodeWardenID(nodeId), fakeNodeWarden);
        TActorId fakeWhiteboard = runtime.AllocateEdgeActor();
        runtime.RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId), fakeWhiteboard);

        NKikimrBlobStorage::TPDiskConfig pdiskConfig;
        pdiskConfig.SetExpectedSlotCount(7);
        pdiskConfig.SetSlotSizeInUnits(2);
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 7, 2u);

        pdiskConfig.ClearExpectedSlotCount();
        pdiskConfig.ClearSlotSizeInUnits();
        pdiskConfig.SetExpectedSlotSize(expectedSlotSize);
        pdiskConfig.SetMaxSlots(8);
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 4, 0u, expectedSlotSize);

        pdiskConfig.ClearExpectedSlotSize();
        pdiskConfig.ClearMaxSlots();
        pdiskConfig.SetExpectedSlotCount(9);
        pdiskConfig.SetSlotSizeInUnits(3);
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 9, 3u);
    }

    CUSTOM_UNIT_TEST(TestInvalidExpectedSlotSettingsDoNotCrashNodeWarden) {
        TTestBasicRuntime runtime(1, false);
        TActorId realNodeWarden = SetupNodeWardenOnly(runtime);

        const ui32 nodeId = runtime.GetNodeId(0);
        const ui32 pdiskId = 1005;
        const TString pdiskPath = "SectorMap:TestInvalidExpectedSlotSettingsDoNotCrashNodeWarden:2400";
        const ui64 expectedSlotSize = 600ull << 30;

        TActorId fakeNodeWarden = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageNodeWardenID(nodeId), fakeNodeWarden);
        TActorId fakeWhiteboard = runtime.AllocateEdgeActor();
        runtime.RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId), fakeWhiteboard);

        NKikimrBlobStorage::TPDiskConfig pdiskConfig;
        pdiskConfig.SetExpectedSlotCount(17);
        pdiskConfig.SetSlotSizeInUnits(7);
        pdiskConfig.SetExpectedSlotSize(expectedSlotSize);
        pdiskConfig.SetMaxSlots(8);
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 4, 0u, expectedSlotSize);

        auto edge = runtime.AllocateEdgeActor();
        THttpRequestMock httpRequest;
        NMonitoring::TMonService2HttpRequest monService2HttpRequest(nullptr, &httpRequest, nullptr, nullptr, "",
            nullptr);
        runtime.Send(new IEventHandle(realNodeWarden, edge, new NMon::TEvHttpInfo(monService2HttpRequest)), 0);
        auto httpInfoRes = runtime.GrabEdgeEventRethrow<NMon::TEvHttpInfoRes>(edge, TDuration::Seconds(1));
        UNIT_ASSERT(httpInfoRes && httpInfoRes->Get());

        TStringStream out;
        httpInfoRes->Get()->Output(out);
        UNIT_ASSERT_C(out.Str().Contains("PDiskConfig has ExpectedSlotSize"), out.Str());
        UNIT_ASSERT_C(out.Str().Contains("ExpectedSlotCount# 17"), out.Str());
        UNIT_ASSERT_C(out.Str().Contains("SlotSizeInUnits# 7"), out.Str());
        UNIT_ASSERT_C(out.Str().Contains("ExpectedSlotSize and MaxSlots take precedence"), out.Str());

        // without MaxSlots the slot count cannot be derived from ExpectedSlotSize, so the
        // invalid setting must be ignored and the explicit slot settings kept in effect
        pdiskConfig.ClearMaxSlots();
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 17, 7u);
    }

    CUSTOM_UNIT_TEST(TestExpectedSlotSizeLargerThanDriveKeepsZeroSlotCount) {
        TTestBasicRuntime runtime(1, false);
        TActorId realNodeWarden = SetupNodeWardenOnly(runtime);

        const ui32 nodeId = runtime.GetNodeId(0);
        const ui32 pdiskId = 1007;
        const TString pdiskPath = "SectorMap:TestExpectedSlotSizeLargerThanDriveKeepsZeroSlotCount:2400";
        const ui64 expectedSlotSize = 3000ull << 30; // larger than the 2400 GB drive

        TActorId fakeNodeWarden = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageNodeWardenID(nodeId), fakeNodeWarden);
        TActorId fakeWhiteboard = runtime.AllocateEdgeActor();
        runtime.RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId), fakeWhiteboard);

        NKikimrBlobStorage::TPDiskConfig pdiskConfig;
        pdiskConfig.SetExpectedSlotSize(expectedSlotSize);
        pdiskConfig.SetMaxSlots(8);
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 0, 0u, expectedSlotSize);

        auto edge = runtime.AllocateEdgeActor();
        THttpRequestMock httpRequest;
        NMonitoring::TMonService2HttpRequest monService2HttpRequest(nullptr, &httpRequest, nullptr, nullptr, "",
            nullptr);
        runtime.Send(new IEventHandle(realNodeWarden, edge, new NMon::TEvHttpInfo(monService2HttpRequest)), 0);
        auto httpInfoRes = runtime.GrabEdgeEventRethrow<NMon::TEvHttpInfoRes>(edge, TDuration::Seconds(1));
        UNIT_ASSERT(httpInfoRes && httpInfoRes->Get());

        TStringStream out;
        httpInfoRes->Get()->Output(out);
        UNIT_ASSERT_C(out.Str().Contains("Drive is smaller than ExpectedSlotSize"), out.Str());
    }

    CUSTOM_UNIT_TEST(TestInferPDiskSlotCountWithRealNodeWarden) {
        TTestBasicRuntime runtime(1, false);
        TActorId realNodeWarden = SetupNodeWardenOnly(runtime);
        UpdateInferPDiskSlotCountSettings(runtime, realNodeWarden,
            100_GB, 16, false);

        const ui32 nodeId = runtime.GetNodeId(0);
        const ui32 pdiskId = 1002;
        const TString pdiskPath = "SectorMap:TestInferPDiskSlotCount:2400";

        TActorId fakeNodeWarden = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageNodeWardenID(nodeId), fakeNodeWarden);
        TActorId fakeWhiteboard = runtime.AllocateEdgeActor();
        runtime.RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId), fakeWhiteboard);

        VERBOSE_COUT("- Test case 1 - create PDisk");
        NKikimrBlobStorage::TPDiskConfig pdiskConfig;
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 12, 2u);

        VERBOSE_COUT("- Test case 1a - repeat the same ApplyServiceSet request");
        auto observer = runtime.AddObserver<NPDisk::TEvChangeExpectedSlotCount>(
            [](NPDisk::TEvChangeExpectedSlotCount::TPtr&) {
                UNIT_FAIL(TStringBuilder() << "EvChangeExpectedSlotCount shouldn't be sent in this case");
            });
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        runtime.SimulateSleep(TDuration::MilliSeconds(100));

        VERBOSE_COUT("- Test case 1b - change InferPDiskSlotCountSettings insignificantly");
        UpdateInferPDiskSlotCountSettings(runtime, realNodeWarden,
            100_GB, 18, false);
        runtime.SimulateSleep(TDuration::MilliSeconds(100));

        observer.Remove();

        VERBOSE_COUT("- Test case 2 - update InferPDiskSlotCountSettings");
        UpdateInferPDiskSlotCountSettings(runtime, realNodeWarden,
            100_GB, 24, false);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 24, 1u);

        VERBOSE_COUT("- Test case 3 - set ExpectedSlotCount explicitly");
        pdiskConfig.SetExpectedSlotCount(17);
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 17, 0u);
    }

    void ChangeGroupSizeInUnits(TTestBasicRuntime& runtime, TString poolName, ui32 groupId, ui32 groupSizeInUnits) {
        TActorId edge = runtime.AllocateEdgeActor();

        auto storagePool = DescribeStoragePool(runtime, poolName);
        auto request = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto& cmd = *request->Record.MutableRequest()->AddCommand()->MutableChangeGroupSizeInUnits();
        cmd.SetBoxId(storagePool.GetBoxId());
        cmd.SetItemConfigGeneration(storagePool.GetItemConfigGeneration());
        cmd.SetStoragePoolId(storagePool.GetStoragePoolId());
        cmd.AddGroupId(groupId);
        cmd.SetSizeInUnits(groupSizeInUnits);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        runtime.SendToPipe(MakeBSControllerID(), edge, request.release(), 0, pipeConfig);

        auto reply = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(edge);
        VERBOSE_COUT("TEvControllerConfigResponse# " << reply->ToString());
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetResponse().GetSuccess(), true);
    }

    void CheckVDiskStateUpdate(TTestBasicRuntime& runtime, TActorId fakeWhiteboard, ui32 groupId,
            ui32 expectedGroupGeneration, ui32 expectedGroupSizeInUnits,
            TDuration simTimeout = TDuration::Seconds(10)) {
        TInstant deadline = runtime.GetCurrentTime() + simTimeout;
        while (true) {
            UNIT_ASSERT_LT(runtime.GetCurrentTime(), deadline);

            const auto ev = runtime.GrabEdgeEventRethrow<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateUpdate>(fakeWhiteboard, deadline - runtime.GetCurrentTime());
            VERBOSE_COUT(" Got TEvVDiskStateUpdate# " << ev->ToString());

            NKikimrWhiteboard::TVDiskStateInfo vdiskInfo = ev->Get()->Record;
            if (vdiskInfo.GetVDiskId().GetGroupID() != groupId || !vdiskInfo.HasGroupSizeInUnits()) {
                continue;
            }

            UNIT_ASSERT_VALUES_EQUAL(vdiskInfo.GetVDiskId().GetGroupGeneration(), expectedGroupGeneration);
            UNIT_ASSERT_VALUES_EQUAL(vdiskInfo.GetGroupSizeInUnits(), expectedGroupSizeInUnits);
            break;
        }
    }

    CUSTOM_UNIT_TEST(TestEvVGenerationChangeRace) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, "", nullptr);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY, NLog::PRI_ERROR);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_PUT, NLog::PRI_ERROR);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY_BLOCK, NLog::PRI_ERROR);
        runtime.SetLogPriority(NKikimrServices::BS_SKELETON, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::BS_LOCALRECOVERY, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_INFO);
        runtime.SetLogPriority(NKikimrServices::BS_CONTROLLER, NLog::PRI_INFO);

        const ui32 nodeId = runtime.GetNodeId(0);
        TActorId fakeWhiteboard = runtime.AllocateEdgeActor();
        runtime.RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId), fakeWhiteboard);

        VERBOSE_COUT(" Starting test");

        TBlockEvents<TEvBlobStorage::TEvLocalRecoveryDone> block(runtime);

        const TString poolName = "testEvVGenerationChangeRace";
        CreateStoragePool(runtime, poolName, "pool-kind-1");
        ui32 groupId = GetGroupFromPool(runtime, poolName);

        CheckVDiskStateUpdate(runtime, fakeWhiteboard, groupId, 1, 0u);
        ChangeGroupSizeInUnits(runtime, poolName, groupId, 2u);
        CheckVDiskStateUpdate(runtime, fakeWhiteboard, groupId, 1, 0u);
        block.Stop().Unblock();
        CheckVDiskStateUpdate(runtime, fakeWhiteboard, groupId, 2, 2u);
    }

    class TSilentPDiskActor : public TActorBootstrapped<TSilentPDiskActor> {
    public:
        void Bootstrap() {
            Become(&TThis::StateFunc);
        }

        STFUNC(StateFunc) {
            if (ev->GetTypeRewrite() == TEvents::TSystem::Poison) {
                PassAway();
            }
        }
    };

    class TSilentPDiskSubsystem : public IPDiskSubsystem {
    public:
        void Start(const TActorContext& ctx, ui32 pdiskId, const TIntrusivePtr<TPDiskConfig>&,
                const NPDisk::TMainKey&, ui32 poolId, ui32 nodeId) override {
            const TActorId actorId = ctx.Register(new TSilentPDiskActor, TMailboxType::HTSwap, poolId);
            ctx.ActorSystem()->RegisterLocalService(MakeBlobStoragePDiskID(nodeId, pdiskId), actorId);
        }
    };

    struct TDDiskLifecycleTestSetup {
        static constexpr ui32 NodeId = 1;
        static constexpr ui32 PDiskId = 1;
        static constexpr ui32 VDiskSlotId = 1;

        TTempDir TempDir;
        std::shared_ptr<NPDisk::IIoContextFactory> IoFactory = std::make_shared<NPDisk::TIoContextFactoryOSS>();
        TTestActorSystem Runtime;
        const ui32 GroupId;
        const TVDiskID VDiskId;
        const TActorId DDiskServiceId;
        TActorId NodeWardenId;

        TDDiskLifecycleTestSetup(bool native = false)
            : Runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>())
            , GroupId(TGroupID(EGroupConfigurationType::Dynamic, 1, 1).GetRaw())
            , VDiskId(GroupId, 1, 0, 0, 0)
            , DDiskServiceId(MakeBlobStorageDDiskId(NodeId, PDiskId, VDiskSlotId))
        {
            if (native) {
                TFile file(TempDir() + "/pdisk.dat", CreateAlways | RdWr);
                file.Resize(ui64{16} << 30);
                file.Close();
                TFormatOptions options;
                options.EnableSmallDiskOptimization = true;
                FormatPDisk(TempDir() + "/pdisk.dat", ui64{16} << 30, 4096, 16 << 20,
                    12345, 1, 2, 3, NPDisk::YdbDefaultPDiskSequence, "requested restart", options);
            }
            Runtime.SetupNodeSubSystems = [native](ui32, TActorSystemSetup* setup) {
                if (native) {
                    setup->RegisterSubSystem<IPDiskSubsystem>(CreatePDiskSubsystem());
                } else {
                    setup->RegisterSubSystem<IPDiskSubsystem>(std::make_unique<TSilentPDiskSubsystem>());
                }
            };
            Runtime.Start();

            auto& appData = *Runtime.GetNode(NodeId)->AppData;
            appData.IoContextFactory = IoFactory.get();
            appData.DomainsInfo->AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dom", 1).Release());
            appData.DynamicNameserviceConfig = new TDynamicNameserviceConfig();
            appData.DynamicNameserviceConfig->MaxStaticNodeId = NodeId;

            TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig(
                new TNodeWardenConfig());
            nodeWardenConfig->DDiskConfig.emplace();
            nodeWardenConfig->DDiskConfig->SetForcePDiskFallback(!native);
            if (native) {
                nodeWardenConfig->DDiskConfig->SetEnableChecksums(false);
                nodeWardenConfig->PBufferConfig.emplace();
                nodeWardenConfig->PBufferConfig->SetEnableWritesBatching(false);
                nodeWardenConfig->PBufferConfig->SetMaxInMemoryCache(0);
                nodeWardenConfig->FeatureFlags->SetEnableSmallDiskOptimization(true);
                nodeWardenConfig->PDiskKey = {.Keys = {NPDisk::YdbDefaultPDiskSequence}, .IsInitialized = true};
            }

            auto* serviceSet = nodeWardenConfig->BlobStorageConfig->MutableServiceSet();
            auto* pdisk = serviceSet->AddPDisks();
            pdisk->SetNodeID(NodeId);
            pdisk->SetPDiskID(PDiskId);
            pdisk->SetPath(native ? TempDir() + "/pdisk.dat" : "silent-pdisk");
            pdisk->SetPDiskGuid(12345);
            pdisk->SetPDiskCategory(TPDiskCategory(NPDisk::DEVICE_TYPE_NVME, 0).GetRaw());

            auto* group = serviceSet->AddGroups();
            FillGroup(group, VDiskId.GroupGeneration);

            if (!native) { FillDDisk(serviceSet->AddVDisks()); }

            NodeWardenId = Runtime.Register(CreateBSNodeWarden(nodeWardenConfig.Release()), NodeId);
            Runtime.RegisterService(MakeBlobStorageNodeWardenID(NodeId), NodeWardenId);
            UNIT_ASSERT(Runtime.WrapInActorContext(NodeWardenId, [](IActor* actor) {
                dynamic_cast<NStorage::TNodeWarden*>(actor)->Bootstrap();
            }));
            if (!native) { UNIT_ASSERT(LookupDDiskActor()); }
        }

        ~TDDiskLifecycleTestSetup() {
            Runtime.Stop();
        }

        void FillGroup(NKikimrBlobStorage::TGroupInfo* group, ui32 generation) const {
            group->SetGroupID(GroupId);
            group->SetGroupGeneration(generation);
            group->SetErasureSpecies(TBlobStorageGroupType::ErasureNone);
            group->SetStoragePoolName("ddisk-pool");
            group->SetDDisk(true);
            auto* location = group->AddRings()->AddFailDomains()->AddVDiskLocations();
            FillLocation(location);
        }

        void FillLocation(NKikimrBlobStorage::TVDiskLocation* location) const {
            location->SetNodeID(NodeId);
            location->SetPDiskID(PDiskId);
            location->SetVDiskSlotID(VDiskSlotId);
            location->SetPDiskGuid(12345);
        }

        void FillDDisk(NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk* vdisk) const {
            VDiskIDFromVDiskID(VDiskId, vdisk->MutableVDiskID());
            FillLocation(vdisk->MutableVDiskLocation());
            vdisk->SetStoragePoolName("ddisk-pool");
        }

        TActorId LookupDDiskActor() {
            return Runtime.GetNode(NodeId)->ActorSystem->LookupLocalService(DDiskServiceId);
        }

        bool IsActorAlive(const TActorId& actorId) {
            return Runtime.WrapInActorContext(actorId, [](IActor*) {});
        }

        template<typename TPredicate>
        void DispatchUntil(TPredicate&& predicate, TStringBuf description) {
            ui32 eventsProcessed = 0;
            Runtime.Sim([&] {
                return !predicate() && ++eventsProcessed <= 200;
            });
            UNIT_ASSERT_C(predicate(), description);
        }

        void DeleteDDisk() {
            UNIT_ASSERT(Runtime.WrapInActorContext(NodeWardenId, [&](IActor* actor) {
                NKikimrBlobStorage::TNodeWardenServiceSet serviceSet;
                auto* vdisk = serviceSet.AddVDisks();
                FillDDisk(vdisk);
                vdisk->SetEntityStatus(NKikimrBlobStorage::DESTROY);
                dynamic_cast<NStorage::TNodeWarden*>(actor)->ApplyServiceSet(
                    serviceSet, true, false, false, "test");
            }));
        }

        void RestartDDisk() {
            Runtime.Send(new IEventHandle(NodeWardenId, {}, new TEvBlobStorage::TEvAskRestartVDisk(PDiskId, VDiskId)),
                NodeId);
        }

    };

#if defined(__linux__)
    struct TRequestedPDiskRestartFixture : TDDiskLifecycleTestSetup {
        using TStatus = NKikimrBlobStorage::NDDisk::TReplyStatus;
        TActorId Edge;
        TActorId PBService;
        std::shared_ptr<NPDisk::TUringRouter> Router;
        std::unique_ptr<TEventHandle<NPDisk::TEvYardInitResult>> RetainedInit;
        std::atomic<bool> Armed{false};
        std::atomic<ui32> Callbacks{0};
        std::atomic<ui32> Admissions{0};
        TManualEvent FirstCallback, SecondCallback, ReleaseFirst, ReleaseSecond;

        TRequestedPDiskRestartFixture()
            : TDDiskLifecycleTestSetup(true)
            , Edge(Runtime.AllocateEdgeActor(NodeId))
            , PBService(MakeBlobStoragePersistentBufferId(NodeId, PDiskId, VDiskSlotId))
        {
            NPDisk::TMainKey key{.Keys = {NPDisk::YdbDefaultPDiskSequence}, .IsInitialized = true};
            Send(MakeBlobStoragePDiskID(NodeId, PDiskId),
                new NPDisk::TEvYardControl(NPDisk::TEvYardControl::PDiskStart, &key));
            auto started = Grab<NPDisk::TEvYardControlResult>();
            UNIT_ASSERT_VALUES_EQUAL_C(started->Get()->Status, NKikimrProto::OK, started->Get()->ErrorReason);
            Send(MakeBlobStoragePDiskID(NodeId, PDiskId),
                new NPDisk::TEvYardControl(NPDisk::TEvYardControl::GetPDiskPointer, nullptr));
            auto pointer = Grab<NPDisk::TEvYardControlResult>();
            UNIT_ASSERT_VALUES_EQUAL(pointer->Get()->Status, NKikimrProto::OK);
            auto* pdisk = reinterpret_cast<NPDisk::TPDisk*>(pointer->Get()->Cookie);
            {
                TGuard<TMutex> guard(pdisk->StateMutex);
                NPDisk::TPDiskTestPeer::ConfigureRouter(*pdisk, [&](NPDisk::TUringRouter& router) {
                    NPDisk::NUringPrivate::TRouterHooks hooks;
                    hooks.AfterAdmission = [&] { if (Armed.load()) { ++Admissions; } };
                    hooks.BeforeTerminalCallback = [&] {
                        if (!Armed.load()) { return; }
                        const auto index = ++Callbacks;
                        if (index == 1) {
                            FirstCallback.Signal();
                            ReleaseFirst.WaitI();
                        } else if (index == 2) {
                            SecondCallback.Signal();
                            ReleaseSecond.WaitI();
                        }
                    };
                    NPDisk::TUringRouterTestPeer::SetHooks(router, std::move(hooks));
                });
            }
            Runtime.WrapInActorContext(NodeWardenId, [&](IActor* actor) {
                NKikimrBlobStorage::TNodeWardenServiceSet incoming;
                FillDDisk(incoming.AddVDisks());
                static_cast<NStorage::TNodeWarden*>(actor)->ApplyServiceSet(incoming, true, false, false, "attach DDisk");
            });
        }

        ~TRequestedPDiskRestartFixture() {
            Armed.store(false);
            ReleaseFirst.Signal();
            ReleaseSecond.Signal();
            Runtime.FilterFunction = {};
            Runtime.FilterEnqueue = {};
            // Retire all callbacks while the instance-local hook state is alive.
            Runtime.Stop();
        }

        void Send(TActorId recipient, IEventBase* event, ui64 cookie = 0) {
            Runtime.Send(new IEventHandle(recipient, Edge, event, 0, cookie), NodeId);
        }
        template<class TEvent>
        std::unique_ptr<TEventHandle<TEvent>> Grab() { return Runtime.WaitForEdgeActorEvent<TEvent>(Edge, false); }
        template<class TEvent>
        void ExpectOk() {
            auto reply = Grab<TEvent>();
            UNIT_ASSERT(reply->Get()->Record.GetStatus() == TStatus::OK);
        }
        NDDisk::TQueryCredentials Connect(TActorId recipient) {
            auto creds = recipient == PBService
                ? NDDisk::TQueryCredentials::ToPersistentBuffer(901, 1, std::nullopt, 0)
                : NDDisk::TQueryCredentials::ToDDisk(901, 1, 0, std::nullopt, 0);
            Send(recipient, new NDDisk::TEvConnect(creds));
            auto reply = Grab<NDDisk::TEvConnectResult>();
            UNIT_ASSERT_C(reply->Get()->Record.GetStatus() == TStatus::OK, reply->Get()->Record.DebugString());
            creds.DDiskInstanceGuid = reply->Get()->Record.GetDDiskInstanceGuid();
            creds.ConnectionToken.emplace(reply->Get()->Record.GetConnectionToken());
            return creds;
        }
        void Write(bool pb, const NDDisk::TQueryCredentials& creds, char value, ui64 lsn) {
            auto buffer = TRcBuf::UninitializedPageAligned(4096);
            memset(buffer.GetDataMut(), value, 4096);
            if (pb) {
                auto request = std::make_unique<NDDisk::TEvWritePersistentBuffer>(
                    creds, NDDisk::TBlockSelector(0, 0, 4096), lsn, NDDisk::TWriteInstruction(0));
                request->AddPayload(TRope(std::move(buffer)));
                Send(PBService, request.release(), lsn);
            } else {
                auto request = std::make_unique<NDDisk::TEvWrite>(
                    creds, NDDisk::TBlockSelector(0, 0, 4096), NDDisk::TWriteInstruction(0));
                request->AddPayload(TRope(std::move(buffer)));
                Send(DDiskServiceId, request.release(), lsn);
            }
        }
        void WriteReply(bool pb) {
            if (pb) { ExpectOk<NDDisk::TEvWritePersistentBufferResult>(); }
            else { ExpectOk<NDDisk::TEvWriteResult>(); }
        }
        void Read(bool pb, const NDDisk::TQueryCredentials& creds, char value, ui64 lsn) {
            if (pb) {
                Send(PBService, new NDDisk::TEvReadPersistentBuffer(creds, {0, 0, 4096}, lsn, creds.Generation, {true}));
                auto result = Grab<NDDisk::TEvReadPersistentBufferResult>();
                UNIT_ASSERT_C(result->Get()->Record.GetStatus() == TStatus::OK, result->Get()->Record.DebugString());
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->GetPayload(0).ConvertToString(), TString(4096, value));
            } else {
                Send(DDiskServiceId, new NDDisk::TEvRead(creds, {0, 0, 4096}, {true}));
                auto result = Grab<NDDisk::TEvReadResult>();
                UNIT_ASSERT_C(result->Get()->Record.GetStatus() == TStatus::OK, result->Get()->Record.DebugString());
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->GetPayload(0).ConvertToString(), TString(4096, value));
            }
        }

        void Run(bool parentFirst) {
            Cerr << "requested restart: connect parent" << Endl;
            auto parentCreds = Connect(DDiskServiceId);
            Cerr << "requested restart: connect PB" << Endl;
            auto pbCreds = Connect(PBService);
            Cerr << "requested restart: warm parent" << Endl;
            Write(false, parentCreds, 'A', 1);
            WriteReply(false);
            Cerr << "requested restart: warm PB" << Endl;
            Write(true, pbCreds, 'B', 1);
            WriteReply(true);
            // Keep a real additional initialization result alive across restart.
            Send(MakeBlobStoragePDiskID(NodeId, PDiskId), new NPDisk::TEvYardInit(
                3, TVDiskID(GroupId + 1, 1, 0, 0, 0), 12345, {}, {}, 2, 0, true));
            Cerr << "requested restart: retain init" << Endl;
            RetainedInit = Grab<NPDisk::TEvYardInitResult>();
            UNIT_ASSERT_VALUES_EQUAL(RetainedInit->Get()->Status, NKikimrProto::OK);
            Router = std::dynamic_pointer_cast<NPDisk::TUringRouter>(RetainedInit->Get()->UringRouter);
            UNIT_ASSERT(Router);
            Cerr << "requested restart: quiesce actors" << Endl;
            // Checksums-disabled allocation can continue zero-formatting reserve
            // chunks after the warm-up client reply. Retire its actor mailbox work too.
            Runtime.Sim([&] {
                bool busy = false;
                for (const auto service : {DDiskServiceId, PBService}) {
                    const auto id = Runtime.GetNode(NodeId)->ActorSystem->LookupLocalService(service);
                    Runtime.WrapInActorContext(id, [&](IActor* actor) {
                        auto& disk = *static_cast<NDDisk::TDDiskActor*>(actor);
                        busy |= disk.GetDirectIoInflight() || !disk.FormattingChunks.empty() || !disk.LogCallbacks.empty();
                    });
                }
                return busy;
            });
            NPDisk::TUringRouterTestPeer::WaitSync(*Router);
            const auto parent = LookupDDiskActor();
            const auto child = Runtime.GetNode(NodeId)->ActorSystem->LookupLocalService(PBService);
            const auto oldPDisk = Runtime.GetNode(NodeId)->ActorSystem->LookupLocalService(MakeBlobStoragePDiskID(NodeId, PDiskId));
            ui32 permissions = 0;
            std::vector<TActorId> gone;
            std::unique_ptr<IEventHandle> replacementBootstrap;
            Runtime.FilterEnqueue = [&](ui32, std::unique_ptr<IEventHandle>& ev, ISchedulerCookie*, TInstant) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::TEvAskWardenRestartPDiskResult::EventType) {
                    UNIT_ASSERT_VALUES_EQUAL(gone.size(), 2);
                    UNIT_ASSERT_VALUES_EQUAL(gone[0], child);
                    UNIT_ASSERT_VALUES_EQUAL(gone[1], parent);
                    ++permissions;
                }
                if (ev->GetTypeRewrite() == TEvents::TEvGone::EventType && (ev->Sender == parent || ev->Sender == child)) {
                    gone.push_back(ev->Sender);
                }
                return true;
            };
            Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (permissions && ev->GetTypeRewrite() == TEvents::TSystem::Bootstrap
                        && ev->Recipient != oldPDisk
                        && ev->Recipient == Runtime.GetNode(NodeId)->ActorSystem->LookupLocalService(MakeBlobStoragePDiskID(NodeId, PDiskId))) {
                    replacementBootstrap = std::move(ev);
                    return false;
                }
                return true;
            };
            Cerr << "requested restart: arm native callbacks" << Endl;
            Armed.store(true);
            Write(!parentFirst, parentFirst ? parentCreds : pbCreds, parentFirst ? 'C' : 'D', 2);
            Runtime.Sim([&] { return Callbacks.load() == 0; });
            Cerr << "requested restart: first callback" << Endl;
            FirstCallback.WaitI();
            Write(parentFirst, parentFirst ? pbCreds : parentCreds, parentFirst ? 'D' : 'C', 2);
            Cerr << "requested restart: admit second" << Endl;
            Runtime.Sim([&] { return Admissions.load() < 2; });
            UNIT_ASSERT_VALUES_EQUAL(Admissions.load(), 2);
            Runtime.WrapInActorContext(NodeWardenId, [&](IActor* actor) {
                auto& warden = *static_cast<NStorage::TNodeWarden*>(actor);
                NKikimrBlobStorage::TNodeWardenServiceSet incoming;
                auto* pdisk = incoming.AddPDisks();
                *pdisk = NStorage::TNodeWardenTestPeer::GetPDisk(warden, PDiskId);
                pdisk->SetEntityStatus(NKikimrBlobStorage::RESTART);
                warden.ApplyServiceSet(incoming, true, false, false, "requested restart");
            });
            auto assertFence = [&] {
                Send(parent, new NDDisk::TEvConnect());
                auto rejected = Grab<NDDisk::TEvConnectResult>();
                UNIT_ASSERT(rejected->Get()->Record.GetStatus() == TStatus::SESSION_MISMATCH);
                UNIT_ASSERT_VALUES_EQUAL(permissions, 0);
                UNIT_ASSERT_VALUES_EQUAL(LookupDDiskActor(), parent);
                const auto currentChild = Runtime.GetNode(NodeId)->ActorSystem->LookupLocalService(PBService);
                UNIT_ASSERT(!currentChild || currentChild == child);
                UNIT_ASSERT_VALUES_EQUAL(Runtime.GetNode(NodeId)->ActorSystem->LookupLocalService(MakeBlobStoragePDiskID(NodeId, PDiskId)), oldPDisk);
                UNIT_ASSERT(std::find(gone.begin(), gone.end(), parent) == gone.end());
            };
            Cerr << "requested restart: release first" << Endl;
            assertFence();
            ReleaseFirst.Signal();
            WriteReply(!parentFirst);
            Cerr << "requested restart: second callback" << Endl;
            SecondCallback.WaitI();
            assertFence();
            Cerr << "requested restart: release second" << Endl;
            ReleaseSecond.Signal();
            WriteReply(parentFirst);
            Armed.store(false);
            Cerr << "requested restart: replacement bootstrap" << Endl;
            Runtime.Sim([&] { return !replacementBootstrap; });
            UNIT_ASSERT(replacementBootstrap);
            UNIT_ASSERT_VALUES_EQUAL(gone.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(gone[0], child);
            UNIT_ASSERT_VALUES_EQUAL(gone[1], parent);
            UNIT_ASSERT_VALUES_EQUAL(permissions, 1);
            UNIT_ASSERT(NPDisk::TUringRouterTestPeer::Retired(*Router));
            struct TRejected : NPDisk::TUringOperationBase {
                ui32 Callbacks = 0;
                void OnComplete(TActorSystem*) noexcept override { ++Callbacks; }
                void OnDrop(TActorSystem*) noexcept override { ++Callbacks; }
            } rejected;
            alignas(4096) char buffer[4096];
            rejected.SetOperationType(NPDisk::TUringOperationBase::EREAD);
            rejected.PrepareIov(buffer, sizeof(buffer), 0);
            UNIT_ASSERT(!RetainedInit->Get()->UringRouter->Read(&rejected));
            UNIT_ASSERT_VALUES_EQUAL(rejected.Callbacks, 0);
            TFile probe(TempDir() + "/pdisk.dat", OpenExisting | RdWr);
            UNIT_ASSERT_VALUES_EQUAL(flock(probe.GetHandle(), LOCK_EX | LOCK_NB), 0);
            UNIT_ASSERT_VALUES_EQUAL(flock(probe.GetHandle(), LOCK_UN), 0);
            probe.Close();
            Runtime.FilterFunction = {};
            Runtime.Send(std::move(replacementBootstrap), NodeId);
            Runtime.Sim([&] { return !LookupDDiskActor() || LookupDDiskActor() == parent; });
            UNIT_ASSERT_UNEQUAL(LookupDDiskActor(), parent);
            Cerr << "requested restart: reconnect" << Endl;
            parentCreds = Connect(DDiskServiceId);
            pbCreds = Connect(PBService);
            UNIT_ASSERT_UNEQUAL(Runtime.GetNode(NodeId)->ActorSystem->LookupLocalService(PBService), child);
            Read(false, parentCreds, 'C', 2);
            Read(true, pbCreds, 'D', 2);
            Write(false, parentCreds, 'E', 3);
            WriteReply(false);
            Write(true, pbCreds, 'F', 3);
            WriteReply(true);
            Read(false, parentCreds, 'E', 3);
            Read(true, pbCreds, 'F', 3);
            UNIT_ASSERT_VALUES_EQUAL(permissions, 1);
        }
    };

    Y_UNIT_TEST(RequestedPDiskRestartDrainsBothNativeCompletionOrders) {
        if (!NPDisk::RequireUring()) { return; }
        for (const bool parentFirst : {false, true}) {
            TRequestedPDiskRestartFixture fixture;
            fixture.Run(parentFirst);
        }
    }
#endif

    Y_UNIT_TEST(TestDDiskDeleteStopsRunningActor) {
        TDDiskLifecycleTestSetup setup;
        const TActorId actorId = setup.LookupDDiskActor();

        setup.DeleteDDisk();
        setup.DispatchUntil([&] { return !setup.IsActorAlive(actorId); },
            "DDisk actor must stop after its VDisk is deleted");
    }

    Y_UNIT_TEST(TestDDiskGoneAllowsRestart) {
        TDDiskLifecycleTestSetup setup;
        const TActorId previousActorId = setup.LookupDDiskActor();

        setup.RestartDDisk();
        setup.DispatchUntil([&] {
            const TActorId currentActorId = setup.LookupDDiskActor();
            return currentActorId && currentActorId != previousActorId;
        }, "NodeWarden must restart DDisk after receiving TEvGone");

        UNIT_ASSERT(!setup.IsActorAlive(previousActorId));
        UNIT_ASSERT(setup.IsActorAlive(setup.LookupDDiskActor()));
    }

    Y_UNIT_TEST(TestDeletedDDiskIncarnationFencesRecreationUntilGone) {
        TDDiskLifecycleTestSetup setup;
        const auto oldActor = setup.LookupDDiskActor();
        std::unique_ptr<IEventHandle> gone;
        std::unique_ptr<IEventHandle> slay;
        setup.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvents::TEvGone::EventType && ev->Sender == oldActor) {
                gone = std::move(ev);
                return false;
            }
            if (ev->GetTypeRewrite() == NPDisk::TEvSlay::EventType) {
                slay = std::move(ev);
                return false;
            }
            return true;
        };
        setup.DeleteDDisk();
        setup.DispatchUntil([&] { return bool(gone); }, "The real DDisk must publish Gone");
        setup.DispatchUntil([&] { return bool(slay); }, "Slot deletion must request PDisk slay");
        const auto* request = slay->Get<NPDisk::TEvSlay>();
        setup.Runtime.Send(new IEventHandle(setup.NodeWardenId, slay->Recipient,
            new NPDisk::TEvSlayResult(NKikimrProto::OK, 0, request->VDiskId,
                request->SlayOwnerRound, setup.PDiskId, setup.VDiskSlotId, {})), setup.NodeId);
        setup.DispatchUntil([&] {
            bool complete = false;
            setup.Runtime.WrapInActorContext(setup.NodeWardenId, [&](IActor* actor) {
                complete = static_cast<NStorage::TNodeWarden*>(actor)->SlayInFlight.empty();
            });
            return complete;
        }, "Slay must complete before recreating the slot");
        setup.Runtime.WrapInActorContext(setup.NodeWardenId, [&](IActor* actor) {
            auto& warden = *static_cast<NStorage::TNodeWarden*>(actor);
            NKikimrBlobStorage::TNodeWardenServiceSet incoming;
            setup.FillDDisk(incoming.AddVDisks());
            warden.ApplyServiceSet(incoming, true, false, false, "recreate while Gone is held");
            const auto& record = warden.LocalVDisks.at({setup.NodeId, setup.PDiskId, setup.VDiskSlotId});
            UNIT_ASSERT(!record.RuntimeData);
            UNIT_ASSERT(warden.DDiskActors.contains(oldActor));
        });
        setup.Runtime.FilterFunction = {};
        setup.Runtime.Send(std::move(gone), setup.NodeId);
        setup.DispatchUntil([&] {
            const auto current = setup.LookupDDiskActor();
            return current && current != oldActor && setup.IsActorAlive(current);
        }, "Replacement starts only after old incarnation Gone");
    }

    struct TStaticGroupProxyTestSetup {
        TTestActorSystem Runtime;
        ui32 NodeId = 1;
        ui32 StaticGroupId;
        TActorId NodeWardenId;

        TStaticGroupProxyTestSetup(ui32 maxStaticNodeId)
            : Runtime(1, NLog::PRI_ERROR, MakeIntrusive<TDomainsInfo>())
            , StaticGroupId(TGroupID(EGroupConfigurationType::Static, 1, 0).GetRaw())
        {
            Runtime.SetupNodeSubSystems = [](ui32, TActorSystemSetup* setup) {
                setup->RegisterSubSystem<IPDiskSubsystem>(CreatePDiskSubsystem());
            };
            Runtime.Start();

            auto& appData = *Runtime.GetNode(1)->AppData;
            appData.DomainsInfo->AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dom", 1).Release());
            appData.DynamicNameserviceConfig = new TDynamicNameserviceConfig();
            appData.DynamicNameserviceConfig->MaxStaticNodeId = maxStaticNodeId;

            TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig(new TNodeWardenConfig());
            ObtainStaticKey(&nodeWardenConfig->StaticKey);

            auto* serviceSet = nodeWardenConfig->BlobStorageConfig->MutableServiceSet();
            auto* group = serviceSet->AddGroups();
            group->SetGroupID(StaticGroupId);
            group->SetGroupGeneration(1);
            group->SetErasureSpecies(TErasureType::ErasureNone);
            auto* ring = group->AddRings();
            auto* failDomain = ring->AddFailDomains();
            auto* vdiskLoc = failDomain->AddVDiskLocations();
            vdiskLoc->SetNodeID(NodeId);
            vdiskLoc->SetPDiskID(1);
            vdiskLoc->SetVDiskSlotID(0);
            vdiskLoc->SetPDiskGuid(12345);

            IActor* ac = CreateBSNodeWarden(nodeWardenConfig.Release());
            NodeWardenId = Runtime.Register(ac, NodeId);
            Runtime.RegisterService(MakeBlobStorageNodeWardenID(NodeId), NodeWardenId);
        }

        void Bootstrap() {
            Runtime.WrapInActorContext(NodeWardenId, [](IActor* wardenActor) {
                auto& warden = *dynamic_cast<NStorage::TNodeWarden*>(wardenActor);
                warden.Bootstrap();
            });
        }

        bool HasGroupProxy() {
            bool result = false;
            Runtime.WrapInActorContext(NodeWardenId, [this, &result](IActor* wardenActor) {
                auto& warden = *dynamic_cast<NStorage::TNodeWarden*>(wardenActor);
                result = warden.HasGroupProxy(StaticGroupId);
            });
            return result;
        }

        void SimulateForwardedRequest() {
            Runtime.WrapInActorContext(NodeWardenId, [this](IActor* wardenActor) {
                auto& warden = *dynamic_cast<NStorage::TNodeWarden*>(wardenActor);
                TActorId sender = Runtime.AllocateEdgeActor(NodeId);
                TActorId proxyId = MakeBlobStorageProxyID(StaticGroupId);
                auto ev = std::make_unique<TEvBlobStorage::TEvStatus>(TInstant::Max());
                TAutoPtr<IEventHandle> handle(new IEventHandle(
                    warden.SelfId(),
                    sender,
                    ev.release(),
                    IEventHandle::FlagForwardOnNondelivery,
                    0,
                    &proxyId
                ));
                warden.HandleForwarded(handle);
            });
        }
    };

    Y_UNIT_TEST(TestDynamicNodeLazyStaticGroupProxyCreation) {
        // MaxStaticNodeId = 0 means node 1 is a dynamic node
        TStaticGroupProxyTestSetup setup(0);
        setup.Bootstrap();

        UNIT_ASSERT_C(!setup.HasGroupProxy(),
            "Static group proxy should not be created at startup on dynamic node");

        setup.SimulateForwardedRequest();

        UNIT_ASSERT_C(setup.HasGroupProxy(),
            "Static group proxy should be created on-demand after a request on dynamic node");
    }

    Y_UNIT_TEST(TestStaticNodeLazyStaticGroupProxyCreation) {
        // MaxStaticNodeId = 100 means node 1 is a static node
        TStaticGroupProxyTestSetup setup(100);
        setup.Bootstrap();

        UNIT_ASSERT_C(!setup.HasGroupProxy(),
            "Static group proxy should NOT be created at startup even on static node");

        setup.SimulateForwardedRequest();

        UNIT_ASSERT_C(setup.HasGroupProxy(),
            "Static group proxy should be created on-demand after a request on static node");
    }

}

} // namespace NBlobStorageNodeWardenTest
} // namespace NKikimr
