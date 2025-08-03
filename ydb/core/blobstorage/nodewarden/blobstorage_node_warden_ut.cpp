#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_http_request.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/protos/blobstorage/entity_status.pb.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <ydb/library/pdisk_io/sector_map.h>
#include <ydb/core/util/random.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/testing/unittest/registar.h>

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

using namespace NActors;

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

    NKikimr::FormatPDisk(path, diskSize, 4 << 10, chunkSize,
            guid, chunkKey, logKey,
            sysLogKey, NPDisk::YdbDefaultPDiskSequence, "Test",
            false, false, sectorMap, enableSmallDiskOptimization);
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

void SetupServices(TTestActorRuntime &runtime, TString extraPath, TIntrusivePtr<NPDisk::TSectorMap> extraSectorMap) {
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

    if (false) { // setup channel profiles
        TIntrusivePtr<TChannelProfiles> channelProfiles = new TChannelProfiles;
        channelProfiles->Profiles.emplace_back();
        TChannelProfiles::TProfile &profile = channelProfiles->Profiles.back();
        for (ui32 channelIdx = 0; channelIdx < 3; ++channelIdx) {
            profile.Channels.push_back(
                TChannelProfiles::TProfile::TChannel(TBlobStorageGroupType::ErasureMirror3, 0,
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
        str << "" << Endl;
        str << "Groups {" << Endl;
        str << "    GroupID: " << groupId << Endl;
        str << "    GroupGeneration: 1 " << Endl;
        str << "    ErasureSpecies: 1 " << Endl;// Mirror3
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
        str << "    }" << Endl;
        str << "}";
        TString staticConfig(str.Str());

        SubstGlobal(staticConfig, "$Node1", Sprintf("%" PRIu32, runtime.GetNodeId(0)));

        TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig(new TNodeWardenConfig(
            STRAND_PDISK && !runtime.IsRealThreads() ?
            static_cast<IPDiskServiceFactory*>(new TStrandedPDiskServiceFactory(runtime)) :
            static_cast<IPDiskServiceFactory*>(new TRealPDiskServiceFactory())));
//            nodeWardenConfig->Monitoring = monitoring;
        google::protobuf::TextFormat::ParseFromString(staticConfig, nodeWardenConfig->BlobStorageConfig.MutableServiceSet());

        if (nodeIndex == 0) {
            nodeWardenConfig->SectorMaps[extraPath] = extraSectorMap;
            ObtainTenantKey(&nodeWardenConfig->TenantKey, app.Keys[0]);
            ObtainStaticKey(&nodeWardenConfig->StaticKey);

            TString baseDir = runtime.GetTempDir();

            TIntrusivePtr<NPDisk::TSectorMap> sectorMap(new NPDisk::TSectorMap());
            sectorMap->ForceSize(64ull << 30ull);


            TString pDiskPath0 = TStringBuilder() << "SectorMap:" << baseDir << "pdisk_map";
            nodeWardenConfig->BlobStorageConfig.MutableServiceSet()->MutablePDisks(0)->SetPath(pDiskPath0);
            nodeWardenConfig->SectorMaps[pDiskPath0] = sectorMap;

            ui64 pDiskGuid = 1;
            static ui64 iteration = 0;
            ++iteration;
            ::NKikimr::FormatPDisk(pDiskPath0, 0, 4 << 10, 32u << 20u, pDiskGuid,
                0x1234567890 + iteration, 0x4567890123 + iteration, 0x7890123456 + iteration,
                NPDisk::YdbDefaultPDiskSequence, "", false, false, sectorMap, false);


            // Magic path from testlib, do not change it
            TString pDiskPath1 = TStringBuilder() << baseDir << "pdisk_1.dat";
            TIntrusivePtr<NPDisk::TSectorMap> sectorMap1(new NPDisk::TSectorMap());
            sectorMap1->ForceSize(64ull << 30ull);
            sectorMap1->ZeroInit(32);
            nodeWardenConfig->SectorMaps[pDiskPath1] = sectorMap1;
        }

        SetupBSNodeWarden(runtime, nodeIndex, nodeWardenConfig.Release());
        SetupTabletResolver(runtime, nodeIndex);
    }

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
        TTabletTypes::BSController, TBlobStorageGroupType::ErasureMirror3, groupId),
        &CreateFlatBsController);

    SetupBoxAndStoragePool(runtime, runtime.AllocateEdgeActor());
}

void Setup(TTestActorRuntime &runtime, TString extraPath, TIntrusivePtr<NPDisk::TSectorMap> extraSectorMap) {
    SetupLogging(runtime);
    SetupServices(runtime, extraPath, extraSectorMap);
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
            const NKikimrBlobStorage::TPDiskConfig* pdiskConfig = nullptr, ui64 inferPDiskSlotCountFromUnitSize = 0,
            TActorId nodeWarden = {}) {
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
        if (inferPDiskSlotCountFromUnitSize != 0) {
            pdisk->SetInferPDiskSlotCountFromUnitSize(inferPDiskSlotCountFromUnitSize);
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
                                                                     true, true);
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
        runtime.Start();

        TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig(new TNodeWardenConfig(static_cast<IPDiskServiceFactory*>(new TRealPDiskServiceFactory())));

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
        runtime.Start();

        ui32 nodeId = 1;
        ui32 pdiskId = 1337;
        ui64 cookie = 555;

        auto &appData = runtime.GetNode(1)->AppData;
        appData->DomainsInfo->AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dom", 1).Release());

        TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig(new TNodeWardenConfig(static_cast<IPDiskServiceFactory*>(new TRealPDiskServiceFactory())));

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

    void TestInferPDiskSlotCount(ui64 driveSize, ui64 unitSizeInBytes,
            ui64 expectedSlotCount, ui32 expectedSlotSizeInUnits, double expectedRelativeError = 0) {
        TIntrusivePtr<TPDiskConfig> pdiskConfig = new TPDiskConfig("fake_drive", 0, 0, 0);

        NStorage::TNodeWarden::InferPDiskSlotCount(pdiskConfig, driveSize, unitSizeInBytes);

        double unitSizeCalculated = double(driveSize) / pdiskConfig->ExpectedSlotCount / pdiskConfig->SlotSizeInUnits;
        double unitSizeRelativeError =  (unitSizeCalculated - unitSizeInBytes) / unitSizeInBytes;

        VERBOSE_COUT(""
            << " driveSize# " << driveSize
            << " unitSizeInBytes# " << unitSizeInBytes
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
                TStringBuilder() << "abs(" << unitSizeRelativeError << ") < " << expectedRelativeError
            );
        }
    }

    CUSTOM_UNIT_TEST(TestInferPDiskSlotCountPureFunction) {
        TestInferPDiskSlotCount(7900, 1000, 8, 1u, 0.0125);
        TestInferPDiskSlotCount(8000, 1000, 8, 1u, std::numeric_limits<double>::epsilon());
        TestInferPDiskSlotCount(8100, 1000, 8, 1u, 0.0125);
        TestInferPDiskSlotCount(16000, 1000, 16, 1u, std::numeric_limits<double>::epsilon());
        TestInferPDiskSlotCount(24000, 1000, 12, 2u, std::numeric_limits<double>::epsilon());
        TestInferPDiskSlotCount(31000, 1000, 16, 2u, 0.032);
        TestInferPDiskSlotCount(50000, 1000, 13, 4u, 0.039);
        TestInferPDiskSlotCount(50000, 100, 16, 32u, 0.024);
        TestInferPDiskSlotCount(18000, 200, 11, 8u, 0.023);

        for (ui64 i=1; i<=1024; i++) {
            // In all cases the relative error doesn't exceed 1/maxSlotCount
            TestInferPDiskSlotCount(i, 1, 0, 0, 1./16);
        }

        const size_t c_200GB = 200'000'000'000;
        const size_t c_2000GB = 2000'000'000'000;

        // Some real-world examples
        TestInferPDiskSlotCount(1919'366'987'776, c_200GB, 10, 1u, 0.041); // "Micron_5200_MTFDDAK1T9TDD"
        TestInferPDiskSlotCount(3199'243'124'736, c_200GB, 16, 1u, 0.001); // "SAMSUNG MZWLR3T8HBLS-00007"
        TestInferPDiskSlotCount(6400'161'873'920, c_200GB, 16, 2u, 0.001); // "INTEL SSDPE2KE064T8"
        TestInferPDiskSlotCount(6398'611'030'016, c_200GB, 16, 2u, 0.001); // "INTEL SSDPF2KX076T1"
        TestInferPDiskSlotCount(17999'117'418'496, c_2000GB, 9, 1u, 0.001); // "WDC  WUH721818ALE6L4"
    }

    void CheckInferredPDiskSettings(TTestBasicRuntime& runtime, TActorId fakeWhiteboard, TActorId fakeNodeWarden,
            ui32 pdiskId, ui32 expectedSlotCount, ui32 expectedSlotSizeInUnits,
            TDuration simTimeout = TDuration::Seconds(10)) {
        for (int attempt=0; attempt<10; ++attempt) {
            // Check EvPDiskStateUpdate sent from PDiskActor to Whiteboard
            const auto ev = runtime.GrabEdgeEventRethrow<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateUpdate>(fakeWhiteboard, simTimeout);
            VERBOSE_COUT(" Got TEvPDiskStateUpdate# " << ev->ToString());

            NKikimrWhiteboard::TPDiskStateInfo pdiskInfo = ev->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(pdiskInfo.GetPDiskId(), pdiskId);
            if (pdiskInfo.GetState() != NKikimrBlobStorage::TPDiskState::Normal) {
                continue;
            }
            UNIT_ASSERT(pdiskInfo.HasExpectedSlotCount());
            UNIT_ASSERT(pdiskInfo.HasSlotSizeInUnits());
            UNIT_ASSERT(pdiskInfo.HasAvailableSize());
            UNIT_ASSERT(pdiskInfo.HasTotalSize());
            UNIT_ASSERT_VALUES_EQUAL(pdiskInfo.GetExpectedSlotCount(), expectedSlotCount);
            UNIT_ASSERT_VALUES_EQUAL(pdiskInfo.GetSlotSizeInUnits(), expectedSlotSizeInUnits);
            break;
        }

        {
            // Check EvControllerUpdateDiskStatus sent from PDiskActor to NodeWarden
            const auto ev = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerUpdateDiskStatus>(fakeNodeWarden, simTimeout);
            VERBOSE_COUT(" Got TEvControllerUpdateDiskStatus# " << ev->ToString());

            NKikimrBlobStorage::TEvControllerUpdateDiskStatus diskStatus = ev->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(diskStatus.PDisksMetricsSize(), 1);

            const NKikimrBlobStorage::TPDiskMetrics &metrics = diskStatus.GetPDisksMetrics(0);
            UNIT_ASSERT_VALUES_EQUAL(metrics.GetPDiskId(), pdiskId);
            UNIT_ASSERT(metrics.HasSlotCount());
            UNIT_ASSERT(metrics.HasSlotSizeInUnits());
            UNIT_ASSERT_VALUES_EQUAL(metrics.GetSlotCount(), expectedSlotCount);
            UNIT_ASSERT_VALUES_EQUAL(metrics.GetSlotSizeInUnits(), expectedSlotSizeInUnits);
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
        runtime.Initialize(app.Unwrap());

        // Setup BSNodeWarden
        TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig(new TNodeWardenConfig(static_cast<IPDiskServiceFactory*>(new TRealPDiskServiceFactory())));
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

    CUSTOM_UNIT_TEST(TestInferPDiskSlotCountExplicitConfig) {
        TTestBasicRuntime runtime(1, false);
        TActorId realNodeWarden = SetupNodeWardenOnly(runtime);

        const ui32 nodeId = runtime.GetNodeId(0);
        const ui32 pdiskId = 1001;
        const TString pdiskPath = "SectorMap:TestInferPDiskSlotCountExplicitConfig:2400";
        const ui64 inferPDiskSlotCountFromUnitSize = 100_GB; // 12 * 2u slots

        TActorId fakeNodeWarden = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageNodeWardenID(nodeId), fakeNodeWarden);
        TActorId fakeWhiteboard = runtime.AllocateEdgeActor();
        runtime.RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId), fakeWhiteboard);

        NKikimrBlobStorage::TPDiskConfig pdiskConfig;
        pdiskConfig.SetExpectedSlotCount(13);
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, inferPDiskSlotCountFromUnitSize, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 13, 0u);
    }

    CUSTOM_UNIT_TEST(TestInferPDiskSlotCountWithRealNodeWarden) {
        TTestBasicRuntime runtime(1, false);
        TActorId realNodeWarden = SetupNodeWardenOnly(runtime);

        const ui32 nodeId = runtime.GetNodeId(0);
        const ui32 pdiskId = 1002;
        const TString pdiskPath = "SectorMap:TestInferPDiskSlotCount:2400";
        const ui64 inferPDiskSlotCountFromUnitSize = 100_GB; // 12 * 2u slots

        TActorId fakeNodeWarden = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeBlobStorageNodeWardenID(nodeId), fakeNodeWarden);
        TActorId fakeWhiteboard = runtime.AllocateEdgeActor();
        runtime.RegisterService(NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId), fakeWhiteboard);

        NKikimrBlobStorage::TPDiskConfig pdiskConfig;
        CreatePDisk(runtime, 0, pdiskPath, 0, pdiskId, 0,
            &pdiskConfig, inferPDiskSlotCountFromUnitSize, realNodeWarden);
        CheckInferredPDiskSettings(runtime, fakeWhiteboard, fakeNodeWarden,
            pdiskId, 12, 2u);
    }
}

} // namespace NBlobStorageNodeWardenTest
} // namespace NKikimr
