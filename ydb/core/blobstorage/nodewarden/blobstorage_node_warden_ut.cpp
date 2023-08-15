#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/base/hive.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_http_request.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/mind/local.h>

#include <ydb/library/pdisk_io/sector_map.h>
#include <util/random/entropy.h>
#include <util/string/printf.h>
#include <util/string/subst.h>
#include <util/stream/file.h>

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
        Cerr << str << Endl; \
    } \
} while(false)

#define LOW_VERBOSE_COUT(str) \
do { \
    if (IsLowVerbose) { \
        Cerr << str << Endl; \
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
    EntropyPool().Read(&chunkKey, sizeof(NKikimr::NPDisk::TKey));
    EntropyPool().Read(&logKey, sizeof(NKikimr::NPDisk::TKey));
    EntropyPool().Read(&sysLogKey, sizeof(NKikimr::NPDisk::TKey));

    if (!isGuidValid) {
        EntropyPool().Read(&guid, sizeof(guid));
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

    const ui32 domainId = DOMAIN_ID;
    const ui32 stateStorageGroup = domainId;

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
        auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds("dc-1", domainId, 0,
                                                                                  domainId, domainId, TVector<ui32>{domainId},
                                                                                  domainId, TVector<ui32>{domainId},
                                                                                  100500,
                                                                                  TVector<ui64>{},
                                                                                  TVector<ui64>{},
                                                                                  TVector<ui64>{},
                                                                                  DefaultPoolKinds(2));
        app.AddDomain(domain.Release());
        app.AddHive(domainId, MakeDefaultHiveID(stateStorageGroup));
    }

    SetupChannelProfiles(app, domainId);

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
        SetupStateStorage(runtime, nodeIndex, stateStorageGroup);

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

    ui64 defaultStateStorageGroup = runtime.GetAppData(0).DomainsInfo->GetDefaultStateStorageGroup(DOMAIN_ID);
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeBSControllerID(defaultStateStorageGroup),
        TTabletTypes::BSController, TBlobStorageGroupType::ErasureMirror3, groupId),
        &CreateFlatBsController);

    SetupBoxAndStoragePool(runtime, runtime.AllocateEdgeActor(), domainId);
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
    ui64 GetBsc(TTestActorRuntime &runtime) {
        ui64 defaultStateStorageGroup = runtime.GetAppData(0).DomainsInfo->GetDefaultStateStorageGroup(DOMAIN_ID);
        ui64 bsController = MakeBSControllerID(defaultStateStorageGroup);
        return bsController;
    }

    ui32 CreatePDisk(TTestActorRuntime &runtime, ui32 nodeIdx, TString path, ui64 guid, ui32 pdiskId, ui64 pDiskCategory) {
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
        runtime.Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), TActorId(), ev.release()));

        return pdiskId;
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

    void CreateStoragePool(TTestBasicRuntime& runtime, ui32 domainId, TString name, TString kind) {
        auto stateStorage = runtime.GetAppData().DomainsInfo->GetDefaultStateStorageGroup(domainId);
        NKikimrBlobStorage::TDefineStoragePool storagePool = runtime.GetAppData().DomainsInfo->GetDomain(domainId).StoragePoolTypes.at(kind);

        TActorId edge = runtime.AllocateEdgeActor();
        auto request = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        Y_VERIFY(storagePool.GetKind() == kind);
        storagePool.ClearStoragePoolId();
        storagePool.SetName(name);
        storagePool.SetNumGroups(1);
        storagePool.SetEncryptionMode(1);
        request->Record.MutableRequest()->AddCommand()->MutableDefineStoragePool()->CopyFrom(storagePool);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        runtime.SendToPipe(MakeBSControllerID(stateStorage), edge, request.release(), 0, pipeConfig);

        auto reply = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetResponse().GetSuccess(), true);
    }

    ui32 GetGroupFromPool(TTestBasicRuntime& runtime, ui32 domainId, TString poolName) {
        auto stateStorage = runtime.GetAppData().DomainsInfo->GetDefaultStateStorageGroup(domainId);

        TActorId edge = runtime.AllocateEdgeActor();
        auto selectGroups = std::make_unique<TEvBlobStorage::TEvControllerSelectGroups>();
        auto *record = &selectGroups->Record;
        record->SetReturnAllMatchingGroups(true);
        auto* groupParams = record->AddGroupParameters();
        groupParams->MutableStoragePoolSpecifier()->SetName(poolName);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        runtime.SendToPipe(MakeBSControllerID(stateStorage), edge, selectGroups.release(), 0, pipeConfig);

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

    NKikimrBlobStorage::TDefineStoragePool DescribeStoragePool(TTestBasicRuntime& runtime, ui32 domainId, const TString& name) {
        auto stateStorage = runtime.GetAppData().DomainsInfo->GetDefaultStateStorageGroup(domainId);

        TActorId edge = runtime.AllocateEdgeActor();
        auto selectGroups = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto* request = selectGroups->Record.MutableRequest();
        auto* readPool = request->AddCommand()->MutableReadStoragePool();
        readPool->SetBoxId(1);
        readPool->AddName(name);

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        runtime.SendToPipe(MakeBSControllerID(stateStorage), edge, selectGroups.release(), 0, pipeConfig);

        auto reply = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetResponse().GetSuccess(), true);
        return reply->Get()->Record.GetResponse().GetStatus(0).GetStoragePool(0);
    }

    void RemoveStoragePool(TTestBasicRuntime& runtime, ui32 domainId, const NKikimrBlobStorage::TDefineStoragePool& storagePool) {
        auto stateStorage = runtime.GetAppData().DomainsInfo->GetDefaultStateStorageGroup(domainId);
        TActorId edge = runtime.AllocateEdgeActor();
        auto selectGroups = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        auto* request = selectGroups->Record.MutableRequest();
        auto* deletePool = request->AddCommand()->MutableDeleteStoragePool();
        deletePool->SetBoxId(1);
        deletePool->SetStoragePoolId(storagePool.GetStoragePoolId());
        deletePool->SetItemConfigGeneration(storagePool.GetItemConfigGeneration());

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        runtime.SendToPipe(MakeBSControllerID(stateStorage), edge, selectGroups.release(), 0, pipeConfig);

        auto reply = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetResponse().GetSuccess(), true);
    }

    struct TBlockUpdates {
        TTestBasicRuntime* Runtime;
        TTestActorRuntime::TEventObserver PrevObserver = nullptr;



        TBlockUpdates(TTestBasicRuntime& runtime)
        : Runtime(&runtime)
        {
            TTestActorRuntime::TEventObserver observer = [=] (TTestActorRuntimeBase& /*runtime*/, TAutoPtr<IEventHandle>& event) -> TTestActorRuntime::EEventAction {
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

        CreateStoragePool(runtime, DOMAIN_ID, "test_storage", "pool-kind-1");
        ui32 groupId = GetGroupFromPool(runtime, DOMAIN_ID, "test_storage");

        ui64 tabletId = 1234;
        ui32 generation = 1;
        BlockGroup(runtime, sender0, tabletId, groupId, generation, true);
        BlockGroup(runtime, sender0, tabletId, groupId, generation, true, NKikimrProto::EReplyStatus::ALREADY);
        BlockGroup(runtime, sender0, tabletId, groupId, generation-1, true, NKikimrProto::EReplyStatus::ALREADY);

        auto describePool = DescribeStoragePool(runtime, DOMAIN_ID, "test_storage");
        {
            TBlockUpdates bloker(runtime);
            RemoveStoragePool(runtime, DOMAIN_ID, describePool);

            ++generation;
            BlockGroup(runtime, sender0, tabletId, groupId, generation++, true);
        }

        ++generation;
        BlockGroup(runtime, sender0, tabletId, groupId, generation++, true);

        auto stateStorage = runtime.GetAppData().DomainsInfo->GetDefaultStateStorageGroup(DOMAIN_ID);
        RebootTablet(runtime, MakeBSControllerID(stateStorage), sender0, sender0.NodeId() - runtime.GetNodeId(0));

        ++generation;
        BlockGroup(runtime, sender0, tabletId, groupId, generation++, true, NKikimrProto::EReplyStatus::NO_GROUP);
    }

    CUSTOM_UNIT_TEST(TestSendToInvalidGroupId) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, "", nullptr);

        auto sender = runtime.AllocateEdgeActor(0);

        CreateStoragePool(runtime, DOMAIN_ID, "test_storage", "pool-kind-1");
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

        CreateStoragePool(runtime, DOMAIN_ID, "test_storage", "pool-kind-1");
        ui32 groupId = GetGroupFromPool(runtime, DOMAIN_ID, "test_storage");

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

        CreateStoragePool(runtime, DOMAIN_ID, "test_storage", "pool-kind-1");

        ui32 generation = 1;
        ui64 tabletId = 1234;
        ui32 groupId = GetGroupFromPool(runtime, DOMAIN_ID, "test_storage");
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

        CreateStoragePool(runtime, DOMAIN_ID, "test_storage", "pool-kind-1");

        ui32 generation = 1;
        ui64 tabletId = 1234;
        ui32 groupId = GetGroupFromPool(runtime, DOMAIN_ID, "test_storage");
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

        CreateStoragePool(runtime, DOMAIN_ID, "test_storage", "pool-kind-1");

        ui32 generation = 1;
        ui64 tabletId = 1234;
        ui32 groupId = GetGroupFromPool(runtime, DOMAIN_ID, "test_storage");
        TString name = Sprintf("%09" PRIu32, groupId);

        Put(runtime, sender0, groupId, TLogoBlobID(tabletId, generation+1, 0, 0, 5, 0), "hello");
        CollectGroup(runtime, sender1, tabletId, groupId, true);

        AssertMonitoringExists(runtime, 0, name);
    }

    CUSTOM_UNIT_TEST(TestGivenPDiskFormatedWithGuid1AndCreatedWithGuid2WhenYardInitThenError) {
        TTempDir tempDir;
        TTestBasicRuntime runtime(2, false);
        TIntrusivePtr<NPDisk::TSectorMap> sectorMap(new NPDisk::TSectorMap(32ull << 30ull));
        Setup(runtime, "SectorMap:new_pdisk", sectorMap);
        TActorId sender0 = runtime.AllocateEdgeActor(0);
//        TActorId sender1 = runtime.AllocateEdgeActor(1);

        VERBOSE_COUT(" Formatting pdisk");
        FormatPDiskRandomKeys(tempDir() + "/new_pdisk.dat", sectorMap->DeviceSize, 32 << 20, 1, false, sectorMap, false);

        VERBOSE_COUT(" Creating PDisk");
        ui64 guid = 1;
        ui64 pDiskCategory = 0;
        EntropyPool().Read(&guid, sizeof(guid));
//        TODO: look why doesn't sernder 1 work
        ui32 pDiskId = CreatePDisk(runtime, 0, tempDir() + "/new_pdisk.dat", guid, 1001, pDiskCategory);

        VERBOSE_COUT(" Verify that PDisk returns ERROR");

        TVDiskID vDiskId;
        ui64 guid2 = guid;
        while (guid2 == guid) {
            EntropyPool().Read(&guid2, sizeof(guid2));
        }
        ui32 nodeId = runtime.GetNodeId(0);
        TActorId pDiskActorId = MakeBlobStoragePDiskID(nodeId, pDiskId);
        for (;;) {
            runtime.Send(new IEventHandle(pDiskActorId, sender0, new NPDisk::TEvYardInit(1, vDiskId, guid)), 0);
            TAutoPtr<IEventHandle> handle;
            if (auto initResult = runtime.GrabEdgeEventRethrow<NPDisk::TEvYardInitResult>(handle, TDuration::Seconds(1))) {
                UNIT_ASSERT(initResult);
                UNIT_ASSERT(initResult->Status == NKikimrProto::CORRUPTED);
                break;
            }
        }
    }

    void TestHttpMonForPath(const TString& path) {
        TTestBasicRuntime runtime(1, false);
        Setup(runtime, "", nullptr);
        auto edge = runtime.AllocateEdgeActor(0);
        TActorId nodeWarden = MakeBlobStorageNodeWardenID(edge.NodeId());
        THttpRequest HttpRequest;
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
}

} // namespace NBlobStorageNodeWardenTest
} // namespace NKikimr
