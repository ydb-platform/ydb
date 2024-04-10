#include "defs.h"
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/core/base/hive.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/mind/local.h>

#include <util/random/entropy.h>
#include <util/stream/file.h>
#include <util/string/printf.h>
#include <util/string/subst.h>

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

constexpr ui32 DOMAIN_ID = 31;

using namespace NActors;

enum EExpected {
    EExpectedEqualData = 0,
    EExpectedDifferentData = 1
};

void FormatPDisk(TString path, ui64 diskSize, ui32 chunkSize, ui64 guid, bool isGuidValid) {
    NPDisk::TKey chunkKey;
    NPDisk::TKey logKey;
    NPDisk::TKey sysLogKey;
    EntropyPool().Read(&chunkKey, sizeof(NKikimr::NPDisk::TKey));
    EntropyPool().Read(&logKey, sizeof(NKikimr::NPDisk::TKey));
    EntropyPool().Read(&sysLogKey, sizeof(NKikimr::NPDisk::TKey));

    if (!isGuidValid) {
        EntropyPool().Read(&guid, sizeof(guid));
    }

    NKikimr::FormatPDisk(path, diskSize, 4 << 10, chunkSize, guid,
        chunkKey, logKey, sysLogKey, NPDisk::YdbDefaultPDiskSequence, "Test",
        false, false, nullptr, false);
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

void SetupServices(TTestActorRuntime &runtime) {
    const ui32 domainsNum = 1;
    const ui32 disksInDomain = 1;

    const ui32 domainId = DOMAIN_ID;
    const ui32 stateStorageGroup = domainId;

    TString keyfile;
    { // prepare keyfile
        TString baseDir = runtime.GetTempDir();
        keyfile = Sprintf("%s/key.txt", baseDir.data());
        TFileOutput file(keyfile);
        file << "some key data";
    }

    TAppPrepare app;

    { // setup domain info
        app.ClearDomainsAndHive();
        app.AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("dc-1", domainId).Release());
        app.AddHive(MakeDefaultHiveID());
    }
    { // setup channel profiles
        TIntrusivePtr<TChannelProfiles> channelProfiles = new TChannelProfiles;
        channelProfiles->Profiles.emplace_back();
        TChannelProfiles::TProfile &profile = channelProfiles->Profiles.back();
        for (ui32 channelIdx = 0; channelIdx < 3; ++channelIdx) {
            profile.Channels.push_back(
                TChannelProfiles::TProfile::TChannel(TBlobStorageGroupType::ErasureMirror3, 0, NKikimrBlobStorage::TVDiskKind::Default));
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

        app.SetKeyForNode(keyfile, nodeIndex);
        ObtainTenantKey(&nodeWardenConfig->TenantKey, app.Keys[nodeIndex]);
        ObtainStaticKey(&nodeWardenConfig->StaticKey);

        if (nodeIndex == 0) {
            static TTempDir tempDir;
            TString pDiskPath = tempDir() + "/pdisk0.dat";
            nodeWardenConfig->BlobStorageConfig.MutableServiceSet()->MutablePDisks(0)->SetPath(pDiskPath);

            ui64 pDiskGuid = 1;
            static ui64 iteration = 0;
            ++iteration;
            ::NKikimr::FormatPDisk(pDiskPath, 16000000000ull, 4 << 10, 32u << 20u, pDiskGuid,
                0x1234567890 + iteration, 0x4567890123 + iteration, 0x7890123456 + iteration,
                NPDisk::YdbDefaultPDiskSequence, "", false, false, nullptr, false);
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
}

void Setup(TTestActorRuntime& runtime) {
    SetupLogging(runtime);
    SetupServices(runtime);
//    runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_DEBUG);
//    runtime.SetLogPriority(NKikimrServices::BS_CONTROLLER, NLog::PRI_DEBUG);
//    runtime.SetLogPriority(NKikimrServices::BS_PDISK, NLog::PRI_DEBUG);
//    runtime.SetLogPriority(NKikimrServices::BS_PROXY_PUT, NLog::PRI_DEBUG);
//    runtime.SetLogPriority(NKikimrServices::BS_QUEUE, NLog::PRI_DEBUG);
}


Y_UNIT_TEST_SUITE(TBlobStorageWardenTest) {
    ui64 GetBsc(TTestActorRuntime& /*runtime*/) {
        return MakeBSControllerID();
    }

    void CreatePDiskInBox(TTestActorRuntime& runtime, const TActorId& sender, ui32 nodeId, ui64 boxId, TString pdiskPath,
            std::optional<ui64> size) {
        if (size) {
            TFile file(pdiskPath, CreateAlways | RdWr | ARW);
            file.Resize(0);
            file.Resize(*size);
            file.Close();
        }

        auto ev = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        auto& record = ev->Record;
        auto *request = record.MutableRequest();
        auto *cmd1 = request->AddCommand()->MutableDefineHostConfig();
        cmd1->SetHostConfigId(boxId);
        auto *drive = cmd1->AddDrive();
        drive->SetPath(pdiskPath);
        drive->SetType(NKikimrBlobStorage::ROT);
        drive->MutablePDiskConfig()->SetChunkSize(32 << 20); // 16 MB
        auto *cmd2 = request->AddCommand()->MutableDefineBox();
        cmd2->SetBoxId(boxId);
        auto *host = cmd2->AddHost();
        host->MutableKey()->SetNodeId(nodeId);
        host->SetHostConfigId(boxId);
        runtime.SendToPipe(GetBsc(runtime), sender, ev.Release(), sender.NodeId() - runtime.GetNodeId(0), GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto res = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(handle);
        UNIT_ASSERT(res);
        UNIT_ASSERT_C(res->Record.GetResponse().GetSuccess(), res->Record.GetResponse().GetErrorDescription().data());
    }

    ui32 CreateGroupInBox(TTestActorRuntime& runtime, const TActorId& sender, ui64 boxId, ui64 poolId,
            ui32 encryptionMode) {
        auto ev = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        auto& record = ev->Record;
        auto *request = record.MutableRequest();
        auto *cmd = request->AddCommand()->MutableDefineStoragePool();
        cmd->SetBoxId(boxId);
        cmd->SetStoragePoolId(poolId);
        cmd->SetEncryptionMode(encryptionMode);
        cmd->SetErasureSpecies("none");
        cmd->SetVDiskKind("Default");
        cmd->SetNumGroups(1);
        cmd->AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::ROT);
        request->AddCommand()->MutableQueryBaseConfig();
        runtime.SendToPipe(GetBsc(runtime), sender, ev.Release(), sender.NodeId() - runtime.GetNodeId(0), GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto res = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(handle);
        UNIT_ASSERT(res);
        UNIT_ASSERT_C(res->Record.GetResponse().GetSuccess(), res->Record.GetResponse().GetErrorDescription().data());
        for (const auto& gr : res->Record.GetResponse().GetStatus(1).GetBaseConfig().GetGroup()) {
            if (gr.GetBoxId() == boxId && gr.GetStoragePoolId() == poolId) {
                return gr.GetGroupId();
            }
        }
        return 0;
    }

    void Put(TTestActorRuntime &runtime, TActorId &sender, ui32 groupId, TLogoBlobID logoBlobId, TString data) {
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
        UNIT_ASSERT_C(putResult->Status == NKikimrProto::OK, "Status# " << NKikimrProto::EReplyStatus_Name(putResult->Status));
        UNIT_ASSERT_EQUAL(handle->Cookie, cookie);
    }

    void Get(TTestActorRuntime &runtime, TActorId &sender, ui32 groupId, TLogoBlobID logoBlobId, TString data) {
        VERBOSE_COUT(" Sending TEvGet");
        TActorId proxy = MakeBlobStorageProxyID(groupId);
        ui32 nodeId = sender.NodeId();
        TActorId nodeWarden = MakeBlobStorageNodeWardenID(nodeId);
        ui64 cookie = 6543210;
        runtime.Send(new IEventHandle(proxy, sender,
            new TEvBlobStorage::TEvGet(logoBlobId, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::LowRead),
            IEventHandle::FlagForwardOnNondelivery, cookie, &nodeWarden), sender.NodeId() - runtime.GetNodeId(0));

        TAutoPtr<IEventHandle> handle;
        auto getResult = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvGetResult>(handle);
        UNIT_ASSERT(getResult);
        UNIT_ASSERT_C(getResult->Status == NKikimrProto::OK,
                "Status# " << NKikimrProto::EReplyStatus_Name(getResult->Status));
        UNIT_ASSERT_EQUAL(handle->Cookie, cookie);
        UNIT_ASSERT(getResult->ResponseSz == 1);
        UNIT_ASSERT(getResult->Responses.Get());
        UNIT_ASSERT_EQUAL((getResult->Responses)[0].Buffer.size(), data.size());
        UNIT_ASSERT_EQUAL((getResult->Responses)[0].Buffer.ConvertToString(), data);
    }

    void VGet(TTestActorRuntime &runtime, TActorId &sender, ui32 groupId, ui32 nodeId, TLogoBlobID logoBlobId,
            TString data, EExpected expected) {
        VERBOSE_COUT(" Sending TEvVGet");
        ui64 cookie = 6543210;
        ui32 pDiskId = 1000;
        ui32 vDiskSlotId = 1000;
        TActorId vDiskActor = MakeBlobStorageVDiskID(nodeId, pDiskId, vDiskSlotId);
        TVDiskID vDiskId(groupId, 1, 0, 0 , 0); // GroupID: 1040187392 GroupGeneration: 1 Ring: 0 Domain: 1 VDisk: 0 }}
        TLogoBlobID id(logoBlobId, 1);
        auto x = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vDiskId,
                TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::AsyncRead,
                TEvBlobStorage::TEvVGet::EFlags::None,
                cookie,
                {id});
        runtime.Send(new IEventHandle(vDiskActor, sender, x.release()), sender.NodeId() - runtime.GetNodeId(0));

        TAutoPtr<IEventHandle> handle;
        auto vgetResult = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVGetResult>(handle);
        UNIT_ASSERT(vgetResult);
        UNIT_ASSERT_C(vgetResult->Record.GetStatus() == NKikimrProto::OK,
                "Status# " << NKikimrProto::EReplyStatus_Name(vgetResult->Record.GetStatus()));
        UNIT_ASSERT_EQUAL(vgetResult->Record.GetCookie(), cookie);
        UNIT_ASSERT(vgetResult->Record.ResultSize() == 1);
        TString resBuffer = vgetResult->GetBlobData(vgetResult->Record.GetResult(0)).ConvertToString();
        UNIT_ASSERT_EQUAL(resBuffer.size(), data.size());
        if (expected == EExpectedEqualData) {
            UNIT_ASSERT_EQUAL(resBuffer, data);
        } else {
            UNIT_ASSERT(resBuffer != data);
        }
    }


    CUSTOM_UNIT_TEST(TestCreatePDiskAndGroup) {
        TTempDir tempDir;
        TTestBasicRuntime runtime(2, false);
        Setup(runtime);
        TActorId sender0 = runtime.AllocateEdgeActor(0);
        TActorId sender1 = runtime.AllocateEdgeActor(1);

        CreatePDiskInBox(runtime, sender0, runtime.GetNodeId(0), 1, TFsPath(tempDir()) / "new_pdisk.dat", 16ULL << 30);
        const ui32 groupId = CreateGroupInBox(runtime, sender0, 1, 1, 0);
        UNIT_ASSERT(groupId != 0);
        Put(runtime, sender0, groupId, TLogoBlobID(100, 0, 0, 0, 3, 0), "xxx");
        Get(runtime, sender0, groupId, TLogoBlobID(100, 0, 0, 0, 3, 0), "xxx");
        VGet(runtime, sender0, groupId, runtime.GetNodeId(0), TLogoBlobID(100, 0, 0, 0, 3, 0), "xxx", EExpectedEqualData);

        // TODO: the proxy just should not be there, check that instead!
        TActorId proxy = MakeBlobStorageProxyID(groupId);
        runtime.Send(new IEventHandle(proxy, sender1, new TEvents::TEvPoisonPill()), 1);

        Put(runtime, sender1, groupId, TLogoBlobID(100, 0, 1, 0, 3, 0), "yyy");
        Get(runtime, sender0, groupId, TLogoBlobID(100, 0, 1, 0, 3, 0), "yyy");
    }

    CUSTOM_UNIT_TEST(TestCreatePDiskAndEncryptedGroup) {
        TTempDir tempDir;
        TTestBasicRuntime runtime(2, false);
        Setup(runtime);
        TActorId sender0 = runtime.AllocateEdgeActor(0);
        TActorId sender1 = runtime.AllocateEdgeActor(1);

        CreatePDiskInBox(runtime, sender0, runtime.GetNodeId(0), 1, TFsPath(tempDir()) / "new_pdisk.dat", 16ULL << 30);
        const ui32 groupId = CreateGroupInBox(runtime, sender0, 1, 1, 1);
        UNIT_ASSERT(groupId != 0);
        Put(runtime, sender0, groupId, TLogoBlobID(100, 0, 0, 0, 3, 0), "xxx");
        Get(runtime, sender0, groupId, TLogoBlobID(100, 0, 0, 0, 3, 0), "xxx");
        VGet(runtime, sender0, groupId, runtime.GetNodeId(0), TLogoBlobID(100, 0, 0, 0, 3, 0), "xxx",
                EExpectedDifferentData);

        // TODO: the proxy just should not be there, check that instead!
        TActorId proxy = MakeBlobStorageProxyID(groupId);
        runtime.Send(new IEventHandle(proxy, sender1, new TEvents::TEvPoisonPill()), 1);

        Put(runtime, sender1, groupId, TLogoBlobID(100, 0, 1, 0, 3, 0), "yyy");
        Get(runtime, sender1, groupId, TLogoBlobID(100, 0, 1, 0, 3, 0), "yyy");
    }

}

} // namespace NBlobStorageNodeWardenTest
} // namespace NKikimr
