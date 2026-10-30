#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>

Y_UNIT_TEST_SUITE(NodeWardenCache) {

    bool TryReadBlob(TEnvironmentSetup& env, ui32 groupId, const TLogoBlobID& blobId, ui32 senderNodeId) {
        const TActorId sender = env.Runtime->AllocateEdgeActor(senderNodeId, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvGet(blobId, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, true,
            env.Runtime->GetClock() + TDuration::Seconds(30));
        if (!res) {
            return false;
        }
        auto *msg = res->Get();
        return msg->Status == NKikimrProto::OK && msg->ResponseSz == 1 && msg->Responses[0].Status == NKikimrProto::OK;
    }

    void DoTestRecoveryWhileBscDown(bool useCache, bool expectReadableAfterRestart) {
        const ui32 controllerNodeId = 3;
        TEnvironmentSetup env{{
            .NodeCount = 3,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .ControllerNodeId = controllerNodeId,
            .Cache = useCache,
        }};

        env.CreateBoxAndPool(/*numDrivesPerNode=*/1, /*numGroups=*/1, /*numStorageNodes=*/2);
        env.Sim(TDuration::Seconds(30));

        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);
        UNIT_ASSERT_VALUES_EQUAL(info->GetTotalVDisksNum(), 1);
        const ui32 vdiskNodeId = info->GetActorId(0).NodeId();
        UNIT_ASSERT_C(vdiskNodeId != controllerNodeId, "VDisk unexpectedly landed on the controller node");

        const ui32 readerNodeId = vdiskNodeId == 1 ? 2 : 1;

        const TLogoBlobID blobId(1, 1, 1, 0, 4, 0);
        env.PutBlob(groupId, blobId, "test");

        UNIT_ASSERT_C(TryReadBlob(env, groupId, blobId, readerNodeId),
            "blob must be readable before the restart");

        env.StopNode(controllerNodeId);

        env.RestartNode(vdiskNodeId);
        env.Sim(TDuration::Seconds(60));

        const bool readable = TryReadBlob(env, groupId, blobId, readerNodeId);
        UNIT_ASSERT_VALUES_EQUAL(readable, expectReadableAfterRestart);
    }

    Y_UNIT_TEST(StorageNodeRecoversFromCacheWhileBscDown) {
        DoTestRecoveryWhileBscDown(/*useCache=*/true, /*expectReadableAfterRestart=*/true);
    }

    Y_UNIT_TEST(StorageNodeStuckWithoutCacheWhileBscDown) {
        DoTestRecoveryWhileBscDown(/*useCache=*/false, /*expectReadableAfterRestart=*/false);
    }

    // The node keeps the main YAML and the dedicated storage YAML in its config directory. A switch to the single
    // config comes, and saving the new main YAML fails (here the temporary file can't be made). The storage YAML the
    // old main one relies on must stay then, until the new main YAML is saved.
    Y_UNIT_TEST(StorageYamlKeptWhenMainYamlNotSaved) {
        TTempDir dir;
        TEnvironmentSetup env{{
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .ConfigPreprocessor = [&](ui32 nodeId, TNodeWardenConfig& config) {
                config.ConfigDirPath = (dir.Path() / ToString(nodeId)).GetPath();
            },
        }};
        env.Sim(TDuration::Seconds(10));

        const TFsPath nodeDir = dir.Path() / "1";
        nodeDir.MkDirs();
        TFileOutput(nodeDir / "storage.yaml").Write("storage config the old main one relies on\n");
        (nodeDir / "temp_config.yaml").MkDirs(); // the temporary file of the main YAML can't be made

        auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
        auto *yaml = update->Record.MutableYamlConfig();
        yaml->SetMainConfig("single config\n");
        yaml->SetMainConfigVersion(1000);
        // no storage config version: the single config mode, where the storage YAML file goes away
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), env.Runtime->AllocateEdgeActor(1),
            update.release()), 1);
        env.Sim(TDuration::Seconds(10));

        UNIT_ASSERT(!(nodeDir / "config.yaml").Exists());
        UNIT_ASSERT_C((nodeDir / "storage.yaml").Exists(), "the storage YAML is gone while the main YAML wasn't saved");
    }
}
