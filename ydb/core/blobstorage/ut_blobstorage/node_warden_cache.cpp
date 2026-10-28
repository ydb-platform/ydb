#include "bridge_config_yaml.h"

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

    // Switching to the single config, deleting the storage YAML fails (here it is a directory with a file in it). The
    // failure must be retried as a failed save is, not end the process of NodeWarden.
    Y_UNIT_TEST(FailedDeleteOfStorageYamlIsRetried) {
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
        (nodeDir / "storage.yaml").MkDirs();
        TFileOutput(nodeDir / "storage.yaml" / "file").Write("keeps the directory from being deleted\n");

        auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
        auto *yaml = update->Record.MutableYamlConfig();
        yaml->SetMainConfig("single config\n");
        yaml->SetMainConfigVersion(1000);
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), env.Runtime->AllocateEdgeActor(1),
            update.release()), 1);
        env.Sim(TDuration::Seconds(10));

        // the exception escaping the save would have ended the process by now
        UNIT_ASSERT((nodeDir / "config.yaml").Exists());
        UNIT_ASSERT((nodeDir / "storage.yaml").Exists());
    }

    // Update A switches to the single config and its save fails; update B, coming right after it, brings the dedicated
    // storage YAML, which is saved. The retry of A is obsolete then: it must not run and delete the storage YAML of B.
    Y_UNIT_TEST(ObsoleteRetryOfSaveKeepsNewerStorageYaml) {
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
        const TFsPath blocker = nodeDir / "temp_config.yaml";
        blocker.MkDirs(); // the main YAML of A can't be saved
        ui32 numRetries = 0;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->HasEvent() && ev->GetTypeName().Contains("TEvRetrySaveConfig") && !numRetries++) {
                blocker.ForceDelete(); // the retry of A would succeed
            }
            return true;
        };

        auto a = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
        a->Record.MutableYamlConfig()->SetMainConfig("single config\n");
        a->Record.MutableYamlConfig()->SetMainConfigVersion(1000);
        auto b = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
        b->Record.MutableYamlConfig()->SetStorageConfig("dedicated storage config\n");
        b->Record.MutableYamlConfig()->SetStorageConfigVersion(1);
        const TActorId edge = env.Runtime->AllocateEdgeActor(1);
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), edge, a.release()), 1);
        env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), edge, b.release()), 1);
        env.Sim(TDuration::Minutes(1));
        env.Runtime->FilterFunction = {};

        UNIT_ASSERT(numRetries);
        UNIT_ASSERT_C((nodeDir / "storage.yaml").Exists(), "the obsolete retry deleted the storage YAML of B");
    }

    // Distconf commits a new main YAML, and its save on node 1 fails. That save is the latest one, so its retry must run
    // and bring the YAML on disk up to date once it can be saved.
    Y_UNIT_TEST(RetryOfDistconfSaveRuns) {
        TTempDir dir;
        TEnvironmentSetup env{{
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .ConfigPreprocessor = [&](ui32 nodeId, TNodeWardenConfig& config) {
                config.ConfigDirPath = (dir.Path() / ToString(nodeId)).GetPath();
            },
            .LocationGenerator = NBridgeTest::GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        env.Sim(TDuration::Seconds(30));

        const TFsPath nodeDir = dir.Path() / "1";
        const TFsPath blocker = nodeDir / "temp_config.yaml";
        blocker.MkDirs(); // the new main YAML can't be saved
        ui32 numRetries = 0;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (nodeId == 1 && ev->HasEvent() && ev->GetTypeName().Contains("TEvRetrySaveConfig") && !numRetries++) {
                blocker.ForceDelete(); // the retry would succeed
            }
            return true;
        };

        const auto res = NBridgeTest::ReplaceMainConfig(env, [](YAML::Node&) {});
        UNIT_ASSERT_EQUAL_C(res.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, res.GetErrorReason());
        const ui64 version = YAML::Load(NBridgeTest::FetchMainConfigYaml(env))["metadata"]["version"].as<ui64>();
        env.Sim(TDuration::Minutes(1));
        env.Runtime->FilterFunction = {};

        UNIT_ASSERT(numRetries);
        const auto saved = YAML::Load(TFileInput(nodeDir / "config.yaml").ReadAll());
        UNIT_ASSERT_VALUES_EQUAL_C(saved["metadata"]["version"].as<ui64>(), version, "the retry of the latest save didn't run");
    }
}
