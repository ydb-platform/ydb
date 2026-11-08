#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/protos/blob_depot_config.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/shred_helpers.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TKvExternalDataSourceStoragePool) {
    Y_UNIT_TEST(RecoverProvisioningAndVolumeReferences) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().RunFakeConfigDispatcher(true).EnablePipeRetries(true));
        runtime.GetAppData().FeatureFlags.SetEnableExternalDataSourceStoragePools(true);
        runtime.GetAppData().FeatureFlags.SetEnableSchemaSecrets(true);
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController),
            [](const TActorId& tablet, TTabletStorageInfo* info) -> IActor* {
                return new TFakeBSController(tablet, info);
            });
        TBlockEvents<TEvBlobStorage::TEvExternalDataSourcePoolRequest> requests(runtime);

        ui64 txId = 100;
        TestCreateExternalDataSource(runtime, ++txId, "/MyRoot", R"(
            Name: "archive" SourceType: "ObjectStorage" Location: "https://storage.example.org/bucket"
            Auth { Aws {
                AwsAccessKeyIdSecretName: "/MyRoot/access" AwsSecretAccessKeySecretName: "/MyRoot/secret" AwsRegion: "us-east-1"
            } }
            Properties { Properties { key: "storage_pool_name" value: "cold" } }
        )");
        runtime.WaitFor("initial provisioning request", [&] { return !requests.empty(); });
        const auto initial = requests.front()->Get()->Record;
        const auto& prefix = initial.GetGroup().GetS3BackendSettings().GetSettings().GetObjectKeyPattern();
        UNIT_ASSERT(prefix.StartsWith("ydb/") && prefix.size() > 4);
        requests.clear(); // no controller response reached SchemeShard
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        runtime.WaitFor("replayed provisioning request", [&] { return !requests.empty(); });
        const auto& retried = requests.back();
        UNIT_ASSERT_VALUES_EQUAL(retried->Get()->Record.SerializeAsString(), initial.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(retried->Cookie, txId);
        auto response = MakeHolder<TEvBlobStorage::TEvExternalDataSourcePoolResult>();
        response->Record.SetSuccess(true);
        response->Record.SetReady(true);
        response->Record.SetSubTxId(initial.GetSubTxId());
        runtime.Send(new IEventHandle(retried->Sender, retried->Recipient, response.Release(), 0, retried->Cookie));
        env.TestWaitNotification(runtime, txId);

        TestModifyACL(runtime, ++txId, "/MyRoot", "archive", "", "other@external", NKikimrScheme::StatusPreconditionFailed);
        const TString volume = R"(
            Name: "volume" PartitionCount: 1
            StorageConfig {
                Channel { PreferredPoolKind: "pool-kind-1" }
                Channel { PreferredPoolKind: "pool-kind-1" }
                Channel { StoragePool: "cold" }
                Channel { ExternalDataSource: "/MyRoot/archive" }
            }
        )";
        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Deny, NACLib::UpdateRow, "reader");
        AsyncModifyACL(runtime, ++txId, "/MyRoot", "archive", acl.SerializeAsString(), "");
        env.TestWaitNotification(runtime, txId);
        auto* denied = CreateSolomonRequest(++txId, "/MyRoot", volume);
        denied->Record.SetUserToken(NACLib::TUserToken("reader", {}).SerializeAsString());
        AsyncSend(runtime, TTestTxConfig::SchemeShard, denied);
        TestModificationResults(runtime, txId, {{NKikimrScheme::StatusAccessDenied, "Access denied for external data source"}});

        TestCreateSolomon(runtime, ++txId, "/MyRoot", volume);
        env.TestWaitNotification(runtime, txId);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        const auto poolName = initial.GetGroup().GetStoragePoolName();
        const auto described = DescribePath(runtime, "/MyRoot/volume").GetPathDescription().GetSolomonDescription();
        UNIT_ASSERT_VALUES_EQUAL(described.BoundChannelsSize(), 4);
        for (ui32 i : {2u, 3u}) {
            const auto& binding = described.GetBoundChannels(i);
            UNIT_ASSERT_VALUES_EQUAL(binding.GetStoragePoolName(), poolName);
            UNIT_ASSERT_VALUES_EQUAL(binding.GetExternalDataSourcePath(), "/MyRoot/archive");
        }
        auto describeRefs = [&] {
            return DescribePath(runtime, "/MyRoot/archive").GetPathDescription()
                .GetExternalDataSourceDescription().GetReferences();
        };
        const auto refs = describeRefs();
        UNIT_ASSERT_VALUES_EQUAL(refs.ReferencesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(refs.GetReferences(0).GetPath(), "/MyRoot/volume");

        TestDropSolomon(runtime, ++txId, "/MyRoot", "volume");
        env.TestWaitNotification(runtime, txId);
        env.TestWaitShardDeletion(runtime, {described.GetPartitions(0).GetShardIdx()});
        UNIT_ASSERT_VALUES_EQUAL(describeRefs().ReferencesSize(), 0);
    }
}
