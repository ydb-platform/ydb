#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/blobstorage/base/blobstorage_shred_events.h>
#include <ydb/core/mind/bscontroller/bsc.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

namespace {

ui64 CreateTestSubdomain(TTestActorRuntime& runtime,
    TTestEnv& env,
    ui64* txId,
    const TString& name) {
    TestCreateExtSubDomain(runtime, ++(*txId), "/MyRoot", Sprintf(R"(
        Name: "%s"
    )", name.c_str()));
    env.TestWaitNotification(runtime, *txId);

    TestAlterExtSubDomain(runtime, ++(*txId), "/MyRoot", Sprintf(R"(
        PlanResolution: 50
        Coordinators: 1
        Mediators: 1
        TimeCastBucketsPerMediator: 2
        ExternalSchemeShard: true
        ExternalHive: false
        Name: "%s"
        StoragePools {
            Name: "name_%s_kind_hdd-1"
            Kind: "common"
        }
        StoragePools {
            Name: "name_%s_kind_hdd-2"
            Kind: "external"
        }
    )", name.c_str(), name.c_str(), name.c_str()));
    env.TestWaitNotification(runtime, *txId);

    ui64 schemeshardId;
    TestDescribeResult(DescribePath(runtime, TStringBuilder() << "/MyRoot/" << name), {
        NLs::PathExist,
        NLs::ExtractTenantSchemeshard(&schemeshardId)
    });

    TestCreateTable(runtime, schemeshardId, ++(*txId), TStringBuilder() << "/MyRoot/" << name,
        R"____(
            Name: "Simple"
            Columns { Name: "key1"  Type: "Uint32"}
            Columns { Name: "Value" Type: "Utf8"}
            KeyColumnNames: ["key1"]
            UniformPartitionsCount: 2
        )____");
    env.TestWaitNotification(runtime, *txId, schemeshardId);

    return schemeshardId;
}

} // namespace

Y_UNIT_TEST_SUITE(TestSuete1) {
    Y_UNIT_TEST(test1) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        // runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController),
                     &CreateFlatBsController);

        ui64 txId = 100;

        CreateTestSubdomain(runtime, env, &txId, "Database1");
        CreateTestSubdomain(runtime, env, &txId, "Database2");


        env.SimulateSleep(runtime, TDuration::Seconds(3));

        auto sender = runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvSchemeShard::TEvDataErasureInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDataErasureInfoResponse>(handle);

        UNIT_ASSERT_EQUAL(response->Record.GetGeneration(), 1);
        UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvDataErasureInfoResponse::COMPLETE);
    }
}
