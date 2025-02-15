#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/blobstorage/base/blobstorage_shred_events.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

namespace {

ui64 CreateTestSubdomain(
    TTestActorRuntime& runtime,
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

class TFakeBSController : public TActor<TFakeBSController>, public NTabletFlatExecutor::TTabletExecutedFlat {
    void DefaultSignalTabletActive(const TActorContext &) override
    {
        // must be empty
    }

    void OnActivateExecutor(const TActorContext &) override
    {
        Become(&TThis::StateWork);
        SignalTabletActive(SelfId());
    }

    void OnDetach(const TActorContext &ctx) override
    {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &, const TActorContext &ctx) override
    {
        Die(ctx);
    }

public:
    TFakeBSController(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, nullptr)
    {
    }

    STFUNC(StateInit)
    {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvControllerShredRequest, Handle);
        }
    }

    void Handle(TEvBlobStorage::TEvControllerShredRequest::TPtr& ev, const TActorContext& ctx) {
        auto record = ev->Get()->Record;
        if (record.GetGeneration() > Generation) {
            Generation = record.GetGeneration();
            Completed = false;
            Progress = 0;
        } else if (record.GetGeneration() == Generation) {
            if (!Completed) {
                Progress += 5000;
                if (Progress >= 10000) {
                    Progress = 10000;
                    Completed = true;
                }
            }
        }
        ctx.Send(ev->Sender, new TEvBlobStorage::TEvControllerShredResponse(Generation, Completed, Progress));
    }

public:
    ui64 Generation = 0;
    bool Completed = true;
    ui32 Progress = 10000;
};

} // namespace

Y_UNIT_TEST_SUITE(TestDataErasure) {
    Y_UNIT_TEST(SimpleDataErasureTest) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
            return new TFakeBSController(tablet, info);
        });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& dataErasureConfig = runtime.GetAppData().DataErasureConfig;
        dataErasureConfig.SetDataErasureIntervalSeconds(3);
        dataErasureConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestSubdomain(runtime, env, &txId, "Database1");
        CreateTestSubdomain(runtime, env, &txId, "Database2");

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
        runtime.DispatchEvents(options);

        auto request = MakeHolder<TEvSchemeShard::TEvDataErasureInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDataErasureInfoResponse>(handle);

        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 1, response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvDataErasureInfoResponse::COMPLETED);
    }

    Y_UNIT_TEST(DataErasureRun3Cycles) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
            return new TFakeBSController(tablet, info);
        });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& dataErasureConfig = runtime.GetAppData().DataErasureConfig;
        dataErasureConfig.SetDataErasureIntervalSeconds(3);
        dataErasureConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestSubdomain(runtime, env, &txId, "Database1");
        CreateTestSubdomain(runtime, env, &txId, "Database2");

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 9));
        runtime.DispatchEvents(options);

        auto request = MakeHolder<TEvSchemeShard::TEvDataErasureInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDataErasureInfoResponse>(handle);

        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 3, response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvDataErasureInfoResponse::COMPLETED);
    }

    Y_UNIT_TEST(DataErasureManualLaunch) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                return new TFakeBSController(tablet, info);
            });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& dataErasureConfig = runtime.GetAppData().DataErasureConfig;
        dataErasureConfig.SetForceManualStartup(true);
        dataErasureConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestSubdomain(runtime, env, &txId, "Database1");
        CreateTestSubdomain(runtime, env, &txId, "Database2");

        {
            auto request = MakeHolder<TEvSchemeShard::TEvDataErasureManualStartupRequest>();
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());
        }

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
        runtime.DispatchEvents(options);

        auto request = MakeHolder<TEvSchemeShard::TEvDataErasureInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDataErasureInfoResponse>(handle);

        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 1, response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvDataErasureInfoResponse::COMPLETED);
    }

    Y_UNIT_TEST(DataErasureManualLaunch3Cycles) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                return new TFakeBSController(tablet, info);
            });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& dataErasureConfig = runtime.GetAppData().DataErasureConfig;
        dataErasureConfig.SetForceManualStartup(true);
        dataErasureConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestSubdomain(runtime, env, &txId, "Database1");
        CreateTestSubdomain(runtime, env, &txId, "Database2");

        auto RunDataErasure = [&runtime] (ui32 expectedGeneration) {
            auto sender = runtime.AllocateEdgeActor();
            {
                auto request = MakeHolder<TEvSchemeShard::TEvDataErasureManualStartupRequest>();
                runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());
            }

            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
            runtime.DispatchEvents(options);

            auto request = MakeHolder<TEvSchemeShard::TEvDataErasureInfoRequest>();
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvDataErasureInfoResponse>(handle);

            UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), expectedGeneration, response->Record.GetGeneration());
            UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvDataErasureInfoResponse::COMPLETED);
        };

        RunDataErasure(1);
        RunDataErasure(2);
        RunDataErasure(3);
    }
}
