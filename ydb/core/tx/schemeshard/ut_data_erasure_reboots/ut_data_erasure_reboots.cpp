#include <ydb/core/blobstorage/base/blobstorage_shred_events.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/schemeshard/ut_helpers/data_erasure_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(DataErasureReboots) {
    Y_UNIT_TEST(Fake) {
    }

    Y_UNIT_TEST(SimpleDataErasureTest) {
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
            runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

            auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
            CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                return new TFakeBSController(tablet, info);
            });

            runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
            auto& dataErasureConfig = runtime.GetAppData().DataErasureConfig;
            dataErasureConfig.SetDataErasureIntervalSeconds(0); // do not schedule
            dataErasureConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

            ui64 txId = 100;

            auto tenantSS = CreateTestExtSubdomain(runtime, *(t.TestEnv), &txId, "Database1");
            auto sender = runtime.AllocateEdgeActor();
            RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
            RebootTablet(runtime, tenantSS, sender);

            {
                TInactiveZone inactive(activeZone);
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
        });
    }
}
