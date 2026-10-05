#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>
#include <ydb/core/base/tablet_resolver.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/tx_proxy.pb.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

Y_UNIT_TEST_SUITE(BSCMigration) {
    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

    void AssertDatabaseSpaceThresholds(TEnvironmentSetup& env, TColor::E block, TColor::E unblock) {
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableReadSettings();
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        UNIT_ASSERT_VALUES_EQUAL(response.StatusSize(), 1);
        const auto& settings = response.GetStatus(0).GetSettings();
        UNIT_ASSERT_VALUES_EQUAL(settings.DatabaseSpaceBlockColorSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.DatabaseSpaceUnblockColorSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.GetDatabaseSpaceBlockColor(0), block);
        UNIT_ASSERT_VALUES_EQUAL(settings.GetDatabaseSpaceUnblockColor(0), unblock);
    }

    void RebootTablet(TEnvironmentSetup& env, ui64 tabletId) {
        auto& runtime = *env.Runtime;
        const TActorId sender = runtime.AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        auto* poison = new TEvents::TEvPoison();
        auto* nested = new IEventHandle(TActorId(), sender, poison);
        runtime.Send(new IEventHandle(MakeTabletResolverID(), sender,
            new TEvTabletResolver::TEvForward(tabletId, nested, {},
                TEvTabletResolver::TEvForward::EActor::Tablet)),
            sender.NodeId());
        {
            auto fwd = env.WaitForEdgeActorEvent<TEvTabletResolver::TEvForwardResult>(sender, false);
            UNIT_ASSERT(fwd);
            UNIT_ASSERT_VALUES_EQUAL_C(fwd->Get()->Status, NKikimrProto::OK, fwd->Get()->ToString());
        }
        env.Sim(TDuration::Seconds(5));
        runtime.Send(new IEventHandle(MakeTabletResolverID(), sender,
            new TEvTabletResolver::TEvTabletProblem(tabletId, TActorId())),
            sender.NodeId());
        env.Sim(TDuration::Seconds(5));
        runtime.DestroyActor(sender);
    }

    Y_UNIT_TEST(DatabaseSpaceThresholdsForNewCluster) {
        TEnvironmentSetup env{{.NodeCount = 1}};
        AssertDatabaseSpaceThresholds(env, TColor::YELLOW, TColor::LIGHT_YELLOW);

        RebootTablet(env, env.TabletId);
        AssertDatabaseSpaceThresholds(env, TColor::YELLOW, TColor::LIGHT_YELLOW);
    }

    Y_UNIT_TEST(DatabaseSpaceThresholdsForExistingCluster) {
        // Local MiniKQL needs a function registry, which TTestActorSystem does not provide; it must outlive env.
        const auto functionRegistry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        TEnvironmentSetup env{{.NodeCount = 1}};
        env.Runtime->GetNode(env.Settings.ControllerNodeId)->AppData->FunctionRegistry = functionRegistry.Get();
        env.CreateBoxAndPool(1, 1);

        // Existing clusters have a State row, but no persisted database space thresholds.
        const TActorId sender = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
        auto request = std::make_unique<TEvTablet::TEvLocalMKQL>();
        request->Record.MutableProgram()->MutableProgram()->SetText(R"(
            (
                (return (AsList
                    (UpdateRow 'State
                        '('('FixedKey (Bool 'true)))
                        '('('DatabaseSpaceBlockColor) '('DatabaseSpaceUnblockColor)))
                ))
            )
        )");
        env.Runtime->SendToPipe(env.TabletId, sender, request.release(), 0, TTestActorSystem::GetPipeConfigWithRetries());
        const auto response = env.WaitForEdgeActorEvent<TEvTablet::TEvLocalMKQLResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrProto::OK,
            response->Get()->Record.DebugString());

        RebootTablet(env, env.TabletId);
        AssertDatabaseSpaceThresholds(env, TColor::GREEN, TColor::GREEN);

        RebootTablet(env, env.TabletId);
        AssertDatabaseSpaceThresholds(env, TColor::GREEN, TColor::GREEN);
    }

    Y_UNIT_TEST(DatabaseSpaceThresholdsPreserveExplicitSettings) {
        TEnvironmentSetup env{{.NodeCount = 1}};
        for (const auto [block, unblock] : {std::pair{TColor::RED, TColor::ORANGE},
                std::pair{TColor::GREEN, TColor::GREEN}}) {
            NKikimrBlobStorage::TConfigRequest request;
            auto* settings = request.AddCommand()->MutableUpdateSettings();
            settings->AddDatabaseSpaceBlockColor(block);
            settings->AddDatabaseSpaceUnblockColor(unblock);
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

            RebootTablet(env, env.TabletId);
            AssertDatabaseSpaceThresholds(env, block, unblock);
        }
    }

    Y_UNIT_TEST(RestartBeforeCompatibilityInfoUpdate) {
        TFeatureFlags ff;
        ff.SetBsControllerRestartBeforeCompatibilityInfoUpdate(true);
        TEnvironmentSetup env{{
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .FeatureFlags = std::move(ff),
        }};

        auto compatibilityInfo = MakeCompatibilityInfo(TVersion{ 26, 1, 1, 0 },
                NKikimrConfig::TCompatibilityRule::BlobStorageController);

        TCompatibilityInfoTest::Reset(&compatibilityInfo);
        env.Sim(TDuration::Seconds(30));

        RebootTablet(env, env.TabletId);
    
        env.CreateBoxAndPool(1, 1);
    }

}
