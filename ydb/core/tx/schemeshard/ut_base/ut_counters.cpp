#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardCountersTest) {

    Y_UNIT_TEST(PathsCounterDecrementsOnFail) {
        TSchemeShard* schemeshard;
        auto ssFactory = [&schemeshard](const TActorId &tablet, TTabletStorageInfo *info) {
            schemeshard = new TSchemeShard(tablet, info);
            return schemeshard;
        };
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        TTestEnv env(runtime, opts, ssFactory);
        runtime.GetAppData().FeatureFlags.SetEnableAlterDatabase(true);
        ui64 txId = 100;

        TestAlterSubDomain(runtime, ++txId, "/", R"(
            Name: "MyRoot"
            SchemeLimits {
                MaxPaths: 1
            }
        )");
        env.TestWaitNotification(runtime, txId);

        ui64 initPathsCount = DescribePath(runtime, "/MyRoot").GetPathDescription().GetDomainDescription()
                                                                 .GetPathsInside();

        TSchemeLimits defaultLimits;
        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
            NLs::DomainLimitsIs(1, defaultLimits.MaxShards),
            NLs::PathsInsideDomain(initPathsCount)
        });

        UNIT_ASSERT_VALUES_EQUAL(schemeshard->TabletCounters->Simple()[COUNTER_PATHS].Get(), 0);
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Dir/Table"
            Columns { Name: "key"   Type: "Uint64"}
            Columns { Name: "value"  Type: "Utf8"}
            KeyColumnNames: ["key"]
        )", { NKikimrScheme::StatusResourceExhausted });
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {
            NLs::PathsInsideDomain(initPathsCount)
        });
        UNIT_ASSERT_VALUES_EQUAL(schemeshard->TabletCounters->Simple()[COUNTER_PATHS].Get(), 0);
    }

}
