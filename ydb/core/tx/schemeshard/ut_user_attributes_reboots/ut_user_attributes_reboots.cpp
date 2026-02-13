#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <google/protobuf/text_format.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TUserAttrsTestWithReboots) {
    Y_UNIT_TEST(InSubdomain) { //+
        TTestWithReboots t(true);
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TVector<TString> userAttrsKeys{"AttrA1", "AttrA2"};
            TUserAttrs userAttrs{{"AttrA1", "ValA1"}, {"AttrA2", "ValA2"}};
            TPathVersion pathVer;

            ui64 expectedDomainPaths;
            {
                TInactiveZone inactive(activeZone);
                auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
                expectedDomainPaths = initialDomainDesc.GetPathDescription().GetDomainDescription().GetPathsInside();
            }

            {
                TInactiveZone inactive(activeZone);
                TestCreateSubDomain(runtime, ++t.TxId,  "/MyRoot/DirA",
                                    "PlanResolution: 50 "
                                    "Coordinators: 1 "
                                    "Mediators: 2 "
                                    "TimeCastBucketsPerMediator: 2 "
                                    "Name: \"USER_0\"",
                                    AlterUserAttrs(userAttrs));
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                pathVer = TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                             {NLs::Finished,
                                              NLs::PathsInsideDomain(0),
                                              NLs::ShardsInsideDomain(3),
                                              NLs::UserAttrsEqual(userAttrs)});
            }

            t.TestEnv->ReliablePropose(runtime, UserAttrsRequest(++t.TxId,  "/MyRoot/DirA", "USER_0",
                                       AlterUserAttrs({}, userAttrsKeys), {pathVer}),
                                       {NKikimrScheme::StatusAccepted, NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusPreconditionFailed});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone guard(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::Finished,
                                    NLs::PathsInsideDomain(0),
                                    NLs::ShardsInsideDomain(3),
                                    NLs::UserAttrsEqual({})});
            }

            AsyncDropSubDomain(runtime, ++t.TxId,  "/MyRoot/DirA", "USER_0");
            t.TestEnv->TestWaitTabletDeletion(runtime, {TTestTxConfig::FakeHiveTablets, TTestTxConfig::FakeHiveTablets+1, TTestTxConfig::FakeHiveTablets+2});
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA/USER_0"),
                                   {NLs::PathNotExist});

                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirA"),
                                   {NLs::NoChildren,
                                    NLs::PathVersionEqual(7),
                                    NLs::PathsInsideDomain(expectedDomainPaths),
                                    NLs::ShardsInsideDomain(0)});
            }
        });
    }

    Y_UNIT_TEST(Reboots) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            TestMkDir(runtime, ++t.TxId, "/MyRoot", "DirB",
                      {NKikimrScheme::StatusAccepted}, AlterUserAttrs({{"AttrA1", "ValA1"}}));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                   {NLs::PathVersionEqual(3)});
            }

            {
                TInactiveZone inactive(activeZone);
                AsyncUserAttrs(runtime, ++t.TxId, "/MyRoot", "DirB", AlterUserAttrs({{"AttrA", "ValA"}}));
                AsyncUserAttrs(runtime, ++t.TxId, "/MyRoot", "DirB", AlterUserAttrs({{"AttrA2", "ValA2"}}));
                TestModificationResult(runtime, t.TxId - 1, NKikimrScheme::StatusAccepted);
                TestModificationResult(runtime, t.TxId , NKikimrScheme::StatusMultipleModifications);
            }
            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId - 1});

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                   {NLs::UserAttrsEqual({{"AttrA", "ValA"}, {"AttrA1", "ValA1"}}), NLs::PathVersionEqual(4)});
            }

            TestUserAttrs(runtime, ++t.TxId, "/MyRoot", "DirB", AlterUserAttrs({{"AttrA3", "ValA3"}}, {"AttrA"}));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                   {NLs::UserAttrsEqual({{"AttrA3", "ValA3"}, {"AttrA1", "ValA1"}}), NLs::PathVersionEqual(5)});
            }

            TestUserAttrs(runtime, ++t.TxId, "/MyRoot", "DirB", AlterUserAttrs({}, {"AttrA3", "AttrA1"}));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/DirB"),
                                   {NLs::UserAttrsEqual({}), NLs::PathVersionEqual(6)});
            }
        });
    }

    Y_UNIT_TEST(AllowedSymbolsReboots) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {

            ui64 expectedDomainVersion;
            {
                TInactiveZone inactive(activeZone);
                auto initialDomainDesc = DescribePath(runtime, "/MyRoot");
                expectedDomainVersion = initialDomainDesc.GetPathDescription().GetSelf().GetPathVersion();
            }

            AsyncMkDir(runtime, ++t.TxId, "/MyRoot", "Dir0:");
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            // After creating Dir0:: MyRoot version increases by 1
            expectedDomainVersion += 1;

            TSchemeLimits limits;
            limits.ExtraPathSymbolsAllowed = "!?@";
            SetSchemeshardSchemaLimits(runtime, limits);

            TestMkDir(runtime, ++t.TxId, "/MyRoot", "Dir1:", {NKikimrScheme::StatusSchemeError});
            // Dir1: creation failed, no version change

            TestMkDir(runtime, ++t.TxId, "/MyRoot", "Dir!");
            // After creating Dir!: MyRoot version increases by 1
            expectedDomainVersion += 1;

            TestMkDir(runtime, ++t.TxId, "/MyRoot", "Dir@");
            // After creating Dir@: MyRoot version increases by 1
            expectedDomainVersion += 1;

            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1, t.TxId-2});
            limits.ExtraPathSymbolsAllowed = "!";
            SetSchemeshardSchemaLimits(runtime, limits);

            TestMkDir(runtime, ++t.TxId, "/MyRoot/Dir@", "Dir!");
            // After creating Dir@/Dir!: MyRoot version increases by 4 (creating subdirectory affects parent)
            expectedDomainVersion += 4;

            TestMkDir(runtime, ++t.TxId, "/MyRoot/Dir@", "Dir@", {NKikimrScheme::StatusSchemeError});
            // Dir@/Dir@ creation failed, no version change

            t.TestEnv->TestWaitNotification(runtime, t.TxId - 1);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                                   {NLs::PathVersionEqual(expectedDomainVersion)});

                TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir@"),
                                   {NLs::Finished, NLs::PathVersionEqual(5)});
                TestDescribeResult(DescribePath(runtime, "/MyRoot/Dir!"),
                                   {NLs::Finished, NLs::NoChildren, NLs::PathVersionEqual(3)});
            }
        });
    }
}
