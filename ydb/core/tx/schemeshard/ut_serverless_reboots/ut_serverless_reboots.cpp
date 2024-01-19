#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

using enum NKikimrSubDomains::EServerlessComputeResourcesMode;

Y_UNIT_TEST_SUITE(TSchemeShardServerLessReboots) {

    using TCheckFunction = std::function<bool (const NKikimrScheme::TEvDescribeSchemeResult&)>;

    void TestTenantSchemeShardSync(const TTestWithReboots& t, TTestActorRuntime& runtime, ui64 tenantSchemeShard,
                                   const TString& path, TCheckFunction checkFunction) {
        bool checkPassed = false;
        for (size_t i = 0; i < 100; ++i) {
            const auto describeResult = DescribePath(runtime, tenantSchemeShard, path);
            if (checkFunction(describeResult)) {
                checkPassed = true;
                break;
            } 
            t.TestEnv->SimulateSleep(runtime, TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(checkPassed);
    }

    TCheckFunction ServerlessComputeResourcesMode(EServerlessComputeResourcesMode serverlessComputeResourcesMode) {
        return [=] (const NKikimrScheme::TEvDescribeSchemeResult& describeResult) {
            if (describeResult.GetStatus() != NKikimrScheme::EStatus::StatusSuccess) {
                return false;
            }
            const auto& domainDesc = describeResult.GetPathDescription().GetDomainDescription();
            if (serverlessComputeResourcesMode) {
                return domainDesc.GetServerlessComputeResourcesMode() == serverlessComputeResourcesMode;
            } else {
                return !domainDesc.HasServerlessComputeResourcesMode();
            }
        };
    }

    Y_UNIT_TEST(TestServerlessComputeResourcesModeWithReboots) {
        TTestWithReboots t;
        t.GetTestEnvOptions().EnableServerlessExclusiveDynamicNodes(true);
        
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            ui64 sharedHive = 0;
            ui64 tenantSchemeShard = 0;

            TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(Name: "SharedDB")"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(
                    StoragePools {
                        Name: "pool-1"
                        Kind: "pool-kind-1"
                    }
                    StoragePools {
                        Name: "pool-2"
                        Kind: "pool-kind-2"
                    }
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    ExternalSchemeShard: true
                    ExternalHive: true
                    Name: "SharedDB"
                )"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);

                ui64 sharedDbSchemeShard = 0;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/SharedDB"),
                                    {NLs::PathExist,
                                     NLs::ExtractTenantSchemeshard(&sharedDbSchemeShard),
                                     NLs::ExtractDomainHive(&sharedHive),
                                     NLs::ServerlessComputeResourcesMode(EServerlessComputeResourcesModeUnspecified)});
                
                UNIT_ASSERT(sharedHive != 0
                            && sharedHive != (ui64)-1
                            && sharedHive != TTestTxConfig::Hive);
                            
                UNIT_ASSERT(sharedDbSchemeShard != 0
                            && sharedDbSchemeShard != (ui64)-1
                            && sharedDbSchemeShard != TTestTxConfig::SchemeShard);
                
                TestDescribeResult(DescribePath(runtime, sharedDbSchemeShard, "/MyRoot/SharedDB"),
                                    {NLs::PathExist,
                                     NLs::ServerlessComputeResourcesMode(EServerlessComputeResourcesModeUnspecified)});
            }

            TString createData = Sprintf(
                R"(
                    ResourcesDomainKey {
                        SchemeShard: %lu
                        PathId: 3 
                    }
                    Name: "ServerLess0"
                )",
                TTestTxConfig::SchemeShard
            );
            TestCreateExtSubDomain(runtime, ++t.TxId,  "/MyRoot", createData);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    ExternalSchemeShard: true
                    ExternalHive: false
                    StoragePools {
                        Name: "pool-1"
                        Kind: "pool-kind-1"
                    }
                    Name: "ServerLess0"
                )"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
                
            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLess0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("ServerLess0"),
                                    NLs::ServerlessComputeResourcesMode(EServerlessComputeResourcesModeShared),
                                    NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});
                                    
                UNIT_ASSERT(tenantSchemeShard != 0
                            && tenantSchemeShard != (ui64)-1
                            && tenantSchemeShard != TTestTxConfig::SchemeShard);

                TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLess0"),
                                   {NLs::PathExist,
                                    NLs::ServerlessComputeResourcesMode(EServerlessComputeResourcesModeShared)});
            }

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(
                    ServerlessComputeResourcesMode: EServerlessComputeResourcesModeExclusive
                    Name: "ServerLess0"
                )"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLess0"),
                                   {NLs::ServerlessComputeResourcesMode(EServerlessComputeResourcesModeExclusive)});
                t.TestEnv->TestServerlessComputeResourcesModeInHive(runtime, "/MyRoot/ServerLess0",
                                                                    EServerlessComputeResourcesModeExclusive, sharedHive);
                TestTenantSchemeShardSync(t, runtime, tenantSchemeShard, "/MyRoot/ServerLess0",
                                          ServerlessComputeResourcesMode(EServerlessComputeResourcesModeExclusive));
            }

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(
                    ServerlessComputeResourcesMode: EServerlessComputeResourcesModeShared
                    Name: "ServerLess0"
                )"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLess0"),
                                   {NLs::ServerlessComputeResourcesMode(EServerlessComputeResourcesModeShared)});
                t.TestEnv->TestServerlessComputeResourcesModeInHive(runtime, "/MyRoot/ServerLess0",
                                                                    EServerlessComputeResourcesModeShared, sharedHive);
                TestTenantSchemeShardSync(t, runtime, tenantSchemeShard, "/MyRoot/ServerLess0",
                                          ServerlessComputeResourcesMode(EServerlessComputeResourcesModeShared));
            }
        });
    }
}
