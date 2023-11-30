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
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);
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

                ui64 tenantSchemeShard = 0;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLess0"),
                                   {NLs::PathExist,
                                    NLs::IsExternalSubDomain("ServerLess0"),
                                    NLs::ServerlessComputeResourcesMode(SERVERLESS_COMPUTE_RESOURCES_MODE_UNSPECIFIED),
                                    NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});
                TestDescribeResult(DescribePath(runtime, tenantSchemeShard, "/MyRoot/ServerLess0"),
                                   {NLs::PathExist,
                                    NLs::ServerlessComputeResourcesMode(SERVERLESS_COMPUTE_RESOURCES_MODE_UNSPECIFIED)});
            }

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(
                    ServerlessComputeResourcesMode: SERVERLESS_COMPUTE_RESOURCES_MODE_DEDICATED
                    Name: "ServerLess0"
                )"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                ui64 tenantSchemeShard = 0;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLess0"),
                                   {NLs::ServerlessComputeResourcesMode(SERVERLESS_COMPUTE_RESOURCES_MODE_DEDICATED),
                                    NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});
                TestTenantSchemeShardSync(t, runtime, tenantSchemeShard, "/MyRoot/ServerLess0",
                                          ServerlessComputeResourcesMode(SERVERLESS_COMPUTE_RESOURCES_MODE_DEDICATED));
            }

            TestAlterExtSubDomain(runtime, ++t.TxId,  "/MyRoot",
                R"(
                    ServerlessComputeResourcesMode: SERVERLESS_COMPUTE_RESOURCES_MODE_SHARED
                    Name: "ServerLess0"
                )"
            );
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                ui64 tenantSchemeShard = 0;
                TestDescribeResult(DescribePath(runtime, "/MyRoot/ServerLess0"),
                                   {NLs::ServerlessComputeResourcesMode(SERVERLESS_COMPUTE_RESOURCES_MODE_SHARED),
                                    NLs::ExtractTenantSchemeshard(&tenantSchemeShard)});
                TestTenantSchemeShardSync(t, runtime, tenantSchemeShard, "/MyRoot/ServerLess0",
                                          ServerlessComputeResourcesMode(SERVERLESS_COMPUTE_RESOURCES_MODE_SHARED));
            }
        });
    }
}
