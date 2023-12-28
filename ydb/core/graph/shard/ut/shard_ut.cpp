#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <util/stream/output.h>
#include <ydb/core/graph/shard/backends.h>

#ifdef NDEBUG
#define Ctest Cnull
#else
#define Ctest Cerr
#endif

Y_DECLARE_OUT_SPEC(, std::vector<TInstant>, stream, value) {
    stream << '[';
    for (auto it = value.begin(); it != value.end(); ++it) {
        if (it != value.begin()) {
            stream << ',';
        }
        stream << it->GetValue();
    }
    stream << ']';
}

Y_DECLARE_OUT_SPEC(, std::vector<double>, stream, value) {
    stream << '[';
    for (auto it = value.begin(); it != value.end(); ++it) {
        if (it != value.begin()) {
            stream << ',';
        }
        stream << *it;
    }
    stream << ']';
}

namespace NKikimr {

using namespace Tests;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(GraphShard) {
    Y_UNIT_TEST(DownsampleFixed) {
        std::vector<TInstant> sourceData = {
            TInstant::FromValue( 1 ),
            TInstant::FromValue( 2 ),
            TInstant::FromValue( 3 ),
            TInstant::FromValue( 4 ),
            TInstant::FromValue( 5 ),
            TInstant::FromValue( 6 ),
            TInstant::FromValue( 7 ),
            TInstant::FromValue( 8 ),
            TInstant::FromValue( 9 ),
            TInstant::FromValue( 10 )
        };
        {
            std::vector<TInstant> targetData = NGraph::TMemoryBackend::Downsample(sourceData, 10);
            Ctest << targetData << Endl;
            std::vector<TInstant> canonData = {
                TInstant::FromValue( 1 ),
                TInstant::FromValue( 2 ),
                TInstant::FromValue( 3 ),
                TInstant::FromValue( 4 ),
                TInstant::FromValue( 5 ),
                TInstant::FromValue( 6 ),
                TInstant::FromValue( 7 ),
                TInstant::FromValue( 8 ),
                TInstant::FromValue( 9 ),
                TInstant::FromValue( 10 )
            };
            UNIT_ASSERT(targetData == canonData);
        }
        {
            std::vector<TInstant> targetData = NGraph::TMemoryBackend::Downsample(sourceData, 5);
            Ctest << targetData << Endl;
            std::vector<TInstant> canonData = {
                TInstant::FromValue( 1 ),
                TInstant::FromValue( 3 ),
                TInstant::FromValue( 5 ),
                TInstant::FromValue( 7 ),
                TInstant::FromValue( 9 )
            };
            UNIT_ASSERT(targetData == canonData);
        }
        {
            std::vector<TInstant> targetData = NGraph::TMemoryBackend::Downsample(sourceData, 1);
            Ctest << targetData << Endl;
            std::vector<TInstant> canonData = { TInstant::FromValue( 1 ) };
            UNIT_ASSERT(targetData == canonData);
        }
    }

    Y_UNIT_TEST(DownsampleFloat) {
        std::vector<double> sourceData = {1,2,3,4,5, 6, 7, 8, 9, 10};
        {
            std::vector<double> targetData = NGraph::TMemoryBackend::Downsample(sourceData, 10);
            Ctest << targetData << Endl;
            std::vector<double> canonData = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
            UNIT_ASSERT(targetData == canonData);
        }
        {
            std::vector<double> targetData = NGraph::TMemoryBackend::Downsample(sourceData, 5);
            Ctest << targetData << Endl;
            std::vector<double> canonData = {1.5, 3.5, 5.5, 7.5, 9.5};
            UNIT_ASSERT(targetData == canonData);
        }
        {
            std::vector<double> targetData = NGraph::TMemoryBackend::Downsample(sourceData, 1);
            Ctest << targetData << Endl;
            std::vector<double> canonData = {5.5};
            UNIT_ASSERT(targetData == canonData);
        }
    }

    TTenantTestConfig GetTenantTestConfig() {
        return {
            .Domains = {
                {
                    .Name = DOMAIN1_NAME,
                    .SchemeShardId = SCHEME_SHARD1_ID,
                    .Subdomains = {TENANT1_1_NAME, TENANT1_2_NAME}
                }
            },
            .HiveId = HIVE_ID,
            .FakeTenantSlotBroker = true,
            .FakeSchemeShard = true,
            .CreateConsole = false,
            .Nodes = {
                {
                    .TenantPoolConfig = {
                        .StaticSlots = {
                            {
                                .Tenant = DOMAIN1_NAME,
                                .Limit = {
                                    .CPU = 1,
                                    .Memory = 1,
                                    .Network = 1
                                }
                            }
                        },
                        .NodeType = "node-type"
                    }
                }
            },
            .DataCenterCount = 1
        };
    }

    Y_UNIT_TEST(CreateGraphShard) {
        TTestBasicRuntime runtime;

        runtime.SetLogPriority(NKikimrServices::GRAPH, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::HIVE, NLog::PRI_TRACE);

        TTestEnv env(runtime);
        ui64 txId = 100;
        TestCreateExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
        )");

        env.TestWaitNotification(runtime, txId);

        TestAlterExtSubDomain(runtime, ++txId,  "/MyRoot", R"(
            Name: "db1"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools {
                Name: "pool-1"
                Kind: "hdd"
            }
            ExternalSchemeShard: true
            ExternalHive: true
            GraphShard: true
        )");

        env.TestWaitNotification(runtime, txId);

        auto result = DescribePath(runtime, "/MyRoot/db1");
        UNIT_ASSERT(result.GetPathDescription().GetDomainDescription().GetProcessingParams().GetGraphShard() != 0);
    }
}

} // NKikimr
