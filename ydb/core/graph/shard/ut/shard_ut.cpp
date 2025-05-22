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
        stream << it->Seconds();
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

Y_DECLARE_OUT_SPEC(, std::vector<std::vector<double>>, stream, value) {
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
    Y_UNIT_TEST(NormalizeAndDownsample1) {
        NGraph::TBaseBackend::TMetricsValues values;
        values.Timestamps = {
            TInstant::Seconds( 100 ),
            TInstant::Seconds( 200 ),
            TInstant::Seconds( 300 ),
            TInstant::Seconds( 400 ),
            TInstant::Seconds( 500 ),
            TInstant::Seconds( 600 ),
            TInstant::Seconds( 700 ),
            TInstant::Seconds( 800 ),
            TInstant::Seconds( 900 ),
            TInstant::Seconds( 1000 )
        };
        values.Values.push_back({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        values.Values.push_back({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

        {
            NGraph::TBaseBackend::NormalizeAndDownsample(values, 10);
            Ctest << values.Timestamps << Endl;
            Ctest << values.Values << Endl;
            std::vector<TInstant> canonTimestamps = {
                TInstant::Seconds( 100 ),
                TInstant::Seconds( 200 ),
                TInstant::Seconds( 300 ),
                TInstant::Seconds( 400 ),
                TInstant::Seconds( 500 ),
                TInstant::Seconds( 600 ),
                TInstant::Seconds( 700 ),
                TInstant::Seconds( 800 ),
                TInstant::Seconds( 900 ),
                TInstant::Seconds( 1000 )
            };
            std::vector<double> canonValues = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
            UNIT_ASSERT(values.Timestamps == canonTimestamps);
            UNIT_ASSERT(values.Values.size() == 2);
            UNIT_ASSERT(values.Values[0] == canonValues);
            UNIT_ASSERT(values.Values[1] == canonValues);
        }
    }

    Y_UNIT_TEST(NormalizeAndDownsample2) {
        NGraph::TBaseBackend::TMetricsValues values;
        values.Timestamps = {
            TInstant::Seconds( 100 ),
            TInstant::Seconds( 200 ),
            TInstant::Seconds( 300 ),
            TInstant::Seconds( 400 ),
            TInstant::Seconds( 500 ),
            TInstant::Seconds( 510 ),
            TInstant::Seconds( 520 ),
            TInstant::Seconds( 530 ),
            TInstant::Seconds( 540 ),
            TInstant::Seconds( 550 ),
            TInstant::Seconds( 560 ),
            TInstant::Seconds( 570 ),
            TInstant::Seconds( 580 ),
            TInstant::Seconds( 590 ),
            TInstant::Seconds( 600 )
        };
        values.Values.push_back({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});

        {
            NGraph::TBaseBackend::NormalizeAndDownsample(values, 10);
            Ctest << values.Timestamps << Endl;
            Ctest << values.Values << Endl;
            std::vector<TInstant> canonTimestamps = {
                TInstant::Seconds( 100 ),
                TInstant::Seconds( 200 ),
                TInstant::Seconds( 300 ),
                TInstant::Seconds( 400 ),
                TInstant::Seconds( 500 ),
                TInstant::Seconds( 520 ),
                TInstant::Seconds( 540 ),
                TInstant::Seconds( 560 ),
                TInstant::Seconds( 580 ),
                TInstant::Seconds( 600 )
            };
            std::vector<double> canonValues = {1, 2, 3, 4, 5, 6.5, 8.5, 10.5, 12.5, 14.5};
            UNIT_ASSERT(values.Timestamps == canonTimestamps);
            UNIT_ASSERT(values.Values.size() == 1);
            UNIT_ASSERT(values.Values[0] == canonValues);
        }
    }

    Y_UNIT_TEST(NormalizeAndDownsample3) {
        NGraph::TBaseBackend::TMetricsValues values;
        values.Timestamps = {
            TInstant::Seconds( 100 ),
            TInstant::Seconds( 200 ),
            TInstant::Seconds( 300 ),
            TInstant::Seconds( 400 ),
            TInstant::Seconds( 500 ),
            TInstant::Seconds( 510 ),
            TInstant::Seconds( 520 ),
            TInstant::Seconds( 530 ),
            TInstant::Seconds( 540 ),
            TInstant::Seconds( 550 ),
            TInstant::Seconds( 560 ),
            TInstant::Seconds( 570 ),
            TInstant::Seconds( 580 ),
            TInstant::Seconds( 590 ),
            TInstant::Seconds( 600 )
        };
        values.Values.push_back({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});

        {
            NGraph::TBaseBackend::NormalizeAndDownsample(values, 6);
            Ctest << values.Timestamps << Endl;
            Ctest << values.Values << Endl;
            std::vector<TInstant> canonTimestamps = {
                TInstant::Seconds( 100 ),
                TInstant::Seconds( 200 ),
                TInstant::Seconds( 300 ),
                TInstant::Seconds( 400 ),
                TInstant::Seconds( 500 ),
                TInstant::Seconds( 600 )
            };
            std::vector<double> canonValues = {1, 2, 3, 4, 5, 10.5};
            UNIT_ASSERT(values.Timestamps == canonTimestamps);
            UNIT_ASSERT(values.Values.size() == 1);
            UNIT_ASSERT(values.Values[0] == canonValues);
        }
    }

    Y_UNIT_TEST(NormalizeAndDownsample4) {
        NGraph::TBaseBackend::TMetricsValues values;
        values.Timestamps = {
            TInstant::Seconds( 100 ),
            TInstant::Seconds( 200 ),
            TInstant::Seconds( 300 ),
            TInstant::Seconds( 400 ),
            TInstant::Seconds( 500 ),
            TInstant::Seconds( 510 ),
            TInstant::Seconds( 520 ),
            TInstant::Seconds( 530 ),
            TInstant::Seconds( 540 ),
            TInstant::Seconds( 550 ),
            TInstant::Seconds( 560 ),
            TInstant::Seconds( 570 ),
            TInstant::Seconds( 580 ),
            TInstant::Seconds( 590 ),
            TInstant::Seconds( 600 )
        };
        values.Values.push_back({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});

        {
            NGraph::TBaseBackend::NormalizeAndDownsample(values, 3);
            Ctest << values.Timestamps << Endl;
            Ctest << values.Values << Endl;
            std::vector<TInstant> canonTimestamps = {
                TInstant::Seconds( 100 ),
                TInstant::Seconds( 350 ),
                TInstant::Seconds( 600 )
            };
            std::vector<double> canonValues = {1, 3, 10};
            UNIT_ASSERT(values.Timestamps == canonTimestamps);
            UNIT_ASSERT(values.Values.size() == 1);
            UNIT_ASSERT(values.Values[0] == canonValues);
        }
    }

    Y_UNIT_TEST(NormalizeAndDownsample5) {
        NGraph::TBaseBackend::TMetricsValues values;
        values.Timestamps = {
            TInstant::Seconds( 100 ),
            TInstant::Seconds( 200 ),
            TInstant::Seconds( 300 ),
            TInstant::Seconds( 400 ),
            TInstant::Seconds( 500 ),
            TInstant::Seconds( 510 ),
            TInstant::Seconds( 520 ),
            TInstant::Seconds( 530 ),
            TInstant::Seconds( 540 ),
            TInstant::Seconds( 550 ),
            TInstant::Seconds( 560 ),
            TInstant::Seconds( 570 ),
            TInstant::Seconds( 580 ),
            TInstant::Seconds( 590 ),
            TInstant::Seconds( 600 )
        };
        values.Values.push_back({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, NAN, 12, NAN, 14, 15});

        {
            NGraph::TBaseBackend::NormalizeAndDownsample(values, 10);
            Ctest << values.Timestamps << Endl;
            Ctest << values.Values << Endl;
            std::vector<TInstant> canonTimestamps = {
                TInstant::Seconds( 100 ),
                TInstant::Seconds( 200 ),
                TInstant::Seconds( 300 ),
                TInstant::Seconds( 400 ),
                TInstant::Seconds( 500 ),
                TInstant::Seconds( 520 ),
                TInstant::Seconds( 540 ),
                TInstant::Seconds( 560 ),
                TInstant::Seconds( 580 ),
                TInstant::Seconds( 600 )
            };
            std::vector<double> canonValues = {1, 2, 3, 4, 5, 6.5, 8.5, 10, 12, 14.5};
            UNIT_ASSERT(values.Timestamps == canonTimestamps);
            UNIT_ASSERT(values.Values.size() == 1);
            UNIT_ASSERT(values.Values[0] == canonValues);
        }
    }

    Y_UNIT_TEST(NormalizeAndDownsample6) {
        NGraph::TBaseBackend::TMetricsValues values;
        values.Timestamps = {
            TInstant::Seconds( 100 ),
            TInstant::Seconds( 200 ),
            TInstant::Seconds( 300 ),
            TInstant::Seconds( 400 ),
            TInstant::Seconds( 500 ),
            TInstant::Seconds( 510 ),
            TInstant::Seconds( 520 ),
            TInstant::Seconds( 530 ),
            TInstant::Seconds( 540 ),
            TInstant::Seconds( 550 ),
            TInstant::Seconds( 560 ),
            TInstant::Seconds( 570 ),
            TInstant::Seconds( 580 ),
            TInstant::Seconds( 590 ),
            TInstant::Seconds( 600 )
        };
        values.Values.push_back({1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15});

        {
            NGraph::TBaseBackend::NormalizeAndDownsample(values, 1);
            Ctest << values.Timestamps << Endl;
            Ctest << values.Values << Endl;
            std::vector<TInstant> canonTimestamps = {
                TInstant::Seconds( 600 )
            };
            std::vector<double> canonValues = {8};
            UNIT_ASSERT(values.Timestamps == canonTimestamps);
            UNIT_ASSERT(values.Values.size() == 1);
            UNIT_ASSERT(values.Values[0] == canonValues);
        }
    }

    Y_UNIT_TEST(CheckHistogramToPercentileConversions) {
        TVector<ui64> bounds = {2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, std::numeric_limits<ui64>::max()};
        TVector<ui64> values = {10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 0};
        ui64 total = std::accumulate(values.begin(), values.end(), 0);
        UNIT_ASSERT(total == 100);
        auto p50 = NGraph::TBaseBackend::GetTimingForPercentile(.50, values, bounds, total);
        Ctest << "p50=" << p50 << Endl;
        UNIT_ASSERT(!isnan(p50));
        UNIT_ASSERT(abs(p50 - 32) < 0.01); // 32ms
        auto p75 = NGraph::TBaseBackend::GetTimingForPercentile(.75, values, bounds, total);
        Ctest << "p75=" << p75 << Endl;
        UNIT_ASSERT(!isnan(p75));
        UNIT_ASSERT(abs(p75 - 192) < 0.01); // 192ms
        auto p90 = NGraph::TBaseBackend::GetTimingForPercentile(.90, values, bounds, total);
        Ctest << "p90=" << p90 << Endl;
        UNIT_ASSERT(!isnan(p90));
        UNIT_ASSERT(abs(p90 - 512) < 0.01); // 512ms
        auto p99 = NGraph::TBaseBackend::GetTimingForPercentile(.99, values, bounds, total);
        Ctest << "p99=" << p99 << Endl;
        UNIT_ASSERT(!isnan(p99));
        UNIT_ASSERT(abs(p99 - 972.8) < 0.01); // 972.8ms
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
