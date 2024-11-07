#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/base/tablet_resolver.h>

#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/protos/statistics.pb.h>

#include <type_traits>

namespace NKikimr {
namespace NStat {

struct TColumnStatisticsProbes {
    struct TProbe {
        ui64 Value;
        ui64 Probe;
    };

    ui16 Tag;
    std::vector<TProbe> Probes;
};

void CheckColumnStatistics(
    TTestActorRuntime& runtime, const TPathId& pathId, const TActorId& sender, const std::vector<TColumnStatisticsProbes>& expected
) {
    auto evGet = std::make_unique<TEvStatistics::TEvGetStatistics>();
    evGet->StatType = NStat::EStatType::COUNT_MIN_SKETCH;

    for (auto item : expected) {
        NStat::TRequest req;
        req.PathId = pathId;
        req.ColumnTag = item.Tag;
        evGet->StatRequests.push_back(req);
    }

    auto statServiceId = NStat::MakeStatServiceID(runtime.GetNodeId(0));
    runtime.Send(statServiceId, sender, evGet.release(), 0, true);

    auto res = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvGetStatisticsResult>(sender);
    auto msg = res->Get();

    UNIT_ASSERT(msg->Success);
    UNIT_ASSERT( msg->StatResponses.size() == expected.size());

    for (size_t i = 0; i < msg->StatResponses.size(); ++i) {
        const auto& stat = msg->StatResponses[i];
        UNIT_ASSERT(stat.Success);

        auto countMin = stat.CountMinSketch.CountMin.get();
        UNIT_ASSERT(countMin != nullptr);

        for (const auto& item : expected[i].Probes) {
            ui64 value = item.Value;
            auto probe = countMin->Probe((const char*)&value, sizeof(ui64));
            UNIT_ASSERT_VALUES_EQUAL(item.Probe, probe);
        }
    }
}

Y_UNIT_TEST_SUITE(ColumnStatistics) {
    Y_UNIT_TEST(CountMinSketchStatistics) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");

        std::unordered_set<ui64> saveStatisticsQueryResponse;
        auto observer = runtime.AddObserver<TEvStatistics::TEvSaveStatisticsQueryResponse>([&](TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr& ev) {
            saveStatisticsQueryResponse.emplace(ev->Get()->PathId.OwnerId);
        });

        CreateColumnStoreTable(env, "Database", "Table1", 1);

        runtime.DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]() {
                return saveStatisticsQueryResponse.size() >= 1;
            }
        });

        auto sender = runtime.AllocateEdgeActor();
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table1");
        std::vector<TColumnStatisticsProbes> expected = {
            {
                .Tag = 1,
                .Probes{ {1, 4}, {2, 4} }
            }
        };
        CheckColumnStatistics(runtime, pathId, sender, expected);
    }

    Y_UNIT_TEST(CountMinSketchServerlessStatistics) {
        TTestEnv env(1, 3);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless1", "/Root/Shared", 1);
        CreateServerlessDatabase(env, "Serverless2", "/Root/Shared", 1);

        std::unordered_set<ui64> saveStatisticsQueryResponse;
        auto observer = runtime.AddObserver<TEvStatistics::TEvSaveStatisticsQueryResponse>([&](TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr& ev) {
            saveStatisticsQueryResponse.emplace(ev->Get()->PathId.OwnerId);
        });

        CreateColumnStoreTable(env, "Serverless1", "Table1", 1);
        CreateColumnStoreTable(env, "Serverless2", "Table2", 1);

        runtime.DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]() {
                return saveStatisticsQueryResponse.size() >= 2;
            }
        });

        auto sender = runtime.AllocateEdgeActor();
        std::vector<TColumnStatisticsProbes> expected = {
            {
                .Tag = 1,
                .Probes{ {1, 4}, {2, 4} }
            }
        };

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless1/Table1");
        CheckColumnStatistics(runtime, pathId1, sender, expected);

        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless2/Table2");
        CheckColumnStatistics(runtime, pathId2, sender, expected);
    }
}

} // NSysView
} // NKikimr
