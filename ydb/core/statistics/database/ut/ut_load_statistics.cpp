#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/database/database.h>
#include <ydb/core/statistics/service/service.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>


namespace NKikimr::NStat {

void AggregateStatistics(
    TTestActorRuntime& runtime, ui64 round, ui64 nodeId, const TString& tablePath, const TActorId& sender
) {
    auto shardIds = GetColumnTableShards(runtime, sender, tablePath);
    auto pathId = ResolvePathId(runtime, tablePath);
    std::vector<ui32> tags = {1, 2};

    auto ev = std::make_unique<TEvStatistics::TEvAggregateStatistics>();
    auto& record = ev->Record;
    record.SetRound(round);

    PathIdFromPathId(pathId, record.MutablePathId());

    auto columnTags = record.MutableColumnTags();
    for (auto tag : tags) {
        columnTags->Add(tag);
    }
    
    auto node = record.AddNodes();
    node->SetNodeId(nodeId);

    auto tabletIds = node->MutableTabletIds();
    for (auto tabletId : shardIds) {
        tabletIds->Add(tabletId);
    }

    auto statisticActor =  NStat::MakeStatServiceID(runtime.GetNodeId(0));
    runtime.Send(statisticActor, sender, ev.release());

    auto evResponse = runtime.GrabEdgeEvent<TEvStatistics::TEvAggregateStatisticsResponse>(sender);
    const auto& response = evResponse->Get()->Record;

    UNIT_ASSERT(response.GetFailedTablets().empty());
    UNIT_ASSERT(response.GetColumns().size() == 2);

    const auto& columns = response.GetColumns();
    for (const auto& column : columns) {
        for (auto& statistic : column.GetStatistics()) {
            if (statistic.GetType() == NKikimr::NStat::COUNT_MIN_SKETCH) {
                auto data = statistic.GetData().data();
                auto sketch = reinterpret_cast<const TCountMinSketch*>(data);
                UNIT_ASSERT(sketch->GetElementCount() == ColumnTableRowsNumber);
            }
        }
    }
}

Y_UNIT_TEST_SUITE(LoadStatistics) {
    Y_UNIT_TEST(Simple) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");

        auto sender = runtime.AllocateEdgeActor(0);
        runtime.Register(CreateStatisticsTableCreator(
            std::make_unique<TEvStatistics::TEvStatTableCreationResponse>(), "/Root/Database"),
            0, 0, TMailboxType::Simple, 0, sender);
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvStatTableCreationResponse>(sender);

        TPathId pathId(1, 1);
        ui64 statType = 1;
        std::vector<ui32> columnTags = {1, 2};
        std::vector<TString> data = {"dataA", "dataB"};

        runtime.Register(CreateSaveStatisticsQuery(sender, "/Root/Database",
            pathId, statType, std::move(columnTags), std::move(data)),
            0, 0, TMailboxType::Simple, 0, sender);
        auto saveResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvSaveStatisticsQueryResponse>(sender);
        UNIT_ASSERT(saveResponse->Get()->Success);

        runtime.Register(CreateLoadStatisticsActor(
            1, sender, "/Root/Database", pathId, statType, 1, 0
        ), 0, 0, TMailboxType::Simple, 0, sender);

        auto res = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        auto msg = res->Get();
        UNIT_ASSERT(msg->Success);
        UNIT_ASSERT(msg->Data);
        UNIT_ASSERT_VALUES_EQUAL(*msg->Data, "dataA");

        runtime.Register(CreateLoadStatisticsActor(
            1, sender, "/Root/Database", pathId, statType, 2, 0
        ), 0, 0, TMailboxType::Simple, 0, sender);

        res = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvLoadStatisticsQueryResponse>(sender);
        msg = res->Get();
        UNIT_ASSERT(msg->Success);
        UNIT_ASSERT(msg->Data);
        UNIT_ASSERT_VALUES_EQUAL(*msg->Data, "dataB");
    }

    Y_UNIT_TEST(Serverless) {
        TTestEnv env(1, 3);
        auto& runtime = *env.GetServer().GetRuntime();

        runtime.GetAppData(0).TenantName = "/Root/Shared";

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless1", "/Root/Shared", 1);
        CreateServerlessDatabase(env, "Serverless2", "/Root/Shared", 1);

        ui32 saveStatisticsQueryResponseCount = 0;
        std::vector<TTestActorRuntimeBase::TEventObserverHolder> observers;
        observers.emplace_back(runtime.AddObserver<TEvStatistics::TEvSaveStatisticsQueryResponse>([&](TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr&) {
            ++saveStatisticsQueryResponseCount;
        }));

        CreateColumnStoreTable(env, "Serverless1", "Table1", 1);
        CreateColumnStoreTable(env, "Serverless2", "Table2", 1);
        
        runtime.DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]() {
                return saveStatisticsQueryResponseCount >= 2;
            }
        });

        auto sender = runtime.AllocateEdgeActor(0);

        ui64 round = 1;
        AggregateStatistics(runtime, round++, 4, "/Root/Serverless1/Table1", sender);
        AggregateStatistics(runtime, round++, 4, "/Root/Serverless2/Table2", sender);
    }
}

} // NKikimr::NStat
