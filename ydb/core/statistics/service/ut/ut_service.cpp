#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/base/tablet_pipecache.h>

#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/protos/statistics.pb.h>

#include <type_traits>

namespace NKikimr {
namespace NStat {

using EResponseStatus = NKikimrStat::TEvStatisticsResponse::EStatus;
using EErrorType = NKikimrStat::TEvAggregateStatisticsResponse::EErrorType;

struct TAggregateStatisticsRequest {
    struct TTablets {
        ui32 NodeId;
        std::vector<ui64> Ids;
    };
    ui64 Round;
    TPathId PathId;
    std::vector<TTablets> Nodes;
    std::vector<ui32> ColumnTags;
};

struct TColumnItem {
    ui32 Tag;
    std::vector<std::string> Cells;
};

struct TStatisticsResponse {
    ui64 TabletId;
    std::vector<TColumnItem> Columns;
    EResponseStatus Status;
};

struct TAggregateStatisticsResponse {
    struct TFailedTablet {
        ui64 TabletId;
        ui32 NodeId;
        EErrorType Error;
    };

    ui64 Round;
    std::vector<TColumnItem> Columns;
    std::vector<TFailedTablet> FailedTablets;
};

std::unique_ptr<TEvStatistics::TEvAggregateStatistics> CreateStatisticsRequest(const TAggregateStatisticsRequest& data) {
    auto ev = std::make_unique<TEvStatistics::TEvAggregateStatistics>();
    auto& record = ev->Record;
    record.SetRound(data.Round);

    PathIdFromPathId(data.PathId, record.MutablePathId());

    auto columnTags = record.MutableColumnTags();
    for (auto tag : data.ColumnTags) {
        columnTags->Add(tag);
    }
    
    for (const auto& tablets : data.Nodes) {
        auto node = record.AddNodes();
        node->SetNodeId(tablets.NodeId);

        auto tabletIds = node->MutableTabletIds();
        for (auto tabletId : tablets.Ids) {
            tabletIds->Add(tabletId);
        }
    }

    return std::move(ev);
}

std::unique_ptr<TEvStatistics::TEvAggregateStatisticsResponse> CreateAggregateStatisticsResponse(const TAggregateStatisticsResponse& data) {
    auto ev = std::make_unique<TEvStatistics::TEvAggregateStatisticsResponse>();
    auto& record = ev->Record;
    record.SetRound(data.Round);

    for (const auto& fail : data.FailedTablets) {
        auto failedTablets = record.AddFailedTablets();
        failedTablets->SetTabletId(fail.TabletId);
        failedTablets->SetNodeId(fail.NodeId);
        failedTablets->SetError(fail.Error);
    }

    for (const auto& col : data.Columns) {
        auto column = record.AddColumns();
        column->SetTag(col.Tag);

        auto statistics = column->AddStatistics();
        statistics->SetType(NKikimr::NStat::COUNT_MIN_SKETCH);
        auto sketch = std::unique_ptr<TCountMinSketch>(TCountMinSketch::Create());

        for (const auto& cell : col.Cells) {
            sketch->Count(cell.data(), cell.size());
        }

        auto buf = sketch->AsStringBuf();
        statistics->SetData(buf.Data(), buf.Size());
    }

    return std::move(ev);
}

std::unique_ptr<TEvStatistics::TEvStatisticsResponse> CreateStatisticsResponse(const TStatisticsResponse& data) {
    auto ev = std::make_unique<TEvStatistics::TEvStatisticsResponse>();
    auto& record = ev->Record;
    record.SetShardTabletId(data.TabletId);
    record.SetStatus(data.Status);

    for (const auto& col : data.Columns) {
        auto column = record.AddColumns();
        column->SetTag(col.Tag);

        auto statistics = column->AddStatistics();
        statistics->SetType(NKikimr::NStat::COUNT_MIN_SKETCH);
        auto sketch = std::unique_ptr<TCountMinSketch>(TCountMinSketch::Create());

        for (const auto& cell : col.Cells) {
            sketch->Count(cell.data(), cell.size());
        }

        auto buf = sketch->AsStringBuf();
        statistics->SetData(buf.Data(), buf.Size());
    }

    return std::move(ev);
}

TStatServiceSettings GetDefaultSettings() {
    auto settings = TStatServiceSettings();
    settings.AggregateKeepAlivePeriod = TDuration::Seconds(15);
    settings.AggregateKeepAliveTimeout = TDuration::Seconds(30);
    settings.AggregateKeepAliveAckTimeout = TDuration::Seconds(30);
    settings.FanOutFactor = 2;
    return settings;
}

std::unordered_map<ui32, ui32> InitializeRuntime(TTestActorRuntime& runtime, ui32 nodesCount,
            const TStatServiceSettings& settings = GetDefaultSettings()) {
    runtime.SetLogPriority(NKikimrServices::STATISTICS, NLog::EPriority::PRI_DEBUG);
    runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&, TDuration, TInstant&){
        return false;
    });
        
    TIntrusivePtr<TTableNameserverSetup> nameserverTable(new TTableNameserverSetup());
    TPortManager pm;

    for (ui32 i = 1; i <= nodesCount; ++i) {
        nameserverTable->StaticNodeTable[i] = std::pair<TString, ui32>("127.0.0." + std::to_string(i), pm.GetPort(12000 + i));
    }

    auto nameserviceActor = GetNameserviceActorId();
    auto pipeActor = MakePipePerNodeCacheID(false);
    auto pipeConfig = MakeIntrusive<TPipePerNodeCacheConfig>();
    
    std::unordered_map<ui32, ui32> indexToNodeId;

    for (ui32 i = 0; i < nodesCount; ++i) {
        ui32 nodeId = runtime.GetNodeId(i);
        indexToNodeId.emplace(i, nodeId);

        auto actorId = NStat::MakeStatServiceID(nodeId);
        runtime.AddLocalService(actorId, TActorSetupCmd(NStat::CreateStatService(settings).Release(), TMailboxType::HTSwap, 0), i);
        runtime.AddLocalService(pipeActor, TActorSetupCmd(CreatePipePerNodeCache(pipeConfig), TMailboxType::HTSwap, 0), i);
        runtime.AddLocalService(nameserviceActor, TActorSetupCmd(CreateNameserverTable(nameserverTable), TMailboxType::Simple, 0), i);
    }

    TTestActorRuntime::TEgg egg{ new TAppData(0, 0, 0, 0, { }, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {} };
    runtime.Initialize(egg);

    return indexToNodeId;
}

std::unordered_map<ui32, ui32> ReverseMap(const std::unordered_map<ui32, ui32>& map) {
    std::unordered_map<ui32, ui32> res;
    for (auto it = map.begin(); it != map.end(); ++it) {
        res.emplace(it->second, it->first);
    }
    return res;
}


Y_UNIT_TEST_SUITE(StatisticsService) {
    Y_UNIT_TEST(ShouldBeVisitEveryNodeAndTablet) {
        size_t nodeCount = 10;
        auto runtime = TTestActorRuntime(nodeCount, 1, false);
        auto indexToNodeIdMap = InitializeRuntime(runtime, nodeCount);
        auto nodeIdToIndexMap = ReverseMap(indexToNodeIdMap);
        std::vector<TAggregateStatisticsRequest::TTablets> nodesTablets = {{.NodeId = indexToNodeIdMap[0], .Ids{0}},
            {.NodeId = indexToNodeIdMap[1], .Ids{1}}, {.NodeId = indexToNodeIdMap[2], .Ids{2}},
            {.NodeId = indexToNodeIdMap[3], .Ids{3}}, {.NodeId = indexToNodeIdMap[4], .Ids{4}},
            {.NodeId = indexToNodeIdMap[5], .Ids{5}}, {.NodeId = indexToNodeIdMap[6], .Ids{6}},
            {.NodeId = indexToNodeIdMap[7], .Ids{7}}, {.NodeId = indexToNodeIdMap[8], .Ids{8}},
            {.NodeId = indexToNodeIdMap[9], .Ids{9}}};

        std::unordered_map<ui32, int> nodes;
        std::unordered_map<ui64, int> tablets;
        std::vector<TTestActorRuntimeBase::TEventObserverHolder> observers;
        observers.emplace_back(runtime.AddObserver<TEvStatistics::TEvAggregateStatistics>([&](TEvStatistics::TEvAggregateStatistics::TPtr& ev) {
            if (nodeIdToIndexMap.find(ev->Recipient.NodeId()) != nodeIdToIndexMap.end()) {
                ++nodes[ev->Recipient.NodeId()];
            }
        }));
        observers.emplace_back(runtime.AddObserver<TEvPipeCache::TEvGetTabletNode>([&](TEvPipeCache::TEvGetTabletNode::TPtr& ev) {
            if (nodeIdToIndexMap.find(ev->Sender.NodeId()) != nodeIdToIndexMap.end()) {
                ++tablets[ev->Get()->TabletId];
                ev.Reset();
            }
        }));

        auto sender = runtime.AllocateEdgeActor();
        runtime.Send(NStat::MakeStatServiceID(indexToNodeIdMap[0]), sender, CreateStatisticsRequest(TAggregateStatisticsRequest{
            .Round = 1,
            .PathId{3, 3},
            .Nodes{ nodesTablets },
            .ColumnTags{1}
        }).release());
        runtime.DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]() {
                    return nodes.size() >= 10 && tablets.size() >= 10;
            }
        });
        
        for (auto it = nodes.begin(); it != nodes.end(); ++it) {
            UNIT_ASSERT_VALUES_EQUAL(it->second, 1);
        }
        for (auto it = tablets.begin(); it != tablets.end(); ++it) {
            UNIT_ASSERT_VALUES_EQUAL(it->second, 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(nodesTablets.size(), nodes.size());
        UNIT_ASSERT_VALUES_EQUAL(nodesTablets.size(), tablets.size());
    }

    Y_UNIT_TEST(ShouldBeAtMostMaxInFlightTabletRequests) {
        size_t nodeCount = 1;
        auto runtime = TTestActorRuntime(nodeCount, 1, false);
        auto settings = GetDefaultSettings()
            .SetMaxInFlightTabletRequests(3);
        auto indexToNodeIdMap = InitializeRuntime(runtime, nodeCount, settings);
        auto nodeIdToIndexMap = ReverseMap(indexToNodeIdMap);
        std::vector<ui64> localTabletsIds = {1, 2, 3, 4, 5, 6, 7, 8};
        std::vector<TAggregateStatisticsRequest::TTablets> nodesTablets = {{.NodeId = indexToNodeIdMap[0], .Ids{localTabletsIds}}};

        size_t inFlight = 0;
        size_t maxInFlight = 0;
        size_t tabletsCount = 0;

        std::vector<TTestActorRuntimeBase::TEventObserverHolder> observers;
        observers.emplace_back(runtime.AddObserver<TEvPipeCache::TEvGetTabletNode>([&](TEvPipeCache::TEvGetTabletNode::TPtr& ev) {
            auto it = nodeIdToIndexMap.find(ev->Sender.NodeId());
            if (it == nodeIdToIndexMap.end()) {
                return;
            }
            auto senderNodeIndex = it->second;
            ++inFlight;
            ++tabletsCount;
            maxInFlight = std::max(maxInFlight, inFlight);

            auto tabletId = ev->Get()->TabletId;

            if (tabletId % 3 == 1) { // non local
                runtime.Send(new IEventHandle(ev->Sender, ev->Sender,
                    new TEvPipeCache::TEvGetTabletNodeResult(tabletId, 10), 0, ev->Cookie), senderNodeIndex, true);
            } else if(tabletId % 3 == 2) { // delivery problem
                runtime.Send(new IEventHandle(ev->Sender, ev->Sender,
                    new TEvPipeCache::TEvDeliveryProblem(tabletId, true), 0, ev->Cookie), senderNodeIndex, true);
            } else {
                runtime.Send(new IEventHandle(ev->Sender, ev->Sender,
                    CreateStatisticsResponse(TStatisticsResponse{
                        .TabletId = tabletId,
                        .Status = NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS
                    }).release(), 0, ev->Cookie), senderNodeIndex, true);
            }
            ev.Reset();
        }));
        observers.emplace_back(runtime.AddObserver<TEvPipeCache::TEvDeliveryProblem>([&](TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
            if (nodeIdToIndexMap.find(ev->Recipient.NodeId()) != nodeIdToIndexMap.end()) {
                --inFlight;
            }
        }));
        observers.emplace_back(runtime.AddObserver<TEvPipeCache::TEvGetTabletNodeResult>([&](TEvPipeCache::TEvGetTabletNodeResult::TPtr& ev) {
            if (nodeIdToIndexMap.find(ev->Recipient.NodeId()) != nodeIdToIndexMap.end()) {
                --inFlight;
            }
        }));
        observers.emplace_back(runtime.AddObserver<TEvStatistics::TEvStatisticsResponse>([&](TEvStatistics::TEvStatisticsResponse::TPtr& ev) {
            if (nodeIdToIndexMap.find(ev->Recipient.NodeId()) != nodeIdToIndexMap.end()) {
                --inFlight;
            }
        }));

        auto sender = runtime.AllocateEdgeActor();
        runtime.Send(NStat::MakeStatServiceID(indexToNodeIdMap[0]), sender, CreateStatisticsRequest(TAggregateStatisticsRequest{
            .Round = 1,
            .PathId{3, 3},
            .Nodes{ nodesTablets },
            .ColumnTags{1}
        }).release());
        runtime.DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]() {
                    return tabletsCount >= localTabletsIds.size();
            }
        });

        UNIT_ASSERT_LT(maxInFlight, settings.MaxInFlightTabletRequests + 1);
    }

    Y_UNIT_TEST(ShouldBeCorrectlyAggregateStatisticsFromAllNodes) {
        size_t nodeCount = 4;
        auto runtime = TTestActorRuntime(nodeCount, 1, false);
        auto indexToNodeIdMap = InitializeRuntime(runtime, nodeCount);
        auto nodeIdToIndexMap = ReverseMap(indexToNodeIdMap);
        std::vector<ui64> localTabletsIds = {1, 2, 3};
        std::vector<TAggregateStatisticsRequest::TTablets> nodesTablets = {{.NodeId = indexToNodeIdMap[0], .Ids{localTabletsIds}},
            {.NodeId = indexToNodeIdMap[1], .Ids{4}}, {.NodeId = indexToNodeIdMap[2], .Ids{5}},
            {.NodeId = indexToNodeIdMap[3], .Ids{6}}};

        std::vector<TTestActorRuntimeBase::TEventObserverHolder> observers;
        observers.emplace_back(runtime.AddObserver<TEvPipeCache::TEvGetTabletNode>([&](TEvPipeCache::TEvGetTabletNode::TPtr& ev) {
            auto tabletId = ev->Get()->TabletId;
            auto senderNodeId = ev->Sender.NodeId();
            auto senderNodeIndex = nodeIdToIndexMap.find(senderNodeId);

            if (senderNodeIndex == nodeIdToIndexMap.end()) {
                return;
            }

            if (tabletId <= localTabletsIds.back()) {
                runtime.Send(new IEventHandle(ev->Sender, ev->Sender,
                        CreateStatisticsResponse(TStatisticsResponse{
                            .TabletId = ev->Get()->TabletId,
                            .Columns{
                                TColumnItem{.Tag = 1, .Cells{"1", "2"}},
                                TColumnItem{.Tag = 2, .Cells{"3"}}
                            },
                            .Status = NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS
                        }).release(), 0, ev->Cookie), senderNodeIndex->second, true);
            } else {
                runtime.Send(new IEventHandle(ev->Sender, ev->Sender,
                    CreateStatisticsResponse(TStatisticsResponse{
                        .TabletId = ev->Get()->TabletId,
                        .Columns{
                            TColumnItem{.Tag = 2, .Cells{"3"}}
                        },
                        .Status = NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS
                    }).release(), 0, ev->Cookie), senderNodeIndex->second, true);
            }

            ev.Reset();
        }));

        auto sender = runtime.AllocateEdgeActor();
        runtime.Send(NStat::MakeStatServiceID(indexToNodeIdMap[0]), sender, CreateStatisticsRequest(TAggregateStatisticsRequest{
            .Round = 1,
            .PathId{3, 3},
            .Nodes{ nodesTablets },
            .ColumnTags{1, 2}
        }).release());
        auto res = runtime.GrabEdgeEvent<TEvStatistics::TEvAggregateStatisticsResponse>(sender);
        const auto& record = res->Get()->Record;

        std::unordered_map<ui32, std::unordered_map<std::string, int>> expected = {
            {1, {{"1", localTabletsIds.size()}, {"2", localTabletsIds.size()}}},
            {2, {{"3", nodesTablets.size() - 1 + localTabletsIds.size()}}},
        };
        
        const auto& columns = record.GetColumns();
        for (const auto& column : columns) {
            const auto tag = column.GetTag();

            for (auto& statistic : column.GetStatistics()) {
                if (statistic.GetType() == NKikimr::NStat::COUNT_MIN_SKETCH) {
                    auto data = statistic.GetData().Data();
                    auto sketch = reinterpret_cast<const TCountMinSketch*>(data);

                    const auto& cells = expected[tag];
                    for (auto it = cells.begin(); it != cells.end(); ++it) {
                        UNIT_ASSERT_VALUES_EQUAL(it->second, sketch->Probe(it->first.data(), it->first.size()));
                    }
                }
            }
        }
    }
}

} // NSysView
} // NKikimr
