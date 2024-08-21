#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/base/tablet_resolver.h>

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

std::unordered_map<ui32, TActorId> InitializeRuntime(TTestActorRuntime& runtime, ui32 nodesCount,
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
    auto resolverActor = MakeTabletResolverID();
    auto resolverConfig = MakeIntrusive<TTabletResolverConfig>();
    std::unordered_map<ui32, TActorId> indexToActor;

    for (ui32 i = 0; i < nodesCount; ++i) {
        ui32 nodeId = runtime.GetNodeId(i);
        auto actorId = NStat::MakeStatServiceID(nodeId);
        indexToActor.emplace(i, actorId);

        runtime.AddLocalService(actorId, TActorSetupCmd(NStat::CreateStatService(settings).Release(), TMailboxType::HTSwap, 0), i);
        runtime.AddLocalService(resolverActor, TActorSetupCmd(CreateTabletResolver(resolverConfig), TMailboxType::Simple, 0), i);
        runtime.AddLocalService(nameserviceActor, TActorSetupCmd(CreateNameserverTable(nameserverTable), TMailboxType::Simple, 0), i);
    }

    TTestActorRuntime::TEgg egg{ new TAppData(0, 0, 0, 0, { }, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {} };
    runtime.Initialize(egg);

    return indexToActor;
}

std::unordered_map<ui32, ui32> GetNodeIdToIndexMap(const std::unordered_map<ui32, TActorId>& map) {
    std::unordered_map<ui32, ui32> res;
    for (auto it = map.begin(); it != map.end(); ++it) {
        res.emplace(it->second.NodeId(), it->first);
    }
    return res;
}

Y_UNIT_TEST_SUITE(StatisticsService) {
    Y_UNIT_TEST(ShouldBeCorrectlyAggregateStatisticsFromAllNodes) {
        size_t nodeCount = 4;
        auto runtime = TTestActorRuntime(nodeCount, 1, false);
        auto indexToActorMap = InitializeRuntime(runtime, nodeCount);
        auto nodeIdToIndexMap = GetNodeIdToIndexMap(indexToActorMap);
        std::vector<ui64> localTabletsIds = {1, 2, 3};
        std::vector<TAggregateStatisticsRequest::TTablets> nodesTablets = {{.NodeId = indexToActorMap[0].NodeId(), .Ids{localTabletsIds}},
            {.NodeId = indexToActorMap[1].NodeId(), .Ids{4}}, {.NodeId = indexToActorMap[2].NodeId(), .Ids{5}},
            {.NodeId = indexToActorMap[3].NodeId(), .Ids{6}}};
        std::unordered_map<TActorId, ui64> pipeToTablet;

        std::vector<TTestActorRuntimeBase::TEventObserverHolder> observers;
        observers.emplace_back(runtime.AddObserver<TEvTabletResolver::TEvForward>([&](TEvTabletResolver::TEvForward::TPtr& ev) {
            auto tabletId = ev->Get()->TabletID;
            auto recipient = indexToActorMap[nodeIdToIndexMap[ev->Sender.NodeId()]];
            pipeToTablet[ev->Sender] = tabletId;

            runtime.Send(new IEventHandle(recipient, ev->Sender,
                new TEvTabletPipe::TEvClientConnected(tabletId, NKikimrProto::OK, ev->Sender, ev->Sender,
                true, false, 0), 0, ev->Cookie), nodeIdToIndexMap[ev->Sender.NodeId()], true);
            ev.Reset();
        }));
        observers.emplace_back(runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvTabletPipe::EvSend:
                    auto msg = ev->Get<TEvStatistics::TEvStatisticsRequest>();
                    if (msg != nullptr) {
                        auto tabletId = pipeToTablet[ev->Recipient];
                        auto senderNodeIndex = nodeIdToIndexMap[ev->Sender.NodeId()];

                        if (tabletId <= localTabletsIds.back()) {
                            runtime.Send(new IEventHandle(ev->Sender, ev->Sender,
                                    CreateStatisticsResponse(TStatisticsResponse{
                                        .TabletId = tabletId,
                                        .Columns{
                                            TColumnItem{.Tag = 1, .Cells{"1", "2"}},
                                            TColumnItem{.Tag = 2, .Cells{"3"}}
                                        },
                                        .Status = NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS
                                    }).release(), 0, ev->Cookie), senderNodeIndex, true);
                        } else {
                            runtime.Send(new IEventHandle(ev->Sender, ev->Sender,
                                CreateStatisticsResponse(TStatisticsResponse{
                                    .TabletId = tabletId,
                                    .Columns{
                                        TColumnItem{.Tag = 2, .Cells{"3"}}
                                    },
                                    .Status = NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS
                                }).release(), 0, ev->Cookie), senderNodeIndex, true);
                        }
                    }
                    break;
            }
        }));

        auto sender = runtime.AllocateEdgeActor();
        runtime.Send(indexToActorMap[0], sender, CreateStatisticsRequest(TAggregateStatisticsRequest{
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

    Y_UNIT_TEST(ShouldBePings) {
        size_t nodeCount = 2;
        auto runtime = TTestActorRuntime(nodeCount, 1, false);
        auto indexToActorMap = InitializeRuntime(runtime, nodeCount,
            GetDefaultSettings()
                .SetAggregateKeepAlivePeriod(TDuration::MilliSeconds(10))
                .SetAggregateKeepAliveTimeout(TDuration::Seconds(3))
                .SetAggregateKeepAliveAckTimeout(TDuration::Seconds(3)));
        auto nodeIdToIndexMap = GetNodeIdToIndexMap(indexToActorMap);
        std::vector<TAggregateStatisticsRequest::TTablets> nodesTablets = {{.NodeId = indexToActorMap[0].NodeId(), .Ids{1}},
            {.NodeId = indexToActorMap[1].NodeId(), .Ids{2}}};

        std::vector<int> ping(3);
        std::vector<int> pong(3);
        auto sender = runtime.AllocateEdgeActor();

        std::vector<TTestActorRuntimeBase::TEventObserverHolder> observers;
        observers.emplace_back(runtime.AddObserver<TEvTabletResolver::TEvForward>([&](TEvTabletResolver::TEvForward::TPtr& ev) {
            ev.Reset();
        }));
        observers.emplace_back(runtime.AddObserver<TEvStatistics::TEvAggregateKeepAlive>([&](TEvStatistics::TEvAggregateKeepAlive::TPtr& ev) {
            if (ev->Recipient == sender) {
                ++ping[0];
                ev.Reset();
                return;
            }

            auto it = nodeIdToIndexMap.find(ev->Recipient.NodeId());
            if (it != nodeIdToIndexMap.end()) {
                ++ping[it->second + 1];
            }
        }));
        observers.emplace_back(runtime.AddObserver<TEvStatistics::TEvAggregateKeepAliveAck>([&](TEvStatistics::TEvAggregateKeepAliveAck::TPtr& ev) {
            auto it = nodeIdToIndexMap.find(ev->Recipient.NodeId());
            if (it != nodeIdToIndexMap.end()) {
                ++pong[it->second + 1];
            }
        }));
        runtime.Send(indexToActorMap[0], sender, CreateStatisticsRequest(TAggregateStatisticsRequest{
            .Round = 1,
            .PathId{3, 3},
            .Nodes{ nodesTablets },
            .ColumnTags{1, 2}
        }).release());

        runtime.DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]() {
                return ping[0] >= 10 && ping[1] >= 10 && pong[2] >= 10;
            }
        });

        for (const auto& node : nodesTablets) {
            for (auto tabletId : node.Ids) {
                auto actorId = NStat::MakeStatServiceID(node.NodeId);
                runtime.Send(new IEventHandle(actorId, actorId,
                    CreateStatisticsResponse(TStatisticsResponse{
                        .TabletId = tabletId,
                        .Status = NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS
                    }).release(), 0, 1), nodeIdToIndexMap[node.NodeId], true);
            }
        }

        auto res = runtime.GrabEdgeEvent<TEvStatistics::TEvAggregateStatisticsResponse>(sender);
        const auto& record = res->Get()->Record;
        UNIT_ASSERT(record.GetFailedTablets().empty());
    }

    Y_UNIT_TEST(RootNodeShouldBeInvalidateByTimeout) {
        size_t nodeCount = 4;
        auto runtime = TTestActorRuntime(nodeCount, 1, false);
        auto indexToActorMap = InitializeRuntime(runtime, nodeCount,
            GetDefaultSettings()
                .SetAggregateKeepAlivePeriod(TDuration::MilliSeconds(5))
                .SetAggregateKeepAliveTimeout(TDuration::MilliSeconds(10))
                .SetAggregateKeepAliveAckTimeout(TDuration::MilliSeconds(10)));
        auto nodeIdToIndexMap = GetNodeIdToIndexMap(indexToActorMap);
        std::vector<TAggregateStatisticsRequest::TTablets> nodesTablets = {{.NodeId = indexToActorMap[0].NodeId(), .Ids{1}},
            {.NodeId = indexToActorMap[1].NodeId(), .Ids{2}}, {.NodeId = indexToActorMap[2].NodeId(), .Ids{3}},
            {.NodeId = indexToActorMap[3].NodeId(), .Ids{4}}};
        std::unordered_map<TActorId, ui64> pipeToTablet;

        std::vector<TTestActorRuntimeBase::TEventObserverHolder> observers;
        observers.emplace_back(runtime.AddObserver<TEvTabletResolver::TEvForward>([&](TEvTabletResolver::TEvForward::TPtr& ev) {
            auto tabletId = ev->Get()->TabletID;
            pipeToTablet[ev->Sender] = tabletId;

            if (tabletId == 2) {
                ev.Reset();
                return;
            }

            auto recipient = indexToActorMap[nodeIdToIndexMap[ev->Sender.NodeId()]];
            runtime.Send(new IEventHandle(recipient, ev->Sender,
                new TEvTabletPipe::TEvClientConnected(tabletId, NKikimrProto::OK, ev->Sender, ev->Sender,
                true, false, 0), 0, ev->Cookie), nodeIdToIndexMap[ev->Sender.NodeId()], true);
            ev.Reset();
        }));
        observers.emplace_back(runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvTabletPipe::EvSend:
                    auto msg = ev->Get<TEvStatistics::TEvStatisticsRequest>();
                    if (msg != nullptr) {
                        runtime.Send(new IEventHandle(ev->Sender, ev->Sender,
                            CreateStatisticsResponse(TStatisticsResponse{
                                .TabletId = pipeToTablet[ev->Recipient],
                                .Status = NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS
                        }).release(), 0, ev->Cookie), nodeIdToIndexMap[ev->Sender.NodeId()], true);
                    }
                    break;
            }
        }));
        observers.emplace_back(runtime.AddObserver<TEvStatistics::TEvAggregateKeepAliveAck>([&](TEvStatistics::TEvAggregateKeepAliveAck::TPtr& ev) {
            if (ev->Sender == indexToActorMap[1]) {
                ev.Reset();
                return;
            }
        }));

        auto sender = runtime.AllocateEdgeActor();
        runtime.Send(indexToActorMap[0], sender, CreateStatisticsRequest(TAggregateStatisticsRequest{
            .Round = 1,
            .PathId{3, 3},
            .Nodes{ nodesTablets },
            .ColumnTags{1, 2}
        }).release());

        auto res = runtime.GrabEdgeEvent<TEvStatistics::TEvAggregateStatisticsResponse>(sender);
        const auto& record = res->Get()->Record;
        size_t expectedFailedTabletsCount = 2;
        UNIT_ASSERT_VALUES_EQUAL(expectedFailedTabletsCount, record.GetFailedTablets().size());

        ui32 expectedError = NKikimrStat::TEvAggregateStatisticsResponse::TYPE_UNAVAILABLE_NODE;
        for (const auto& fail : record.GetFailedTablets()) {
            ui32 actualError = fail.GetError();
            UNIT_ASSERT_VALUES_EQUAL(expectedError, actualError);
        }
    }

    Y_UNIT_TEST(ChildNodesShouldBeInvalidateByTimeout) {
        size_t nodeCount = 4;
        auto runtime = TTestActorRuntime(nodeCount, 1, false);
        auto indexToActorMap = InitializeRuntime(runtime, nodeCount,
            GetDefaultSettings()
                .SetAggregateKeepAlivePeriod(TDuration::MilliSeconds(5))
                .SetAggregateKeepAliveTimeout(TDuration::MilliSeconds(10)));
        auto nodeIdToIndexMap = GetNodeIdToIndexMap(indexToActorMap);
        std::vector<TAggregateStatisticsRequest::TTablets> nodesTablets = {{.NodeId = indexToActorMap[0].NodeId(), .Ids{1}},
            {.NodeId = indexToActorMap[1].NodeId(), .Ids{2}}, {.NodeId = indexToActorMap[2].NodeId(), .Ids{3}},
            {.NodeId = indexToActorMap[3].NodeId(), .Ids{4}}};
        std::unordered_map<TActorId, ui64> pipeToTablet;

        std::vector<TTestActorRuntimeBase::TEventObserverHolder> observers;
        observers.emplace_back(runtime.AddObserver<TEvTabletResolver::TEvForward>([&](TEvTabletResolver::TEvForward::TPtr& ev) {
            auto tabletId = ev->Get()->TabletID;
            pipeToTablet[ev->Sender] = tabletId;

            if (tabletId == 2) {
                ev.Reset();
                return;
            }

            auto recipient = indexToActorMap[nodeIdToIndexMap[ev->Sender.NodeId()]];
            runtime.Send(new IEventHandle(recipient, ev->Sender,
                new TEvTabletPipe::TEvClientConnected(tabletId, NKikimrProto::OK, ev->Sender, ev->Sender,
                true, false, 0), 0, ev->Cookie), nodeIdToIndexMap[ev->Sender.NodeId()], true);
            ev.Reset();
        }));
        observers.emplace_back(runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvTabletPipe::EvSend:
                    auto msg = ev->Get<TEvStatistics::TEvStatisticsRequest>();
                    if (msg != nullptr) {
                        runtime.Send(new IEventHandle(ev->Sender, ev->Sender,
                            CreateStatisticsResponse(TStatisticsResponse{
                                .TabletId = pipeToTablet[ev->Recipient],
                                .Status = NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS
                        }).release(), 0, ev->Cookie), nodeIdToIndexMap[ev->Sender.NodeId()], true);
                    }
                    break;
            }
        }));
        observers.emplace_back(runtime.AddObserver<TEvStatistics::TEvAggregateKeepAlive>([&](TEvStatistics::TEvAggregateKeepAlive::TPtr& ev) {
            if (ev->Sender == indexToActorMap[1]) {
                ev.Reset();
                return;
            }
        }));

        auto sender = runtime.AllocateEdgeActor();
        runtime.Send(indexToActorMap[0], sender, CreateStatisticsRequest(TAggregateStatisticsRequest{
            .Round = 1,
            .PathId{3, 3},
            .Nodes{ nodesTablets },
            .ColumnTags{1, 2}
        }).release());

        auto res = runtime.GrabEdgeEvent<TEvStatistics::TEvAggregateStatisticsResponse>(sender);
        const auto& record = res->Get()->Record;
        size_t expectedFailedTabletsCount = 2;
        UNIT_ASSERT_VALUES_EQUAL(expectedFailedTabletsCount, record.GetFailedTablets().size());

        ui32 expectedError = NKikimrStat::TEvAggregateStatisticsResponse::TYPE_UNAVAILABLE_NODE;
        for (const auto& fail : record.GetFailedTablets()) {
            ui32 actualError = fail.GetError();
            UNIT_ASSERT_VALUES_EQUAL(expectedError, actualError);
        }
    }

    Y_UNIT_TEST(ShouldBeCcorrectProcessingOfLocalTablets) {
        size_t nodeCount = 1;
        auto runtime = TTestActorRuntime(nodeCount, 1, false);
        auto settings = GetDefaultSettings()
            .SetMaxInFlightTabletRequests(3);
        auto indexToActorMap = InitializeRuntime(runtime, nodeCount, settings);
        auto nodeIdToIndexMap = GetNodeIdToIndexMap(indexToActorMap);
        std::vector<ui64> localTabletsIds = {1, 2, 3, 4, 5, 6, 7, 8};
        std::vector<TAggregateStatisticsRequest::TTablets> nodesTablets = {{.NodeId = indexToActorMap[0].NodeId(), .Ids{localTabletsIds}}};

        std::vector<TTestActorRuntimeBase::TEventObserverHolder> observers;
        observers.emplace_back(runtime.AddObserver<TEvTabletResolver::TEvForward>([&](TEvTabletResolver::TEvForward::TPtr& ev) {
            auto tabletId = ev->Get()->TabletID;
            auto senderNodeIndex = nodeIdToIndexMap[ev->Sender.NodeId()];
            auto recipient = indexToActorMap[senderNodeIndex];

            if (tabletId % 3 == 1) {
                runtime.Send(new IEventHandle(recipient, ev->Sender,
                    new TEvTabletPipe::TEvClientConnected(tabletId, NKikimrProto::ERROR, ev->Sender, ev->Sender,
                    true, false, 0), 0, ev->Cookie), senderNodeIndex, true);
            } else if(tabletId % 3 == 2) {
                runtime.Send(new IEventHandle(recipient, ev->Sender,
                    new TEvTabletPipe::TEvClientDestroyed(tabletId, ev->Sender, ev->Sender), 0, ev->Cookie), senderNodeIndex, true);
            } else {
                auto actor = indexToActorMap[senderNodeIndex];
                runtime.Send(new IEventHandle(actor, actor,
                    CreateStatisticsResponse(TStatisticsResponse{
                        .TabletId = tabletId,
                        .Status = NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS
                    }).release(), 0, 1), senderNodeIndex, true);
            }
            ev.Reset();
        }));

        auto sender = runtime.AllocateEdgeActor();
        runtime.Send(indexToActorMap[0], sender, CreateStatisticsRequest(TAggregateStatisticsRequest{
            .Round = 1,
            .PathId{3, 3},
            .Nodes{ nodesTablets },
            .ColumnTags{1}
        }).release());

        auto res = runtime.GrabEdgeEvent<TEvStatistics::TEvAggregateStatisticsResponse>(sender);
        const auto& record = res->Get()->Record;
        size_t expectedFailedTabletsCount = 6;
        UNIT_ASSERT_VALUES_EQUAL(expectedFailedTabletsCount, record.GetFailedTablets().size());

        ui32 expectedError = NKikimrStat::TEvAggregateStatisticsResponse::TYPE_NON_LOCAL_TABLET;
        for (const auto& fail : record.GetFailedTablets()) {
            ui32 actualError = fail.GetError();
            UNIT_ASSERT_VALUES_EQUAL(expectedError, actualError);
        }
    }
}

} // NSysView
} // NKikimr
