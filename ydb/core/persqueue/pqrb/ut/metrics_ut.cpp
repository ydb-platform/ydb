#include <ydb/core/persqueue/pqrb/read_balancer.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/pqrb/read_balancer__metrics.h>
#include <ydb/core/persqueue/pqrb/read_balancer__sqs_metrics.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>

#include <functional>
#include <optional>

namespace NKikimr::NPQ {

namespace {

class TRunMetricsTest : public TActorBootstrapped<TRunMetricsTest> {
public:
    TRunMetricsTest(TActorId edge, std::function<void(const TActorContext&)> test)
        : Edge(edge)
        , Test(std::move(test))
    {}

    void Bootstrap() {
        Test(ActorContext());
        Send(Edge, new TEvents::TEvWakeup());
        PassAway();
    }

private:
    TActorId Edge;
    std::function<void(const TActorContext&)> Test;
};

struct TExportedMetrics : NMonitoring::ICountableConsumer {
    THashMap<TString, ui64> Counters;
    size_t Histograms = 0;

    void OnCounter(const TString&, const TString& name, const NMonitoring::TCounterForPtr* counter) override {
        Counters[name] = counter->Val();
    }

    void OnHistogram(const TString&, const TString&, NMonitoring::IHistogramSnapshotPtr, bool) override {
        ++Histograms;
    }

    void OnGroupBegin(const TString&, const TString&, const NMonitoring::TDynamicCounters*) override {}
    void OnGroupEnd(const TString&, const TString&, const NMonitoring::TDynamicCounters*) override {}
};

} // namespace

Y_UNIT_TEST_SUITE(TPqrbMetrics) {

Y_UNIT_TEST_TWIN(DisableMonitoringMetrics, initiallyDisabled) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(TAppPrepare().Unwrap());
    runtime.GetAppData().Counters = new NMonitoring::TDynamicCounters();
    const auto edge = runtime.AllocateEdgeActor();
    runtime.Register(new TRunMetricsTest(edge, [&](const TActorContext& ctx) {
        auto root = AppData(ctx)->Counters;
        auto exportMetrics = [&] {
            TExportedMetrics exported;
            // GetServiceCounters also registers utils/CounterLookups, which is not a topic metric.
            for (const auto* service : {"topics", "topics_serverless", "sqs", "ymq_public"}) {
                if (auto group = root->FindSubgroup("counters", service)) {
                    group->Accept("counters", service, exported);
                }
            }
            return exported;
        };
        TDatabaseInfo database;
        database.DatabasePath = "/Root";
        const TString topicPath = "/Root/topic";
        NKikimrPQ::TPQTabletConfig config;
        UNIT_ASSERT(!config.HasMetricsLevel());
        if (initiallyDisabled) {
            config.SetMetricsLevel(METRICS_LEVEL_DISABLED);
        }
        config.SetEnableCompactification(true);
        config.SetSqsExportMetrics(true);
        config.SetSqsAccountName("account");
        config.SetSqsQueueName("queue");
        config.AddAllPartitions()->SetPartitionId(0);
        auto* consumer = config.AddConsumers();
        consumer->SetName("ydb-sqs-consumer");
        consumer->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);

        TTopicMetricsHandler handler;
        handler.Initialize(config, database, topicPath, ctx);
        const auto initial = exportMetrics();
        UNIT_ASSERT_VALUES_EQUAL(initial.Counters.empty(), initiallyDisabled);
        UNIT_ASSERT_VALUES_EQUAL(initial.Histograms == 0, initiallyDisabled);

        ui64 round = 0;
        const std::optional<ui32> levels[] = {
            initiallyDisabled ? std::optional<ui32>(METRICS_LEVEL_DISABLED) : std::nullopt,
            METRICS_LEVEL_OBJECT, METRICS_LEVEL_DISABLED, METRICS_LEVEL_DISABLED,
            std::nullopt, METRICS_LEVEL_DISABLED, METRICS_LEVEL_DETAILED, METRICS_LEVEL_DATABASE
        };
        for (const auto level : levels) {
            ++round;
            const bool disabled = level.has_value() && *level == METRICS_LEVEL_DISABLED;
            if (level) {
                config.SetMetricsLevel(*level);
            } else {
                config.ClearMetricsLevel();
            }
            handler.UpdateConfig(config, database, topicPath, ctx);

            NKikimrPQ::TStatusResponse_TPartResult status;
            status.SetPartition(0);
            status.SetPartitionSize(round * 100);
            status.SetUsedReserveSize(round * 10);
            status.SetAvgWriteSpeedPerSec(round);
            auto* counters = status.MutableAggregatedCounters();
            for (int i = 0; i <= METRIC_TOTAL_PART_SIZE; ++i) {
                counters->AddValues(i == METRIC_TOTAL_PART_SIZE ? round * 100 : 0);
            }
            auto* consumerCounters = counters->AddConsumerAggregatedCounters();
            consumerCounters->SetConsumer("ydb-sqs-consumer");
            for (int i = 0; i <= METRIC_TOTAL_COMMIT_MESSAGE_LAG; ++i) {
                consumerCounters->AddValues(i == METRIC_TOTAL_COMMIT_MESSAGE_LAG ? round * 2 : 0);
            }
            handler.Handle(std::move(status));
            handler.UpdateMetrics();

            NKikimrPQ::TEvTopicSqsActionMetrics action;
            action.MutableSendMessage()->SetSendMessageCount(1);
            handler.AddSqsActionMetrics(action);

            // Statistics used by SchemeShard must advance even when nothing is exported.
            UNIT_ASSERT_VALUES_EQUAL(handler.GetTopicMetrics().TotalDataSize, round * 100);
            UNIT_ASSERT_VALUES_EQUAL(handler.GetTopicMetrics().TotalUsedReserveSize, round * 10);
            UNIT_ASSERT_VALUES_EQUAL(handler.GetTopicMetrics().TotalAvgWriteSpeedPerSec, round);
            UNIT_ASSERT_VALUES_EQUAL(handler.GetPartitionMetrics().at(0).DataSize, round * 100);
            UNIT_ASSERT_VALUES_EQUAL(handler.GetPartitionMetrics().at(0).UsedReserveSize, round * 10);

            // Accept uses the same traversal (including expiration) as monitoring encoders.
            const auto exported = exportMetrics();
            if (disabled) {
                UNIT_ASSERT_C(exported.Counters.empty(), "Disabled metrics are still exported");
                UNIT_ASSERT_VALUES_EQUAL(exported.Histograms, 0);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(exported.Counters.at("topic.storage_bytes"), round * 100);
                UNIT_ASSERT_VALUES_EQUAL(exported.Counters.at("topic.committed_lag_messages"), round * 2);
                UNIT_ASSERT_VALUES_EQUAL(exported.Counters.at("topic.partition.active_count"), 1);
                UNIT_ASSERT_VALUES_EQUAL(exported.Counters.at("MessagesCount"), round * 2);
                UNIT_ASSERT(exported.Counters.at("SendMessage_Count") > 0);
                UNIT_ASSERT(exported.Histograms > 0);
            }
        }
    }));
    auto done = runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(done);
}

Y_UNIT_TEST(SqsMetricsApplicableRequiresExportFlagAndQueueName) {
    NKikimrPQ::TPQTabletConfig config;
    UNIT_ASSERT(!TTopicSqsMetricsHandler::IsApplicable(config));

    config.SetSqsExportMetrics(true);
    UNIT_ASSERT(!TTopicSqsMetricsHandler::IsApplicable(config));

    config.SetSqsQueueName("q");
    UNIT_ASSERT(TTopicSqsMetricsHandler::IsApplicable(config));

    config.SetSqsExportMetrics(false);
    UNIT_ASSERT(!TTopicSqsMetricsHandler::IsApplicable(config));
}

Y_UNIT_TEST(InitializePartitionsAggregatesTopicMetrics) {
    TTopicMetricsHandler handler;
    handler.InitializePartitions(0, 10, 1);
    handler.InitializePartitions(1, 25, 4);

    UNIT_ASSERT_VALUES_EQUAL(handler.GetTopicMetrics().TotalDataSize, 35u);
    UNIT_ASSERT_VALUES_EQUAL(handler.GetTopicMetrics().TotalUsedReserveSize, 5u);
    UNIT_ASSERT_VALUES_EQUAL(handler.GetPartitionMetrics().size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(handler.GetPartitionMetrics().at(1).DataSize, 25u);
}

} // Y_UNIT_TEST_SUITE(TPqrbMetrics)

} // namespace NKikimr::NPQ
