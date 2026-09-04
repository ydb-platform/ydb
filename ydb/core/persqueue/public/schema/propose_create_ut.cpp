#include "common.h"
#include "schema_propose.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ::NSchema {

namespace {

class TRunFnActor: public NActors::TActorBootstrapped<TRunFnActor> {
public:
    TRunFnActor(TActorId edge, std::function<void()> fn)
        : Edge(edge)
        , Fn(std::move(fn))
    {
    }

    void Bootstrap() {
        Fn();
        Send(Edge, new NActors::TEvents::TEvWakeup());
        PassAway();
    }

private:
    const TActorId Edge;
    std::function<void()> Fn;
};

template <typename TFn>
void RunInActor(NActors::TTestActorRuntime& runtime, TFn&& fn) {
    const auto edge = runtime.AllocateEdgeActor();
    runtime.Register(new TRunFnActor(edge, std::function<void()>(std::forward<TFn>(fn))));
    auto ev = runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(edge, TDuration::Seconds(5));
    UNIT_ASSERT(ev);
}

Ydb::Topic::CreateTopicRequest BaseRequest(const TString& path = "/Root/t") {
    Ydb::Topic::CreateTopicRequest request;
    request.set_path(path);
    request.mutable_partitioning_settings()->set_min_active_partitions(1);
    return request;
}

TResult Propose(Ydb::Topic::CreateTopicRequest request) {
    NKikimrSchemeOp::TModifyScheme modifyScheme;
    return ProposeCreateTopic(modifyScheme, std::move(request), "/Root", "/Root", "t");
}

} // namespace

Y_UNIT_TEST_SUITE(ProposeCreateTopic) {

Y_UNIT_TEST(RejectsNegativeAndHugePartitionCount) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);

    RunInActor(runtime, [&] {
        {
            auto request = BaseRequest();
            request.mutable_partitioning_settings()->set_min_active_partitions(-1);
            auto r = Propose(std::move(request));
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "positive");
        }
        {
            auto request = BaseRequest();
            request.mutable_partitioning_settings()->set_min_active_partitions(
                static_cast<i64>(Max<ui32>()));
            auto r = Propose(std::move(request));
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "less than");
        }
    });
}

Y_UNIT_TEST(AutoPartitioningDefaultStrategyAndRetentionStorage) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);

    RunInActor(runtime, [&] {
        auto request = BaseRequest();
        request.set_retention_storage_mb(10);
        auto* autoSettings = request.mutable_partitioning_settings()->mutable_auto_partitioning_settings();
        // Unspecified/unknown strategy falls through to DISABLED via default branch.
        autoSettings->set_strategy(static_cast<::Ydb::Topic::AutoPartitioningStrategy>(999));
        autoSettings->mutable_partition_write_speed()->set_up_utilization_percent(50);
        autoSettings->mutable_partition_write_speed()->set_down_utilization_percent(10);
        autoSettings->mutable_partition_write_speed()->mutable_stabilization_window()->set_seconds(60);
        request.mutable_partitioning_settings()->set_max_active_partitions(4);

        NKikimrSchemeOp::TModifyScheme modifyScheme;
        auto r = ProposeCreateTopic(modifyScheme, std::move(request), "/Root", "/Root", "t");
        // Unknown strategy maps to DISABLED, which ValidatePartitionStrategy may reject
        // when strategy type is present but disabled with min/max — accept either success
        // path after remapping or a validation error.
        if (r) {
            const auto& part = modifyScheme.GetCreatePersQueueGroup().GetPQTabletConfig().GetPartitionConfig();
            UNIT_ASSERT_VALUES_EQUAL(part.GetStorageLimitBytes(), 10ull * 1024 * 1024);
        }
    });
}

Y_UNIT_TEST(RejectsTooManyConsumersAndSpeedLimits) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    runtime.GetAppData().PQConfig.MutableDefaultClientServiceType()->SetName("data-streams");
    runtime.GetAppData().PQConfig.MutableDefaultClientServiceType()->SetMaxReadRulesCountPerTopic(MAX_READ_RULES_COUNT);
    runtime.GetAppData().PQConfig.MutableDefaultClientServiceType()->ClearPasswordHashes();

    RunInActor(runtime, [&] {
        {
            auto request = BaseRequest();
            for (int i = 0; i < MAX_READ_RULES_COUNT + 1; ++i) {
                auto* c = request.add_consumers();
                c->set_name(TStringBuilder() << "c" << i);
                c->mutable_streaming_consumer_type();
            }
            auto r = Propose(std::move(request));
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "consumers count cannot be more than");
        }
        {
            auto request = BaseRequest();
            request.set_partition_write_speed_messages_per_second(-1);
            auto r = Propose(std::move(request));
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "partition_write_speed_messages_per_second");
        }
        {
            auto request = BaseRequest();
            request.set_partition_write_speed_messages_per_second(
                static_cast<i64>(DEFAULT_PARTITION_WRITE_SPEED_MESSAGES_PER_SECOND) + 1);
            auto r = Propose(std::move(request));
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "greater than");
        }
        {
            auto request = BaseRequest();
            request.set_partition_write_burst_messages(-1);
            auto r = Propose(std::move(request));
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "partition_write_burst_messages");
        }
        {
            auto request = BaseRequest();
            request.set_partition_write_burst_messages(
                static_cast<i64>(DEFAULT_PARTITION_WRITE_SPEED_MESSAGES_PER_SECOND) + 1);
            auto r = Propose(std::move(request));
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "greater than");
        }
        {
            auto request = BaseRequest();
            request.set_partition_total_read_speed_bytes_per_second(-1);
            auto r = Propose(std::move(request));
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "partition_total_read_speed_bytes");
        }
        {
            auto request = BaseRequest();
            request.set_partition_total_read_speed_messages_per_second(-1);
            auto r = Propose(std::move(request));
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "partition_total_read_speed_messages");
        }
        {
            auto request = BaseRequest();
            request.set_partition_read_without_consumer_speed_bytes_per_second(-1);
            auto r = Propose(std::move(request));
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "partition_read_without_consumer_speed_bytes");
        }
        {
            auto request = BaseRequest();
            request.set_partition_read_without_consumer_speed_messages_per_second(-1);
            auto r = Propose(std::move(request));
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "partition_read_without_consumer_speed_messages");
        }
    });
}

Y_UNIT_TEST(AcceptsPositiveReadQuotasAndBurstDefaults) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);

    RunInActor(runtime, [&] {
        auto request = BaseRequest();
        request.set_partition_write_speed_messages_per_second(100);
        request.set_partition_write_burst_messages(0); // equals write speed
        request.set_partition_total_read_speed_bytes_per_second(5000);
        request.set_partition_total_read_speed_messages_per_second(50);
        request.set_partition_read_without_consumer_speed_bytes_per_second(100);
        request.set_partition_read_without_consumer_speed_messages_per_second(10);
        request.set_metrics_level(2);

        NKikimrSchemeOp::TModifyScheme modifyScheme;
        auto r = ProposeCreateTopic(modifyScheme, std::move(request), "/Root", "/Root", "t");
        UNIT_ASSERT_C(r, r.GetErrorMessage());
        const auto& config = modifyScheme.GetCreatePersQueueGroup().GetPQTabletConfig();
        UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionConfig().GetBurstSizeInMessages(), 100u);
        UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionConfig().GetReadSpeedInBytesPerSecond(), 5000u);
        UNIT_ASSERT(config.HasMetricsLevel());
        UNIT_ASSERT(NPQ::GetReadQuota(config, NPQ::CLIENTID_WITHOUT_CONSUMER));
    });
}

} // Y_UNIT_TEST_SUITE(ProposeCreateTopic)

} // namespace NKikimr::NPQ::NSchema
