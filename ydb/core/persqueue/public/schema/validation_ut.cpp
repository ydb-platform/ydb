#include "common.h"
#include "check_dlq_topics.h"
#include "schema_ut_helpers.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/services/lib/actors/consumers_advanced_monitoring_settings.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>

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

} // namespace

Y_UNIT_TEST_SUITE(SchemaValidation) {

Y_UNIT_TEST(GetWorkingDirAndName) {
    {
        auto [dir, name] = GetWorkingDirAndName("/Root/db/topic");
        UNIT_ASSERT_VALUES_EQUAL(dir, "/Root/db");
        UNIT_ASSERT_VALUES_EQUAL(name, "topic");
    }
    {
        auto [dir, name] = GetWorkingDirAndName("/Root/topic");
        UNIT_ASSERT_VALUES_EQUAL(dir, "/Root");
        UNIT_ASSERT_VALUES_EQUAL(name, "topic");
    }
    {
        // Invalid path should not throw out of GetWorkingDirAndName.
        auto [dir, name] = GetWorkingDirAndName("");
        UNIT_ASSERT(dir.empty());
        UNIT_ASSERT(name.empty());
    }
}

Y_UNIT_TEST(CheckRetentionPeriod) {
    {
        auto r = CheckRetentionPeriod(3600);
        UNIT_ASSERT(r.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*r, 3600);
    }
    {
        auto r = CheckRetentionPeriod(0);
        UNIT_ASSERT(!r.has_value());
        UNIT_ASSERT_STRING_CONTAINS(r.error(), "positive");
    }
    {
        auto r = CheckRetentionPeriod(-10);
        UNIT_ASSERT(!r.has_value());
        UNIT_ASSERT_STRING_CONTAINS(r.error(), "positive");
    }
    {
        auto r = CheckRetentionPeriod(i64(Max<i32>()) + 1);
        UNIT_ASSERT(!r.has_value());
        UNIT_ASSERT_STRING_CONTAINS(r.error(), "less than");
    }
    {
        auto r = CheckRetentionPeriod(Max<i32>());
        UNIT_ASSERT(r.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*r, Max<i32>());
    }
}

Y_UNIT_TEST(ConvertPositiveDuration) {
    {
        google::protobuf::Duration d;
        d.set_seconds(5);
        auto r = ConvertPositiveDuration(d);
        UNIT_ASSERT(r.has_value());
        UNIT_ASSERT_VALUES_EQUAL(r->Seconds(), 5u);
    }
    {
        google::protobuf::Duration d;
        d.set_seconds(-1);
        auto r = ConvertPositiveDuration(d);
        UNIT_ASSERT(!r.has_value());
        UNIT_ASSERT_STRING_CONTAINS(r.error(), "negative");
    }
    {
        google::protobuf::Duration d;
        d.set_seconds(0);
        d.set_nanos(0);
        auto r = ConvertPositiveDuration(d);
        UNIT_ASSERT(r.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*r, TDuration::Zero());
    }
}

Y_UNIT_TEST(ConvertConsumerAvailabilityPeriod) {
    {
        google::protobuf::Duration d;
        d.set_seconds(0);
        auto r = ConvertConsumerAvailabilityPeriod(d, "c1");
        UNIT_ASSERT(r.has_value());
        UNIT_ASSERT(!r->has_value()); // zero means clear / unset
    }
    {
        google::protobuf::Duration d;
        d.set_seconds(2);
        auto r = ConvertConsumerAvailabilityPeriod(d, "c1");
        UNIT_ASSERT(r.has_value());
        UNIT_ASSERT(r->has_value());
        UNIT_ASSERT_VALUES_EQUAL((*r)->Seconds(), 2u);
    }
    {
        google::protobuf::Duration d;
        d.set_seconds(-5);
        auto r = ConvertConsumerAvailabilityPeriod(d, "c1");
        UNIT_ASSERT(!r.has_value());
        UNIT_ASSERT_VALUES_EQUAL(r.error().GetStatus(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(r.error().GetErrorMessage(), "c1");
    }
}

Y_UNIT_TEST(ValidateDuration) {
    {
        google::protobuf::Duration d;
        d.set_seconds(1);
        d.set_nanos(0);
        UNIT_ASSERT(ValidateDuration(d, "timeout"));
    }
    {
        google::protobuf::Duration d;
        d.set_seconds(-1);
        auto r = ValidateDuration(d, "timeout");
        UNIT_ASSERT(!r);
        UNIT_ASSERT_VALUES_EQUAL(r.GetStatus(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "timeout");
    }
    {
        google::protobuf::Duration d;
        d.set_seconds(1);
        d.set_nanos(-1);
        auto r = ValidateDuration(d, "delay");
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "delay");
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "negative");
    }
}

Y_UNIT_TEST(ConvertDurationToMs32) {
    {
        google::protobuf::Duration d;
        d.set_seconds(0);
        d.set_nanos(0);
        UNIT_ASSERT_VALUES_EQUAL(ConvertDurationToMs32(d), 0u);
    }
    {
        google::protobuf::Duration d;
        d.set_seconds(1);
        d.set_nanos(500'000'000);
        UNIT_ASSERT_VALUES_EQUAL(ConvertDurationToMs32(d), 1500u);
    }
    {
        google::protobuf::Duration d;
        d.set_seconds(0);
        d.set_nanos(999'999); // less than 1ms
        UNIT_ASSERT_VALUES_EQUAL(ConvertDurationToMs32(d), 0u);
    }
    {
        // Saturate at Max<ui32>() for huge durations.
        google::protobuf::Duration d;
        d.set_seconds(i64(Max<ui32>()) / 1000 + 10);
        UNIT_ASSERT_VALUES_EQUAL(ConvertDurationToMs32(d), Max<ui32>());
    }
}

Y_UNIT_TEST(IfEqualThenDefault) {
    UNIT_ASSERT_VALUES_EQUAL(IfEqualThenDefault(0, 0, 42), 42);
    UNIT_ASSERT_VALUES_EQUAL(IfEqualThenDefault(7, 0, 42), 7);
    UNIT_ASSERT_VALUES_EQUAL(IfEqualThenDefault(TString("a"), TString("a"), TString("b")), "b");
    UNIT_ASSERT_VALUES_EQUAL(IfEqualThenDefault(TString("a"), TString("x"), TString("b")), "a");
}

Y_UNIT_TEST(CopyConfigClearsVolatileFields) {
    NKikimrSchemeOp::TPersQueueGroupDescription source;
    source.SetName("topic");
    source.SetTotalGroupCount(3);
    source.MutablePQTabletConfig()->AddPartitionKeySchema()->SetName("key");
    source.MutablePQTabletConfig()->SetRequireAuthRead(true);

    NKikimrSchemeOp::TPersQueueGroupDescription target;
    CopyConfig(target, source);

    UNIT_ASSERT_VALUES_EQUAL(target.GetName(), "topic");
    UNIT_ASSERT(!target.HasTotalGroupCount());
    UNIT_ASSERT_VALUES_EQUAL(target.GetPQTabletConfig().PartitionKeySchemaSize(), 0u);
    UNIT_ASSERT(target.GetPQTabletConfig().GetRequireAuthRead());
}

Y_UNIT_TEST(GetLocalClusterName) {
    UNIT_ASSERT_VALUES_EQUAL(GetLocalClusterName(nullptr), "");

    {
        auto list = MakeIntrusive<NPQ::NClusterTracker::TClustersList>();
        UNIT_ASSERT_VALUES_EQUAL(GetLocalClusterName(list), "");
    }
    {
        auto list = MakeIntrusive<NPQ::NClusterTracker::TClustersList>();
        list->Clusters.push_back({.Name = "dc1", .IsLocal = true});
        list->LocalCluster = &list->Clusters.front();
        UNIT_ASSERT_VALUES_EQUAL(GetLocalClusterName(list), "dc1");
    }
}

Y_UNIT_TEST(ValidatePartitionStrategyBounds) {
    NKikimrPQ::TPQTabletConfig config;
    auto* strategy = config.MutablePartitionStrategy();
    strategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);
    strategy->SetMinPartitionCount(10);
    strategy->SetMaxPartitionCount(5);
    strategy->SetScaleThresholdSeconds(30);
    strategy->SetScaleUpPartitionWriteSpeedThresholdPercent(80);
    strategy->SetScaleDownPartitionWriteSpeedThresholdPercent(20);

    auto r = ValidatePartitionStrategy(config);
    UNIT_ASSERT(!r);
    UNIT_ASSERT_VALUES_EQUAL(r.GetStatus(), Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "Max active partitions");
}

Y_UNIT_TEST(ValidatePartitionStrategyNoStrategyOk) {
    NKikimrPQ::TPQTabletConfig config;
    UNIT_ASSERT(ValidatePartitionStrategy(config));
}

Y_UNIT_TEST(ValidatePartitionStrategyNegativeMinMax) {
    // Min/MaxPartitionCount are uint32 in TPQTabletConfig — GetMinPartitionCount() < 0
    // is dead code. Setting -1 via the protobuf setter stores 4294967295 and fails the
    // max < min check instead. Document and lock that behavior.
    {
        NKikimrPQ::TPQTabletConfig config;
        auto* strategy = config.MutablePartitionStrategy();
        strategy->SetMinPartitionCount(static_cast<ui32>(-1));
        strategy->SetMaxPartitionCount(10);
        strategy->SetScaleThresholdSeconds(30);
        strategy->SetScaleUpPartitionWriteSpeedThresholdPercent(80);
        strategy->SetScaleDownPartitionWriteSpeedThresholdPercent(20);
        auto r = ValidatePartitionStrategy(config);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_VALUES_EQUAL(r.GetStatus(), Ydb::StatusIds::BAD_REQUEST);
        // Dead "< 0" branch never fires for uint32; max < min does.
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "Max active partitions");
    }
    {
        NKikimrPQ::TPQTabletConfig config;
        auto* strategy = config.MutablePartitionStrategy();
        strategy->SetMinPartitionCount(1);
        strategy->SetMaxPartitionCount(static_cast<ui32>(-2));
        strategy->SetScaleThresholdSeconds(30);
        strategy->SetScaleUpPartitionWriteSpeedThresholdPercent(80);
        strategy->SetScaleDownPartitionWriteSpeedThresholdPercent(20);
        // Max = 4294967294, Min = 1 → max >= min, so this config is accepted!
        // The "< 0" guard cannot reject unsigned wraparound.
        auto r = ValidatePartitionStrategy(config);
        UNIT_ASSERT(r);
    }
}

Y_UNIT_TEST(ValidatePartitionStrategyThresholdPercents) {
    auto makeOkBase = [] {
        NKikimrPQ::TPQTabletConfig config;
        auto* strategy = config.MutablePartitionStrategy();
        strategy->SetMinPartitionCount(1);
        strategy->SetMaxPartitionCount(2);
        strategy->SetScaleThresholdSeconds(30);
        strategy->SetScaleUpPartitionWriteSpeedThresholdPercent(80);
        strategy->SetScaleDownPartitionWriteSpeedThresholdPercent(20);
        return config;
    };

    {
        auto config = makeOkBase();
        config.MutablePartitionStrategy()->SetScaleUpPartitionWriteSpeedThresholdPercent(101);
        auto r = ValidatePartitionStrategy(config);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "scale up");
    }
    {
        auto config = makeOkBase();
        config.MutablePartitionStrategy()->SetScaleDownPartitionWriteSpeedThresholdPercent(101);
        auto r = ValidatePartitionStrategy(config);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "scale down");
    }
    {
        auto config = makeOkBase();
        config.MutablePartitionStrategy()->SetScaleThresholdSeconds(0);
        auto r = ValidatePartitionStrategy(config);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "greater than 0");
    }
    {
        auto config = makeOkBase();
        config.MutablePartitionStrategy()->SetScaleThresholdSeconds(1);
        UNIT_ASSERT(ValidatePartitionStrategy(config));
    }
    {
        auto config = makeOkBase();
        config.MutablePartitionConfig()->SetStorageLimitBytes(1_MB);
        auto r = ValidatePartitionStrategy(config);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "incompatible with retention storage");
    }
    {
        auto config = makeOkBase();
        config.MutablePartitionStrategy()->SetMaxPartitionCount(0); // unlimited
        UNIT_ASSERT(ValidatePartitionStrategy(config));
    }
}

Y_UNIT_TEST(CollectDlqTopicPaths) {
    NKikimrPQ::TPQTabletConfig config;
    {
        auto* c = config.AddConsumers();
        c->SetName("stream");
        c->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING);
        c->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
        c->SetDeadLetterPolicyEnabled(true);
        c->SetDeadLetterQueue("ignored");
    }
    {
        auto* c = config.AddConsumers();
        c->SetName("mlp_off");
        c->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
        c->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
        c->SetDeadLetterPolicyEnabled(false);
        c->SetDeadLetterQueue("dlq");
    }
    {
        auto* c = config.AddConsumers();
        c->SetName("mlp_sqs");
        c->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
        c->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
        c->SetDeadLetterPolicyEnabled(true);
        c->SetDeadLetterQueue("sqs://queue");
    }
    {
        auto* c = config.AddConsumers();
        c->SetName("mlp_ok");
        c->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
        c->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
        c->SetDeadLetterPolicyEnabled(true);
        c->SetDeadLetterQueue("dlq");
    }
    {
        auto* c = config.AddConsumers();
        c->SetName("mlp_delete");
        c->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
        c->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE);
        c->SetDeadLetterPolicyEnabled(true);
        c->SetDeadLetterQueue("dlq2");
    }

    auto paths = CollectDlqTopicPaths(config, "/Root");
    UNIT_ASSERT_VALUES_EQUAL(paths.size(), 1u);
    UNIT_ASSERT(paths.contains("/Root/dlq"));

    NKikimrPQ::TPQTabletConfig oldConfig = config;
    NKikimrPQ::TPQTabletConfig newConfig = config;
    auto* added = newConfig.AddConsumers();
    added->SetName("mlp_new");
    added->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
    added->SetDeadLetterPolicy(NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
    added->SetDeadLetterPolicyEnabled(true);
    added->SetDeadLetterQueue("dlq_new");

    auto newPaths = CollectNewDlqTopicPaths(newConfig, oldConfig, "/Root");
    UNIT_ASSERT_VALUES_EQUAL(newPaths.size(), 1u);
    UNIT_ASSERT(newPaths.contains("/Root/dlq_new"));
}

Y_UNIT_TEST(TResultBoolConversion) {
    UNIT_ASSERT(TResult{});
    UNIT_ASSERT(!TResult(Ydb::StatusIds::BAD_REQUEST, "err"));
    TResult r(Ydb::StatusIds::NOT_FOUND, "missing");
    UNIT_ASSERT_VALUES_EQUAL(r.GetStatus(), Ydb::StatusIds::NOT_FOUND);
    UNIT_ASSERT_VALUES_EQUAL(r.GetErrorMessage(), "missing");
}

Y_UNIT_TEST(ValidateConfigWriteSpeedAndBurst) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    auto& pq = runtime.GetAppData().PQConfig;
    pq.ClearValidWriteSpeedLimitsKbPerSec();
    pq.AddValidWriteSpeedLimitsKbPerSec(1); // 1 KB/s
    pq.ClearValidRetentionLimits();
    auto* limit = pq.AddValidRetentionLimits();
    limit->SetMinPeriodSeconds(3600);
    limit->SetMaxPeriodSeconds(86400);
    limit->SetMinStorageMegabytes(0);
    limit->SetMaxStorageMegabytes(1024);

    auto makeBase = [] {
        NKikimrPQ::TPQTabletConfig config;
        config.MutablePartitionConfig()->SetWriteSpeedInBytesPerSecond(1_KB);
        config.MutablePartitionConfig()->SetBurstSize(1_KB);
        config.MutablePartitionConfig()->SetLifetimeSeconds(3600);
        return config;
    };

    RunInActor(runtime, [&] {
        UNIT_ASSERT(ValidateConfig(makeBase(), EOperation::Create));

        {
            auto config = makeBase();
            config.MutablePartitionConfig()->SetWriteSpeedInBytesPerSecond(2_KB);
            auto r = ValidateConfig(config, EOperation::Create);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "write_speed");
        }
        {
            auto config = makeBase();
            config.MutablePartitionConfig()->SetBurstSize(3_MB);
            auto r = ValidateConfig(config, EOperation::Create);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "Invalid write burst");
        }
        {
            auto config = makeBase();
            config.MutablePartitionConfig()->SetLifetimeSeconds(60);
            auto r = ValidateConfig(config, EOperation::Create);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "retention hours");
        }
        {
            pq.ClearValidWriteSpeedLimitsKbPerSec();
            auto config = makeBase();
            config.MutablePartitionConfig()->SetWriteSpeedInBytesPerSecond(12345);
            UNIT_ASSERT(ValidateConfig(config, EOperation::Create));
        }
    });
}

Y_UNIT_TEST(ValidateConfigMlpAndStorageIncompatible) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    auto& pq = runtime.GetAppData().PQConfig;
    pq.ClearValidWriteSpeedLimitsKbPerSec();
    pq.ClearValidRetentionLimits();

    RunInActor(runtime, [&] {
        NKikimrPQ::TPQTabletConfig config;
        config.MutablePartitionConfig()->SetWriteSpeedInBytesPerSecond(1_MB);
        config.MutablePartitionConfig()->SetBurstSize(1_MB);
        config.MutablePartitionConfig()->SetLifetimeSeconds(3600);
        config.MutablePartitionConfig()->SetStorageLimitBytes(10_MB);
        auto* consumer = config.AddConsumers();
        consumer->SetName("shared");
        consumer->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);

        auto r = ValidateConfig(config, EOperation::Create);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "shared consumers");
    });
}

Y_UNIT_TEST(ValidateConsumersDuplicatesAndLimits) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    auto& pq = runtime.GetAppData().PQConfig;
    pq.MutableDefaultClientServiceType()->SetName("data-streams");
    pq.MutableDefaultClientServiceType()->SetMaxReadRulesCountPerTopic(1);
    pq.ClearClientServiceType();

    RunInActor(runtime, [&] {
        {
            NKikimrPQ::TPQTabletConfig config;
            auto* a = config.AddConsumers();
            a->SetName("c1");
            a->SetServiceType("data-streams");
            auto* b = config.AddConsumers();
            b->SetName("c1");
            b->SetServiceType("data-streams");
            auto r = ValidateConsumersConfig(config, EOperation::Create);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_VALUES_EQUAL(r.GetStatus(), Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "Duplicate consumer");
        }
        {
            NKikimrPQ::TPQTabletConfig config;
            auto* a = config.AddConsumers();
            a->SetName("c1");
            a->SetServiceType("data-streams");
            auto* b = config.AddConsumers();
            b->SetName("c1");
            b->SetServiceType("data-streams");
            auto r = ValidateConsumersConfig(config, EOperation::Alter);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_VALUES_EQUAL(r.GetStatus(), Ydb::StatusIds::ALREADY_EXISTS);
        }
        {
            NKikimrPQ::TPQTabletConfig config;
            auto* a = config.AddConsumers();
            a->SetName("c1");
            a->SetServiceType("data-streams");
            a->SetImportant(true);
            a->SetAvailabilityPeriodMs(1000);
            auto r = ValidateConsumersConfig(config, EOperation::Create);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "mutually exclusive");
        }
        {
            NKikimrPQ::TPQTabletConfig config;
            auto* a = config.AddConsumers();
            a->SetName("c1");
            a->SetServiceType("data-streams");
            auto* b = config.AddConsumers();
            b->SetName("c2");
            b->SetServiceType("data-streams");
            auto r = ValidateConsumersConfig(config, EOperation::Create);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "service type");
        }
        {
            NKikimrPQ::TPQTabletConfig config;
            config.MutableCodecs()->AddIds(0);
            auto* a = config.AddConsumers();
            a->SetName("c1");
            a->SetServiceType("data-streams");
            a->MutableCodec()->AddIds(1);
            auto r = ValidateConsumersConfig(config, EOperation::Create);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "unsupported codec");
        }
    });
}

Y_UNIT_TEST(FillMeteringModeBranches) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

    RunInActor(runtime, [&] {
        {
            runtime.GetAppData().PQConfig.MutableBillingMeteringConfig()->SetEnabled(true);
            NKikimrPQ::TPQTabletConfig config;
            UNIT_ASSERT(FillMeteringMode(config, Ydb::Topic::METERING_MODE_UNSPECIFIED, EOperation::Create));
            UNIT_ASSERT_VALUES_EQUAL(
                NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(config.GetMeteringMode()),
                NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(
                    NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS));

            config.ClearMeteringMode();
            UNIT_ASSERT(FillMeteringMode(config, Ydb::Topic::METERING_MODE_UNSPECIFIED, EOperation::Alter));
            UNIT_ASSERT(!config.HasMeteringMode());

            UNIT_ASSERT(FillMeteringMode(config, Ydb::Topic::METERING_MODE_REQUEST_UNITS, EOperation::Alter));
            UNIT_ASSERT_VALUES_EQUAL(
                NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(config.GetMeteringMode()),
                NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(
                    NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS));

            UNIT_ASSERT(FillMeteringMode(config, Ydb::Topic::METERING_MODE_RESERVED_CAPACITY, EOperation::Alter));
            UNIT_ASSERT_VALUES_EQUAL(
                NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(config.GetMeteringMode()),
                NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(
                    NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY));

            auto r = FillMeteringMode(
                config,
                static_cast<Ydb::Topic::MeteringMode>(999),
                EOperation::Create);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "Unknown metering mode");
        }
        {
            runtime.GetAppData().PQConfig.MutableBillingMeteringConfig()->SetEnabled(false);
            NKikimrPQ::TPQTabletConfig config;
            UNIT_ASSERT(FillMeteringMode(config, Ydb::Topic::METERING_MODE_UNSPECIFIED, EOperation::Create));
            auto r = FillMeteringMode(config, Ydb::Topic::METERING_MODE_REQUEST_UNITS, EOperation::Create);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_VALUES_EQUAL(r.GetStatus(), Ydb::StatusIds::PRECONDITION_FAILED);
        }
    });
}

Y_UNIT_TEST(ProcessTopicAttributesAdvancedMonitoringAndId) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    runtime.GetAppData().FeatureFlags.SetEnableTopicSourceIdMappingById(true);

    RunInActor(runtime, [&] {
        NGRpcProxy::V1::TConsumersAdvancedMonitoringSettings monitoring;

        {
            google::protobuf::Map<TProtoStringType, TProtoStringType> attrs;
            attrs["_advanced_monitoring"] = "{}";
            NKikimrSchemeOp::TPersQueueGroupDescription config;
            auto r = ProcessTopicAttributes(attrs, &config, EOperation::Create, /*fcc=*/true, monitoring);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "not supported in non-federation");
        }
        {
            google::protobuf::Map<TProtoStringType, TProtoStringType> attrs;
            attrs["_advanced_monitoring"] = "not-json";
            NKikimrSchemeOp::TPersQueueGroupDescription config;
            auto r = ProcessTopicAttributes(attrs, &config, EOperation::Create, /*fcc=*/false, monitoring);
            UNIT_ASSERT(!r);
        }
        {
            google::protobuf::Map<TProtoStringType, TProtoStringType> attrs;
            attrs["_id"] = "abc";
            NKikimrSchemeOp::TPersQueueGroupDescription config;
            auto r = ProcessTopicAttributes(attrs, &config, EOperation::Create, /*fcc=*/false, monitoring);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "not a valid positive integer");
        }
        {
            google::protobuf::Map<TProtoStringType, TProtoStringType> attrs;
            attrs["_id"] = "0";
            NKikimrSchemeOp::TPersQueueGroupDescription config;
            auto r = ProcessTopicAttributes(attrs, &config, EOperation::Create, /*fcc=*/false, monitoring);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "greater than 0");
        }
        {
            google::protobuf::Map<TProtoStringType, TProtoStringType> attrs;
            attrs["_id"] = "123";
            NKikimrSchemeOp::TPersQueueGroupDescription config;
            UNIT_ASSERT(ProcessTopicAttributes(attrs, &config, EOperation::Create, /*fcc=*/false, monitoring));
            UNIT_ASSERT_VALUES_EQUAL(config.GetPQTabletConfig().GetId().GetId(), 123u);
        }
        {
            google::protobuf::Map<TProtoStringType, TProtoStringType> attrs;
            attrs["_id"] = "123";
            NKikimrSchemeOp::TPersQueueGroupDescription config;
            UNIT_ASSERT(ProcessTopicAttributes(attrs, &config, EOperation::Create, /*fcc=*/true, monitoring));
            UNIT_ASSERT(!config.GetPQTabletConfig().HasId());
        }
    });
}

Y_UNIT_TEST(AddConsumerServiceTypeAndCodecs) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());
    auto& pq = runtime.GetAppData().PQConfig;
    pq.SetTopicsAreFirstClassCitizen(true);
    pq.MutableDefaultClientServiceType()->SetName("data-streams");
    pq.MutableDefaultClientServiceType()->SetMaxReadRulesCountPerTopic(10);
    pq.MutableDefaultClientServiceType()->ClearPasswordHashes();
    pq.ClearClientServiceType();
    auto* st = pq.AddClientServiceType();
    st->SetName("secure");
    st->SetMaxReadRulesCountPerTopic(2);
    st->AddPasswordHashes("5f4dcc3b5aa765d61d8327deb882cf99"); // md5("password")

    RunInActor(runtime, [&] {
    auto types = GetSupportedClientServiceTypes();
    UNIT_ASSERT(types.contains("data-streams"));
    UNIT_ASSERT(types.contains("secure"));

    NKikimrPQ::TPQTabletConfig config;
    config.SetEnableCompactification(true);

    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("bad/name");
        consumer.mutable_streaming_consumer_type();
        auto r = AddConsumer(&config, consumer, types, true, nullptr);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "illegal symbols");
    }
    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("");
        consumer.mutable_streaming_consumer_type();
        auto r = AddConsumer(&config, consumer, types, true, nullptr);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "empty name");
    }
    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name(TString{NPQ::CLIENTID_COMPACTION_CONSUMER});
        consumer.mutable_streaming_consumer_type();
        NKikimrPQ::TPQTabletConfig noCompact = config;
        noCompact.SetEnableCompactification(false);
        auto r = AddConsumer(&noCompact, consumer, types, true, nullptr);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "compactification");
    }
    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("c_version");
        consumer.mutable_streaming_consumer_type();
        (*consumer.mutable_attributes())["_version"] = "x";
        auto r = AddConsumer(&config, consumer, types, true, nullptr);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "_version");
    }
    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("c_svc");
        consumer.mutable_streaming_consumer_type();
        (*consumer.mutable_attributes())["_service_type"] = "missing";
        auto r = AddConsumer(&config, consumer, types, true, nullptr);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "Unknown _service_type");
    }
    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("c_pass_bad");
        consumer.mutable_streaming_consumer_type();
        (*consumer.mutable_attributes())["_service_type"] = "secure";
        (*consumer.mutable_attributes())["_service_type_password"] = "wrong";
        auto r = AddConsumer(&config, consumer, types, true, nullptr);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "incorrect client service type password");
    }
    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("c_pass_ok");
        consumer.mutable_streaming_consumer_type();
        (*consumer.mutable_attributes())["_service_type"] = "secure";
        (*consumer.mutable_attributes())["_service_type_password"] = "password";
        (*consumer.mutable_attributes())["_sqs_read_request_attempt_id_period_ms"] = "1500";
        consumer.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);
        UNIT_ASSERT(AddConsumer(&config, consumer, types, true, nullptr));
        const auto* c = NPQ::GetConsumer(config, "c_pass_ok");
        UNIT_ASSERT(c);
        UNIT_ASSERT_VALUES_EQUAL(c->GetServiceType(), "secure");
        UNIT_ASSERT_VALUES_EQUAL(c->GetReadRequestAttemptIdPeriodMs(), 1500u);
    }
    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("c_no_pass");
        consumer.mutable_streaming_consumer_type();
        (*consumer.mutable_attributes())["_service_type"] = "secure";
        pq.SetForceClientServiceTypePasswordCheck(true);
        auto r = AddConsumer(&config, consumer, types, true, nullptr);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "no client service type password");
        pq.SetForceClientServiceTypePasswordCheck(false);
    }
    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("c_bad_codec");
        consumer.mutable_streaming_consumer_type();
        consumer.mutable_supported_codecs()->add_codecs(static_cast<Ydb::Topic::Codec>(0));
        auto r = AddConsumer(&config, consumer, types, true, nullptr);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "Unknown codec");
    }
    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("c_important");
        consumer.mutable_streaming_consumer_type();
        consumer.set_important(true);
        // FCC + disabled disk quota forbids important.
        runtime.GetAppData().FeatureFlags.SetEnableTopicDiskSubDomainQuota(false);
        auto r = AddConsumer(&config, consumer, types, true, nullptr);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "important flag is forbiden");
    }
    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("c_read_from");
        consumer.mutable_streaming_consumer_type();
        consumer.mutable_read_from()->set_seconds(-1);
        auto r = AddConsumer(&config, consumer, types, false, nullptr);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "can't be negative");
    }
    });
}

Y_UNIT_TEST(ProcessTopicAttributesInvalidValues) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

    RunInActor(runtime, [&] {
        auto expectBad = [&](const TString& key, const TString& value, const TString& needle) {
            NKikimrSchemeOp::TPersQueueGroupDescription config;
            config.MutablePQTabletConfig()->MutablePartitionConfig();
            google::protobuf::Map<TProtoStringType, TProtoStringType> attrs;
            attrs[key] = value;
            NGRpcProxy::V1::TConsumersAdvancedMonitoringSettings monitoring;
            auto r = ProcessTopicAttributes(attrs, &config, EOperation::Create, /*fcc=*/true, monitoring);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), needle);
        };

        expectBad("_allow_unauthenticated_read", "not-bool", "not bool");
        expectBad("_allow_unauthenticated_write", "xx", "not bool");
        expectBad("_abc_id", "NaN", "not integer");
        expectBad("_max_partition_storage_size", "-5", "can't be negative");
        expectBad("_max_partition_storage_size", "oops", "not ui64");
        expectBad("_message_group_seqno_retention_period_ms", "-1", "can't be negative");
        expectBad(
            "_message_group_seqno_retention_period_ms",
            ToString(DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS + 1),
            "must be less than default limit");
        expectBad("_message_group_seqno_retention_period_ms", "bad", "not ui64");
        expectBad("_max_partition_message_groups_seqno_stored", "-3", "can't be negative");
        expectBad("_max_partition_message_groups_seqno_stored", "x", "not ui64");
        expectBad("_timestamp_type", "WeirdTime", "incorrect value");
        expectBad("_advanced_monitoring", "{}", "not supported in non-federation");
    });
}

Y_UNIT_TEST(ValidateLocalClusterBranches) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

    RunInActor(runtime, [&] {
        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);

        auto clusters = MakeIntrusive<NPQ::NClusterTracker::TClustersList>();
        clusters->Clusters.push_back({.Name = "dc1", .IsLocal = true});
        clusters->LocalCluster = &clusters->Clusters.front();

        {
            NKikimrPQ::TPQTabletConfig config;
            config.SetLocalDC(true);
            config.SetDC("dc2");
            auto r = ValidateLocalCluster(clusters, config);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "Local cluster is not correct");
        }
        {
            NKikimrPQ::TPQTabletConfig config;
            config.SetLocalDC(false);
            config.SetDC("unknown");
            auto r = ValidateLocalCluster(clusters, config);
            UNIT_ASSERT(!r);
            UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "Unknown cluster");
        }
        {
            NKikimrPQ::TPQTabletConfig config;
            config.SetLocalDC(true);
            config.SetDC("dc1");
            UNIT_ASSERT(ValidateLocalCluster(clusters, config));
        }

        runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
        NKikimrPQ::TPQTabletConfig config;
        config.SetDC("whatever");
        UNIT_ASSERT(ValidateLocalCluster(clusters, config));
    });
}

Y_UNIT_TEST(ValidateConsumersTooMany) {
    NActors::TTestBasicRuntime runtime(1, false);
    runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

    RunInActor(runtime, [&] {
        NKikimrPQ::TPQTabletConfig config;
        for (int i = 0; i < MAX_READ_RULES_COUNT + 1; ++i) {
            auto* c = config.AddConsumers();
            c->SetName(TStringBuilder() << "c" << i);
            c->SetServiceType("data-streams");
        }
        auto r = ValidateConsumersConfig(config, EOperation::Create);
        UNIT_ASSERT(!r);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "read rules count cannot be more than");
    });
}

} // Y_UNIT_TEST_SUITE(SchemaValidation)

} // namespace NKikimr::NPQ::NSchema
