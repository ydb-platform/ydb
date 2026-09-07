#include "schema_ut_helpers.h"

#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>

namespace NKikimr::NPQ::NSchema {

using namespace NTests;

Y_UNIT_TEST_SUITE(SchemaOps) {

Y_UNIT_TEST(CreateAlterDropSmoke) {
    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_smoke";

    CreateTopic(runtime, path);

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* consumer = request.add_add_consumers();
        consumer->set_name("extra");
        consumer->mutable_streaming_consumer_type();
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    {
        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT(NPQ::GetConsumer(config, "extra"));
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.add_drop_consumers("extra");
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    {
        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT(!NPQ::GetConsumer(config, "extra"));
    }

    AssertStatus(DoDrop(runtime, path), Ydb::StatusIds::SUCCESS);
    AssertStatus(DoDrop(runtime, path), Ydb::StatusIds::SCHEME_ERROR);
}

Y_UNIT_TEST(AddAndRemoveConsumerActors) {
    auto setup = CreateSetup("CoreAddRemove");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_add_remove";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("c2");
        consumer.mutable_streaming_consumer_type();
        AssertStatus(DoAddConsumer(runtime, path, consumer), Ydb::StatusIds::SUCCESS);
        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT(NPQ::GetConsumer(config, "c2"));
    }

    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("shared_c");
        auto* type = consumer.mutable_shared_consumer_type();
        type->set_keep_messages_order(true);
        AssertStatus(DoAddConsumer(runtime, path, consumer), Ydb::StatusIds::SUCCESS);
        auto config = DescribeTabletConfig(runtime, path);
        const auto* c = NPQ::GetConsumer(config, "shared_c");
        UNIT_ASSERT(c);
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrPQ::TPQTabletConfig::EConsumerType_Name(c->GetType()),
            NKikimrPQ::TPQTabletConfig::EConsumerType_Name(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP));
    }

    AssertStatus(DoRemoveConsumer(runtime, path, "c2"), Ydb::StatusIds::SUCCESS);
    AssertStatus(DoRemoveConsumer(runtime, path, "shared_c"), Ydb::StatusIds::SUCCESS);
    AssertStatus(DoRemoveConsumer(runtime, path, "missing"), Ydb::StatusIds::NOT_FOUND);
}

Y_UNIT_TEST(CannotChangeConsumerType) {
    auto setup = CreateSetup("CoreConsumerType");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_type_change";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alter = request.add_alter_consumers();
        alter->set_name("user");
        alter->mutable_alter_shared_consumer_type();
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::BAD_REQUEST, "Cannot alter consumer type");
    }

    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("shared_c");
        consumer.mutable_shared_consumer_type();
        AssertStatus(DoAddConsumer(runtime, path, consumer), Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alter = request.add_alter_consumers();
        alter->set_name("shared_c");
        alter->mutable_alter_streaming_consumer_type();
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::BAD_REQUEST, "Cannot alter consumer type");
    }
}

Y_UNIT_TEST(AlterCdcAllowsRetentionAndWriteLimits) {
    auto setup = CreateSetup("CoreCdcAlter");
    ExecuteDDL(*setup, "CREATE TABLE table_cdc (id Uint64, PRIMARY KEY (id))");
    ExecuteDDL(*setup, "ALTER TABLE table_cdc ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES')");

    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/table_cdc/feed";

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.set_set_retention_storage_mb(100);
        request.set_set_partition_write_speed_bytes_per_second(9000);
        request.set_set_partition_write_burst_bytes(100500);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    const auto& partConfig = DescribePartitionConfig(runtime, path);
    UNIT_ASSERT_VALUES_EQUAL(partConfig.GetStorageLimitBytes(), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(partConfig.GetWriteSpeedInBytesPerSecond(), 9000u);
    UNIT_ASSERT_VALUES_EQUAL(partConfig.GetBurstSize(), 100500u);

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        (*request.mutable_alter_attributes())["_allowed_codecs"] = "RAW";
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::BAD_REQUEST, "Full alter of cdc stream is forbidden");
    }
}

Y_UNIT_TEST(AlterMessagesPerSecond) {
    auto setup = CreateSetup("CoreAlterMsgPerSec");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_alter_msg";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.set_set_partition_write_speed_messages_per_second(1234);
        request.set_set_partition_write_burst_messages(5678);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);

        const auto& partConfig = DescribePartitionConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(partConfig.GetWriteSpeedInMessagesPerSecond(), 1234u);
        UNIT_ASSERT_VALUES_EQUAL(partConfig.GetBurstSizeInMessages(), 5678u);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.set_set_partition_write_speed_messages_per_second(-1);
        AssertStatus(
            DoAlter(runtime, request),
            Ydb::StatusIds::BAD_REQUEST,
            "partition_write_speed_messages_per_second");
    }
}

Y_UNIT_TEST(AlterPreservesMeteringModeWhenUnspecified) {
    // Regression: AlterTopic used to call FillMeteringMode(..., EOperation::Create),
    // which rewrote RESERVED_CAPACITY to REQUEST_UNITS when set_metering_mode was unset.
    auto setup = CreateMeteringSetup("CoreAlterMetering");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_metering";

    {
        auto request = MakeCreateTopicRequest(path);
        request.set_metering_mode(Ydb::Topic::METERING_MODE_RESERVED_CAPACITY);
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);
        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(config.GetMeteringMode()),
            NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(
                NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY));
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* consumer = request.add_add_consumers();
        consumer->set_name("extra");
        consumer->mutable_streaming_consumer_type();
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);

        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(config.GetMeteringMode()),
            NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(
                NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY));
        UNIT_ASSERT(NPQ::GetConsumer(config, "extra"));
    }
}

Y_UNIT_TEST(TopicAttributesSmoke) {
    auto setup = CreateSetup("CoreTopicAttrs");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_attrs";

    auto request = MakeCreateTopicRequest(path);
    auto& attrs = *request.mutable_attributes();
    attrs["_allow_unauthenticated_read"] = "true";
    attrs["_allow_unauthenticated_write"] = "false";
    attrs["_abc_slug"] = "slug";
    attrs["_federation_account"] = "acc";
    attrs["_abc_id"] = "42";
    attrs["_max_partition_storage_size"] = "1048576";
    attrs["_message_group_seqno_retention_period_ms"] = "60000";
    attrs["_max_partition_message_groups_seqno_stored"] = "100";
    attrs["_cleanup_policy"] = "compact";
    attrs["_sqs_queue_name"] = "q";
    attrs["_sqs_account_name"] = "a";
    attrs["_sqs_cloud_id"] = "c";
    attrs["_sqs_folder_id"] = "f";
    attrs["_sqs_export_metrics"] = "true";
    AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);

    auto config = DescribeTabletConfig(runtime, path);
    UNIT_ASSERT(!config.GetRequireAuthRead());
    UNIT_ASSERT(config.GetRequireAuthWrite());
    UNIT_ASSERT_VALUES_EQUAL(config.GetAbcSlug(), "slug");
    UNIT_ASSERT_VALUES_EQUAL(config.GetFederationAccount(), "acc");
    UNIT_ASSERT_VALUES_EQUAL(config.GetAbcId(), 42u);
    UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionConfig().GetMaxSizeInPartition(), 1048576);
    UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionConfig().GetSourceIdLifetimeSeconds(), 60u);
    UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionConfig().GetSourceIdMaxCounts(), 100);
    UNIT_ASSERT(config.GetEnableCompactification());
    UNIT_ASSERT_VALUES_EQUAL(config.GetSqsQueueName(), "q");
    UNIT_ASSERT(config.GetSqsExportMetrics());
}

Y_UNIT_TEST(TopicAttributesValidation) {
    auto setup = CreateSetup("CoreTopicAttrsBad");
    auto& runtime = setup->GetRuntime();

    auto tryCreate = [&](const TString& path, const TString& key, const TString& value, const TString& err) {
        auto request = MakeCreateTopicRequest(path);
        (*request.mutable_attributes())[key] = value;
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::BAD_REQUEST, err);
    };

    tryCreate("/Root/bad_attr_unknown", "_no_such_attr", "1", "not supported");
    tryCreate("/Root/bad_partitions_per_tablet", "_partitions_per_tablet", "21", "greater than 20");
    tryCreate("/Root/bad_partitions_per_tablet_nan", "_partitions_per_tablet", "x", "not ui32");
    tryCreate("/Root/bad_unauth_read", "_allow_unauthenticated_read", "maybe", "not bool");
    tryCreate("/Root/bad_abc_id", "_abc_id", "zz", "not integer");
    tryCreate("/Root/bad_max_part_size", "_max_partition_storage_size", "-1", "can't be negative");
    tryCreate("/Root/bad_msg_group_ms", "_message_group_seqno_retention_period_ms", "-5", "can't be negative");
    tryCreate("/Root/bad_msg_group_count", "_max_partition_message_groups_seqno_stored", "-1", "can't be negative");
    tryCreate("/Root/bad_timestamp", "_timestamp_type", "weird", "incorrect value");
    tryCreate("/Root/bad_sqs_metrics", "_sqs_export_metrics", "nope", "not bool");
}

Y_UNIT_TEST(CreateAlterDropPrepareOnlyAndIfFlags) {
    auto setup = CreateSetup("CorePrepareIf");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_prepare";

    {
        auto result = DoCreate(runtime, MakeCreateTopicRequest(path), "/Root", /*prepareOnly=*/true);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(result->ModifyScheme.HasCreatePersQueueGroup() ||
            result->ModifyScheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup ||
            result->ModifyScheme.ByteSizeLong() > 0);
        // Topic must not exist yet.
        auto edge = runtime.AllocateEdgeActor();
        runtime.Register(NDescriber::CreateDescriberActor(edge, "/Root", {path}));
        auto response = runtime.GrabEdgeEvent<NDescriber::TEvDescribeTopicsResponse>(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(response->Topics.begin()->second.Status, NDescriber::EStatus::NOT_FOUND);
    }

    AssertStatus(DoCreate(runtime, MakeCreateTopicRequest(path)), Ydb::StatusIds::SUCCESS);
    AssertStatus(DoCreate(runtime, MakeCreateTopicRequest(path), "/Root", false, /*ifNotExists=*/true), Ydb::StatusIds::SUCCESS);

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.set_set_partition_write_speed_messages_per_second(11);
        auto result = DoAlter(runtime, request, "/Root", /*prepareOnly=*/true);
        AssertStatus(result, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(result->ModifyScheme.ByteSizeLong() > 0);
        // Not applied.
        UNIT_ASSERT_VALUES_UNEQUAL(
            DescribePartitionConfig(runtime, path).GetWriteSpeedInMessagesPerSecond(), 11u);
    }

    AssertStatus(DoAlter(runtime, [&]{
        Ydb::Topic::AlterTopicRequest request;
        request.set_path("/Root/missing_topic");
        request.set_set_partition_write_speed_messages_per_second(1);
        return request;
    }(), "/Root", false, /*ifExists=*/true), Ydb::StatusIds::SUCCESS);

    AssertStatus(DoDrop(runtime, "/Root/missing_drop", "/Root", /*ifExists=*/true), Ydb::StatusIds::SUCCESS);
    AssertStatus(DoDrop(runtime, path), Ydb::StatusIds::SUCCESS);
}

Y_UNIT_TEST(CreateAlterViaPromise) {
    auto setup = CreateSetup("CorePromiseApi");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_promise";

    {
        auto future = DoCreateViaPromise(runtime, MakeCreateTopicRequest(path));
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(future.GetValue().Status, Ydb::StatusIds::SUCCESS);
    }
    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* consumer = request.add_add_consumers();
        consumer->set_name("p1");
        consumer->mutable_streaming_consumer_type();
        auto future = DoAlterViaPromise(runtime, request);
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(future.GetValue().Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(NPQ::GetConsumer(DescribeTabletConfig(runtime, path), "p1"));
    }
}

Y_UNIT_TEST(AlterSharedConsumerDlqAndReadQuotas) {
    auto setup = CreateSetup("CoreAlterSharedDlq");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_alter_shared";
    AssertStatus(DoCreate(runtime, MakeCreateTopicRequest("/Root/dlq_alter")), Ydb::StatusIds::SUCCESS);
    AssertStatus(DoCreate(runtime, MakeCreateTopicRequest("/Root/dlq_alter2")), Ydb::StatusIds::SUCCESS);

    {
        auto request = MakeCreateTopicRequest(path);
        request.clear_consumers();
        auto* consumer = request.add_consumers();
        consumer->set_name("shared_c");
        auto* type = consumer->mutable_shared_consumer_type();
        type->set_keep_messages_order(true);
        type->mutable_dead_letter_policy()->set_enabled(true);
        type->mutable_dead_letter_policy()->mutable_move_action()->set_dead_letter_queue("dlq_alter");
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alter = request.add_alter_consumers();
        alter->set_name("shared_c");
        auto* alterType = alter->mutable_alter_shared_consumer_type();
        alterType->mutable_set_default_processing_timeout()->set_seconds(9);
        alterType->mutable_set_receive_message_delay()->set_seconds(1);
        alterType->mutable_set_receive_message_wait_time()->set_seconds(2);
        auto* policy = alterType->mutable_alter_dead_letter_policy();
        policy->set_set_enabled(true);
        policy->mutable_alter_condition()->set_set_max_processing_attempts(7);
        policy->mutable_alter_move_action()->set_set_dead_letter_queue("dlq_alter2");
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);

        auto config = DescribeTabletConfig(runtime, path);
        const auto* c = NPQ::GetConsumer(config, "shared_c");
        UNIT_ASSERT(c);
        UNIT_ASSERT_VALUES_EQUAL(c->GetDefaultProcessingTimeoutSeconds(), 9);
        UNIT_ASSERT_VALUES_EQUAL(c->GetMaxProcessingAttempts(), 7);
        UNIT_ASSERT_VALUES_EQUAL(c->GetDeadLetterQueue(), "dlq_alter2");
        UNIT_ASSERT_VALUES_EQUAL(c->GetDefaultDelayMessageTimeMs(), 1000u);
        UNIT_ASSERT_VALUES_EQUAL(c->GetDefaultReceiveMessageWaitTimeMs(), 2000u);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alter = request.add_alter_consumers();
        alter->set_name("shared_c");
        alter->set_set_read_speed_bytes_per_second(111);
        alter->set_set_read_speed_messages_per_second(22);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
        auto config = DescribeTabletConfig(runtime, path);
        const auto* quota = NPQ::GetReadQuota(config, "shared_c");
        UNIT_ASSERT(quota);
        UNIT_ASSERT_VALUES_EQUAL(quota->GetSpeedInBytesPerSecond(), 111u);
        UNIT_ASSERT_VALUES_EQUAL(quota->GetSpeedInMessagesPerSecond(), 22u);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.set_set_partition_total_read_speed_bytes_per_second(1000);
        request.set_set_partition_total_read_speed_messages_per_second(50);
        request.set_set_partition_read_without_consumer_speed_bytes_per_second(200);
        request.set_set_partition_read_without_consumer_speed_messages_per_second(10);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
        auto part = DescribePartitionConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(part.GetReadSpeedInBytesPerSecond(), 1000u);
        UNIT_ASSERT_VALUES_EQUAL(part.GetReadSpeedInMessagesPerSecond(), 50u);
        auto config = DescribeTabletConfig(runtime, path);
        const auto* without = NPQ::GetReadQuota(config, CLIENTID_WITHOUT_CONSUMER);
        UNIT_ASSERT(without);
        UNIT_ASSERT_VALUES_EQUAL(without->GetSpeedInBytesPerSecond(), 200u);
        UNIT_ASSERT_VALUES_EQUAL(without->GetSpeedInMessagesPerSecond(), 10u);
    }
}

Y_UNIT_TEST(AlterAutoPartitioningAndCodecs) {
    auto setup = CreateSetup("CoreAlterAutoPart");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_autopart";

    {
        auto request = MakeCreateTopicRequest(path, 2);
        auto* autoSettings = request.mutable_partitioning_settings()->mutable_auto_partitioning_settings();
        autoSettings->set_strategy(Ydb::Topic::AUTO_PARTITIONING_STRATEGY_SCALE_UP);
        autoSettings->mutable_partition_write_speed()->set_up_utilization_percent(80);
        autoSettings->mutable_partition_write_speed()->set_down_utilization_percent(20);
        autoSettings->mutable_partition_write_speed()->mutable_stabilization_window()->set_seconds(30);
        request.mutable_partitioning_settings()->set_max_active_partitions(4);
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.mutable_alter_partitioning_settings()->set_set_min_active_partitions(-1);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::BAD_REQUEST, "non-negative");
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* settings = request.mutable_alter_partitioning_settings();
        settings->set_set_min_active_partitions(2);
        settings->set_set_max_active_partitions(6);
        auto* autoSettings = settings->mutable_alter_auto_partitioning_settings();
        autoSettings->set_set_strategy(Ydb::Topic::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN);
        autoSettings->mutable_set_partition_write_speed()->set_set_up_utilization_percent(70);
        autoSettings->mutable_set_partition_write_speed()->set_set_down_utilization_percent(15);
        autoSettings->mutable_set_partition_write_speed()->mutable_set_stabilization_window()->set_seconds(40);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);

        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionStrategy().GetMaxPartitionCount(), 6);
        UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionStrategy().GetScaleUpPartitionWriteSpeedThresholdPercent(), 70);
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrPQ::TPQTabletConfig::TPartitionStrategyType_Name(config.GetPartitionStrategy().GetPartitionStrategyType()),
            NKikimrPQ::TPQTabletConfig::TPartitionStrategyType_Name(
                NKikimrPQ::TPQTabletConfig::CAN_SPLIT_AND_MERGE));
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.mutable_set_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);
        request.mutable_set_supported_codecs()->add_codecs(Ydb::Topic::CODEC_GZIP);
        request.set_set_metrics_level(1);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(config.GetCodecs().IdsSize(), 2u);
        UNIT_ASSERT(config.HasMetricsLevel());
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.mutable_reset_metrics_level();
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(!DescribeTabletConfig(runtime, path).HasMetricsLevel());
    }
}

Y_UNIT_TEST(ConsumerValidationErrors) {
    auto setup = CreateSetup("CoreConsumerErrors");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_cons_err";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("bad/name");
        consumer.mutable_streaming_consumer_type();
        AssertStatus(DoAddConsumer(runtime, path, consumer), Ydb::StatusIds::BAD_REQUEST, "illegal symbols");
    }
    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("");
        consumer.mutable_streaming_consumer_type();
        AssertStatus(DoAddConsumer(runtime, path, consumer), Ydb::StatusIds::BAD_REQUEST, "empty name");
    }
    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.add_drop_consumers("missing_consumer");
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::NOT_FOUND, "drop_consumers");
    }
    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alter = request.add_alter_consumers();
        alter->set_name("missing_alter");
        alter->set_set_important(true);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::NOT_FOUND, "alter_consumers");
    }
}

Y_UNIT_TEST(MeteringDisabledRejectsExplicitMode) {
    auto setup = CreateSetup("CoreMeteringOff");
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.MutableBillingMeteringConfig()->SetEnabled(false);

    auto request = MakeCreateTopicRequest("/Root/topic_metering_off");
    request.set_metering_mode(Ydb::Topic::METERING_MODE_RESERVED_CAPACITY);
    AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::PRECONDITION_FAILED, "serverless");
}

Y_UNIT_TEST(CreateAutoPartitioningStrategiesAndCodecs) {
    auto setup = CreateSetup("CoreCreateAuto");
    auto& runtime = setup->GetRuntime();

    auto makeAuto = [&](const TString& path, Ydb::Topic::AutoPartitioningStrategy strategy) {
        auto request = MakeCreateTopicRequest(path, 2);
        auto* autoSettings = request.mutable_partitioning_settings()->mutable_auto_partitioning_settings();
        autoSettings->set_strategy(strategy);
        autoSettings->mutable_partition_write_speed()->set_up_utilization_percent(75);
        autoSettings->mutable_partition_write_speed()->set_down_utilization_percent(25);
        autoSettings->mutable_partition_write_speed()->mutable_stabilization_window()->set_seconds(45);
        request.mutable_partitioning_settings()->set_max_active_partitions(8);
        request.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);
        request.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_GZIP);
        request.set_partition_write_burst_messages(100);
        request.set_partition_total_read_speed_bytes_per_second(5000);
        request.set_partition_total_read_speed_messages_per_second(50);
        request.set_partition_read_without_consumer_speed_bytes_per_second(100);
        request.set_partition_read_without_consumer_speed_messages_per_second(10);
        request.set_metrics_level(2);
        return request;
    };

    AssertStatus(
        DoCreate(runtime, makeAuto("/Root/topic_auto_up_down", Ydb::Topic::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN)),
        Ydb::StatusIds::SUCCESS);
    AssertStatus(
        DoCreate(runtime, makeAuto("/Root/topic_auto_paused", Ydb::Topic::AUTO_PARTITIONING_STRATEGY_PAUSED)),
        Ydb::StatusIds::SUCCESS);

    {
        auto config = DescribeTabletConfig(runtime, "/Root/topic_auto_up_down");
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrPQ::TPQTabletConfig::TPartitionStrategyType_Name(config.GetPartitionStrategy().GetPartitionStrategyType()),
            NKikimrPQ::TPQTabletConfig::TPartitionStrategyType_Name(
                NKikimrPQ::TPQTabletConfig::CAN_SPLIT_AND_MERGE));
        UNIT_ASSERT_VALUES_EQUAL(config.GetCodecs().IdsSize(), 2u);
        UNIT_ASSERT(config.HasMetricsLevel());
        auto part = config.GetPartitionConfig();
        UNIT_ASSERT_VALUES_EQUAL(part.GetBurstSizeInMessages(), 100u);
        UNIT_ASSERT_VALUES_EQUAL(part.GetReadSpeedInBytesPerSecond(), 5000u);
    }
}

Y_UNIT_TEST(CreateAttributeEdgeCases) {
    auto setup = CreateSetup("CoreAttrEdges");
    auto& runtime = setup->GetRuntime();

    {
        auto request = MakeCreateTopicRequest("/Root/topic_attr_empty_bools");
        auto& attrs = *request.mutable_attributes();
        attrs["_allow_unauthenticated_read"] = "";
        attrs["_allow_unauthenticated_write"] = "";
        attrs["_abc_id"] = "";
        attrs["_max_partition_storage_size"] = "";
        attrs["_message_group_seqno_retention_period_ms"] = "";
        attrs["_max_partition_message_groups_seqno_stored"] = "";
        attrs["_sqs_export_metrics"] = "";
        attrs["_timestamp_type"] = "";
        attrs["_partitions_per_tablet"] = "2";
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);
        auto config = DescribeTabletConfig(runtime, "/Root/topic_attr_empty_bools");
        UNIT_ASSERT(config.GetRequireAuthRead());
        UNIT_ASSERT(config.GetRequireAuthWrite());
        UNIT_ASSERT_VALUES_EQUAL(config.GetAbcId(), 0u);
        UNIT_ASSERT(config.GetSqsExportMetrics());
    }

    {
        auto request = MakeCreateTopicRequest("/Root/topic_attr_ts_log");
        (*request.mutable_attributes())["_timestamp_type"] = "LogAppendTime";
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    {
        // Retention too large for default limits.
        auto request = MakeCreateTopicRequest("/Root/topic_bad_retention");
        request.mutable_retention_period()->set_seconds(1); // too small vs typical limits? or use huge
        // Use a clearly invalid retention of Max i32 seconds may still fit synthetic limits;
        // use negative via CheckRetentionPeriod path:
        request.mutable_retention_period()->set_seconds(-5);
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::BAD_REQUEST, "retention_period");
    }

    {
        auto request = MakeCreateTopicRequest("/Root/topic_bad_codec");
        request.mutable_supported_codecs()->add_codecs(static_cast<Ydb::Topic::Codec>(0));
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::BAD_REQUEST, "Unknown codec");
    }

    {
        auto request = MakeCreateTopicRequest("/Root/topic_neg_parts");
        request.mutable_partitioning_settings()->set_min_active_partitions(-3);
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::BAD_REQUEST, "positive");
    }
}

Y_UNIT_TEST(AlterSharedDlqSetDeleteAndEmptyMoveRejected) {
    auto setup = CreateSetup("CoreAlterDlqActions");
    auto& runtime = setup->GetRuntime();
    AssertStatus(DoCreate(runtime, MakeCreateTopicRequest("/Root/dlq_set")), Ydb::StatusIds::SUCCESS);
    const TString path = "/Root/topic_dlq_actions";

    {
        auto request = MakeCreateTopicRequest(path);
        request.clear_consumers();
        auto* consumer = request.add_consumers();
        consumer->set_name("shared_c");
        auto* type = consumer->mutable_shared_consumer_type();
        type->mutable_dead_letter_policy()->set_enabled(true);
        type->mutable_dead_letter_policy()->mutable_move_action()->set_dead_letter_queue("dlq_set");
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alter = request.add_alter_consumers();
        alter->set_name("shared_c");
        auto* policy = alter->mutable_alter_shared_consumer_type()->mutable_alter_dead_letter_policy();
        policy->mutable_set_move_action()->set_dead_letter_queue("");
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::BAD_REQUEST, "cannot be empty");
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alter = request.add_alter_consumers();
        alter->set_name("shared_c");
        auto* policy = alter->mutable_alter_shared_consumer_type()->mutable_alter_dead_letter_policy();
        policy->mutable_set_delete_action();
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
        auto config = DescribeTabletConfig(runtime, path);
        const auto* c = NPQ::GetConsumer(config, "shared_c");
        UNIT_ASSERT(c);
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(c->GetDeadLetterPolicy()),
            NKikimrPQ::TPQTabletConfig::EDeadLetterPolicy_Name(
                NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE));
    }
}

Y_UNIT_TEST(AlterWriteBurstAndRetentionAndClearQuotas) {
    auto setup = CreateSetup("CoreAlterBurstRetention");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_burst_ret";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.set_set_partition_write_speed_bytes_per_second(0); // default speed
        request.set_set_partition_write_burst_bytes(0); // equals write speed
        request.mutable_set_retention_period()->set_seconds(7200);
        request.set_set_partition_write_speed_messages_per_second(0);
        request.set_set_partition_write_burst_messages(0);
        request.set_set_partition_total_read_speed_bytes_per_second(0);
        request.set_set_partition_total_read_speed_messages_per_second(0);
        request.set_set_partition_read_without_consumer_speed_bytes_per_second(0);
        request.set_set_partition_read_without_consumer_speed_messages_per_second(0);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
        auto part = DescribePartitionConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(part.GetLifetimeSeconds(), 7200u);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.mutable_set_retention_period()->set_seconds(-1);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::BAD_REQUEST, "retention_period");
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.set_set_partition_write_burst_messages(-2);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::BAD_REQUEST, "partition_write_burst_messages");
    }
}

Y_UNIT_TEST(ConsumerReadQuotaAndAvailabilityPeriod) {
    auto setup = CreateSetup("CoreConsumerQuota");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_cons_quota";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("quota_c");
        consumer.mutable_streaming_consumer_type();
        consumer.set_read_speed_bytes_per_second(1000);
        consumer.set_read_speed_messages_per_second(20);
        consumer.mutable_availability_period()->set_seconds(30);
        (*consumer.mutable_attributes())["_version"] = "7";
        AssertStatus(DoAddConsumer(runtime, path, consumer), Ydb::StatusIds::SUCCESS);
        auto config = DescribeTabletConfig(runtime, path);
        const auto* c = NPQ::GetConsumer(config, "quota_c");
        UNIT_ASSERT(c);
        UNIT_ASSERT_VALUES_EQUAL(c->GetVersion(), 7u);
        UNIT_ASSERT_VALUES_EQUAL(c->GetAvailabilityPeriodMs(), 30000u);
        const auto* quota = NPQ::GetReadQuota(config, "quota_c");
        UNIT_ASSERT(quota);
        UNIT_ASSERT_VALUES_EQUAL(quota->GetSpeedInBytesPerSecond(), 1000u);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alter = request.add_alter_consumers();
        alter->set_name("quota_c");
        alter->set_set_read_speed_bytes_per_second(0);
        alter->set_set_read_speed_messages_per_second(0);
        alter->mutable_reset_availability_period();
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::Consumer consumer;
        consumer.set_name("bad_quota");
        consumer.mutable_streaming_consumer_type();
        consumer.set_read_speed_bytes_per_second(-1);
        AssertStatus(DoAddConsumer(runtime, path, consumer), Ydb::StatusIds::BAD_REQUEST, "read_speed_bytes");
    }
}

Y_UNIT_TEST(MeteringEnabledCreateModes) {
    auto setup = CreateMeteringSetup("CoreMeteringModes");
    auto& runtime = setup->GetRuntime();

    {
        auto request = MakeCreateTopicRequest("/Root/topic_metering_default");
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);
        auto config = DescribeTabletConfig(runtime, "/Root/topic_metering_default");
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(config.GetMeteringMode()),
            NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(
                NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS));
    }
    {
        auto request = MakeCreateTopicRequest("/Root/topic_metering_ru");
        request.set_metering_mode(Ydb::Topic::METERING_MODE_REQUEST_UNITS);
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);
    }
    {
        auto request = MakeCreateTopicRequest("/Root/topic_metering_rc");
        request.set_metering_mode(Ydb::Topic::METERING_MODE_RESERVED_CAPACITY);
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);
        Ydb::Topic::AlterTopicRequest alter;
        alter.set_path("/Root/topic_metering_rc");
        alter.set_set_metering_mode(Ydb::Topic::METERING_MODE_REQUEST_UNITS);
        AssertStatus(DoAlter(runtime, alter), Ydb::StatusIds::SUCCESS);
        auto config = DescribeTabletConfig(runtime, "/Root/topic_metering_rc");
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(config.GetMeteringMode()),
            NKikimrPQ::TPQTabletConfig::EMeteringMode_Name(
                NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS));
    }
}

Y_UNIT_TEST(AlterAutoPartitioningScaleUpAndDisable) {
    auto setup = CreateSetup("CoreAlterAutoScale");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_auto_scale_alter";

    {
        auto request = MakeCreateTopicRequest(path, 2);
        auto* autoSettings = request.mutable_partitioning_settings()->mutable_auto_partitioning_settings();
        autoSettings->set_strategy(Ydb::Topic::AUTO_PARTITIONING_STRATEGY_SCALE_UP);
        autoSettings->mutable_partition_write_speed()->set_up_utilization_percent(80);
        autoSettings->mutable_partition_write_speed()->set_down_utilization_percent(20);
        autoSettings->mutable_partition_write_speed()->mutable_stabilization_window()->set_seconds(30);
        request.mutable_partitioning_settings()->set_max_active_partitions(4);
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* settings = request.mutable_alter_partitioning_settings();
        settings->set_set_min_active_partitions(2);
        settings->set_set_max_active_partitions(6);
        auto* alterAuto = settings->mutable_alter_auto_partitioning_settings();
        alterAuto->set_set_strategy(Ydb::Topic::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN);
        alterAuto->mutable_set_partition_write_speed()->set_set_up_utilization_percent(70);
        alterAuto->mutable_set_partition_write_speed()->set_set_down_utilization_percent(30);
        alterAuto->mutable_set_partition_write_speed()->mutable_set_stabilization_window()->set_seconds(40);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionStrategy().GetMinPartitionCount(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionStrategy().GetMaxPartitionCount(), 6u);
        UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionStrategy().GetScaleThresholdSeconds(), 40u);
    }

    {
        // SchemeShard rejects flipping an active strategy to DISABLED via alter;
        // PAUSED is the supported "stop scaling" path.
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alterAuto = request.mutable_alter_partitioning_settings()
            ->mutable_alter_auto_partitioning_settings();
        alterAuto->set_set_strategy(Ydb::Topic::AUTO_PARTITIONING_STRATEGY_PAUSED);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.mutable_alter_partitioning_settings()->set_set_min_active_partitions(-1);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::BAD_REQUEST, "non-negative");
    }
}

Y_UNIT_TEST(AlterConsumerAttributesAndCodecs) {
    auto setup = CreateSetup("CoreAlterConsAttrs");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_alter_cons_attrs";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alter = request.add_alter_consumers();
        alter->set_name("user");
        alter->set_set_important(false);
        alter->mutable_set_read_from()->set_seconds(10);
        alter->mutable_set_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);
        (*alter->mutable_alter_attributes())["_version"] = "3";
        alter->mutable_set_availability_period()->set_seconds(15);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
        auto config = DescribeTabletConfig(runtime, path);
        const auto* c = NPQ::GetConsumer(config, "user");
        UNIT_ASSERT(c);
        UNIT_ASSERT_VALUES_EQUAL(c->GetVersion(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(c->GetAvailabilityPeriodMs(), 15000u);
    }
}

Y_UNIT_TEST(AlterRetentionStorageMb) {
    auto setup = CreateSetup("CoreAlterRetentionMb");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_retention_mb";
    CreateTopic(runtime, path);

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.set_set_retention_storage_mb(10);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
        auto part = DescribePartitionConfig(runtime, path);
        UNIT_ASSERT_VALUES_EQUAL(part.GetStorageLimitBytes(), 10ull * 1024 * 1024);
    }
    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.set_set_retention_storage_mb(0);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
        auto part = DescribePartitionConfig(runtime, path);
        UNIT_ASSERT(!part.HasStorageLimitBytes() || part.GetStorageLimitBytes() == 0);
    }
}

Y_UNIT_TEST(DropIfExistsSucceedsForMissing) {
    auto setup = CreateSetup("CoreDropIfExists");
    auto& runtime = setup->GetRuntime();
    AssertStatus(DoDrop(runtime, "/Root/missing_drop_if", "/Root", /*ifExists=*/true), Ydb::StatusIds::SUCCESS);
}

Y_UNIT_TEST(AlterRejectsNegativeSpeedsAndHugePartitions) {
    auto setup = CreateSetup("CoreAlterNegSpeeds");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_alter_neg_speeds";
    CreateTopic(runtime, path);

    auto expectBad = [&](auto&& mutate, const TString& needle) {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        mutate(request);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::BAD_REQUEST, needle);
    };

    expectBad([](auto& r) {
        r.mutable_alter_partitioning_settings()->set_set_min_active_partitions(
            static_cast<i64>(Max<ui32>()));
    }, "less than");

    expectBad([](auto& r) {
        r.set_set_partition_total_read_speed_bytes_per_second(-1);
    }, "partition_total_read_speed_bytes");

    expectBad([](auto& r) {
        r.set_set_partition_total_read_speed_messages_per_second(-1);
    }, "partition_total_read_speed_messages");

    expectBad([](auto& r) {
        r.set_set_partition_read_without_consumer_speed_bytes_per_second(-1);
    }, "partition_read_without_consumer_speed_bytes");

    expectBad([](auto& r) {
        r.set_set_partition_read_without_consumer_speed_messages_per_second(-1);
    }, "partition_read_without_consumer_speed_messages");

    expectBad([](auto& r) {
        r.set_set_partition_write_speed_messages_per_second(
            static_cast<i64>(DEFAULT_PARTITION_WRITE_SPEED_MESSAGES_PER_SECOND) + 1);
    }, "greater than");

    expectBad([](auto& r) {
        r.set_set_partition_write_burst_messages(
            static_cast<i64>(DEFAULT_PARTITION_WRITE_SPEED_MESSAGES_PER_SECOND) + 1);
    }, "greater than");

    expectBad([](auto& r) {
        r.mutable_set_supported_codecs()->add_codecs(static_cast<Ydb::Topic::Codec>(0));
    }, "Unknown codec");

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.set_set_content_based_deduplication(true);
        request.set_set_partition_total_read_speed_bytes_per_second(1111);
        request.set_set_partition_total_read_speed_messages_per_second(22);
        request.set_set_partition_read_without_consumer_speed_bytes_per_second(333);
        request.set_set_partition_read_without_consumer_speed_messages_per_second(4);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
        auto config = DescribeTabletConfig(runtime, path);
        UNIT_ASSERT(config.GetContentBasedDeduplication());
        auto part = config.GetPartitionConfig();
        UNIT_ASSERT_VALUES_EQUAL(part.GetReadSpeedInBytesPerSecond(), 1111u);
        UNIT_ASSERT(NPQ::GetReadQuota(config, NPQ::CLIENTID_WITHOUT_CONSUMER));
    }
}

Y_UNIT_TEST(AlterDlqMoveActionRequiresExistingMove) {
    auto setup = CreateSetup("CoreAlterDlqMoveReq");
    auto& runtime = setup->GetRuntime();
    AssertStatus(DoCreate(runtime, MakeCreateTopicRequest("/Root/dlq_move_req")), Ydb::StatusIds::SUCCESS);
    const TString path = "/Root/topic_dlq_move_req";

    {
        auto request = MakeCreateTopicRequest(path);
        request.clear_consumers();
        auto* consumer = request.add_consumers();
        consumer->set_name("shared_c");
        auto* type = consumer->mutable_shared_consumer_type();
        type->mutable_dead_letter_policy()->set_enabled(true);
        type->mutable_dead_letter_policy()->mutable_delete_action();
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alter = request.add_alter_consumers();
        alter->set_name("shared_c");
        auto* policy = alter->mutable_alter_shared_consumer_type()->mutable_alter_dead_letter_policy();
        policy->mutable_alter_move_action()->set_set_dead_letter_queue("dlq_move_req");
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::BAD_REQUEST, "Cannot alter move action");
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alter = request.add_alter_consumers();
        alter->set_name("shared_c");
        auto* policy = alter->mutable_alter_shared_consumer_type()->mutable_alter_dead_letter_policy();
        policy->mutable_set_move_action()->set_dead_letter_queue("dlq_move_req");
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alter = request.add_alter_consumers();
        alter->set_name("shared_c");
        auto* policy = alter->mutable_alter_shared_consumer_type()->mutable_alter_dead_letter_policy();
        policy->mutable_alter_move_action()->set_set_dead_letter_queue("");
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::BAD_REQUEST, "cannot be empty");
    }
}

Y_UNIT_TEST(AlterEnableAutopartitioningAndServiceConsumerGuards) {
    auto setup = CreateSetup("CoreAlterEnableAuto");
    auto& runtime = setup->GetRuntime();
    const TString path = "/Root/topic_enable_auto";

    {
        auto request = MakeCreateTopicRequest(path);
        (*request.mutable_attributes())["_cleanup_policy"] = "compact";
        AssertStatus(DoCreate(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alterAuto = request.mutable_alter_partitioning_settings()
            ->mutable_alter_auto_partitioning_settings();
        alterAuto->set_set_strategy(Ydb::Topic::AUTO_PARTITIONING_STRATEGY_SCALE_UP);
        request.mutable_alter_partitioning_settings()->set_set_max_active_partitions(4);
        alterAuto->mutable_set_partition_write_speed()->set_set_up_utilization_percent(70);
        alterAuto->mutable_set_partition_write_speed()->set_set_down_utilization_percent(20);
        alterAuto->mutable_set_partition_write_speed()->mutable_set_stabilization_window()->set_seconds(30);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SUCCESS);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.mutable_alter_partitioning_settings()->set_set_max_active_partitions(-1);
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::BAD_REQUEST, "non-negative");
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.mutable_alter_partitioning_settings()->set_set_max_active_partitions(
            static_cast<i64>(Max<ui32>()));
        AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::BAD_REQUEST, "less than");
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alterAuto = request.mutable_alter_partitioning_settings()
            ->mutable_alter_auto_partitioning_settings();
        alterAuto->set_set_strategy(static_cast<Ydb::Topic::AutoPartitioningStrategy>(999));
        auto result = DoAlter(runtime, request);
        UNIT_ASSERT(result);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        request.add_drop_consumers(TString{NPQ::CLIENTID_COMPACTION_CONSUMER});
        AssertStatus(
            DoAlter(runtime, request),
            Ydb::StatusIds::BAD_REQUEST,
            "Cannot drop service consumer");
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        request.set_path(path);
        auto* alter = request.add_alter_consumers();
        alter->set_name(TString{NPQ::CLIENTID_COMPACTION_CONSUMER});
        alter->set_set_important(true);
        AssertStatus(
            DoAlter(runtime, request),
            Ydb::StatusIds::BAD_REQUEST,
            "Cannot alter service consumer");
    }
}

Y_UNIT_TEST(AlterIfExistsMissingIsSuccess) {
    auto setup = CreateSetup("CoreAlterIfExists");
    auto& runtime = setup->GetRuntime();
    Ydb::Topic::AlterTopicRequest request;
    request.set_path("/Root/missing_alter_if_exists");
    request.set_set_retention_storage_mb(1);
    AssertStatus(
        DoAlter(runtime, request, "/Root", /*prepareOnly=*/false, /*ifExists=*/true),
        Ydb::StatusIds::SUCCESS);
}

Y_UNIT_TEST(AlterMissingWithoutIfExistsIsSchemeError) {
    auto setup = CreateSetup("CoreAlterMissing");
    auto& runtime = setup->GetRuntime();
    Ydb::Topic::AlterTopicRequest request;
    request.set_path("/Root/missing_alter_strict");
    request.set_set_retention_storage_mb(1);
    AssertStatus(DoAlter(runtime, request), Ydb::StatusIds::SCHEME_ERROR);
}

} // Y_UNIT_TEST_SUITE(SchemaOps)

} // namespace NKikimr::NPQ::NSchema
