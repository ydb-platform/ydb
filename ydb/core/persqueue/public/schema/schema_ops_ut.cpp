#include "schema_ut_helpers.h"

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

} // Y_UNIT_TEST_SUITE(SchemaOps)

} // namespace NKikimr::NPQ::NSchema
