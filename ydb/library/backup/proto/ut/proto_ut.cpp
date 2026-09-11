#include <ydb/library/backup/proto/proto.h>

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>
#include <google/protobuf/unknown_field_set.h>

namespace NYdb::NBackup {

Y_UNIT_TEST_SUITE(BackupProto) {
    Y_UNIT_TEST(ParseUnknownFields) {
        Ydb::Topic::CreateTopicRequest topic;
        UNIT_ASSERT(ParseProto(R"(
            path: "/Root/Topic"
            future_limit: 1048576
            future_settings { enabled: true }
            consumers {
                name: "consumer"
                future_setting: true
                read_from { seconds: 42 future_timestamp_setting: 1 }
            }
            999: 100
        )", topic));

        UNIT_ASSERT_VALUES_EQUAL(topic.path(), "/Root/Topic");
        UNIT_ASSERT_VALUES_EQUAL(topic.consumers_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(topic.consumers(0).name(), "consumer");
        UNIT_ASSERT_VALUES_EQUAL(topic.consumers(0).read_from().seconds(), 42);
    }

    Y_UNIT_TEST(RejectInvalidKnownFields) {
        for (const TString text : {
            R"(retention_storage_mb: "invalid")",
            R"(retention_period { seconds: "invalid" })",
            R"(metering_mode: FUTURE_METERING_MODE)",
        }) {
            Ydb::Topic::CreateTopicRequest topic;
            UNIT_ASSERT_C(!ParseProto(text, topic), text);
        }
    }

    Y_UNIT_TEST(RejectMalformedUnknownFields) {
        for (const TString text : {
            "future_settings { enabled:",
            "future_settings { enabled: true",
            "future_limit:",
        }) {
            Ydb::Topic::CreateTopicRequest topic;
            UNIT_ASSERT_C(!ParseProto(text, topic), text);
        }
    }

    Y_UNIT_TEST(PrintUnknownFields) {
        Ydb::Table::CreateTableRequest table;
        table.set_path("/Root/Table");
        auto* index = table.add_indexes();
        index->set_name("vector_index");
        auto* settings = index->mutable_global_vector_kmeans_tree_index()->mutable_vector_settings();
        settings->set_clusters(8);
        table.GetReflection()->MutableUnknownFields(&table)->AddVarint(999, 100);
        settings->GetReflection()->MutableUnknownFields(settings)->AddVarint(999, 1);
        settings->GetReflection()->MutableUnknownFields(settings)->AddLengthDelimited(1000, "future settings");

        // Emulate unknown fields arriving in a binary description from another version.
        Ydb::Table::CreateTableRequest received;
        UNIT_ASSERT(received.ParseFromString(table.SerializeAsString()));

        TString text;
        UNIT_ASSERT(PrintProto(received, text));
        Ydb::Table::CreateTableRequest restored;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(text, &restored));
        UNIT_ASSERT_VALUES_EQUAL(restored.path(), "/Root/Table");
        UNIT_ASSERT_VALUES_EQUAL(restored.indexes_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(restored.indexes(0).name(), "vector_index");
        UNIT_ASSERT_VALUES_EQUAL(restored.indexes(0).global_vector_kmeans_tree_index().vector_settings().clusters(), 8);

        UNIT_ASSERT_VALUES_EQUAL(received.GetReflection()->GetUnknownFields(received).field_count(), 1);
        const auto& receivedSettings = received.indexes(0).global_vector_kmeans_tree_index().vector_settings();
        UNIT_ASSERT_VALUES_EQUAL(receivedSettings.GetReflection()->GetUnknownFields(receivedSettings).field_count(), 2);
    }
}

} // namespace NYdb::NBackup
