#include <library/cpp/testing/unittest/registar.h>
#include "json_proto_conversion.h"

Y_UNIT_TEST_SUITE(JsonProtoConversion) {

Y_UNIT_TEST(JsonToProtoSingleValue) {
  {
    Ydb::DataStreams::V1::DeleteStreamRequest message;
    NJson::TJsonValue jsonValue;
    jsonValue["EnforceConsumerDeletion"] = true;
    jsonValue["StreamName"] = "stream";
    NKikimr::NHttpProxy::JsonToProto(jsonValue, &message);

    UNIT_ASSERT_VALUES_EQUAL(message.stream_name(), "stream");
    UNIT_ASSERT_VALUES_EQUAL(message.enforce_consumer_deletion(), true);
  }
  {
    Ydb::DataStreams::V1::DeleteStreamRequest message;
    NJson::TJsonValue jsonValue;
    jsonValue["EnforceConsumerDeletion"] = false;
    jsonValue["StreamName"] = "not_stream";
    NKikimr::NHttpProxy::JsonToProto(jsonValue, &message);

    UNIT_ASSERT_VALUES_EQUAL(message.stream_name(), "not_stream");
    UNIT_ASSERT_VALUES_EQUAL(message.enforce_consumer_deletion(), false);
  }
}

Y_UNIT_TEST(JsonToProtoArray) {
  {
    Ydb::DataStreams::V1::PutRecordsRequest message;
    NJson::TJsonValue jsonValue;
    jsonValue["StreamName"] = "stream";
    auto& records = jsonValue["Records"];
    NJson::TJsonValue record;
    record["Data"] = "MTIzCg==";
    record["ExplicitHashKey"] = "exp0";
    record["PartitionKey"] = "part0";
    records.AppendValue(record);
    record["Data"] = "NDU2Cg==";
    record["ExplicitHashKey"] = "exp1";
    record["PartitionKey"] = "part1";
    records.AppendValue(record);

    NKikimr::NHttpProxy::JsonToProto(jsonValue, &message);

    UNIT_ASSERT_VALUES_EQUAL(message.stream_name(), "stream");

    UNIT_ASSERT_VALUES_EQUAL(message.records(0).explicit_hash_key(), "exp0");
    UNIT_ASSERT_VALUES_EQUAL(message.records(0).partition_key(), "part0");
    UNIT_ASSERT_VALUES_EQUAL(message.records(0).data(), "123\n");
    UNIT_ASSERT_VALUES_EQUAL(message.records(1).explicit_hash_key(), "exp1");
    UNIT_ASSERT_VALUES_EQUAL(message.records(1).partition_key(), "part1");
    UNIT_ASSERT_VALUES_EQUAL(message.records(1).data(), "456\n");
  }

  {
    Ydb::DataStreams::V1::EnhancedMetrics message;
    NJson::TJsonValue jsonValue;
    const auto N{5};
    for (int i = 0; i < N; ++i) {
      auto str = TStringBuilder() << "metric_" << i;
      jsonValue["shard_level_metrics"].AppendValue(str);
    }

    NKikimr::NHttpProxy::JsonToProto(jsonValue, &message);

    for (int i = 0; i < N; ++i) {
      auto str = TStringBuilder() << "metric_" << i;
      UNIT_ASSERT_VALUES_EQUAL(message.shard_level_metrics(i), str);
    }
  }

}

} // Y_UNIT_TEST_SUITE(JsonProtoConversion)
