#include <library/cpp/testing/unittest/registar.h>
#include "json_proto_conversion.h"
#include <ydb/public/api/protos/draft/ymq.pb.h>

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

Y_UNIT_TEST(NlohmannJsonToProtoArray) {
  {
    Ydb::DataStreams::V1::PutRecordsRequest message;
    nlohmann::json jsonValue;
    jsonValue["StreamName"] = "stream";
    auto& records = jsonValue["Records"];
    nlohmann::json record;
    record["Data"] = nlohmann::json::binary({123,34,116,105,99,107,101,114,
                                             83,121,109,98,111,108,34,58,
                                             34,66,82,75,46,65,34,44,
                                             34,116,114,97,100,101,84,121,
                                             112,101,34,58,34,83,69,76,
                                             76,34,44,34,112,114,105,99,
                                             101,34,58,50,53,49,54,50,
                                             48,46,49,49,44,34,113,117,
                                             97,110,116,105,116,121,34,58,
                                             51,56,50,52,44,34,105,100,
                                             34,58,54,125}, 42);
    record["ExplicitHashKey"] = "exp0";
    record["PartitionKey"] = "part0";
    records.push_back(record);
    record["Data"] = nlohmann::json::binary({123,34,116,105,99,107,101,114,
                                             83,121,109,98,111,108,34,58,
                                             34,66,82,75,46,65,34,44,
                                             34,116,114,97,100,101,84,121,
                                             112,101,34,58,34,83,69,76,
                                             76,34,44,34,112,114,105,99,
                                             101,34,58,50,53,49,54,50,
                                             48,46,49,49,44,34,113,117,
                                             97,110,116,105,116,121,34,58,
                                             51,49,50,52,44,34,105,100,
                                             34,58,50,125}, 42);
    record["ExplicitHashKey"] = "exp1";
    record["PartitionKey"] = "part1";
    records.push_back(record);
    record["Data"] = nlohmann::json::binary({116,105,99,107,101,114,83,121,
                                             109,98,111,108,66,82,75,46,
                                             65,116,114,97,100,101,84,121,
                                             112,101,83,69,76,76,112,114,
                                             105,99,101,50,53,49,54,50,
                                             48,46,0,0,113,117,97,110,
                                             116,105,116,121,51}, 42);
    record["ExplicitHashKey"] = "exp2";
    record["PartitionKey"] = "part2";
    records.push_back(record);
    NKikimr::NHttpProxy::NlohmannJsonToProto(jsonValue, &message);

    UNIT_ASSERT_VALUES_EQUAL(message.stream_name(), "stream");

    UNIT_ASSERT_VALUES_EQUAL(message.records(0).explicit_hash_key(), "exp0");
    UNIT_ASSERT_VALUES_EQUAL(message.records(0).partition_key(), "part0");
    UNIT_ASSERT_VALUES_EQUAL(message.records(0).data(),
        "{\"tickerSymbol\":\"BRK.A\",\"tradeType\":\"SELL\",\"price\":251620.11,\"quantity\":3824,\"id\":6}");
    UNIT_ASSERT_VALUES_EQUAL(message.records(1).explicit_hash_key(), "exp1");
    UNIT_ASSERT_VALUES_EQUAL(message.records(1).partition_key(), "part1");
    UNIT_ASSERT_VALUES_EQUAL(message.records(1).data(),
        "{\"tickerSymbol\":\"BRK.A\",\"tradeType\":\"SELL\",\"price\":251620.11,\"quantity\":3124,\"id\":2}");
    // This one last record is just an array of bytes with 0 bytes in it
    UNIT_ASSERT_VALUES_EQUAL(message.records(2).explicit_hash_key(), "exp2");
    UNIT_ASSERT_VALUES_EQUAL(message.records(2).partition_key(), "part2");
    std::string binaryWithNull{'t','i','c','k','e','r','S','y','m','b','o','l',
                               'B','R','K','.','A','t','r','a','d','e','T','y',
                               'p','e','S','E','L','L','p','r','i','c','e','2',
                               '5','1','6','2','0','.','\0','\0','q','u','a','n',
                               't','i','t','y','3'};
    UNIT_ASSERT_VALUES_EQUAL(message.records(2).data().size(), binaryWithNull.size());
    for (size_t i = 0; i < binaryWithNull.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(binaryWithNull[i], message.records(2).data()[i]);
    }
  }

  {
    Ydb::DataStreams::V1::PutRecordsRequest message;
    nlohmann::json jsonValue;
    jsonValue["StreamName"] = "stream";
    auto& records = jsonValue["Records"];
    nlohmann::json record;
    record["Data"] = "MTIzCg==";
    record["ExplicitHashKey"] = "exp0";
    record["PartitionKey"] = "part0";
    records.push_back(record);

    NKikimr::NHttpProxy::NlohmannJsonToProto(jsonValue, &message);

    UNIT_ASSERT_VALUES_EQUAL(message.stream_name(), "stream");

    UNIT_ASSERT_VALUES_EQUAL(message.records(0).data(), "123\n");
    UNIT_ASSERT_VALUES_EQUAL(message.records(0).explicit_hash_key(), "exp0");
    UNIT_ASSERT_VALUES_EQUAL(message.records(0).partition_key(), "part0");
  }

}

Y_UNIT_TEST(JsonToProtoMap) {
  {
    Ydb::Ymq::V1::CreateQueueRequest message;

    NJson::TJsonValue jsonObject;
    jsonObject["QueueName"] = "SampleQueueName";

    NJson::TJsonMap attributes;
    attributes["DelaySeconds"] = "900";
    attributes["MaximumMessageSize"] = "1024";

    jsonObject["Attributes"] = attributes;

    NKikimr::NHttpProxy::JsonToProto(jsonObject, &message);

    UNIT_ASSERT_VALUES_EQUAL(message.queue_name(), "SampleQueueName");
    UNIT_ASSERT_VALUES_EQUAL(message.attributes().find("DelaySeconds")->second, "900");
    UNIT_ASSERT_VALUES_EQUAL(message.attributes().find("MaximumMessageSize")->second, "1024");
  }
}

Y_UNIT_TEST(ProtoMapToJson) {
  {
    Ydb::Ymq::V1::GetQueueAttributesResult message;
    message.mutable_attributes()->insert({google::protobuf::MapPair<TString, TString>("DelaySeconds", "900")});
    message.mutable_attributes()->insert({google::protobuf::MapPair<TString, TString>("MaximumMessageSize", "1024")});

    NJson::TJsonValue jsonObject;
    NKikimr::NHttpProxy::ProtoToJson(message, jsonObject, false);

    UNIT_ASSERT_VALUES_EQUAL(jsonObject.GetMap().find("Attributes")->second.GetMap().size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(jsonObject.GetMap().find("Attributes")->second.GetMap().find("DelaySeconds")->second.GetString(), "900");
    UNIT_ASSERT_VALUES_EQUAL(jsonObject.GetMap().find("Attributes")->second.GetMap().find("MaximumMessageSize")->second.GetString(), "1024");
  }
}

Y_UNIT_TEST(NlohmannJsonToProtoMap) {
  {
    nlohmann::json jsonObject;
    jsonObject["QueueName"] = "SampleQueueName";

    nlohmann::json attributes;
    attributes["DelaySeconds"] = "900";
    attributes["MaximumMessageSize"] = "1024";
    jsonObject["Attributes"] = attributes;
    nlohmann::json record;

    Ydb::Ymq::V1::CreateQueueRequest message;
    NKikimr::NHttpProxy::NlohmannJsonToProto(jsonObject, &message);

    UNIT_ASSERT_VALUES_EQUAL(message.queue_name(), "SampleQueueName");
    UNIT_ASSERT_VALUES_EQUAL(message.attributes().find("DelaySeconds")->second, "900");
    UNIT_ASSERT_VALUES_EQUAL(message.attributes().find("MaximumMessageSize")->second, "1024");
  }
}
} // Y_UNIT_TEST_SUITE(JsonProtoConversion)
