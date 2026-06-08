#include <library/cpp/testing/unittest/registar.h>

#include <strstream>

#include <ydb/core/kafka_proxy/kafka_messages.h>

using namespace NKafka;

void Print(std::string& sb);

static constexpr size_t BUFFER_SIZE = 1 << 16;

Y_UNIT_TEST_SUITE(Serialization) {

Y_UNIT_TEST(RequestHeader) {
    TKafkaWriteBuffer sb(BUFFER_SIZE);

    TRequestHeaderData value;

    value.RequestApiKey = 3;
    value.RequestApiVersion = 7;
    value.CorrelationId = 11;
    value.ClientId = { "clientId-value" };

    TKafkaWritable writable(sb);
    value.Write(writable, 1);

    TRequestHeaderData result;
    TKafkaReadable readable(sb.GetFrontBuffer());
    result.Read(readable, 1);

    UNIT_ASSERT_EQUAL(result.RequestApiKey, 3);
    UNIT_ASSERT_EQUAL(result.RequestApiVersion, 7);
    UNIT_ASSERT_EQUAL(result.CorrelationId, 11);
    UNIT_ASSERT_EQUAL(*result.ClientId, "clientId-value");
}

Y_UNIT_TEST(ResponseHeader) {
    TKafkaWriteBuffer sb(BUFFER_SIZE);

    TResponseHeaderData value;

    value.CorrelationId = 13;

    TKafkaWritable writable(sb);
    value.Write(writable, 0);

    TKafkaReadable readable(sb.GetFrontBuffer());
    TResponseHeaderData result;
    result.Read(readable, 0);

    UNIT_ASSERT_EQUAL(result.CorrelationId, 13);
}

Y_UNIT_TEST(ApiVersionsRequest) {
    TKafkaWriteBuffer sb(BUFFER_SIZE);

    TApiVersionsRequestData value;

    value.ClientSoftwareName = { "apache-kafka-java" };
    value.ClientSoftwareVersion = { "3.4.0" };

    TKafkaWritable writable(sb);
    value.Write(writable, 3);

    TKafkaReadable readable(sb.GetFrontBuffer());
    TApiVersionsRequestData result;
    result.Read(readable, 3);

    UNIT_ASSERT_EQUAL(*result.ClientSoftwareName, "apache-kafka-java");
    UNIT_ASSERT_EQUAL(*result.ClientSoftwareVersion, "3.4.0");
}

Y_UNIT_TEST(ApiVersionsResponse) {
    TString longString = "long-string-value-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    TKafkaWriteBuffer sb(BUFFER_SIZE);

    TApiVersionsResponseData value;

    value.ErrorCode = 7;

    {
        TApiVersionsResponseData::TApiVersion version;
        version.ApiKey = 11;
        version.MinVersion = 13;
        version.MaxVersion = 17;

        value.ApiKeys.push_back(version);
    }
    {
        TApiVersionsResponseData::TApiVersion version;
        version.ApiKey = 33;
        version.MinVersion = 37;
        version.MaxVersion = 41;

        value.ApiKeys.push_back(version);
    }

    TApiVersionsResponseData::TFinalizedFeatureKey finalizeFeature;
    finalizeFeature.Name = { longString };
    finalizeFeature.MaxVersionLevel = 19;
    finalizeFeature.MinVersionLevel = 23;

    value.FinalizedFeatures.push_back(finalizeFeature);
    value.FinalizedFeaturesEpoch = 29;

    value.ThrottleTimeMs = 31;
    value.ZkMigrationReady = true;

    TKafkaWritable writable(sb);
    value.Write(writable, 3);

    TKafkaReadable readable(sb.GetFrontBuffer());
    TApiVersionsResponseData result;
    result.Read(readable, 3);

    UNIT_ASSERT_EQUAL(result.ErrorCode, 7);
    UNIT_ASSERT_EQUAL(result.ApiKeys.size(), 2ul);
    UNIT_ASSERT_EQUAL(result.ApiKeys[0].ApiKey, 11);
    UNIT_ASSERT_EQUAL(result.ApiKeys[0].MinVersion, 13);
    UNIT_ASSERT_EQUAL(result.ApiKeys[0].MaxVersion, 17);
    UNIT_ASSERT_EQUAL(result.ApiKeys[1].ApiKey, 33);
    UNIT_ASSERT_EQUAL(result.ApiKeys[1].MinVersion, 37);
    UNIT_ASSERT_EQUAL(result.ApiKeys[1].MaxVersion, 41);
    UNIT_ASSERT_EQUAL(result.FinalizedFeatures.size(), 1ul);
    UNIT_ASSERT_EQUAL(*result.FinalizedFeatures[0].Name, longString);
    UNIT_ASSERT_EQUAL(result.FinalizedFeatures[0].MaxVersionLevel, 19);
    UNIT_ASSERT_EQUAL(result.FinalizedFeatures[0].MinVersionLevel, 23);
    UNIT_ASSERT_EQUAL(result.FinalizedFeaturesEpoch, 29l);
    UNIT_ASSERT_EQUAL(result.ThrottleTimeMs, 31);
    UNIT_ASSERT_EQUAL(result.ZkMigrationReady, true);
}

Y_UNIT_TEST(ApiVersion_WithoutSupportedFeatures) {
    TKafkaWriteBuffer sb(BUFFER_SIZE);

    TApiVersionsResponseData value;
    size_t expectedSize = value.Size(2);

    value.SupportedFeatures.resize(1);
    value.SupportedFeatures[0].Name = "Feature name";

    size_t size = value.Size(2);

    UNIT_ASSERT_VALUES_EQUAL_C(expectedSize, size, "SupportedFeatures is not presents for 2 version");
}

Y_UNIT_TEST(ProduceRequest) {
    TKafkaWriteBuffer sb(BUFFER_SIZE);

    TProduceRequestData value;

    value.TransactionalId = { "transactional-id-value-123456" };
    value.Acks = 3;
    value.TimeoutMs = 5;
    value.TopicData.resize(2);
    value.TopicData[0].Name = "/it/is/some/topic/name";
    value.TopicData[0].PartitionData.resize(2);
    value.TopicData[0].PartitionData[0].Index = 0;
    value.TopicData[0].PartitionData[1].Index = 1;
    value.TopicData[1].Name = "/it/is/other/topic/name";
    value.TopicData[1].PartitionData.resize(1);
    value.TopicData[1].PartitionData[0].Index = 0;

    TKafkaWritable writable(sb);
    value.Write(writable, 3);


    TKafkaReadable readable(sb.GetFrontBuffer());
    TProduceRequestData result;
    result.Read(readable, 3);

    UNIT_ASSERT(result.TransactionalId);
    UNIT_ASSERT_EQUAL(*result.TransactionalId, "transactional-id-value-123456" );
    UNIT_ASSERT_EQUAL(result.Acks, 3);
    UNIT_ASSERT_EQUAL(result.TimeoutMs, 5);
    UNIT_ASSERT_EQUAL(result.TopicData.size(), 2ul);
    UNIT_ASSERT(result.TopicData[0].Name);
    UNIT_ASSERT_EQUAL(*result.TopicData[0].Name, "/it/is/some/topic/name");
    UNIT_ASSERT_EQUAL(result.TopicData[0].PartitionData.size(), 2ul);
    UNIT_ASSERT_EQUAL(result.TopicData[0].PartitionData[0].Index, 0);
    UNIT_ASSERT_EQUAL(result.TopicData[0].PartitionData[0].Records, std::nullopt);
    UNIT_ASSERT_EQUAL(result.TopicData[0].PartitionData[1].Index, 1);
    UNIT_ASSERT_EQUAL(result.TopicData[0].PartitionData[1].Records, std::nullopt);
    UNIT_ASSERT(result.TopicData[1].Name);
    UNIT_ASSERT_EQUAL(*result.TopicData[1].Name, "/it/is/other/topic/name");
    UNIT_ASSERT_EQUAL(result.TopicData[1].PartitionData.size(), 1ul);
    UNIT_ASSERT_EQUAL(result.TopicData[1].PartitionData[0].Index, 0);
    UNIT_ASSERT_EQUAL(result.TopicData[1].PartitionData[0].Records, std::nullopt);
}

struct Meta_TKafkaStruct {
    using Type = TRequestHeaderData;
    using TypeDesc = NKafka::NPrivate::TKafkaStructDesc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
    static const Type Default; // = 7;

    static constexpr TKafkaVersions PresentVersions{3, 97};
    static constexpr TKafkaVersions TaggedVersions{11, 17};
    static constexpr TKafkaVersions NullableVersions{5, 19};
    static constexpr TKafkaVersions FlexibleVersions{7, Max<TKafkaVersion>()};
};

Y_UNIT_TEST(Struct_IsDefault) {
    TRequestHeaderData value;
    UNIT_ASSERT(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaStruct>(value)); // all fields have default values

    value.RequestApiKey = 123;
    UNIT_ASSERT(!NKafka::NPrivate::IsDefaultValue<Meta_TKafkaStruct>(value)); // field changed
}

Y_UNIT_TEST(TRequestHeaderData_reference) {
    // original kafka serialized value (java implementation)
    ui8 reference[] = {0x00, 0x03, 0x00, 0x07, 0x00, 0x00, 0x00, 0x0D, 0x00, 0x10, 0x63, 0x6C, 0x69, 0x65, 0x6E, 0x74,
                       0x2D, 0x69, 0x64, 0x2D, 0x73, 0x74, 0x72, 0x69, 0x6E, 0x67, 0x00};

    TKafkaWriteBuffer sb(BUFFER_SIZE);
    TKafkaWritable writable(sb);
    TKafkaReadable readable(sb.GetFrontBuffer());

    TRequestHeaderData value;
    value.RequestApiKey = 3;
    value.RequestApiVersion = 7;
    value.CorrelationId = 13;
    value.ClientId = "client-id-string";

    value.Write(writable, 2);

    UNIT_ASSERT_EQUAL(sb.GetFrontBuffer().size(), sizeof(reference));
    for(size_t i = 0; i < sizeof(reference); ++i) {
        UNIT_ASSERT_EQUAL(*(sb.GetFrontBuffer().data() + i), reference[i]);
    }


    TRequestHeaderData result;
    result.Read(readable, 2);

    UNIT_ASSERT_EQUAL(result.RequestApiKey, 3);
    UNIT_ASSERT_EQUAL(result.RequestApiVersion, 7);
    UNIT_ASSERT_EQUAL(result.CorrelationId, 13);
    UNIT_ASSERT_EQUAL(result.ClientId, "client-id-string");
}

Y_UNIT_TEST(RequestHeader_reference) {
    ui8 reference[] = {0x00, 0x12, 0x00, 0x00, 0x7F, 0x6F, 0x6F, 0x68, 0x00, 0x0A, 0x70, 0x72, 0x6F, 0x64, 0x75, 0x63,
                     0x65, 0x72, 0x2D, 0x31};

    TKafkaWriteBuffer sb(BUFFER_SIZE);
    sb.write((char*)reference, sizeof(reference));

    TKafkaReadable readable(sb.GetFrontBuffer());
    TRequestHeaderData result;
    result.Read(readable, 1);

    UNIT_ASSERT_EQUAL(result.RequestApiKey, 0x12);
    UNIT_ASSERT_EQUAL(result.RequestApiVersion, 0x00);
    UNIT_ASSERT_EQUAL(result.ClientId, "producer-1");
}

Y_UNIT_TEST(ProduceRequestData) {
    ui8 reference[] = {0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x04, 0x00, 0x0A, 0x70, 0x72, 0x6F, 0x64, 0x75, 0x63,
                       0x65, 0x72, 0x2D, 0x31, 0x00, 0x00, 0xFF, 0xFF, 0x00, 0x00, 0x75, 0x30, 0x02, 0x08, 0x74, 0x6F,
                       0x70, 0x69, 0x63, 0x2D, 0x31, 0x02, 0x00, 0x00, 0x00, 0x00, 0xCD, 0x01, 0x00, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0xFF, 0xFF, 0xFF, 0xFF, 0x02, 0x36, 0xDD, 0x01,
                       0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01, 0x89, 0x0C, 0x95, 0xAF, 0x25, 0x00,
                       0x00, 0x01, 0x89, 0x0C, 0x95, 0xB2, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
                       0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x5C, 0x00, 0x00, 0x00, 0x06, 0x6B, 0x65,
                       0x79, 0x0E, 0x6D, 0x73, 0x67, 0x2D, 0x31, 0x2D, 0x31, 0x06, 0x06, 0x68, 0x2D, 0x31, 0x0A, 0x76,
                       0x2D, 0x31, 0x2D, 0x31, 0x06, 0x68, 0x2D, 0x32, 0x0A, 0x76, 0x2D, 0x32, 0x2D, 0x31, 0x06, 0x68,
                       0x2D, 0x33, 0x0A, 0x76, 0x2D, 0x33, 0x2D, 0x31, 0x5E, 0x00, 0xE6, 0x0B, 0x02, 0x06, 0x6B, 0x65,
                       0x79, 0x0E, 0x6D, 0x73, 0x67, 0x2D, 0x31, 0x2D, 0x32, 0x06, 0x06, 0x68, 0x2D, 0x31, 0x0A, 0x76,
                       0x2D, 0x31, 0x2D, 0x32, 0x06, 0x68, 0x2D, 0x32, 0x0A, 0x76, 0x2D, 0x32, 0x2D, 0x32, 0x06, 0x68,
                       0x2D, 0x33, 0x0A, 0x76, 0x2D, 0x33, 0x2D, 0x32, 0x5E, 0x00, 0xE8, 0x0B, 0x04, 0x06, 0x6B, 0x65,
                       0x79, 0x0E, 0x6D, 0x73, 0x67, 0x2D, 0x31, 0x2D, 0x33, 0x06, 0x06, 0x68, 0x2D, 0x31, 0x0A, 0x76,
                       0x2D, 0x31, 0x2D, 0x33, 0x06, 0x68, 0x2D, 0x32, 0x0A, 0x76, 0x2D, 0x32, 0x2D, 0x33, 0x06, 0x68,
                       0x2D, 0x33, 0x0A, 0x76, 0x2D, 0x33, 0x2D, 0x33, 0x00, 0x00, 0x00};

    TBuffer buffer((char*)reference, sizeof(reference));
    TKafkaReadable readable(buffer);

    Cerr << ">>>>> Buffer size: " << buffer.Size() << Endl;

    TRequestHeaderData header;
    header.Read(readable, 2);

    TProduceRequestData result;
    result.Read(readable, 9);

    UNIT_ASSERT_EQUAL(result.Acks, -1);
    UNIT_ASSERT_EQUAL(result.TimeoutMs, 30000);

    auto& r0 = *result.TopicData[0].PartitionData[0].Records;
    UNIT_ASSERT_EQUAL(r0.BaseOffset, 0);
    UNIT_ASSERT_EQUAL(r0.BatchLength, 192);
    UNIT_ASSERT_EQUAL(r0.PartitionLeaderEpoch, -1);
    UNIT_ASSERT_EQUAL(r0.Magic, 2);
    UNIT_ASSERT_EQUAL(r0.Crc, 920453412);
    UNIT_ASSERT_EQUAL(r0.Attributes, 0);
    UNIT_ASSERT_EQUAL(r0.LastOffsetDelta, 2);
    UNIT_ASSERT_EQUAL(r0.BaseTimestamp, 1688133283621);
    UNIT_ASSERT_EQUAL(r0.MaxTimestamp, 1688133284377);
    UNIT_ASSERT_EQUAL(r0.ProducerId, 1);
    UNIT_ASSERT_EQUAL(r0.ProducerEpoch, 1);
    UNIT_ASSERT_EQUAL(r0.BaseSequence, 0);

    UNIT_ASSERT_EQUAL(r0.Records.size(), (size_t)3);

    UNIT_ASSERT_EQUAL(r0.Records[0].Key, TString("key", 3));
    UNIT_ASSERT_EQUAL(r0.Records[0].Value, TString("msg-1-1", 7));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers.size(), (size_t)3);
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers[0].Key, TString("h-1", 3));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers[0].Value, TString("v-1-1", 5));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers[1].Key, TString("h-2", 3));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers[1].Value, TString("v-2-1", 5));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers[2].Key, TString("h-3", 3));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers[2].Value, TString("v-3-1", 5));

    UNIT_ASSERT_EQUAL(r0.Records[1].Key, TString("key", 3));
    UNIT_ASSERT_EQUAL(r0.Records[1].Value, TString("msg-1-2", 7));
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers.size(), (size_t)3);
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers[0].Key, TString("h-1", 3));
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers[0].Value, TString("v-1-2", 5));
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers[1].Key, TString("h-2", 3));
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers[1].Value, TString("v-2-2", 5));
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers[2].Key, TString("h-3", 3));
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers[2].Value, TString("v-3-2", 5));

    UNIT_ASSERT_EQUAL(r0.Records[2].Key, TString("key", 3));
    UNIT_ASSERT_EQUAL(r0.Records[2].Value, TString("msg-1-3", 7));
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers.size(), (size_t)3);
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers[0].Key, TString("h-1", 3));
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers[0].Value, TString("v-1-3", 5));
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers[1].Key, TString("h-2", 3));
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers[1].Value, TString("v-2-3", 5));
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers[2].Key, TString("h-3", 3));
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers[2].Value, TString("v-3-3", 5));

    TKafkaWriteBuffer sb(sizeof(reference));
    TKafkaWritable writable(sb);

    header.Write(writable, 2);
    result.Write(writable, 9);

    UNIT_ASSERT_EQUAL(sb.GetFrontBuffer().size(), sizeof(reference));
    for(size_t i = 0; i < sizeof(reference); ++i) {
        UNIT_ASSERT_EQUAL(*(sb.GetFrontBuffer().data() + i), (char)reference[i]);
    }
}

Y_UNIT_TEST(ProduceRequestData_Record_v0) {
    ui8 reference[] = {0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x05, 0x00, 0x07, 0x72, 0x64, 0x6B, 0x61, 0x66, 0x6B,
                       0x61, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x75, 0x30, 0x00, 0x00, 0x00, 0x01, 0x00, 0x12, 0x2F,
                       0x52, 0x6F, 0x6F, 0x74, 0x2F, 0x74, 0x65, 0x73, 0x74, 0x2F, 0x74, 0x6F, 0x70, 0x69, 0x63, 0x2D,
                       0x31, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x1A, 0x00, 0x00, 0x00, 0x2B, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1F, 0x20, 0x6F, 0x55, 0x26, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x05, 0x6B, 0x65, 0x79, 0x2D, 0x31, 0x00, 0x00, 0x00, 0x0C, 0x74, 0x65, 0x73, 0x74,
                       0x20, 0x6D, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65};

    TBuffer buffer((char*)reference, sizeof(reference));
    TKafkaReadable readable(buffer);

    Cerr << ">>>>> Buffer size: " << buffer.Size() << Endl;

    TRequestHeaderData header;
    header.Read(readable, 1);

    TProduceRequestData result;
    result.Read(readable, header.RequestApiVersion);

    UNIT_ASSERT_EQUAL(result.Acks, -1);
    UNIT_ASSERT_EQUAL(result.TimeoutMs, 30000);

    auto& r0 = *result.TopicData[0].PartitionData[0].Records;
    UNIT_ASSERT_EQUAL(r0.BaseOffset, 0);
    UNIT_ASSERT_EQUAL(r0.BatchLength, 0);
    UNIT_ASSERT_EQUAL(r0.PartitionLeaderEpoch, -1);
    UNIT_ASSERT_EQUAL(r0.Magic, 2);
    UNIT_ASSERT_EQUAL(r0.Crc, 0);
    UNIT_ASSERT_EQUAL(r0.Attributes, 0);
    UNIT_ASSERT_EQUAL(r0.LastOffsetDelta, 0);
    UNIT_ASSERT_EQUAL(r0.BaseTimestamp, 0);
    UNIT_ASSERT_EQUAL(r0.MaxTimestamp, -1);
    UNIT_ASSERT_EQUAL(r0.ProducerId, -1);
    UNIT_ASSERT_EQUAL(r0.ProducerEpoch, -1);
    UNIT_ASSERT_EQUAL(r0.BaseSequence, -1);

    UNIT_ASSERT_EQUAL(r0.Records.size(), (size_t)1);

    UNIT_ASSERT_EQUAL(r0.Records[0].Key, TString("key-1", 5));
    UNIT_ASSERT_EQUAL(r0.Records[0].Value, TString("test message", 12));
    UNIT_ASSERT_VALUES_EQUAL(r0.Records[0].Headers.size(), (size_t)0);
    UNIT_ASSERT_VALUES_EQUAL(r0.Records[0].OffsetDelta, 0);
    UNIT_ASSERT_VALUES_EQUAL(r0.Records[0].TimestampDelta, 0);
}

//

Y_UNIT_TEST(ProduceRequestData_Record_v0_manyMessages) {
    ui8 reference[] = { 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x05, 0x00, 0x07, 0x72, 0x64,
                       0x6B, 0x61, 0x66, 0x6B, 0x61, 0x00, 0x00, 0xFF, 0xFF, 0x00, 0x00, 0x75, 0x30, 0x02,
                       0x05, 0x61, 0x61, 0x61, 0x61, 0x02, 0x00, 0x00, 0x00, 0x00, 0x7D, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x13, 0x5F, 0x1B, 0x4F, 0x8D, 0x00, 0x00, 0xFF, 0xFF, 0xFF,
                       0xFF, 0x00, 0x00, 0x00, 0x05, 0x61, 0x61, 0x61, 0x61, 0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x00, 0x01, 0x00, 0x00, 0x00, 0x13, 0xBA, 0x6C, 0x26, 0x93, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF,
                       0x00, 0x00, 0x00, 0x05, 0x62, 0x62, 0x62, 0x62, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x02, 0x00, 0x00, 0x00, 0x13, 0x50, 0x6E, 0x03, 0xA6, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0x00,
                       0x00, 0x00, 0x05, 0x63, 0x63, 0x63, 0x63, 0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
                       0x00, 0x00, 0x00, 0x13, 0xAB, 0xF3, 0xF2, 0xEE, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00,
                       0x00, 0x05, 0x64, 0x64, 0x64, 0x64, 0x64, 0x00, 0x00, 0x00};

    TBuffer buffer((char*)reference, sizeof(reference));
    TKafkaReadable readable(buffer);

    Cerr << ">>>>> Buffer size: " << buffer.Size() << Endl;

    TRequestHeaderData header;
    header.Read(readable, 2);

    TProduceRequestData result;
    result.Read(readable, header.RequestApiVersion);

    UNIT_ASSERT_EQUAL(result.Acks, -1);
    UNIT_ASSERT_EQUAL(result.TimeoutMs, 30000);

    auto& r0 = *result.TopicData[0].PartitionData[0].Records;
    UNIT_ASSERT_EQUAL(r0.BaseOffset, 0);
    UNIT_ASSERT_EQUAL(r0.BatchLength, 0);
    UNIT_ASSERT_EQUAL(r0.PartitionLeaderEpoch, -1);
    UNIT_ASSERT_EQUAL(r0.Magic, 2);
    UNIT_ASSERT_EQUAL(r0.Crc, 0);
    UNIT_ASSERT_EQUAL(r0.Attributes, 0);
    UNIT_ASSERT_EQUAL(r0.LastOffsetDelta, 3);
    UNIT_ASSERT_EQUAL(r0.BaseTimestamp, 0);
    UNIT_ASSERT_EQUAL(r0.MaxTimestamp, -1);
    UNIT_ASSERT_EQUAL(r0.ProducerId, -1);
    UNIT_ASSERT_EQUAL(r0.ProducerEpoch, -1);
    UNIT_ASSERT_EQUAL(r0.BaseSequence, -1);

    UNIT_ASSERT_VALUES_EQUAL(r0.Records.size(), (size_t)4);

    //UNIT_ASSERT_EQUAL(r0.Records[0].Key, TString("", 0));
    UNIT_ASSERT_EQUAL(r0.Records[0].Value, TString("aaaaa", 5));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers.size(), (size_t)0);
    UNIT_ASSERT_VALUES_EQUAL(r0.Records[0].OffsetDelta, 0);
    UNIT_ASSERT_VALUES_EQUAL(r0.Records[0].TimestampDelta, 0);

    //UNIT_ASSERT_EQUAL(r0.Records[0].Key, TString("", 0));
    UNIT_ASSERT_EQUAL(r0.Records[1].Value, TString("bbbbb", 5));
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers.size(), (size_t)0);
    UNIT_ASSERT_VALUES_EQUAL(r0.Records[1].OffsetDelta, 1);
    UNIT_ASSERT_VALUES_EQUAL(r0.Records[1].TimestampDelta, 0);

    //UNIT_ASSERT_EQUAL(r0.Records[0].Key, TString("", 0));
    UNIT_ASSERT_EQUAL(r0.Records[2].Value, TString("ccccc", 5));
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers.size(), (size_t)0);
    UNIT_ASSERT_VALUES_EQUAL(r0.Records[2].OffsetDelta, 2);
    UNIT_ASSERT_VALUES_EQUAL(r0.Records[2].TimestampDelta, 0);

    //UNIT_ASSERT_EQUAL(r0.Records[0].Key, TString("", 0));
    UNIT_ASSERT_EQUAL(r0.Records[3].Value, TString("ddddd", 5));
    UNIT_ASSERT_EQUAL(r0.Records[3].Headers.size(), (size_t)0);
    UNIT_ASSERT_VALUES_EQUAL(r0.Records[3].OffsetDelta, 3);
    UNIT_ASSERT_VALUES_EQUAL(r0.Records[3].TimestampDelta, 0);
}

void Print(std::string& sb) {
    Cerr << Hex(sb.begin(), sb.end()) << Endl;
}

}
