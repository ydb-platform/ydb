#include <library/cpp/testing/gtest/gtest.h>

#include <strstream>

#include "../kafka_messages.h"

using namespace NKafka;

void Print(std::stringstream& sb);

TEST(Serialization, RequestHeader) {
    std::stringstream sb;

    TRequestHeaderData value;

    value.requestApiKey = 3;
    value.requestApiVersion = 7;
    value.correlationId = 11;
    value.clientId = { "clientId-value" };

    TKafkaWritable writable(sb);
    value.Write(writable, 1);

    //Print(sb);

    TRequestHeaderData result;

    //sb.seekg(0);

    TKafkaReadable readable(sb);
    result.Read(readable, 1);

    EXPECT_EQ(result.requestApiKey, 3);
    EXPECT_EQ(result.requestApiVersion, 7);
    EXPECT_EQ(result.correlationId, 11);
    EXPECT_EQ(*result.clientId, "clientId-value");
}

TEST(Serialization, ResponseHeader) {
    std::stringstream sb;

    TResponseHeaderData value;

    value.correlationId = 13;

    TKafkaWritable writable(sb);
    value.Write(writable, 0);

    //Print(sb);

    TResponseHeaderData result;

    TKafkaReadable readable(sb);
    result.Read(readable, 0);

    EXPECT_EQ(result.correlationId, 13);
}

TEST(Serialization, ApiVersionsRequest) {
    std::stringstream sb;

    TApiVersionsRequestData value;

    value.clientSoftwareName = { "apache-kafka-java" };
    value.clientSoftwareVersion = { "3.4.0" };

    TKafkaWritable writable(sb);
    value.Write(writable, 3);

    //Print(sb);

    TApiVersionsRequestData result;

    TKafkaReadable readable(sb);
    result.Read(readable, 3);

    EXPECT_EQ(*result.clientSoftwareName, "apache-kafka-java");
    EXPECT_EQ(*result.clientSoftwareVersion, "3.4.0");
}

TEST(Serialization, ApiVersionsResponse) {
    TString longString = "long-string-value-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"; 

    std::stringstream sb;

    TApiVersionsResponseData value;

    value.errorCode = 7;

    {
        TApiVersionsResponseData::TApiVersion version;
        version.apiKey = 11;
        version.minVersion = 13;
        version.maxVersion = 17;

        value.apiKeys.push_back(version);
    }
    {
        TApiVersionsResponseData::TApiVersion version;
        version.apiKey = 33;
        version.minVersion = 37;
        version.maxVersion = 41;

        value.apiKeys.push_back(version);
    }

    TApiVersionsResponseData::TFinalizedFeatureKey finalizeFeature;
    finalizeFeature.name = { longString };
    finalizeFeature.maxVersionLevel = 19;
    finalizeFeature.minVersionLevel = 23;

    value.finalizedFeatures.push_back(finalizeFeature);
    value.finalizedFeaturesEpoch = 29;

    value.throttleTimeMs = 31;
    value.zkMigrationReady = true;

    TKafkaWritable writable(sb);
    value.Write(writable, 3);

    TApiVersionsResponseData result;

    TKafkaReadable readable(sb);
    result.Read(readable, 3);

    EXPECT_EQ(result.errorCode, 7);
    EXPECT_EQ(result.apiKeys.size(), 2ul);
    EXPECT_EQ(result.apiKeys[0].apiKey, 11);
    EXPECT_EQ(result.apiKeys[0].minVersion, 13);
    EXPECT_EQ(result.apiKeys[0].maxVersion, 17);
    EXPECT_EQ(result.apiKeys[1].apiKey, 33);
    EXPECT_EQ(result.apiKeys[1].minVersion, 37);
    EXPECT_EQ(result.apiKeys[1].maxVersion, 41);
    EXPECT_EQ(result.finalizedFeatures.size(), 1ul);
    EXPECT_EQ(*result.finalizedFeatures[0].name, longString);
    EXPECT_EQ(result.finalizedFeatures[0].maxVersionLevel, 19);
    EXPECT_EQ(result.finalizedFeatures[0].minVersionLevel, 23);
    EXPECT_EQ(result.finalizedFeaturesEpoch, 29l);
    EXPECT_EQ(result.throttleTimeMs, 31);
    EXPECT_EQ(result.zkMigrationReady, true);
}

TEST(Serialization, ProduceRequest) {
    char data0[] = "it-is produce data message 1";
    char data1[] = "it-is produce data other message 2";

    std::stringstream sb;

    TProduceRequestData value;

    value.transactionalId = { "transactional-id-value-123456" };
    value.acks = 3;
    value.timeoutMs = 5;
    value.topicData.resize(2);
    value.topicData[0].name = "/it/is/some/topic/name";
    value.topicData[0].partitionData.resize(2);
    value.topicData[0].partitionData[0].index = 0;
    value.topicData[0].partitionData[0].records = { TBuffer(data0, sizeof(data0)) };
    value.topicData[0].partitionData[1].index = 1;
    value.topicData[0].partitionData[1].records = {};
    value.topicData[1].name = "/it/is/other/topic/name";
    value.topicData[1].partitionData.resize(1);
    value.topicData[1].partitionData[0].index = 0;
    value.topicData[1].partitionData[0].records = { TBuffer(data1, sizeof(data1)) };

    TKafkaWritable writable(sb);
    value.Write(writable, 3);

    TProduceRequestData result;

    TKafkaReadable readable(sb);
    result.Read(readable, 3);

    EXPECT_TRUE(result.transactionalId);
    EXPECT_EQ(*result.transactionalId, "transactional-id-value-123456" );
    EXPECT_EQ(result.acks, 3);
    EXPECT_EQ(result.timeoutMs, 5);
    EXPECT_EQ(result.topicData.size(), 2ul);
    EXPECT_TRUE(result.topicData[0].name);
    EXPECT_EQ(*result.topicData[0].name, "/it/is/some/topic/name");
    EXPECT_EQ(result.topicData[0].partitionData.size(), 2ul);
    EXPECT_EQ(result.topicData[0].partitionData[0].index, 0);
    EXPECT_TRUE(result.topicData[0].partitionData[0].records);
    EXPECT_EQ(*result.topicData[0].partitionData[0].records, TBuffer(data0, sizeof(data0)));
    EXPECT_EQ(result.topicData[0].partitionData[1].index, 1);
    EXPECT_EQ(result.topicData[0].partitionData[1].records, std::nullopt);
    EXPECT_TRUE(result.topicData[1].name);
    EXPECT_EQ(*result.topicData[1].name, "/it/is/other/topic/name");
    EXPECT_EQ(result.topicData[1].partitionData.size(), 1ul);
    EXPECT_EQ(result.topicData[1].partitionData[0].index, 0);
    EXPECT_TRUE(result.topicData[1].partitionData[0].records);
    EXPECT_EQ(*result.topicData[1].partitionData[0].records, TBuffer(data1, sizeof(data1)));
}


TEST(Serialization, UnsignedVarint) {
    std::vector<ui32> values = {0, 1, 127, 128, 32191};

    for(ui32 v : values) {
        std::stringstream sb;
        TKafkaWritable writable(sb);
        TKafkaReadable redable(sb);

        writable.writeUnsignedVarint(v);
        ui32 r = redable.readUnsignedVarint();
        EXPECT_EQ(r, v);
    }
}

#define SIMPLE_HEAD(Type_, Value)                   \
    Meta_##Type_::Type value = Value;               \
    Meta_##Type_::Type result;                      \
                                                    \
    std::stringstream sb;                           \
    TKafkaWritable writable(sb);                    \
    TKafkaReadable readable(sb);                    \
                                                    \
    Y_UNUSED(readable);                             \
    Y_UNUSED(result);                               \
                                                    \
    NKafka::NPrivate::TWriteCollector collector;



struct Meta_TKafkaInt8 {
    using Type = TKafkaInt8;
    using TypeDesc = NKafka::NPrivate::TKafkaInt8Desc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
    static const Type Default; // = 7;
                
    static constexpr TKafkaVersion PresentVersionMin = 3;
    static constexpr TKafkaVersion PresentVersionMax = 97;
    static constexpr TKafkaVersion TaggedVersionMin = 11;
    static constexpr TKafkaVersion TaggedVersionMax = 17;
    static constexpr TKafkaVersion NullableVersionMin = 5;
    static constexpr TKafkaVersion NullableVersionMax = 19;
    static constexpr TKafkaVersion FlexibleVersionMin = 7;
    static constexpr TKafkaVersion FlexibleVersionMax = Max<TKafkaVersion>();
};

const Meta_TKafkaInt8::Type Meta_TKafkaInt8::Default = 7;

TEST(Serialization, TKafkaInt8_NotPresentVersion) {
    SIMPLE_HEAD(TKafkaInt8, 37);

    NKafka::NPrivate::Write<Meta_TKafkaInt8>(collector, writable, 0, value);
    sb.get();
    EXPECT_TRUE(sb.eof()); // For version 0 value is not serializable. Stream must be empty
    EXPECT_EQ(collector.NumTaggedFields, 0u);

    NKafka::NPrivate::Read<Meta_TKafkaInt8>(readable, 0, result);
    EXPECT_EQ(result, Meta_TKafkaInt8::Default); // For version 0 value is not serializable
}

TEST(Serialization, TKafkaInt8_PresentVersion_NotTaggedVersion) {
    SIMPLE_HEAD(TKafkaInt8, 37);

    NKafka::NPrivate::Write<Meta_TKafkaInt8>(collector, writable, 3, value);
    NKafka::NPrivate::Read<Meta_TKafkaInt8>(readable, 3, result);

    EXPECT_EQ(collector.NumTaggedFields, 0u);
    EXPECT_EQ(result, value); // Must read same that write
}

TEST(Serialization, TKafkaInt8_PresentVersion_TaggedVersion) {
    SIMPLE_HEAD(TKafkaInt8, 37);

    NKafka::NPrivate::Write<Meta_TKafkaInt8>(collector, writable, 11, value);
    EXPECT_EQ(collector.NumTaggedFields, 1u);

    NKafka::NPrivate::WriteTag<Meta_TKafkaInt8>(writable, 11, value);

    i32 tag = readable.readUnsignedVarint();
    EXPECT_EQ(tag, Meta_TKafkaInt8::Tag);

    ui32 size = readable.readUnsignedVarint();
    EXPECT_EQ(size, sizeof(TKafkaInt8));

    NKafka::NPrivate::ReadTag<Meta_TKafkaInt8>(readable, 11, result);
    EXPECT_EQ(result, value); // Must read same that write
}

TEST(Serialization, TKafkaInt8_PresentVersion_TaggedVersion_Default) {
    SIMPLE_HEAD(TKafkaInt8, Meta_TKafkaInt8::Default);

    NKafka::NPrivate::Write<Meta_TKafkaInt8>(collector, writable, 11, value);
    EXPECT_EQ(collector.NumTaggedFields, 0u); // not serialize default value for tagged version
}


struct Meta_TKafkaStruct {
    using Type = TRequestHeaderData;
    using TypeDesc = NKafka::NPrivate::TKafkaStructDesc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
    static const Type Default; // = 7;
                
    static constexpr TKafkaVersion PresentVersionMin = 3;
    static constexpr TKafkaVersion PresentVersionMax = 97;
    static constexpr TKafkaVersion TaggedVersionMin = 11;
    static constexpr TKafkaVersion TaggedVersionMax = 17;
    static constexpr TKafkaVersion NullableVersionMin = 5;
    static constexpr TKafkaVersion NullableVersionMax = 19;
    static constexpr TKafkaVersion FlexibleVersionMin = 7;
    static constexpr TKafkaVersion FlexibleVersionMax = Max<TKafkaVersion>();
};

TEST(Serialization, Struct_IsDefault) {
    TRequestHeaderData value;
    EXPECT_TRUE(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaStruct>(value)); // all fields have default values

    value.requestApiKey = 123;
    EXPECT_FALSE(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaStruct>(value)); // field changed
}


struct Meta_TKafkaString {
    using Type = TKafkaString;
    using TypeDesc = NKafka::NPrivate::TKafkaStringDesc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
    static const Type Default; // = 7;
                
    static constexpr TKafkaVersion PresentVersionMin = 3;
    static constexpr TKafkaVersion PresentVersionMax = 97;
    static constexpr TKafkaVersion TaggedVersionMin = 11;
    static constexpr TKafkaVersion TaggedVersionMax = 17;
    static constexpr TKafkaVersion NullableVersionMin = 5;
    static constexpr TKafkaVersion NullableVersionMax = 19;
    static constexpr TKafkaVersion FlexibleVersionMin = 7;
    static constexpr TKafkaVersion FlexibleVersionMax = Max<TKafkaVersion>();
};

const Meta_TKafkaString::Type Meta_TKafkaString::Default = "default_value";

TEST(Serialization, TKafkaString_IsDefault) {
    TKafkaString value;
    EXPECT_FALSE(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaString>(value)); // std::nullopt != "default_value"

    value = "random_string";
    EXPECT_FALSE(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaString>(value)); // "random_string" != "default_value"

    value = "default_value";
    EXPECT_TRUE(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaString>(value));
}

TEST(Serialization, TKafkaString_PresentVersion_NotTaggedVersion) {
    SIMPLE_HEAD(TKafkaString, { "some value" });

    NKafka::NPrivate::Write<Meta_TKafkaString>(collector, writable, 3, value);
    NKafka::NPrivate::Read<Meta_TKafkaString>(readable, 3, result);

    EXPECT_EQ(collector.NumTaggedFields, 0u);
    EXPECT_EQ(result, value); // Must read same that write
}

TEST(Serialization, TKafkaString_PresentVersion_TaggedVersion) {
    SIMPLE_HEAD(TKafkaString, { "some value" });

    NKafka::NPrivate::Write<Meta_TKafkaString>(collector, writable, 11, value);
    EXPECT_EQ(collector.NumTaggedFields, 1u);

    NKafka::NPrivate::WriteTag<Meta_TKafkaString>(writable, 11, value);

    i32 tag = readable.readUnsignedVarint();
    EXPECT_EQ(tag, Meta_TKafkaString::Tag);

    ui32 size = readable.readUnsignedVarint();
    EXPECT_EQ(size, value->size() + NKafka::NPrivate::SizeOfUnsignedVarint(value->size() + 1)); // "+1" because serialized as unsigned int, and null serialized with size equals 0

    NKafka::NPrivate::ReadTag<Meta_TKafkaString>(readable, 11, result);
    EXPECT_EQ(result, value); // Must read same that write
}

TEST(Serialization, TKafkaString_PresentVersion_TaggedVersion_Default) {
    SIMPLE_HEAD(TKafkaInt8, Meta_TKafkaInt8::Default);

    NKafka::NPrivate::Write<Meta_TKafkaInt8>(collector, writable, 11, value);
    EXPECT_EQ(collector.NumTaggedFields, 0u); // not serialize default value for tagged version
}



struct Meta_TKafkaArray {
    using Type = std::vector<TKafkaString>;
    using TypeDesc = NKafka::NPrivate::TKafkaArrayDesc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
                
    static constexpr TKafkaVersion PresentVersionMin = 3;
    static constexpr TKafkaVersion PresentVersionMax = 97;
    static constexpr TKafkaVersion TaggedVersionMin = 11;
    static constexpr TKafkaVersion TaggedVersionMax = 17;
    static constexpr TKafkaVersion NullableVersionMin = 5;
    static constexpr TKafkaVersion NullableVersionMax = 19;
    static constexpr TKafkaVersion FlexibleVersionMin = 7;
    static constexpr TKafkaVersion FlexibleVersionMax = Max<TKafkaVersion>();
};

TEST(Serialization, TKafkaArray_IsDefault) {
    Meta_TKafkaArray::Type value;
    EXPECT_TRUE(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaArray>(value)); // array is empty

    value.push_back("random_string");
    EXPECT_FALSE(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaArray>(value)); // array contains elements
}

TEST(Serialization, TKafkaArray_PresentVersion_NotTaggedVersion) {
    SIMPLE_HEAD(TKafkaArray, { "some value" });

    NKafka::NPrivate::Write<Meta_TKafkaArray>(collector, writable, 3, value);
    NKafka::NPrivate::Read<Meta_TKafkaArray>(readable, 3, result);

    EXPECT_EQ(collector.NumTaggedFields, 0u);
    EXPECT_EQ(result, value); // Must read same that write
}

TEST(Serialization, TKafkaArray_PresentVersion_TaggedVersion) {
    TString v = "some value";
    SIMPLE_HEAD(TKafkaArray, { v });

    NKafka::NPrivate::Write<Meta_TKafkaArray>(collector, writable, 11, value);
    EXPECT_EQ(collector.NumTaggedFields, 1u);

    NKafka::NPrivate::WriteTag<Meta_TKafkaArray>(writable, 11, value);

    i32 tag = readable.readUnsignedVarint();
    EXPECT_EQ(tag, Meta_TKafkaArray::Tag);

    ui32 size = readable.readUnsignedVarint();
    EXPECT_EQ(size, v.length() // array element data
        + NKafka::NPrivate::SizeOfUnsignedVarint(value.size()) // array size
        + NKafka::NPrivate::SizeOfUnsignedVarint(v.length() + 1) // string size. +1 because null string serialize as 0-length
    );

    NKafka::NPrivate::ReadTag<Meta_TKafkaArray>(readable, 11, result);
    EXPECT_EQ(result, value); // Must read same that write
}

TEST(Serialization, TKafkaArray_PresentVersion_TaggedVersion_Default) {
    SIMPLE_HEAD(TKafkaArray, {});

    NKafka::NPrivate::Write<Meta_TKafkaArray>(collector, writable, 11, value);
    EXPECT_EQ(collector.NumTaggedFields, 0u); // not serialize default value for tagged version
}



struct Meta_TKafkaBytes {
    using Type = TKafkaBytes;
    using TypeDesc = NKafka::NPrivate::TKafkaBytesDesc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
                
    static constexpr TKafkaVersion PresentVersionMin = 3;
    static constexpr TKafkaVersion PresentVersionMax = 97;
    static constexpr TKafkaVersion TaggedVersionMin = 11;
    static constexpr TKafkaVersion TaggedVersionMax = 17;
    static constexpr TKafkaVersion NullableVersionMin = 5;
    static constexpr TKafkaVersion NullableVersionMax = 19;
    static constexpr TKafkaVersion FlexibleVersionMin = 7;
    static constexpr TKafkaVersion FlexibleVersionMax = Max<TKafkaVersion>();
};

TEST(Serialization, TKafkaBytes_IsDefault) {
    Meta_TKafkaBytes::Type value;
    EXPECT_TRUE(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaBytes>(value)); // value is std::nullopt

    value = TBuffer();
    value->Resize(10);
    EXPECT_FALSE(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaBytes>(value)); // value is not null
}

TEST(Serialization, TKafkaBytes_PresentVersion_NotTaggedVersion) {
    SIMPLE_HEAD(TKafkaBytes, TBuffer("0123456789", 10));

    NKafka::NPrivate::Write<Meta_TKafkaBytes>(collector, writable, 3, value);
    NKafka::NPrivate::Read<Meta_TKafkaBytes>(readable, 3, result);

    EXPECT_EQ(collector.NumTaggedFields, 0u);
    EXPECT_EQ(result, value); // Must read same that write
}

TEST(Serialization, TKafkaBytes_PresentVersion_TaggedVersion) {
    SIMPLE_HEAD(TKafkaBytes, TBuffer("0123456789", 10));

    NKafka::NPrivate::Write<Meta_TKafkaBytes>(collector, writable, 11, value);
    EXPECT_EQ(collector.NumTaggedFields, 1u);

    NKafka::NPrivate::WriteTag<Meta_TKafkaBytes>(writable, 11, value);

    i32 tag = readable.readUnsignedVarint();
    EXPECT_EQ(tag, Meta_TKafkaArray::Tag);

    ui32 size = readable.readUnsignedVarint();
    EXPECT_EQ(size, value->size() // byffer data
        + NKafka::NPrivate::SizeOfUnsignedVarint(value->size() + 1) // buffer size. +1 because null value stored as size 0
    );

    NKafka::NPrivate::ReadTag<Meta_TKafkaBytes>(readable, 11, result);
    EXPECT_EQ(result, value); // Must read same that write
}

TEST(Serialization, TKafkaBytes_PresentVersion_TaggedVersion_Default) {
    SIMPLE_HEAD(TKafkaBytes, std::nullopt);

    NKafka::NPrivate::Write<Meta_TKafkaBytes>(collector, writable, 11, value);
    EXPECT_EQ(collector.NumTaggedFields, 0u); // not serialize default value for tagged version
}


TEST(Serialization, TRequestHeaderData_reference) {
    // original kafka serialized value (java implementation)
    ui8 reference[] = {0x00, 0x03, 0x00, 0x07, 0x00, 0x00, 0x00, 0x0D, 0x00, 0x10, 0x63, 0x6C, 0x69, 0x65, 0x6E, 0x74, 0x2D, 0x69, 0x64, 0x2D, 0x73, 0x74, 0x72, 0x69, 0x6E, 0x67, 0x00};

    std::stringstream sb;
    TKafkaWritable writable(sb);
    TKafkaReadable readable(sb);

    TRequestHeaderData value;
    value.requestApiKey = 3;
    value.requestApiVersion = 7;
    value.correlationId = 13;
    value.clientId = "client-id-string";

    value.Write(writable, 2);

    for(ui8 r : reference) {
        ui8 v = sb.get();
        EXPECT_EQ(v, r);
    }

    sb.write((char*)reference, sizeof(reference));

    TRequestHeaderData result;
    result.Read(readable, 2);
    EXPECT_EQ(result.requestApiKey, 3);
    EXPECT_EQ(result.requestApiVersion, 7);
    EXPECT_EQ(result.correlationId, 13);
    EXPECT_EQ(result.clientId, "client-id-string");
}

struct Meta_TKafkaFloat64 {
    using Type = TKafkaFloat64;
    using TypeDesc = NKafka::NPrivate::TKafkaFloat64Desc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
    static const Type Default; // = 7;
                
    static constexpr TKafkaVersion PresentVersionMin = 3;
    static constexpr TKafkaVersion PresentVersionMax = 97;
    static constexpr TKafkaVersion TaggedVersionMin = 11;
    static constexpr TKafkaVersion TaggedVersionMax = 17;
    static constexpr TKafkaVersion NullableVersionMin = 5;
    static constexpr TKafkaVersion NullableVersionMax = 19;
    static constexpr TKafkaVersion FlexibleVersionMin = 7;
    static constexpr TKafkaVersion FlexibleVersionMax = Max<TKafkaVersion>();
};

const Meta_TKafkaFloat64::Type Meta_TKafkaFloat64::Default = 7.875;

TEST(Serialization, TKafkaFloat64_PresentVersion_NotTaggedVersion) {
    // original kafka serialized value (java implementation)
    ui8 reference[] = {0x40, 0x09, 0x21, 0xCA, 0xC0, 0x83, 0x12, 0x6F};

    SIMPLE_HEAD(TKafkaFloat64, 3.1415);

    NKafka::NPrivate::Write<Meta_TKafkaFloat64>(collector, writable, 3, value);
    NKafka::NPrivate::Read<Meta_TKafkaFloat64>(readable, 3, result);

    EXPECT_EQ(collector.NumTaggedFields, 0u);
    EXPECT_EQ(result, value); // Must read same that write

    NKafka::NPrivate::Write<Meta_TKafkaFloat64>(collector, writable, 3, value);
    for(ui8 r : reference) {
        ui8 v = sb.get();
        EXPECT_EQ(v, r);
    }
}

TEST(Serialization, ProduceRequestData_reference) {
    // original kafka serialized value (java implementation)
    ui8 reference[] = {0x02, 0x37, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x03, 0x0D, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74,
                       0x69, 0x6F, 0x6E, 0x2D, 0x31, 0x31, 0x04, 0x00, 0x00, 0x00, 0x0D, 0x1C, 0x72, 0x65, 0x63, 0x6F,
                       0x72, 0x64, 0x2D, 0x31, 0x33, 0x2D, 0x69, 0x74, 0x2D, 0x69, 0x73, 0x2D, 0x6B, 0x61, 0x66, 0x6B,
                       0x61, 0x2D, 0x62, 0x79, 0x74, 0x65, 0x73, 0x00, 0x00, 0x00, 0x00, 0x11, 0x1C, 0x72, 0x65, 0x63,
                       0x6F, 0x72, 0x64, 0x2D, 0x31, 0x37, 0x2D, 0x69, 0x74, 0x2D, 0x69, 0x73, 0x2D, 0x6B, 0x61, 0x66,
                       0x6B, 0x61, 0x2D, 0x62, 0x79, 0x74, 0x65, 0x73, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x0D, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6F, 0x6E, 0x2D, 0x32, 0x33, 0x01, 0x00, 0x00};

    std::stringstream sb;
    TKafkaWritable writable(sb);
    TKafkaReadable readable(sb);

    TProduceRequestData value;
    value.acks = 3;
    value.timeoutMs = 5;
    value.transactionalId = "7";

    value.topicData.resize(2);
    value.topicData[0].name = "partition-11";
    value.topicData[0].partitionData.resize(3);
    value.topicData[0].partitionData[0].index = 13;
    value.topicData[0].partitionData[0].records = TKafkaRawBytes("record-13-it-is-kafka-bytes", 27);
    value.topicData[0].partitionData[1].index = 17;
    value.topicData[0].partitionData[1].records = TKafkaRawBytes("record-17-it-is-kafka-bytes", 27);

    value.topicData[1].name = "partition-23";

    value.Write(writable, 9);

//    Print(sb);

    for(ui8 r : reference) {
        ui8 v = sb.get();
        EXPECT_EQ(v, r);
    }

    sb.write((char*)reference, sizeof(reference));

    TProduceRequestData result;
    result.Read(readable, 9);

    EXPECT_EQ(result.acks, 3);
    EXPECT_EQ(result.timeoutMs, 5);
    EXPECT_EQ(result.transactionalId, "7");

    EXPECT_EQ(result.topicData.size(), 2ul);
    EXPECT_EQ(result.topicData[0].name, "partition-11");
    EXPECT_EQ(result.topicData[0].partitionData.size(), 3ul);
    EXPECT_EQ(result.topicData[0].partitionData[0].index, 13);
    EXPECT_EQ(result.topicData[0].partitionData[0].records, TKafkaRawBytes("record-13-it-is-kafka-bytes", 27));
    EXPECT_EQ(result.topicData[0].partitionData[1].index, 17);
    EXPECT_EQ(result.topicData[0].partitionData[1].records, TKafkaRawBytes("record-17-it-is-kafka-bytes", 27));
    EXPECT_EQ(result.topicData[0].partitionData[2].index, 0);
    EXPECT_EQ(result.topicData[0].partitionData[2].records, std::nullopt);

    EXPECT_EQ(result.topicData[1].name, "partition-23");
    EXPECT_EQ(result.topicData[1].partitionData.size(), 0ul);
}


char Hex(const unsigned char c) {
    return c < 10 ? '0' + c : 'A' + c - 10;
}

void Print(std::stringstream& sb) {
    while(true) {
        char c = sb.get();
        if (sb.eof()) {
            break;
        }
        Cerr << ", 0x" << Hex(c >> 4) << Hex(c & 0x0F);
    }
    Cerr << Endl;

    sb.seekg(-sb.tellg());
}
