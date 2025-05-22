#include <library/cpp/testing/unittest/registar.h>

#include <strstream>

#include <ydb/core/kafka_proxy/kafka_messages.h>

using namespace NKafka;

void Print(std::string& sb);

static constexpr size_t BUFFER_SIZE = 1 << 16;

Y_UNIT_TEST_SUITE(Serialization) {

Y_UNIT_TEST(RequestHeader) {
    TWritableBuf sb(nullptr, BUFFER_SIZE);

    TRequestHeaderData value;

    value.RequestApiKey = 3;
    value.RequestApiVersion = 7;
    value.CorrelationId = 11;
    value.ClientId = { "clientId-value" };

    TKafkaWritable writable(sb);
    value.Write(writable, 1);

    TRequestHeaderData result;
    TKafkaReadable readable(sb.GetBuffer());
    result.Read(readable, 1);

    UNIT_ASSERT_EQUAL(result.RequestApiKey, 3);
    UNIT_ASSERT_EQUAL(result.RequestApiVersion, 7);
    UNIT_ASSERT_EQUAL(result.CorrelationId, 11);
    UNIT_ASSERT_EQUAL(*result.ClientId, "clientId-value");
}

Y_UNIT_TEST(ResponseHeader) {
    TWritableBuf sb(nullptr, BUFFER_SIZE);

    TResponseHeaderData value;

    value.CorrelationId = 13;

    TKafkaWritable writable(sb);
    value.Write(writable, 0);

    TKafkaReadable readable(sb.GetBuffer());
    TResponseHeaderData result;
    result.Read(readable, 0);

    UNIT_ASSERT_EQUAL(result.CorrelationId, 13);
}

Y_UNIT_TEST(ApiVersionsRequest) {
    TWritableBuf sb(nullptr, BUFFER_SIZE);

    TApiVersionsRequestData value;

    value.ClientSoftwareName = { "apache-kafka-java" };
    value.ClientSoftwareVersion = { "3.4.0" };

    TKafkaWritable writable(sb);
    value.Write(writable, 3);

    TKafkaReadable readable(sb.GetBuffer());
    TApiVersionsRequestData result;
    result.Read(readable, 3);

    UNIT_ASSERT_EQUAL(*result.ClientSoftwareName, "apache-kafka-java");
    UNIT_ASSERT_EQUAL(*result.ClientSoftwareVersion, "3.4.0");
}

Y_UNIT_TEST(ApiVersionsResponse) {
    TString longString = "long-string-value-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"; 

    TWritableBuf sb(nullptr, BUFFER_SIZE);

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

    TKafkaReadable readable(sb.GetBuffer());
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
    TWritableBuf sb(nullptr, BUFFER_SIZE);

    TApiVersionsResponseData value;
    size_t expectedSize = value.Size(2);

    value.SupportedFeatures.resize(1);
    value.SupportedFeatures[0].Name = "Feature name";

    size_t size = value.Size(2);

    UNIT_ASSERT_VALUES_EQUAL_C(expectedSize, size, "SupportedFeatures is not presents for 2 version");
}

Y_UNIT_TEST(ProduceRequest) {
    TWritableBuf sb(nullptr, BUFFER_SIZE);

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


    TKafkaReadable readable(sb.GetBuffer());
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

template<class T>
void CheckUnsignedVarint(const std::vector<T>& values)  {
    for(T v : values) {
        Cerr << ">>>>> Check value=" << v << Endl << Flush;
        TWritableBuf sb(nullptr, BUFFER_SIZE);
        TKafkaWritable writable(sb);
        TKafkaReadable readable(sb.GetBuffer());

        writable.writeUnsignedVarint(v);

        UNIT_ASSERT_EQUAL_C(sb.Size(), NKafka::NPrivate::SizeOfUnsignedVarint<T>(v),
            TStringBuilder() << "Size mismatch " << sb.Size() << " != " << NKafka::NPrivate::SizeOfUnsignedVarint<T>(v));


        T r = readable.readUnsignedVarint<T>();
        UNIT_ASSERT_EQUAL_C(r, v, TStringBuilder() << r << " != " << v);
    }
}

Y_UNIT_TEST(UnsignedVarint32) {
    CheckUnsignedVarint<ui32>({0, 1, 127, 128, 32191, Max<i32>(), Max<ui32>()});
}

Y_UNIT_TEST(UnsignedVarint64) {
    CheckUnsignedVarint<ui64>({0, 1, 127, 128, 32191, Max<i32>(), static_cast<unsigned long>(Max<i32>()) + 1, Max<i64>(), Max<ui64>()});
}

template<class T>
void CheckVarint(const std::vector<T>& values) {
    for(T v : values) {
        Cerr << ">>>>> Check value=" << v << Endl << Flush;
        TWritableBuf sb(nullptr, BUFFER_SIZE);
        TKafkaWritable writable(sb);
        TKafkaReadable readable(sb.GetBuffer());

        writable.writeVarint(v);

        UNIT_ASSERT_EQUAL_C(sb.Size(), NKafka::NPrivate::SizeOfVarint<T>(v),
            TStringBuilder() << "Size mismatch " << sb.Size() << " != " << NKafka::NPrivate::SizeOfVarint<T>(v));

        T r = readable.readVarint<T>();

        UNIT_ASSERT_EQUAL_C(r, v, TStringBuilder() << r << " != " << v);
    }
}

Y_UNIT_TEST(Varint32) {
    CheckVarint<i32>({ Min<i32>(), -167966, -1, 0, 1, 127, 128, 32191, Max<i32>()});
}

Y_UNIT_TEST(Varint64) {
    CheckVarint<i64>({Min<i64>(), Min<i32>(), -167966, -1, 0, 1, 127, 128, 32191, static_cast<unsigned long>(Max<i32>()) + 1, Max<i64>()});
}

template<class T>
void CheckVarint_WrongBytes(std::vector<ui8> bytes) {
    TWritableBuf sb(nullptr, BUFFER_SIZE);
    TKafkaWritable writable(sb);
    TKafkaReadable readable(sb.GetBuffer());

    writable.write((char*)bytes.data(), bytes.size());

    try {
        readable.readUnsignedVarint<T>();
        UNIT_FAIL("Must be exception");
    } catch(const yexception& e) {
        UNIT_ASSERT_STRING_CONTAINS(e.what(), "illegal varint length");
    }
}

Y_UNIT_TEST(UnsignedVarint32_Wrong) {
    CheckVarint_WrongBytes<ui32>({0xFF, 0xFF, 0xFF, 0xFF, 0xFF});
}

Y_UNIT_TEST(UnsignedVarint64_Wrong) {
    CheckVarint_WrongBytes<ui64>({0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF});
}

Y_UNIT_TEST(UnsignedVarint32_Deserialize) {
    std::vector<ui8> bytes = {0x81, 0x83, 0x05};

    TWritableBuf sb(nullptr, BUFFER_SIZE);
    TKafkaWritable writable(sb);
    TKafkaReadable readable(sb.GetBuffer());

    writable.write((char*)bytes.data(), bytes.size());

    ui32 result = readable.readUnsignedVarint<ui32>();
    UNIT_ASSERT_EQUAL(result, 1 + (3 << 7) + (5 << 14));
}

#define SIMPLE_HEAD(Type_, Value)                   \
    Meta_##Type_::Type value = Value;               \
    Meta_##Type_::Type result;                      \
                                                    \
    TWritableBuf sb(nullptr, BUFFER_SIZE);          \
    TKafkaWritable writable(sb);                    \
    TKafkaReadable readable(sb.GetBuffer());        \
                                                    \
    Y_UNUSED(readable);                             \
    Y_UNUSED(result);                               \
                                                    \
    NKafka::NPrivate::TWriteCollector collector;



struct Meta_TKafkaInt8 {
    using Type = TKafkaInt8;
    using TypeDesc = NKafka::NPrivate::TKafkaIntDesc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
    static const Type Default; // = 7;
                
    static constexpr TKafkaVersions PresentVersions{3, 97};
    static constexpr TKafkaVersions TaggedVersions{11, 17};
    static constexpr TKafkaVersions NullableVersions{5, 19};
    static constexpr TKafkaVersions FlexibleVersions{7, Max<TKafkaVersion>()};
};

const Meta_TKafkaInt8::Type Meta_TKafkaInt8::Default = 7;

Y_UNIT_TEST(TKafkaInt8_NotPresentVersion) {
    SIMPLE_HEAD(TKafkaInt8, 37);

    NKafka::NPrivate::Write<Meta_TKafkaInt8>(collector, writable, 0, value);
    UNIT_ASSERT_EQUAL(sb.Size(), (size_t)0); // For version 0 value is not serializable. Stream must be empty
    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);

    NKafka::NPrivate::Read<Meta_TKafkaInt8>(readable, 0, result);
    UNIT_ASSERT_EQUAL(result, Meta_TKafkaInt8::Default); // For version 0 value is not serializable
}

Y_UNIT_TEST(TKafkaInt8_PresentVersion_NotTaggedVersion) {
    SIMPLE_HEAD(TKafkaInt8, 37);

    NKafka::NPrivate::Write<Meta_TKafkaInt8>(collector, writable, 3, value);
    NKafka::NPrivate::Read<Meta_TKafkaInt8>(readable, 3, result);

    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
    UNIT_ASSERT_EQUAL(result, value); // Must read same that write
}

Y_UNIT_TEST(TKafkaInt8_PresentVersion_TaggedVersion) {
    SIMPLE_HEAD(TKafkaInt8, 37);

    NKafka::NPrivate::Write<Meta_TKafkaInt8>(collector, writable, 11, value);
    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 1u);

    NKafka::NPrivate::WriteTag<Meta_TKafkaInt8>(writable, 11, value);

    ui32 tag = readable.readUnsignedVarint<ui32>();
    UNIT_ASSERT_EQUAL(tag, Meta_TKafkaInt8::Tag);

    ui32 size = readable.readUnsignedVarint<ui32>();
    UNIT_ASSERT_EQUAL(size, sizeof(TKafkaInt8));

    NKafka::NPrivate::ReadTag<Meta_TKafkaInt8>(readable, 11, result);
    UNIT_ASSERT_EQUAL(result, value); // Must read same that write
}

Y_UNIT_TEST(TKafkaInt8_PresentVersion_TaggedVersion_Default) {
    SIMPLE_HEAD(TKafkaInt8, Meta_TKafkaInt8::Default);

    NKafka::NPrivate::Write<Meta_TKafkaInt8>(collector, writable, 11, value);
    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u); // not serialize default value for tagged version
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

struct Meta_TKafkaString {
    using Type = TKafkaString;
    using TypeDesc = NKafka::NPrivate::TKafkaStringDesc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
    static const Type Default; // = 7;
                
    static constexpr TKafkaVersions PresentVersions{3, 97};
    static constexpr TKafkaVersions TaggedVersions{11, 17};
    static constexpr TKafkaVersions NullableVersions{5, 19};
    static constexpr TKafkaVersions FlexibleVersions{7, Max<TKafkaVersion>()};
};

const Meta_TKafkaString::Type Meta_TKafkaString::Default = "default_value";

Y_UNIT_TEST(TKafkaString_IsDefault) {
    TKafkaString value;
    UNIT_ASSERT(!NKafka::NPrivate::IsDefaultValue<Meta_TKafkaString>(value)); // std::nullopt != "default_value"

    value = "random_string";
    UNIT_ASSERT(!NKafka::NPrivate::IsDefaultValue<Meta_TKafkaString>(value)); // "random_string" != "default_value"

    value = "default_value";
    UNIT_ASSERT(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaString>(value));
}

Y_UNIT_TEST(TKafkaString_PresentVersion_NotTaggedVersion) {
    SIMPLE_HEAD(TKafkaString, { "some value" });

    NKafka::NPrivate::Write<Meta_TKafkaString>(collector, writable, 3, value);
    NKafka::NPrivate::Read<Meta_TKafkaString>(readable, 3, result);

    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
    UNIT_ASSERT_EQUAL(result, value); // Must read same that write
}

Y_UNIT_TEST(TKafkaString_PresentVersion_TaggedVersion) {
    SIMPLE_HEAD(TKafkaString, { "some value" });

    NKafka::NPrivate::Write<Meta_TKafkaString>(collector, writable, 11, value);
    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 1u);

    NKafka::NPrivate::WriteTag<Meta_TKafkaString>(writable, 11, value);

    ui32 tag = readable.readUnsignedVarint<ui32>();
    UNIT_ASSERT_EQUAL(tag, Meta_TKafkaString::Tag);

    ui32 size = readable.readUnsignedVarint<ui32>();
    UNIT_ASSERT_EQUAL(size, value->size() + NKafka::NPrivate::SizeOfUnsignedVarint(value->size() + 1)); // "+1" because serialized as unsigned int, and null serialized with size equals 0

    NKafka::NPrivate::ReadTag<Meta_TKafkaString>(readable, 11, result);
    UNIT_ASSERT_EQUAL(result, value); // Must read same that write
}

Y_UNIT_TEST(TKafkaString_PresentVersion_TaggedVersion_Default) {
    SIMPLE_HEAD(TKafkaInt8, Meta_TKafkaInt8::Default);

    NKafka::NPrivate::Write<Meta_TKafkaInt8>(collector, writable, 11, value);
    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u); // not serialize default value for tagged version
}


struct Meta_TKafkaArray {
    using Type = std::vector<TKafkaString>;
    using TypeDesc = NKafka::NPrivate::TKafkaArrayDesc;
    using ItemType = TKafkaString;
    using ItemTypeDesc = NKafka::NPrivate::TKafkaStringDesc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
                
    static constexpr TKafkaVersions PresentVersions{3, 97};
    static constexpr TKafkaVersions TaggedVersions{11, 17};
    static constexpr TKafkaVersions NullableVersions{5, 19};
    static constexpr TKafkaVersions FlexibleVersions{7, Max<TKafkaVersion>()};
};

Y_UNIT_TEST(TKafkaArray_IsDefault) {
    Meta_TKafkaArray::Type value;
    UNIT_ASSERT(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaArray>(value)); // array is empty

    value.push_back("random_string");
    UNIT_ASSERT(!NKafka::NPrivate::IsDefaultValue<Meta_TKafkaArray>(value)); // array contains elements
}

Y_UNIT_TEST(TKafkaArray_PresentVersion_NotTaggedVersion) {
    SIMPLE_HEAD(TKafkaArray, { "some value" });

    NKafka::NPrivate::Write<Meta_TKafkaArray>(collector, writable, 3, value);
    NKafka::NPrivate::Read<Meta_TKafkaArray>(readable, 3, result);

    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
    UNIT_ASSERT_EQUAL(result, value); // Must read same that write
}

Y_UNIT_TEST(TKafkaArray_PresentVersion_TaggedVersion) {
    TString v = "some value";
    SIMPLE_HEAD(TKafkaArray, { v });

    NKafka::NPrivate::Write<Meta_TKafkaArray>(collector, writable, 11, value);
    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 1u);

    NKafka::NPrivate::WriteTag<Meta_TKafkaArray>(writable, 11, value);

    ui32 tag = readable.readUnsignedVarint<ui32>();
    UNIT_ASSERT_EQUAL(tag, Meta_TKafkaArray::Tag);

    ui32 size = readable.readUnsignedVarint<ui32>();
    UNIT_ASSERT_EQUAL(size, v.length() // array element data
        + NKafka::NPrivate::SizeOfUnsignedVarint(value.size()) // array size
        + NKafka::NPrivate::SizeOfUnsignedVarint(v.length() + 1) // string size. +1 because null string serialize as 0-length
    );

    NKafka::NPrivate::ReadTag<Meta_TKafkaArray>(readable, 11, result);
    UNIT_ASSERT_EQUAL(result, value); // Must read same that write
}

Y_UNIT_TEST(TKafkaArray_PresentVersion_TaggedVersion_Default) {
    SIMPLE_HEAD(TKafkaArray, {});

    NKafka::NPrivate::Write<Meta_TKafkaArray>(collector, writable, 11, value);
    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u); // not serialize default value for tagged version
}



struct Meta_TKafkaBytes {
    using Type = TKafkaBytes;
    using TypeDesc = NKafka::NPrivate::TKafkaBytesDesc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
                
    static constexpr TKafkaVersions PresentVersions{3, 97};
    static constexpr TKafkaVersions TaggedVersions{11, 17};
    static constexpr TKafkaVersions NullableVersions{5, 19};
    static constexpr TKafkaVersions FlexibleVersions{7, Max<TKafkaVersion>()};
};

Y_UNIT_TEST(TKafkaBytes_IsDefault) {
    Meta_TKafkaBytes::Type value;
    UNIT_ASSERT(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaBytes>(value)); // value is std::nullopt

    char v[] = "value";
    value = TArrayRef<char>(v);
    UNIT_ASSERT(!NKafka::NPrivate::IsDefaultValue<Meta_TKafkaBytes>(value)); // value is not null
}

Y_UNIT_TEST(TKafkaBytes_PresentVersion_NotTaggedVersion) {
    char v[] = "0123456789";
    SIMPLE_HEAD(TKafkaBytes, TArrayRef(v));

    NKafka::NPrivate::Write<Meta_TKafkaBytes>(collector, writable, 3, value);
    NKafka::NPrivate::Read<Meta_TKafkaBytes>(readable, 3, result);

    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
    UNIT_ASSERT_EQUAL(result->size(), value->size());
    UNIT_ASSERT_EQUAL(TString(result->begin(), result->size()), TString(value->begin(), value->size())); // Must read same that write
}

Y_UNIT_TEST(TKafkaBytes_PresentVersion_TaggedVersion) {
    char v[] = "0123456789";
    SIMPLE_HEAD(TKafkaBytes, TArrayRef(v));

    NKafka::NPrivate::Write<Meta_TKafkaBytes>(collector, writable, 11, value);
    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 1u);

    NKafka::NPrivate::WriteTag<Meta_TKafkaBytes>(writable, 11, value);

    ui32 tag = readable.readUnsignedVarint<ui32>();
    UNIT_ASSERT_EQUAL(tag, Meta_TKafkaArray::Tag);

    ui32 size = readable.readUnsignedVarint<ui32>();
    UNIT_ASSERT_EQUAL(size, value->size() // byffer data
        + NKafka::NPrivate::SizeOfUnsignedVarint(value->size() + 1) // buffer size. +1 because null value stored as size 0
    );

    NKafka::NPrivate::ReadTag<Meta_TKafkaBytes>(readable, 11, result);
    UNIT_ASSERT_EQUAL(result->size(), value->size());
    UNIT_ASSERT_EQUAL(TString(result->begin(), result->size()), TString(value->begin(), value->size())); // Must read same that write
}

Y_UNIT_TEST(TKafkaBytes_PresentVersion_TaggedVersion_Default) {
    SIMPLE_HEAD(TKafkaBytes, std::nullopt);

    NKafka::NPrivate::Write<Meta_TKafkaBytes>(collector, writable, 11, value);
    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u); // not serialize default value for tagged version
}


Y_UNIT_TEST(TRequestHeaderData_reference) {
    // original kafka serialized value (java implementation)
    ui8 reference[] = {0x00, 0x03, 0x00, 0x07, 0x00, 0x00, 0x00, 0x0D, 0x00, 0x10, 0x63, 0x6C, 0x69, 0x65, 0x6E, 0x74,
                       0x2D, 0x69, 0x64, 0x2D, 0x73, 0x74, 0x72, 0x69, 0x6E, 0x67, 0x00};

    TWritableBuf sb(nullptr, BUFFER_SIZE);
    TKafkaWritable writable(sb);
    TKafkaReadable readable(sb.GetBuffer());

    TRequestHeaderData value;
    value.RequestApiKey = 3;
    value.RequestApiVersion = 7;
    value.CorrelationId = 13;
    value.ClientId = "client-id-string";

    value.Write(writable, 2);

    UNIT_ASSERT_EQUAL(sb.Size(), sizeof(reference));
    for(size_t i = 0; i < sizeof(reference); ++i) {
        UNIT_ASSERT_EQUAL(*(sb.Data() + i), reference[i]);
    }


    TRequestHeaderData result;
    result.Read(readable, 2);

    UNIT_ASSERT_EQUAL(result.RequestApiKey, 3);
    UNIT_ASSERT_EQUAL(result.RequestApiVersion, 7);
    UNIT_ASSERT_EQUAL(result.CorrelationId, 13);
    UNIT_ASSERT_EQUAL(result.ClientId, "client-id-string");
}

struct Meta_TKafkaFloat64 {
    using Type = TKafkaFloat64;
    using TypeDesc = NKafka::NPrivate::TKafkaFloat64Desc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
    static const Type Default; // = 7;
                
    static constexpr TKafkaVersions PresentVersions{3, 97};
    static constexpr TKafkaVersions TaggedVersions{11, 17};
    static constexpr TKafkaVersions NullableVersions{5, 19};
    static constexpr TKafkaVersions FlexibleVersions{7, Max<TKafkaVersion>()};
};

const Meta_TKafkaFloat64::Type Meta_TKafkaFloat64::Default = 7.875;

Y_UNIT_TEST(TKafkaFloat64_PresentVersion_NotTaggedVersion) {
    // original kafka serialized value (java implementation)
    ui8 reference[] = {0x40, 0x09, 0x21, 0xCA, 0xC0, 0x83, 0x12, 0x6F};

    SIMPLE_HEAD(TKafkaFloat64, 3.1415);

    NKafka::NPrivate::Write<Meta_TKafkaFloat64>(collector, writable, 3, value);
    NKafka::NPrivate::Read<Meta_TKafkaFloat64>(readable, 3, result);

    UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
    UNIT_ASSERT_EQUAL(result, value); // Must read same that write

    UNIT_ASSERT_EQUAL(sb.Size(), sizeof(reference));
    for(size_t i = 0; i < sizeof(reference); ++i) {
        UNIT_ASSERT_EQUAL(*(sb.Data() + i), (char)reference[i]);
    }
}

Y_UNIT_TEST(RequestHeader_reference) {
    ui8 reference[] = {0x00, 0x12, 0x00, 0x00, 0x7F, 0x6F, 0x6F, 0x68, 0x00, 0x0A, 0x70, 0x72, 0x6F, 0x64, 0x75, 0x63, 
                     0x65, 0x72, 0x2D, 0x31};

    TWritableBuf sb(nullptr, BUFFER_SIZE);
    sb.write((char*)reference, sizeof(reference));

    TKafkaReadable readable(sb.GetBuffer());
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
    
    UNIT_ASSERT_EQUAL(r0.Records[0].Key, TKafkaRawBytes("key", 3));
    UNIT_ASSERT_EQUAL(r0.Records[0].Value, TKafkaRawBytes("msg-1-1", 7));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers.size(), (size_t)3);
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers[0].Key, TKafkaRawBytes("h-1", 3));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers[0].Value, TKafkaRawBytes("v-1-1", 5));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers[1].Key, TKafkaRawBytes("h-2", 3));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers[1].Value, TKafkaRawBytes("v-2-1", 5));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers[2].Key, TKafkaRawBytes("h-3", 3));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers[2].Value, TKafkaRawBytes("v-3-1", 5));
    
    UNIT_ASSERT_EQUAL(r0.Records[1].Key, TKafkaRawBytes("key", 3));
    UNIT_ASSERT_EQUAL(r0.Records[1].Value, TKafkaRawBytes("msg-1-2", 7));
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers.size(), (size_t)3);
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers[0].Key, TKafkaRawBytes("h-1", 3));
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers[0].Value, TKafkaRawBytes("v-1-2", 5));
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers[1].Key, TKafkaRawBytes("h-2", 3));
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers[1].Value, TKafkaRawBytes("v-2-2", 5));
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers[2].Key, TKafkaRawBytes("h-3", 3));
    UNIT_ASSERT_EQUAL(r0.Records[1].Headers[2].Value, TKafkaRawBytes("v-3-2", 5));
    
    UNIT_ASSERT_EQUAL(r0.Records[2].Key, TKafkaRawBytes("key", 3));
    UNIT_ASSERT_EQUAL(r0.Records[2].Value, TKafkaRawBytes("msg-1-3", 7));
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers.size(), (size_t)3);
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers[0].Key, TKafkaRawBytes("h-1", 3));
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers[0].Value, TKafkaRawBytes("v-1-3", 5));
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers[1].Key, TKafkaRawBytes("h-2", 3));
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers[1].Value, TKafkaRawBytes("v-2-3", 5));
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers[2].Key, TKafkaRawBytes("h-3", 3));
    UNIT_ASSERT_EQUAL(r0.Records[2].Headers[2].Value, TKafkaRawBytes("v-3-3", 5));

    TWritableBuf sb(nullptr, sizeof(reference));
    TKafkaWritable writable(sb);

    header.Write(writable, 2);
    result.Write(writable, 9);

    UNIT_ASSERT_EQUAL(sb.Size(), sizeof(reference));
    for(size_t i = 0; i < sizeof(reference); ++i) {
        UNIT_ASSERT_EQUAL(*(sb.Data() + i), (char)reference[i]);
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
    UNIT_ASSERT_EQUAL(r0.PartitionLeaderEpoch, 0);
    UNIT_ASSERT_EQUAL(r0.Magic, 0);
    UNIT_ASSERT_EQUAL(r0.Crc, 544167206);
    UNIT_ASSERT_EQUAL(r0.Attributes, 0);
    UNIT_ASSERT_EQUAL(r0.LastOffsetDelta, 0);
    UNIT_ASSERT_EQUAL(r0.BaseTimestamp, 0);
    UNIT_ASSERT_EQUAL(r0.MaxTimestamp, 0);
    UNIT_ASSERT_EQUAL(r0.ProducerId, 0);
    UNIT_ASSERT_EQUAL(r0.ProducerEpoch, 0);
    UNIT_ASSERT_EQUAL(r0.BaseSequence, 0);

    UNIT_ASSERT_EQUAL(r0.Records.size(), (size_t)1);
    
    UNIT_ASSERT_EQUAL(r0.Records[0].Key, TKafkaRawBytes("key-1", 5));
    UNIT_ASSERT_EQUAL(r0.Records[0].Value, TKafkaRawBytes("test message", 12));
    UNIT_ASSERT_EQUAL(r0.Records[0].Headers.size(), (size_t)0);
}

char Hex(const unsigned char c) {
    return c < 10 ? '0' + c : 'A' + c - 10;
}

void Print(std::string& sb) {
    for(size_t i = 0; i < sb.length(); ++i) {
        char c = sb.at(i);
        if (i > 0) {
            Cerr << ", ";
        }
        Cerr << "0x" << Hex(c >> 4) << Hex(c & 0x0F);
    }
    Cerr << Endl;
}

}
