#include <ydb/library/kafka/kafka_messages_int.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKafka {
namespace {

static constexpr size_t BUFFER_SIZE = 1 << 16;

#define SIMPLE_HEAD(Type_, Value)                   \
    Meta_##Type_::Type value = Value;               \
    Meta_##Type_::Type result;                      \
                                                    \
    TKafkaWriteBuffer sb(BUFFER_SIZE);          \
    TKafkaWritable writable(sb);                    \
    TKafkaReadable readable(sb.GetFrontBuffer());   \
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
    static const Type Default;

    static constexpr TKafkaVersions PresentVersions{3, 97};
    static constexpr TKafkaVersions TaggedVersions{11, 17};
    static constexpr TKafkaVersions NullableVersions{5, 19};
    static constexpr TKafkaVersions FlexibleVersions{7, Max<TKafkaVersion>()};
};

const Meta_TKafkaInt8::Type Meta_TKafkaInt8::Default = 7;

static_assert(Meta_TKafkaInt8::Tag == 31);
static_assert(Meta_TKafkaInt8::About[0] != '\0');
static_assert(Meta_TKafkaInt8::NullableVersions.Min == 5);
static_assert(Meta_TKafkaInt8::FlexibleVersions.Min == 7);

struct Meta_TKafkaString {
    using Type = TKafkaString;
    using TypeDesc = NKafka::NPrivate::TKafkaStringDesc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
    static const Type Default;

    static constexpr TKafkaVersions PresentVersions{3, 97};
    static constexpr TKafkaVersions TaggedVersions{11, 17};
    static constexpr TKafkaVersions NullableVersions{5, 19};
    static constexpr TKafkaVersions FlexibleVersions{7, Max<TKafkaVersion>()};
};

const Meta_TKafkaString::Type Meta_TKafkaString::Default = "default_value";

static_assert(Meta_TKafkaString::Tag == 31);
static_assert(Meta_TKafkaString::About[0] != '\0');
static_assert(Meta_TKafkaString::NullableVersions.Min == 5);
static_assert(Meta_TKafkaString::FlexibleVersions.Min == 7);

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

static_assert(Meta_TKafkaArray::Tag == 31);
static_assert(Meta_TKafkaArray::About[0] != '\0');
static_assert(Meta_TKafkaArray::NullableVersions.Min == 5);
static_assert(Meta_TKafkaArray::FlexibleVersions.Min == 7);

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

static_assert(Meta_TKafkaBytes::Tag == 31);
static_assert(Meta_TKafkaBytes::About[0] != '\0');
static_assert(Meta_TKafkaBytes::NullableVersions.Min == 5);
static_assert(Meta_TKafkaBytes::FlexibleVersions.Min == 7);

struct Meta_TKafkaFloat64 {
    using Type = TKafkaFloat64;
    using TypeDesc = NKafka::NPrivate::TKafkaFloat64Desc;

    static constexpr const char* Name = "value";
    static constexpr const char* About = "The test field.";
    static constexpr const TKafkaInt32 Tag = 31;
    static const Type Default;

    static constexpr TKafkaVersions PresentVersions{3, 97};
    static constexpr TKafkaVersions TaggedVersions{11, 17};
    static constexpr TKafkaVersions NullableVersions{5, 19};
    static constexpr TKafkaVersions FlexibleVersions{7, Max<TKafkaVersion>()};
};

const Meta_TKafkaFloat64::Type Meta_TKafkaFloat64::Default = 7.875;

static_assert(Meta_TKafkaFloat64::Tag == 31);
static_assert(Meta_TKafkaFloat64::About[0] != '\0');
static_assert(Meta_TKafkaFloat64::NullableVersions.Min == 5);
static_assert(Meta_TKafkaFloat64::FlexibleVersions.Min == 7);

Y_UNIT_TEST_SUITE(KafkaMessagesInt) {
    Y_UNIT_TEST(TKafkaInt8_NotPresentVersion) {
        SIMPLE_HEAD(TKafkaInt8, 37);

        NKafka::NPrivate::Write<Meta_TKafkaInt8>(collector, writable, 0, value);
        UNIT_ASSERT_EQUAL(sb.GetFrontBuffer().size(), (size_t)0);
        UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);

        NKafka::NPrivate::Read<Meta_TKafkaInt8>(readable, 0, result);
        UNIT_ASSERT_EQUAL(result, Meta_TKafkaInt8::Default);
    }

    Y_UNIT_TEST(TKafkaInt8_PresentVersion_NotTaggedVersion) {
        SIMPLE_HEAD(TKafkaInt8, 37);

        NKafka::NPrivate::Write<Meta_TKafkaInt8>(collector, writable, 3, value);
        NKafka::NPrivate::Read<Meta_TKafkaInt8>(readable, 3, result);

        UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
        UNIT_ASSERT_EQUAL(result, value);
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
        UNIT_ASSERT_EQUAL(result, value);
    }

    Y_UNIT_TEST(TKafkaInt8_PresentVersion_TaggedVersion_Default) {
        SIMPLE_HEAD(TKafkaInt8, Meta_TKafkaInt8::Default);

        NKafka::NPrivate::Write<Meta_TKafkaInt8>(collector, writable, 11, value);
        UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
    }

    Y_UNIT_TEST(TKafkaString_IsDefault) {
        TKafkaString value;
        UNIT_ASSERT(!NKafka::NPrivate::IsDefaultValue<Meta_TKafkaString>(value));

        value = "random_string";
        UNIT_ASSERT(!NKafka::NPrivate::IsDefaultValue<Meta_TKafkaString>(value));

        value = "default_value";
        UNIT_ASSERT(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaString>(value));
    }

    Y_UNIT_TEST(TKafkaString_PresentVersion_NotTaggedVersion) {
        SIMPLE_HEAD(TKafkaString, { "some value" });

        NKafka::NPrivate::Write<Meta_TKafkaString>(collector, writable, 3, value);
        NKafka::NPrivate::Read<Meta_TKafkaString>(readable, 3, result);

        UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
        UNIT_ASSERT_EQUAL(result, value);
    }

    Y_UNIT_TEST(TKafkaString_PresentVersion_TaggedVersion) {
        SIMPLE_HEAD(TKafkaString, { "some value" });

        NKafka::NPrivate::Write<Meta_TKafkaString>(collector, writable, 11, value);
        UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 1u);

        NKafka::NPrivate::WriteTag<Meta_TKafkaString>(writable, 11, value);

        ui32 tag = readable.readUnsignedVarint<ui32>();
        UNIT_ASSERT_EQUAL(tag, Meta_TKafkaString::Tag);

        ui32 size = readable.readUnsignedVarint<ui32>();
        UNIT_ASSERT_EQUAL(size, value->size() + NKafka::NPrivate::SizeOfUnsignedVarint(value->size() + 1));

        NKafka::NPrivate::ReadTag<Meta_TKafkaString>(readable, 11, result);
        UNIT_ASSERT_EQUAL(result, value);
    }

    Y_UNIT_TEST(TKafkaString_PresentVersion_TaggedVersion_Default) {
        SIMPLE_HEAD(TKafkaString, Meta_TKafkaString::Default);

        NKafka::NPrivate::Write<Meta_TKafkaString>(collector, writable, 11, value);
        UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
    }

    Y_UNIT_TEST(TKafkaArray_IsDefault) {
        Meta_TKafkaArray::Type value;
        UNIT_ASSERT(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaArray>(value));

        value.push_back("random_string");
        UNIT_ASSERT(!NKafka::NPrivate::IsDefaultValue<Meta_TKafkaArray>(value));
    }

    Y_UNIT_TEST(TKafkaArray_PresentVersion_NotTaggedVersion) {
        SIMPLE_HEAD(TKafkaArray, { "some value" });

        NKafka::NPrivate::Write<Meta_TKafkaArray>(collector, writable, 3, value);
        NKafka::NPrivate::Read<Meta_TKafkaArray>(readable, 3, result);

        UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
        UNIT_ASSERT_EQUAL(result, value);
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
        UNIT_ASSERT_EQUAL(size, v.length()
            + NKafka::NPrivate::SizeOfUnsignedVarint(value.size())
            + NKafka::NPrivate::SizeOfUnsignedVarint(v.length() + 1));

        NKafka::NPrivate::ReadTag<Meta_TKafkaArray>(readable, 11, result);
        UNIT_ASSERT_EQUAL(result, value);
    }

    Y_UNIT_TEST(TKafkaArray_PresentVersion_TaggedVersion_Default) {
        SIMPLE_HEAD(TKafkaArray, {});

        NKafka::NPrivate::Write<Meta_TKafkaArray>(collector, writable, 11, value);
        UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
    }

    Y_UNIT_TEST(TKafkaBytes_IsDefault) {
        Meta_TKafkaBytes::Type value;
        UNIT_ASSERT(NKafka::NPrivate::IsDefaultValue<Meta_TKafkaBytes>(value));

        char v[] = "value";
        value = TArrayRef(v);
        UNIT_ASSERT(!NKafka::NPrivate::IsDefaultValue<Meta_TKafkaBytes>(value));
    }

    Y_UNIT_TEST(TKafkaBytes_PresentVersion_NotTaggedVersion) {
        char v[] = "0123456789";
        SIMPLE_HEAD(TKafkaBytes, TArrayRef(v));

        NKafka::NPrivate::Write<Meta_TKafkaBytes>(collector, writable, 3, value);
        NKafka::NPrivate::Read<Meta_TKafkaBytes>(readable, 3, result);

        UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
        UNIT_ASSERT_EQUAL(*result, *value);
    }

    Y_UNIT_TEST(TKafkaBytes_PresentVersion_TaggedVersion) {
        char v[] = "0123456789";
        SIMPLE_HEAD(TKafkaBytes, TArrayRef(v));

        NKafka::NPrivate::Write<Meta_TKafkaBytes>(collector, writable, 11, value);
        UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 1u);

        NKafka::NPrivate::WriteTag<Meta_TKafkaBytes>(writable, 11, value);

        ui32 tag = readable.readUnsignedVarint<ui32>();
        UNIT_ASSERT_EQUAL(tag, Meta_TKafkaBytes::Tag);

        ui32 size = readable.readUnsignedVarint<ui32>();
        UNIT_ASSERT_EQUAL(size, value->size()
            + NKafka::NPrivate::SizeOfUnsignedVarint(value->size() + 1));

        NKafka::NPrivate::ReadTag<Meta_TKafkaBytes>(readable, 11, result);
        UNIT_ASSERT_EQUAL(*result, *value);
    }

    Y_UNIT_TEST(TKafkaBytes_PresentVersion_TaggedVersion_Default) {
        SIMPLE_HEAD(TKafkaBytes, std::nullopt);

        NKafka::NPrivate::Write<Meta_TKafkaBytes>(collector, writable, 11, value);
        UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
    }

    Y_UNIT_TEST(TKafkaFloat64_PresentVersion_NotTaggedVersion) {
        ui8 reference[] = {0x40, 0x09, 0x21, 0xCA, 0xC0, 0x83, 0x12, 0x6F};

        SIMPLE_HEAD(TKafkaFloat64, 3.1415);

        NKafka::NPrivate::Write<Meta_TKafkaFloat64>(collector, writable, 3, value);
        NKafka::NPrivate::Read<Meta_TKafkaFloat64>(readable, 3, result);

        UNIT_ASSERT_EQUAL(collector.NumTaggedFields, 0u);
        UNIT_ASSERT_EQUAL(result, value);

        UNIT_ASSERT_EQUAL(sb.GetFrontBuffer().size(), sizeof(reference));
        for (size_t i = 0; i < sizeof(reference); ++i) {
            UNIT_ASSERT_EQUAL(*(sb.GetFrontBuffer().data() + i), (char)reference[i]);
        }
    }
}

} // namespace
} // namespace NKafka
