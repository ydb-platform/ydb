#include <library/cpp/testing/unittest/gtest.h>

#include <library/cpp/type_info/type_info.h>

#include "utils.h"

TEST(TypeSerialize, Void) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Void().Get()), R"(void)");
}

TEST(TypeSerialize, Null) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Null().Get()), R"(null)");
}

TEST(TypeSerialize, Bool) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Bool().Get()), R"(bool)");
}

TEST(TypeSerialize, Int8) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Int8().Get()), R"(int8)");
}

TEST(TypeSerialize, Int16) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Int16().Get()), R"(int16)");
}

TEST(TypeSerialize, Int32) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Int32().Get()), R"(int32)");
}

TEST(TypeSerialize, Int64) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Int64().Get()), R"(int64)");
}

TEST(TypeSerialize, Uint8) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Uint8().Get()), R"(uint8)");
}

TEST(TypeSerialize, Uint16) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Uint16().Get()), R"(uint16)");
}

TEST(TypeSerialize, Uint32) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Uint32().Get()), R"(uint32)");
}

TEST(TypeSerialize, Uint64) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Uint64().Get()), R"(uint64)");
}

TEST(TypeSerialize, Float) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Float().Get()), R"(float)");
}

TEST(TypeSerialize, Double) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Double().Get()), R"(double)");
}

TEST(TypeSerialize, String) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::String().Get()), R"(string)");
}

TEST(TypeSerialize, Utf8) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Utf8().Get()), R"(utf8)");
}

TEST(TypeSerialize, Date) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Date().Get()), R"(date)");
}

TEST(TypeSerialize, Datetime) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Datetime().Get()), R"(datetime)");
}

TEST(TypeSerialize, Timestamp) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Timestamp().Get()), R"(timestamp)");
}

TEST(TypeSerialize, TzDate) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::TzDate().Get()), R"(tz_date)");
}

TEST(TypeSerialize, TzDatetime) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::TzDatetime().Get()), R"(tz_datetime)");
}

TEST(TypeSerialize, TzTimestamp) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::TzTimestamp().Get()), R"(tz_timestamp)");
}

TEST(TypeSerialize, Interval) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Interval().Get()), R"(interval)");
}

TEST(TypeSerialize, Decimal) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Decimal(20, 10).Get()), R"({type_name=decimal; precision=20; scale=10})");
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Decimal(10, 10).Get()), R"({type_name=decimal; precision=10; scale=10})");
}

TEST(TypeSerialize, Json) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Json().Get()), R"(json)");
}

TEST(TypeSerialize, Yson) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Yson().Get()), R"(yson)");
}

TEST(TypeSerialize, Uuid) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Uuid().Get()), R"(uuid)");
}

TEST(TypeSerialize, Optional) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Optional(NTi::Void()).Get()), R"({type_name=optional; item=void})");
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Optional(NTi::String()).Get()), R"({type_name=optional; item=string})");
}

TEST(TypeSerialize, List) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::List(NTi::Void()).Get()), R"({type_name=list; item=void})");
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::List(NTi::String()).Get()), R"({type_name=list; item=string})");
}

TEST(TypeSerialize, Dict) {
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Dict(NTi::Void(), NTi::Void()).Get()), R"({type_name=dict; key=void; value=void})");
    ASSERT_YSON_EQ(NTi::NIo::SerializeYson(NTi::Dict(NTi::Int32(), NTi::String()).Get()), R"({type_name=dict; key=int32; value=string})");
}

TEST(TypeSerialize, StructEmpty) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Struct({}).Get()),
        R"({type_name=struct; members=[]})");
}

TEST(TypeSerialize, Struct) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Struct({{"ItemB", NTi::String()}, {"ItemA", NTi::List(NTi::Int64())}}).Get()),
        R"({type_name=struct; members=[{name=ItemB; type=string}; {name=ItemA; type={type_name=list; item=int64}}]})");
}

TEST(TypeSerialize, StructNamedEmpty) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Struct("S", {}).Get()),
        R"({type_name=struct; members=[]})");
}

TEST(TypeSerialize, StructNamedEmptyNoNames) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Struct("S", {}).Get(),
            /* binary = */ false,
            /* includeTags = */ true),
        R"({type_name=struct; members=[]})");
}

TEST(TypeSerialize, StructNamed) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Struct("S", {{"ItemB", NTi::String()}, {"ItemA", NTi::List(NTi::Int64())}}).Get()),
        R"({type_name=struct; members=[{name=ItemB; type=string}; {name=ItemA; type={type_name=list; item=int64}}]})");
}

TEST(TypeSerialize, TupleEmpty) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Tuple({}).Get()),
        R"({type_name=tuple; elements=[]})");
}

TEST(TypeSerialize, Tuple) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Tuple({{NTi::String()}, {NTi::List(NTi::Int64())}}).Get()),
        R"({type_name=tuple; elements=[{type=string}; {type={type_name=list; item=int64}}]})");
}

TEST(TypeSerialize, TupleNamedEmpty) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Tuple("S", {}).Get()),
        R"({type_name=tuple; elements=[]})");
}

TEST(TypeSerialize, TupleNamedEmptyNoNames) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Tuple("S", {}).Get(),
            /* binary = */ false),
        R"({type_name=tuple; elements=[]})");
}

TEST(TypeSerialize, VariantStruct) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Variant(
                NTi::Struct({{"ItemB", NTi::String()}, {"ItemA", NTi::List(NTi::Int64())}}))
                .Get()),
        R"({type_name=variant; members=[{name=ItemB; type=string}; {name=ItemA; type={type_name=list; item=int64}}]})");
}

TEST(TypeSerialize, VariantTuple) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Variant(
                NTi::Tuple({
                    {NTi::String()},
                    {NTi::List(NTi::Int64())},
                }))
                .Get()),
        R"({type_name=variant; elements=[{type=string}; {type={type_name=list; item=int64}}]})");
}

TEST(TypeSerialize, Tagged) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Tagged(NTi::String(), "Url").Get()),
        R"({type_name=tagged; tag=Url; item=string})");
}

TEST(TypeSerialize, TaggedNoTags) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Tagged(NTi::String(), "Url").Get(),
            /* binary = */ false,
            /* includeTags = */ false),
        R"(string)");
}

TEST(TypeSerialize, MultipleType) {
    auto result = TString();

    {
        auto writer = NYsonPull::MakeTextWriter(NYsonPull::NOutput::FromString(&result), NYsonPull::EStreamType::ListFragment);

        writer.BeginStream();
        NTi::NIo::SerializeYsonMultiple(NTi::Optional(NTi::String()).Get(), writer.GetConsumer());
        NTi::NIo::SerializeYsonMultiple(NTi::List(NTi::Utf8()).Get(), writer.GetConsumer());
        writer.EndStream();
    }

    ASSERT_YSON_EQ("[" + result + "]", R"([{type_name=optional; item=string}; {type_name=list; item=utf8}])");
}

TEST(TypeSerialize, Binary) {
    ASSERT_YSON_EQ(
        NTi::NIo::SerializeYson(
            NTi::Tagged(NTi::String(), "Url").Get(),
            /* binary = */ true),
        R"({type_name=tagged; tag=Url; item=string})");
}
