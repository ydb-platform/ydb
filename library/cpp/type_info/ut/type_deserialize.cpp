#include <library/cpp/testing/unittest/gtest.h>

#include <library/cpp/type_info/type_info.h>

#include <library/cpp/yson_pull/yson.h>

#include "utils.h"

TEST(TypeDeserialize, Void) {
    ASSERT_DESERIALIZED_EQ(NTi::Void(), R"(void)");
    ASSERT_DESERIALIZED_EQ(NTi::Void(), R"({type_name=void})");
}

TEST(TypeDeserialize, Null) {
    ASSERT_DESERIALIZED_EQ(NTi::Null(), R"(null)");
    ASSERT_DESERIALIZED_EQ(NTi::Null(), R"({type_name=null})");
}

TEST(TypeDeserialize, Bool) {
    ASSERT_DESERIALIZED_EQ(NTi::Bool(), R"(bool)");
    ASSERT_DESERIALIZED_EQ(NTi::Bool(), R"({type_name=bool})");
}

TEST(TypeDeserialize, Int8) {
    ASSERT_DESERIALIZED_EQ(NTi::Int8(), R"(int8)");
    ASSERT_DESERIALIZED_EQ(NTi::Int8(), R"({type_name=int8})");
}

TEST(TypeDeserialize, Int16) {
    ASSERT_DESERIALIZED_EQ(NTi::Int16(), R"(int16)");
    ASSERT_DESERIALIZED_EQ(NTi::Int16(), R"({type_name=int16})");
}

TEST(TypeDeserialize, Int32) {
    ASSERT_DESERIALIZED_EQ(NTi::Int32(), R"(int32)");
    ASSERT_DESERIALIZED_EQ(NTi::Int32(), R"({type_name=int32})");
}

TEST(TypeDeserialize, Int64) {
    ASSERT_DESERIALIZED_EQ(NTi::Int64(), R"(int64)");
    ASSERT_DESERIALIZED_EQ(NTi::Int64(), R"({type_name=int64})");
}

TEST(TypeDeserialize, Uint8) {
    ASSERT_DESERIALIZED_EQ(NTi::Uint8(), R"(uint8)");
    ASSERT_DESERIALIZED_EQ(NTi::Uint8(), R"({type_name=uint8})");
}

TEST(TypeDeserialize, Uint16) {
    ASSERT_DESERIALIZED_EQ(NTi::Uint16(), R"(uint16)");
    ASSERT_DESERIALIZED_EQ(NTi::Uint16(), R"({type_name=uint16})");
}

TEST(TypeDeserialize, Uint32) {
    ASSERT_DESERIALIZED_EQ(NTi::Uint32(), R"(uint32)");
    ASSERT_DESERIALIZED_EQ(NTi::Uint32(), R"({type_name=uint32})");
}

TEST(TypeDeserialize, Uint64) {
    ASSERT_DESERIALIZED_EQ(NTi::Uint64(), R"(uint64)");
    ASSERT_DESERIALIZED_EQ(NTi::Uint64(), R"({type_name=uint64})");
}

TEST(TypeDeserialize, Float) {
    ASSERT_DESERIALIZED_EQ(NTi::Float(), R"(float)");
    ASSERT_DESERIALIZED_EQ(NTi::Float(), R"({type_name=float})");
}

TEST(TypeDeserialize, Double) {
    ASSERT_DESERIALIZED_EQ(NTi::Double(), R"(double)");
    ASSERT_DESERIALIZED_EQ(NTi::Double(), R"({type_name=double})");
}

TEST(TypeDeserialize, String) {
    ASSERT_DESERIALIZED_EQ(NTi::String(), R"(string)");
    ASSERT_DESERIALIZED_EQ(NTi::String(), R"({type_name=string})");
}

TEST(TypeDeserialize, Utf8) {
    ASSERT_DESERIALIZED_EQ(NTi::Utf8(), R"(utf8)");
    ASSERT_DESERIALIZED_EQ(NTi::Utf8(), R"({type_name=utf8})");
}

TEST(TypeDeserialize, Date) {
    ASSERT_DESERIALIZED_EQ(NTi::Date(), R"(date)");
    ASSERT_DESERIALIZED_EQ(NTi::Date(), R"({type_name=date})");
}

TEST(TypeDeserialize, Datetime) {
    ASSERT_DESERIALIZED_EQ(NTi::Datetime(), R"(datetime)");
    ASSERT_DESERIALIZED_EQ(NTi::Datetime(), R"({type_name=datetime})");
}

TEST(TypeDeserialize, Timestamp) {
    ASSERT_DESERIALIZED_EQ(NTi::Timestamp(), R"(timestamp)");
    ASSERT_DESERIALIZED_EQ(NTi::Timestamp(), R"({type_name=timestamp})");
}

TEST(TypeDeserialize, TzDate) {
    ASSERT_DESERIALIZED_EQ(NTi::TzDate(), R"(tz_date)");
    ASSERT_DESERIALIZED_EQ(NTi::TzDate(), R"({type_name=tz_date})");
}

TEST(TypeDeserialize, TzDatetime) {
    ASSERT_DESERIALIZED_EQ(NTi::TzDatetime(), R"(tz_datetime)");
    ASSERT_DESERIALIZED_EQ(NTi::TzDatetime(), R"({type_name=tz_datetime})");
}

TEST(TypeDeserialize, TzTimestamp) {
    ASSERT_DESERIALIZED_EQ(NTi::TzTimestamp(), R"(tz_timestamp)");
    ASSERT_DESERIALIZED_EQ(NTi::TzTimestamp(), R"({type_name=tz_timestamp})");
}

TEST(TypeDeserialize, Interval) {
    ASSERT_DESERIALIZED_EQ(NTi::Interval(), R"(interval)");
    ASSERT_DESERIALIZED_EQ(NTi::Interval(), R"({type_name=interval})");
}

TEST(TypeDeserialize, Decimal) {
    ASSERT_DESERIALIZED_EQ(NTi::Decimal(20, 10), R"({type_name=decimal; precision=20; scale=10})");
    ASSERT_DESERIALIZED_EQ(NTi::Decimal(20, 10), R"({scale=10; type_name=decimal; precision=20})");
    ASSERT_DESERIALIZED_EQ(NTi::Decimal(10, 10), R"({type_name=decimal; precision=10; scale=10})");
}

TEST(TypeDeserialize, DecimalMissingTypeParameters) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({precision=20; scale=10})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=decimal; scale=10})");
        }(),
        NTi::TDeserializationException, R"(missing required key "precision")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=decimal; precision=20})");
        }(),
        NTi::TDeserializationException, R"(missing required key "scale")");
}

TEST(TypeDeserialize, Json) {
    ASSERT_DESERIALIZED_EQ(NTi::Json(), R"(json)");
    ASSERT_DESERIALIZED_EQ(NTi::Json(), R"({type_name=json})");
}

TEST(TypeDeserialize, Yson) {
    ASSERT_DESERIALIZED_EQ(NTi::Yson(), R"(yson)");
    ASSERT_DESERIALIZED_EQ(NTi::Yson(), R"({type_name=yson})");
}

TEST(TypeDeserialize, Uuid) {
    ASSERT_DESERIALIZED_EQ(NTi::Uuid(), R"(uuid)");
    ASSERT_DESERIALIZED_EQ(NTi::Uuid(), R"({type_name=uuid})");
}

TEST(TypeDeserialize, Optional) {
    ASSERT_DESERIALIZED_EQ(NTi::Optional(NTi::Void()), R"({type_name=optional; item=void})");
    ASSERT_DESERIALIZED_EQ(NTi::Optional(NTi::String()), R"({type_name=optional; item=string})");
    ASSERT_DESERIALIZED_EQ(NTi::Optional(NTi::String()), R"({item=string; type_name=optional})");
}

TEST(TypeDeserialize, OptionalMissingTypeParameters) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({item=string})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=optional})");
        }(),
        NTi::TDeserializationException, R"(missing required key "item")");
}

TEST(TypeDeserialize, List) {
    ASSERT_DESERIALIZED_EQ(NTi::List(NTi::Void()), R"({type_name=list; item=void})");
    ASSERT_DESERIALIZED_EQ(NTi::List(NTi::String()), R"({type_name=list; item=string})");
    ASSERT_DESERIALIZED_EQ(NTi::List(NTi::String()), R"({item=string; type_name=list})");
}

TEST(TypeDeserialize, ListMissingTypeParameters) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({item=string})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=list})");
        }(),
        NTi::TDeserializationException, R"(missing required key "item")");
}

TEST(TypeDeserialize, Dict) {
    ASSERT_DESERIALIZED_EQ(NTi::Dict(NTi::Void(), NTi::Void()), R"({type_name=dict; key=void; value=void})");
    ASSERT_DESERIALIZED_EQ(NTi::Dict(NTi::Int32(), NTi::String()), R"({type_name=dict; key=int32; value=string})");
    ASSERT_DESERIALIZED_EQ(NTi::Dict(NTi::Int32(), NTi::String()), R"({key=int32; value=string; type_name=dict})");
    ASSERT_DESERIALIZED_EQ(NTi::Dict(NTi::Int32(), NTi::String()), R"({value=string; key=int32; type_name=dict})");
}

TEST(TypeDeserialize, DictMissingTypeParameters) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({key=string; value=string})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=dict})");
        }(),
        NTi::TDeserializationException, R"(missing required keys "key" and "value")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=dict; value=string})");
        }(),
        NTi::TDeserializationException, R"(missing required key "key")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=dict; key=string})");
        }(),
        NTi::TDeserializationException, R"(missing required key "value")");
}

TEST(TypeDeserialize, StructEmpty) {
    ASSERT_DESERIALIZED_EQ(
        NTi::Struct({}),
        R"({type_name=struct; members=[]})");
    ASSERT_DESERIALIZED_EQ(
        NTi::Struct({}),
        R"({members=[]; type_name=struct})");
}

TEST(TypeDeserialize, Struct) {
    auto ty = NTi::Struct({{"ItemB", NTi::String()}, {"ItemA", NTi::List(NTi::Int64())}});

    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({type_name=struct; members=[{name=ItemB; type=string}; {name=ItemA; type={type_name=list; item=int64}}]})");
    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({members=[{name=ItemB; type=string}; {name=ItemA; type={type_name=list; item=int64}}]; type_name=struct})");
    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({type_name=struct; members=[{type=string; name=ItemB}; {name=ItemA; type={item=int64; type_name=list}}]})");
    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({type_name=struct; members=[{name=ItemB; type=string}; {name=ItemA; type={type_name=list; item=int64}}]; name=#})");
    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({type_name=struct; name=#; members=[{name=ItemB; type=string}; {name=ItemA; type={type_name=list; item=int64}}]})");
}

TEST(TypeDeserialize, StructMissingTypeParameters) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({members=[]})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=struct})");
        }(),
        NTi::TDeserializationException, R"(missing required key "members")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=struct; name=S})");
        }(),
        NTi::TDeserializationException, R"(missing required key "members")");
}

TEST(TypeDeserialize, TupleEmpty) {
    ASSERT_DESERIALIZED_EQ(
        NTi::Tuple({}),
        R"({type_name=tuple; elements=[]})");

    ASSERT_DESERIALIZED_EQ(
        NTi::Tuple({}),
        R"({elements=[]; type_name=tuple})");
}

TEST(TypeDeserialize, Tuple) {
    auto ty = NTi::Tuple({{NTi::String()}, {NTi::List(NTi::Int64())}});

    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({type_name=tuple; elements=[{type=string}; {type={type_name=list; item=int64}}]})");
    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({elements=[{type=string}; {type={item=int64; type_name=list}}]; type_name=tuple})");
    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({type_name=tuple; name=#; elements=[{type=string}; {type={type_name=list; item=int64}}]})");
    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({type_name=tuple; elements=[{type=string}; {type={type_name=list; item=int64}}]; name=#})");
}

TEST(TypeDeserialize, TupleMissingTypeParameters) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({elements=[]})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=tuple})");
        }(),
        NTi::TDeserializationException, R"(missing required key "elements")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=tuple; name=T})");
        }(),
        NTi::TDeserializationException, R"(missing required key "elements")");
}

TEST(TypeDeserialize, VariantStruct) {
    auto ty = NTi::Variant(NTi::Struct({{"ItemB", NTi::String()}, {"ItemA", NTi::List(NTi::Int64())}}));

    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({type_name=variant; members=[{name=ItemB; type=string}; {name=ItemA; type={type_name=list; item=int64}}]})");
    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({members=[{name=ItemB; type=string}; {name=ItemA; type={type_name=list; item=int64}}]; type_name=variant})");
    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({type_name=variant; name=#; members=[{name=ItemB; type=string}; {name=ItemA; type={type_name=list; item=int64}}]})");
    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({type_name=variant; members=[{name=ItemB; type=string}; {name=ItemA; type={type_name=list; item=int64}}]; name=#})");
}

TEST(TypeDeserialize, VariantTuple) {
    auto ty = NTi::Variant(NTi::Tuple({{NTi::String()}, {NTi::List(NTi::Int64())}}));

    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({type_name=variant; elements=[{type=string}; {type={type_name=list; item=int64}}]})");
    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({elements=[{type=string}; {type={type_name=list; item=int64}}]; type_name=variant})");
    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({type_name=variant; name=#; elements=[{type=string}; {type={type_name=list; item=int64}}]})");
    ASSERT_DESERIALIZED_EQ(
        ty,
        R"({type_name=variant; elements=[{type=string}; {type={type_name=list; item=int64}}]; name=#})");
}

TEST(TypeDeserialize, VariantMissingTypeParameters) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({elements=[]})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({name=X})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=variant})");
        }(),
        NTi::TDeserializationException, R"(missing both keys "members" and "elements")");
}

TEST(TypeDeserialize, Tagged) {
    ASSERT_DESERIALIZED_EQ(
        NTi::Tagged(NTi::String(), "Url"),
        R"({type_name=tagged; tag=Url; item=string})");
    ASSERT_DESERIALIZED_EQ(
        NTi::Tagged(NTi::String(), "Url"),
        R"({type_name=tagged; item=string; tag=Url})");
    ASSERT_DESERIALIZED_EQ(
        NTi::Tagged(NTi::String(), "Url"),
        R"({item=string; tag=Url; type_name=tagged})");
}

TEST(TypeDeserialize, TaggedMissingTypeParameters) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({tag=T; item=string})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({tag=T})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=tagged; tag=T})");
        }(),
        NTi::TDeserializationException, R"(missing required key "item")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({type_name=tagged; item=string})");
        }(),
        NTi::TDeserializationException, R"(missing required key "tag")");
}

TEST(TypeDeserialize, ComplexTypeAsString) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"(decimal)");
        }(),
        NTi::TDeserializationException, R"(missing required keys "precision" and "scale")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"(optional)");
        }(),
        NTi::TDeserializationException, R"(missing required key "item")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"(list)");
        }(),
        NTi::TDeserializationException, R"(missing required key "item")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"(dict)");
        }(),
        NTi::TDeserializationException, R"(missing required keys "key" and "value")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"(struct)");
        }(),
        NTi::TDeserializationException, R"(missing required key "members")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"(tuple)");
        }(),
        NTi::TDeserializationException, R"(missing required key "elements")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"(variant)");
        }(),
        NTi::TDeserializationException, R"(missing both keys "members" and "elements")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"(tagged)");
        }(),
        NTi::TDeserializationException, R"(missing required keys "tag" and "item")");
}

TEST(TypeDeserialize, MissingTypeName) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({item=string})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({tag=Url})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        []() {
            NTi::NIo::DeserializeYson(*NTi::HeapFactory(), R"({key=string; value=int32})");
        }(),
        NTi::TDeserializationException, R"(missing required key "type_name")");
}

TEST(TypeDeserialize, UnknownKeys) {
    auto tupleType = NTi::Tuple({{NTi::String()}, {NTi::List(NTi::Int64())}});

    ASSERT_DESERIALIZED_EQ(
        tupleType,
        R"({type_name=tuple; unknown_key=<>0; elements=[{type=string}; {type={type_name=list; item=int64}}]})");

    ASSERT_DESERIALIZED_EQ(
        tupleType,
        R"({type_name=tuple; elements=[{unknown_key={foo=<>0}; type=string}; {type={type_name=list; item=int64}}]})");

    auto structType = NTi::Struct({{"ItemB", NTi::String()}, {"ItemA", NTi::List(NTi::Int64())}});
    ASSERT_DESERIALIZED_EQ(
        structType,
        R"({type_name=struct; unknown_key=[]; members=[{name=ItemB; type=string}; {name=ItemA; type={type_name=list; item=int64}}]})");
    ASSERT_DESERIALIZED_EQ(
        structType,
        R"({type_name=struct; members=[{name=ItemB; unknown_key=foo; type=string}; {name=ItemA; type={type_name=list; item=int64}}]})");

    auto utf8Type = NTi::Utf8();
    ASSERT_DESERIALIZED_EQ(
        utf8Type,
        R"({type_name=utf8; unknown_key=[];})");
}


TEST(TypeDeserialize, DeepType) {
    auto ty = TStringBuilder();
    for (size_t i = 0; i < 100; ++i)
        ty << "{type_name=optional; item=";
    ty << "string";
    for (size_t i = 0; i < 100; ++i)
        ty << "}";

    UNIT_ASSERT_EXCEPTION_CONTAINS([&ty]() {
        NTi::NIo::DeserializeYson(*NTi::HeapFactory(), ty);
    }(),
                                   NTi::TDeserializationException, R"(too deep)");
}

TEST(TypeDeserialize, MultipleTypes) {
    auto ty = "{type_name=optional; item=string}; {type_name=list; item=utf8}";

    auto reader = NYsonPull::TReader(NYsonPull::NInput::FromMemory(ty), NYsonPull::EStreamType::ListFragment);
    auto factory = NTi::PoolFactory();

    {
        auto ty1 = NTi::Optional(NTi::String());
        ASSERT_STRICT_EQ(NTi::NIo::DeserializeYsonMultipleRaw(*factory, reader), ty1.Get());
    }

    {
        auto ty2 = NTi::List(NTi::Utf8());
        ASSERT_STRICT_EQ(NTi::NIo::DeserializeYsonMultipleRaw(*factory, reader), ty2.Get());
    }

    ASSERT_EQ(NTi::NIo::DeserializeYsonMultipleRaw(*factory, reader), nullptr);
}
