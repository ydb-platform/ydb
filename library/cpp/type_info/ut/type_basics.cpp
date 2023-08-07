#include <library/cpp/testing/unittest/gtest.h>

#include <library/cpp/type_info/type_info.h>

#include "utils.h"

TEST_TF(TypeBasics, Void) {
    auto t = f.Void();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Void);
    ASSERT_TRUE(t->IsVoid());
}

TEST_TF(TypeBasics, Bool) {
    auto t = f.Bool();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Bool);
    ASSERT_TRUE(t->IsBool());
}

TEST_TF(TypeBasics, Int8) {
    auto t = f.Int8();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Int8);
    ASSERT_TRUE(t->IsInt8());
}

TEST_TF(TypeBasics, Int16) {
    auto t = f.Int16();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Int16);
    ASSERT_TRUE(t->IsInt16());
}

TEST_TF(TypeBasics, Int32) {
    auto t = f.Int32();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Int32);
    ASSERT_TRUE(t->IsInt32());
}

TEST_TF(TypeBasics, Int64) {
    auto t = f.Int64();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Int64);
    ASSERT_TRUE(t->IsInt64());
}

TEST_TF(TypeBasics, Uint8) {
    auto t = f.Uint8();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Uint8);
    ASSERT_TRUE(t->IsUint8());
}

TEST_TF(TypeBasics, Uint16) {
    auto t = f.Uint16();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Uint16);
    ASSERT_TRUE(t->IsUint16());
}

TEST_TF(TypeBasics, Uint32) {
    auto t = f.Uint32();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Uint32);
    ASSERT_TRUE(t->IsUint32());
}

TEST_TF(TypeBasics, Uint64) {
    auto t = f.Uint64();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Uint64);
    ASSERT_TRUE(t->IsUint64());
}

TEST_TF(TypeBasics, Float) {
    auto t = f.Float();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Float);
    ASSERT_TRUE(t->IsFloat());
}

TEST_TF(TypeBasics, Double) {
    auto t = f.Double();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Double);
    ASSERT_TRUE(t->IsDouble());
}

TEST_TF(TypeBasics, String) {
    auto t = f.String();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::String);
    ASSERT_TRUE(t->IsString());
}

TEST_TF(TypeBasics, Utf8) {
    auto t = f.Utf8();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Utf8);
    ASSERT_TRUE(t->IsUtf8());
}

TEST_TF(TypeBasics, Date) {
    auto t = f.Date();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Date);
    ASSERT_TRUE(t->IsDate());
}

TEST_TF(TypeBasics, Datetime) {
    auto t = f.Datetime();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Datetime);
    ASSERT_TRUE(t->IsDatetime());
}

TEST_TF(TypeBasics, Timestamp) {
    auto t = f.Timestamp();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Timestamp);
    ASSERT_TRUE(t->IsTimestamp());
}

TEST_TF(TypeBasics, TzDate) {
    auto t = f.TzDate();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::TzDate);
    ASSERT_TRUE(t->IsTzDate());
}

TEST_TF(TypeBasics, TzDatetime) {
    auto t = f.TzDatetime();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::TzDatetime);
    ASSERT_TRUE(t->IsTzDatetime());
}

TEST_TF(TypeBasics, TzTimestamp) {
    auto t = f.TzTimestamp();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::TzTimestamp);
    ASSERT_TRUE(t->IsTzTimestamp());
}

TEST_TF(TypeBasics, Interval) {
    auto t = f.Interval();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Interval);
    ASSERT_TRUE(t->IsInterval());
}

TEST_TF(TypeBasics, Decimal) {
    auto t = f.Decimal(20, 10);
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Decimal);
    ASSERT_TRUE(t->IsDecimal());
    ASSERT_EQ(t->GetPrecision(), 20);
    ASSERT_EQ(t->GetScale(), 10);
}

TEST_TF(TypeBasics, Json) {
    auto t = f.Json();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Json);
    ASSERT_TRUE(t->IsJson());
}

TEST_TF(TypeBasics, Yson) {
    auto t = f.Yson();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Yson);
    ASSERT_TRUE(t->IsYson());
}

TEST_TF(TypeBasics, Uuid) {
    auto t = f.Uuid();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Uuid);
    ASSERT_TRUE(t->IsUuid());
}

TEST_TF(TypeBasics, Optional) {
    auto t = f.Optional(f.Void());
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Optional);
    ASSERT_TRUE(t->IsOptional());
    ASSERT_TRUE(t->GetItemType()->IsVoid());
    ASSERT_EQ(t->GetItemType().Get(), t->GetItemTypeRaw());
}

TEST_TF(TypeBasics, List) {
    auto t = f.List(f.Void());
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::List);
    ASSERT_TRUE(t->IsList());
    ASSERT_TRUE(t->GetItemType()->IsVoid());
    ASSERT_EQ(t->GetItemType().Get(), t->GetItemTypeRaw());
}

TEST_TF(TypeBasics, Dict) {
    auto t = f.Dict(f.Void(), f.String());
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Dict);
    ASSERT_TRUE(t->IsDict());
    ASSERT_TRUE(t->GetKeyType()->IsVoid());
    ASSERT_EQ(t->GetKeyType().Get(), t->GetKeyTypeRaw());
    ASSERT_TRUE(t->GetValueType()->IsString());
    ASSERT_EQ(t->GetValueType().Get(), t->GetValueTypeRaw());
}

TEST_TF(TypeBasics, EmptyStruct) {
    auto t = f.Struct({});
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Struct);
    ASSERT_TRUE(t->IsStruct());
    ASSERT_FALSE(t->GetName().Defined());
    ASSERT_EQ(t->GetMembers().size(), 0);
}

TEST_TF(TypeBasics, NamedEmptyStruct) {
    auto t = f.Struct("S", {});
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Struct);
    ASSERT_TRUE(t->IsStruct());
    ASSERT_TRUE(t->GetName().Defined());
    ASSERT_EQ(t->GetName().GetRef(), "S");
    ASSERT_EQ(t->GetMembers().size(), 0);
}

TEST_TF(TypeBasics, Struct) {
    auto t = f.Struct({{"a", f.Int64()}, {"b", f.Int8()}});
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Struct);
    ASSERT_TRUE(t->IsStruct());
    ASSERT_FALSE(t->GetName().Defined());
    ASSERT_EQ(t->GetMembers().size(), 2);
    ASSERT_EQ(t->GetMembers()[0].GetName(), "a");
    ASSERT_TRUE(t->GetMembers()[0].GetTypeRaw()->IsInt64());
    ASSERT_EQ(t->GetMembers()[0].GetType().Get(), t->GetMembers()[0].GetTypeRaw());
    ASSERT_EQ(t->GetMembers()[1].GetName(), "b");
    ASSERT_TRUE(t->GetMembers()[1].GetTypeRaw()->IsInt8());
    ASSERT_EQ(t->GetMembers()[1].GetType().Get(), t->GetMembers()[1].GetTypeRaw());
}

TEST_TF(TypeBasics, NamedStruct) {
    auto t = f.Struct("S", {{"a", f.Int64()}, {"b", f.Int8()}});
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Struct);
    ASSERT_TRUE(t->IsStruct());
    ASSERT_TRUE(t->GetName().Defined());
    ASSERT_EQ(t->GetName().GetRef(), "S");
    ASSERT_EQ(t->GetMembers().size(), 2);
    ASSERT_EQ(t->GetMembers()[0].GetName(), "a");
    ASSERT_TRUE(t->GetMembers()[0].GetTypeRaw()->IsInt64());
    ASSERT_EQ(t->GetMembers()[0].GetType().Get(), t->GetMembers()[0].GetTypeRaw());
    ASSERT_EQ(t->GetMembers()[1].GetName(), "b");
    ASSERT_TRUE(t->GetMembers()[1].GetTypeRaw()->IsInt8());
    ASSERT_EQ(t->GetMembers()[1].GetType().Get(), t->GetMembers()[1].GetTypeRaw());
}

TEST_TF(TypeBasics, EmptyTuple) {
    auto t = f.Tuple({});
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Tuple);
    ASSERT_TRUE(t->IsTuple());
    ASSERT_FALSE(t->GetName().Defined());
    ASSERT_EQ(t->GetElements().size(), 0);
}

TEST_TF(TypeBasics, NamedEmptyTuple) {
    auto t = f.Tuple("T", {});
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Tuple);
    ASSERT_TRUE(t->IsTuple());
    ASSERT_TRUE(t->GetName().Defined());
    ASSERT_EQ(t->GetName().GetRef(), "T");
    ASSERT_EQ(t->GetElements().size(), 0);
}

TEST_TF(TypeBasics, Tuple) {
    auto t = f.Tuple({{f.Int64()}, {f.Int8()}});
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Tuple);
    ASSERT_TRUE(t->IsTuple());
    ASSERT_FALSE(t->GetName().Defined());
    ASSERT_EQ(t->GetElements().size(), 2);
    ASSERT_TRUE(t->GetElements()[0].GetTypeRaw()->IsInt64());
    ASSERT_EQ(t->GetElements()[0].GetType().Get(), t->GetElements()[0].GetTypeRaw());
    ASSERT_TRUE(t->GetElements()[1].GetTypeRaw()->IsInt8());
    ASSERT_EQ(t->GetElements()[1].GetType().Get(), t->GetElements()[1].GetTypeRaw());
}

TEST_TF(TypeBasics, NamedTuple) {
    auto t = f.Tuple("T", {{f.Int64()}, {f.Int8()}});
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Tuple);
    ASSERT_TRUE(t->IsTuple());
    ASSERT_TRUE(t->GetName().Defined());
    ASSERT_EQ(t->GetName().GetRef(), "T");
    ASSERT_EQ(t->GetElements().size(), 2);
    ASSERT_TRUE(t->GetElements()[0].GetTypeRaw()->IsInt64());
    ASSERT_EQ(t->GetElements()[0].GetType().Get(), t->GetElements()[0].GetTypeRaw());
    ASSERT_TRUE(t->GetElements()[1].GetTypeRaw()->IsInt8());
    ASSERT_EQ(t->GetElements()[1].GetType().Get(), t->GetElements()[1].GetTypeRaw());
}

TEST_TF(TypeBasics, VariantOverStruct) {
    auto t = f.Variant(f.Struct("Inner", {{"x", f.Void()}}));
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Variant);
    ASSERT_TRUE(t->IsVariant());
    ASSERT_FALSE(t->GetName().Defined());
    ASSERT_TRUE(t->IsVariantOverStruct());
    ASSERT_FALSE(t->IsVariantOverTuple());
    ASSERT_EQ(t->GetUnderlyingType()->AsStruct()->GetName().GetRef(), "Inner");
    ASSERT_EQ(t->GetUnderlyingType().Get(), t->GetUnderlyingTypeRaw());
}

TEST_TF(TypeBasics, NamedVariantOverStruct) {
    auto t = f.Variant("V", f.Struct("Inner", {{"x", f.Void()}}));
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Variant);
    ASSERT_TRUE(t->IsVariant());
    ASSERT_TRUE(t->GetName().Defined());
    ASSERT_EQ(t->GetName().GetRef(), "V");
    ASSERT_TRUE(t->IsVariantOverStruct());
    ASSERT_FALSE(t->IsVariantOverTuple());
    ASSERT_EQ(t->GetUnderlyingType()->AsStruct()->GetName().GetRef(), "Inner");
    ASSERT_EQ(t->GetUnderlyingType().Get(), t->GetUnderlyingTypeRaw());
}

TEST_TF(TypeBasics, VariantOverTuple) {
    auto t = f.Variant(f.Tuple("Inner", {{f.Void()}}));
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Variant);
    ASSERT_TRUE(t->IsVariant());
    ASSERT_FALSE(t->GetName().Defined());
    ASSERT_FALSE(t->IsVariantOverStruct());
    ASSERT_TRUE(t->IsVariantOverTuple());
    ASSERT_EQ(t->GetUnderlyingType()->AsTuple()->GetName().GetRef(), "Inner");
    ASSERT_EQ(t->GetUnderlyingType().Get(), t->GetUnderlyingTypeRaw());
}

TEST_TF(TypeBasics, NamedVariantOverTuple) {
    auto t = f.Variant("V", f.Tuple("Inner", {{f.Void()}}));
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Variant);
    ASSERT_TRUE(t->IsVariant());
    ASSERT_TRUE(t->GetName().Defined());
    ASSERT_EQ(t->GetName().GetRef(), "V");
    ASSERT_FALSE(t->IsVariantOverStruct());
    ASSERT_TRUE(t->IsVariantOverTuple());
    ASSERT_EQ(t->GetUnderlyingType()->AsTuple()->GetName().GetRef(), "Inner");
    ASSERT_EQ(t->GetUnderlyingType().Get(), t->GetUnderlyingTypeRaw());
}

TEST_TF(TypeBasics, Tagged) {
    auto t = f.Tagged(f.Void(), "T");
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Tagged);
    ASSERT_TRUE(t->IsTagged());
    ASSERT_EQ(t->GetTag(), "T");
    ASSERT_TRUE(t->GetItemType()->IsVoid());
    ASSERT_EQ(t->GetItemType().Get(), t->GetItemTypeRaw());
}

TEST_TF(TypeBasics, EmptyStructLookupByName) {
    auto s = f.Struct({});

    ASSERT_FALSE(s->HasMember("item"));
    ASSERT_EQ(s->GetMemberIndex("item"), -1);

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        [=]() {
            s->GetMember("item");
        }(),
        NTi::TItemNotFound, "no item named 'item'");
}

TEST_TF(TypeBasics, StructLookupByName) {
    auto s = f.Struct({
        {"a", f.Void()},
        {"field2", f.String()},
        {"field1", f.Utf8()},
        {"", f.Null()},
    });

    ASSERT_TRUE(s->HasMember("a"));
    ASSERT_EQ(s->GetMemberIndex("a"), 0);

    ASSERT_TRUE(s->HasMember("field2"));
    ASSERT_EQ(s->GetMemberIndex("field2"), 1);

    ASSERT_TRUE(s->HasMember("field1"));
    ASSERT_EQ(s->GetMemberIndex("field1"), 2);

    ASSERT_TRUE(s->HasMember(""));
    ASSERT_EQ(s->GetMemberIndex(""), 3);

    ASSERT_FALSE(s->HasMember("b"));
    ASSERT_EQ(s->GetMemberIndex("b"), -1);

    ASSERT_EQ(s->GetMember("a").GetName(), "a");
    ASSERT_TRUE(s->GetMember("a").GetTypeRaw()->IsVoid());

    ASSERT_EQ(s->GetMember("field2").GetName(), "field2");
    ASSERT_TRUE(s->GetMember("field2").GetTypeRaw()->IsString());

    ASSERT_EQ(s->GetMember("field1").GetName(), "field1");
    ASSERT_TRUE(s->GetMember("field1").GetTypeRaw()->IsUtf8());

    ASSERT_EQ(s->GetMember("").GetName(), "");
    ASSERT_TRUE(s->GetMember("").GetTypeRaw()->IsNull());

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        [=]() {
            s->GetMember("b");
        }(),
        NTi::TItemNotFound, "no item named 'b'");
}
