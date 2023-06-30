#include <library/cpp/testing/unittest/gtest.h>

#include <library/cpp/type_info/type_info.h>

TEST(TypeFactoryRaw, Void) {
    auto f = NTi::PoolFactory(false);
    auto t = f->VoidRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Void);
    ASSERT_TRUE(t->IsVoid());
}

TEST(TypeFactoryRaw, Bool) {
    auto f = NTi::PoolFactory(false);
    auto t = f->BoolRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Bool);
    ASSERT_TRUE(t->IsBool());
}

TEST(TypeFactoryRaw, Int8) {
    auto f = NTi::PoolFactory(false);
    auto t = f->Int8Raw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Int8);
    ASSERT_TRUE(t->IsInt8());
}

TEST(TypeFactoryRaw, Int16) {
    auto f = NTi::PoolFactory(false);
    auto t = f->Int16Raw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Int16);
    ASSERT_TRUE(t->IsInt16());
}

TEST(TypeFactoryRaw, Int32) {
    auto f = NTi::PoolFactory(false);
    auto t = f->Int32Raw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Int32);
    ASSERT_TRUE(t->IsInt32());
}

TEST(TypeFactoryRaw, Int64) {
    auto f = NTi::PoolFactory(false);
    auto t = f->Int64Raw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Int64);
    ASSERT_TRUE(t->IsInt64());
}

TEST(TypeFactoryRaw, Uint8) {
    auto f = NTi::PoolFactory(false);
    auto t = f->Uint8Raw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Uint8);
    ASSERT_TRUE(t->IsUint8());
}

TEST(TypeFactoryRaw, Uint16) {
    auto f = NTi::PoolFactory(false);
    auto t = f->Uint16Raw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Uint16);
    ASSERT_TRUE(t->IsUint16());
}

TEST(TypeFactoryRaw, Uint32) {
    auto f = NTi::PoolFactory(false);
    auto t = f->Uint32Raw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Uint32);
    ASSERT_TRUE(t->IsUint32());
}

TEST(TypeFactoryRaw, Uint64) {
    auto f = NTi::PoolFactory(false);
    auto t = f->Uint64Raw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Uint64);
    ASSERT_TRUE(t->IsUint64());
}

TEST(TypeFactoryRaw, Float) {
    auto f = NTi::PoolFactory(false);
    auto t = f->FloatRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Float);
    ASSERT_TRUE(t->IsFloat());
}

TEST(TypeFactoryRaw, Double) {
    auto f = NTi::PoolFactory(false);
    auto t = f->DoubleRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Double);
    ASSERT_TRUE(t->IsDouble());
}

TEST(TypeFactoryRaw, String) {
    auto f = NTi::PoolFactory(false);
    auto t = f->StringRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::String);
    ASSERT_TRUE(t->IsString());
}

TEST(TypeFactoryRaw, Utf8) {
    auto f = NTi::PoolFactory(false);
    auto t = f->Utf8Raw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Utf8);
    ASSERT_TRUE(t->IsUtf8());
}

TEST(TypeFactoryRaw, Date) {
    auto f = NTi::PoolFactory(false);
    auto t = f->DateRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Date);
    ASSERT_TRUE(t->IsDate());
}

TEST(TypeFactoryRaw, Datetime) {
    auto f = NTi::PoolFactory(false);
    auto t = f->DatetimeRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Datetime);
    ASSERT_TRUE(t->IsDatetime());
}

TEST(TypeFactoryRaw, Timestamp) {
    auto f = NTi::PoolFactory(false);
    auto t = f->TimestampRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Timestamp);
    ASSERT_TRUE(t->IsTimestamp());
}

TEST(TypeFactoryRaw, TzDate) {
    auto f = NTi::PoolFactory(false);
    auto t = f->TzDateRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::TzDate);
    ASSERT_TRUE(t->IsTzDate());
}

TEST(TypeFactoryRaw, TzDatetime) {
    auto f = NTi::PoolFactory(false);
    auto t = f->TzDatetimeRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::TzDatetime);
    ASSERT_TRUE(t->IsTzDatetime());
}

TEST(TypeFactoryRaw, TzTimestamp) {
    auto f = NTi::PoolFactory(false);
    auto t = f->TzTimestampRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::TzTimestamp);
    ASSERT_TRUE(t->IsTzTimestamp());
}

TEST(TypeFactoryRaw, Interval) {
    auto f = NTi::PoolFactory(false);
    auto t = f->IntervalRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Interval);
    ASSERT_TRUE(t->IsInterval());
}

TEST(TypeFactoryRaw, Decimal) {
    auto f = NTi::PoolFactory(false);
    auto t = f->DecimalRaw(20, 10);
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Decimal);
    ASSERT_TRUE(t->IsDecimal());
    ASSERT_EQ(t->GetPrecision(), 20);
    ASSERT_EQ(t->GetScale(), 10);
}

TEST(TypeFactoryRaw, Json) {
    auto f = NTi::PoolFactory(false);
    auto t = f->JsonRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Json);
    ASSERT_TRUE(t->IsJson());
}

TEST(TypeFactoryRaw, Yson) {
    auto f = NTi::PoolFactory(false);
    auto t = f->YsonRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Yson);
    ASSERT_TRUE(t->IsYson());
}

TEST(TypeFactoryRaw, Uuid) {
    auto f = NTi::PoolFactory(false);
    auto t = f->UuidRaw();
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Uuid);
    ASSERT_TRUE(t->IsUuid());
}

TEST(TypeFactoryRaw, Optional) {
    auto f = NTi::PoolFactory(false);
    auto t = f->OptionalRaw(f->VoidRaw());
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Optional);
    ASSERT_TRUE(t->IsOptional());
    ASSERT_TRUE(t->GetItemType()->IsVoid());
    ASSERT_EQ(t->GetItemType().Get(), t->GetItemTypeRaw());
}

TEST(TypeFactoryRaw, List) {
    auto f = NTi::PoolFactory(false);
    auto t = f->ListRaw(f->VoidRaw());
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::List);
    ASSERT_TRUE(t->IsList());
    ASSERT_TRUE(t->GetItemType()->IsVoid());
    ASSERT_EQ(t->GetItemType().Get(), t->GetItemTypeRaw());
}

TEST(TypeFactoryRaw, Dict) {
    auto f = NTi::PoolFactory(false);
    auto t = f->DictRaw(f->VoidRaw(), f->StringRaw());
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Dict);
    ASSERT_TRUE(t->IsDict());
    ASSERT_TRUE(t->GetKeyType()->IsVoid());
    ASSERT_EQ(t->GetKeyType().Get(), t->GetKeyTypeRaw());
    ASSERT_TRUE(t->GetValueType()->IsString());
    ASSERT_EQ(t->GetValueType().Get(), t->GetValueTypeRaw());
}

TEST(TypeFactoryRaw, EmptyStruct) {
    auto f = NTi::PoolFactory(false);
    auto t = f->StructRaw({});
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Struct);
    ASSERT_TRUE(t->IsStruct());
    ASSERT_FALSE(t->GetName().Defined());
    ASSERT_EQ(t->GetMembers().size(), 0);
}

TEST(TypeFactoryRaw, NamedEmptyStruct) {
    auto f = NTi::PoolFactory(false);
    auto t = f->StructRaw("S", {});
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Struct);
    ASSERT_TRUE(t->IsStruct());
    ASSERT_TRUE(t->GetName().Defined());
    ASSERT_EQ(t->GetName().GetRef(), "S");
    ASSERT_EQ(t->GetMembers().size(), 0);
}

TEST(TypeFactoryRaw, Struct) {
    auto f = NTi::PoolFactory(false);
    auto t = f->StructRaw({{"a", f->Int64Raw()}, {"b", f->Int8Raw()}});
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

TEST(TypeFactoryRaw, NamedStruct) {
    auto f = NTi::PoolFactory(false);
    auto t = f->StructRaw("S", {{"a", f->Int64Raw()}, {"b", f->Int8Raw()}});
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

TEST(TypeFactoryRaw, EmptyTuple) {
    auto f = NTi::PoolFactory(false);
    auto t = f->TupleRaw({});
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Tuple);
    ASSERT_TRUE(t->IsTuple());
    ASSERT_FALSE(t->GetName().Defined());
    ASSERT_EQ(t->GetElements().size(), 0);
}

TEST(TypeFactoryRaw, NamedEmptyTuple) {
    auto f = NTi::PoolFactory(false);
    auto t = f->TupleRaw("T", {});
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Tuple);
    ASSERT_TRUE(t->IsTuple());
    ASSERT_TRUE(t->GetName().Defined());
    ASSERT_EQ(t->GetName().GetRef(), "T");
    ASSERT_EQ(t->GetElements().size(), 0);
}

TEST(TypeFactoryRaw, Tuple) {
    auto f = NTi::PoolFactory(false);
    auto t = f->TupleRaw({{f->Int64Raw()}, {f->Int8Raw()}});
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Tuple);
    ASSERT_TRUE(t->IsTuple());
    ASSERT_FALSE(t->GetName().Defined());
    ASSERT_EQ(t->GetElements().size(), 2);
    ASSERT_TRUE(t->GetElements()[0].GetTypeRaw()->IsInt64());
    ASSERT_EQ(t->GetElements()[0].GetType().Get(), t->GetElements()[0].GetTypeRaw());
    ASSERT_TRUE(t->GetElements()[1].GetTypeRaw()->IsInt8());
    ASSERT_EQ(t->GetElements()[1].GetType().Get(), t->GetElements()[1].GetTypeRaw());
}

TEST(TypeFactoryRaw, NamedTuple) {
    auto f = NTi::PoolFactory(false);
    auto t = f->TupleRaw("T", {{f->Int64Raw()}, {f->Int8Raw()}});
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

TEST(TypeFactoryRaw, VariantOverStruct) {
    auto f = NTi::PoolFactory(false);
    auto t = f->VariantRaw(f->StructRaw("Inner", {{"x", f->VoidRaw()}}));
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Variant);
    ASSERT_TRUE(t->IsVariant());
    ASSERT_FALSE(t->GetName().Defined());
    ASSERT_TRUE(t->IsVariantOverStruct());
    ASSERT_FALSE(t->IsVariantOverTuple());
    ASSERT_EQ(t->GetUnderlyingType()->AsStruct()->GetName().GetRef(), "Inner");
    ASSERT_EQ(t->GetUnderlyingType().Get(), t->GetUnderlyingTypeRaw());
}

TEST(TypeFactoryRaw, NamedVariantOverStruct) {
    auto f = NTi::PoolFactory(false);
    auto t = f->VariantRaw("V", f->StructRaw("Inner", {{"x", f->VoidRaw()}}));
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Variant);
    ASSERT_TRUE(t->IsVariant());
    ASSERT_TRUE(t->GetName().Defined());
    ASSERT_EQ(t->GetName().GetRef(), "V");
    ASSERT_TRUE(t->IsVariantOverStruct());
    ASSERT_FALSE(t->IsVariantOverTuple());
    ASSERT_EQ(t->GetUnderlyingType()->AsStruct()->GetName().GetRef(), "Inner");
    ASSERT_EQ(t->GetUnderlyingType().Get(), t->GetUnderlyingTypeRaw());
}

TEST(TypeFactoryRaw, VariantOverTuple) {
    auto f = NTi::PoolFactory(false);
    auto t = f->VariantRaw(f->TupleRaw("Inner", {{f->VoidRaw()}}));
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Variant);
    ASSERT_TRUE(t->IsVariant());
    ASSERT_FALSE(t->GetName().Defined());
    ASSERT_FALSE(t->IsVariantOverStruct());
    ASSERT_TRUE(t->IsVariantOverTuple());
    ASSERT_EQ(t->GetUnderlyingType()->AsTuple()->GetName().GetRef(), "Inner");
    ASSERT_EQ(t->GetUnderlyingType().Get(), t->GetUnderlyingTypeRaw());
}

TEST(TypeFactoryRaw, NamedVariantOverTuple) {
    auto f = NTi::PoolFactory(false);
    auto t = f->VariantRaw("V", f->TupleRaw("Inner", {{f->VoidRaw()}}));
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Variant);
    ASSERT_TRUE(t->IsVariant());
    ASSERT_TRUE(t->GetName().Defined());
    ASSERT_EQ(t->GetName().GetRef(), "V");
    ASSERT_FALSE(t->IsVariantOverStruct());
    ASSERT_TRUE(t->IsVariantOverTuple());
    ASSERT_EQ(t->GetUnderlyingType()->AsTuple()->GetName().GetRef(), "Inner");
    ASSERT_EQ(t->GetUnderlyingType().Get(), t->GetUnderlyingTypeRaw());
}

TEST(TypeFactoryRaw, Tagged) {
    auto f = NTi::PoolFactory(false);
    auto t = f->TaggedRaw(f->VoidRaw(), "T");
    ASSERT_EQ(t->GetTypeName(), NTi::ETypeName::Tagged);
    ASSERT_TRUE(t->IsTagged());
    ASSERT_EQ(t->GetTag(), "T");
    ASSERT_TRUE(t->GetItemType()->IsVoid());
    ASSERT_EQ(t->GetItemType().Get(), t->GetItemTypeRaw());
}
