#include <library/cpp/testing/unittest/gtest.h>

#include <library/cpp/type_info/type_info.h>

#include "utils.h"

TEST_TF(TypeShow, Void) {
    ASSERT_EQ(ToString(*f.Void()), "Void");
}

TEST_TF(TypeShow, Null) {
    ASSERT_EQ(ToString(*f.Null()), "Null");
}

TEST_TF(TypeShow, Bool) {
    ASSERT_EQ(ToString(*f.Bool()), "Bool");
}

TEST_TF(TypeShow, Int8) {
    ASSERT_EQ(ToString(*f.Int8()), "Int8");
}

TEST_TF(TypeShow, Int16) {
    ASSERT_EQ(ToString(*f.Int16()), "Int16");
}

TEST_TF(TypeShow, Int32) {
    ASSERT_EQ(ToString(*f.Int32()), "Int32");
}

TEST_TF(TypeShow, Int64) {
    ASSERT_EQ(ToString(*f.Int64()), "Int64");
}

TEST_TF(TypeShow, Uint8) {
    ASSERT_EQ(ToString(*f.Uint8()), "Uint8");
}

TEST_TF(TypeShow, Uint16) {
    ASSERT_EQ(ToString(*f.Uint16()), "Uint16");
}

TEST_TF(TypeShow, Uint32) {
    ASSERT_EQ(ToString(*f.Uint32()), "Uint32");
}

TEST_TF(TypeShow, Uint64) {
    ASSERT_EQ(ToString(*f.Uint64()), "Uint64");
}

TEST_TF(TypeShow, Float) {
    ASSERT_EQ(ToString(*f.Float()), "Float");
}

TEST_TF(TypeShow, Double) {
    ASSERT_EQ(ToString(*f.Double()), "Double");
}

TEST_TF(TypeShow, String) {
    ASSERT_EQ(ToString(*f.String()), "String");
}

TEST_TF(TypeShow, Utf8) {
    ASSERT_EQ(ToString(*f.Utf8()), "Utf8");
}

TEST_TF(TypeShow, Date) {
    ASSERT_EQ(ToString(*f.Date()), "Date");
}

TEST_TF(TypeShow, Datetime) {
    ASSERT_EQ(ToString(*f.Datetime()), "Datetime");
}

TEST_TF(TypeShow, Timestamp) {
    ASSERT_EQ(ToString(*f.Timestamp()), "Timestamp");
}

TEST_TF(TypeShow, TzDate) {
    ASSERT_EQ(ToString(*f.TzDate()), "TzDate");
}

TEST_TF(TypeShow, TzDatetime) {
    ASSERT_EQ(ToString(*f.TzDatetime()), "TzDatetime");
}

TEST_TF(TypeShow, TzTimestamp) {
    ASSERT_EQ(ToString(*f.TzTimestamp()), "TzTimestamp");
}

TEST_TF(TypeShow, Interval) {
    ASSERT_EQ(ToString(*f.Interval()), "Interval");
}

TEST_TF(TypeShow, Decimal) {
    ASSERT_EQ(ToString(*f.Decimal(20, 10)), "Decimal(20, 10)");
}

TEST_TF(TypeShow, Json) {
    ASSERT_EQ(ToString(*f.Json()), "Json");
}

TEST_TF(TypeShow, Yson) {
    ASSERT_EQ(ToString(*f.Yson()), "Yson");
}

TEST_TF(TypeShow, Uuid) {
    ASSERT_EQ(ToString(*f.Uuid()), "Uuid");
}

TEST_TF(TypeShow, Optional) {
    ASSERT_EQ(ToString(*f.Optional(f.String())), "Optional<String>");
}

TEST_TF(TypeShow, List) {
    ASSERT_EQ(ToString(*f.List(f.String())), "List<String>");
}

TEST_TF(TypeShow, Dict) {
    ASSERT_EQ(ToString(*f.Dict(f.String(), f.Int32())), "Dict<String, Int32>");
}

TEST_TF(TypeShow, Struct) {
    ASSERT_EQ(
        ToString(*f.Struct({})),
        "Struct<>");
    ASSERT_EQ(
        ToString(*f.Struct({{"x1", f.Void()}})),
        "Struct<'x1': Void>");
    ASSERT_EQ(
        ToString(*f.Struct({{"x1", f.Void()}, {"x2", f.String()}})),
        "Struct<'x1': Void, 'x2': String>");
    ASSERT_EQ(
        ToString(*f.Struct("Name", {})),
        "Struct['Name']<>");
    ASSERT_EQ(
        ToString(*f.Struct("Name", {{"x1", f.Void()}})),
        "Struct['Name']<'x1': Void>");
    ASSERT_EQ(
        ToString(*f.Struct("Name", {{"x1", f.Void()}, {"x2", f.String()}})),
        "Struct['Name']<'x1': Void, 'x2': String>");
}

TEST_TF(TypeShow, Tuple) {
    ASSERT_EQ(
        ToString(*f.Tuple({})),
        "Tuple<>");
    ASSERT_EQ(
        ToString(*f.Tuple({{f.Void()}})),
        "Tuple<Void>");
    ASSERT_EQ(
        ToString(*f.Tuple({{f.Void()}, {f.String()}})),
        "Tuple<Void, String>");
    ASSERT_EQ(
        ToString(*f.Tuple("Name", {})),
        "Tuple['Name']<>");
    ASSERT_EQ(
        ToString(*f.Tuple("Name", {{f.Void()}})),
        "Tuple['Name']<Void>");
    ASSERT_EQ(
        ToString(*f.Tuple("Name", {{f.Void()}, {f.String()}})),
        "Tuple['Name']<Void, String>");
}

TEST_TF(TypeShow, VariantStruct) {
    ASSERT_EQ(
        ToString(*f.Variant(f.Struct({{"x1", f.Void()}}))),
        "Variant<'x1': Void>");
    ASSERT_EQ(
        ToString(*f.Variant(f.Struct({{"x1", f.Void()}, {"x2", f.String()}}))),
        "Variant<'x1': Void, 'x2': String>");
    ASSERT_EQ(
        ToString(*f.Variant("Name", f.Struct({{"x1", f.Void()}}))),
        "Variant['Name']<'x1': Void>");
    ASSERT_EQ(
        ToString(*f.Variant("Name", f.Struct({{"x1", f.Void()}, {"x2", f.String()}}))),
        "Variant['Name']<'x1': Void, 'x2': String>");
}

TEST_TF(TypeShow, VariantTuple) {
    ASSERT_EQ(
        ToString(*f.Variant(f.Tuple({{f.Void()}}))),
        "Variant<Void>");
    ASSERT_EQ(
        ToString(*f.Variant(f.Tuple({{f.Void()}, {f.String()}}))),
        "Variant<Void, String>");
    ASSERT_EQ(
        ToString(*f.Variant("Name", f.Tuple({{f.Void()}}))),
        "Variant['Name']<Void>");
    ASSERT_EQ(
        ToString(*f.Variant("Name", f.Tuple({{f.Void()}, {f.String()}}))),
        "Variant['Name']<Void, String>");
}

TEST_TF(TypeShow, Tagged) {
    ASSERT_EQ(
        ToString(*f.Tagged(f.Void(), "Tag")),
        "Tagged<Void, 'Tag'>");
}
