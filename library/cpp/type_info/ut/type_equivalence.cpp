#include <library/cpp/testing/unittest/gtest.h>

#include <library/cpp/type_info/type_info.h>

#include "utils.h"

TEST_TF(TypeEquivalence, StrictEqSelf) {
    ASSERT_STRICT_EQ(
        f.Void(),
        f.Void());
    ASSERT_STRICT_EQ(
        f.Bool(),
        f.Bool());
    ASSERT_STRICT_EQ(
        f.Int8(),
        f.Int8());
    ASSERT_STRICT_EQ(
        f.Int16(),
        f.Int16());
    ASSERT_STRICT_EQ(
        f.Int32(),
        f.Int32());
    ASSERT_STRICT_EQ(
        f.Int64(),
        f.Int64());
    ASSERT_STRICT_EQ(
        f.Uint8(),
        f.Uint8());
    ASSERT_STRICT_EQ(
        f.Uint16(),
        f.Uint16());
    ASSERT_STRICT_EQ(
        f.Uint32(),
        f.Uint32());
    ASSERT_STRICT_EQ(
        f.Uint64(),
        f.Uint64());
    ASSERT_STRICT_EQ(
        f.Float(),
        f.Float());
    ASSERT_STRICT_EQ(
        f.Double(),
        f.Double());
    ASSERT_STRICT_EQ(
        f.String(),
        f.String());
    ASSERT_STRICT_EQ(
        f.Utf8(),
        f.Utf8());
    ASSERT_STRICT_EQ(
        f.Date(),
        f.Date());
    ASSERT_STRICT_EQ(
        f.Datetime(),
        f.Datetime());
    ASSERT_STRICT_EQ(
        f.Timestamp(),
        f.Timestamp());
    ASSERT_STRICT_EQ(
        f.TzDate(),
        f.TzDate());
    ASSERT_STRICT_EQ(
        f.TzDatetime(),
        f.TzDatetime());
    ASSERT_STRICT_EQ(
        f.TzTimestamp(),
        f.TzTimestamp());
    ASSERT_STRICT_EQ(
        f.Interval(),
        f.Interval());
    ASSERT_STRICT_EQ(
        f.Decimal(20, 10),
        f.Decimal(20, 10));
    ASSERT_STRICT_EQ(
        f.Json(),
        f.Json());
    ASSERT_STRICT_EQ(
        f.Yson(),
        f.Yson());
    ASSERT_STRICT_EQ(
        f.Uuid(),
        f.Uuid());
    ASSERT_STRICT_EQ(
        f.Optional(f.Void()),
        f.Optional(f.Void()));
    ASSERT_STRICT_EQ(
        f.List(f.Void()),
        f.List(f.Void()));
    ASSERT_STRICT_EQ(
        f.Dict(f.Void(), f.String()),
        f.Dict(f.Void(), f.String()));
    ASSERT_STRICT_EQ(
        f.Struct({}),
        f.Struct({}));
    ASSERT_STRICT_EQ(
        f.Struct("S", {}),
        f.Struct("S", {}));
    ASSERT_STRICT_EQ(
        f.Struct({{"a", f.Int64()}, {"b", f.Int8()}}),
        f.Struct({{"a", f.Int64()}, {"b", f.Int8()}}));
    ASSERT_STRICT_EQ(
        f.Struct("S", {{"a", f.Int64()}, {"b", f.Int8()}}),
        f.Struct("S", {{"a", f.Int64()}, {"b", f.Int8()}}));
    ASSERT_STRICT_EQ(
        f.Tuple({}),
        f.Tuple({}));
    ASSERT_STRICT_EQ(
        f.Tuple("T", {}),
        f.Tuple("T", {}));
    ASSERT_STRICT_EQ(
        f.Tuple({{f.Int64()}, {f.Int8()}}),
        f.Tuple({{f.Int64()}, {f.Int8()}}));
    ASSERT_STRICT_EQ(
        f.Tuple("T", {{f.Int64()}, {f.Int8()}}),
        f.Tuple("T", {{f.Int64()}, {f.Int8()}}));
    ASSERT_STRICT_EQ(
        f.Variant(f.Struct("Inner", {{"x", f.Void()}})),
        f.Variant(f.Struct("Inner", {{"x", f.Void()}})));
    ASSERT_STRICT_EQ(
        f.Variant("V", f.Struct("Inner", {{"x", f.Void()}})),
        f.Variant("V", f.Struct("Inner", {{"x", f.Void()}})));
    ASSERT_STRICT_EQ(
        f.Variant(f.Tuple("Inner", {{f.Void()}})),
        f.Variant(f.Tuple("Inner", {{f.Void()}})));
    ASSERT_STRICT_EQ(
        f.Variant("V", f.Tuple("Inner", {{f.Void()}})),
        f.Variant("V", f.Tuple("Inner", {{f.Void()}})));
    ASSERT_STRICT_EQ(
        f.Tagged(f.Void(), "T"),
        f.Tagged(f.Void(), "T"));
}

TEST_TF(TypeEquivalence, StrictNeOtherType) {
    ASSERT_STRICT_NE(
        f.Void(),
        f.Bool());
    ASSERT_STRICT_NE(
        f.Bool(),
        f.Int8());
    ASSERT_STRICT_NE(
        f.Int8(),
        f.Int16());
    ASSERT_STRICT_NE(
        f.Int16(),
        f.Int32());
    ASSERT_STRICT_NE(
        f.Int32(),
        f.Int64());
    ASSERT_STRICT_NE(
        f.Int64(),
        f.Uint8());
    ASSERT_STRICT_NE(
        f.Uint8(),
        f.Uint16());
    ASSERT_STRICT_NE(
        f.Uint16(),
        f.Uint32());
    ASSERT_STRICT_NE(
        f.Uint32(),
        f.Uint64());
    ASSERT_STRICT_NE(
        f.Uint64(),
        f.Float());
    ASSERT_STRICT_NE(
        f.Float(),
        f.Double());
    ASSERT_STRICT_NE(
        f.Double(),
        f.String());
    ASSERT_STRICT_NE(
        f.String(),
        f.Utf8());
    ASSERT_STRICT_NE(
        f.Utf8(),
        f.Date());
    ASSERT_STRICT_NE(
        f.Date(),
        f.Datetime());
    ASSERT_STRICT_NE(
        f.Datetime(),
        f.Timestamp());
    ASSERT_STRICT_NE(
        f.Timestamp(),
        f.TzDate());
    ASSERT_STRICT_NE(
        f.TzDate(),
        f.TzDatetime());
    ASSERT_STRICT_NE(
        f.TzDatetime(),
        f.TzTimestamp());
    ASSERT_STRICT_NE(
        f.TzTimestamp(),
        f.Interval());
    ASSERT_STRICT_NE(
        f.Interval(),
        f.Decimal(20, 10));
    ASSERT_STRICT_NE(
        f.Decimal(20, 10),
        f.Json());
    ASSERT_STRICT_NE(
        f.Json(),
        f.Yson());
    ASSERT_STRICT_NE(
        f.Yson(),
        f.Uuid());
    ASSERT_STRICT_NE(
        f.Uuid(),
        f.Optional(f.Void()));
    ASSERT_STRICT_NE(
        f.Optional(f.Void()),
        f.List(f.Void()));
    ASSERT_STRICT_NE(
        f.List(f.Void()),
        f.Dict(f.Void(), f.String()));
    ASSERT_STRICT_NE(
        f.Dict(f.Void(), f.String()),
        f.Struct({}));
    ASSERT_STRICT_NE(
        f.Struct({}),
        f.Struct("S", {}));
    ASSERT_STRICT_NE(
        f.Struct("S", {}),
        f.Struct({{"a", f.Int64()}, {"b", f.Int8()}}));
    ASSERT_STRICT_NE(
        f.Struct({{"a", f.Int64()}, {"b", f.Int8()}}),
        f.Struct("S", {{"a", f.Int64()}, {"b", f.Int8()}}));
    ASSERT_STRICT_NE(
        f.Struct("S", {{"a", f.Int64()}, {"b", f.Int8()}}),
        f.Tuple({}));
    ASSERT_STRICT_NE(
        f.Tuple({}),
        f.Tuple("T", {}));
    ASSERT_STRICT_NE(
        f.Tuple("T", {}),
        f.Tuple({{f.Int64()}, {f.Int8()}}));
    ASSERT_STRICT_NE(
        f.Tuple({{f.Int64()}, {f.Int8()}}),
        f.Tuple("T", {{f.Int64()}, {f.Int8()}}));
    ASSERT_STRICT_NE(
        f.Tuple("T", {{f.Int64()}, {f.Int8()}}),
        f.Variant(f.Struct("Inner", {{"x", f.Void()}})));
    ASSERT_STRICT_NE(
        f.Variant(f.Struct("Inner", {{"x", f.Void()}})),
        f.Variant("V", f.Struct("Inner", {{"x", f.Void()}})));
    ASSERT_STRICT_NE(
        f.Variant("V", f.Struct("Inner", {{"x", f.Void()}})),
        f.Variant(f.Tuple("Inner", {{f.Void()}})));
    ASSERT_STRICT_NE(
        f.Variant(f.Tuple("Inner", {{f.Void()}})),
        f.Variant("V", f.Tuple("Inner", {{f.Void()}})));
    ASSERT_STRICT_NE(
        f.Variant("V", f.Tuple("Inner", {{f.Void()}})),
        f.Tagged(f.Void(), "T"));
    ASSERT_STRICT_NE(
        f.Tagged(f.Void(), "T"),
        f.Void());
}

TEST_TF(TypeEquivalence, StrictNeDecimal) {
    ASSERT_STRICT_NE(
        f.Decimal(20, 10),
        f.Decimal(21, 10));
    ASSERT_STRICT_NE(
        f.Decimal(20, 10),
        f.Decimal(20, 11));
}

TEST_TF(TypeEquivalence, StrictNeStruct) {
    ASSERT_STRICT_NE(
        f.Struct({}),
        f.Struct("", {}));
    ASSERT_STRICT_NE(
        f.Struct("name", {}),
        f.Struct("other name", {}));
    ASSERT_STRICT_NE(
        f.Struct({}),
        f.Struct({{"x", f.Void()}}));
    ASSERT_STRICT_NE(
        f.Struct({{"x", f.Void()}}),
        f.Struct({{"y", f.Void()}}));
    ASSERT_STRICT_NE(
        f.Struct({{"x", f.Void()}}),
        f.Struct({{"x", f.String()}}));
    ASSERT_STRICT_NE(
        f.Struct("name", {}),
        f.Struct("name", {{"x", f.Void()}}));
    ASSERT_STRICT_NE(
        f.Struct("name", {{"x", f.Void()}}),
        f.Struct("name", {{"y", f.Void()}}));
    ASSERT_STRICT_NE(
        f.Struct("name", {{"x", f.Void()}}),
        f.Struct("name", {{"x", f.String()}}));
    ASSERT_STRICT_NE(
        f.Struct({{"x", f.Void()}, {"y", f.String()}}),
        f.Struct({{"x", f.String()}, {"y", f.Void()}}));
    ASSERT_STRICT_NE(
        f.Struct({{"x", f.Void()}, {"y", f.Void()}}),
        f.Struct({{"y", f.Void()}, {"x", f.Void()}}));
    ASSERT_STRICT_NE(
        f.Struct({{"x", f.Void()}, {"y", f.Void()}}),
        f.Struct({{"x", f.Void()}, {"y", f.Void()}, {"z", f.Void()}}));
}

TEST_TF(TypeEquivalence, StrictNeTuple) {
    ASSERT_STRICT_NE(
        f.Tuple({}),
        f.Tuple("", {}));
    ASSERT_STRICT_NE(
        f.Tuple("name", {}),
        f.Tuple("other name", {}));
    ASSERT_STRICT_NE(
        f.Tuple({}),
        f.Tuple({{f.Void()}}));
    ASSERT_STRICT_NE(
        f.Tuple({{f.Void()}}),
        f.Tuple({{f.String()}}));
    ASSERT_STRICT_NE(
        f.Tuple("name", {}),
        f.Tuple("name", {{f.Void()}}));
    ASSERT_STRICT_NE(
        f.Tuple("name", {{f.Void()}}),
        f.Tuple("name", {{f.String()}}));
    ASSERT_STRICT_NE(
        f.Tuple({{f.String()}, {f.Void()}}),
        f.Tuple({{f.Void()}, {f.String()}}));
    ASSERT_STRICT_NE(
        f.Tuple({{f.Void()}, {f.Void()}}),
        f.Tuple({{f.Void()}, {f.Void()}, {f.Void()}}));
}

TEST_TF(TypeEquivalence, StrictNeVariant) {
    ASSERT_STRICT_NE(
        f.Variant(f.Tuple({{f.Void()}})),
        f.Variant("", f.Tuple({{f.Void()}})));
    ASSERT_STRICT_NE(
        f.Variant("", f.Tuple({{f.Void()}})),
        f.Variant("X", f.Tuple({{f.Void()}})));
    ASSERT_STRICT_NE(
        f.Variant(f.Tuple({{f.Void()}})),
        f.Variant(f.Tuple({{f.String()}})));
    ASSERT_STRICT_NE(
        f.Variant("X", f.Tuple({{f.Utf8()}})),
        f.Variant("X", f.Tuple({{f.String()}})));
    ASSERT_STRICT_NE(
        f.Variant(f.Tuple({{f.Utf8()}})),
        f.Variant(f.Struct({{"_", f.Utf8()}})));
    ASSERT_STRICT_NE(
        f.Variant(f.Struct({{"item1", f.String()}})),
        f.Variant(f.Struct({{"item2", f.String()}})));
    ASSERT_STRICT_NE(
        f.Variant("X", f.Struct({{"item2", f.String()}})),
        f.Variant("X", f.Struct({{"item1", f.String()}})));
}

TEST_TF(TypeEquivalence, StrictNeTagged) {
    ASSERT_STRICT_NE(
        f.Tagged(f.String(), "Tag"),
        f.Tagged(f.String(), "Other tag"));
    ASSERT_STRICT_NE(
        f.Tagged(f.String(), "Tag"),
        f.Tagged(f.Utf8(), "Tag"));
}

TEST_TF(TypeEquivalence, StrictNeDeep) {
    auto t1 = f.Struct({
        {"i", f.Optional(f.String())},
        {"don't", f.Utf8()},
        {"have", f.Tuple("GeoCoordinates", {{f.Float()}, {f.Float()}})},
        {"enough", f.List(f.Optional(f.String()))},
        {"fantasy", f.Dict(f.String(), f.List(f.Utf8()))}, // < difference
        {"to", f.Yson()},
        {"think", f.Optional(f.Json())},
        {"of", f.Optional(f.String())},
        {"a", f.List(f.Tuple({{f.String()}, {f.Tuple("GeoCoordinates", {{f.Float()}, {f.Float()}})}}))},
        {"meaningful", f.Optional(f.String())},
        {"example", f.Dict(f.Optional(f.Decimal(10, 5)), f.List(f.Dict(f.Int32(), f.Float())))},
    });

    auto t2 = f.Struct({
        {"i", f.Optional(f.String())},
        {"don't", f.Utf8()},
        {"have", f.Tuple("GeoCoordinates", {{f.Float()}, {f.Float()}})},
        {"enough", f.List(f.Optional(f.String()))},
        {"fantasy", f.Dict(f.String(), f.List(f.String()))}, // < difference
        {"to", f.Yson()},
        {"think", f.Optional(f.Json())},
        {"of", f.Optional(f.String())},
        {"a", f.List(f.Tuple({{f.String()}, {f.Tuple("GeoCoordinates", {{f.Float()}, {f.Float()}})}}))},
        {"meaningful", f.Optional(f.String())},
        {"example", f.Dict(f.Optional(f.Decimal(10, 5)), f.List(f.Dict(f.Int32(), f.Float())))},
    });

    ASSERT_STRICT_NE(t1, t2);
}
