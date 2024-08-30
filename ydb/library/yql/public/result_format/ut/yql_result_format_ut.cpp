#include <ydb/library/yql/public/result_format/yql_result_format.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NResult {

void Test(const TString& input) {
    TTypeBuilder builder;
    auto node = NYT::NodeFromYsonString(input);
    ParseType(node, builder);
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(builder.GetResult()), NYT::NodeToCanonicalYsonString(node));
}

Y_UNIT_TEST_SUITE(TParseType) {
    Y_UNIT_TEST(Throwing) {
        TThrowingTypeVisitor v;
        UNIT_ASSERT_EXCEPTION(v.OnNull(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnVoid(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEmptyList(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEmptyDict(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBool(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnInt8(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnUint8(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnInt16(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnUint16(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnInt32(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnUint32(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnInt64(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnUint64(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnFloat(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDouble(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnString(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnUtf8(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnYson(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnJson(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnJsonDocument(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnUuid(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDyNumber(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDate(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDatetime(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTimestamp(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTzDate(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTzDatetime(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTzTimestamp(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnInterval(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDate32(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDatetime64(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTimestamp64(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTzDate32(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTzDatetime64(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTzTimestamp64(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnInterval64(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDecimal(10, 1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeginOptional(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEndOptional(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeginList(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEndList(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeginTuple(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTupleItem(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEndTuple(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeginStruct(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnStructItem("foo"), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEndStruct(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeginDict(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDictKey(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDictPayload(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEndDict(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeginVariant(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEndVariant(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeginTagged("foo"), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEndTagged(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnPgType("int4", "N"), TUnsupportedException);
    }

    Y_UNIT_TEST(BadNotList) {
        UNIT_ASSERT_EXCEPTION(Test("foo"), TUnsupportedException);
    }

    Y_UNIT_TEST(BadEmptyList) {
        UNIT_ASSERT_EXCEPTION(Test("[]"), TUnsupportedException);
    }

    Y_UNIT_TEST(BadNotStringName) {
        UNIT_ASSERT_EXCEPTION(Test("[#]"), TUnsupportedException);
    }

    Y_UNIT_TEST(BadUnknownName) {
        UNIT_ASSERT_EXCEPTION(Test("[unknown]"), TUnsupportedException);
    }

    Y_UNIT_TEST(Void) {
        Test("[VoidType]");
    }

    Y_UNIT_TEST(Null) {
        Test("[NullType]");
    }

    Y_UNIT_TEST(EmptyList) {
        Test("[EmptyListType]");
    }

    Y_UNIT_TEST(EmptyDict) {
        Test("[EmptyDictType]");
    }

    Y_UNIT_TEST(BadUnknownDataName) {
        UNIT_ASSERT_EXCEPTION(Test("[DataType;unknown]"), TUnsupportedException);
    }

    Y_UNIT_TEST(Bool) {
        Test("[DataType;Bool]");
    }

    Y_UNIT_TEST(Int8) {
        Test("[DataType;Int8]");
    }

    Y_UNIT_TEST(Uint8) {
        Test("[DataType;Uint8]");
    }    

    Y_UNIT_TEST(Int16) {
        Test("[DataType;Int16]");
    }

    Y_UNIT_TEST(Uint16) {
        Test("[DataType;Uint16]");
    }

    Y_UNIT_TEST(Int32) {
        Test("[DataType;Int32]");
    }

    Y_UNIT_TEST(Uint32) {
        Test("[DataType;Uint32]");
    }

    Y_UNIT_TEST(Int64) {
        Test("[DataType;Int64]");
    }

    Y_UNIT_TEST(Uint64) {
        Test("[DataType;Uint64]");
    }

    Y_UNIT_TEST(Float) {
        Test("[DataType;Float]");
    }

    Y_UNIT_TEST(Double) {
        Test("[DataType;Double]");
    }

    Y_UNIT_TEST(String) {
        Test("[DataType;String]");
    }

    Y_UNIT_TEST(Utf8) {
        Test("[DataType;Utf8]");
    }

    Y_UNIT_TEST(Yson) {
        Test("[DataType;Yson]");
    }

    Y_UNIT_TEST(Json) {
        Test("[DataType;Json]");
    }

    Y_UNIT_TEST(JsonDocument) {
        Test("[DataType;JsonDocument]");
    }

    Y_UNIT_TEST(Uuid) {
        Test("[DataType;Uuid]");
    }

    Y_UNIT_TEST(DyNumber) {
        Test("[DataType;DyNumber]");
    }

    Y_UNIT_TEST(Date) {
        Test("[DataType;Date]");
    }

    Y_UNIT_TEST(Datetime) {
        Test("[DataType;Datetime]");
    }

    Y_UNIT_TEST(Timestamp) {
        Test("[DataType;Timestamp]");
    }

    Y_UNIT_TEST(TzDate) {
        Test("[DataType;TzDate]");
    }

    Y_UNIT_TEST(TzDatetime) {
        Test("[DataType;TzDatetime]");
    }    

    Y_UNIT_TEST(TzTimestamp) {
        Test("[DataType;TzTimestamp]");
    }

    Y_UNIT_TEST(Interval) {
        Test("[DataType;Interval]");
    }

    Y_UNIT_TEST(Date32) {
        Test("[DataType;Date32]");
    }

    Y_UNIT_TEST(Datetime64) {
        Test("[DataType;Datetime64]");
    }

    Y_UNIT_TEST(Timestamp64) {
        Test("[DataType;Timestamp64]");
    }

    Y_UNIT_TEST(TzDate32) {
        Test("[DataType;TzDate32]");
    }

    Y_UNIT_TEST(TzDatetime64) {
        Test("[DataType;TzDatetime64]");
    }

    Y_UNIT_TEST(TzTimestamp64) {
        Test("[DataType;TzTimestamp64]");
    }

    Y_UNIT_TEST(Interval64) {
        Test("[DataType;Interval64]");
    }

    Y_UNIT_TEST(Decimal) {
        Test("[DataType;Decimal;\"10\";\"1\"]");
    }

    Y_UNIT_TEST(OptionalInt32) {
        Test("[OptionalType;[DataType;Int32]]");
    }

    Y_UNIT_TEST(ListInt32) {
        Test("[ListType;[DataType;Int32]]");
    }

    Y_UNIT_TEST(ListListInt32) {
        Test("[ListType;[ListType;[DataType;Int32]]]");
    }

    Y_UNIT_TEST(TupleInt32String) {
        Test("[TupleType;[[DataType;Int32];[DataType;String]]]");
    }

    Y_UNIT_TEST(EmptyTuple) {
        Test("[TupleType;[]]");
    }

    Y_UNIT_TEST(StructInt32String) {
        Test("[StructType;[[foo;[DataType;Int32]];[bar;[DataType;String]]]]");
    }

    Y_UNIT_TEST(EmptyStruct) {
        Test("[StructType;[]]");
    }

    Y_UNIT_TEST(DictInt32String) {
        Test("[DictType;[DataType;Int32];[DataType;String]]");
    }

    Y_UNIT_TEST(VariantTupleInt32String) {
        Test("[VariantType;[TupleType;[[DataType;Int32];[DataType;String]]]]");
    }

    Y_UNIT_TEST(TaggedInt32) {
        Test("[TaggedType;foo;[DataType;Int32]]");
    }

    Y_UNIT_TEST(PgType) {
        Test("[PgType;int4;N]");
    }
}

}
