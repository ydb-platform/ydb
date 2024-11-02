#include <yql/essentials/public/result_format/yql_result_format_type.h>
#include <yql/essentials/public/result_format/yql_result_format_data.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NResult {

void Test(const TString& inputType, const TString& inputData, const TMaybe<TString>& altData = Nothing()) {
    TDataBuilder builder;
    auto nodeType = NYT::NodeFromYsonString(inputType);
    auto nodeData = NYT::NodeFromYsonString(inputData);
    ParseData(nodeType, nodeData, builder);
    UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(builder.GetResult()), NYT::NodeToCanonicalYsonString(nodeData));
    if (altData.Defined()) {
        TDataBuilder builder2;
        auto nodeData2 = NYT::NodeFromYsonString(*altData);
        ParseData(nodeType, nodeData2, builder2);
        UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(builder2.GetResult()), NYT::NodeToCanonicalYsonString(nodeData));
    }
}

Y_UNIT_TEST_SUITE(ParseData) {
    Y_UNIT_TEST(EmptyVisitor) {
        TEmptyDataVisitor v;
        v.OnVoid();
        v.OnNull();
        v.OnEmptyList();
        v.OnEmptyDict();
        v.OnBool(true);
        v.OnInt8(1);
        v.OnUint8(1);
        v.OnInt16(1);
        v.OnUint16(1);
        v.OnInt32(1);
        v.OnUint32(1);
        v.OnInt64(1);
        v.OnUint64(1);
        v.OnFloat(1.2f);
        v.OnDouble(1.2f);
        v.OnString("foo", true);
        v.OnUtf8("foo");
        v.OnYson("foo", true);
        v.OnJson("foo");
        v.OnJsonDocument("foo");
        v.OnUuid("foo", true);
        v.OnDyNumber("foo", true);
        v.OnDate(1);
        v.OnDatetime(1);
        v.OnTimestamp(1);
        v.OnTzDate("foo");
        v.OnTzDatetime("foo");
        v.OnTzTimestamp("foo");
        v.OnInterval(1);
        v.OnDate32(1);
        v.OnDatetime64(1);
        v.OnTimestamp64(1);
        v.OnTzDate32("foo");
        v.OnTzDatetime64("foo");
        v.OnTzTimestamp64("foo");
        v.OnInterval64(1);
        v.OnDecimal("1.2");
        v.OnBeginOptional();
        v.OnBeforeOptionalItem();
        v.OnAfterOptionalItem();
        v.OnEmptyOptional();
        v.OnEndOptional();
        v.OnBeginList();
        v.OnBeforeListItem();
        v.OnAfterListItem();
        v.OnEndList();
        v.OnBeginTuple();
        v.OnBeforeTupleItem();
        v.OnAfterTupleItem();
        v.OnEndTuple();
        v.OnBeginStruct();
        v.OnBeforeStructItem();
        v.OnAfterStructItem();
        v.OnEndStruct();
        v.OnBeginDict();
        v.OnBeforeDictItem();
        v.OnBeforeDictKey();
        v.OnAfterDictKey();
        v.OnBeforeDictPayload();
        v.OnAfterDictPayload();
        v.OnAfterDictItem();
        v.OnEndDict();
        v.OnBeginVariant(0);
        v.OnEndVariant();
        v.OnPg("foo",true);
    }

    Y_UNIT_TEST(ThrowingVisitor) {
        TThrowingDataVisitor v;
        UNIT_ASSERT_EXCEPTION(v.OnVoid(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnNull(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEmptyList(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEmptyDict(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBool(true), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnInt8(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnUint8(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnInt16(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnUint16(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnInt32(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnUint32(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnInt64(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnUint64(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnFloat(1.2f), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDouble(1.2f), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnString("foo", true), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnUtf8("foo"), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnYson("foo", true), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnJson("foo"), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnJsonDocument("foo"), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnUuid("foo", true), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDyNumber("foo", true), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDate(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDatetime(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTimestamp(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTzDate("foo"), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTzDatetime("foo"), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTzTimestamp("foo"), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnInterval(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDate32(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDatetime64(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTimestamp64(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTzDate32("foo"), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTzDatetime64("foo"), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnTzTimestamp64("foo"), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnInterval64(1), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnDecimal("1.2"), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeginOptional(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeforeOptionalItem(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnAfterOptionalItem(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEmptyOptional(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEndOptional(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeginList(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeforeListItem(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnAfterListItem(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEndList(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeginTuple(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeforeTupleItem(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnAfterTupleItem(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEndTuple(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeginStruct(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeforeStructItem(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnAfterStructItem(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEndStruct(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeginDict(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeforeDictItem(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeforeDictKey(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnAfterDictKey(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeforeDictPayload(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnAfterDictPayload(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnAfterDictItem(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEndDict(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnBeginVariant(0), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnEndVariant(), TUnsupportedException);
        UNIT_ASSERT_EXCEPTION(v.OnPg("foo",true), TUnsupportedException);
    }

    Y_UNIT_TEST(Void) {
        Test("[VoidType]","Void");
    }

    Y_UNIT_TEST(Null) {
        Test("[NullType]","#");
    }

    Y_UNIT_TEST(EmptyList) {
        Test("[EmptyListType]","[]");
    }

    Y_UNIT_TEST(EmptyDict) {
        Test("[EmptyDictType]","[]");
    }

    Y_UNIT_TEST(Bool) {
        Test("[DataType;Bool]","%true");
        Test("[DataType;Bool]","%false");
    }

    Y_UNIT_TEST(Int8) {
        Test("[DataType;Int8]","\"1\"");
    }

    Y_UNIT_TEST(Uint8) {
        Test("[DataType;Uint8]","\"1\"");
    }

    Y_UNIT_TEST(Int16) {
        Test("[DataType;Int16]","\"1\"");
    }

    Y_UNIT_TEST(Uint16) {
        Test("[DataType;Uint16]","\"1\"");
    }

    Y_UNIT_TEST(Int32) {
        Test("[DataType;Int32]","\"1\"");
    }

    Y_UNIT_TEST(Uint32) {
        Test("[DataType;Uint32]","\"1\"");
    }

    Y_UNIT_TEST(Int64) {
        Test("[DataType;Int64]","\"1\"");
    }

    Y_UNIT_TEST(Uint64) {
        Test("[DataType;Uint64]","\"1\"");
    }

    Y_UNIT_TEST(Float) {
        Test("[DataType;Float]","\"1.2\"");
        Test("[DataType;Float]","\"-inf\"");
        Test("[DataType;Float]","\"nan\"");
    }

    Y_UNIT_TEST(Double) {
        Test("[DataType;Double]","\"1.2\"");
        Test("[DataType;Double]","\"-inf\"");
        Test("[DataType;Double]","\"nan\"");
    }

    Y_UNIT_TEST(String) {
        Test("[DataType;String]","\"foo\"");
        Test("[DataType;String]","[\"/w==\"]");
    }

    Y_UNIT_TEST(Utf8) {
        Test("[DataType;Utf8]","\"foo\"");
    }

    Y_UNIT_TEST(Json) {
        Test("[DataType;Json]","\"[]\"");
    }

    Y_UNIT_TEST(Yson) {
        Test("[DataType;Yson]",R"(
            {
                "$attributes" = {
                    "a" = {
                        "$type" = "int64";
                        "$value" = "1"
                    }
                };
                "$value" = {
                    "foo" = {
                        "$type" = "string";
                        "$value" = "bar"
                    }
                }
            })");
    }

    Y_UNIT_TEST(JsonDocument) {
        Test("[DataType;JsonDocument]","\"[]\"");
    }

    Y_UNIT_TEST(Uuid) {
        Test("[DataType;Uuid]","[\"AIQOVZvi1EGnFkRmVUQAAA==\"]");
    }

    Y_UNIT_TEST(DyNumber) {
        Test("[DataType;DyNumber]","[\"AoIS\"]");
    }

    Y_UNIT_TEST(Date) {
        Test("[DataType;Date]","\"1\"");
    }

    Y_UNIT_TEST(Datetime) {
        Test("[DataType;Datetime]","\"1\"");
    }

    Y_UNIT_TEST(Timestamp) {
        Test("[DataType;Timestamp]","\"1\"");
    }

    Y_UNIT_TEST(TzDate) {
        Test("[DataType;TzDate]","\"2011-04-27,Europe/Moscow\"");
    }

    Y_UNIT_TEST(TzDatetime) {
        Test("[DataType;TzDatetime]","\"2011-04-27T01:02:03,Europe/Moscow\"");
    }

    Y_UNIT_TEST(TzTimestamp) {
        Test("[DataType;TzTimestamp]","\"2011-04-27T01:02:03.456789,Europe/Moscow\"");
    }

    Y_UNIT_TEST(Interval) {
        Test("[DataType;Interval]","\"1\"");
    }

    Y_UNIT_TEST(Date32) {
        Test("[DataType;Date32]","\"1\"");
    }

    Y_UNIT_TEST(Datetime64) {
        Test("[DataType;Datetime64]","\"1\"");
    }

    Y_UNIT_TEST(Timestamp64) {
        Test("[DataType;Timestamp64]","\"1\"");
    }

    Y_UNIT_TEST(TzDate32) {
        Test("[DataType;TzDate32]","\"2011-04-27,Europe/Moscow\"");
    }

    Y_UNIT_TEST(TzDatetime64) {
        Test("[DataType;TzDatetime64]","\"2011-04-27T01:02:03,Europe/Moscow\"");
    }

    Y_UNIT_TEST(TzTimestamp64) {
        Test("[DataType;TzTimestamp64]","\"2011-04-27T01:02:03.456789,Europe/Moscow\"");
    }

    Y_UNIT_TEST(Interval64) {
        Test("[DataType;Interval64]","\"1\"");
    }

    Y_UNIT_TEST(Decimal) {
        Test("[DataType;Decimal;\"10\";\"1\"]","\"1.2\"");
        Test("[DataType;Decimal;\"10\";\"1\"]","\"-inf\"");
        Test("[DataType;Decimal;\"10\";\"1\"]","\"nan\"");
    }

    Y_UNIT_TEST(Optional1) {
        Test("[OptionalType;[DataType;Int32]]","[]");
        Test("[OptionalType;[DataType;Int32]]","[\"1\"]");
    }

    Y_UNIT_TEST(Optional2) {
        Test("[OptionalType;[OptionalType;[DataType;Int32]]]","[]", "#");
        Test("[OptionalType;[OptionalType;[DataType;Int32]]]","[[]]", "[#]");
        Test("[OptionalType;[OptionalType;[DataType;Int32]]]","[[\"1\"]]");
    }

    Y_UNIT_TEST(List1) {
        Test("[ListType;[DataType;Int32]]","[]");
        Test("[ListType;[DataType;Int32]]","[\"1\"]");
        Test("[ListType;[DataType;Int32]]","[\"1\";\"2\"]");
    }

    Y_UNIT_TEST(List2) {
        Test("[ListType;[ListType;[DataType;Int32]]]","[]");
        Test("[ListType;[ListType;[DataType;Int32]]]","[[];[\"1\"]]");
        Test("[ListType;[ListType;[DataType;Int32]]]","[[\"1\";\"2\";\"3\"];[\"4\";\"5\";\"6\"]]");
    }

    Y_UNIT_TEST(EmptyTuple) {
        Test("[TupleType;[]]","[]");
    }

    Y_UNIT_TEST(Tuple) {
        Test("[TupleType;[[DataType;Int32];[DataType;String]]]","[\"1\";\"foo\"]");
    }

    Y_UNIT_TEST(EmptyStruct) {
        Test("[StructType;[]]","[]");
    }

    Y_UNIT_TEST(Struct) {
        Test("[StructType;[[foo;[DataType;Int32]];[bar;[DataType;String]]]]","[\"1\";\"foo\"]");
    }

    Y_UNIT_TEST(Dict) {
        Test("[DictType;[DataType;Int32];[DataType;String]]","[]");
        Test("[DictType;[DataType;Int32];[DataType;String]]","[[\"1\";\"foo\"]]");
        Test("[DictType;[DataType;Int32];[DataType;String]]","[[\"1\";\"foo\"];[\"2\";\"bar\"]]");
    }

    Y_UNIT_TEST(Variant) {
        Test("[VariantType;[TupleType;[[DataType;Int32];[DataType;String]]]]","[\"0\";\"7\"]");
        Test("[VariantType;[TupleType;[[DataType;Int32];[DataType;String]]]]","[\"1\";\"foo\"]");
        Test("[VariantType;[StructType;[[foo;[DataType;Int32]];[bar;[DataType;String]]]]]","[\"0\";\"7\"]");
        Test("[VariantType;[StructType;[[foo;[DataType;Int32]];[bar;[DataType;String]]]]]","[\"1\";\"foo\"]");
    }

    Y_UNIT_TEST(Tagged) {
        Test("[TaggedType;foo;[DataType;Int32]]","\"1\"");
    }

    Y_UNIT_TEST(Pg) {
        Test("[PgType;\"text\";\"S\"]","#");
        Test("[PgType;\"text\";\"S\"]","\"foo\"");
        Test("[PgType;\"bytea\";\"U\"]","[\"EjRWeJoAvN4=\"]");
    }
}

}
