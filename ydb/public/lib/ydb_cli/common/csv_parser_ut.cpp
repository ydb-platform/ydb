#include "csv_parser.h"

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

using namespace NYdb;
using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(YdbCliCsvParserTests) {
    bool CompareValues(const TValue& lhs, const TValue& rhs) {
        TString stringFirst, stringSecond;
        NProtoBuf::TextFormat::PrintToString(lhs.GetProto(), &stringFirst);
        NProtoBuf::TextFormat::PrintToString(rhs.GetProto(), &stringSecond);
        return stringFirst == stringSecond;
    }

    void CommonTestParams(TString&& header, TString&& data, const std::map<TString, TValue>& result) {
        std::map<TString, TType> paramTypes;
        for (const auto& [name, value] : result) {
            paramTypes.insert({name, value.GetType()});
        }
        std::map<TString, TString> paramSources;
        TCsvParser parser(std::move(header), ',', "", &paramTypes, &paramSources);
        TParamsBuilder paramBuilder;
        parser.GetParams(std::move(data), paramBuilder, TCsvParser::TParseMetadata{});
        auto values = paramBuilder.Build().GetValues();
        UNIT_ASSERT_EQUAL(values.size(), result.size());
        for (const auto& [name, value] : result) {
            auto it = values.find(name);
            UNIT_ASSERT_UNEQUAL(it, values.end());
            UNIT_ASSERT(CompareValues(value, it->second));
        }
    }

    void CommonTestValue(TString&& header, TString&& data, const TValue& result) {
        std::map<TString, TType> paramTypes;
        for (auto member : result.GetType().GetProto().struct_type().members()) {
            paramTypes.insert({member.name(), member.type()});
        }

        TCsvParser parser(std::move(header), ',', "", &paramTypes, nullptr);
        TValueBuilder valueBuilder;
        parser.GetValue(std::move(data), valueBuilder, result.GetType(), TCsvParser::TParseMetadata{});
        UNIT_ASSERT(CompareValues(valueBuilder.Build(), result));
    }

    TValue MakeStruct(const TString& name, const TValue& value) {
        return TValueBuilder().BeginStruct().AddMember(name, value).EndStruct().Build();
    }

    Y_UNIT_TEST(IntegerTypesTestParams) {
        CommonTestParams("name", "-1", {{"$name", TValueBuilder().Int8(-1).Build()}});
        CommonTestParams("name", "-10000", {{"$name", TValueBuilder().Int16(-10000).Build()}});
        CommonTestParams("name", "-1000000", {{"$name", TValueBuilder().Int32(-1000000).Build()}});
        CommonTestParams("name", "-100000000000", {{"$name", TValueBuilder().Int64(-100000000000).Build()}});
        CommonTestParams("name", "1", {{"$name", TValueBuilder().Uint8(1).Build()}});
        CommonTestParams("name", "10000", {{"$name", TValueBuilder().Uint16(10000).Build()}});
        CommonTestParams("name", "1000000", {{"$name", TValueBuilder().Uint32(1000000).Build()}});
        CommonTestParams("name", "100000000000", {{"$name", TValueBuilder().Uint64(100000000000).Build()}});
    }

    Y_UNIT_TEST(IntegerTypesTestValue) {
        CommonTestValue("name", "-1", MakeStruct("name", TValueBuilder().Int8(-1).Build()));
        CommonTestValue("name", "-10000", MakeStruct("name", TValueBuilder().Int16(-10000).Build()));
        CommonTestValue("name", "-1000000", MakeStruct("name", TValueBuilder().Int32(-1000000).Build()));
        CommonTestValue("name", "-100000000000", MakeStruct("name", TValueBuilder().Int64(-100000000000).Build()));
        CommonTestValue("name", "1", MakeStruct("name", TValueBuilder().Uint8(1).Build()));
        CommonTestValue("name", "10000", MakeStruct("name", TValueBuilder().Uint16(10000).Build()));
        CommonTestValue("name", "1000000", MakeStruct("name", TValueBuilder().Uint32(1000000).Build()));
        CommonTestValue("name", "100000000000", MakeStruct("name", TValueBuilder().Uint64(100000000000).Build()));
    }

    Y_UNIT_TEST(DateTypesTestParams) {
        CommonTestParams("name", "\"2001-01-01\"", {{"$name", TValueBuilder().Date(TInstant::ParseIso8601("2001-01-01")).Build()}});
        CommonTestParams("name", "\"2001-01-01T12:12:12\"", {{"$name", TValueBuilder().Datetime(TInstant::ParseIso8601("2001-01-01T12:12:12")).Build()}});
        CommonTestParams("name", "\"2001-01-01T12:12:12.111111\"", {{"$name", TValueBuilder().Timestamp(TInstant::ParseIso8601("2001-01-01T12:12:12.111111")).Build()}});
        CommonTestParams("name", "12000", {{"$name", TValueBuilder().Date(TInstant::Days(12000)).Build()}});
        CommonTestParams("name", "1200000", {{"$name", TValueBuilder().Datetime(TInstant::Seconds(1200000)).Build()}});
        CommonTestParams("name", "120000000", {{"$name", TValueBuilder().Timestamp(TInstant::MicroSeconds(120000000)).Build()}});
        CommonTestParams("name", "-2000", {{"$name", TValueBuilder().Interval(-2000).Build()}});
        CommonTestParams("name", "\"2001-01-01,Europe/Moscow\"", {{"$name", TValueBuilder().TzDate("2001-01-01,Europe/Moscow").Build()}});
        CommonTestParams("name", "\"2001-01-01T12:12:12,Europe/Moscow\"", {{"$name", TValueBuilder().TzDatetime("2001-01-01T12:12:12,Europe/Moscow").Build()}});
        CommonTestParams("name", "\"2001-01-01T12:12:12.111111,Europe/Moscow\"", {{"$name", TValueBuilder().TzTimestamp("2001-01-01T12:12:12.111111,Europe/Moscow").Build()}});
    }

    Y_UNIT_TEST(DateTypesTestValue) {
        CommonTestValue("name", "\"2001-01-01\"", MakeStruct("name", TValueBuilder().Date(TInstant::ParseIso8601("2001-01-01")).Build()));
        CommonTestValue("name", "\"2001-01-01T12:12:12\"", MakeStruct("name", TValueBuilder().Datetime(TInstant::ParseIso8601("2001-01-01T12:12:12")).Build()));
        CommonTestValue("name", "\"2001-01-01T12:12:12.111111\"", MakeStruct("name", TValueBuilder().Timestamp(TInstant::ParseIso8601("2001-01-01T12:12:12.111111")).Build()));
        CommonTestValue("name", "12000", MakeStruct("name", TValueBuilder().Date(TInstant::Days(12000)).Build()));
        CommonTestValue("name", "1200000", MakeStruct("name", TValueBuilder().Datetime(TInstant::Seconds(1200000)).Build()));
        CommonTestValue("name", "120000000", MakeStruct("name", TValueBuilder().Timestamp(TInstant::MicroSeconds(120000000)).Build()));
        CommonTestValue("name", "-2000", MakeStruct("name", TValueBuilder().Interval(-2000).Build()));
        CommonTestValue("name", "\"2001-01-01,Europe/Moscow\"", MakeStruct("name", TValueBuilder().TzDate("2001-01-01,Europe/Moscow").Build()));
        CommonTestValue("name", "\"2001-01-01T12:12:12,Europe/Moscow\"", MakeStruct("name", TValueBuilder().TzDatetime("2001-01-01T12:12:12,Europe/Moscow").Build()));
        CommonTestValue("name", "\"2001-01-01T12:12:12.111111,Europe/Moscow\"", MakeStruct("name", TValueBuilder().TzTimestamp("2001-01-01T12:12:12.111111,Europe/Moscow").Build()));
    }

    Y_UNIT_TEST(OtherPrimitiveTypeTestParams) {
        CommonTestParams("name", "строка", {{"$name", TValueBuilder().Utf8("строка").Build()}});
        CommonTestParams("name", "строка", {{"$name", TValueBuilder().String("строка").Build()}});
        CommonTestParams("name", "true", {{"$name", TValueBuilder().Bool(true).Build()}});
        CommonTestParams("name", "false", {{"$name", TValueBuilder().Bool(false).Build()}});
        CommonTestParams("name", "1.183", {{"$name", TValueBuilder().Float(1.183).Build()}});
        CommonTestParams("name", "1.183", {{"$name", TValueBuilder().Double(1.183).Build()}});
        CommonTestParams("name", "1.183", {{"$name", TValueBuilder().DyNumber("1.183").Build()}});
        CommonTestParams("name", "1.183", {{"$name", TValueBuilder().Decimal(TString("1.183")).Build()}});
        CommonTestParams("name", "inf", {{"$name", TValueBuilder().Decimal(TString("inf")).Build()}});
        CommonTestParams("name", "-inf", {{"$name", TValueBuilder().Decimal(TString("-inf")).Build()}});
        CommonTestParams("name", "nan", {{"$name", TValueBuilder().Decimal(TString("nan")).Build()}});
        CommonTestParams("name", "-nan", {{"$name", TValueBuilder().Decimal(TString("-nan")).Build()}});
        CommonTestParams("name", "550e8400-e29b-41d4-a716-446655440000", {{"$name", TValueBuilder().Uuid(TUuidValue("550e8400-e29b-41d4-a716-446655440000")).Build()}});
        CommonTestParams("name", "\"{\"\"a\"\":10, \"\"b\"\":\"\"string\"\"}\"", {{"$name", TValueBuilder().Json("{\"a\":10, \"b\":\"string\"}").Build()}});
        CommonTestParams("name", "строка", {{"$name", TValueBuilder().OptionalUtf8("строка").Build()}});
        CommonTestParams("name", "\"\"", {{"$name", TValueBuilder().OptionalUtf8({}).Build()}});
        CommonTestParams("name", "данные", {{"$name", TValueBuilder().Pg(TPgValue(TPgValue::VK_TEXT, "данные", TPgType("some_type"))).Build()}});
    }

    Y_UNIT_TEST(OtherPrimitiveTypesTestValue) {
        CommonTestValue("name", "строка", MakeStruct("name", TValueBuilder().Utf8("строка").Build()));
        CommonTestValue("name", "строка", MakeStruct("name", TValueBuilder().String("строка").Build()));
        CommonTestValue("name", "true", MakeStruct("name", TValueBuilder().Bool(true).Build()));
        CommonTestValue("name", "false", MakeStruct("name", TValueBuilder().Bool(false).Build()));
        CommonTestValue("name", "1.183", MakeStruct("name", TValueBuilder().Float(1.183).Build()));
        CommonTestValue("name", "1.183", MakeStruct("name", TValueBuilder().Double(1.183).Build()));
        CommonTestValue("name", "1.183", MakeStruct("name", TValueBuilder().DyNumber("1.183").Build()));
        CommonTestValue("name", "1.183", MakeStruct("name", TValueBuilder().Decimal(TString("1.183")).Build()));
        CommonTestValue("name", "550e8400-e29b-41d4-a716-446655440000", MakeStruct("name", TValueBuilder().Uuid(TUuidValue("550e8400-e29b-41d4-a716-446655440000")).Build()));
        CommonTestValue("name", "\"{\"\"a\"\":10, \"\"b\"\":\"\"string\"\"}\"", MakeStruct("name", TValueBuilder().Json("{\"a\":10, \"b\":\"string\"}").Build()));
        CommonTestValue("name", "строка", MakeStruct("name", TValueBuilder().OptionalUtf8("строка").Build()));
        CommonTestValue("name", "\"\"", MakeStruct("name", TValueBuilder().OptionalUtf8({}).Build()));
        CommonTestValue("name", "данные", MakeStruct("name", TValueBuilder().Pg(TPgValue(TPgValue::VK_TEXT, "данные", TPgType("some_type"))).Build()));
    }

    Y_UNIT_TEST(EdgeValuesTestParams) {
        CommonTestParams("name", "255", {{"$name", TValueBuilder().Uint8(255).Build()}});
        CommonTestParams("name", "65535", {{"$name", TValueBuilder().Uint16(65535).Build()}});
        CommonTestParams("name", "4294967295", {{"$name", TValueBuilder().Uint32(4294967295).Build()}});
        CommonTestParams("name", "18446744073709551615", {{"$name", TValueBuilder().Uint64(std::numeric_limits<ui64>::max()).Build()}});

        CommonTestParams("name", "127", {{"$name", TValueBuilder().Int8(127).Build()}});
        CommonTestParams("name", "32767", {{"$name", TValueBuilder().Int16(32767).Build()}});
        CommonTestParams("name", "2147483647", {{"$name", TValueBuilder().Int32(2147483647).Build()}});
        CommonTestParams("name", "9223372036854775807", {{"$name", TValueBuilder().Int64(std::numeric_limits<i64>::max()).Build()}});

        CommonTestParams("name", "-128", {{"$name", TValueBuilder().Int8(-128).Build()}});
        CommonTestParams("name", "-32768", {{"$name", TValueBuilder().Int16(-32768).Build()}});
        CommonTestParams("name", "-2147483648", {{"$name", TValueBuilder().Int32(-2147483648).Build()}});
        CommonTestParams("name", "-9223372036854775808", {{"$name", TValueBuilder().Int64(std::numeric_limits<i64>::min()).Build()}});

        double minDouble = std::numeric_limits<double>::lowest();
        double maxDouble = std::numeric_limits<double>::max();
        CommonTestParams("name", std::to_string(minDouble), {{"$name", TValueBuilder().Double(minDouble).Build()}});
        CommonTestParams("name", std::to_string(maxDouble), {{"$name", TValueBuilder().Double(maxDouble).Build()}});
    }

    Y_UNIT_TEST(MultipleFields) {
        CommonTestParams("a,b,c,d", "строка,187.201,false,\"\"", {
            {"$a", TValueBuilder().Utf8("строка").Build()},
            {"$b", TValueBuilder().OptionalDouble(187.201).Build()},
            {"$c", TValueBuilder().Bool(false).Build()},
            {"$d", TValueBuilder().OptionalInt64({}).Build()},
        });
        CommonTestValue("a,b,c,d", "строка,187.201,false,\"\"", TValueBuilder()
            .BeginStruct()
                .AddMember("a")
                    .Utf8("строка")
                .AddMember("b")
                    .OptionalDouble(187.201)
                .AddMember("c")
                    .Bool(false)
                .AddMember("d")
                    .OptionalInt64({})
            .EndStruct()
            .Build()
        );
    }
}