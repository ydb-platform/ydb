#include "csv_parser.h"

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

using namespace NYdb;
using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(YdbCliCsvParserTests) {
    void AssertValuesEqual(const TValue& actual, const TValue& expected) {
        TString actualString, expectedString;
        NProtoBuf::TextFormat::PrintToString(actual.GetProto(), &actualString);
        NProtoBuf::TextFormat::PrintToString(expected.GetProto(), &expectedString);
        UNIT_ASSERT_VALUES_EQUAL_C(actualString, expectedString,
            "Expected string: \"" << expectedString << "\", actual string: \"" << actualString << "\"");
    }

    void CommonTestParams(TString&& header, TString&& data, const std::map<TString, TValue>& estimatedResult) {
        std::map<std::string, TType> paramTypes;
        for (const auto& [name, value] : estimatedResult) {
            paramTypes.insert({name, value.GetType()});
        }
        std::map<TString, TString> paramSources;
        TCsvParser parser(std::move(header), ',', "", &paramTypes, &paramSources);
        TParamsBuilder paramBuilder;
        parser.BuildParams(data, paramBuilder, TCsvParser::TParseMetadata{});
        auto values = paramBuilder.Build().GetValues();
        UNIT_ASSERT_EQUAL(values.size(), estimatedResult.size());
        for (const auto& [name, value] : estimatedResult) {
            auto it = values.find(name);
            UNIT_ASSERT_UNEQUAL(it, values.end());
            AssertValuesEqual(value, it->second);
        }
    }

    void CommonTestValue(TString&& header, TString&& data, const TValue& estimatedResult) {
        std::map<std::string, TType> paramTypes;
        for (auto member : estimatedResult.GetType().GetProto().struct_type().members()) {
            paramTypes.insert({member.name(), member.type()});
        }

        TCsvParser parser(std::move(header), ',', "", &paramTypes, nullptr);
        TValueBuilder valueBuilder;
        parser.BuildValue(data, valueBuilder, estimatedResult.GetType(), TCsvParser::TParseMetadata{});
        AssertValuesEqual(valueBuilder.Build(), estimatedResult);
    }

    void CommonTestBuildList(TString&& header, std::vector<TString>&& data, const TValue& estimatedResult) {
        std::map<std::string, TType> columnTypes;
        for (auto member : estimatedResult.GetType().GetProto().list_type().item().struct_type().members()) {
            columnTypes.insert({member.name(), member.type()});
        }

        TCsvParser parser(std::move(header), ',', "", &columnTypes, nullptr);
        parser.BuildLineType();
        TValue builtResult = parser.BuildList(data, "testFile.csv", 0);
        AssertValuesEqual(builtResult, estimatedResult);
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

    Y_UNIT_TEST(IntegerTypesTestList) {
        CommonTestBuildList(
            "int8,int16,int32,int64,uint8,uint16,uint32,uint64",
            {
                "-1,-10000,-1000000,-100000000000,1,10000,1000000,100000000000",
                "-2,-20000,-2000000,-200000000000,2,20000,2000000,200000000000"
            },
            TValueBuilder().BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("int8").Int8(-1)
                    .AddMember("int16").Int16(-10000)
                    .AddMember("int32").Int32(-1000000)
                    .AddMember("int64").Int64(-100000000000)
                    .AddMember("uint8").Uint8(1)
                    .AddMember("uint16").Uint16(10000)
                    .AddMember("uint32").Uint32(1000000)
                    .AddMember("uint64").Uint64(100000000000)
                    .EndStruct()
                .AddListItem().BeginStruct()
                    .AddMember("int8").Int8(-2)
                    .AddMember("int16").Int16(-20000)
                    .AddMember("int32").Int32(-2000000)
                    .AddMember("int64").Int64(-200000000000)
                    .AddMember("uint8").Uint8(2)
                    .AddMember("uint16").Uint16(20000)
                    .AddMember("uint32").Uint32(2000000)
                    .AddMember("uint64").Uint64(200000000000)
                    .EndStruct()
                .EndList().Build());
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

    Y_UNIT_TEST(DateTypesTestBuildList) {
        CommonTestBuildList(
            "dateIso8601,dateTimeIso8601,timestampIso8601,date,datetime,timestamp,interval,tzDate,tzDatetime,tzTimestamp",
            {
                "\"2001-01-01\",\"2001-01-01T12:12:12\",\"2001-01-01T12:12:12.111111\",12000,1200000,120000000,-2000,\"2001-01-01,Europe/Moscow\",\"2001-01-01T12:12:12,Europe/Moscow\",\"2001-01-01T12:12:12.111111,Europe/Moscow\"",
                "\"2001-01-02\",\"2001-01-01T12:12:13\",\"2001-01-01T12:12:12.222222\",13000,1300000,130000000,-3000,\"2001-01-02,Europe/Moscow\",\"2001-01-01T12:12:13,Europe/Moscow\",\"2001-01-01T12:12:12.222222,Europe/Moscow\""
            },
            TValueBuilder().BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("dateIso8601").Date(TInstant::ParseIso8601("2001-01-01"))
                    .AddMember("dateTimeIso8601").Datetime(TInstant::ParseIso8601("2001-01-01T12:12:12"))
                    .AddMember("timestampIso8601").Timestamp(TInstant::ParseIso8601("2001-01-01T12:12:12.111111"))
                    .AddMember("date").Date(TInstant::Days(12000))
                    .AddMember("datetime").Datetime(TInstant::Seconds(1200000))
                    .AddMember("timestamp").Timestamp(TInstant::MicroSeconds(120000000))
                    .AddMember("interval").Interval(-2000)
                    .AddMember("tzDate").TzDate("2001-01-01,Europe/Moscow")
                    .AddMember("tzDatetime").TzDatetime("2001-01-01T12:12:12,Europe/Moscow")
                    .AddMember("tzTimestamp").TzTimestamp("2001-01-01T12:12:12.111111,Europe/Moscow")
                    .EndStruct()
                .AddListItem().BeginStruct()
                    .AddMember("dateIso8601").Date(TInstant::ParseIso8601("2001-01-02"))
                    .AddMember("dateTimeIso8601").Datetime(TInstant::ParseIso8601("2001-01-01T12:12:13"))
                    .AddMember("timestampIso8601").Timestamp(TInstant::ParseIso8601("2001-01-01T12:12:12.222222"))
                    .AddMember("date").Date(TInstant::Days(13000))
                    .AddMember("datetime").Datetime(TInstant::Seconds(1300000))
                    .AddMember("timestamp").Timestamp(TInstant::MicroSeconds(130000000))
                    .AddMember("interval").Interval(-3000)
                    .AddMember("tzDate").TzDate("2001-01-02,Europe/Moscow")
                    .AddMember("tzDatetime").TzDatetime("2001-01-01T12:12:13,Europe/Moscow")
                    .AddMember("tzTimestamp").TzTimestamp("2001-01-01T12:12:12.222222,Europe/Moscow")
                    .EndStruct()
                .EndList().Build());
    }

    Y_UNIT_TEST(OtherPrimitiveTypeTestParams) {
        CommonTestParams("name", "строка", {{"$name", TValueBuilder().Utf8("строка").Build()}});
        CommonTestParams("name", "строка", {{"$name", TValueBuilder().String("строка").Build()}});
        CommonTestParams("name", "true", {{"$name", TValueBuilder().Bool(true).Build()}});
        CommonTestParams("name", "false", {{"$name", TValueBuilder().Bool(false).Build()}});
        CommonTestParams("name", "1.183", {{"$name", TValueBuilder().Float(1.183).Build()}});
        CommonTestParams("name", "1.183", {{"$name", TValueBuilder().Double(1.183).Build()}});
        CommonTestParams("name", "1.183", {{"$name", TValueBuilder().DyNumber("1.183").Build()}});
        CommonTestParams("name", "1.183", {{"$name", TValueBuilder().Decimal(TDecimalValue("1.183", 22, 9)).Build()}});
        CommonTestParams("name", "155555555555555.183", {{"$name", TValueBuilder().Decimal(TDecimalValue("155555555555555.183", 35, 10)).Build()}});
        CommonTestParams("name", "inf", {{"$name", TValueBuilder().Decimal(TDecimalValue("inf", 22, 9)).Build()}});
        CommonTestParams("name", "-inf", {{"$name", TValueBuilder().Decimal(TDecimalValue("-inf", 22, 9)).Build()}});
        CommonTestParams("name", "nan", {{"$name", TValueBuilder().Decimal(TDecimalValue("nan", 22, 9)).Build()}});
        CommonTestParams("name", "-nan", {{"$name", TValueBuilder().Decimal(TDecimalValue("-nan", 22, 9)).Build()}});
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
        CommonTestValue("name", "1.183", MakeStruct("name", TValueBuilder().Decimal(TDecimalValue("1.183", 22, 9)).Build()));
        CommonTestValue("name", "550e8400-e29b-41d4-a716-446655440000", MakeStruct("name", TValueBuilder().Uuid(TUuidValue("550e8400-e29b-41d4-a716-446655440000")).Build()));
        CommonTestValue("name", "\"{\"\"a\"\":10, \"\"b\"\":\"\"string\"\"}\"", MakeStruct("name", TValueBuilder().Json("{\"a\":10, \"b\":\"string\"}").Build()));
        CommonTestValue("name", "строка", MakeStruct("name", TValueBuilder().OptionalUtf8("строка").Build()));
        CommonTestValue("name", "данные", MakeStruct("name", TValueBuilder().Pg(TPgValue(TPgValue::VK_TEXT, "данные", TPgType("some_type"))).Build()));
    }

    Y_UNIT_TEST(OtherPrimitiveTypesTestBuildList) {
        CommonTestBuildList(
            "utf8,string,bool,float,double,dyNumber,decimal,uuid,json,optionalUtf8,pg",
            {
                "строка1,строка2,true,1.183,1.183,\"1.183\",1.183,550e8400-e29b-41d4-a716-446655440000,\"{\"\"a\"\":10, \"\"b\"\":\"\"string1\"\"}\",\"строка1\",\"данные1\"",
                "строка3,строка4,false,1.184,1.184,\"1.184\",1.184,550e8400-e29b-41d4-a716-446655440001,\"{\"\"a\"\":11, \"\"b\"\":\"\"string2\"\"}\",\"строка2\",\"данные2\""
            },
            TValueBuilder().BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("utf8").Utf8("строка1")
                    .AddMember("string").String("строка2")
                    .AddMember("bool").Bool(true)
                    .AddMember("float").Float(1.183)
                    .AddMember("double").Double(1.183)
                    .AddMember("dyNumber").DyNumber("1.183")
                    .AddMember("decimal").Decimal(TDecimalValue("1.183", 22, 9))
                    .AddMember("uuid").Uuid(TUuidValue("550e8400-e29b-41d4-a716-446655440000"))
                    .AddMember("json").Json("{\"a\":10, \"b\":\"string1\"}")
                    .AddMember("optionalUtf8").OptionalUtf8("строка1")
                    .AddMember("pg").Pg(TPgValue(TPgValue::VK_TEXT, "данные1", TPgType("some_type")))
                    .EndStruct()
                .AddListItem().BeginStruct()
                    .AddMember("utf8").Utf8("строка3")
                    .AddMember("string").String("строка4")
                    .AddMember("bool").Bool(false)
                    .AddMember("float").Float(1.184)
                    .AddMember("double").Double(1.184)
                    .AddMember("dyNumber").DyNumber("1.184")
                    .AddMember("decimal").Decimal(TDecimalValue("1.184", 22, 9))
                    .AddMember("uuid").Uuid(TUuidValue("550e8400-e29b-41d4-a716-446655440001"))
                    .AddMember("json").Json("{\"a\":11, \"b\":\"string2\"}")
                    .AddMember("optionalUtf8").OptionalUtf8("строка2")
                    .AddMember("pg").Pg(TPgValue(TPgValue::VK_TEXT, "данные2", TPgType("some_type")))
                    .EndStruct()
                .EndList().Build());
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

    Y_UNIT_TEST(RepeatedEscaping) {
        CommonTestBuildList(
            "col1,escaped2,col3,escaped4,col5",
            {
                "aa,\"text2.1 \"\"text2.2 escaped\"\" text2.3\",bb,\"text4.1 \"\"text4.2 escaped\"\" text4.3\",5"
            },
            TValueBuilder().BeginList()
                .AddListItem().BeginStruct()
                    .AddMember("col1").Utf8("aa")
                    .AddMember("escaped2").Utf8("text2.1 \"text2.2 escaped\" text2.3")
                    .AddMember("col3").Utf8("bb")
                    .AddMember("escaped4").Utf8("text4.1 \"text4.2 escaped\" text4.3")
                    .AddMember("col5").Uint8(5)
                    .EndStruct()
                .EndList().Build());
        // TODO: same tests for BuildParams and BuildValue when NCsvFormat::CsvSplitter will be fixed
    }

    Y_UNIT_TEST(ShuffledColumns) {
        std::map<std::string, TType> tableColumnTypes = {
            {"col1", TTypeBuilder().Primitive(EPrimitiveType::Utf8).Build()},
            {"col2", TTypeBuilder().BeginOptional().Primitive(EPrimitiveType::Int64).EndOptional().Build()},
            {"col3", TTypeBuilder().Primitive(EPrimitiveType::Bool).Build()},
        };

        TString csvHeader = "col4,col3,col5,col1,col6";
        std::vector<TString> data = {
            "col4 unused value,true,col5 unused value,col1 value,col6 unused value"
        };

        TCsvParser parser(std::move(csvHeader), ',', "", &tableColumnTypes, nullptr);
        parser.BuildLineType();
        TValue builtResult = parser.BuildList(data, "testFile.csv", 0);

        TValue expexctedResult = TValueBuilder().BeginList()
            .AddListItem().BeginStruct()
                .AddMember("col3").Bool(true)
                .AddMember("col1").Utf8("col1 value")
                .EndStruct()
            .EndList().Build();
        AssertValuesEqual(builtResult, expexctedResult);
    }
}