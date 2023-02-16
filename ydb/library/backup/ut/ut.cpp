#include <ydb/library/backup/backup.h>
#include <ydb/library/backup/query_builder.h>
#include <ydb/library/backup/util.h>

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/strbuf.h>
#include <library/cpp/string_utils/quote/quote.h>

namespace NYdb {

Y_UNIT_TEST_SUITE(BackupToolValuePrintParse) {

void TestResultSetParsedOk(const TString& protoStr, const TString& expect) {
    Ydb::ResultSet proto;
    google::protobuf::TextFormat::ParseFromString(protoStr, &proto);

    TResultSet result(proto);

    TStringStream got;
    got.Reserve(1 << 10);
    NBackup::ProcessResultSet(got, result);
    UNIT_ASSERT(got.Size());
    UNIT_ASSERT_NO_DIFF(got.Str(), expect);
}

Y_UNIT_TEST(ParseValuesFromString) {
    constexpr ui32 ColSize = 3;
    constexpr ui32 RowSize = 2;
    const EPrimitiveType colType[ColSize] = {EPrimitiveType::Uint32, EPrimitiveType::String, EPrimitiveType::Int64};

    auto tableDesc = NTable::TTableBuilder()
            .AddNullableColumn("ColUint", colType[0])
            .AddNullableColumn("ColStr", colType[1])
            .AddNullableColumn("ColInt", colType[2])
            .Build();

    NBackup::TQueryBuilder qb("path/to/table", tableDesc.GetColumns());
    qb.Begin();
    qb.AddLine("123,\"qwe\",-6454");
    qb.AddLine("984213,\"bwijertqw\",512993");
    TParams params = qb.EndAndGetResultingParams();

    auto value = params.GetValue("$items");
    UNIT_ASSERT(value);

    TValueParser parser(*value);
    UNIT_ASSERT(parser.GetKind() == TTypeParser::ETypeKind::List);

    parser.OpenList();
    for (ui32 row = 0; row < RowSize; ++row) {
        const bool nextItemOk = parser.TryNextListItem();
        UNIT_ASSERT(nextItemOk);
        UNIT_ASSERT(parser.GetKind() == TTypeParser::ETypeKind::Struct);
        parser.OpenStruct();
        for (ui32 col = 0; col < ColSize; ++col) {
            const bool nextMemberOk = parser.TryNextMember();
            UNIT_ASSERT(nextMemberOk);
            UNIT_ASSERT(parser.GetKind() == TTypeParser::ETypeKind::Optional);
            parser.OpenOptional();
            UNIT_ASSERT(parser.GetPrimitiveType() == colType[col]);
            parser.CloseOptional();
        }
        parser.CloseStruct();
    }
    parser.CloseList();
}

Y_UNIT_TEST(ParseValuesFromFile) {
    constexpr ui32 ColSize = 3;
    constexpr ui32 RowSize = 2000;
    const EPrimitiveType colType[ColSize] = {EPrimitiveType::Uint32, EPrimitiveType::String, EPrimitiveType::Int64};

    TTempDir tempDir;
    const TString dataFileName = tempDir.Name() + Sprintf("data_%02d.csv", 0);
    {
        TFile dataFile(dataFileName, CreateAlways | WrOnly);
        TStringStream ss;
        for (ui32 row = 0; row < RowSize; ++row) {
            if (row % 2 == 0) {
                TString col2str = TStringBuilder() << "TestString" << 2 * row << "with number";
                CGIEscape(col2str);
                ss << "null" << ",\"" << col2str << "\"," << row*row << Endl;
            } else {
                ss << row << ",null," << row*row << Endl;
            }
        }
        TString str = ss.Str();
        dataFile.Write(str.Detach(), str.Size());
    }

    auto tableDesc = NTable::TTableBuilder()
            .AddNullableColumn("ColUint", colType[0])
            .AddNullableColumn("ColStr", colType[1])
            .AddNullableColumn("ColInt", colType[2])
            .Build();

    NBackup::TQueryFromFileIterator it("table_path", dataFileName, tableDesc.GetColumns(), 4096, 0, 0);

    ui32 rowsRead = 0;
    while (!it.Empty()) {
        auto params = it.ReadNextGetParams();

        auto value = params.GetValue("$items");
        UNIT_ASSERT(value);
        TValueParser parser(*value);

        UNIT_ASSERT(parser.GetKind() == TTypeParser::ETypeKind::List);
        parser.OpenList();
        while (parser.TryNextListItem()) {
            UNIT_ASSERT(parser.GetKind() == TTypeParser::ETypeKind::Struct);
            parser.OpenStruct();
            for (ui32 col = 0; col < ColSize; ++col) {
                const bool nextMemberOk = parser.TryNextMember();
                UNIT_ASSERT(nextMemberOk);
                UNIT_ASSERT(parser.GetKind() == TTypeParser::ETypeKind::Optional);
                parser.OpenOptional();
                UNIT_ASSERT(parser.GetPrimitiveType() == colType[col]);
                switch (col) {
                case 0: {
                    UNIT_ASSERT(parser.GetPrimitiveType() == EPrimitiveType::Uint32);
                    parser.CloseOptional();
                    const TMaybe<ui32> val = parser.GetOptionalUint32();
                    if (rowsRead % 2 == 0) {
                        UNIT_ASSERT(!val);
                    } else {
                        UNIT_ASSERT(val);
                        UNIT_ASSERT(*val == rowsRead);
                    }
                    break;
                }
                case 1: {
                    UNIT_ASSERT(parser.GetPrimitiveType() == EPrimitiveType::String);
                    parser.CloseOptional();
                    TString col2str = TStringBuilder() << "TestString" << 2 * rowsRead << "with number";
                    const TMaybe<TString> val = parser.GetOptionalString();
                    if (rowsRead % 2 == 0) {
                        UNIT_ASSERT(val);
                        UNIT_ASSERT_STRINGS_EQUAL(*val, col2str);
                    } else {
                        UNIT_ASSERT(!val);
                    }
                    break;
                }
                case 2: {
                    UNIT_ASSERT(parser.GetPrimitiveType() == EPrimitiveType::Int64);
                    parser.CloseOptional();
                    const TMaybe<i64> val = parser.GetOptionalInt64();
                    UNIT_ASSERT(val);
                    UNIT_ASSERT(*val == rowsRead*rowsRead);
                    break;
                }
                default:
                    UNIT_FAIL("Unexpected columt number");
                }
            }
            const bool nextMemberOk = parser.TryNextMember();
            UNIT_ASSERT(!nextMemberOk);
            parser.CloseStruct();
            ++rowsRead;
        }
        parser.CloseList();
    }
    UNIT_ASSERT(rowsRead == RowSize);
}

Y_UNIT_TEST(ResultSetBoolPrintTest) {
    TString resultSetStr = R"_(
        columns:{
            name: "Col1"
            type: { type_id: BOOL }
        }
        rows:{ items:{ bool_value: true } }
        rows:{ items:{ bool_value: false } }
        truncated: false
    )_";

    const TString expect = TStringBuilder()
        << "1" << Endl
        << "0" << Endl;
    TestResultSetParsedOk(resultSetStr, expect);
}

Y_UNIT_TEST(ResultSetInt8PrintTest) {
    TString resultSetStr = R"_(
        columns:{
            name: "Col1"
            type: { type_id: INT8 }
        }
        columns:{
            name: "Col2"
            type: { type_id: UINT8 }
        }
        rows:{
            items:{ int32_value: 5 }
            items:{ uint32_value: 230 }
        }
        rows:{
            items:{ int32_value: -66 }
            items:{ uint32_value: 152 }
        }
        truncated: false
    )_";

    const TString expect = TStringBuilder()
        << "5,230" << Endl
        << "-66,152" << Endl;
    TestResultSetParsedOk(resultSetStr, expect);
}

Y_UNIT_TEST(ResultSetInt16PrintTest) {
    TString resultSetStr = R"_(
        columns:{
            name: "Col1"
            type: { type_id: INT16 }
        }
        columns:{
            name: "Col2"
            type: { type_id: UINT16 }
        }
        rows:{
            items:{ int32_value: 6141 }
            items:{ uint32_value: 60192 }
        }

        rows:{
            items:{ int32_value: -6491 }
            items:{ uint32_value: 10612 }
        }

        truncated: false
    )_";

    const TString expect = TStringBuilder()
        << "6141,60192" << Endl
        << "-6491,10612" << Endl;
    TestResultSetParsedOk(resultSetStr, expect);
}

Y_UNIT_TEST(ResultSetInt32PrintTest) {
    TString resultSetStr = R"_(
        columns:{
            name: "Col6"
            type: { type_id: INT32 }
        }
        columns:{
            name: "Col7"
            type: { type_id: UINT32 }
        }
        rows:{
            items:{ int32_value: 15120 }
            items:{ uint32_value: 5219612 }
        }
        rows:{
            items:{ int32_value: -5052 }
            items:{ uint32_value: 14245121 }
        }
        truncated: false
    )_";

    const TString expect = TStringBuilder()
        << "15120,5219612" << Endl
        << "-5052,14245121" << Endl;
    TestResultSetParsedOk(resultSetStr, expect);
}

Y_UNIT_TEST(ResultSetInt64PrintTest) {
    TString resultSetStr = R"_(
        columns:{
            name: "Col8"
            type: { type_id: INT64 }
        }
        columns:{
            name: "Col9"
            type: { type_id: UINT64 }
        }
        rows:{
            items:{ int64_value: 612421 }
            items:{ uint64_value: 6512460 }
        }
        rows:{
            items:{ int64_value: -6124211241 }
            items:{ uint64_value: 961223124 }
        }
        truncated: false
    )_";

    const TString expect = TStringBuilder()
        << "612421,6512460" << Endl
        << "-6124211241,961223124" << Endl;
    TestResultSetParsedOk(resultSetStr, expect);
}

Y_UNIT_TEST(ResultSetFloatPrintTest) {
    TString resultSetStr = R"_(
        columns:{
            name: "Col1"
            type: { type_id: FLOAT }
        }
        columns:{
            name: "Col2"
            type: { type_id: DOUBLE }
        }
        rows:{
            items:{ float_value: .23 }
            items:{ double_value: 5125.123155 }
        }
        rows:{
            items:{ float_value: -6531.124 }
            items:{ double_value: -5012.23123155 }
        }
        rows:{
            items:{ float_value: -inf }
            items:{ double_value: inf }
        }
        rows:{
            items:{ float_value: nan }
            items:{ double_value: -nan }
        }
        truncated: false
    )_";

    const TString expect = TStringBuilder()
        << "0.23,5125.123155" << Endl
        << "-6531.12,-5012.231232" << Endl
        << "-inf,inf" << Endl
        << "nan,nan" << Endl;
    TestResultSetParsedOk(resultSetStr, expect);
}


Y_UNIT_TEST(ResultSetIntarvalsPrintTest) {
    TString resultSetStr = R"_(
        columns:{
            name: "Col1"
            type: { type_id: DATE }
        }
        columns:{
            name: "Col2"
            type: { type_id: DATETIME }
        }
        columns:{
            name: "Col3"
            type: { type_id: TIMESTAMP }
        }
        rows:{
            items:{ uint32_value: 17966 }
            items:{ uint32_value: 1552321844 }
            items:{ uint64_value: 1552321844314382 }
        }
        rows:{
            items:{ uint32_value: 8905 }
            items:{ uint32_value: 769417971 }
            items:{ uint64_value: 769417971968123 }
        }
        truncated: false
    )_";

    const TString expect = TStringBuilder()
        << "2019-03-11T00:00:00.000000Z,2019-03-11T16:30:44.000000Z,2019-03-11T16:30:44.314382Z" << Endl
        << "1994-05-20T00:00:00.000000Z,1994-05-20T07:12:51.000000Z,1994-05-20T07:12:51.968123Z" << Endl;
    TestResultSetParsedOk(resultSetStr, expect);
}

Y_UNIT_TEST(ResultSetStringPrintTest) {
    TString resultSetStr = R"_(
        columns:{
            name: "Col1"
            type: { type_id: STRING }
        }
        columns:{
            name: "Col2"
            type: { type_id: STRING }
        }
        columns:{
            name: "Col3"
            type: { type_id: STRING }
        }
        rows:{
            items:{ bytes_value: "simplestring" }
            items:{ bytes_value: "Space_And_Underscore Containing String" }
            items:{ bytes_value: "String\"with\"quote\"marks" }
        }
        rows:{
            items:{ bytes_value: "~Allowed.symbols_string;!*@$^/" }
            items:{ bytes_value: "NotAllowed\":\n#%&(),\\|" }
            items:{ bytes_value: "String,with,commas.and.dots" }
        }
        truncated: false
    )_";

    const TString expect = TStringBuilder()
        << "\"simplestring\","
           "\"Space_And_Underscore+Containing+String\","
           "\"String%22with%22quote%22marks\"" << Endl
        << "\"~Allowed.symbols_string;!*@$%5E/\","
           "\"NotAllowed%22%3A%0A%23%25%26%28%29%2C%5C%7C\","
           "\"String%2Cwith%2Ccommas.and.dots\"" << Endl;
    TestResultSetParsedOk(resultSetStr, expect);
}

Y_UNIT_TEST(ResultSetUtf8PrintTest) {
    TString resultSetStr = R"_(
        columns:{
            name: "Col1"
            type: { type_id: UTF8 }
        }
        columns:{
            name: "Col12"
            type: { type_id: UTF8 }
        }
        rows:{
            items:{ text_value: "Текст на русском" }
            items:{ text_value: "Русские.,/mixed?with_ASCII" }
        }
        rows:{
            items:{ text_value: "Just-utf–8—text" }
            items:{ text_value: "Another┼text" }
        }
        truncated: false
    )_";

    const TString expect = TStringBuilder()
        << "\"%D0%A2%D0%B5%D0%BA%D1%81%D1%82+%D0%BD%D0%B0+%D1%80%D1%83%D1%81%D1%81%D0%BA%D0%BE%D0%BC\","
                                "\"%D0%A0%D1%83%D1%81%D1%81%D0%BA%D0%B8%D0%B5.%2C/mixed%3Fwith_ASCII\"" << Endl
        << "\"Just-utf%E2%80%938%E2%80%94text\",\"Another%E2%94%BCtext\"" << Endl;
    TestResultSetParsedOk(resultSetStr, expect);
}

Y_UNIT_TEST(ResultSetVoidPrintTest) {
    TString resultSetStr = R"_(
        columns:{
            name: "Col1"
            type:{ optional_type:{ item:{ type_id: INT32 } } }
        }
        columns:{
            name: "Col12"
            type:{ optional_type:{ item:{ type_id: STRING } } }
        }
        rows:{
            items:{ null_flag_value: NULL_VALUE }
            items:{ nested_value:{ bytes_value: "Not_null_string" } }
        }
        rows:{
            items:{ nested_value:{ int32_value: -752192 } }
            items:{ null_flag_value: NULL_VALUE }
        }
        truncated: false
    )_";

    const TString expect = TStringBuilder()
        << "null,\"Not_null_string\"" << Endl
        << "-752192,null" << Endl;
    TestResultSetParsedOk(resultSetStr, expect);
}

Y_UNIT_TEST(ResultSetDecimalPrintTest) {
    TString resultSetStr = R"_(
        columns:{
            name: "Col1"
            type:{ optional_type:{ item:{ type_id: INT32 } } }
        }
        columns:{
            name: "Col12"
            type:{ decimal_type: { precision: 23 scale: 8 } }
        }
        rows:{
            items:{ nested_value:{ int32_value: 1 } }
            items:{ high_128: 123 low_128: 456 }
        }
        rows:{
            items:{ nested_value:{ int32_value: 2 } }
            items:{ high_128: 93 low_128: 72 }
        }
        truncated: false
    )_";

    const TString expect = TStringBuilder()
        << "1,22689495210662.74849224" << Endl
        << "2,17155471988549.8830036" << Endl;
    TestResultSetParsedOk(resultSetStr, expect);
}

Y_UNIT_TEST(ResultSetDyNumberPrintTest) {
    TString resultSetStr = R"_(
        columns:{
            name: "Col1"
            type: { type_id: DYNUMBER }
        }
        rows:{ items:{ text_value: "0" } }
        rows:{ items:{ text_value: ".0" } }
        rows:{ items:{ text_value: "0.93" } }
        rows:{ items:{ text_value: "1" } }
        rows:{ items:{ text_value: "-1" } }
        rows:{ items:{ text_value: "724.1" } }
        rows:{ items:{ text_value: "1E-130" } }
        rows:{ items:{ text_value: "9.9999999999999999999999999999999999999E+125" } }
        rows:{ items:{ text_value: "-1E-130" } }
        rows:{ items:{ text_value: "-9.9999999999999999999999999999999999999E+125" } }
        truncated: false
    )_";

    const TString expect = TStringBuilder()
        << "0" << Endl
        << ".0" << Endl
        << "0.93" << Endl
        << "1" << Endl
        << "-1" << Endl
        << "724.1" << Endl
        << "1E-130" << Endl
        << "9.9999999999999999999999999999999999999E+125" << Endl
        << "-1E-130" << Endl
        << "-9.9999999999999999999999999999999999999E+125" << Endl;
    TestResultSetParsedOk(resultSetStr, expect);
}

Y_UNIT_TEST(ResultSetJsonDocumentPrintTest) {
    TString resultSetStr = R"_(
        columns:{
            name: "Col1"
            type: { type_id: JSON_DOCUMENT }
        }
        rows:{ items:{ text_value: '{"key": "value"}' } }
        rows:{ items:{ text_value: '{"key": 0}' } }
        rows:{ items:{ text_value: '{"key": 0.12}' } }
        rows:{ items:{ text_value: '{"key": -1}' } }
        rows:{ items:{ text_value: '{"key": [1, 2]}' } }
        rows:{ items:{ text_value: '{"key": {"1": "one", "2": 2}}' } }
        truncated: false
    )_";

    const TString expect = TStringBuilder()
        << "\"" << CGIEscapeRet(R"({"key": "value"})") << "\"" << Endl
        << "\"" << CGIEscapeRet(R"({"key": 0})") << "\"" << Endl
        << "\"" << CGIEscapeRet(R"({"key": 0.12})") << "\"" << Endl
        << "\"" << CGIEscapeRet(R"({"key": -1})") << "\"" << Endl
        << "\"" << CGIEscapeRet(R"({"key": [1, 2]})") << "\"" << Endl
        << "\"" << CGIEscapeRet(R"({"key": {"1": "one", "2": 2}})") << "\"" << Endl;
    TestResultSetParsedOk(resultSetStr, expect);
}

}

Y_UNIT_TEST_SUITE(UtilTest) {

Y_UNIT_TEST(SizeFromStringParsing) {
    UNIT_ASSERT_EQUAL(SizeFromString("1"), 1);
    UNIT_ASSERT_EQUAL(SizeFromString("582"), 582);
    UNIT_ASSERT_EQUAL(SizeFromString("852421"), 852421);
}

Y_UNIT_TEST(SizeFromStringParsingWithDecimalPrefix) {
    UNIT_ASSERT_EQUAL(SizeFromString("1K"), 1000);
    UNIT_ASSERT_EQUAL(SizeFromString("9238M"), ui64{9238}*1000*1000);
    UNIT_ASSERT_EQUAL(SizeFromString("12315G"), ui64{12315}*1000*1000*1000);
    UNIT_ASSERT_EQUAL(SizeFromString("642T"), ui64{642}*1000*1000*1000*1000);
}

Y_UNIT_TEST(SizeFromStringParsingWithBinaryPrefix) {
    UNIT_ASSERT_EQUAL(SizeFromString("1Ki"), 1024);
    UNIT_ASSERT_EQUAL(SizeFromString("692Mi"), ui64{692}*1024*1024);
    UNIT_ASSERT_EQUAL(SizeFromString("42851Gi"), ui64{42851}*1024*1024*1024);
    UNIT_ASSERT_EQUAL(SizeFromString("8321Ti"), ui64{8321}*1024*1024*1024*1024);
}

Y_UNIT_TEST(SizeFromStringParsingErrors) {
    UNIT_CHECK_GENERATED_EXCEPTION(SizeFromString(""), yexception);
    UNIT_CHECK_GENERATED_EXCEPTION(SizeFromString("123i321"), yexception);
    UNIT_CHECK_GENERATED_EXCEPTION(SizeFromString("12KK"), yexception);
    UNIT_CHECK_GENERATED_EXCEPTION(SizeFromString("12k"), yexception);
    UNIT_CHECK_GENERATED_EXCEPTION(SizeFromString("12UI"), yexception);
    UNIT_CHECK_GENERATED_EXCEPTION(SizeFromString("NR"), yexception);
}

Y_UNIT_TEST(PathParseTest) {
    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("/my_db/", "/my_db/"), "/");
    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("/my_db/", "/my_db"), "/");
    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("/my_db", "/my_db/"), "/");
    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("/my_db", "/my_db"), "/");
    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("my_db/", "/my_db/"), "/");
    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("my_db/", "/my_db"), "/");
    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("my_db", "/my_db/"), "/");
    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("my_db", "/my_db"), "/");
    UNIT_CHECK_GENERATED_EXCEPTION(RelPathFromAbsolute("/my_db", "my_db"), yexception);

    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("/my_db/", "/my_db/my_folder"), "my_folder");
    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("/my_db/", "/my_db/my_folder/"), "my_folder/");
    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("/my_db", "/my_db/my_folder"), "my_folder");
    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("/my_db", "/my_db/my_folder/"), "my_folder/");
    UNIT_CHECK_GENERATED_EXCEPTION(RelPathFromAbsolute("/my_db", "my_db/my_folder"), yexception);
    UNIT_CHECK_GENERATED_EXCEPTION(RelPathFromAbsolute("/my_db", "/other_db/my_folder"), yexception);

    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("/ru/my_db/", "/ru/my_db/my_folder"), "my_folder");
    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("/ru/my_db/", "/ru/my_db/"), "/");
    UNIT_ASSERT_EQUAL(RelPathFromAbsolute("/ru/my_db/", "/ru/my_db"), "/");
    UNIT_CHECK_GENERATED_EXCEPTION(RelPathFromAbsolute("/ru/my_db", "/ru/my_"), yexception);
    UNIT_CHECK_GENERATED_EXCEPTION(RelPathFromAbsolute("/ru/my_db", "/ru"), yexception);
    UNIT_CHECK_GENERATED_EXCEPTION(RelPathFromAbsolute("/ru/my_db", "/"), yexception);
    UNIT_CHECK_GENERATED_EXCEPTION(RelPathFromAbsolute("/ru/my_db", ""), yexception);
}

}

} // NYdb
