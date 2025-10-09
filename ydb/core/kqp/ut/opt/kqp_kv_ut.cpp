#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/codec.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <util/system/env.h>


extern "C" {
#include "catalog/pg_type_d.h"
}

namespace
{

using namespace NYdb;
using namespace NYdb::NTable;

struct ReadRowsPgParam
{
    ui32 TypeId;
    TString TypeMod;
    TString ValueContent;
};

} // namespace

namespace
{

using namespace NYdb;
using namespace NYdb::NTable;

template <typename T>
void ValidateSinglePgRowResult(T& result, const TString& columnName, const TPgValue& expectedValue) {
    TResultSetParser parser{result.GetResultSet()};
    UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 1);

    bool gotRows = false;
    while( parser.TryNextRow()) {
        gotRows = true;
        auto& col = parser.ColumnParser(columnName);

        const auto pgValue = col.GetPg();
        UNIT_ASSERT_VALUES_EQUAL(pgValue.Content_, expectedValue.Content_);
        UNIT_ASSERT_VALUES_EQUAL(pgValue.Kind_, expectedValue.Kind_);
        UNIT_ASSERT_VALUES_EQUAL(pgValue.PgType_.TypeName, expectedValue.PgType_.TypeName);
        UNIT_ASSERT_VALUES_EQUAL(pgValue.PgType_.TypeModifier, expectedValue.PgType_.TypeModifier);
    }
    Y_ENSURE(gotRows, "empty select result");
}

} // namespace

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpKv) {
    Y_UNIT_TEST(BulkUpsert) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            --!syntax_v1

            CREATE TABLE TestTable (
                Key Int64,
                Data Uint32,
                Value Utf8,
                PRIMARY KEY (Key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 10; ++i) {
            rows.AddListItem()
                .BeginStruct()
                .AddMember("Key").Int64(i)
                .AddMember("Data").Uint32(i*137)
                .AddMember("Value").Utf8("abcde")
                .EndStruct();
        }
        rows.EndList();

        auto upsertResult = db.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM TestTable ;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);
        auto res = FormatResultSetYson(result.GetResultSet(0));
        Cout << res << Endl;
        CompareYson(R"(
            [[[0u];[0];["abcde"]];[[137u];[1];["abcde"]];[[274u];[2];["abcde"]];[[411u];[3];["abcde"]];[[548u];[4];["abcde"]];[[685u];[5];["abcde"]];[[822u];[6];["abcde"]];[[959u];[7];["abcde"]];[[1096u];[8];["abcde"]];[[1233u];[9];["abcde"]]]
        )", res);
    }

    Y_UNIT_TEST(ReadRows_SpecificKey) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE TestTable (
                Key Uint64,
                Data Uint32,
                Value Utf8,
                PRIMARY KEY (Key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 1000; ++i) {
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(i * 1921763474923857134ull + 1858343823)
                    .AddMember("Data").Uint32(i)
                    .AddMember("Value").Utf8("abcde")
                .EndStruct();
        }
        rows.EndList();

        auto upsertResult = db.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder keys;
        keys.BeginList();
        for (size_t i = 0; i < 5; ++i) {
            keys.AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(i * 1921763474923857134ull + 1858343823)
                .EndStruct();
        }
        keys.EndList();
        auto selectResult = db.ReadRows("/Root/TestTable", keys.Build()).GetValueSync();
        Cerr << "IsSuccess(): " << selectResult.IsSuccess() << " GetStatus(): " << selectResult.GetStatus() << Endl;
        UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());
        auto res = FormatResultSetYson(selectResult.GetResultSet());
        CompareYson(R"(
            [
                [[1858343823u];[0u];["abcde"]];
                [[1921763476782200957u];[1u];["abcde"]];
                [[3843526951706058091u];[2u];["abcde"]];
                [[5765290426629915225u];[3u];["abcde"]];
                [[7687053901553772359u];[4u];["abcde"]]
            ]
        )", res);
    }

    Y_UNIT_TEST(ReadRows_UnknownTable) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE TestTable (
                Key Uint64,
                Data Uint32,
                Value Utf8,
                PRIMARY KEY (Key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder keys;
        keys.BeginList();
        for (size_t i = 0; i < 5; ++i) {
            keys.AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(i * 1921763474923857134ull + 1858343823)
                .EndStruct();
        }
        keys.EndList();
        auto selectResult = db.ReadRows("/Root/WrongTable", keys.Build()).GetValueSync();
        UNIT_ASSERT_C(!selectResult.IsSuccess(), selectResult.GetIssues().ToString());
        UNIT_ASSERT_C(selectResult.GetIssues().ToString().Size(), "Expect non-empty issue in case of error");
        UNIT_ASSERT_EQUAL(selectResult.GetStatus(), EStatus::SCHEME_ERROR);
        auto res = FormatResultSetYson(selectResult.GetResultSet());
        CompareYson("[]", res);
    }

    Y_UNIT_TEST(ReadRows_NonExistentKeys) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE TestTable (
                Key Uint64,
                Data Uint32,
                Value Utf8,
                PRIMARY KEY (Key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 1000; ++i) {
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(i + 10)
                    .AddMember("Data").Uint32(i)
                    .AddMember("Value").Utf8("abcde")
                .EndStruct();
        }
        rows.EndList();

        auto upsertResult = db.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        {
            NYdb::TValueBuilder keys;
            keys.BeginList();
            // there is no keys with i < 5
            for (size_t i = 0; i < 5; ++i) {
                keys.AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(i)
                    .EndStruct();
            }
            keys.EndList();

            auto selectResult = db.ReadRows("/Root/TestTable", keys.Build()).GetValueSync();
            Cerr << "IsSuccess(): " << selectResult.IsSuccess() << " GetStatus(): " << selectResult.GetStatus() << Endl;
            UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());
            auto res = FormatResultSetYson(selectResult.GetResultSet());
            Cerr << res << Endl;
            CompareYson("[]", res);
        }
        {
            NYdb::TValueBuilder keys;
            keys.BeginList();
            // there are no keys with i < 10, but 5 keys with i in [10, 15)
            for (size_t i = 0; i < 15; ++i) {
                keys.AddListItem()
                    .BeginStruct()
                        .AddMember("Key").Uint64(i)
                    .EndStruct();
            }
            keys.EndList();

            auto selectResult = db.ReadRows("/Root/TestTable", keys.Build()).GetValueSync();
            Cerr << "IsSuccess(): " << selectResult.IsSuccess() << " GetStatus(): " << selectResult.GetStatus() << Endl;
            UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());
            auto res = FormatResultSetYson(selectResult.GetResultSet());
            CompareYson(R"([
                [[10u];[0u];["abcde"]];
                [[11u];[1u];["abcde"]];
                [[12u];[2u];["abcde"]];
                [[13u];[3u];["abcde"]];
                [[14u];[4u];["abcde"]]
            ])", TString{res});
        }
        {
            NYdb::TValueBuilder keys;
            keys.BeginList();
            keys.EndList();

            auto selectResult = db.ReadRows("/Root/TestTable", keys.Build()).GetValueSync();
            UNIT_ASSERT_EQUAL(selectResult.GetStatus(), EStatus::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(ReadRows_NotFullPK) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE TestTable (
                Key Uint64,
                Data Uint32,
                Value Utf8,
                PRIMARY KEY (Key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder keys;
        keys.BeginList()
            .AddListItem()
                .BeginStruct()
                .EndStruct()
        .EndList();

        auto selectResult = db.ReadRows("/Root/TestTable", keys.Build()).GetValueSync();
        UNIT_ASSERT_C(!selectResult.IsSuccess(), selectResult.GetIssues().ToString());
        UNIT_ASSERT_C(selectResult.GetIssues().ToString().Size(), "Expect non-empty issues in case of error");
    }

    Y_UNIT_TEST(ReadRows_SpecificReturnValue) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        auto kikimr = TKikimrRunner{settings};

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        const auto tableName = "/Root/TestTable";
        const auto keyColumnName = "Key";
        const auto valueToReturnColumnName_1 = "Value_1";
        const auto valueToReturnColumnName_2 = "Value_2";
        const auto valueToIgnoreColumnName = "Value_3";

        TTableBuilder builder;
        builder.AddNonNullableColumn(keyColumnName, EPrimitiveType::Uint64);
        builder.SetPrimaryKeyColumn(keyColumnName);
        builder.AddNullableColumn(valueToReturnColumnName_1, EPrimitiveType::Uint64);
        builder.AddNullableColumn(valueToReturnColumnName_2, EPrimitiveType::Uint64);
        builder.AddNullableColumn(valueToIgnoreColumnName, EPrimitiveType::Uint64);

        auto result = session.CreateTable(tableName, builder.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const ui64 keyValue = 1;
        const ui64 valueToReturn_1 = 2;
        const ui64 valueToReturn_2 = 3;
        const ui64 valueToIgnore = 4;

        NYdb::TValueBuilder rows;
        rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                    .AddMember(keyColumnName).Uint64(keyValue)
                    .AddMember(valueToReturnColumnName_1).Uint64(valueToReturn_1)
                    .AddMember(valueToReturnColumnName_2).Uint64(valueToReturn_2)
                    .AddMember(valueToIgnoreColumnName).Uint64(valueToIgnore)
                .EndStruct();
        rows.EndList();

        auto upsertResult = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder keys;
        keys.BeginList();
            keys.AddListItem()
                .BeginStruct()
                    .AddMember(keyColumnName).Uint64(keyValue)
                .EndStruct();
        keys.EndList();

        auto selectResult = db.ReadRows(tableName, keys.Build(), {valueToReturnColumnName_1, valueToReturnColumnName_2}).GetValueSync();
        Cerr << "IsSuccess(): " << selectResult.IsSuccess() << " GetStatus(): " << selectResult.GetStatus() << Endl;
        UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());

        auto res = FormatResultSetYson(selectResult.GetResultSet());
        CompareYson(Sprintf("[[[%du];[%du]]]", valueToReturn_1, valueToReturn_2), TString{res});
    }

    Y_UNIT_TEST_TWIN(ReadRows_ExternalBlobs, UseExtBlobsPrecharge) {
        NKikimrConfig::TImmediateControlsConfig controls;

        if (UseExtBlobsPrecharge) {
            controls.MutableDataShardControls()->SetReadIteratorKeysExtBlobsPrecharge(1); // sets to "true"
        }

        NKikimrConfig::TFeatureFlags flags;
        flags.SetEnablePublicApiExternalBlobs(true);
        auto settings = TKikimrSettings()
            .SetFeatureFlags(flags)
            .SetWithSampleTables(false)
            .SetControls(controls);
        auto kikimr = TKikimrRunner{settings};

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        const auto tableName = "/Root/TestTable";
        const auto keyColumnName_1 = "blob_id";
        const auto keyColumnName_2 = "chunk_num";
        const auto dataColumnName = "data";

        TTableBuilder builder;
        builder
            .BeginStorageSettings()
                .SetExternal("test")
                .SetStoreExternalBlobs(true)
            .EndStorageSettings();
        builder.AddNonNullableColumn(keyColumnName_1, EPrimitiveType::Uuid);
        builder.AddNonNullableColumn(keyColumnName_2, EPrimitiveType::Int32);
        builder.SetPrimaryKeyColumns({keyColumnName_1, keyColumnName_2});
        builder.AddNullableColumn(dataColumnName, EPrimitiveType::String);

        auto result = session.CreateTable(tableName, builder.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        TString largeValue(1_MB, 'L');
        
        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (int i = 0; i < 10; i++) {
            rows.AddListItem()
                .BeginStruct()
                    .AddMember(keyColumnName_1).Uuid(NYdb::TUuidValue("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"))
                    .AddMember(keyColumnName_2).Int32(i)
                    .AddMember(dataColumnName).String(largeValue)
                .EndStruct();
        }
        rows.EndList();

        auto upsertResult = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder keys;
        keys.BeginList();
        for (int i = 0; i < 10; i++) {
            keys.AddListItem()
                .BeginStruct()
                    .AddMember(keyColumnName_1).Uuid(NYdb::TUuidValue("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"))
                    .AddMember(keyColumnName_2).Int32(i)
                .EndStruct();
        }
        keys.EndList();

        auto server = &kikimr.GetTestServer();

        WaitForCompaction(server, tableName);

        auto selectResult = db.ReadRows(tableName, keys.Build()).GetValueSync();

        UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());

        TResultSetParser parser{selectResult.GetResultSet()};
        UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 10);

        UNIT_ASSERT(parser.TryNextRow());

        auto val = parser.GetValue(0);
        TValueParser valParser(val);
        TUuidValue v = valParser.GetUuid();
        Cout << v.ToString() << Endl;
    }

    TVector<::ReadRowsPgParam> readRowsPgParams
    {
        {.TypeId = BOOLOID, .TypeMod={}, .ValueContent="t"},
        {.TypeId = CHAROID, .TypeMod={}, .ValueContent="v"},
        {.TypeId = INT2OID, .TypeMod={}, .ValueContent="42"},
        {.TypeId = INT4OID, .TypeMod={}, .ValueContent="42"},
        {.TypeId = INT8OID, .TypeMod={}, .ValueContent="42"},
        {.TypeId = FLOAT4OID, .TypeMod={}, .ValueContent="42.42"},
        {.TypeId = FLOAT8OID, .TypeMod={}, .ValueContent="42.42"},
        {.TypeId = TEXTOID, .TypeMod={}, .ValueContent="i'm a text"},
        {.TypeId = BPCHAROID, .TypeMod={}, .ValueContent="i'm a text"},
        {.TypeId = VARCHAROID, .TypeMod={}, .ValueContent="i'm a text"},
        {.TypeId = NAMEOID, .TypeMod={}, .ValueContent="i'm a text"},
        {.TypeId = NUMERICOID, .TypeMod={}, .ValueContent="42.42"},
        {.TypeId = MONEYOID, .TypeMod={}, .ValueContent="$42.42"},
        {.TypeId = DATEOID, .TypeMod={}, .ValueContent="1999-01-01"},
        {.TypeId = TIMEOID, .TypeMod={}, .ValueContent="23:59:59.999"},
        {.TypeId = TIMESTAMPOID, .TypeMod={}, .ValueContent="1999-01-01 23:59:59.999"},
        {.TypeId = TIMETZOID, .TypeMod={}, .ValueContent="23:59:59.999+03"},
        {.TypeId = TIMESTAMPTZOID, .TypeMod={}, .ValueContent="1999-01-01 23:59:59.999+00"},
        {.TypeId = INTERVALOID, .TypeMod={}, .ValueContent="1 year 2 mons 3 days 01:02:03"},
        {.TypeId = BITOID, .TypeMod={}, .ValueContent="1011"},
        {.TypeId = VARBITOID, .TypeMod={}, .ValueContent="1011"},
        {.TypeId = POINTOID, .TypeMod={}, .ValueContent="(42,24)"},
        {.TypeId = LINEOID, .TypeMod={}, .ValueContent="{42,24,13}"},
        {.TypeId = LSEGOID, .TypeMod={}, .ValueContent="[(0,0),(42,42)]"},
        {.TypeId = BOXOID, .TypeMod={}, .ValueContent="(42,42),(0,0)"},
        {.TypeId = PATHOID, .TypeMod={}, .ValueContent="((0,0),(42,42),(13,13))"},
        {.TypeId = POLYGONOID, .TypeMod={}, .ValueContent="((0,0),(42,42),(13,13))"},
        {.TypeId = CIRCLEOID, .TypeMod={}, .ValueContent="<(0,0),42>"},
        {.TypeId = INETOID, .TypeMod={}, .ValueContent="127.0.0.1/16"},
        {.TypeId = CIDROID, .TypeMod={}, .ValueContent="16.0.0.0/8"},
        {.TypeId = MACADDROID, .TypeMod={}, .ValueContent="08:00:2b:01:02:03"},
        {.TypeId = MACADDR8OID, .TypeMod={}, .ValueContent="08:00:2b:01:02:03:04:05"},
        {.TypeId = UUIDOID, .TypeMod={}, .ValueContent="00000000-0000-0000-0000-000000000042"},
        {.TypeId = JSONOID, .TypeMod={}, .ValueContent="{\"value\": 42}"},
        {.TypeId = JSONBOID, .TypeMod={}, .ValueContent="{\"value\": 42}"},
        {.TypeId = JSONPATHOID, .TypeMod={}, .ValueContent="($.\"field\"[*] > 42)"},
        {.TypeId = XMLOID, .TypeMod={}, .ValueContent="<tag>42</tag>"},
        {.TypeId = TSQUERYOID, .TypeMod={}, .ValueContent="'a' & 'b1'"},
        {.TypeId = TSVECTOROID, .TypeMod={}, .ValueContent="'cat' 'fat' '|'"},
        {.TypeId = INT2VECTOROID, .TypeMod={}, .ValueContent="42 24 13"},
        {.TypeId = BYTEAOID, .TypeMod={}, .ValueContent="\\x627974656120ff"},
        {.TypeId = BYTEAARRAYOID, .TypeMod={}, .ValueContent="{\"\\\\x61ff\",\"\\\\x6231ff\"}"},
        {.TypeId = BPCHAROID, .TypeMod="4", .ValueContent="abcd"},
        {.TypeId = VARCHAROID, .TypeMod="4", .ValueContent="abcd"},
        {.TypeId = BITOID, .TypeMod="4", .ValueContent="1101"},
        {.TypeId = VARBITOID, .TypeMod="4", .ValueContent="1101"},
        {.TypeId = NUMERICOID, .TypeMod="4", .ValueContent="1234"},
        {.TypeId = TIMEOID, .TypeMod="4", .ValueContent="23:59:59.9999"},
        {.TypeId = TIMETZOID, .TypeMod="4", .ValueContent="23:59:59.9999+00"},
        {.TypeId = TIMESTAMPOID, .TypeMod="4", .ValueContent="1999-01-01 23:59:59.9999"},
        {.TypeId = TIMESTAMPTZOID, .TypeMod="4", .ValueContent="1999-01-01 23:59:59.9999+00"},
    };
    ::ReadRowsPgParam readRowsPgNullParam{.TypeId = BOOLOID, .TypeMod={}, .ValueContent=""};

    Y_UNIT_TEST(ReadRows_PgValue) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        auto kikimr = TKikimrRunner{settings};

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        const auto tableName = "/Root/TestTable";
        const auto keyColumnName = "Key";
        const auto valueColumnName = "Value";

        const auto testSingle = [&](const ::ReadRowsPgParam& testParam, bool isNull)
        {
            auto typeDesc = NPg::TypeDescFromPgTypeId(testParam.TypeId);
            UNIT_ASSERT(!!typeDesc);
            const auto typeName = NPg::PgTypeNameFromTypeDesc(typeDesc);
            const auto& pgType = TPgType(typeName, testParam.TypeMod);

            Cout << Sprintf("TestParam: type: `%s`; mod: `%s`; is null: %s\n", typeName.data(), testParam.TypeMod.data(), isNull ? "+" : "-" );

            TTableBuilder builder;
            builder.AddNonNullableColumn(keyColumnName, EPrimitiveType::Uint64);
            builder.SetPrimaryKeyColumn(keyColumnName);
            builder.AddNullableColumn(valueColumnName, pgType);

            auto result = session.CreateTable(tableName, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            const ui64 keyValue = 1;
            const TPgValue pgValue(
                isNull ? TPgValue::VK_NULL : TPgValue::VK_TEXT,
                testParam.ValueContent,
                pgType
            );
            NYdb::TValueBuilder rows;
            rows.BeginList();
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember(keyColumnName).Uint64(keyValue)
                        .AddMember(valueColumnName).Pg(pgValue)
                    .EndStruct();
            rows.EndList();
            auto upsertResult = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

            NYdb::TValueBuilder keys;
            keys.BeginList();
                keys.AddListItem()
                    .BeginStruct()
                        .AddMember(keyColumnName).Uint64(keyValue)
                    .EndStruct();
            keys.EndList();
            auto selectResult = db.ReadRows(tableName, keys.Build()).GetValueSync();
            UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());

            ValidateSinglePgRowResult(selectResult, valueColumnName, pgValue);

            result = session.DropTable(tableName).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        for (const auto& testParam: readRowsPgParams)
        {
            testSingle(testParam, false);
        }
        testSingle(readRowsPgNullParam, true);
    }

    TVector<::ReadRowsPgParam> readRowsPgKeyParams
    {
        {.TypeId = TEXTOID, .TypeMod={}, .ValueContent="i'm a text"},
        {.TypeId = BITOID, .TypeMod="4", .ValueContent="0110"},
    };
    ::ReadRowsPgParam readRowsPgNullKeyParam{.TypeId = TEXTOID, .TypeMod={}, .ValueContent=""};

    Y_UNIT_TEST(ReadRows_PgKey) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        auto kikimr = TKikimrRunner{settings};

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        const auto tableName = "/Root/TestTable";
        const auto keyColumnName = "Key";
        const auto valueColumnName = "Value";

        const auto testSingle = [&](const ::ReadRowsPgParam& testParam, bool isNull)
        {
            auto typeDesc = NPg::TypeDescFromPgTypeId(testParam.TypeId);
            UNIT_ASSERT(!!typeDesc);
            const auto typeName = NPg::PgTypeNameFromTypeDesc(typeDesc);
            const auto& pgType = TPgType(typeName, testParam.TypeMod);

            Cout << Sprintf("TestParam: type: `%s`; mod: `%s`; is null: %s\n", typeName.data(), testParam.TypeMod.data(), isNull ? "+" : "-" );

            TTableBuilder builder;
            builder.AddNullableColumn(keyColumnName, pgType);
            builder.SetPrimaryKeyColumn(keyColumnName);
            builder.AddNullableColumn(valueColumnName, EPrimitiveType::Uint64);

            auto result = session.CreateTable(tableName, builder.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            const TPgValue pgValue(
                isNull ? TPgValue::VK_NULL : TPgValue::VK_TEXT,
                testParam.ValueContent,
                pgType
            );
            const ui64 value = 1;
            NYdb::TValueBuilder rows;
            rows.BeginList();
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember(keyColumnName).Pg(pgValue)
                        .AddMember(valueColumnName).Uint64(value)
                    .EndStruct();
            rows.EndList();
            auto upsertResult = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

            NYdb::TValueBuilder keys;
            keys.BeginList();
                keys.AddListItem()
                    .BeginStruct()
                        .AddMember(keyColumnName).Pg(pgValue)
                    .EndStruct();
            keys.EndList();
            auto selectResult = db.ReadRows(tableName, keys.Build()).GetValueSync();
            UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());

            ValidateSinglePgRowResult(selectResult, keyColumnName, pgValue);

            result = session.DropTable(tableName).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        for (const auto& testParam: readRowsPgKeyParams)
        {
            testSingle(testParam, false);
        }
        testSingle(readRowsPgNullKeyParam, true);
    }

    Y_UNIT_TEST(ReadRows_Decimal) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE TestTable (
                Key22 Decimal(22,9),
                Key35 Decimal(35,10),
                Value22 Decimal(22,9),
                Value35 Decimal(35,10),
                ValueInt Uint64,
                PRIMARY KEY (Key22, Key35)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        // Bad case: upsert Uin64 to Decimal column
        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key22").Decimal(TDecimalValue("0", 22, 9))
                    .AddMember("Key35").Uint64(0)
                    .AddMember("Value22").Decimal(TDecimalValue("0", 22, 9))
                    .AddMember("Value35").Decimal(TDecimalValue("0", 35, 10))
                    .AddMember("ValueInt").Uint64(0)
                .EndStruct();
            rows.EndList();

            auto upsertResult = db.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT(!upsertResult.IsSuccess());
            TString issues = upsertResult.GetIssues().ToString();
            UNIT_ASSERT_C(issues.Contains("Type mismatch, got type Uint64 for column Key35, but expected Decimal(35,10)"), issues);
        }
        
        // Bad case: upsert Decimal to Uin64 column
        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key22").Decimal(TDecimalValue("0", 22, 9))
                    .AddMember("Key35").Decimal(TDecimalValue("0", 35, 10))
                    .AddMember("Value22").Decimal(TDecimalValue("0", 22, 9))
                    .AddMember("Value35").Decimal(TDecimalValue("0", 35, 10))
                    .AddMember("ValueInt").Decimal(TDecimalValue("0", 35, 10))
                .EndStruct();
            rows.EndList();

            auto upsertResult = db.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT(!upsertResult.IsSuccess());
            TString issues = upsertResult.GetIssues().ToString();
            UNIT_ASSERT_C(issues.Contains("Type mismatch, got type Decimal(35,10) for column ValueInt, but expected Uint64"), issues);
        }

        // Bad case: upsert Decimal(35,10) to Decimal(22,9) column
        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key22").Decimal(TDecimalValue("0", 35, 10))
                    .AddMember("Key35").Decimal(TDecimalValue("0", 35, 10))
                    .AddMember("Value22").Decimal(TDecimalValue("0", 22, 9))
                    .AddMember("Value35").Decimal(TDecimalValue("0", 35, 10))
                    .AddMember("ValueInt").Uint64(0)
                .EndStruct();
            rows.EndList();

            auto upsertResult = db.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT(!upsertResult.IsSuccess());
            TString issues = upsertResult.GetIssues().ToString();
            UNIT_ASSERT_C(issues.Contains("Type mismatch, got type Decimal(35,10) for column Key22, but expected Decimal(22,9)"), issues);
        }

        // Good case
        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (ui32 i = 0; i < 10; ++i) {
                rows.AddListItem()
                    .BeginStruct()
                        .AddMember("Key22").Decimal(TDecimalValue(Sprintf("%u.123456789", i), 22, 9))
                        .AddMember("Key35").Decimal(TDecimalValue(Sprintf("%u.123456789", i * 1000), 35, 10))
                        .AddMember("Value22").Decimal(TDecimalValue(Sprintf("%u.123456789", i * 10), 22, 9))
                        .AddMember("Value35").Decimal(TDecimalValue(Sprintf("%u.123456789", i * 1000000), 35, 10))
                        .AddMember("ValueInt").Uint64(i)
                    .EndStruct();
            }
            rows.EndList();

            auto upsertResult = db.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }

        // Good case: upsert overflowed Decimal
        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key22").Decimal(TDecimalValue("12345678901234567890.1234567891", 22, 9))
                    .AddMember("Key35").Decimal(TDecimalValue("1234567890123456789012345678901234567890.1234567891", 35, 10))
                    .AddMember("Value22").Decimal(TDecimalValue("inf", 22, 9))
                    .AddMember("Value35").Decimal(TDecimalValue("inf", 35, 10))
                    .AddMember("ValueInt").Uint64(999999999)
                .EndStruct();
            rows.EndList();

            auto upsertResult = db.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());
        }         

        // Bad case: lookup by Uint64 value in Decimal key
        {
            NYdb::TValueBuilder keys;
            keys.BeginList().AddListItem()
                .BeginStruct()
                    .AddMember("Key22").Uint64(1)
                    .AddMember("Key35").Decimal(TDecimalValue("1000.123456789", 35, 10))
                .EndStruct();
            keys.EndList();
            auto selectResult = db.ReadRows("/Root/TestTable", keys.Build()).GetValueSync();
            UNIT_ASSERT(!selectResult.IsSuccess());
            TString issues = selectResult.GetIssues().ToString();
            UNIT_ASSERT_C(issues.Contains("Type mismatch, got type Uint64 for column Key22, but expected Decimal(22,9)"), issues);
        }

        // Bad case: lookup by Decimal(35,10) value in Decimal(22,9) key
        {
            NYdb::TValueBuilder keys;
            keys.BeginList().AddListItem()
                .BeginStruct()
                    .AddMember("Key22").Decimal(TDecimalValue("1.123456789", 35, 10))
                    .AddMember("Key35").Decimal(TDecimalValue("1000.123456789", 35, 10))
                .EndStruct();
            keys.EndList();
            auto selectResult = db.ReadRows("/Root/TestTable", keys.Build()).GetValueSync();
            UNIT_ASSERT(!selectResult.IsSuccess());
            TString issues = selectResult.GetIssues().ToString();
            UNIT_ASSERT_C(issues.Contains("Type mismatch, got type Decimal(35,10) for column Key22, but expected Decimal(22,9)"), issues);
        }

        // Good case: lookup decimal
        {
            NYdb::TValueBuilder keys;
            keys.BeginList();
            for (size_t i = 0; i < 3; ++i) {
                keys.AddListItem()
                    .BeginStruct()
                        .AddMember("Key22").Decimal(TDecimalValue(Sprintf("%u.123456789", i), 22, 9))
                        .AddMember("Key35").Decimal(TDecimalValue(Sprintf("%u.123456789", i * 1000), 35, 10))
                    .EndStruct();
            }
            keys.EndList();
            auto selectResult = db.ReadRows("/Root/TestTable", keys.Build()).GetValueSync();
            UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());
            auto res = FormatResultSetYson(selectResult.GetResultSet());
            CompareYson(R"(
                [
                    [["0.123456789"];["0.123456789"];["0.123456789"];["0.123456789"];[0u]];
                    [["1.123456789"];["1000.123456789"];["10.123456789"];["1000000.123456789"];[1u]];
                    [["2.123456789"];["2000.123456789"];["20.123456789"];["2000000.123456789"];[2u]]        
                ]
            )", res);
        }

        // Good case: lookup overflowed decimal
        {
            NYdb::TValueBuilder keys;
            keys.BeginList();
            keys.AddListItem()
                .BeginStruct()
                    .AddMember("Key22").Decimal(TDecimalValue("inf", 22, 9))
                    .AddMember("Key35").Decimal(TDecimalValue("inf", 35, 10))
                .EndStruct();
            keys.EndList();
            auto selectResult = db.ReadRows("/Root/TestTable", keys.Build()).GetValueSync();
            UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());
            auto res = FormatResultSetYson(selectResult.GetResultSet());
            CompareYson(R"([[["inf"];["inf"];["inf"];["inf"];[999999999u]];])", TString{res});
        }        
    }

    Y_UNIT_TEST(ReadRows_Nulls) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        auto kikimr = TKikimrRunner{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(R"(
            CREATE TABLE TestTable (
                Key Uint64,
                Data Uint32,
                Value Utf8,
                PRIMARY KEY (Key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (size_t i = 0; i < 5; ++i) {
            rows.AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(i * 1921763474923857134ull + 1858343823)
                .EndStruct();
        }
        rows.EndList();

        auto upsertResult = db.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        NYdb::TValueBuilder keys;
        keys.BeginList();
        for (size_t i = 0; i < 5; ++i) {
            keys.AddListItem()
                .BeginStruct()
                    .AddMember("Key").Uint64(i * 1921763474923857134ull + 1858343823)
                .EndStruct();
        }
        keys.EndList();
        auto selectResult = db.ReadRows("/Root/TestTable", keys.Build()).GetValueSync();
        Cerr << "IsSuccess(): " << selectResult.IsSuccess() << " GetStatus(): " << selectResult.GetStatus() << Endl;
        UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());
        auto res = FormatResultSetYson(selectResult.GetResultSet());
        CompareYson(R"(
            [
                [[1858343823u];#;#];
                [[1921763476782200957u];#;#];
                [[3843526951706058091u];#;#];
                [[5765290426629915225u];#;#];
                [[7687053901553772359u];#;#]
            ]
        )", TString{res});
    }


}

} // namespace NKikimr::NKqp
