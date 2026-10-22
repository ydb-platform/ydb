#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;

Y_UNIT_TEST_SUITE(KqpRboCompatibility) {
    Y_UNIT_TEST(SelectedUniqueIndexRetainsDuplicateNullBusinessKeys) {
        NKikimrConfig::TAppConfig appConfig;
        auto* service = appConfig.MutableTableServiceConfig();
        service->SetEnableNewRBO(true);
        service->SetEnableFallbackToYqlOptimizer(false);
        service->SetEnableFallbackOnDML(false);
        service->SetEnableFallbackOnMultipleStatements(false);
        service->SetDefaultLangVer(202501);
        service->SetDefaultCostBasedOptimizationLevel(0);
        service->SetEnableAutoIndexSelectionForIndexLookupJoin(false);
        service->SetEnableIndexStreamWrite(true);
        auto settings = TKikimrSettings(appConfig);
        settings.SetWithSampleTables(false);
        settings.FeatureFlags.SetEnableAddUniqueIndex(true);
        settings.FeatureFlags.SetEnableAccessToIndexImplTables(true);
        TKikimrRunner kikimr(settings);
        auto tableSession = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        const auto created = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/SelectedUniqueRows` (
                Id Int32 NOT NULL, Flag Bool NOT NULL, U String,
                CoverValue String, Payload Int32, PRIMARY KEY (Id),
                INDEX by_fu GLOBAL UNIQUE SYNC ON (Flag, U) COVER (CoverValue)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());

        struct TRow {
            i32 Id;
            bool Flag;
            std::optional<std::string> U;
            std::optional<std::string> Cover;
            std::optional<i32> Payload;
        };
        std::vector<TRow> expected{
            {1, true, std::nullopt, "c1", 11},
            {2, true, std::nullopt, "c2", 22},
            {3, true, "a", "c3", 33},
            {4, false, "a", "c4", 44},
        };
        // INSERT SELECT supplies the subquery required by new RBO's DML path.
        // These writes use the real uniqueness checker; BulkUpsert is not used.
        const auto insert = [&](const std::vector<TRow>& input) {
            TValueBuilder rows;
            rows.BeginList();
            for (const auto& row : input) {
                rows.AddListItem().BeginStruct()
                    .AddMember("Id").Int32(row.Id)
                    .AddMember("Flag").Bool(row.Flag)
                    .AddMember("U").OptionalString(row.U)
                    .AddMember("CoverValue").OptionalString(row.Cover)
                    .AddMember("Payload").OptionalInt32(row.Payload)
                    .EndStruct();
            }
            const auto params = TParamsBuilder().AddParam("$rows", rows.EndList().Build()).Build();
            return querySession.ExecuteQuery(R"(
                PRAGMA YqlSelect = 'force';
                DECLARE $rows AS List<Struct<Id:Int32,Flag:Bool,U:String?,CoverValue:String?,Payload:Int32?>>;
                INSERT INTO `/Root/SelectedUniqueRows` (Id, Flag, U, CoverValue, Payload)
                SELECT Id, Flag, U, CoverValue, Payload FROM AS_TABLE($rows);
            )", NQuery::TTxControl::BeginTx().CommitTx(), params).GetValueSync();
        };
        const auto seeded = insert(expected);
        UNIT_ASSERT_C(seeded.IsSuccess(), seeded.GetIssues().ToString());

        const TString indexPath = "/Root/SelectedUniqueRows/by_fu/indexImplTable";
        const auto described = tableSession.DescribeTable(indexPath).GetValueSync();
        UNIT_ASSERT_C(described.IsSuccess(), described.GetIssues().ToString());
        const auto& description = described.GetTableDescription();
        const std::vector<std::string> physicalKey{"Flag", "U", "Id"};
        UNIT_ASSERT_VALUES_EQUAL(description.GetPrimaryKeyColumns(), physicalKey);
        const auto storedColumns = description.GetTableColumns();
        UNIT_ASSERT_VALUES_EQUAL(storedColumns.size(), 4);
        for (const auto& column : storedColumns) {
            if (column.Name == "Id") {
                UNIT_ASSERT_VALUES_EQUAL(column.Type.ToString(), "Int32");
            } else if (column.Name == "Flag") {
                UNIT_ASSERT_VALUES_EQUAL(column.Type.ToString(), "Bool");
            } else {
                UNIT_ASSERT_C(column.Name == "U" || column.Name == "CoverValue", column.Name);
                UNIT_ASSERT_VALUES_EQUAL(column.Type.ToString(), "String?");
            }
        }
        const auto expectedRow = [&](i32 id) -> const TRow& {
            const auto found = std::find_if(expected.begin(), expected.end(),
                [&](const TRow& row) { return row.Id == id; });
            UNIT_ASSERT_C(found != expected.end(), "Unexpected row Id=" << id);
            return *found;
        };
        const auto checkStoredRows = [&]() {
            // Read both physical tables directly and consume every part. This
            // checks NULL retention and rollback of a rejected unique insert.
            // SQL planning cannot replace the index assertion with a base scan.
            for (const bool indexRead : {false, true}) {
                const TString readPath = indexRead ? indexPath : TString{"/Root/SelectedUniqueRows"};
                auto stream = tableSession.ReadTable(readPath, NYdb::NTable::TReadTableSettings().Ordered(true)).GetValueSync();
                UNIT_ASSERT_C(stream.IsSuccess(), stream.GetIssues().ToString());
                std::vector<i32> actualIds;
                for (;;) {
                    auto part = stream.ReadNext().GetValueSync();
                    if (!part.IsSuccess()) {
                        UNIT_ASSERT_C(part.EOS(), part.GetIssues().ToString());
                        break;
                    }
                    const auto resultSet = part.ExtractPart();
                    // ReadTable marks each streaming fragment as truncated. Completeness
                    // is checked by consuming EOS and comparing the entire row bag below.
                    TResultSetParser parser(resultSet);
                    while (parser.TryNextRow()) {
                        // ReadTable may wrap required storage columns in Optional.
                        auto& idCell = parser.ColumnParser("Id");
                        const auto id = idCell.GetKind() == TTypeParser::ETypeKind::Optional
                            ? idCell.GetOptionalInt32() : std::optional<i32>{idCell.GetInt32()};
                        UNIT_ASSERT(id);
                        const auto& row = expectedRow(*id);
                        auto& flagCell = parser.ColumnParser("Flag");
                        const auto flag = flagCell.GetKind() == TTypeParser::ETypeKind::Optional
                            ? flagCell.GetOptionalBool() : std::optional<bool>{flagCell.GetBool()};
                        UNIT_ASSERT(flag);
                        UNIT_ASSERT_VALUES_EQUAL(*flag, row.Flag);
                        UNIT_ASSERT(parser.ColumnParser("U").GetOptionalString() == row.U);
                        UNIT_ASSERT(parser.ColumnParser("CoverValue").GetOptionalString() == row.Cover);
                        if (!indexRead) {
                            UNIT_ASSERT(parser.ColumnParser("Payload").GetOptionalInt32() == row.Payload);
                        }
                        actualIds.push_back(*id);
                    }
                }
                std::vector<i32> expectedIds;
                for (const auto& row : expected) { expectedIds.push_back(row.Id); }
                std::sort(actualIds.begin(), actualIds.end());
                std::sort(expectedIds.begin(), expectedIds.end());
                UNIT_ASSERT_VALUES_EQUAL(actualIds, expectedIds);
            }
        };

        const auto counters = TKqpCounters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters).GetKqpCounters();
        const auto successes = counters->GetCounter("Compilation/NewRBO/Success");
        const auto failures = counters->GetCounter("Compilation/NewRBO/Failed");
        const auto checkReads = [&](bool firstCompilation) {
            // Automatic selection must choose the covered Bool-prefix scan and
            // then the noncovering prefix scan plus primary-key lookup.
            for (const bool fetch : {false, true}) {
                const TString query = TStringBuilder()
                    << "PRAGMA YqlSelect = 'force'; SELECT Id, U, "
                    << (fetch ? "Payload" : "CoverValue") << " AS Value "
                    << "FROM `/Root/SelectedUniqueRows` WHERE Flag = true;";
                const auto successesBefore = successes->Val();
                const auto failuresBefore = failures->Val();
                const auto explain = querySession.ExecuteQuery(query, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
                UNIT_ASSERT_C(explain.IsSuccess(), query << "\n" << explain.GetIssues().ToString());
                if (firstCompilation) { UNIT_ASSERT_C(successes->Val() > successesBefore, query); }
                UNIT_ASSERT_VALUES_EQUAL_C(failures->Val(), failuresBefore, query);
                UNIT_ASSERT_C(explain.GetStats() && explain.GetStats()->GetPlan(), query);
                const TString plan{*explain.GetStats()->GetPlan()};
                NJson::TJsonValue json;
                UNIT_ASSERT_C(NJson::ReadJsonTree(plan, &json, true), plan);
                const auto& simplified = json["SimplifiedPlan"];
                UNIT_ASSERT_C(simplified.IsMap(), plan);
                const auto index = FindPlanNodeByKv(simplified, "Table", "SelectedUniqueRows/by_fu/indexImplTable");
                UNIT_ASSERT_C(index.IsDefined(), plan);
                UNIT_ASSERT_VALUES_EQUAL_C(index["Index"].GetStringSafe(), "by_fu", plan);
                UNIT_ASSERT_VALUES_EQUAL_C(index["Name"].GetStringSafe(), "TableRangeScan", plan);
                const auto& rangeKeys = index["ReadRangesKeys"].GetArraySafe();
                UNIT_ASSERT_VALUES_EQUAL_C(rangeKeys.size(), 1, plan);
                UNIT_ASSERT_VALUES_EQUAL_C(rangeKeys[0].GetStringSafe(), "Flag", plan);
                const auto lookup = FindPlanNodeByKv(simplified, "Name", "TableLookup");
                UNIT_ASSERT_VALUES_EQUAL_C(lookup.IsDefined(), fetch, plan);
                if (fetch) {
                    UNIT_ASSERT_VALUES_EQUAL_C(lookup["Table"].GetStringSafe(), "/Root/SelectedUniqueRows", plan);
                }

                const auto result = querySession.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString() << "\n" << plan);
                UNIT_ASSERT_VALUES_EQUAL_C(failures->Val(), failuresBefore, query);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
                const auto& resultSet = result.GetResultSet(0);
                UNIT_ASSERT(!resultSet.Truncated());
                const auto& columns = resultSet.GetColumnsMeta();
                UNIT_ASSERT_VALUES_EQUAL(columns.size(), 3);
                UNIT_ASSERT_VALUES_EQUAL(columns[0].Name, "Id");
                UNIT_ASSERT_VALUES_EQUAL(columns[0].Type.ToString(), "Int32");
                UNIT_ASSERT_VALUES_EQUAL(columns[1].Name, "U");
                UNIT_ASSERT_VALUES_EQUAL(columns[1].Type.ToString(), "String?");
                UNIT_ASSERT_VALUES_EQUAL(columns[2].Name, "Value");
                UNIT_ASSERT_VALUES_EQUAL(columns[2].Type.ToString(), fetch ? "Int32?" : "String?");
                std::vector<i32> actualIds, expectedIds;
                TResultSetParser parser(resultSet);
                while (parser.TryNextRow()) {
                    const auto id = parser.ColumnParser(0).GetInt32();
                    const auto& row = expectedRow(id);
                    UNIT_ASSERT(parser.ColumnParser(1).GetOptionalString() == row.U);
                    if (fetch) {
                        UNIT_ASSERT(parser.ColumnParser(2).GetOptionalInt32() == row.Payload);
                    } else {
                        UNIT_ASSERT(parser.ColumnParser(2).GetOptionalString() == row.Cover);
                    }
                    actualIds.push_back(id);
                }
                for (const auto& row : expected) {
                    if (row.Flag) { expectedIds.push_back(row.Id); }
                }
                // SQL has no ORDER BY: normalize only for exact bag comparison.
                std::sort(actualIds.begin(), actualIds.end());
                std::sort(expectedIds.begin(), expectedIds.end());
                UNIT_ASSERT_VALUES_EQUAL_C(actualIds, expectedIds, query << "\n" << FormatResultSetYson(resultSet));
            }
        };
        checkStoredRows();
        checkReads(true);

        const auto rejected = insert({{5, true, "a", "conflict", 55}});
        UNIT_ASSERT_VALUES_EQUAL_C(rejected.GetStatus(), EStatus::PRECONDITION_FAILED, rejected.GetIssues().ToString());
        checkStoredRows();
        checkReads(false);

        const TRow anotherNull{6, true, std::nullopt, "c6", 66};
        const auto accepted = insert({anotherNull});
        UNIT_ASSERT_C(accepted.IsSuccess(), accepted.GetIssues().ToString());
        expected.push_back(anotherNull);
        checkStoredRows();
        checkReads(false);
    }

    Y_UNIT_TEST_TWIN(SafeCastOptionalResults, PhysicalStagePeephole) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableNewRBOPhysicalStagePeephole(PhysicalStagePeephole);
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        const auto params = TParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                    .AddListItem().BeginStruct()
                        .AddMember("value").Decimal(TDecimalValue("42", 7, 0))
                        .AddMember("number").Int32(42)
                        .AddMember("text").String("42")
                        .AddMember("nullable").OptionalInt32(42)
                    .EndStruct()
                    .AddListItem().BeginStruct()
                        .AddMember("value").Decimal(TDecimalValue("42", 7, 0))
                        .AddMember("number").Int32(-7)
                        .AddMember("text").String("invalid")
                        .AddMember("nullable").OptionalInt32(std::nullopt)
                    .EndStruct()
                .EndList()
            .Build()
            .Build();
        const auto result = kikimr.GetQueryClient().ExecuteQuery(R"(
            DECLARE $rows AS List<Struct<value:Decimal(7,0),number:Int32,text:String,nullable:Int32?>>;
            SELECT
                CAST(value AS Decimal(7,0)?) AS value,
                CAST(number AS Decimal(7,0)?) AS numeric_decimal,
                CAST(number AS String?) AS numeric_string,
                CAST(text AS Decimal(7,0)?) AS parsed_decimal,
                CAST(value AS Decimal(7,0)??) AS nested_decimal,
                CAST(nullable AS String??) AS nested_string
            FROM AS_TABLE($rows) ORDER BY number DESC;
        )", NQuery::TTxControl::NoTx(), params).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto& resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
        const TVector<TString> types = {"Decimal(7,0)?", "Decimal(7,0)?", "String?",
            "Decimal(7,0)?", "Decimal(7,0)??", "String??"};
        for (size_t column = 0; column < types.size(); ++column) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.GetColumnsMeta()[column].Type.ToString(), types[column]);
        }
        TResultSetParser parser(resultSet);
        for (const bool present : {true, false}) {
            UNIT_ASSERT(parser.TryNextRow());
            const auto value = parser.ColumnParser(0).GetOptionalDecimal();
            UNIT_ASSERT(value);
            UNIT_ASSERT_VALUES_EQUAL(value->ToString(), "42");
            const auto numeric = parser.ColumnParser(1).GetOptionalDecimal();
            UNIT_ASSERT(numeric);
            UNIT_ASSERT_VALUES_EQUAL(numeric->ToString(), present ? "42" : "-7");
            const auto text = parser.ColumnParser(2).GetOptionalString();
            UNIT_ASSERT(text);
            UNIT_ASSERT_VALUES_EQUAL(*text, present ? "42" : "-7");
            const auto parsed = parser.ColumnParser(3).GetOptionalDecimal();
            UNIT_ASSERT_VALUES_EQUAL(parsed.has_value(), present);
            if (parsed) {
                UNIT_ASSERT_VALUES_EQUAL(parsed->ToString(), "42");
            }
            auto& nestedDecimal = parser.ColumnParser(4);
            nestedDecimal.OpenOptional();
            UNIT_ASSERT(!nestedDecimal.IsNull());
            const auto decimal = nestedDecimal.GetOptionalDecimal();
            UNIT_ASSERT(decimal);
            UNIT_ASSERT_VALUES_EQUAL(decimal->ToString(), "42");
            auto& nestedString = parser.ColumnParser(5);
            nestedString.OpenOptional();
            UNIT_ASSERT(!nestedString.IsNull());
            const auto string = nestedString.GetOptionalString();
            UNIT_ASSERT_VALUES_EQUAL(string.has_value(), present);
            if (string) {
                UNIT_ASSERT_VALUES_EQUAL(*string, "42");
            }
        }
    }

    Y_UNIT_TEST(ManyJoinsWithDecimalRanges) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetEnableNewRBOPhysicalStagePeephole(false);
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        // Filtering each joined table by a non-nullable decimal key produces
        // optional casts in the read ranges.
        constexpr ui32 JoinCount = 90;
        TStringBuilder schema;
        for (ui32 i = 0; i <= JoinCount; ++i) {
            schema << "CREATE TABLE `/Root/t" << i << "` ("
                << "scope Decimal(7,0) NOT NULL, key Uint64 NOT NULL, value String,"
                << "PRIMARY KEY (scope, key));\n";
        }
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto created = session.ExecuteSchemeQuery(schema).GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());

        TStringBuilder query;
        query << "SELECT t0.key";
        for (ui32 i = 1; i <= JoinCount; ++i) {
            query << ", t" << i << ".value AS value" << i;
        }
        query << " FROM `/Root/t0` AS t0\n";
        for (ui32 i = 1; i <= JoinCount; ++i) {
            query << "LEFT JOIN (SELECT key, value FROM `/Root/t" << i
                << "` WHERE scope = CAST('0' AS Decimal(7,0))) AS t" << i
                << " ON t0.key = t" << i << ".key\n";
        }
        const auto counters = TKqpCounters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters).GetKqpCounters();
        const auto successes = counters->GetCounter("Compilation/NewRBO/Success");
        const auto failures = counters->GetCounter("Compilation/NewRBO/Failed");
        const auto successesBefore = successes->Val();
        const auto failuresBefore = failures->Val();
        const auto result = kikimr.GetQueryClient().ExecuteQuery(query, NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats() && result.GetStats()->GetPlan());
        UNIT_ASSERT(!result.GetStats()->GetPlan()->empty());
        UNIT_ASSERT_VALUES_EQUAL(successes->Val(), successesBefore + 1);
        UNIT_ASSERT_VALUES_EQUAL(failures->Val(), failuresBefore);
    }
}

} // namespace
} // namespace NKikimr::NKqp
