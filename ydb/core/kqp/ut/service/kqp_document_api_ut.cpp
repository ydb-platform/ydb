#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

static void CreateSampleTables(TSession session) {
    auto tableDesc = TTableBuilder()
        .AddNullableColumn("Key1", EPrimitiveType::String)
        .AddNullableColumn("Key2", EPrimitiveType::String)
        .AddNullableColumn("Value", EPrimitiveType::Json)
        .SetPrimaryKeyColumns({"Key1", "Key2"})
        .AddAttribute("__document_api_version", "1")
        .Build();

    auto schemeResult = session.CreateTable("/Root/DocumentApiTest", std::move(tableDesc)).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(schemeResult.GetStatus(), EStatus::SUCCESS, schemeResult.GetIssues().ToString());
}

Y_UNIT_TEST_SUITE(KqpDocumentApi) {
    Y_UNIT_TEST(RestrictWrite) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto query = R"(
            UPSERT INTO `/Root/DocumentApiTest` (Key1, Key2, Value) VALUES
                ("Key1_1", "Key2_1", CAST("{Value: 10}" AS Json));
        )";

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION));

        auto settings = TExecDataQuerySettings().RequestType("_document_api_request");
        result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(RestrictWriteExplicitPrepare) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto text = R"(
            DECLARE $rows AS List<Struct<Key1:String?, Key2:String?>>;
            UPSERT INTO `/Root/DocumentApiTest` (Key1, Key2)
                SELECT Key1, Key2 FROM AS_TABLE($rows);
        )";

        {
            auto prepareResult = session.PrepareDataQuery(text).ExtractValueSync();

            prepareResult.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT(!prepareResult.IsSuccess());
            UNIT_ASSERT(HasIssue(prepareResult.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION));
        }

        auto prepareSettings = TPrepareDataQuerySettings().RequestType("_document_api_request");
        auto prepareResult = session.PrepareDataQuery(text, prepareSettings).ExtractValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());

        auto params = prepareResult.GetQuery().GetParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key1").OptionalString("k1")
                        .AddMember("Key2").OptionalString("k2")
                    .EndStruct()
                .EndList()
                .Build()
            .Build();

        auto execSettings = TExecDataQuerySettings().RequestType("_document_api_request");
        auto result = prepareResult.GetQuery().Execute(
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
            std::move(params), execSettings).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(AllowRead) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto query = R"(
            SELECT * FROM `/Root/DocumentApiTest`;
        )";

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(RestrictAlter) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto query = R"(
            ALTER TABLE `/Root/DocumentApiTest` DROP COLUMN Value;
        )";

        const auto tests = TVector<std::pair<TExecSchemeQuerySettings, bool>>{
            {TExecSchemeQuerySettings(), false},
            {TExecSchemeQuerySettings().RequestType("_document_api_request"), true},
        };

        for (const auto& [settings, success] : tests) {
            auto result = session.ExecuteSchemeQuery(query, settings).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.IsSuccess(), success);
        }
    }

    Y_UNIT_TEST(RestrictDrop) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        auto query = R"(
            DROP TABLE `/Root/DocumentApiTest`;
        )";

        auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION));
    }

    Y_UNIT_TEST(Scripting) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTables(session);

        NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());

        auto script = R"(
            SELECT * FROM `/Root/DocumentApiTest`;
            COMMIT;
            ALTER TABLE `/Root/DocumentApiTest` DROP COLUMN Value;
        )";

        auto result = client.ExecuteYqlScript(script).GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION));
    }
}

} // namspace NKqp
} // namespace NKikimr
