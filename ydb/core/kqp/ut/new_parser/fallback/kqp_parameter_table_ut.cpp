#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;

struct TParameterTableFixture {
    TCapturedLog Logs{true};
    TKikimrRunner Kikimr;
    NQuery::TSession Session;
    bool Native;
    ui32 QueryIndex = 0;

    explicit TParameterTableFixture(bool native)
        : Kikimr(Settings(native))
        , Session(Kikimr.GetQueryClient().GetSession().GetValueSync().GetSession())
        , Native(native)
    {}

    TKikimrSettings Settings(bool native) {
        NKikimrConfig::TAppConfig app;
        auto* config = app.MutableTableServiceConfig();
        config->SetEnableNewYqlParser(native);
        config->SetEnableNewRBO(true);
        config->SetEnableFallbackToYqlOptimizer(false);
        config->SetEnableIndexStreamWrite(true);
        config->SetEnableNewRBOPhysicalStagePeephole(false);
        config->SetCompileQueryCacheSize(0);
        auto settings = TKikimrSettings(app).SetWithSampleTables(false).SetLogCapture(Logs);
        settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_INFO);
        settings.LogSettings->DefaultLogPriority = NActors::NLog::PRI_CRIT;
        return settings;
    }

    void CreateTable() {
        auto session = Kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        const auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/T` (
                pk Int64, s Int64, v Int64,
                PRIMARY KEY (pk),
                INDEX idx GLOBAL SYNC ON (s)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    NQuery::TExecuteQueryResult Execute(const TString& sql, const TParams& params = TParamsBuilder().Build()) {
        // The LRU retains one entry even with CompileQueryCacheSize=0. Use a
        // distinct query text so every request must exercise schema resolution.
        const TString query = TStringBuilder() << sql << "\n-- AS_TABLE request " << ++QueryIndex;
        const auto start = Logs.Snapshot().size();
        auto result = Session.ExecuteQuery(query, NQuery::TTxControl::BeginTx().CommitTx(), params,
            NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Basic)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), sql << ": " << result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats());
        UNIT_ASSERT_C(!TProtoAccessor::GetProto(*result.GetStats()).compilation().from_cache(), query);
        if (Native) {
            const auto deadline = TInstant::Now() + TDuration::Seconds(10);
            TString text;
            do {
                text = Logs.Snapshot().substr(start);
                if (text.Contains("New YQL native execution contract built")) { break; }
                Sleep(TDuration::MilliSeconds(10));
            } while (TInstant::Now() < deadline);
            // Optimizer success alone cannot distinguish native lowering from
            // a frontend fallback that subsequently uses the same optimizer.
            UNIT_ASSERT_C(text.Contains("New YQL native execution contract built"), query << "\n" << text);
            UNIT_ASSERT_C(!text.Contains("New YQL parser fallback:") && !text.Contains("New YQL frontend fallback:"),
                query << "\n" << text);
        }
        return result;
    }
};

TParams KeyParameter(i64 key) {
    return TParamsBuilder().AddParam("$rows").BeginList()
        .AddListItem().BeginStruct().AddMember("pk").Int64(key).EndStruct()
        .EndList().Build().Build();
}

void CheckResult(const NQuery::TExecuteQueryResult& result, const TVector<TString>& columns, TStringBuf expected) {
    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
    const auto& rows = result.GetResultSet(0);
    UNIT_ASSERT_VALUES_EQUAL(rows.GetColumnsMeta().size(), columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(rows.GetColumnsMeta()[i].Name, columns[i]);
    }
    UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(rows), expected);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpNewYqlParameterTable) {
    Y_UNIT_TEST_TWIN(SdkSchemaSelect, Native) {
        TParameterTableFixture f(Native);
        const auto rows = TParamsBuilder().AddParam("$rows").BeginList()
            .AddListItem().BeginStruct().AddMember("id").Int64(2).AddMember("value").OptionalString("two").EndStruct()
            .AddListItem().BeginStruct().AddMember("id").Int64(1).AddMember("value").EmptyOptional(EPrimitiveType::String).EndStruct()
            .EndList().Build().Build();
        const TString query = "SELECT r.value, r.id FROM AS_TABLE($rows) AS r WHERE r.id > 0 ORDER BY r.id;";
        CheckResult(f.Execute(query, rows), {"value", "id"}, "[[#;1];[[\"two\"];2]]");

        const auto empty = TParamsBuilder().AddParam("$rows")
            .EmptyList(TTypeBuilder().BeginStruct()
                .AddMember("id").Primitive(EPrimitiveType::Int64)
                .AddMember("value").BeginOptional().Primitive(EPrimitiveType::String).EndOptional()
                .EndStruct().Build()).Build().Build();
        CheckResult(f.Execute(query, empty), {"value", "id"}, "[]");
    }

    // Based on KqpRboIndexReturning::ManyEmptyReturningClauses. Each request
    // contains one DELETE, so multi-statement fallback cannot hide AS_TABLE.
    Y_UNIT_TEST_TWIN(SdkSchemaDeleteReturning, Native) {
        TParameterTableFixture f(Native);
        f.CreateTable();
        f.Execute("UPSERT INTO `/Root/T` (pk, s, v) VALUES (1, 10, 100);");

        const TString all = "DELETE FROM `/Root/T` ON SELECT * FROM AS_TABLE($rows) RETURNING v, pk, s;";
        CheckResult(f.Execute(all, KeyParameter(0)), {"v", "pk", "s"}, "[]");
        const auto empty = TParamsBuilder().AddParam("$rows")
            .EmptyList(TTypeBuilder().BeginStruct().AddMember("pk").Primitive(EPrimitiveType::Int64)
                .EndStruct().Build()).Build().Build();
        CheckResult(f.Execute(all, empty), {"v", "pk", "s"}, "[]");
        CheckResult(f.Execute(all, KeyParameter(1)), {"v", "pk", "s"}, "[[[100];[1];[10]]]");
        CheckResult(f.Execute("DELETE FROM `/Root/T` ON SELECT * FROM AS_TABLE($rows) RETURNING pk;",
            KeyParameter(1)), {"pk"}, "[]");
        CheckResult(f.Execute("SELECT pk, s, v FROM `/Root/T`;"), {"pk", "s", "v"}, "[]");
        CheckResult(f.Execute("SELECT pk, s FROM `/Root/T/idx/indexImplTable`;"), {"pk", "s"}, "[]");
    }

    // Based on UpdateIndexKeyChangeWithReturningVerifyIndex; supply the update
    // source schema exclusively through the SDK, with one statement per request.
    Y_UNIT_TEST_TWIN(SdkSchemaUpdateIndexReturning, Native) {
        TParameterTableFixture f(Native);
        f.CreateTable();
        f.Execute("UPSERT INTO `/Root/T` (pk, s, v) VALUES (1, 10, 100);");
        const auto rows = TParamsBuilder().AddParam("$rows").BeginList()
            .AddListItem().BeginStruct().AddMember("pk").Int64(1).AddMember("s").Int64(20).EndStruct()
            .EndList().Build().Build();
        CheckResult(f.Execute("UPDATE `/Root/T` ON SELECT * FROM AS_TABLE($rows) RETURNING pk, s, v;", rows),
            {"pk", "s", "v"}, "[[[1];[20];[100]]]");
        CheckResult(f.Execute("SELECT pk, s, v FROM `/Root/T`;"), {"pk", "s", "v"}, "[[[1];[20];[100]]]");
        CheckResult(f.Execute("SELECT pk, s FROM `/Root/T/idx/indexImplTable`;"), {"pk", "s"}, "[[[1];[20]]]");
    }
}

} // namespace NKikimr::NKqp
