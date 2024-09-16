#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/yql/sql/sql.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <util/folder/filelist.h>

#include <format>

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void EnableViewsFeatureFlag(TKikimrRunner& kikimr) {
    kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableViews(true);
}

void DisableViewsFeatureFlag(TKikimrRunner& kikimr) {
    kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableViews(false);
}

NKikimrSchemeOp::TViewDescription GetViewDescription(TTestActorRuntime& runtime, const TString& path) {
    const auto pathQueryResult = Navigate(runtime,
                                          runtime.AllocateEdgeActor(),
                                          path,
                                          NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
    const auto& pathEntry = pathQueryResult->ResultSet.at(0);
    UNIT_ASSERT_EQUAL(pathEntry.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindView);
    UNIT_ASSERT(pathEntry.ViewInfo);
    return pathEntry.ViewInfo->Description;
}

void ExpectUnknownEntry(TTestActorRuntime& runtime, const TString& path) {
    const auto pathQueryResult = Navigate(runtime,
                                          runtime.AllocateEdgeActor(),
                                          path,
                                          NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
    UNIT_ASSERT_EQUAL(pathQueryResult->ErrorCount, 1);
    const auto& pathEntry = pathQueryResult->ResultSet.at(0);
    UNIT_ASSERT_EQUAL(pathEntry.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);
}

void EnableLogging() {
    using namespace NYql::NLog;
    YqlLogger().ResetBackend(CreateLogBackend("cerr"));
    for (const auto component : {EComponent::Default, EComponent::Sql, EComponent::ProviderKqp}) {
        YqlLogger().SetComponentLevel(component, ELevel::INFO);
    }
}

TString ReadWholeFile(const TString& path) {
    TFileInput file(path);
    return file.ReadAll();
}

void ExecuteDataDefinitionQuery(TSession& session, const TString& script) {
    const auto result = session.ExecuteSchemeQuery(script).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "Failed to execute the following DDL script:\n"
                                          << script << "\nThe issues:\n" << result.GetIssues().ToString());
}

TDataQueryResult ExecuteDataModificationQuery(TSession& session,
                                              const TString& script,
                                              const TExecDataQuerySettings& settings = {}
) {
    const auto result = session.ExecuteDataQuery(
            script,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
            settings
        ).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "Failed to execute the following DML script:\n"
                                          << script << "\nThe issues:\n" << result.GetIssues().ToString());

    return result;
}

TValue GetSingleResult(const TDataQueryResult& rawResults) {
    auto resultSetParser = rawResults.GetResultSetParser(0);
    UNIT_ASSERT(resultSetParser.TryNextRow());
    return resultSetParser.GetValue(0);
}

TValue GetSingleResult(TSession& session, const TString& query, const TExecDataQuerySettings& settings = {}) {
    return GetSingleResult(ExecuteDataModificationQuery(session, query, settings));
}

TInstant GetTimestamp(const TValue& value) {
    return TValueParser(value).GetTimestamp();
}

int GetInteger(const TValue& value) {
    return TValueParser(value).GetInt32();
}

TMaybe<bool> GetFromCacheStat(const TQueryStats& stats) {
    const auto& proto = TProtoAccessor::GetProto(stats);
    if (!proto.Hascompilation()) {
        return Nothing();
    }
    return proto.Getcompilation().Getfrom_cache();
}

void AssertFromCache(const TMaybe<TQueryStats>& stats, bool expectedValue) {
    UNIT_ASSERT(stats.Defined());
    const auto isFromCache = GetFromCacheStat(*stats);
    UNIT_ASSERT_C(isFromCache.Defined(), stats->ToString());
    UNIT_ASSERT_VALUES_EQUAL_C(*isFromCache, expectedValue, stats->ToString());
}

void CompareResults(const TDataQueryResult& first, const TDataQueryResult& second) {
    const auto& firstResults = first.GetResultSets();
    const auto& secondResults = second.GetResultSets();

    UNIT_ASSERT_VALUES_EQUAL(firstResults.size(), secondResults.size());
    for (size_t i = 0; i < firstResults.size(); ++i) {
        CompareYson(FormatResultSetYson(firstResults[i]), FormatResultSetYson(secondResults[i]));
    }
}

void InitializeTablesAndSecondaryViews(TSession& session) {
    const auto inputFolder = ArcadiaFromCurrentLocation(__SOURCE_FILE__, "input");
    ExecuteDataDefinitionQuery(session, ReadWholeFile(inputFolder + "/create_tables_and_secondary_views.sql"));
    ExecuteDataModificationQuery(session, ReadWholeFile(inputFolder + "/fill_tables.sql"));
}

}

Y_UNIT_TEST_SUITE(TCreateAndDropViewTest) {

    Y_UNIT_TEST(CheckCreatedView) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        EnableViewsFeatureFlag(kikimr);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        constexpr const char* path = "/Root/TheView";
        constexpr const char* queryInView = "SELECT 1";

        const TString creationQuery = std::format(R"(
                CREATE VIEW `{}` WITH (security_invoker = true) AS {};
            )",
            path,
            queryInView
        );
        ExecuteDataDefinitionQuery(session, creationQuery);

        const auto viewDescription = GetViewDescription(runtime, path);
        UNIT_ASSERT_EQUAL(viewDescription.GetQueryText(), queryInView);
    }

    Y_UNIT_TEST(CreateViewDisabledFeatureFlag) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        constexpr const char* path = "/Root/TheView";

        const TString creationQuery = std::format(R"(
                CREATE VIEW `{}` WITH (security_invoker = true) AS SELECT 1;
            )",
            path
        );

        DisableViewsFeatureFlag(kikimr);
        const auto creationResult = session.ExecuteSchemeQuery(creationQuery).ExtractValueSync();
        UNIT_ASSERT(!creationResult.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(creationResult.GetIssues().ToString(), "Error: Views are disabled");
    }

    Y_UNIT_TEST(InvalidQuery) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        EnableViewsFeatureFlag(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        constexpr const char* path = "/Root/TheView";
        constexpr const char* queryInView = R"(
            SELECT "foo" / "bar"
        )";

        const auto parsedAst = NSQLTranslation::SqlToYql(queryInView, {});
        UNIT_ASSERT_C(parsedAst.IsOk(), parsedAst.Issues.ToString());

        const TString creationQuery = std::format(R"(
                CREATE VIEW `{}` WITH (security_invoker = true) AS {};
            )",
            path,
            queryInView
        );

        const auto creationResult = session.ExecuteSchemeQuery(creationQuery).ExtractValueSync();
        UNIT_ASSERT(!creationResult.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(creationResult.GetIssues().ToString(), "Error: Cannot divide type String and String");
    }

    Y_UNIT_TEST(ListCreatedView) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        EnableViewsFeatureFlag(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        // .sys directory is always present in the `/Root`, that's why we need a subfolder
        constexpr const char* folder = "/Root/ThisSubfolderIsNecessary";
        constexpr const char* viewName = "TheView";
        const auto path = TStringBuilder() << folder << '/' << viewName;
        constexpr const char* queryInView = "SELECT 1";

        const TString creationQuery = std::format(R"(
                CREATE VIEW `{}` WITH (security_invoker = true) AS {};
            )",
            path.c_str(),
            queryInView
        );
        ExecuteDataDefinitionQuery(session, creationQuery);

        auto schemeClient = kikimr.GetSchemeClient();
        const auto lsResults = schemeClient.ListDirectory(folder).GetValueSync();
        UNIT_ASSERT_C(lsResults.IsSuccess(), lsResults.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(lsResults.GetChildren().size(), 1);
        const auto& viewEntry = lsResults.GetChildren()[0];
        UNIT_ASSERT_VALUES_EQUAL(viewEntry.Name, viewName);
        UNIT_ASSERT_VALUES_EQUAL(viewEntry.Type, NYdb::NScheme::ESchemeEntryType::View);
    }

    Y_UNIT_TEST(CreateSameViewTwice) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        EnableViewsFeatureFlag(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        constexpr const char* path = "/Root/TheView";
        constexpr const char* queryInView = "SELECT 1";

        const TString creationQuery = std::format(R"(
                CREATE VIEW `{}` WITH (security_invoker = true) AS {};
            )",
            path,
            queryInView
        );
        ExecuteDataDefinitionQuery(session, creationQuery);
        {
            const auto creationResult = session.ExecuteSchemeQuery(creationQuery).GetValueSync();
            UNIT_ASSERT(!creationResult.IsSuccess());
            UNIT_ASSERT(creationResult.GetIssues().ToString().Contains("error: path exist, request accepts it"));
        }
    }

    Y_UNIT_TEST(DropView) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        EnableViewsFeatureFlag(kikimr);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        constexpr const char* path = "/Root/TheView";
        constexpr const char* queryInView = "SELECT 1";

        const TString creationQuery = std::format(R"(
                CREATE VIEW `{}` WITH (security_invoker = true) AS {};
            )",
            path,
            queryInView
        );
        ExecuteDataDefinitionQuery(session, creationQuery);

        const TString dropQuery = std::format(R"(
                DROP VIEW `{}`;
            )",
            path
        );
        ExecuteDataDefinitionQuery(session, dropQuery);
        ExpectUnknownEntry(runtime, path);
    }

    Y_UNIT_TEST(DropViewDisabledFeatureFlag) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        constexpr const char* path = "/Root/TheView";

        const TString creationQuery = std::format(R"(
                CREATE VIEW `{}` WITH (security_invoker = true) AS SELECT 1;
            )",
            path
        );
        EnableViewsFeatureFlag(kikimr);
        ExecuteDataDefinitionQuery(session, creationQuery);

        const TString dropQuery = std::format(R"(
                DROP VIEW `{}`;
            )",
            path
        );
        DisableViewsFeatureFlag(kikimr);
        const auto dropResult = session.ExecuteSchemeQuery(dropQuery).ExtractValueSync();
        UNIT_ASSERT(!dropResult.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(dropResult.GetIssues().ToString(), "Error: Views are disabled");
    }

    Y_UNIT_TEST(DropSameViewTwice) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        EnableViewsFeatureFlag(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        constexpr const char* path = "/Root/TheView";
        constexpr const char* queryInView = "SELECT 1";

        const TString creationQuery = std::format(R"(
                CREATE VIEW `{}` WITH (security_invoker = true) AS {};
            )",
            path,
            queryInView
        );
        ExecuteDataDefinitionQuery(session, creationQuery);

        const TString dropQuery = std::format(R"(
                DROP VIEW `{}`;
            )",
            path
        );
        ExecuteDataDefinitionQuery(session, dropQuery);
        {
            const auto dropResult = session.ExecuteSchemeQuery(dropQuery).GetValueSync();
            UNIT_ASSERT(!dropResult.IsSuccess());
            UNIT_ASSERT(dropResult.GetIssues().ToString().Contains("Error: Path does not exist"));
        }
    }

    Y_UNIT_TEST(DropViewInFolder) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        EnableViewsFeatureFlag(kikimr);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        constexpr const char* path = "/Root/some/path/to/TheView";
        constexpr const char* queryInView = "SELECT 1";

        const TString creationQuery = std::format(R"(
                CREATE VIEW `{}` WITH (security_invoker = true) AS {};
            )",
            path,
            queryInView
        );
        ExecuteDataDefinitionQuery(session, creationQuery);

        const TString dropQuery = std::format(R"(
                DROP VIEW `{}`;
            )",
            path
        );
        ExecuteDataDefinitionQuery(session, dropQuery);
        ExpectUnknownEntry(runtime, path);
    }

    Y_UNIT_TEST(ContextPollution) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        EnableViewsFeatureFlag(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        ExecuteDataDefinitionQuery(session, R"(
            CREATE VIEW InnerView WITH (security_invoker = TRUE) AS SELECT 1;
        )");
        ExecuteDataDefinitionQuery(session, R"(
            CREATE VIEW OuterView WITH (security_invoker = TRUE) AS SELECT * FROM InnerView;
        )");

        ExecuteDataDefinitionQuery(session, R"(
            DROP VIEW OuterView;
            CREATE VIEW OuterView WITH (security_invoker = TRUE) AS SELECT * FROM InnerView;
        )");
    }
}

Y_UNIT_TEST_SUITE(TSelectFromViewTest) {

    Y_UNIT_TEST(OneTable) {
        TKikimrRunner kikimr;
        EnableViewsFeatureFlag(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        constexpr const char* viewName = "/Root/TheView";
        constexpr const char* testTable = "/Root/Test";
        const auto innerQuery = std::format(R"(
                SELECT * FROM `{}`
            )",
            testTable
        );

        const TString creationQuery = std::format(R"(
                CREATE VIEW `{}` WITH (security_invoker = true) AS {};
            )",
            viewName,
            innerQuery
        );
        ExecuteDataDefinitionQuery(session, creationQuery);

        const auto etalonResults = ExecuteDataModificationQuery(session, std::format(R"(
                    SELECT * FROM ({});
                )",
                innerQuery
            )
        );
        const auto selectFromViewResults = ExecuteDataModificationQuery(session, std::format(R"(
                    SELECT * FROM `{}`;
                )",
                viewName
            )
        );
        CompareResults(etalonResults, selectFromViewResults);
    }

    Y_UNIT_TEST(DisabledFeatureFlag) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        constexpr const char* path = "/Root/TheView";

        const TString creationQuery = std::format(R"(
                CREATE VIEW `{}` WITH (security_invoker = true) AS SELECT 1;
            )",
            path
        );
        EnableViewsFeatureFlag(kikimr);
        ExecuteDataDefinitionQuery(session, creationQuery);

        const TString selectQuery = std::format(R"(
                SELECT * FROM `{}`;
            )",
            path
        );
        DisableViewsFeatureFlag(kikimr);
        const auto selectResult = session.ExecuteDataQuery(
                selectQuery,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()
            ).ExtractValueSync();
        UNIT_ASSERT(!selectResult.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(selectResult.GetIssues().ToString(), "Error: Views are disabled");
    }

    Y_UNIT_TEST(ReadTestCasesFromFiles) {
        TKikimrRunner kikimr;
        EnableViewsFeatureFlag(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        InitializeTablesAndSecondaryViews(session);
        EnableLogging();

        const auto testcasesFolder = ArcadiaFromCurrentLocation(__SOURCE_FILE__, "input/cases");
        TDirsList testcases;
        testcases.Fill(testcasesFolder);
        TString testcase;
        while (testcase = testcases.Next()) {
            const auto pathPrefix = TStringBuilder() << testcasesFolder << '/' << testcase << '/';
            ExecuteDataDefinitionQuery(session, ReadWholeFile(pathPrefix + "create_view.sql"));

            const auto etalonResults = ExecuteDataModificationQuery(session, ReadWholeFile(pathPrefix + "etalon_query.sql"));
            const auto selectFromViewResults = ExecuteDataModificationQuery(session, ReadWholeFile(pathPrefix + "select_from_view.sql"));
            CompareResults(etalonResults, selectFromViewResults);

            ExecuteDataDefinitionQuery(session, ReadWholeFile(pathPrefix + "drop_view.sql"));
        }
    }

    Y_UNIT_TEST(QueryCacheIsUpdated) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        EnableViewsFeatureFlag(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        constexpr const char* viewName = "TheView";

        const auto getCreationQuery = [&viewName](const char* innerQuery) -> TString {
            return std::format(R"(
                    CREATE VIEW {} WITH (security_invoker = TRUE) AS {};
                )",
                viewName,
                innerQuery
            );
        };
        constexpr const char* firstInnerQuery = "SELECT 1";
        ExecuteDataDefinitionQuery(session, getCreationQuery(firstInnerQuery));

        const TString selectFromViewQuery = std::format(R"(
                SELECT * FROM {};
            )",
            viewName
        );
        TExecDataQuerySettings queryExecutionSettings;
        queryExecutionSettings.KeepInQueryCache(true);
        queryExecutionSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);
        ExecuteDataModificationQuery(session, selectFromViewQuery, queryExecutionSettings);
        // make sure the server side cache is working by calling the same query twice
        const auto cachedQueryRawResult = ExecuteDataModificationQuery(session, selectFromViewQuery, queryExecutionSettings);
        AssertFromCache(cachedQueryRawResult.GetStats(), true);
        UNIT_ASSERT_VALUES_EQUAL(GetInteger(GetSingleResult(cachedQueryRawResult)), 1);

        // recreate the view with a different query inside
        ExecuteDataDefinitionQuery(session, std::format(R"(
                    DROP VIEW {};
                )",
                viewName
            )
        );
        constexpr const char* secondInnerQuery = "SELECT 2";
        ExecuteDataDefinitionQuery(session, getCreationQuery(secondInnerQuery));

        const auto secondCallRawResult = ExecuteDataModificationQuery(session, selectFromViewQuery, queryExecutionSettings);
        AssertFromCache(secondCallRawResult.GetStats(), false);
        UNIT_ASSERT_VALUES_EQUAL(GetInteger(GetSingleResult(secondCallRawResult)), 2);
    }
}

Y_UNIT_TEST_SUITE(TEvaluateExprInViewTest) {

    Y_UNIT_TEST(EvaluateExpr) {
        TKikimrRunner kikimr;
        EnableViewsFeatureFlag(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        constexpr const char* viewName = "TheView";
        constexpr const char* timeQuery = R"(
            SELECT EvaluateExpr(CurrentUtcTimestamp())
        )";

        const TString creationQuery = std::format(R"(
                CREATE VIEW {} WITH (security_invoker = TRUE) AS {};
            )",
            viewName,
            timeQuery
        );
        ExecuteDataDefinitionQuery(session, creationQuery);

        const TString selectFromViewQuery = std::format(R"(
                SELECT * FROM {};
            )",
            viewName
        );
        TExecDataQuerySettings queryExecutionSettings;
        queryExecutionSettings.KeepInQueryCache(true);
        const auto executeTwice = [&](const TString& query) {
            return TVector<TInstant>{
                GetTimestamp(GetSingleResult(session, query, queryExecutionSettings)),
                GetTimestamp(GetSingleResult(session, query, queryExecutionSettings))
            };
        };
        const auto viewResults = executeTwice(selectFromViewQuery);
        const auto etalonResults = executeTwice(timeQuery);
        UNIT_ASSERT_EQUAL_C(viewResults[0] < viewResults[1], etalonResults[0] < etalonResults[1],
                            TStringBuilder()
                                << "\nQuery cache works differently for EvaluateExpr written (1) in a view versus (2) in a plain SELECT statement.\n"
                                << "(1) SELECT from view results: (first call) " << viewResults[0] << ", (second call) " << viewResults[1]
                                << "(2) SELECT EvaluateExpr(...) results: (first call) " << etalonResults[0] << ", (second call) " << etalonResults[1] << "\n"
        );
    }

    Y_UNIT_TEST(NakedCallToCurrentTimeFunction) {
        TKikimrRunner kikimr;
        EnableViewsFeatureFlag(kikimr);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        constexpr const char* viewName = "TheView";
        constexpr const char* timeQuery = R"(
            SELECT CurrentUtcTimestamp()
        )";

        const TString creationQuery = std::format(R"(
                CREATE VIEW {} WITH (security_invoker = TRUE) AS {};
            )",
            viewName,
            timeQuery
        );
        ExecuteDataDefinitionQuery(session, creationQuery);

        const TString selectFromViewQuery = std::format(R"(
                SELECT * FROM {};
            )",
            viewName
        );
        TExecDataQuerySettings queryExecutionSettings;
        queryExecutionSettings.KeepInQueryCache(true);
        const auto executeTwice = [&](const TString& query) {
            return TVector<TInstant>{
                GetTimestamp(GetSingleResult(session, query, queryExecutionSettings)),
                GetTimestamp(GetSingleResult(session, query, queryExecutionSettings))
            };
        };
        const auto viewResults = executeTwice(selectFromViewQuery);
        UNIT_ASSERT_LT(viewResults[0], viewResults[1]);
    }
}
