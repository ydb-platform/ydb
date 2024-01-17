#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/yql/sql/sql.h>

#include <format>

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NYdb::NTable;

namespace {

void SetEnableViewsFeatureFlag(TKikimrRunner& kikimr) {
    kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableViews(true);
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

void ExecuteDataDefinitionQuery(TSession& session, const TString& script) {
    const auto result = session.ExecuteSchemeQuery(script, {}).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

}

Y_UNIT_TEST_SUITE(TKQPViewTest) {

    Y_UNIT_TEST(CheckCreatedView) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        SetEnableViewsFeatureFlag(kikimr);
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

    Y_UNIT_TEST(InvalidQuery) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        SetEnableViewsFeatureFlag(kikimr);
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

        const auto creationResult = session.ExecuteSchemeQuery(creationQuery, {}).ExtractValueSync();
        UNIT_ASSERT(!creationResult.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(creationResult.GetIssues().ToString(), "Error: Cannot divide type String and String");
    }

    Y_UNIT_TEST(ListCreatedView) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        SetEnableViewsFeatureFlag(kikimr);
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
        SetEnableViewsFeatureFlag(kikimr);
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
        SetEnableViewsFeatureFlag(kikimr);
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

    Y_UNIT_TEST(DropSameViewTwice) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        SetEnableViewsFeatureFlag(kikimr);
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
}
