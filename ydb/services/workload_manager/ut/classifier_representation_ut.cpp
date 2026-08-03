#include <ydb/services/workload_manager/ut/common/workload_service_ut_common.h>
#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/object.h>
#include <ydb/services/metadata/abstract/request_features.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <unordered_set>


namespace NKikimr::NWorkloadManager {

using namespace NYdb;
using namespace NResourcePool;

Y_UNIT_TEST_SUITE(TResourcePoolClassifierConfigSerialization) {
    Y_UNIT_TEST(RoundTripBasicFields) {
        TClassifierSettings settings;
        settings.Rank = 10;
        settings.ResourcePool = "pool_a";
        settings.MemberName = "user@org";
        settings.HasAppName = "ydb-cli";
        settings.HasFullScan = TRegexPredicate::FromGlob("/Root/db/*");
        settings.HasPath = TRegexPredicate::FromGlob("/Root/archive/?");
        settings.HasStream = true;

        const auto json = TResourcePoolClassifierConfig::SerializeToJson(settings);
        const auto restored = TResourcePoolClassifierConfig::DeserializeFromJson(json, 10);

        UNIT_ASSERT_VALUES_EQUAL(restored.Rank, 10);
        UNIT_ASSERT_VALUES_EQUAL(*restored.ResourcePool, "pool_a");
        UNIT_ASSERT_VALUES_EQUAL(*restored.MemberName, "user@org");
        UNIT_ASSERT_VALUES_EQUAL(*restored.HasAppName, "ydb-cli");
        UNIT_ASSERT_VALUES_EQUAL(restored.HasFullScan->Pattern, "/Root/db/*");
        UNIT_ASSERT_VALUES_EQUAL(restored.HasPath->Pattern, "/Root/archive/?");
        UNIT_ASSERT_VALUES_EQUAL(*restored.HasStream, true);
        UNIT_ASSERT(!restored.Action.has_value());
    }

    Y_UNIT_TEST(EmptyStringsTreatedAsAbsent) {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["resource_pool"] = "pool_a";
        json["member_name"] = "";
        json["has_app_name"] = "";
        json["has_full_scan"] = "";
        json["has_path"] = "";
        json["action"] = "";

        const auto settings = TResourcePoolClassifierConfig::DeserializeFromJson(json, 1);
        UNIT_ASSERT_VALUES_EQUAL(*settings.ResourcePool, "pool_a");
        UNIT_ASSERT(!settings.MemberName.has_value());
        UNIT_ASSERT(!settings.HasAppName.has_value());
        UNIT_ASSERT(!settings.HasFullScan.has_value());
        UNIT_ASSERT(!settings.HasPath.has_value());
        UNIT_ASSERT(!settings.Action.has_value());
    }

    Y_UNIT_TEST(HasStreamAcceptsBooleanAndString) {
        {
            NJson::TJsonValue json(NJson::JSON_MAP);
            json["resource_pool"] = "pool_a";
            json["has_stream"] = true;
            const auto settings = TResourcePoolClassifierConfig::DeserializeFromJson(json, 1);
            UNIT_ASSERT_VALUES_EQUAL(*settings.HasStream, true);
        }
        {
            NJson::TJsonValue json(NJson::JSON_MAP);
            json["resource_pool"] = "pool_a";
            json["has_stream"] = "false";
            const auto settings = TResourcePoolClassifierConfig::DeserializeFromJson(json, 1);
            UNIT_ASSERT_VALUES_EQUAL(*settings.HasStream, false);
        }
    }

    Y_UNIT_TEST(InvalidActionSkipped) {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["resource_pool"] = "pool_a";
        json["action"] = "not_a_real_action";

        const auto settings = TResourcePoolClassifierConfig::DeserializeFromJson(json, 1);
        UNIT_ASSERT_VALUES_EQUAL(*settings.ResourcePool, "pool_a");
        UNIT_ASSERT(!settings.Action.has_value());
    }

    Y_UNIT_TEST(RejectActionClearsResourcePool) {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["resource_pool"] = "pool_a";
        json["action"] = "reject";

        const auto settings = TResourcePoolClassifierConfig::DeserializeFromJson(json, 1);
        UNIT_ASSERT(!settings.ResourcePool.has_value());
        UNIT_ASSERT(settings.Action.has_value());
        UNIT_ASSERT_EQUAL(*settings.Action, EClassifierAction::Reject);
    }

    Y_UNIT_TEST(SerializeRejectOmitsResourcePool) {
        TClassifierSettings settings;
        settings.Action = EClassifierAction::Reject;
        settings.ResourcePool = "should_not_be_written";

        const auto json = TResourcePoolClassifierConfig::SerializeToJson(settings);
        UNIT_ASSERT(!json.Has("resource_pool"));
        UNIT_ASSERT_VALUES_EQUAL(json["action"].GetString(), "reject");
    }

    Y_UNIT_TEST(ParseRejectsEmptyResourcePool) {
        NYql::TFeaturesExtractor extractor(
            THashMap<TString, TString>{
                {"resource_pool", ""},
            },
            std::unordered_set<TString>{});
        TResourcePoolClassifierConfig::TParseResult result;
        const auto error = TResourcePoolClassifierConfig::ParseFromFeaturesExtractor(extractor, &result);
        UNIT_ASSERT(error.has_value());
        UNIT_ASSERT_STRING_CONTAINS(*error, "Failed to parse property resource_pool");
        UNIT_ASSERT_STRING_CONTAINS(*error, "resource pool name must not be empty");
    }
}

namespace {

TQueryRunnerResult ReadSysView(TIntrusivePtr<IYdbSetup> ydb, const TString& classifierName) {
    return ydb->ExecuteQuery(
        TStringBuilder() << R"(
            SELECT *
            FROM `.sys/resource_pool_classifiers`
            WHERE Name = ")" << classifierName << R"("
        )",
        TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID));
}


TQueryRunnerResult ReadMetadataTable(TIntrusivePtr<IYdbSetup> ydb, const TString& classifierName) {
    return ydb->ExecuteQuery(
        TStringBuilder() << R"(
            SELECT database, name, rank, config
            FROM `.metadata/workload_manager/classifiers/resource_pool_classifiers`
            WHERE name = ")" << classifierName << R"("
        )",
        TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID));
}

/// Parse a JSON string; fatally fail the test on parse error.
NJson::TJsonValue ParseJson(const TString& raw) {
    NJson::TJsonValue json;
    UNIT_ASSERT_C(NJson::ReadJsonTree(raw, &json),
        TStringBuilder() << "config column must be valid JSON, got: " << raw);
    return json;
}

}  // namespace


Y_UNIT_TEST_SUITE(ClassifierRepresentation) {

    /// Minimal classifier (Name, Rank, ResourcePool only):
    Y_UNIT_TEST(MinimalClassifier) {
        auto ydb = TYdbSetupSettings().Create();
        const TString dbName = TStringBuilder() << "/" << ydb->GetSettings().DomainName_;

        ydb->ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER some_classifier WITH (
                RESOURCE_POOL = "default",
                RANK          = 10
            );
        )");

        ydb->WaitForClassifierPropagation();

        // ---- sys-view ----
        {
            auto result = ReadSysView(ydb, "some_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in sys-view");

            // Mandatory columns
            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("Name").GetOptionalUtf8(),         "some_classifier");
            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("Rank").GetOptionalInt64(),         10);
            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("ResourcePool").GetOptionalUtf8(), "default");

            // Optional columns must be NULL
            UNIT_ASSERT_C(!rs.ColumnParser("MemberName").GetOptionalUtf8().has_value(),
                "MemberName must be NULL when absent");
            UNIT_ASSERT_C(!rs.ColumnParser("HasAppName").GetOptionalUtf8().has_value(),
                "HasAppName must be NULL when absent");
            UNIT_ASSERT_C(!rs.ColumnParser("HasFullScan").GetOptionalUtf8().has_value(),
                "HasFullScan must be NULL when absent");
            UNIT_ASSERT_C(!rs.ColumnParser("HasPath").GetOptionalUtf8().has_value(),
                "HasPath must be NULL when absent");
            UNIT_ASSERT_C(!rs.ColumnParser("Action").GetOptionalUtf8().has_value(),
                "Action must be NULL when absent");
            UNIT_ASSERT_C(!rs.ColumnParser("HasStream").GetOptionalBool().has_value(),
                "HasStream must be NULL when absent");
        }

        // ---- metadata table ----
        {
            auto result = ReadMetadataTable(ydb, "some_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in metadata table");

            // Scalar columns
            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("database").GetOptionalUtf8(), dbName);
            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("name").GetOptionalUtf8(),     "some_classifier");
            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("rank").GetOptionalInt64(),    10);

            // JSON config: mandatory key present, optional keys absent
            const auto json = ParseJson(TString(*rs.ColumnParser("config").GetOptionalJsonDocument()));
            UNIT_ASSERT_C(json.Has("resource_pool"), "JSON must contain 'resource_pool'");
            UNIT_ASSERT_VALUES_EQUAL(json["resource_pool"].GetString(), "default");
            UNIT_ASSERT_C(!json.Has("member_name"),   "'member_name' must be absent from JSON");
            UNIT_ASSERT_C(!json.Has("has_app_name"),  "'has_app_name' must be absent from JSON");
            UNIT_ASSERT_C(!json.Has("has_full_scan"), "'has_full_scan' must be absent from JSON");
            UNIT_ASSERT_C(!json.Has("has_path"),      "'has_path' must be absent from JSON");
            UNIT_ASSERT_C(!json.Has("action"),        "'action' must be absent from JSON");
            UNIT_ASSERT_C(!json.Has("has_stream"),    "'has_stream' must be absent from JSON");
        }
    }

    /// Full classifier (every optional field set) shows correct values on both surfaces.
    Y_UNIT_TEST(AllOptionalFields_PresentWithCorrectValues) {
        auto ydb = TYdbSetupSettings().Create();

        ydb->ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER some_classifier WITH (
                RESOURCE_POOL = "default",
                MEMBER_NAME   = "alice@staff",
                HAS_APP_NAME  = "myapp",
                HAS_FULL_SCAN = "/Root/.*",
                HAS_PATH      = "/Root/db/.*",
                HAS_STREAM    = "true",
                RANK          = 30
            );
        )");

        ydb->WaitForClassifierPropagation();

        // ---- sys-view ----
        {
            auto result = ReadSysView(ydb, "some_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in sys-view");

            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("MemberName").GetOptionalUtf8(),  "alice@staff");
            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("HasAppName").GetOptionalUtf8(),  "myapp");
            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("HasFullScan").GetOptionalUtf8(), "/Root/.*");
            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("HasPath").GetOptionalUtf8(),     "/Root/db/.*");

            const auto hasStream = rs.ColumnParser("HasStream").GetOptionalBool();
            UNIT_ASSERT_C(hasStream.has_value(), "HasStream must not be NULL");
            UNIT_ASSERT_VALUES_EQUAL(*hasStream, true);

            // Action was not specified
            UNIT_ASSERT_C(!rs.ColumnParser("Action").GetOptionalUtf8().has_value(),
                "Action must be NULL when not specified");
        }

        // ---- metadata table ----
        {
            auto result = ReadMetadataTable(ydb, "some_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in metadata table");

            const auto json = ParseJson(TString(*rs.ColumnParser("config").GetOptionalJsonDocument()));
            UNIT_ASSERT_VALUES_EQUAL(json["member_name"].GetString(),  "alice@staff");
            UNIT_ASSERT_VALUES_EQUAL(json["has_app_name"].GetString(), "myapp");
            UNIT_ASSERT_VALUES_EQUAL(json["has_full_scan"].GetString(), "/Root/.*");
            UNIT_ASSERT_VALUES_EQUAL(json["has_path"].GetString(),     "/Root/db/.*");

            UNIT_ASSERT_C(json.Has("has_stream"), "JSON must contain 'has_stream'");
            UNIT_ASSERT_C(json["has_stream"].GetType() == NJson::JSON_BOOLEAN,
                "'has_stream' must be a JSON boolean");
            UNIT_ASSERT_VALUES_EQUAL(json["has_stream"].GetBoolean(), true);

            UNIT_ASSERT_C(!json.Has("action"), "'action' must be absent from JSON when not specified");
        }
    }

    /// HAS_STREAM = false is stored as boolean false (not NULL, not a string) on both surfaces.
    Y_UNIT_TEST(HasStream_FalseStoredAsBoolNotNull) {
        auto ydb = TYdbSetupSettings().Create();

        ydb->ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER some_classifier WITH (
                RESOURCE_POOL = "default",
                HAS_STREAM    = "false",
                RANK          = 40
            );
        )");

        ydb->WaitForClassifierPropagation();

        // ---- sys-view ----
        {
            auto result = ReadSysView(ydb, "some_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in sys-view");

            const auto hasStream = rs.ColumnParser("HasStream").GetOptionalBool();
            UNIT_ASSERT_C(hasStream.has_value(), "HasStream must not be NULL when explicitly set to false");
            UNIT_ASSERT_VALUES_EQUAL(*hasStream, false);
        }

        // ---- metadata table ----
        {
            auto result = ReadMetadataTable(ydb, "some_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in metadata table");

            const auto json = ParseJson(TString(*rs.ColumnParser("config").GetOptionalJsonDocument()));
            UNIT_ASSERT_C(json.Has("has_stream"), "JSON must contain 'has_stream'");
            UNIT_ASSERT_C(json["has_stream"].GetType() == NJson::JSON_BOOLEAN,
                "'has_stream' must be a JSON boolean");
            UNIT_ASSERT_VALUES_EQUAL(json["has_stream"].GetBoolean(), false);
        }
    }

    /// ACTION = "reject" is stored correctly; ResourcePool defaults to "default".
    Y_UNIT_TEST(Action_RejectClassifier) {
        auto ydb = TYdbSetupSettings().Create();

        ydb->ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER some_classifier WITH (
                ACTION = "reject",
                RANK   = 50
            );
        )");

        ydb->WaitForClassifierPropagation();

        // ---- sys-view ----
        {
            auto result = ReadSysView(ydb, "some_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in sys-view");

            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("Action").GetOptionalUtf8(), "reject");
            // ResourcePool must be NULL for reject classifiers (not stored in config)
            UNIT_ASSERT_C(!rs.ColumnParser("ResourcePool").GetOptionalUtf8().has_value(),
                "ResourcePool must be NULL for Reject classifiers");
            // Other optional fields absent
            UNIT_ASSERT_C(!rs.ColumnParser("MemberName").GetOptionalUtf8().has_value(),
                "MemberName must be NULL");
        }

        // ---- metadata table ----
        {
            auto result = ReadMetadataTable(ydb, "some_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in metadata table");

            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("rank").GetOptionalInt64(), 50);

            const auto json = ParseJson(TString(*rs.ColumnParser("config").GetOptionalJsonDocument()));
            UNIT_ASSERT_C(json.Has("action"), "JSON must contain 'action'");
            UNIT_ASSERT_VALUES_EQUAL(json["action"].GetString(), "reject");
            UNIT_ASSERT_C(!json.Has("member_name"), "'member_name' must be absent");
        }
    }

    /// ALTER updates the representation on both surfaces.
    Y_UNIT_TEST(AlterUpdatesRepresentation) {
        auto ydb = TYdbSetupSettings().Create();

        ydb->ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER some_classifier WITH (
                RESOURCE_POOL = "default",
                MEMBER_NAME   = "original@user",
                RANK          = 60
            );
        )");

        ydb->WaitForClassifierPropagation();

        ydb->ExecuteSchemeQuery(R"(
            ALTER RESOURCE POOL CLASSIFIER some_classifier SET (
                MEMBER_NAME = "updated@user",
                RANK        = 99
            );
        )");

        ydb->WaitForClassifierPropagation();

        // ---- sys-view ----
        {
            auto result = ReadSysView(ydb, "some_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in sys-view");

            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("Rank").GetOptionalInt64(),       99);
            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("MemberName").GetOptionalUtf8(), "updated@user");
        }

        // ---- metadata table ----
        {
            auto result = ReadMetadataTable(ydb, "some_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in metadata table");

            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("rank").GetOptionalInt64(), 99);

            const auto json = ParseJson(TString(*rs.ColumnParser("config").GetOptionalJsonDocument()));
            UNIT_ASSERT_VALUES_EQUAL(json["member_name"].GetString(), "updated@user");
        }
    }

    /// Creating a classifier without an explicit RANK auto-assigns rank = maxRank + 1000.
    Y_UNIT_TEST(AutoRankIsMaxRankPlus1000) {
        auto ydb = TYdbSetupSettings().Create();

        // Explicit rank
        ydb->ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER explicit_rank_classifier WITH (
                RESOURCE_POOL = "default",
                MEMBER_NAME   = "explicit@user",
                RANK          = 20
            );
        )");

        // Auto rank (no RANK specified)
        ydb->ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER auto_rank_classifier WITH (
                RESOURCE_POOL = "default",
                MEMBER_NAME   = "auto@user"
            );
        )");

        ydb->WaitForClassifierPropagation();

        // The auto-assigned rank must be the previous max (20) + 1000 = 1020.
        {
            auto result = ReadSysView(ydb, "auto_rank_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in sys-view");
            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("Rank").GetOptionalInt64(), 1020);
        }

        {
            auto result = ReadMetadataTable(ydb, "auto_rank_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in metadata table");
            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("rank").GetOptionalInt64(), 1020);
        }
    }

    /// ALTER ... RESET (MEMBER_NAME) clears the optional field from the representation.
    Y_UNIT_TEST(ResetMemberNameClearsField) {
        auto ydb = TYdbSetupSettings().Create();

        ydb->ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER some_classifier WITH (
                RESOURCE_POOL = "default",
                MEMBER_NAME   = "present@user",
                RANK          = 20
            );
        )");

        ydb->WaitForClassifierPropagation();

        ydb->ExecuteSchemeQuery(R"(
            ALTER RESOURCE POOL CLASSIFIER some_classifier RESET (
                MEMBER_NAME
            );
        )");

        ydb->WaitForClassifierPropagation();

        // ---- sys-view: MemberName is NULL after RESET; Rank is unchanged ----
        {
            auto result = ReadSysView(ydb, "some_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in sys-view");
            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("Rank").GetOptionalInt64(), 20);
            UNIT_ASSERT_C(!rs.ColumnParser("MemberName").GetOptionalUtf8().has_value(),
                "MemberName must be NULL after RESET");
        }

        // ---- metadata table: member_name key is absent after RESET; rank unchanged ----
        {
            auto result = ReadMetadataTable(ydb, "some_classifier");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in metadata table");

            UNIT_ASSERT_VALUES_EQUAL(*rs.ColumnParser("rank").GetOptionalInt64(), 20);
            const auto json = ParseJson(TString(*rs.ColumnParser("config").GetOptionalJsonDocument()));
            UNIT_ASSERT_C(!json.Has("member_name"), "'member_name' must be absent from JSON after RESET");
        }
    }

    /// For each optional classifier property, SET then RESET should restore the original state.
    /// Tests: member_name, has_app_name, has_full_scan, has_path, has_stream, action.
    Y_UNIT_TEST(SetThenResetRestoresOriginalState) {
        auto ydb = TYdbSetupSettings().Create();

        // Create a minimal classifier with only resource_pool
        ydb->ExecuteSchemeQuery(R"(
            CREATE RESOURCE POOL CLASSIFIER my_cls WITH (
                RESOURCE_POOL = "default",
                RANK          = 10
            );
        )");

        ydb->WaitForClassifierPropagation();

        auto getConfigJson = [&]() -> NJson::TJsonValue {
            auto result = ReadMetadataTable(ydb, "my_cls");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in metadata table");
            return ParseJson(TString(*rs.ColumnParser("config").GetOptionalJsonDocument()));
        };

        auto getSysViewAction = [&]() -> std::optional<TString> {
            auto result = ReadSysView(ydb, "my_cls");
            TSampleQueries::CheckSuccess(result);
            NYdb::TResultSetParser rs(result.GetResultSet(0));
            UNIT_ASSERT_C(rs.TryNextRow(), "Expected one row in sys-view");
            return rs.ColumnParser("Action").GetOptionalUtf8();
        };

        const auto originalJson = getConfigJson();
        UNIT_ASSERT_C(!originalJson.Has("member_name"), "baseline should not have member_name");
        UNIT_ASSERT_C(!originalJson.Has("has_app_name"), "baseline should not have has_app_name");
        UNIT_ASSERT_C(!originalJson.Has("has_full_scan"), "baseline should not have has_full_scan");
        UNIT_ASSERT_C(!originalJson.Has("has_path"), "baseline should not have has_path");
        UNIT_ASSERT_C(!originalJson.Has("has_stream"), "baseline should not have has_stream");
        UNIT_ASSERT_C(!originalJson.Has("action"), "baseline should not have action");

        // --- member_name: SET then RESET ---
        {
            ydb->ExecuteSchemeQuery(R"(
                ALTER RESOURCE POOL CLASSIFIER my_cls SET (MEMBER_NAME = "test@user");
            )");
            ydb->WaitForClassifierPropagation();
            {
                auto json = getConfigJson();
                UNIT_ASSERT_VALUES_EQUAL(json["member_name"].GetString(), "test@user");
            }

            ydb->ExecuteSchemeQuery(R"(
                ALTER RESOURCE POOL CLASSIFIER my_cls RESET (MEMBER_NAME);
            )");
            ydb->WaitForClassifierPropagation();
            {
                auto json = getConfigJson();
                UNIT_ASSERT_C(!json.Has("member_name"), "member_name must be absent after RESET");
            }
        }

        // --- has_app_name: SET then RESET ---
        {
            ydb->ExecuteSchemeQuery(R"(
                ALTER RESOURCE POOL CLASSIFIER my_cls SET (HAS_APP_NAME = "my_app");
            )");
            ydb->WaitForClassifierPropagation();
            {
                auto json = getConfigJson();
                UNIT_ASSERT_VALUES_EQUAL(json["has_app_name"].GetString(), "my_app");
            }

            ydb->ExecuteSchemeQuery(R"(
                ALTER RESOURCE POOL CLASSIFIER my_cls RESET (HAS_APP_NAME);
            )");
            ydb->WaitForClassifierPropagation();
            {
                auto json = getConfigJson();
                UNIT_ASSERT_C(!json.Has("has_app_name"), "has_app_name must be absent after RESET");
            }
        }

        // --- has_full_scan: SET then RESET ---
        {
            ydb->ExecuteSchemeQuery(R"(
                ALTER RESOURCE POOL CLASSIFIER my_cls SET (HAS_FULL_SCAN = "/Root/*");
            )");
            ydb->WaitForClassifierPropagation();
            {
                auto json = getConfigJson();
                UNIT_ASSERT_VALUES_EQUAL(json["has_full_scan"].GetString(), "/Root/*");
            }

            ydb->ExecuteSchemeQuery(R"(
                ALTER RESOURCE POOL CLASSIFIER my_cls RESET (HAS_FULL_SCAN);
            )");
            ydb->WaitForClassifierPropagation();
            {
                auto json = getConfigJson();
                UNIT_ASSERT_C(!json.Has("has_full_scan"), "has_full_scan must be absent after RESET");
            }
        }

        // --- has_path: SET then RESET ---
        {
            ydb->ExecuteSchemeQuery(R"(
                ALTER RESOURCE POOL CLASSIFIER my_cls SET (HAS_PATH = "/Root/db/*");
            )");
            ydb->WaitForClassifierPropagation();
            {
                auto json = getConfigJson();
                UNIT_ASSERT_VALUES_EQUAL(json["has_path"].GetString(), "/Root/db/*");
            }

            ydb->ExecuteSchemeQuery(R"(
                ALTER RESOURCE POOL CLASSIFIER my_cls RESET (HAS_PATH);
            )");
            ydb->WaitForClassifierPropagation();
            {
                auto json = getConfigJson();
                UNIT_ASSERT_C(!json.Has("has_path"), "has_path must be absent after RESET");
            }
        }

        // --- has_stream: SET then RESET ---
        {
            ydb->ExecuteSchemeQuery(R"(
                ALTER RESOURCE POOL CLASSIFIER my_cls SET (HAS_STREAM = "true");
            )");
            ydb->WaitForClassifierPropagation();
            {
                auto json = getConfigJson();
                UNIT_ASSERT_VALUES_EQUAL(json["has_stream"].GetBoolean(), true);
            }

            ydb->ExecuteSchemeQuery(R"(
                ALTER RESOURCE POOL CLASSIFIER my_cls RESET (HAS_STREAM);
            )");
            ydb->WaitForClassifierPropagation();
            {
                auto json = getConfigJson();
                UNIT_ASSERT_C(!json.Has("has_stream"), "has_stream must be absent after RESET");
            }
        }

        // --- action: SET then RESET ---
        // ACTION="reject" and RESOURCE_POOL are mutually exclusive: setting the
        // action drops resource_pool from the config. Resetting the action alone
        // would leave the classifier with neither, which is invalid, so the
        // baseline resource_pool must be restored in the same ALTER.
        {
            ydb->ExecuteSchemeQuery(R"(
                ALTER RESOURCE POOL CLASSIFIER my_cls SET (ACTION = "reject");
            )");
            ydb->WaitForClassifierPropagation();
            {
                UNIT_ASSERT_VALUES_EQUAL(*getSysViewAction(), "reject");
                auto json = getConfigJson();
                UNIT_ASSERT_VALUES_EQUAL(json["action"].GetString(), "reject");
            }

            ydb->ExecuteSchemeQuery(R"(
                ALTER RESOURCE POOL CLASSIFIER my_cls RESET (ACTION), SET (RESOURCE_POOL = "default");
            )");
            ydb->WaitForClassifierPropagation();
            {
                UNIT_ASSERT_C(!getSysViewAction().has_value(),
                    "Action must be NULL in sys-view after RESET(ACTION)");
                auto json = getConfigJson();
                UNIT_ASSERT_C(!json.Has("action"), "action must be absent from JSON after RESET");
            }
        }

        // Final check: config should match the original baseline
        {
            auto finalJson = getConfigJson();
            UNIT_ASSERT(originalJson == finalJson);
        }
    }

}  // Y_UNIT_TEST_SUITE(ClassifierRepresentation)

}  // namespace NKikimr::NWorkloadManager
