#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/services/workload_manager/ut/common/workload_service_ut_common.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <fmt/format.h>

#include <unordered_map>
#include <vector>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace fmt::literals;

Y_UNIT_TEST_SUITE(KqpSchemeStreamingQuery) {
    std::unique_ptr<TKikimrRunner> SetupStreamingSource(bool enableStreamingQueries = true, bool enableStateRecompute = false) {
        NKikimrConfig::TAppConfig config;
        auto& featureFlags = *config.MutableFeatureFlags();
        featureFlags.SetEnableStreamingQueries(enableStreamingQueries);
        featureFlags.SetEnableExternalDataSources(/* value */ true);
        featureFlags.SetEnableResourcePools(/* value */ true);
        featureFlags.SetEnableStreamingQueryDisposition(/* value */ true);
        featureFlags.SetEnableStreamingQueryReadFrom(/* value */ true);
        featureFlags.SetEnableStreamingQueryStateRecompute(enableStateRecompute);
        config.MutableTableServiceConfig()->SetDqChannelVersion(/* value */ 1u);

        auto kikimr = std::make_unique<TKikimrRunner>(NKqp::TKikimrSettings(config)
            .SetEnableStreamingQueries(enableStreamingQueries)
            .SetEnableExternalDataSources(/* value */ true)
            .SetEnableResourcePools(/* value */ true)
            .SetInitFederatedQuerySetupFactory(/* value */ true));

        const auto result = kikimr->GetQueryClient().ExecuteQuery(fmt::format(R"(
            CREATE TOPIC MyTopic;
            CREATE EXTERNAL DATA SOURCE MySource WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "localhost:{port}",
                DATABASE_NAME = "/Root",
                AUTH_METHOD = "NONE"
            );)",
            "port"_a = kikimr->GetTestServer().GetGRpcServer().GetPort()),
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

        return kikimr;
    }

    Y_UNIT_TEST(DisableStreamingQueries) {
        auto kikimr = SetupStreamingSource(/* enableStreamingQueries */ false);
        auto db = kikimr->GetQueryClient();

        auto checkQuery = [&db](const TString& query, EStatus status, const TString& error) {
            Cerr << "Check query:\n" << query << "\n";
            const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), error);
        };

        auto checkDisabled = [checkQuery](const TString& query) {
            checkQuery(query, EStatus::UNSUPPORTED, "Streaming queries are disabled. Please contact your system administrator to enable it");
        };

        // CREATE STREAMING QUERY
        checkDisabled(R"(
            CREATE STREAMING QUERY MyQuery WITH (
                RUN = FALSE
            ) AS DO BEGIN
                INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
            END DO)");

        // ALTER STREAMING QUERY
        checkDisabled(R"(
            ALTER STREAMING QUERY MyQuery
            SET (RUN = FALSE);)");

        // DROP STREAMING QUERY
        checkQuery("DROP STREAMING QUERY MyQuery;",
            EStatus::NOT_FOUND,
            "Streaming query /Root/MyQuery not found or you don't have access permissions");
    }

    Y_UNIT_TEST(StreamingQueriesValidation) {
        auto kikimr = SetupStreamingSource();
        auto db = kikimr->GetQueryClient();

        // Test create

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE STREAMING QUERY `MyFolder/MyQuery` WITH (
                    UNKNOWN_PROPERTY = TRUE
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unknown property: unknown_property");
        }

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE STREAMING QUERY `MyFolder/MyQuery` WITH (
                    RUN = "yes"
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "RUN property must be 'true' or 'false'");
        }

        for (const TString& ifNotExists : {"", "IF NOT EXISTS"}) {
            for (const TString& force : {"TRUE", "FALSE"}) {
                const auto query = fmt::format(R"(
                    CREATE STREAMING QUERY {if_not_exists} `MyFolder/MyQuery` WITH (
                        RUN = FALSE,
                        FORCE = {force}
                    ) AS DO BEGIN
                        INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                    END DO)",
                    "if_not_exists"_a = ifNotExists, "force"_a = force);
                const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, query << "\n" << result.GetIssues().ToOneLineString());
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Invalid properties for creation new streaming query");
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Got unexpected properties: FORCE");
            }
        }

        // Test alter

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE STREAMING QUERY `MyFolder/MyQuery` WITH (
                    RUN = FALSE
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        {
            const auto result = db.ExecuteQuery(R"(
                ALTER STREAMING QUERY `MyFolder/MyQuery` SET (
                    UNKNOWN_PROPERTY = TRUE
                );)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unknown property: unknown_property");
        }

        {
            const auto result = db.ExecuteQuery(R"(
                ALTER STREAMING QUERY `MyFolder/MyQuery` SET (
                    FORCE = "yes"
                );)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "FORCE property must be 'true' or 'false'");
        }

        {
            const auto result = db.ExecuteQuery(R"(
                ALTER STREAMING QUERY `MyFolder/MyQuery` AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Changing the query text will result in the loss of the checkpoint.");
        }
    }

    void CheckObjectProperties(TTestActorRuntime& runtime, const TString& path, const std::unordered_map<TString, TString>& expectedProperties) {
        auto streamingQueryDesc = Navigate(runtime, runtime.AllocateEdgeActor(), path, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& streamingQuery = streamingQueryDesc->ResultSet.at(/* pos */ 0);
        UNIT_ASSERT_VALUES_EQUAL(streamingQuery.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindStreamingQuery);
        UNIT_ASSERT(streamingQuery.StreamingQueryInfo);
        UNIT_ASSERT_VALUES_EQUAL(streamingQuery.StreamingQueryInfo->Description.GetName(), SplitPath(path).back());
        const auto& properties = streamingQuery.StreamingQueryInfo->Description.GetProperties().GetProperties();
        UNIT_ASSERT_GE(properties.size(), expectedProperties.size());

        for (const auto& [key, value] : expectedProperties) {
            UNIT_ASSERT_C(properties.contains(key), key);
            UNIT_ASSERT_VALUES_EQUAL(properties.at(key), value);
        }
    }

    void CheckObjectNotFound(TTestActorRuntime& runtime, const TString& path) {
        auto streamingQueryDesc = Navigate(runtime, runtime.AllocateEdgeActor(), path, NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& streamingQuery = streamingQueryDesc->ResultSet.at(/* pos */ 0);
        UNIT_ASSERT_VALUES_EQUAL(streamingQueryDesc->ErrorCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(streamingQuery.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindUnknown);
    }

    Y_UNIT_TEST(StreamingQueryReadFrom) {
        auto kikimr = SetupStreamingSource();
        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        auto db = kikimr->GetQueryClient();

        const auto timestamp = TInstant::ParseIso8601("2025-05-04T11:30:34.336938Z");
        NYql::NPq::NProto::StreamingDisposition earliest;
        earliest.mutable_oldest();
        NYql::NPq::NProto::StreamingDisposition latest;
        latest.mutable_fresh();
        NYql::NPq::NProto::StreamingDisposition fromTime;
        *fromTime.mutable_from_time()->mutable_timestamp() = NProtoInterop::CastToProto(timestamp);
        NYql::NPq::NProto::StreamingDisposition fromExpression;
        *fromExpression.mutable_from_time()->mutable_timestamp() = NProtoInterop::CastToProto(timestamp + TDuration::Seconds(/* s */ 1));

        const std::vector<std::pair<TString, NYql::NPq::NProto::StreamingDisposition>> cases = {
            {"EARLIEST", earliest},
            {"latest", latest},
            {"\"EaRlIeSt\"u", earliest},
            {"Just(\"EARLIEST\")", earliest},
            {"Just(\"LATEST\"u)", latest},
            {"(EARLIEST)", earliest},
            {"Timestamp(\"2025-05-04T11:30:34.336938Z\")", fromTime},
            {"$timestamp", fromTime},
            {"Just($timestamp)", fromTime},
            {"$timestamp + Interval(\"PT1S\")", fromExpression},
        };

        for (const bool alter : {false, true}) {
            for (const auto& [value, disposition] : cases) {
                const TString query = TStringBuilder()
                    << "$timestamp = Timestamp(\"2025-05-04T11:30:34.336938Z\"); "
                    << (alter ? "ALTER STREAMING QUERY MyQuery SET (" : "CREATE OR REPLACE STREAMING QUERY MyQuery WITH (")
                    << "RUN = NOT TRUE, RESOURCE_POOL = \"my_\" || \"pool\", READ_FROM = " << value << ")"
                    << (alter ? ";" : " AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO;");
                const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, query << "\n" << result.GetIssues().ToString());
                CheckObjectProperties(runtime, "/Root/MyQuery", {
                    {"streaming_disposition", disposition.SerializeAsString()},
                    {"run", "false"},
                    {"resource_pool", "my_pool"},
                });
            }
        }

        const auto before = TInstant::Now() - TDuration::Seconds(/* s */ 1);
        const auto result = db.ExecuteQuery("ALTER STREAMING QUERY MyQuery SET (READ_FROM = CurrentUtcTimestamp() - Interval(\"PT1S\"));",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        const auto after = TInstant::Now() - TDuration::Seconds(/* s */ 1);
        const auto entry = Navigate(runtime, runtime.AllocateEdgeActor(), "/Root/MyQuery", NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& properties = entry->ResultSet.at(/* pos */ 0).StreamingQueryInfo->Description.GetProperties().GetProperties();
        NYql::NPq::NProto::StreamingDisposition disposition;
        UNIT_ASSERT(disposition.ParseFromString(properties.at("streaming_disposition")));
        UNIT_ASSERT(disposition.has_from_time());
        const auto actual = NProtoInterop::CastFromProto(disposition.from_time().timestamp());
        UNIT_ASSERT_GE(actual, before);
        UNIT_ASSERT_LE(actual, after);
    }

    Y_UNIT_TEST_TWIN(StreamingQueryRecoveryForce, Replace) {
        auto kikimr = SetupStreamingSource(/* enableStreamingQueries */ true, /* enableStateRecompute */ true);
        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        auto db = kikimr->GetQueryClient();
        const TString body = " AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO;";
        const auto create = db.ExecuteQuery("CREATE STREAMING QUERY MyQuery WITH (RUN = FALSE)" + body,
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());

        for (const TString& force : {"", ", FORCE = FALSE", ", FORCE = TRUE"}) {
            for (const TString& disposition : {"", ", STREAMING_DISPOSITION = from_checkpoint", ", STREAMING_DISPOSITION = from_checkpoint_force"}) {
                const TString query = TString(Replace
                    ? "CREATE OR REPLACE STREAMING QUERY MyQuery WITH (RUN = FALSE"
                    : "ALTER STREAMING QUERY MyQuery SET (RUN = FALSE") + force + disposition + ")" + body;
                const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString());
                NYql::NPq::NProto::StreamingDisposition expected;
                expected.mutable_from_last_checkpoint()->set_force(disposition.empty()
                    ? force == ", FORCE = TRUE"
                    : disposition == ", STREAMING_DISPOSITION = from_checkpoint_force");
                CheckObjectProperties(runtime, "/Root/MyQuery", {{"streaming_disposition", expected.SerializeAsString()}});
            }
        }
    }

    Y_UNIT_TEST_TWIN(StreamingQueryOutputFromFeatureFlag, Enabled) {
        auto kikimr = SetupStreamingSource(/* enableStreamingQueries */ true, /* enableStateRecompute */ Enabled);
        auto db = kikimr->GetQueryClient();
        const TString body = " AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO;";
        const auto create = db.ExecuteQuery("CREATE STREAMING QUERY MyQuery WITH (RUN = FALSE)" + body,
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());

        for (const TString& prefix : {
            "CREATE STREAMING QUERY NewQuery WITH (",
            "CREATE OR REPLACE STREAMING QUERY MyQuery WITH (",
            "ALTER STREAMING QUERY MyQuery SET (",
        }) {
            const TString query = prefix + "RUN = FALSE, OUTPUT_FROM = Timestamp(\"2025-05-04T11:30:34Z\"))" + body;
            const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), Enabled ? EStatus::SUCCESS : EStatus::GENERIC_ERROR,
                query << "\n" << result.GetIssues().ToString());

            if constexpr (!Enabled) {
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "OUTPUT_FROM is disabled");
            }
        }
    }

    Y_UNIT_TEST(StreamingQueryOutputFromWithReadFrom) {
        auto kikimr = SetupStreamingSource(/* enableStreamingQueries */ true, /* enableStateRecompute */ true);
        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        auto db = kikimr->GetQueryClient();
        const auto timestamp = TInstant::ParseIso8601("2025-05-04T11:30:34.336938Z");
        const std::vector<std::pair<TString, TInstant>> outputTimes = {
            {"Timestamp(\"1970-01-01T00:00:00Z\")", TInstant::Zero()},
            {"$timestamp", timestamp},
            {"Just($timestamp)", timestamp},
            {"$timestamp + Interval(\"PT1S\")", timestamp + TDuration::Seconds(/* s */ 1)},
        };

        for (const bool alter : {false, true}) {
            for (const TString& readFrom : {"", "EARLIEST", "LATEST", "$timestamp - Interval(\"PT2S\")"}) {
                NYql::NPq::NProto::StreamingDisposition expected;

                if (readFrom == "EARLIEST") {
                    expected.mutable_oldest();
                } else if (readFrom == "LATEST") {
                    expected.mutable_fresh();
                } else if (readFrom) {
                    *expected.mutable_from_time()->mutable_timestamp() = NProtoInterop::CastToProto(timestamp - TDuration::Seconds(/* s */ 2));
                }

                for (const auto& [output, outputTime] : outputTimes) {
                    *expected.mutable_output_start_time() = NProtoInterop::CastToProto(outputTime);
                    const TString query = TStringBuilder()
                        << "$timestamp = Timestamp(\"2025-05-04T11:30:34.336938Z\"); "
                        << (alter ? "ALTER STREAMING QUERY MyQuery SET (" : "CREATE OR REPLACE STREAMING QUERY MyQuery WITH (")
                        << "RUN = FALSE, OUTPUT_FROM = " << output
                        << (readFrom ? TString(", READ_FROM = ") + readFrom : TString()) << ")"
                        << (alter ? ";" : " AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO;");
                    const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, query << "\n" << result.GetIssues().ToString());
                    CheckObjectProperties(runtime, "/Root/MyQuery", {
                        {"streaming_disposition", expected.SerializeAsString()},
                        {"run", "false"},
                    });
                }
            }
        }

        const auto before = TInstant::Now() - TDuration::Seconds(/* s */ 1);
        const auto result = db.ExecuteQuery("ALTER STREAMING QUERY MyQuery SET (OUTPUT_FROM = CurrentUtcTimestamp() - Interval(\"PT1S\"));",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        const auto after = TInstant::Now() - TDuration::Seconds(/* s */ 1);
        const auto entry = Navigate(runtime, runtime.AllocateEdgeActor(), "/Root/MyQuery", NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& properties = entry->ResultSet.at(/* pos */ 0).StreamingQueryInfo->Description.GetProperties().GetProperties();
        NYql::NPq::NProto::StreamingDisposition disposition;
        UNIT_ASSERT(disposition.ParseFromString(properties.at("streaming_disposition")));
        UNIT_ASSERT(disposition.has_output_start_time());
        const auto actual = NProtoInterop::CastFromProto(disposition.output_start_time());
        UNIT_ASSERT_GE(actual, before);
        UNIT_ASSERT_LE(actual, after);
    }

    void CheckStreamingQuerySettingError(const TStatus& result, const TString& query, const TString& expectedError,
        EStatus expectedStatus = EStatus::GENERIC_ERROR)
    {
        const TString issues = result.GetIssues().ToString();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, query << "\n" << issues);
        UNIT_ASSERT_C(!HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR), query << "\n" << issues);
        UNIT_ASSERT_C(!HasIssue(result.GetIssues(), NYql::TIssuesIds::UNEXPECTED), query << "\n" << issues);
        UNIT_ASSERT_C(!to_lower(issues).Contains("internal error"), query << "\n" << issues);
        UNIT_ASSERT_C(!issues.Contains("TYqlPanic"), query << "\n" << issues);
        UNIT_ASSERT_C(issues.Contains(expectedError), query << "\nExpected: " << expectedError << "\n" << issues);
    }

    Y_UNIT_TEST(StreamingQueryReadFromValidation) {
        auto kikimr = SetupStreamingSource();
        auto db = kikimr->GetQueryClient();

        for (const bool alter : {false, true}) {
            for (const TString& value : {
                "OLDEST", "FRESH", "\"2025-05-04T11:30:34.336938Z\"", "1234567", "TRUE", "NULL",
                "\"\"", "\"1746358234000000\"", "\"OLDEST\"u",
                "Date(\"2025-05-04\")", "Datetime(\"2025-05-04T11:30:34Z\")", "Interval(\"PT1S\")",
                "Nothing(Timestamp?)", "Just(Just(Timestamp(\"2025-05-04T11:30:34Z\")))",
                "CAST(\"not a timestamp\" AS Timestamp)", "Timestamp(\"1970-01-01T00:00:00Z\") - Interval(\"PT1S\")",
                "Just(1234567)", "Just(\"OLDEST\")", "[Timestamp(\"2025-05-04T11:30:34Z\")]",
                "(FROM_TIME = \"2025-05-04T11:30:34Z\")"
            }) {
                const TString query = TStringBuilder()
                    << (alter ? "ALTER STREAMING QUERY MyQuery SET (" : "CREATE STREAMING QUERY MyQuery WITH (")
                    << "READ_FROM = " << value << ")"
                    << (alter ? ";" : " AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO;");

                for (const auto mode : {NQuery::EExecMode::Explain, NQuery::EExecMode::Execute}) {
                    const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx(),
                        NQuery::TExecuteQuerySettings().ExecMode(mode)).ExtractValueSync();
                    CheckStreamingQuerySettingError(result, query, "Timestamp");
                }
            }

            const TString query = TStringBuilder()
                << (alter ? "ALTER STREAMING QUERY MyQuery SET (" : "CREATE STREAMING QUERY MyQuery WITH (")
                << "READ_FROM = EARLIEST, STREAMING_DISPOSITION = OLDEST)"
                << (alter ? ";" : " AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO;");
            const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
            CheckStreamingQuerySettingError(result, query, "READ_FROM and STREAMING_DISPOSITION are mutually exclusive");
        }
    }

    TString StreamingQueryWithSetting(bool alter, const TString& setting) {
        return TStringBuilder()
            << (alter ? "ALTER STREAMING QUERY MyQuery SET (" : "CREATE STREAMING QUERY MyQuery WITH (")
            << setting << ")"
            << (alter ? ";" : " AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO;");
    }

    Y_UNIT_TEST_TWIN(StreamingQueryOutputFromValidation, Alter) {
        auto kikimr = SetupStreamingSource();
        auto db = kikimr->GetQueryClient();

        for (const TString& value : {
            "EARLIEST", "LATEST", "\"2025-05-04T11:30:34.336938Z\"", "\"1746358234000000\"",
            "1234567", "TRUE", "NULL", "Date(\"2025-05-04\")", "Datetime(\"2025-05-04T11:30:34Z\")",
            "Interval(\"PT1S\")", "Nothing(Timestamp?)", "Just(Just(Timestamp(\"2025-05-04T11:30:34Z\")))",
            "CAST(\"not a timestamp\" AS Timestamp)", "Timestamp(\"1970-01-01T00:00:00Z\") - Interval(\"PT1S\")",
            "Just(1234567)", "[Timestamp(\"2025-05-04T11:30:34Z\")]",
        }) {
            const auto query = StreamingQueryWithSetting(Alter, "OUTPUT_FROM = " + value);

            for (const auto mode : {NQuery::EExecMode::Explain, NQuery::EExecMode::Execute}) {
                const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().ExecMode(mode)).ExtractValueSync();
                CheckStreamingQuerySettingError(result, query, "Timestamp");
            }
        }
    }

    Y_UNIT_TEST(StreamingQuerySettingFilledOptionalLiterals) {
        auto kikimr = SetupStreamingSource();
        auto db = kikimr->GetQueryClient();
        NYql::NPq::NProto::StreamingDisposition disposition;
        disposition.mutable_time_ago()->mutable_duration()->set_seconds(/* value */ 1);

        for (const bool alter : {false, true}) {
            const TString settings = TStringBuilder()
                << "RUN = CAST(\"false\" AS Bool), RESOURCE_POOL = Just(\"my_pool\"" << (alter ? "u" : "") << "), "
                << "STREAMING_DISPOSITION = (TIME_AGO = Just(\"PT1S\"))";
            const auto query = StreamingQueryWithSetting(alter, settings);
            const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, query << "\n" << result.GetIssues().ToString());
            CheckObjectProperties(*kikimr->GetTestServer().GetRuntime(), "/Root/MyQuery", {
                {"run", "false"},
                {"resource_pool", "my_pool"},
                {"streaming_disposition", disposition.SerializeAsString()},
            });
        }
    }

    Y_UNIT_TEST_TWIN(StreamingQuerySettingNestedDispositionValidation, Alter) {
        auto kikimr = SetupStreamingSource();
        auto db = kikimr->GetQueryClient();
        const std::vector<std::pair<TString, TString>> cases = {
            {"STREAMING_DISPOSITION = (READ_FROM = Timestamp(\"2025-05-04T11:30:34Z\"))",
                "Streaming query setting must have type String, Utf8 or Bool"},
            {"STREAMING_DISPOSITION = (STREAMING_DISPOSITION = (TIME_AGO = \"PT1S\"))",
                "Expected data type, but got: Unit"},
            {"RESOURCE_POOL = (VALUE = \"my_pool\")", "Expected data type, but got: Unit"},
        };
        for (const auto& [setting, expectedError] : cases) {
            const auto query = StreamingQueryWithSetting(Alter, setting);

            for (const auto mode : {NQuery::EExecMode::Explain, NQuery::EExecMode::Execute}) {
                const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().ExecMode(mode)).ExtractValueSync();
                CheckStreamingQuerySettingError(result, query, expectedError);
            }
        }
    }

    Y_UNIT_TEST_TWIN(StreamingQuerySettingInvalidDataTypes, Alter) {
        auto kikimr = SetupStreamingSource();
        auto db = kikimr->GetQueryClient();
        const std::vector<std::pair<TString, TString>> cases = {
            {"NULL", "Expected data type, but got: Null"},
            {"Nothing(String?)", "Expected data type, but got: Optional<String>"},
            {"Just(Just(\"my_pool\"))", "Expected data type, but got: Optional<String?>"},
            {"Just([\"my_pool\"])", "Expected data type, but got: Optional<List<String>>"},
            {"[\"my_pool\"]", "Expected data type, but got: List<String>"},
            {"AsTuple()", "Expected data type, but got: Tuple"},
            {"AsTuple(\"my_pool\", FALSE)", "Expected data type, but got: Tuple"},
            {"AsStruct(\"my_pool\" AS pool)", "Expected data type, but got: Struct"},
            {"AsDict(AsTuple(\"pool\", \"my_pool\"))", "Expected data type, but got: Dict"},
            {"42", "Streaming query setting must have type String, Utf8 or Bool"},
            {"Just(42)", "Streaming query setting must have type String, Utf8 or Bool"},
            {"1.5", "Streaming query setting must have type String, Utf8 or Bool"},
            {"Timestamp(\"2025-05-04T11:30:34Z\")", "Streaming query setting must have type String, Utf8 or Bool"},
            {"Just(Timestamp(\"2025-05-04T11:30:34Z\"))", "Streaming query setting must have type String, Utf8 or Bool"},
            {"Interval(\"PT1S\")", "Streaming query setting must have type String, Utf8 or Bool"},
        };
        for (const auto& [value, expectedError] : cases) {
            for (const TString& setting : {"RUN", "RESOURCE_POOL", "STREAMING_DISPOSITION", "FROM_TIME", "TIME_AGO"}) {
                const auto assignment = setting + " = " + value;
                const bool nested = setting == "FROM_TIME" || setting == "TIME_AGO";
                const auto query = StreamingQueryWithSetting(Alter,
                    nested ? "STREAMING_DISPOSITION = (" + assignment + ")" : assignment);

                for (const auto mode : {NQuery::EExecMode::Explain, NQuery::EExecMode::Execute}) {
                    const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx(),
                        NQuery::TExecuteQuerySettings().ExecMode(mode)).ExtractValueSync();
                    CheckStreamingQuerySettingError(result, query, expectedError);
                    UNIT_ASSERT_C(TString(result.GetIssues().ToString()).Contains("At streaming query setting " + setting),
                        query << "\n" << result.GetIssues().ToString());
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(StreamingQuerySettingQueryParameterDependency, Alter) {
        auto kikimr = SetupStreamingSource();
        auto db = kikimr->GetQueryClient();
        const auto params = TParamsBuilder()
            .AddParam("$timestamp").Timestamp(TInstant::ParseIso8601("2025-05-04T11:30:34Z")).Build()
            .AddParam("$pool").String("my_pool").Build()
            .Build();

        for (const TString& setting : {
            "READ_FROM = $timestamp",
            "READ_FROM = $timestamp + Interval(\"PT1S\")",
            "OUTPUT_FROM = $timestamp",
            "OUTPUT_FROM = $timestamp + Interval(\"PT1S\")",
            "RESOURCE_POOL = \"prefix_\" || $pool",
        }) {
            const TString query = TStringBuilder()
                << "DECLARE $timestamp AS Timestamp; DECLARE $pool AS String; "
                << StreamingQueryWithSetting(Alter, setting);

            for (const auto mode : {NQuery::EExecMode::Explain, NQuery::EExecMode::Execute}) {
                const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx(), params,
                    NQuery::TExecuteQuerySettings().ExecMode(mode)).ExtractValueSync();
                CheckStreamingQuerySettingError(result, query, TStringBuilder()
                    << "Cannot evaluate expression that depends on query parameter: "
                    << (setting.StartsWith("RESOURCE_POOL") ? "$pool" : "$timestamp"));
            }
        }
    }

    Y_UNIT_TEST_TWIN(StreamingQuerySettingWorldDependency, Alter) {
        auto kikimr = SetupStreamingSource();
        auto db = kikimr->GetQueryClient();
        const auto createTable = db.ExecuteQuery(R"(
            CREATE TABLE SettingsSource (Key Uint64 NOT NULL, Value Timestamp NOT NULL, PRIMARY KEY (Key));
        )", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(createTable.IsSuccess(), createTable.GetIssues().ToString());
        const auto write = db.ExecuteQuery(R"(
            UPSERT INTO SettingsSource (Key, Value) VALUES (1, Timestamp("2025-05-04T11:30:34Z"));
        )", NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());

        const TString prefix = R"(
            $rows = SELECT Value FROM SettingsSource WHERE Key = 1;
        )";
        const auto read = db.ExecuteQuery(prefix + "SELECT * FROM $rows;",
            NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
        TResultSetParser parser(read.GetResultSet(/* resultIndex */ 0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(/* columnIndex */ 0).GetTimestamp(), TInstant::ParseIso8601("2025-05-04T11:30:34Z"));

        for (const TString& setting : {
            "READ_FROM = Unwrap($rows)",
            "READ_FROM = Unwrap($rows + Interval(\"PT1S\"))",
            "OUTPUT_FROM = Unwrap($rows)",
            "OUTPUT_FROM = Unwrap($rows + Interval(\"PT1S\"))",
            "RESOURCE_POOL = Unwrap(CAST($rows AS String))",
        }) {
            const TString query = prefix + StreamingQueryWithSetting(Alter, setting);

            for (const auto mode : {NQuery::EExecMode::Explain, NQuery::EExecMode::Execute}) {
                const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().ExecMode(mode)).ExtractValueSync();
                CheckStreamingQuerySettingError(result, query, "Only pure expressions are supported");
            }
        }
    }

    Y_UNIT_TEST_TWIN(StreamingQuerySettingInvalidExpressionTypes, Alter) {
        auto kikimr = SetupStreamingSource();
        auto db = kikimr->GetQueryClient();
        const std::vector<std::pair<TString, TString>> cases = {
            {"ListType(Timestamp)", "Expected persistable data, but got: Type<List<Timestamp>>"},
            {"($x) -> { RETURN $x; }", "Lambda is not allowed as argument"},
            {"TypeHandle(Timestamp)", "Expected persistable data, but got: Resource"},
            {"Yql::Iterator([Timestamp(\"2025-05-04T11:30:34Z\")])", "Expected persistable data, but got: Stream<Timestamp>"},
            {"Just(TypeHandle(Timestamp))", "Expected persistable data, but got: Optional<Resource"},
        };
        for (const auto& [value, expectedError] : cases) {
            for (const TString& setting : {"READ_FROM", "OUTPUT_FROM", "RESOURCE_POOL"}) {
                const auto query = StreamingQueryWithSetting(Alter, setting + " = " + value);

                for (const auto mode : {NQuery::EExecMode::Explain, NQuery::EExecMode::Execute}) {
                    const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx(),
                        NQuery::TExecuteQuerySettings().ExecMode(mode)).ExtractValueSync();
                    CheckStreamingQuerySettingError(result, query, expectedError);
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(StreamingQuerySettingEvaluationFailure, Alter) {
        auto kikimr = SetupStreamingSource();
        auto db = kikimr->GetQueryClient();

        for (const TString& setting : {"READ_FROM", "OUTPUT_FROM", "RESOURCE_POOL"}) {
            const auto query = StreamingQueryWithSetting(Alter, setting + R"( =
                Unwrap(CAST("not a timestamp" AS Timestamp), "invalid read-from timestamp"))");

            for (const auto mode : {NQuery::EExecMode::Explain, NQuery::EExecMode::Execute}) {
                const auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().ExecMode(mode)).ExtractValueSync();
                CheckStreamingQuerySettingError(result, query, "invalid read-from timestamp", EStatus::PRECONDITION_FAILED);
            }
        }
    }

    Y_UNIT_TEST(CreateStreamingQueryBasic) {
        auto kikimr = SetupStreamingSource();
        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        auto db = kikimr->GetQueryClient();

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << R"(
                CREATE TABLE test_table (Key Int32 NOT NULL, PRIMARY KEY (Key));
                CREATE STREAMING QUERY `MyFolder/MyStreamingQuery` WITH (
                    RUN = FALSE,
                    RESOURCE_POOL = "my_pool",
                    STREAMING_DISPOSITION = (
                        TIME_AGO = "PT10S"
                    ),
                ) AS DO /*комментарий*/)" << "\r" << R"(BEGIN
PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;
INSERT INTO MySource.MyTopic SELECT /* hint */ * FROM MySource.MyTopic
END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            NYql::NPq::NProto::StreamingDisposition disposition;
            disposition.mutable_time_ago()->mutable_duration()->set_seconds(/* value */ 10);
            CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {
                {"run", "false"},
                {"resource_pool", "my_pool"},
                {"streaming_disposition", disposition.SerializeAsString()},
                {"__query_text", "\nPRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;\nINSERT INTO MySource.MyTopic SELECT /* hint */ * FROM MySource.MyTopic\n"}
            });
        }

        {
            auto schemeClient = kikimr->GetSchemeClient(TCommonClientSettings().AuthToken(BUILTIN_ACL_ROOT));
            const auto result = schemeClient.DescribePath("/Root/MyFolder/MyStreamingQuery").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            const auto& entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "MyStreamingQuery");
            UNIT_ASSERT_VALUES_EQUAL(entry.Owner, BUILTIN_ACL_ROOT);
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, NYdb::NScheme::ESchemeEntryType::StreamingQuery);
        }

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE OR REPLACE STREAMING QUERY `MyFolder/MyStreamingQuery` WITH (
                    RUN = FALSE
                ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            NYql::NPq::NProto::StreamingDisposition disposition;
            disposition.mutable_from_last_checkpoint()->set_force(/* value */ true);
            CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {
                {"run", "false"},
                {"resource_pool", ""},
                {"streaming_disposition", disposition.SerializeAsString()},
                {"__query_text", " INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic "}
            });
        }

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE STREAMING QUERY IF NOT EXISTS `MyFolder/MyStreamingQuery` WITH (
                    RUN = FALSE,
                    RESOURCE_POOL = "other_pool"
                ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT /* other hint */ * FROM MySource.MyTopic END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {
                {"run", "false"},
                {"resource_pool", ""},
                {"__query_text", " INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic "}
            });
        }

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE OR REPLACE STREAMING QUERY IF NOT EXISTS `MyFolder/MyQuery` WITH (
                    RUN = FALSE
                ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT /* third hint */ * FROM MySource.MyTopic END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            NYql::NPq::NProto::StreamingDisposition disposition;
            disposition.mutable_from_last_checkpoint()->set_force(/* value */ true);
            CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {
                {"run", "false"},
                {"resource_pool", ""},
                {"streaming_disposition", disposition.SerializeAsString()},
                {"__query_text", " INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic "}
            });
        }
    }

    void CheckStreamingQueryBodyValidation(TKikimrRunner& kikimr, const TString& prefix) {
        auto db = kikimr.GetQueryClient();

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << prefix << R"()
                AS DO BEGIN
                    $x = 1;
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Streaming query must have at least one streaming read from topic");
        }

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << prefix << R"()
                AS DO BEGIN
                    SELECT 42;
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Results is not allowed for streaming queries, please use INSERT to record the query result");
        }

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << prefix << R"()
                AS DO BEGIN
                    INSERT INTO `MyFolder/MyTable` (Key) VALUES ("1");
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Only UPSERT writing mode is supported for YDB writes inside streaming queries, got mode: INSERT_ABORT");
        }

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << prefix << R"()
                AS DO BEGIN
                    CREATE TABLE `MyFolder/OtherTable` (
                        Key Int32 NOT NULL,
                        PRIMARY KEY (Key)
                    );
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Operations with YDB objects is not allowed inside streaming queries");
        }

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << prefix << R"()
                AS DO BEGIN
                    INSERT INTO MyTable (Key) VALUES ("1")
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Cannot find table 'db.[/Root/MyTable]' because it does not exist or you do not have access permissions.");
        }

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << "$x = \"str\";" << prefix << R"()
                AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT Data || $x FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unknown name: $x");
        }

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << prefix << R"(,
                    RUN = TRUE,
                    RUN = FALSE,
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Found duplicated parameter: RUN");
        }

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << prefix << R"(,
                    PROPERTY_A = C
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unknown property: property_a");
        }

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << prefix << R"(,
                    PROPERTY_A = (
                        PROPERTY_B = C
                    )
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "At streaming query setting PROPERTY_A");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Expected data type, but got: Unit");
        }

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << prefix << R"(,
                    STREAMING_DISPOSITION = SOME_VALUE
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Invalid value for streaming_disposition: 'some_value'");
        }

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << prefix << R"(,
                    STREAMING_DISPOSITION = (
                        FROM_TIME = "2025-05-04T11:30:34.336938Z",
                        TIME_AGO = "PT1H",
                    )
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Invalid value for streaming_disposition: properties 'from_time' and 'time_ago' are mutually exclusive");
        }

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << prefix << R"(,
                    STREAMING_DISPOSITION = (
                        FROM_TIME = "some_value"
                    )
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Invalid value for streaming_disposition: property 'from_time' is not a valid ISO 8601 timestamp: 'some_value'");
        }

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << prefix << R"(,
                    STREAMING_DISPOSITION = (
                        TIME_AGO = "some_value"
                    )
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Invalid value for streaming_disposition: property 'time_ago' is not a valid ISO 8601 duration: 'some_value'");
        }

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << prefix << R"(,
                    STREAMING_DISPOSITION = (
                        TIME_AGO = "PT1H",
                        OTHER_PROPERTY = "some_value",
                    )
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unknown streaming_disposition property: other_property");
        }
    }

    Y_UNIT_TEST(CreateStreamingQueryErrors) {
        auto kikimr = SetupStreamingSource();
        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        auto db = kikimr->GetQueryClient();

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE STREAMING QUERY `MyFolder/MyStreamingQuery` WITH (
                    RUN = FALSE
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
                END DO;

                CREATE TABLE `MyFolder/MyTable` (
                    Key String NOT NULL,
                    PRIMARY KEY (Key)
                );)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {});
        }

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE STREAMING QUERY `MyFolder/MyStreamingQuery` WITH (
                    RUN = FALSE
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT /* hint */ * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Streaming query /Root/MyFolder/MyStreamingQuery already exists");
        }

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE OR REPLACE STREAMING QUERY `MyFolder/MyTable` WITH (
                    RUN = FALSE
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT /* hint */ * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Path /Root/MyFolder/MyTable exists, but it is not a streaming query");
        }

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE STREAMING QUERY IF NOT EXISTS `MyFolder/MyTable` WITH (
                    RUN = FALSE
                ) AS DO BEGIN
                    INSERT INTO MySource.MyTopic SELECT /* hint */ * FROM MySource.MyTopic
                END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Path /Root/MyFolder/MyTable exists, but it is not a streaming query");
        }

        CheckStreamingQueryBodyValidation(*kikimr, "CREATE STREAMING QUERY `MyFolder/OtherQuery` WITH (RUN = FALSE ");
        CheckStreamingQueryBodyValidation(*kikimr, "CREATE STREAMING QUERY `MyFolder/OtherQuery` WITH (RUN = TRUE ");
    }

    bool IsStreamingQueryOperationConflict(TStringBuf issues) {
        return (issues.Contains(" failed StatusPreconditionFailed ")
                && (issues.Contains("(reason: Streaming query already under operation)")
                    || issues.Contains("(reason: fail user constraint in ApplyIf section: path version mistmach,")))
            || (issues.Contains(" failed StatusMultipleModifications ")
                && (issues.Contains(", error: path exists but creating right now (")
                    || issues.Contains(", error: path is under operation (")
                    || issues.Contains(", error: path is being deleted right now (")))
            || issues.Contains("Streaming query info was changed due to multiple modifications inflight")
            || issues.Contains("Streaming query has multiple modifications inflight")
            || (issues.Contains("Lock streaming query failed") && issues.Contains("Transaction locks invalidated"));
    }

    Y_UNIT_TEST(ParallelCreateStreamingQuery) {
        auto kikimr = SetupStreamingSource();
        auto db = kikimr->GetQueryClient();

        constexpr ui64 PARALLEL_QUERIES = 100;
        std::vector<NQuery::TAsyncExecuteQueryResult> results;
        results.reserve(PARALLEL_QUERIES);
        for (ui64 i = 0; i < PARALLEL_QUERIES; ++i) {
            results.emplace_back(db.ExecuteQuery(R"(
                CREATE STREAMING QUERY `MyFolder/MyStreamingQuery` WITH (
                    RUN = FALSE,
                    RESOURCE_POOL = "my_pool"
                ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO)",
                NQuery::TTxControl::NoTx()));
        }

        ui64 successCount = 0;

        for (auto& resultFeature : results) {
            const auto result = resultFeature.ExtractValueSync();
            if (result.GetStatus() == EStatus::SUCCESS) {
                ++successCount;
            } else if (result.GetStatus() == EStatus::SCHEME_ERROR) {
                const auto& issues = result.GetIssues().ToString();
                if (!issues.contains("query /Root/MyFolder/MyStreamingQuery already exists")) {
                    UNIT_FAIL(TStringBuilder() << "Unexpected SCHEME_ERROR error: " << issues);
                }
            } else if (result.GetStatus() == EStatus::PRECONDITION_FAILED) {
                const auto& issues = result.GetIssues().ToString();
                if (!IsStreamingQueryOperationConflict(issues)) {
                    UNIT_FAIL(TStringBuilder() << "Unexpected PRECONDITION_FAILED error: " << issues);
                }
            } else {
                UNIT_FAIL(TStringBuilder() << "Unexpected result status: " << result.GetStatus() << ", issues: " << result.GetIssues().ToOneLineString());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(successCount, 1);
        CheckObjectProperties(*kikimr->GetTestServer().GetRuntime(), "/Root/MyFolder/MyStreamingQuery", {
            {"run", "false"},
            {"resource_pool", "my_pool"},
            {"__query_text", " INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic "}
        });
    }

    Y_UNIT_TEST(AlterStreamingQueryBasic) {
        auto kikimr = SetupStreamingSource();
        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        auto db = kikimr->GetQueryClient();

        {
            const auto result = db.ExecuteQuery(TStringBuilder() << R"(
                CREATE TABLE test_table (Key Int32 NOT NULL, PRIMARY KEY (Key));
                CREATE STREAMING QUERY `MyFolder/MyStreamingQuery` WITH (
                    RUN = FALSE,
                    RESOURCE_POOL = "my_pool"
                ) AS DO /*комментарий*/)" << "\r" << R"(BEGIN
PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;
INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic
END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {
                {"run", "false"},
                {"resource_pool", "my_pool"},
                {"__query_text", "\nPRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;\nINSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic\n"}
            });
        }

        {
            const auto result = db.ExecuteQuery(R"(
                ALTER STREAMING QUERY `MyFolder/MyStreamingQuery` SET (
                    FORCE = TRUE
                ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {
                {"run", "false"},
                {"resource_pool", "my_pool"},
                {"__query_text", " INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic "}
            });
        }

        {
            const auto now = TInstant::Now();
            const auto result = db.ExecuteQuery(TStringBuilder() << R"(
                ALTER STREAMING QUERY `MyFolder/MyStreamingQuery` SET (
                    RESOURCE_POOL = "other_pool",
                    STREAMING_DISPOSITION = (
                        FROM_TIME = ")" << now.ToString() << R"("
                    ),
                );)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            NYql::NPq::NProto::StreamingDisposition disposition;
            *disposition.mutable_from_time()->mutable_timestamp() = NProtoInterop::CastToProto(now);
            CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {
                {"run", "false"},
                {"resource_pool", "other_pool"},
                {"streaming_disposition", disposition.SerializeAsString()},
                {"__query_text", " INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic "}
            });
        }

        {
            CheckObjectNotFound(runtime, "/Root/OtherFolder/MyStreamingQuery");

            const auto result = db.ExecuteQuery(R"(
                ALTER STREAMING QUERY IF EXISTS `OtherFolder/MyStreamingQuery` SET (
                    RESOURCE_POOL = "other_pool"
                );)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {
                {"run", "false"},
                {"resource_pool", "other_pool"},
                {"__query_text", " INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic "}
            });
        }
    }

    Y_UNIT_TEST(AlterStreamingQueryErrors) {
        auto kikimr = SetupStreamingSource();
        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        auto db = kikimr->GetQueryClient();

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE STREAMING QUERY `MyFolder/OtherQuery` WITH (
                    RUN = FALSE
                ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO;

                CREATE TABLE `MyFolder/MyTable` (
                    Key String NOT NULL,
                    PRIMARY KEY (Key)
                );)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            CheckObjectProperties(runtime, "/Root/MyFolder/OtherQuery", {});
            CheckObjectNotFound(runtime, "/Root/MyFolder/MyStreamingQuery");
        }

        {
            const auto result = db.ExecuteQuery(R"(
                ALTER STREAMING QUERY `MyFolder/MyStreamingQuery` SET (
                    RUN = FALSE
                );)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::NOT_FOUND, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Streaming query /Root/MyFolder/MyStreamingQuery not found or you don't have access permissions");
        }

        {
            const auto result = db.ExecuteQuery(R"(
                ALTER STREAMING QUERY `MyFolder/MyTable` SET (
                    RUN = FALSE
                );)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Path /Root/MyFolder/MyTable exists, but it is not a streaming query");
        }

        {
            const auto result = db.ExecuteQuery(R"(
                ALTER STREAMING QUERY IF EXISTS `MyFolder/MyTable` SET (
                    RUN = FALSE
                );)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Path /Root/MyFolder/MyTable exists, but it is not a streaming query");
        }

        CheckStreamingQueryBodyValidation(*kikimr, "ALTER STREAMING QUERY `MyFolder/OtherQuery` SET (FORCE = TRUE, RUN = FALSE ");
        CheckStreamingQueryBodyValidation(*kikimr, "ALTER STREAMING QUERY `MyFolder/OtherQuery` SET (FORCE = TRUE, RUN = TRUE ");
    }

    Y_UNIT_TEST(ParallelAlterStreamingQuery) {
        auto kikimr = SetupStreamingSource();
        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        auto db = kikimr->GetQueryClient();

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE STREAMING QUERY `MyFolder/MyStreamingQuery` WITH (
                    RUN = FALSE,
                    RESOURCE_POOL = "my_pool"
                ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {
                {"run", "false"},
                {"resource_pool", "my_pool"},
                {"__query_text", " INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic "}
            });
        }

        constexpr ui64 PARALLEL_QUERIES = 100;
        std::vector<NQuery::TAsyncExecuteQueryResult> results;
        results.reserve(PARALLEL_QUERIES);
        for (ui64 i = 0; i < PARALLEL_QUERIES; ++i) {
            results.emplace_back(db.ExecuteQuery(R"(
                ALTER STREAMING QUERY `MyFolder/MyStreamingQuery` SET (
                    FORCE = TRUE,
                    RESOURCE_POOL = "other_pool"
                ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT /* hint */ * FROM MySource.MyTopic END DO)",
                NQuery::TTxControl::NoTx()));
        }

        ui64 successCount = 0;

        for (auto& resultFeature : results) {
            const auto result = resultFeature.ExtractValueSync();
            if (result.GetStatus() == EStatus::SUCCESS) {
                ++successCount;
            } else if (result.GetStatus() == EStatus::PRECONDITION_FAILED) {
                const auto& issues = result.GetIssues().ToString();
                if (!IsStreamingQueryOperationConflict(issues)) {
                    UNIT_FAIL(TStringBuilder() << "Unexpected PRECONDITION_FAILED error: " << issues);
                }
            } else {
                UNIT_FAIL(TStringBuilder() << "Unexpected result status: " << result.GetStatus() << ", issues: " << result.GetIssues().ToOneLineString());
            }
        }

        UNIT_ASSERT_GE(successCount, 1);
        CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {
            {"run", "false"},
            {"resource_pool", "other_pool"},
            {"__query_text", " INSERT INTO MySource.MyTopic SELECT /* hint */ * FROM MySource.MyTopic "}
        });
    }

    Y_UNIT_TEST(DropStreamingQueryBasic) {
        auto kikimr = SetupStreamingSource();
        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        auto db = kikimr->GetQueryClient();

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE STREAMING QUERY `MyFolder/MyStreamingQuery` WITH (
                    RUN = FALSE
                ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {});
        }

        {
            const auto result = db.ExecuteQuery(R"(
                DROP STREAMING QUERY `MyFolder/MyStreamingQuery`;)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            CheckObjectNotFound(runtime, "/Root/MyFolder/MyStreamingQuery");
        }

        {
            const auto result = db.ExecuteQuery(R"(
                DROP STREAMING QUERY IF EXISTS `MyFolder/MyStreamingQuery`;)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            CheckObjectNotFound(runtime, "/Root/MyFolder/MyStreamingQuery");
        }
    }

    Y_UNIT_TEST(DropStreamingQueryErrors) {
        auto kikimr = SetupStreamingSource();
        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        auto db = kikimr->GetQueryClient();

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE TABLE `MyFolder/MyTable` (
                    Key Int32 NOT NULL,
                    PRIMARY KEY (Key)
                );)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            CheckObjectNotFound(runtime, "/Root/MyFolder/MyStreamingQuery");
        }

        {
            const auto result = db.ExecuteQuery(R"(
                DROP STREAMING QUERY `MyFolder/MyStreamingQuery`;)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::NOT_FOUND, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Streaming query /Root/MyFolder/MyStreamingQuery not found or you don't have access permissions");
        }

        {
            const auto result = db.ExecuteQuery(R"(
                DROP STREAMING QUERY `MyFolder/MyTable`;)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Path /Root/MyFolder/MyTable exists, but it is not a streaming query");
        }

        {
            const auto result = db.ExecuteQuery(R"(
                DROP STREAMING QUERY IF EXISTS `MyFolder/MyTable`;)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Path /Root/MyFolder/MyTable exists, but it is not a streaming query");
        }
    }

    Y_UNIT_TEST(ParallelDropStreamingQuery) {
        auto kikimr = SetupStreamingSource();
        auto& runtime = *kikimr->GetTestServer().GetRuntime();
        auto db = kikimr->GetQueryClient();

        {
            const auto result = db.ExecuteQuery(R"(
                CREATE STREAMING QUERY `MyFolder/MyStreamingQuery` WITH (
                    RUN = FALSE
                ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO)",
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            CheckObjectProperties(runtime, "/Root/MyFolder/MyStreamingQuery", {});
        }

        constexpr ui64 PARALLEL_QUERIES = 100;
        std::vector<NQuery::TAsyncExecuteQueryResult> results;
        results.reserve(PARALLEL_QUERIES);
        for (ui64 i = 0; i < PARALLEL_QUERIES; ++i) {
            results.emplace_back(db.ExecuteQuery(R"(
                DROP STREAMING QUERY `MyFolder/MyStreamingQuery`;)",
                NQuery::TTxControl::NoTx(), NoRetryExecuteQuerySettings()));
        }

        ui64 successCount = 0;

        for (auto& resultFeature : results) {
            const auto result = resultFeature.ExtractValueSync();
            if (result.GetStatus() == EStatus::SUCCESS) {
                ++successCount;
            } else if (result.GetStatus() == EStatus::NOT_FOUND) {
                const auto& issues = result.GetIssues().ToString();
                if (!issues.contains("Streaming query /Root/MyFolder/MyStreamingQuery not found or you don't have access permissions") &&
                    !issues.contains("Path `/Root/MyFolder/MyStreamingQuery` does not exist")) {
                    UNIT_FAIL(TStringBuilder() << "Unexpected NOT_FOUND error: " << issues);
                }
            } else if (result.GetStatus() == EStatus::PRECONDITION_FAILED) {
                const auto& issues = result.GetIssues().ToString();
                if (!IsStreamingQueryOperationConflict(issues)) {
                    UNIT_FAIL(TStringBuilder() << "Unexpected PRECONDITION_FAILED error: " << issues);
                }
            } else {
                UNIT_FAIL(TStringBuilder() << "Unexpected result status: " << result.GetStatus() << ", issues: " << result.GetIssues().ToOneLineString());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(successCount, 1);
        CheckObjectNotFound(runtime, "/Root/MyFolder/MyStreamingQuery");
    }

    Y_UNIT_TEST(StreamingQueriesAclValidation) {
        auto kikimr = SetupStreamingSource();
        auto db = kikimr->GetQueryClient();

        constexpr char createUser[] = "create@builtin";
        constexpr char removeUser[] = "remove@builtin";
        constexpr char describeUser[] = "describe@builtin";
        constexpr char alterUser[] = "alter@builtin";
        constexpr char emptyUser[] = "empty@builtin";

        {
            const auto result = db.ExecuteQuery(fmt::format(R"(
                GRANT ALL ON `/Root/MySource` TO `{create_user}`, `{remove_user}`, `{describe_user}`, `{alter_user}`, `{empty_user}`;
                GRANT CREATE TABLE ON `/Root` TO `{create_user}`;
                GRANT REMOVE SCHEMA, DESCRIBE SCHEMA ON `/Root` TO `{remove_user}`;
                GRANT DESCRIBE SCHEMA ON `/Root` TO `{describe_user}`;
                GRANT ALTER SCHEMA, DESCRIBE SCHEMA ON `/Root` TO `{alter_user}`;)",
                "create_user"_a = createUser,
                "remove_user"_a = removeUser,
                "describe_user"_a = describeUser,
                "alter_user"_a = alterUser,
                "empty_user"_a = emptyUser),
                NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }

        const auto checkNotFound = [&](const char* sql, const char* user) {
            const auto result = kikimr->GetQueryClient(NQuery::TClientSettings().AuthToken(user))
                .ExecuteQuery(sql, NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::NOT_FOUND, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "not found or you don't have access permissions");
        };

        const auto checkAccessDenied = [&](const char* sql, const char* user, const TString& error) {
            const auto result = kikimr->GetQueryClient(NQuery::TClientSettings().AuthToken(user))
                .ExecuteQuery(sql, NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAUTHORIZED, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), error);
        };

        const auto checkSuccess = [&](const char* sql, const char* user) {
            const auto result = kikimr->GetQueryClient(NQuery::TClientSettings().AuthToken(user))
                .ExecuteQuery(sql, NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        };

        {   // Test create permissions
            constexpr char sql[] = R"(
                CREATE STREAMING QUERY MyStreamingQuery WITH (
                    RUN = FALSE
                ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO)";

            checkAccessDenied(sql, emptyUser, "Access denied");
            checkAccessDenied(sql, removeUser, "Access denied");
            checkAccessDenied(sql, describeUser, "Access denied");
            checkAccessDenied(sql, alterUser, "Access denied");
            checkSuccess(sql, createUser);
        }

        {   // Test describe permissions
            const auto checkDescribeAccessDenied = [&](const char* user) {
                const auto result = NYdb::NScheme::TSchemeClient(kikimr->GetDriver(), TCommonClientSettings().AuthToken(user))
                    .DescribePath("/Root/MyStreamingQuery").ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAUTHORIZED, result.GetIssues().ToOneLineString());
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Access denied");
            };

            const auto checkDescribeSuccess = [&](const char* user) {
                const auto result = NYdb::NScheme::TSchemeClient(kikimr->GetDriver(), TCommonClientSettings().AuthToken(user))
                    .DescribePath("/Root/MyStreamingQuery").ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            };

            checkDescribeAccessDenied(emptyUser);
            checkDescribeSuccess(createUser);
            checkDescribeSuccess(describeUser);
            checkDescribeSuccess(removeUser);
            checkDescribeSuccess(alterUser);
        }

        {   // Test alter permissions
            constexpr char sql[] = R"(
                ALTER STREAMING QUERY MyStreamingQuery SET (
                    RUN = TRUE
                ))";

            checkNotFound(sql, emptyUser);
            checkAccessDenied(sql, removeUser, "You don't have access permissions for streaming query /Root/MyStreamingQuery");
            checkAccessDenied(sql, describeUser, "You don't have access permissions for streaming query /Root/MyStreamingQuery");
            checkSuccess(sql, alterUser);
            checkSuccess(sql, createUser);
        }

        {   // Test remove permissions
            constexpr char sql[] = "DROP STREAMING QUERY MyStreamingQuery";

            checkNotFound(sql, emptyUser);
            checkAccessDenied(sql, alterUser, "You don't have access permissions for streaming query /Root/MyStreamingQuery");
            checkAccessDenied(sql, describeUser, "You don't have access permissions for streaming query /Root/MyStreamingQuery");
            checkSuccess(sql, removeUser);
        }
    }

    Y_UNIT_TEST(StreamingQueriesOnServerless) {
        auto ydb = NWorkloadManager::TYdbSetupSettings()
            .CreateSampleTenants(/* value */ true)
            .Create();

        const auto& tenantName = ydb->GetSettings().GetServerlessTenantName();
        const auto settings = NWorkloadManager::TQueryRunnerSettings()
            .PoolId("")
            .Database(tenantName)
            .NodeIndex(ydb->GetServerlessTenantInfo().NodeIdx);

        NWorkloadManager::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(fmt::format(R"(
                CREATE TOPIC MyTopic;
                CREATE EXTERNAL DATA SOURCE MySource WITH (
                    SOURCE_TYPE = "Ydb",
                    LOCATION = "localhost:{port}",
                    DATABASE_NAME = "{database}",
                    AUTH_METHOD = "NONE"
                );
            )",
            "port"_a = ydb->GetGrpcPort(),
            "database"_a = tenantName
        ), settings));

        NWorkloadManager::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(R"(
            CREATE STREAMING QUERY MyStreamingQuery WITH (
                RUN = TRUE
            ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic END DO
        )", settings));

        const auto queryName = TStringBuilder() << tenantName << "/MyStreamingQuery";
        TString queryText = " INSERT INTO MySource.MyTopic SELECT * FROM MySource.MyTopic ";
        CheckObjectProperties(*ydb->GetRuntime(), queryName, {
            {"run", "true"},
            {"resource_pool", ""},
            {"__query_text", queryText}
        });

        const auto checkSysView = [&](const TString& text, bool expectExistance = true, const TString& filter = "") {
            const auto& result = ydb->ExecuteQuery(
                TStringBuilder() << "SELECT * FROM `.sys/streaming_queries` " << filter
            , settings);
            NWorkloadManager::TSampleQueries::CheckSuccess(result);

            UNIT_ASSERT_VALUES_EQUAL(result.ResultSets.size(), 1);
            NYdb::TResultSetParser resultParser(result.ResultSets[0]);

            UNIT_ASSERT_VALUES_EQUAL(resultParser.RowsCount(), expectExistance);
            UNIT_ASSERT_VALUES_EQUAL(resultParser.ColumnsCount(), 22);

            if (expectExistance) {
                UNIT_ASSERT(resultParser.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(*resultParser.ColumnParser("Path").GetOptionalUtf8(), queryName);
                UNIT_ASSERT_VALUES_EQUAL(*resultParser.ColumnParser("Status").GetOptionalUtf8(), "RUNNING");
                UNIT_ASSERT_VALUES_EQUAL(*resultParser.ColumnParser("Issues").GetOptionalUtf8(), "{}");
                UNIT_ASSERT_STRING_CONTAINS(*resultParser.ColumnParser("Plan").GetOptionalUtf8(), "Write MySource");
                UNIT_ASSERT_STRING_CONTAINS(*resultParser.ColumnParser("Ast").GetOptionalUtf8(), "/Root/test-serverless/MySource");
                UNIT_ASSERT_VALUES_EQUAL(*resultParser.ColumnParser("Text").GetOptionalUtf8(), text);
                UNIT_ASSERT_VALUES_EQUAL(*resultParser.ColumnParser("Run").GetOptionalBool(), true);
                UNIT_ASSERT_VALUES_EQUAL(*resultParser.ColumnParser("ResourcePool").GetOptionalUtf8(), "");
                UNIT_ASSERT_VALUES_EQUAL(*resultParser.ColumnParser("RetryCount").GetOptionalUint64(), 0);
                UNIT_ASSERT(!resultParser.ColumnParser("LastFailAt").GetOptionalTimestamp());
                UNIT_ASSERT(!resultParser.ColumnParser("SuspendedUntil").GetOptionalTimestamp());
                UNIT_ASSERT(!resultParser.ColumnParser("LastExecutionId").GetOptionalUtf8()->empty());
                UNIT_ASSERT_STRING_CONTAINS(*resultParser.ColumnParser("PreviousExecutionIds").GetOptionalUtf8(), "[");
            }
        };

        Sleep(TDuration::Seconds(/* s */ 2));
        checkSysView(queryText);
        checkSysView(queryText, /* expectExistance */ true, TStringBuilder() << "WHERE Path = '" << queryName << "'");
        checkSysView(queryText, /* expectExistance */ true, TStringBuilder() << "WHERE Path >= '" << queryName << "'");
        checkSysView(queryText, /* expectExistance */ true, TStringBuilder() << "WHERE Path <= '" << queryName << "'");
        checkSysView(queryText, /* expectExistance */ false, TStringBuilder() << "WHERE Path > '" << queryName << "'");
        checkSysView(queryText, /* expectExistance */ false, TStringBuilder() << "WHERE Path < '" << queryName << "'");

        NWorkloadManager::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(R"(
            ALTER STREAMING QUERY MyStreamingQuery SET (
                FORCE = TRUE
            ) AS DO BEGIN INSERT INTO MySource.MyTopic SELECT /* hint */ * FROM MySource.MyTopic END DO
        )", settings));

        queryText = " INSERT INTO MySource.MyTopic SELECT /* hint */ * FROM MySource.MyTopic ";
        CheckObjectProperties(*ydb->GetRuntime(), queryName, {
            {"run", "true"},
            {"resource_pool", ""},
            {"__query_text", queryText}
        });

        Sleep(TDuration::Seconds(/* s */ 2));
        checkSysView(queryText);

        NWorkloadManager::TSampleQueries::CheckSuccess(ydb->ExecuteQuery(R"(
            DROP STREAMING QUERY MyStreamingQuery
        )", settings));

        CheckObjectNotFound(*ydb->GetRuntime(), queryName);
        checkSysView(queryText, /* expectExistance */ false);
    }
}

} // namespace NKikimr::NKqp
