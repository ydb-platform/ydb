#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/testlib/common_helper.h>

#include <library/cpp/threading/local_executor/local_executor.h>

#include <util/generic/serialized_enum.h>
#include <util/string/printf.h>

#include <format>
namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpConstraints) {

    static NKikimrPQ::TPQConfig DefaultPQConfig() {
        NKikimrPQ::TPQConfig pqConfig;
        pqConfig.SetEnabled(true);
        pqConfig.SetEnableProtoSourceIdInfo(true);
        pqConfig.SetTopicsAreFirstClassCitizen(true);
        pqConfig.SetRequireCredentialsInNewProtocol(false);
        pqConfig.AddClientServiceType()->SetName("data-streams");
        return pqConfig;
    }

    Y_UNIT_TEST(CreateTableSerialTypeForbidden) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/SerialTableDisabled` (
                    Key Serial,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR,
                                       result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AddSerialColumnForbidden) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/SerialTableCreateAndAlter` (
                    Key Int32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/SerialTableCreateAndAlter` ADD COLUMN SeqNo Serial;
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR,
                                       result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateTableWithDefaultForbidden) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(false);
        auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/CreateAndAlterDefaultsDisabled` (
                    Key Int32,
                    Value String Default "empty",
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR,
                                       result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AddColumnWithDefaultForbidden) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(false);
        auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/SerialTableCreateAndAlter` (
                    Key Int32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/SerialTableCreateAndAlter` ADD COLUMN SeqNo DEFAULT 5;
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR,
                                       result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(SerialTypeNegative1) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/SerialTableNeg1` (
                    Key SerialUnknown,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR,
                                       result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(SerialTypeNegative2) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/SerialTableNeg2` (
                    Key Uint32,
                    Value Serial,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST,
                                       result.GetIssues().ToString());
        }
    }


    void TestSerialType(TString serialType) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(true);
        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = Sprintf(R"(
                --!syntax_v1
                CREATE TABLE `/Root/SerialTable%s` (
                    Key %s,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )", serialType.c_str(), serialType.c_str());

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        {
            TString query = Sprintf(R"(
                UPSERT INTO `/Root/SerialTable%s` (Value) VALUES ("New");
            )", serialType.c_str());

            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result =
                session
                    .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                                      execSettings)
                    .ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }
        {
            TString query = Sprintf(R"(
                SELECT * FROM `/Root/SerialTable%s`;
            )", serialType.c_str());

            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result =
                session
                    .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                                      execSettings)
                    .ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
            CompareYson(R"(
                    [
                        [1;["New"]]
                    ]
                )",
                NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(SerialTypeSmallSerial) {
        TestSerialType("SmallSerial");
    }

    Y_UNIT_TEST(SerialTypeSerial2) {
        TestSerialType("serial2");
    }

    Y_UNIT_TEST(SerialTypeSerial) {
        TestSerialType("Serial");
    }

    Y_UNIT_TEST(SerialTypeSerial4) {
        TestSerialType("Serial4");
    }

    Y_UNIT_TEST(SerialTypeBigSerial) {
        TestSerialType("BigSerial");
    }

    Y_UNIT_TEST(SerialTypeSerial8) {
        TestSerialType("serial8");
    }

    Y_UNIT_TEST(DefaultsAndDeleteAndUpdate) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(true);
        auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/DefaultsAndDeleteAndUpdate` (
                    Key Int32,
                    Value String DEFAULT "somestring",
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        {
            TString query = R"(
                UPSERT INTO `/Root/DefaultsAndDeleteAndUpdate` (Key) VALUES (1), (2), (3), (4);
            )";

            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result =
                session
                    .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                                      execSettings)
                    .ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        {
            TString query = R"(
                SELECT * FROM `/Root/DefaultsAndDeleteAndUpdate`;
            )";

            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result =
                session
                    .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                                      execSettings)
                    .ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
            CompareYson(R"(
                    [
                        [[1];["somestring"]];
                        [[2];["somestring"]];
                        [[3];["somestring"]];
                        [[4];["somestring"]]
                    ]
                )",
                NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            TString query = R"(
                $object_pk = <|Key:1|>;
                DELETE FROM `/Root/DefaultsAndDeleteAndUpdate` ON
                SELECT * FROM AS_TABLE(AsList($object_pk));
            )";

            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result =
                session
                    .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                                      execSettings)
                    .ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterTableAddColumnWithDefaultValue) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableFeatureFlags()->SetEnableAddColumsWithDefaults(true);
        auto serverSettings = TKikimrSettings().SetAppConfig(appConfig);
        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/SerialTableCreateAndAlter` (
                    Key Int32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/SerialTableCreateAndAlter` ADD COLUMN Exists bool DEFAULT false;
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DefaultValuesForTable) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(true);
        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TableWithDefaults` (
                    Key Int32 Default 7,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        {
            TString query = R"(
                UPSERT INTO `/Root/TableWithDefaults` (Value) VALUES ("New");
            )";

            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result =
                session
                    .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                                      execSettings)
                    .ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }
        {
            TString query = R"(
                SELECT * FROM `/Root/TableWithDefaults`;
            )";

            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result =
                session
                    .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                                      execSettings)
                    .ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
            CompareYson(R"(
                    [
                        [[7];["New"]]
                    ]
                )",
                NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DefaultValuesForTableNegative2) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(true);
        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TableWithDefaults2` (
                    Key Int32 Default 1,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DefaultValuesForTableNegative3) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(true);
        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TableWithDefaults` (
                    Key Int32 NOT NULL Default null,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                                        "Default expr Key is nullable or optional, but column has not null constraint");
        }
    }

    Y_UNIT_TEST(DefaultValuesForTableNegative4) {

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(true);

        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TableWithDefaults` (
                    Key Uint32 Default "someText",
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR,
                                       result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(IndexedTableAndNotNullColumn) {

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(true);

        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/AlterTableAddNotNullColumn` (
                    Key Uint32,
                    Value String,
                    Value2 Int32 NOT NULL DEFAULT 1,
                    PRIMARY KEY (Key),
                    INDEX ByValue GLOBAL ON (Value) COVER (Value2)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        auto fQuery = [&](TString query) -> TString {
            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result =
                session
                    .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                                      execSettings)
                    .ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
            if (result.GetResultSets().size() > 0)
                return NYdb::FormatResultSetYson(result.GetResultSet(0));
            return "";
        };

        fQuery(R"(
            UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (1, "Old");
        )");

        auto fCompareTable = [&](TString expected) {
            TString query = R"(
                SELECT * FROM `/Root/AlterTableAddNotNullColumn` ORDER BY Key;
            )";
            CompareYson(expected, fQuery(query));
        };

        fCompareTable(R"(
            [
                [[1u];["Old"];1]
            ]
        )");

        fQuery(R"(
            INSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (2, "New");
        )");

        fCompareTable(R"(
            [
                [[1u];["Old"];1];[[2u];["New"];1]
            ]
        )");


        fQuery(R"(
            UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value, Value2) VALUES (2, "New", 2);
        )");

        fCompareTable(R"(
            [
                [[1u];["Old"];1];[[2u];["New"];2]
            ]
        )");

        fQuery(R"(
            UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (2, "OldNew");
        )");

        fQuery(R"(
            UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (3, "BrandNew");
        )");

        fCompareTable(R"(
            [
                [[1u];["Old"];1];[[2u];["OldNew"];2];[[3u];["BrandNew"];1]
            ]
        )");

        CompareYson(
            fQuery("SELECT Value, Value2 FROM `/Root/AlterTableAddNotNullColumn` VIEW ByValue WHERE Value = \"OldNew\""),
            R"(
            [
                [["OldNew"];2]
            ]
            )"
        );

        CompareYson(
            fQuery("SELECT Value, Value2 FROM `/Root/AlterTableAddNotNullColumn` VIEW ByValue WHERE Value = \"BrandNew\""),
            R"(
            [
                [["BrandNew"];1]
            ]
            )"
        );

    }

    Y_UNIT_TEST(AddNonColumnDoesnotReturnInternalError) {

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(true);
        appConfig.MutableFeatureFlags()->SetEnableAddColumsWithDefaults(true);

        TKikimrRunner kikimr(TKikimrSettings().SetUseRealThreads(false).SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); } );
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); } );
        auto querySession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); } );

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/AddNonColumnDoesnotReturnInternalError` (
                    Key Uint32 NOT NULL,
                    Value String  NOT NULL,
                    Value2 String NOT NULL DEFAULT "text",
                    PRIMARY KEY (Key)
                );
            )";

            auto result = kikimr.RunCall([&]{ return session.ExecuteSchemeQuery(query).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        auto fQuery = [&](TString query) -> TString {
            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result = kikimr.RunCall([&] {
                return querySession
                    .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                                      execSettings)
                    .ExtractValueSync(); } );

            if (result.GetStatus() == EStatus::SUCCESS) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                           result.GetIssues().ToString());
                if (result.GetResultSets().size() > 0)
                    return NYdb::FormatResultSetYson(result.GetResultSet(0));
                return "";
            } else {
                return TStringBuilder() << result.GetStatus() << ": " << result.GetIssues().ToString();
            }
        };

        fQuery(R"(
            UPSERT INTO `/Root/AddNonColumnDoesnotReturnInternalError` (Key, Value) VALUES (1, "Old");
        )");

        fQuery(R"(
            UPDATE `/Root/AddNonColumnDoesnotReturnInternalError` SET Value2="Updated" WHERE Key=1;
        )");

        auto fCompareTable = [&](TString expected) {
            TString query = R"(
                SELECT * FROM `/Root/AddNonColumnDoesnotReturnInternalError` ORDER BY Key;
            )";
            CompareYson(expected, fQuery(query));
        };

        fCompareTable(R"(
            [
                [1u;"Old";"Updated"]
            ]
        )");

        auto alterQuery = R"(
            --!syntax_v1
            ALTER TABLE `/Root/AddNonColumnDoesnotReturnInternalError` ADD COLUMN Value3 Int32 NOT NULL DEFAULT 7;
        )";

        bool enabledCapture = true;
        TVector<TAutoPtr<IEventHandle>> delayedUpsertRows;
        auto grab = [&delayedUpsertRows, &enabledCapture](TAutoPtr<IEventHandle>& ev) -> auto {
            if (enabledCapture && ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvUploadRowsRequest::EventType) {
                delayedUpsertRows.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&delayedUpsertRows](IEventHandle&) {
            return delayedUpsertRows.size() > 0;
        });

        runtime.SetObserverFunc(grab);

        auto alterFuture = kikimr.RunInThreadPool([&] { return session.ExecuteSchemeQuery(alterQuery).GetValueSync(); });

        runtime.DispatchEvents(opts);
        Y_VERIFY_S(delayedUpsertRows.size() > 0, "no upload rows requests");

        fQuery(R"(
            UPSERT INTO `/Root/AddNonColumnDoesnotReturnInternalError`
                (Key, Value) VALUES (2, "New");
        )");

        fQuery(R"(
            UPSERT INTO `/Root/AddNonColumnDoesnotReturnInternalError`
                (Key, Value) VALUES (1, "Changed");
        )");

        {

            TString result = fQuery(R"(
                SELECT Key, Value2, Value3 FROM `/Root/AddNonColumnDoesnotReturnInternalError`;
            )");

            Cerr << result << Endl;
            UNIT_ASSERT_STRING_CONTAINS(result, "GENERIC_ERROR");
            UNIT_ASSERT_STRING_CONTAINS(result, "Member not found: Value3. Did you mean Value");
        }

        {
            TString result = fQuery(R"(
                UPSERT INTO `/Root/AddNonColumnDoesnotReturnInternalError`
                    (Key, Value, Value2, Value3) VALUES (1, "4", "four", 1);
            )");

            Cerr << result << Endl;
            UNIT_ASSERT_STRING_CONTAINS(result, "BAD_REQUEST");
            UNIT_ASSERT_STRING_CONTAINS(result, "is under build operation");
        }

        {
            TString result = fQuery(R"(
                UPDATE `/Root/AddNonColumnDoesnotReturnInternalError` SET Value3=1 WHERE Key=1;
            )");

            Cerr << result << Endl;
            UNIT_ASSERT_STRING_CONTAINS(result, "BAD_REQUEST");
            UNIT_ASSERT_STRING_CONTAINS(result, "is under the build operation");
        }

        {
            TString result = fQuery(R"(
                DELETE FROM `/Root/AddNonColumnDoesnotReturnInternalError` WHERE Value3=7;
            )");

            Cerr << result << Endl;
            UNIT_ASSERT_STRING_CONTAINS(result, "GENERIC_ERROR");
            UNIT_ASSERT_STRING_CONTAINS(result, "Member not found");
        }

        fCompareTable(R"(
            [
                [1u;"Changed";"Updated"];
                [2u;"New";"text"]
            ]
        )");

        enabledCapture = false;
        for (const auto& ev: delayedUpsertRows) {
            runtime.Send(ev);
        }

        auto result = runtime.WaitFuture(alterFuture);

        fCompareTable(R"(
            [
                [1u;"Changed";"Updated";7];
                [2u;"New";"text";7]
            ]
        )");
    }

    Y_UNIT_TEST(IndexedTableAndNotNullColumnAddNotNullColumn) {

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(true);
        appConfig.MutableFeatureFlags()->SetEnableAddColumsWithDefaults(true);

        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/AlterTableAddNotNullColumn` (
                    Key Uint32,
                    Value String,
                    Value2 Int32 NOT NULL DEFAULT 1,
                    PRIMARY KEY (Key),
                    INDEX ByValue GLOBAL ON (Value) COVER (Value2)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        auto fQuery = [&](TString query) -> TString {
            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result =
                session
                    .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                                      execSettings)
                    .ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
            if (result.GetResultSets().size() > 0)
                return NYdb::FormatResultSetYson(result.GetResultSet(0));
            return "";
        };

        fQuery(R"(
            UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (1, "Old");
        )");

        auto fCompareTable = [&](TString expected) {
            TString query = R"(
                SELECT * FROM `/Root/AlterTableAddNotNullColumn` ORDER BY Key;
            )";
            CompareYson(expected, fQuery(query));
        };

        fCompareTable(R"(
            [
                [[1u];["Old"];1]
            ]
        )");

        fQuery(R"(
            INSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (2, "New");
        )");

        fCompareTable(R"(
            [
                [[1u];["Old"];1];[[2u];["New"];1]
            ]
        )");

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/AlterTableAddNotNullColumn` ADD COLUMN Value3 Int32 NOT NULL DEFAULT 7;
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        fCompareTable(R"(
            [
                [[1u];["Old"];1;7];[[2u];["New"];1;7]
            ]
        )");

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/AlterTableAddNotNullColumn` ADD COLUMN Value4 Int8 NOT NULL DEFAULT Int8("-24");
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

         fCompareTable(R"(
            [
                [[1u];["Old"];1;7;-24];[[2u];["New"];1;7;-24]
            ]
        )");

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/AlterTableAddNotNullColumn` ADD COLUMN Value5 Int8 NOT NULL DEFAULT -25;
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

         fCompareTable(R"(
            [
                [[1u];["Old"];1;7;-24;-25];[[2u];["New"];1;7;-24;-25]
            ]
        )");

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/AlterTableAddNotNullColumn` ADD COLUMN Value6 Float NOT NULL DEFAULT Float("1.0");
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

         fCompareTable(R"(
            [
                [[1u];["Old"];1;7;-24;-25;1.];[[2u];["New"];1;7;-24;-25;1.]
            ]
        )");

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/AlterTableAddNotNullColumn` ADD COLUMN Value7 Double NOT NULL DEFAULT Double("1.0");
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

         fCompareTable(R"(
            [
                [[1u];["Old"];1;7;-24;-25;1.;1.];[[2u];["New"];1;7;-24;-25;1.;1.]
            ]
        )");

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/AlterTableAddNotNullColumn` ADD COLUMN Value8 Yson NOT NULL DEFAULT Yson("[123]");
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

         fCompareTable(R"(
            [
                [[1u];["Old"];1;7;-24;-25;1.;1.;"[123]"];[[2u];["New"];1;7;-24;-25;1.;1.;"[123]"]
            ]
        )");

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/AlterTableAddNotNullColumn` ADD COLUMN Value9 Json NOT NULL DEFAULT Json("{\"age\" : 22}");
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

         fCompareTable(R"(
            [
                [[1u];["Old"];1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}"];[[2u];["New"];1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}"]
            ]
        )");

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/AlterTableAddNotNullColumn` ADD COLUMN Valuf1 Decimal(22,9) NOT NULL DEFAULT Decimal("1.11", 22, 9);
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

         fCompareTable(R"(
            [
                [[1u];["Old"];1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}";"1.11"];[[2u];["New"];1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}";"1.11"]
            ]
        )");

    }

    Y_UNIT_TEST(Utf8AndDefault) {

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(true);
        appConfig.MutableFeatureFlags()->SetEnableAddColumsWithDefaults(true);

        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/Utf8AndDefault` (
                    Key Uint32,
                    Value Utf8 NOT NULL DEFAULT Utf8("Simple"),
                    Value3 Utf8 NOT NULL DEFAULT CAST("Impossibru" as Utf8),
                    PRIMARY KEY (Key),
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        auto fQuery = [&](TString query) -> TString {
            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result =
                session
                    .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                                      execSettings)
                    .ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
            if (result.GetResultSets().size() > 0)
                return NYdb::FormatResultSetYson(result.GetResultSet(0));
            return "";
        };

        fQuery(R"(
            UPSERT INTO `/Root/Utf8AndDefault` (Key) VALUES (1);
        )");

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/Utf8AndDefault` ADD COLUMN Value2 Utf8 NOT NULL DEFAULT Utf8("Hard");
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/Utf8AndDefault` ADD COLUMN Value4 Utf8 NOT NULL DEFAULT CAST("Value4" as Utf8);
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        auto fCompareTable = [&](TString expected) {
            TString query = R"(
                SELECT * FROM `/Root/Utf8AndDefault` ORDER BY Key;
            )";
            CompareYson(expected, fQuery(query));
        };

        fCompareTable(R"(
            [
                [[1u];"Simple";"Hard";"Impossibru";"Value4"]
            ]
        )");
    }

    Y_UNIT_TEST(AlterTableAddNotNullWithDefault) {

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(true);

        appConfig.MutableFeatureFlags()->SetEnableAddColumsWithDefaults(true);

        TKikimrRunner kikimr(TKikimrSettings().SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/AlterTableAddNotNullColumn` (
                    Key Uint32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        auto fQuery = [&](TString query) -> TString {
            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result =
                session
                    .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                                      execSettings)
                    .ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
            if (result.GetResultSets().size() > 0)
                return NYdb::FormatResultSetYson(result.GetResultSet(0));
            return "";
        };

        fQuery(R"(
            UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (1, "Old");
        )");

        auto fCompareTable = [&](TString expected) {
            TString query = R"(
                SELECT * FROM `/Root/AlterTableAddNotNullColumn` ORDER BY Key;
            )";
            CompareYson(expected, fQuery(query));
        };

        fCompareTable(R"(
            [
                [[1u];["Old"]]
            ]
        )");

        {
            auto query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/AlterTableAddNotNullColumn` ADD COLUMN Value2 Int32 NOT NULL DEFAULT 1;
            )";

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        Sleep(TDuration::Seconds(3));

        fQuery(R"(
            INSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (2, "New");
        )");

        fCompareTable(R"(
            [
                [[1u];["Old"];1];[[2u];["New"];1]
            ]
        )");


        fQuery(R"(
            UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value, Value2) VALUES (2, "New", 2);
        )");

        fCompareTable(R"(
            [
                [[1u];["Old"];1];[[2u];["New"];2]
            ]
        )");

        fQuery(R"(
            UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (2, "OldNew");
        )");

        fQuery(R"(
            UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (3, "BrandNew");
        )");

        fCompareTable(R"(
            [
                [[1u];["Old"];1];[[2u];["OldNew"];2];[[3u];["BrandNew"];1]
            ]
        )");

    }

    Y_UNIT_TEST(SetNotNull) {
        struct TValue {
        private:
            int _value = 0;
            bool _isNull = false;
        public:
            TValue(int value) : _value(value) {}
            TValue() : _isNull(true) {}

            int GetValue() const {
                assert(!_isNull);
                return _value;
            }

            bool IsNull() const {
                return _isNull;
            }

            std::string ToString() const {
                if (IsNull()) {
                    return "NULL";
                }

                return std::to_string(GetValue());
            }
        };


        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
        appConfig.MutableFeatureFlags()->SetEnableChangeNotNullConstraint(true);

        TKikimrRunner kikimr(TKikimrSettings().SetUseRealThreads(false).SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); } );
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); } );
        auto querySession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); } );

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        {
            auto createTable = R"sql(
                --!syntax_v1
                CREATE TABLE `/Root/SetNotNull` (
                    myKey Int32 NOT NULL,
                    myValue Int32 DEFAULT(0),
                    PRIMARY KEY (myKey)
                );
            )sql";

            auto result = kikimr.RunCall([&]{ return session.ExecuteSchemeQuery(createTable).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto insertValues = [&](TValue key, TValue val) -> std::pair<EStatus, TString> {
            auto query = TString(std::format(R"sql(
                --!syntax_v1
                UPSERT INTO `/Root/SetNotNull` (myKey, myValue)
                VALUES
                ( {}, {} );
            )sql", key.ToString(), val.ToString()));

            NYdb::NTable::TExecDataQuerySettings execSettings;
            execSettings.KeepInQueryCache(true);
            execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

            auto result = kikimr.RunCall([&] {
                return querySession
                    .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                                      execSettings)
                    .ExtractValueSync(); } );

            return {result.GetStatus(), result.GetIssues().ToString()};
        };

        {
            const int countOfLines = 10;
            for (int i = 1; i <= countOfLines; i++) {
                auto key = TValue(i);
                auto value = (i != countOfLines ? TValue(10 * i) : TValue());
                auto [status, issue] = insertValues(key, value);
                UNIT_ASSERT_VALUES_EQUAL_C(status, EStatus::SUCCESS, issue);
            }
        }

        auto setNotNull = R"sql(
            --!syntax_v1
            ALTER TABLE `/Root/SetNotNull` ALTER COLUMN myValue SET NOT NULL;
        )sql";

        bool enabledCapture = true;
        TVector<TAutoPtr<IEventHandle>> delayedCheckingColumns;
        auto grab = [&delayedCheckingColumns, &enabledCapture](TAutoPtr<IEventHandle>& ev) -> auto {
            if (enabledCapture && ev->GetTypeRewrite() == TEvDataShard::TEvCheckConstraintCreateRequest::EventType) {
                delayedCheckingColumns.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&delayedCheckingColumns](IEventHandle&) {
            return delayedCheckingColumns.size() > 0;
        });

        runtime.SetObserverFunc(grab);

        auto setNotNullFuture = kikimr.RunInThreadPool([&] { return session.ExecuteSchemeQuery(setNotNull).GetValueSync(); });

        runtime.DispatchEvents(opts);
        Y_VERIFY_S(delayedCheckingColumns.size() > 0, "no upload rows requests");

        {
            auto key = TValue(201);
            auto value = TValue();
            auto [status, issue] = insertValues(key, value);
            UNIT_ASSERT_VALUES_EQUAL_C(status, EStatus::BAD_REQUEST, issue);
            UNIT_ASSERT_STRING_CONTAINS(issue, "All not null columns should be initialized");
        }

        enabledCapture = false;
        for (const auto& ev: delayedCheckingColumns) {
            runtime.Send(ev);
        }

        auto result = runtime.WaitFuture(setNotNullFuture);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    }
}
} // namespace NKikimr::NKqp
