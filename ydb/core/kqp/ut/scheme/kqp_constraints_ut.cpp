#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpConstraints) {
    Y_UNIT_TEST(SerialTypeNegative1) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/SerialTableNeg1` (
                    Key SerialUnknown,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unknown simple type 'SerialUnknown'");
        }
    }

    Y_UNIT_TEST(SerialTypeForNonKeyColumn) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/SerialTable` (
                    Key Uint32,
                    Value Serial,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                UPSERT INTO `/Root/SerialTable` (Key) VALUES (1);
            )";

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TString query = R"(
                SELECT * FROM `/Root/SerialTable`;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1u];1]
            ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            TString query = R"(
                ALTER TABLE `/Root/SerialTable`
                DROP COLUMN Value;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Can't drop serial column: 'Value'");
        }

        {
            TString query = R"(
                ALTER TABLE `/Root/SerialTable`
                ALTER COLUMN Value DROP NOT NULL;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Cannot alter serial column 'Value'");
        }

        {
            TString query = R"(
                ALTER TABLE `/Root/SerialTable`
                ADD FAMILY Family2 (
                     DATA = "test",
                     COMPRESSION = "off"
                ),
                ALTER COLUMN Value SET FAMILY Family2;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Cannot alter serial column 'Value'");
        }
    }


    void TestSerialType(TString serialType) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = Sprintf(R"(
                CREATE TABLE `/Root/SerialTable%s` (
                    Key %s,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )", serialType.c_str(), serialType.c_str());

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TString query = Sprintf(R"(
                UPSERT INTO `/Root/SerialTable%s` (Value) VALUES ("New");
            )", serialType.c_str());

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TString query = Sprintf(R"(
                SELECT * FROM `/Root/SerialTable%s`;
            )", serialType.c_str());

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [1;["New"]]
            ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(SerialTypeSmallSerial) {
        TestSerialType("SmallSerial");
    }

    Y_UNIT_TEST(SerialTypeSerial2) {
        TestSerialType("Serial2");
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
        TestSerialType("Serial8");
    }

    Y_UNIT_TEST(AddSerialColumnForbidden) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/SerialTableCreateAndAlter` (
                    Key Int32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/SerialTableCreateAndAlter`
                ADD COLUMN SeqNo Serial;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Column addition with serial data type is unsupported");
        }
    }

    Y_UNIT_TEST(DropCreateSerial) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/SerialTable` (
                    Key Serial,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TString query = R"(
                UPSERT INTO `/Root/SerialTable` (Value) VALUES ("New");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TString query = R"(
                SELECT * FROM `/Root/SerialTable`;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                DROP TABLE `/Root/SerialTable`;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                CREATE TABLE `/Root/SerialTable` (
                    Key Serial,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TString query = R"(
                UPSERT INTO `/Root/SerialTable` (Value) VALUES ("New");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TString query = Sprintf(R"(
                SELECT * FROM `/Root/SerialTable`;
            )");

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [1;["New"]]
            ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DefaultsAndDeleteAndUpdate) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/DefaultsAndDeleteAndUpdate` (
                    Key Int32,
                    Value String DEFAULT "somestring",
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TString query = R"(
                UPSERT INTO `/Root/DefaultsAndDeleteAndUpdate` (Key) VALUES (1), (2), (3), (4);
            )";

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TString query = R"(
                SELECT * FROM `/Root/DefaultsAndDeleteAndUpdate`;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1];["somestring"]];
                [[2];["somestring"]];
                [[3];["somestring"]];
                [[4];["somestring"]]
            ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            TString query = R"(
                $object_pk = <|Key:1|>;
                DELETE FROM `/Root/DefaultsAndDeleteAndUpdate` ON
                SELECT * FROM AS_TABLE(AsList($object_pk));
            )";

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AddColumnWithDefaultForbidden) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetEnableAddColumsWithDefaults(false)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/SerialTableCreateAndAlter` (
                    Key Int32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/SerialTableCreateAndAlter`
                ADD COLUMN SeqNo Int32 DEFAULT 5;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Adding columns with defaults is disabled");
        }
    }

    Y_UNIT_TEST(AlterTableAddColumnWithDefaultValue) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetEnableAddColumsWithDefaults(true)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            const auto query = R"(
                CREATE TABLE `/Root/CreateAndAlterDefault` (
                    Key Int32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = R"(
                UPSERT INTO `/Root/CreateAndAlterDefault` (Key, Value)
                VALUES (1, "One"), (2, "Two"), (3, "Three");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = R"(
                ALTER TABLE `/Root/CreateAndAlterDefault`
                ADD COLUMN DefaultValue bool DEFAULT false;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = R"(
                SELECT Key, Value, DefaultValue FROM `/Root/CreateAndAlterDefault` ORDER BY Key;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1];["One"];[%false]];
                [[2];["Two"];[%false]];
                [[3];["Three"];[%false]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const auto query = R"(
                UPSERT INTO `/Root/CreateAndAlterDefault` (Key, Value, DefaultValue)
                VALUES (4, "Four", true);

                UPSERT INTO `/Root/CreateAndAlterDefault` (Key, Value)
                VALUES (5, "Five");

                UPSERT INTO `/Root/CreateAndAlterDefault` (Key, Value, DefaultValue)
                VALUES (6, "Six", false);
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = R"(
                SELECT Key, Value, DefaultValue FROM `/Root/CreateAndAlterDefault` ORDER BY Key;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1];["One"];[%false]];
                [[2];["Two"];[%false]];
                [[3];["Three"];[%false]];
                [[4];["Four"];[%true]];
                [[5];["Five"];[%false]];
                [[6];["Six"];[%false]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DefaultValuesForTable) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/TableWithDefaults` (
                    Key Int32 Default 7,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TString query = R"(
                UPSERT INTO `/Root/TableWithDefaults` (Value) VALUES ("New");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TString query = R"(
                SELECT * FROM `/Root/TableWithDefaults`;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[7];["New"]]
            ])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(DefaultValuesForTableNegative2) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/TableWithDefaults2` (
                    Key Int32 Default 1,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DefaultValuesForTableNegative3) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/TableWithDefaults` (
                    Key Int32 NOT NULL Default NULL,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Default expr Key is nullable or optional, but column has not null constraint");
        }
    }

    Y_UNIT_TEST(DefaultValuesForTableNegative4) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/TableWithDefaults` (
                    Key Uint32 Default "someText",
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Default expr Key type mismatch, expected: Uint32, actual: String");
        }
    }

    Y_UNIT_TEST_TWIN(IndexedTableAndNotNullColumn, StreamIndex) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(StreamIndex);
        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/AlterTableAddNotNullColumn` (
                    Key Uint32,
                    Value String,
                    Value2 Int32 NOT NULL DEFAULT 1,
                    PRIMARY KEY (Key),
                    INDEX ByValue GLOBAL ON (Value) COVER (Value2)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto fQuery = [&](TString query) -> TString {
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            if (result.GetResultSets().empty()) {
                return "";
            }

            return FormatResultSetYson(result.GetResultSet(0));
        };

        auto fCompareTable = [&](TString expected) {
            CompareYson(expected, fQuery(R"(
                SELECT * FROM `/Root/AlterTableAddNotNullColumn` ORDER BY Key;
            )"));
        };

        {
            fQuery(R"(
                UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (1, "Old");
            )");

            fCompareTable(R"([
                [[1u];["Old"];1]
            ])");
        }

        {
            fQuery(R"(
                INSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (2, "New");
            )");

            fCompareTable(R"([
                [[1u];["Old"];1];
                [[2u];["New"];1]
            ])");
        }

        {
            fQuery(R"(
                UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value, Value2) VALUES (2, "New", 2);
            )");

            fCompareTable(R"([
                [[1u];["Old"];1];
                [[2u];["New"];2]
            ])");
        }

        {
            fQuery(R"(
                UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (2, "OldNew");
            )");

            fQuery(R"(
                UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (3, "BrandNew");
            )");

            fCompareTable(R"([
                [[1u];["Old"];1];
                [[2u];["OldNew"];2];
                [[3u];["BrandNew"];1]
            ])");
        }

        CompareYson(fQuery("SELECT Value, Value2 FROM `/Root/AlterTableAddNotNullColumn` VIEW ByValue WHERE Value = \"OldNew\""), R"([
            [["OldNew"];2]
        ])");

        CompareYson(fQuery("SELECT Value, Value2 FROM `/Root/AlterTableAddNotNullColumn` VIEW ByValue WHERE Value = \"BrandNew\""), R"([
            [["BrandNew"];1]
        ])");
    }

    Y_UNIT_TEST(IndexAutoChooseAndNonReadyIndex) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetUseRealThreads(false)
            .SetWithSampleTables(false));

        auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); } );
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); } );
        auto querySession = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); } );

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        {
            auto query = R"(
                CREATE TABLE `/Root/IndexChooseAndNonReadyIndex` (
                    Key Uint32 NOT NULL,
                    Value String  NOT NULL,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = kikimr.RunCall([&]{ return session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                                       result.GetIssues().ToString());
        }

        auto fQuery = [&](TString query) -> TString {
            auto result = kikimr.RunCall([&] {
                return querySession.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            });

            if (result.GetStatus() == EStatus::SUCCESS) {
                if (result.GetResultSets().empty()) {
                    return "";
                }

                return FormatResultSetYson(result.GetResultSet(0));
            }

            return TStringBuilder() << result.GetStatus() << ": " << result.GetIssues().ToString();
        };

        auto fCompareTable = [&](TString expected) {
            CompareYson(expected, fQuery(R"(
                SELECT * FROM `/Root/IndexChooseAndNonReadyIndex` WHERE Value = "Old";
            )"));
        };

        {
            fQuery(R"(
                UPSERT INTO `/Root/IndexChooseAndNonReadyIndex` (Key, Value) VALUES (1, "Old");
            )");

            fCompareTable(R"([
                [1u;"Old"]
            ])");
        }

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

        auto alterQuery = R"(
            ALTER TABLE `/Root/IndexChooseAndNonReadyIndex`
            ADD INDEX Index GLOBAL ON (Value);
        )";

        auto alterFuture = kikimr.RunInThreadPool([&] { return session.ExecuteQuery(alterQuery, TTxControl::NoTx()).GetValueSync(); });

        runtime.DispatchEvents(opts);
        Y_VERIFY_S(delayedUpsertRows.size() > 0, "no upload rows requests");

        fCompareTable(R"([
            [1u;"Old"]
        ])");

        enabledCapture = false;
        for (const auto& ev: delayedUpsertRows) {
            runtime.Send(ev);
        }

        auto result = runtime.WaitFuture(alterFuture);
        fCompareTable(R"([
            [1u;"Old"]
        ])");
    }

    Y_UNIT_TEST(AddNonColumnDoesnotReturnInternalError) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetUseRealThreads(false)
            .SetEnableAddColumsWithDefaults(true)
            .SetDisableMissingDefaultColumnsInBulkUpsert(true)
            .SetWithSampleTables(false));

        auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); } );
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); } );
        auto querySession = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); } );

        auto tableClient = kikimr.RunCall([&] { return kikimr.GetTableClient(); } );

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        {
            auto query = R"(
                CREATE TABLE `/Root/AddNonColumnDoesnotReturnInternalError` (
                    Key Uint32 NOT NULL,
                    Value String  NOT NULL,
                    Value2 String NOT NULL DEFAULT "text",
                    PRIMARY KEY (Key)
                );
            )";

            auto result = kikimr.RunCall([&]{ return session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync(); });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto fQuery = [&](TString query) -> TString {
            auto result = kikimr.RunCall([&] {
                return querySession.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            });

            if (result.GetStatus() == EStatus::SUCCESS) {
                if (result.GetResultSets().empty()) {
                    return "";
                }

                return FormatResultSetYson(result.GetResultSet(0));
            }

            return TStringBuilder() << result.GetStatus() << ": " << result.GetIssues().ToString();
        };

        auto fCompareTable = [&](TString expected) {
            CompareYson(expected, fQuery(R"(
                SELECT * FROM `/Root/AddNonColumnDoesnotReturnInternalError` ORDER BY Key;
            )"));
        };

        {
            fQuery(R"(
                UPSERT INTO `/Root/AddNonColumnDoesnotReturnInternalError` (Key, Value) VALUES (1, "Old");
            )");

            fQuery(R"(
                UPDATE `/Root/AddNonColumnDoesnotReturnInternalError` SET Value2="Updated" WHERE Key=1;
            )");

            fCompareTable(R"([
                [1u;"Old";"Updated"]
            ])");
        }

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

        auto alterQuery = R"(
            ALTER TABLE `/Root/AddNonColumnDoesnotReturnInternalError`
            ADD COLUMN Value3 Int32 DEFAULT 7;
        )";

        auto alterFuture = kikimr.RunInThreadPool([&] { return session.ExecuteQuery(alterQuery, TTxControl::NoTx()).GetValueSync(); });

        runtime.DispatchEvents(opts);
        Y_VERIFY_S(delayedUpsertRows.size() > 0, "no upload rows requests");

        fQuery(R"(
            UPSERT INTO `/Root/AddNonColumnDoesnotReturnInternalError` (Key, Value) VALUES (2, "New");
        )");

        fQuery(R"(
            UPSERT INTO `/Root/AddNonColumnDoesnotReturnInternalError` (Key, Value) VALUES (1, "Changed");
        )");

        {
            TString result = fQuery(R"(
                SELECT Key, Value2, Value3 FROM `/Root/AddNonColumnDoesnotReturnInternalError`;
            )");

            UNIT_ASSERT_STRING_CONTAINS(result, "GENERIC_ERROR");
            UNIT_ASSERT_STRING_CONTAINS(result, "Member not found: Value3. Did you mean Value");
        }

        {
            TString result = fQuery(R"(
                PRAGMA OrderedColumns;
                SELECT * FROM `/Root/AddNonColumnDoesnotReturnInternalError`;
            )");

            UNIT_ASSERT_STRING_CONTAINS(result, R"([[1u;"Changed";"Updated"];[2u;"New";"text"]])");
        }

        {
            auto rowsBuilder = NYdb::TValueBuilder();
            rowsBuilder.BeginList();
            for (ui32 i = 10; i <= 15; ++i) {
                rowsBuilder.AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .Uint32(i)
                    .AddMember("Value")
                        .String("String")
                    .AddMember("Value2")
                        .String("String2")
                    .AddMember("Value3")
                        .Int32(10)
                    .EndStruct();

            }
            rowsBuilder.EndList();
            auto result = kikimr.RunCall([&] {
                return tableClient.BulkUpsert("/Root/AddNonColumnDoesnotReturnInternalError", rowsBuilder.Build()).GetValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Column Value3 is under build operation");
        }

        {
            TString result = fQuery(R"(
                UPSERT INTO `/Root/AddNonColumnDoesnotReturnInternalError` (Key, Value, Value2, Value3) VALUES (1, "4", "four", 1);
            )");

            UNIT_ASSERT_STRING_CONTAINS(result, "BAD_REQUEST");
            UNIT_ASSERT_STRING_CONTAINS(result, "is under build operation");
        }

        {
            TString result = fQuery(R"(
                UPDATE `/Root/AddNonColumnDoesnotReturnInternalError` SET Value3 = 1 WHERE Key = 1;
            )");

            UNIT_ASSERT_STRING_CONTAINS(result, "BAD_REQUEST");
            UNIT_ASSERT_STRING_CONTAINS(result, "is under the build operation");
        }

        {
            TString result = fQuery(R"(
                DELETE FROM `/Root/AddNonColumnDoesnotReturnInternalError` WHERE Value3 = 7;
            )");

            UNIT_ASSERT_STRING_CONTAINS(result, "GENERIC_ERROR");
            UNIT_ASSERT_STRING_CONTAINS(result, "Member not found");
        }

        fCompareTable(R"([
            [1u;"Changed";"Updated"];
            [2u;"New";"text"]
        ])");

        enabledCapture = false;
        for (const auto& ev: delayedUpsertRows) {
            runtime.Send(ev);
        }

        {
            auto rowsBuilder = NYdb::TValueBuilder();
            rowsBuilder.BeginList();
            for (ui32 i = 10; i <= 12; ++i) {
                rowsBuilder.AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .Uint32(i)
                    .AddMember("Value")
                        .String("String")
                    .AddMember("Value2")
                        .String("String2")
                    .EndStruct();

            }
            rowsBuilder.EndList();
            auto result = kikimr.RunCall([&] {
                return tableClient.BulkUpsert("/Root/AddNonColumnDoesnotReturnInternalError", rowsBuilder.Build()).GetValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Missing default columns: Value3");
        }

        runtime.WaitFuture(alterFuture);

        {
            auto rowsBuilder = NYdb::TValueBuilder();
            rowsBuilder.BeginList();
            for (ui32 i = 10; i <= 15; ++i) {
                rowsBuilder.AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .Uint32(i)
                    .AddMember("Value")
                        .String("String")
                    .AddMember("Value2")
                        .String("String2")
                    .AddMember("Value3")
                        .Int32(10)
                    .EndStruct();

            }
            rowsBuilder.EndList();
            auto result = kikimr.RunCall([&] {
                return tableClient.BulkUpsert("/Root/AddNonColumnDoesnotReturnInternalError", rowsBuilder.Build()).GetValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        fCompareTable(R"([
            [1u;"Changed";"Updated";[7]];
            [2u;"New";"text";[7]];
            [10u;"String";"String2";[10]];
            [11u;"String";"String2";[10]];
            [12u;"String";"String2";[10]];
            [13u;"String";"String2";[10]];
            [14u;"String";"String2";[10]];
            [15u;"String";"String2";[10]];
        ])");
    }

    Y_UNIT_TEST(IndexedTableAndNotNullColumnAddNotNullColumn) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetEnableAddColumsWithDefaults(true)
            .SetEnableParameterizedDecimal(true)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/AlterTableAddNotNullColumn` (
                    Key Uint32,
                    Value String,
                    Value2 Int32 NOT NULL DEFAULT 1,
                    PRIMARY KEY (Key),
                    INDEX ByValue GLOBAL ON (Value) COVER (Value2)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto fQuery = [&](TString query) -> TString {
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            if (result.GetResultSets().empty()) {
                return "";
            }

            return FormatResultSetYson(result.GetResultSet(0));
        };

        auto fCompareTable = [&](TString expected) {
            CompareYson(expected, fQuery(R"(
                SELECT * FROM `/Root/AlterTableAddNotNullColumn` ORDER BY Key;
            )"));
        };

        {
            fQuery(R"(
                UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (1, "Old");
            )");

            fCompareTable(R"([
                [[1u];["Old"];1]
            ])");
        }

        {
            fQuery(R"(
                INSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (2, "New");
            )");

            fCompareTable(R"([
                [[1u];["Old"];1];
                [[2u];["New"];1]
            ])");
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/AlterTableAddNotNullColumn`
                ADD COLUMN Value3 Int32 NOT NULL DEFAULT 7;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            fCompareTable(R"([
                [[1u];["Old"];1;7];
                [[2u];["New"];1;7]
            ])");
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/AlterTableAddNotNullColumn`
                ADD COLUMN Value4 Int8 NOT NULL DEFAULT Int8("-24");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            fCompareTable(R"([
                [[1u];["Old"];1;7;-24];
                [[2u];["New"];1;7;-24]
            ])");
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/AlterTableAddNotNullColumn`
                ADD COLUMN Value5 Int8 NOT NULL DEFAULT -25;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            fCompareTable(R"([
                [[1u];["Old"];1;7;-24;-25];
                [[2u];["New"];1;7;-24;-25]
            ])");
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/AlterTableAddNotNullColumn`
                ADD COLUMN Value6 Float NOT NULL DEFAULT Float("1.0");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            fCompareTable(R"([
                [[1u];["Old"];1;7;-24;-25;1.];
                [[2u];["New"];1;7;-24;-25;1.]
            ])");
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/AlterTableAddNotNullColumn`
                ADD COLUMN Value7 Double NOT NULL DEFAULT Double("1.0");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            fCompareTable(R"([
                [[1u];["Old"];1;7;-24;-25;1.;1.];
                [[2u];["New"];1;7;-24;-25;1.;1.]
            ])");
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/AlterTableAddNotNullColumn`
                ADD COLUMN Value8 Yson NOT NULL DEFAULT Yson("[123]");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            fCompareTable(R"([
                [[1u];["Old"];1;7;-24;-25;1.;1.;"[123]"];
                [[2u];["New"];1;7;-24;-25;1.;1.;"[123]"]
            ])");
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/AlterTableAddNotNullColumn`
                ADD COLUMN Value9 Json NOT NULL DEFAULT Json("{\"age\" : 22}");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            fCompareTable(R"([
                [[1u];["Old"];1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}"];
                [[2u];["New"];1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}"]
            ])");
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/AlterTableAddNotNullColumn`
                ADD COLUMN Valuf1 Decimal(22,9) NOT NULL DEFAULT Decimal("1.11", 22, 9);
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            fCompareTable(R"([
                [[1u];["Old"];1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}";"1.11"];
                [[2u];["New"];1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}";"1.11"]
            ])");
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/AlterTableAddNotNullColumn`
                ADD COLUMN Valuf35 Decimal(35,10) NOT NULL DEFAULT Decimal("155555555555555.11", 35, 10);
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            fCompareTable(R"([
                [[1u];["Old"];1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}";"1.11";"155555555555555.11"];
                [[2u];["New"];1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}";"1.11";"155555555555555.11"]
            ])");
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/AlterTableAddNotNullColumn`
                ADD COLUMN Value10 Int16 NOT NULL DEFAULT Int16("-213");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            fCompareTable(R"([
                [[1u];["Old"];-213;1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}";"1.11";"155555555555555.11"];
                [[2u];["New"];-213;1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}";"1.11";"155555555555555.11"]
            ])");
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/AlterTableAddNotNullColumn`
                ADD COLUMN Value11 Uint16 NOT NULL DEFAULT 213;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            fCompareTable(R"([
                [[1u];["Old"];-213;213u;1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}";"1.11";"155555555555555.11"];
                [[2u];["New"];-213;213u;1;7;-24;-25;1.;1.;"[123]";"{\"age\" : 22}";"1.11";"155555555555555.11"]
            ])");
        }
    }

    Y_UNIT_TEST(DefaultAndIndexesTestDefaultColumnNotIncludedInIndex) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE test (
                    A Int64 NOT NULL,
                    B Int64,
                    Created Int32 DEFAULT 1,
                    Deleted Int32 DEFAULT 0,
                    PRIMARY KEY (A ),
                    INDEX testIndex GLOBAL ON (B, A)
                )
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto fQuery = [&](TString query) -> TString {
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            if (result.GetResultSets().empty()) {
                return "";
            }

            return FormatResultSetYson(result.GetResultSet(0));
        };

        fQuery(R"(
            UPSERT INTO test (A, B, Created, Deleted) VALUES (5, 15, 1, 0)
        )");

        fQuery(R"(
            $to_upsert = (
                SELECT A FROM `test`
                WHERE A = 5
            );

            UPSERT INTO `test` (A, Deleted)
            SELECT A, 10 AS Deleted FROM $to_upsert;
        )");

        CompareYson(R"([
            [5;[15];[1];[10]]
        ])", fQuery(R"(
            SELECT A, B, Created, Deleted FROM `test` ORDER BY A;
        )"));
    }

    Y_UNIT_TEST(Utf8AndDefault) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetEnableAddColumsWithDefaults(true)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/Utf8AndDefault` (
                    Key Uint32,
                    Value Utf8 NOT NULL DEFAULT Utf8("Simple"),
                    Value3 Utf8 NOT NULL DEFAULT CAST("Impossibru" as Utf8),
                    PRIMARY KEY (Key),
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto fQuery = [&](TString query) -> TString {
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,result.GetIssues().ToString());

            if (result.GetResultSets().empty()) {
                return "";
            }

            return FormatResultSetYson(result.GetResultSet(0));
        };

        auto fCompareTable = [&](TString expected) {
            CompareYson(expected, fQuery(R"(
                SELECT * FROM `/Root/Utf8AndDefault` ORDER BY Key;
            )"));
        };

        fQuery(R"(
            UPSERT INTO `/Root/Utf8AndDefault` (Key) VALUES (1);
        )");

        {
            auto query = R"(
                ALTER TABLE `/Root/Utf8AndDefault`
                ADD COLUMN Value2 Utf8 NOT NULL DEFAULT Utf8("Hard");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/Utf8AndDefault`
                ADD COLUMN Value4 Utf8 NOT NULL DEFAULT CAST("Value4" as Utf8);
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        fCompareTable(R"([
            [[1u];"Simple";"Hard";"Impossibru";"Value4"]
        ])");

        {
            fQuery(R"(
                UPSERT INTO `/Root/Utf8AndDefault` (Key) VALUES (2);
            )");

            fCompareTable(R"([
                [[1u];"Simple";"Hard";"Impossibru";"Value4"];
                [[2u];"Simple";"Hard";"Impossibru";"Value4"]
            ])");
        }
    }

    Y_UNIT_TEST(AlterTableAddNotNullWithDefault) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetEnableAddColumsWithDefaults(true)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/AlterTableAddNotNullColumn` (
                    Key Uint32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto fQuery = [&](TString query) -> TString {
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            if (result.GetResultSets().empty()) {
                return "";
            }

            return FormatResultSetYson(result.GetResultSet(0));
        };

        auto fCompareTable = [&](TString expected) {
            CompareYson(expected, fQuery(R"(
                SELECT * FROM `/Root/AlterTableAddNotNullColumn` ORDER BY Key;
            )"));
        };

        {
            fQuery(R"(
                UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (1, "Old");
            )");

            fCompareTable(R"([
                [[1u];["Old"]]
            ])");
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/AlterTableAddNotNullColumn`
                ADD COLUMN Value2 Int32 NOT NULL DEFAULT 1;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        Sleep(TDuration::Seconds(3));

        {
            fQuery(R"(
                INSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (2, "New");
            )");

                fCompareTable(R"([
                [[1u];["Old"];1];
                [[2u];["New"];1]
            ])");
        }

        {
            fQuery(R"(
                UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value, Value2) VALUES (2, "New", 2);
            )");

                fCompareTable(R"([
                [[1u];["Old"];1];
                [[2u];["New"];2]
            ])");
        }

        {
            fQuery(R"(
                UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (2, "OldNew");
            )");

            fQuery(R"(
                UPSERT INTO `/Root/AlterTableAddNotNullColumn` (Key, Value) VALUES (3, "BrandNew");
            )");

            fCompareTable(R"([
                [[1u];["Old"];1];
                [[2u];["OldNew"];2];
                [[3u];["BrandNew"];1]
            ])");
        }
    }

    Y_UNIT_TEST(AlterTableAddColumnWithDefaultRejection) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetEnableAddColumsWithDefaults(true)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Uint32,
                    Value Utf8,
                    PRIMARY KEY (Key),
                );
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/TestTable`
                ADD COLUMN DefaultValue Json NOT NULL DEFAULT Json("not [json]");
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Invalid value \"not [json]\" for type Json");
        }

        {
            auto query = R"(
                SELECT DefaultValue FROM `/Root/TestTable`;
            )";

            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Member not found: DefaultValue");
        }
    }

    Y_UNIT_TEST(AlterTableAddColumnWithDefaultCancellation) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetEnableAddColumsWithDefaults(true)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 100000, 4, 4, 1000, 1);

        {
            auto query = R"(
                ALTER TABLE `/Root/LargeTable`
                ADD COLUMN DefaultValue Utf8 NOT NULL DEFAULT Utf8("Default Value");
            )";

            auto script = db.ExecuteScript(query).ExtractValueSync();
            UNIT_ASSERT_C(script.Status().IsSuccess(), script.Status().GetIssues().ToString());

            auto opClient = NOperation::TOperationClient(kikimr.GetDriver());
            auto status = opClient.Cancel(script.Id()).ExtractValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

            auto op = opClient.Get<TScriptExecutionOperation>(script.Id()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(op.Status().GetStatus(), EStatus::CANCELLED, op.Status().GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(op.Status().GetIssues().ToOneLineString(), "Request was canceled by user");
        }

        {
            auto query = R"(
                SELECT DefaultValue FROM `/Root/LargeTable`;
            )";

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Member not found: DefaultValue");
        }
    }

    Y_UNIT_TEST(AlterTableAddColumnWithDefaultOlap) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetEnableAddColumsWithDefaults(true)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();

        {
            auto query = R"(
                CREATE TABLE `/Root/AlterTableAddColumnWithDefaultOlap` (
                    Key Uint32 NOT NULL,
                    PRIMARY KEY (Key),
                ) WITH (
                    STORE = COLUMN
                );
            )";

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/AlterTableAddColumnWithDefaultOlap`
                ADD COLUMN DefaultValue Int32 NOT NULL DEFAULT 1;
            )";

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Default values are not supported in column tables");
        }
    }

    Y_UNIT_TEST_TWIN(DefaultColumnAndBulkUpsert, DisableMissingDefaultColumnsInBulkUpsert) {
        TKikimrRunner kikimr(TKikimrSettings()
            .SetEnableAddColumsWithDefaults(true)
            .SetDisableMissingDefaultColumnsInBulkUpsert(DisableMissingDefaultColumnsInBulkUpsert)
            .SetWithSampleTables(false));

        auto queryClient = kikimr.GetQueryClient();
        auto tableClient = kikimr.GetTableClient();

        {
            auto query = R"(
                CREATE TABLE `/Root/DefaultColumnAndBulkUpsert` (
                    Key Uint32 NOT NULL,
                    Value1 String DEFAULT "Default value",
                    Value2 Int64 DEFAULT 123,
                    PRIMARY KEY (Key),
                );
            )";

            auto result = queryClient.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                UPSERT INTO `/Root/DefaultColumnAndBulkUpsert` (Key) VALUES (1), (2);
            )";

            auto result = queryClient.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                UPSERT INTO `/Root/DefaultColumnAndBulkUpsert` (Key, Value1) VALUES (3, "Value1"), (4, "Value2");
            )";

            auto result = queryClient.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/DefaultColumnAndBulkUpsert` ADD COLUMN Value3 Utf8 DEFAULT "Value3"u;
            )";

            auto result = queryClient.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto rowsBuilder = NYdb::TValueBuilder();
            rowsBuilder.BeginList();
            for (ui32 i = 10; i <= 15; ++i) {
                rowsBuilder.AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .Uint32(i)
                    .AddMember("Value2")
                        .OptionalInt64(0)
                    .EndStruct();

            }
            rowsBuilder.EndList();

            auto result = tableClient.BulkUpsert("/Root/DefaultColumnAndBulkUpsert", rowsBuilder.Build()).ExtractValueSync();
            if (DisableMissingDefaultColumnsInBulkUpsert) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SCHEME_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Missing default columns: Value3, Value1");
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            }
        }
    }

    // Y_UNIT_TEST(SetNotNull) {
    //     struct TValue {
    //     private:
    //         int _value = 0;
    //         bool _isNull = false;
    //     public:
    //         TValue(int value) : _value(value) {}
    //         TValue() : _isNull(true) {}

    //         int GetValue() const {
    //             assert(!_isNull);
    //             return _value;
    //         }

    //         bool IsNull() const {
    //             return _isNull;
    //         }

    //         std::string ToString() const {
    //             if (IsNull()) {
    //                 return "NULL";
    //             }

    //             return std::to_string(GetValue());
    //         }
    //     };

    //     NKikimrConfig::TAppConfig appConfig;
    //     appConfig.MutableTableServiceConfig()->SetEnableSequences(false);
    //     appConfig.MutableFeatureFlags()->SetEnableChangeNotNullConstraint(true);

    //     TKikimrRunner kikimr(TKikimrSettings().SetUseRealThreads(false).SetPQConfig(DefaultPQConfig()).SetAppConfig(appConfig));
    //     auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); } );
    //     auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); } );
    //     auto querySession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); } );

    //     auto& runtime = *kikimr.GetTestServer().GetRuntime();

    //     {
    //         auto createTable = R"sql(
    //             --!syntax_v1
    //             CREATE TABLE `/Root/SetNotNull` (
    //                 myKey Int32 NOT NULL,
    //                 myValue Int32 DEFAULT(0),
    //                 PRIMARY KEY (myKey)
    //             );
    //         )sql";

    //         auto result = kikimr.RunCall([&]{ return session.ExecuteSchemeQuery(createTable).GetValueSync(); });
    //         UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    //     }

    //     auto insertValues = [&](TValue key, TValue val) -> std::pair<EStatus, TString> {
    //         auto query = TString(std::format(R"sql(
    //             --!syntax_v1
    //             UPSERT INTO `/Root/SetNotNull` (myKey, myValue)
    //             VALUES
    //             ( {}, {} );
    //         )sql", key.ToString(), val.ToString()));

    //         NYdb::NTable::TExecDataQuerySettings execSettings;
    //         execSettings.KeepInQueryCache(true);
    //         execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

    //         auto result = kikimr.RunCall([&] {
    //             return querySession
    //                 .ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
    //                                   execSettings)
    //                 .ExtractValueSync(); } );

    //         return {result.GetStatus(), result.GetIssues().ToString()};
    //     };

    //     {
    //         const int countOfLines = 10;
    //         for (int i = 1; i <= countOfLines; i++) {
    //             auto key = TValue(i);
    //             auto value = (i != countOfLines ? TValue(10 * i) : TValue());
    //             auto [status, issue] = insertValues(key, value);
    //             UNIT_ASSERT_VALUES_EQUAL_C(status, EStatus::SUCCESS, issue);
    //         }
    //     }

    //     auto setNotNull = R"sql(
    //         --!syntax_v1
    //         ALTER TABLE `/Root/SetNotNull` ALTER COLUMN myValue SET NOT NULL;
    //     )sql";

    //     bool enabledCapture = true;
    //     TVector<TAutoPtr<IEventHandle>> delayedCheckingColumns;
    //     auto grab = [&delayedCheckingColumns, &enabledCapture](TAutoPtr<IEventHandle>& ev) -> auto {
    //         if (enabledCapture && ev->GetTypeRewrite() == TEvDataShard::TEvCheckConstraintCreateRequest::EventType) {
    //             delayedCheckingColumns.emplace_back(ev.Release());
    //             return TTestActorRuntime::EEventAction::DROP;
    //         }

    //         return TTestActorRuntime::EEventAction::PROCESS;
    //     };

    //     TDispatchOptions opts;
    //     opts.FinalEvents.emplace_back([&delayedCheckingColumns](IEventHandle&) {
    //         return delayedCheckingColumns.size() > 0;
    //     });

    //     runtime.SetObserverFunc(grab);

    //     auto setNotNullFuture = kikimr.RunInThreadPool([&] { return session.ExecuteSchemeQuery(setNotNull).GetValueSync(); });

    //     runtime.DispatchEvents(opts);
    //     Y_VERIFY_S(delayedCheckingColumns.size() > 0, "no upload rows requests");

    //     {
    //         auto key = TValue(201);
    //         auto value = TValue();
    //         auto [status, issue] = insertValues(key, value);
    //         UNIT_ASSERT_VALUES_EQUAL_C(status, EStatus::BAD_REQUEST, issue);
    //         UNIT_ASSERT_STRING_CONTAINS(issue, "All not null columns should be initialized");
    //     }

    //     enabledCapture = false;
    //     for (const auto& ev: delayedCheckingColumns) {
    //         runtime.Send(ev);
    //     }

    //     auto result = runtime.WaitFuture(setNotNullFuture);
    //     UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    // }

    Y_UNIT_TEST(AlterTableSetDefaultLiteral) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSetDropDefaultValue(true);

        TKikimrRunner kikimr(TKikimrSettings(appConfig)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/SetDefaultTest` (
                    Key Int32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/SetDefaultTest` ALTER COLUMN Value SET DEFAULT "hello"u;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                UPSERT INTO `/Root/SetDefaultTest` (Key) VALUES (1);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                SELECT Key, Value FROM `/Root/SetDefaultTest` ORDER BY Key;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [[1];["hello"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(AlterTableSetDefaultInt) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSetDropDefaultValue(true);

        TKikimrRunner kikimr(TKikimrSettings(appConfig)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/SetDefaultIntTest` (
                    Key Int32,
                    Value Int32,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/SetDefaultIntTest` ALTER COLUMN Value SET DEFAULT 42;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                UPSERT INTO `/Root/SetDefaultIntTest` (Key) VALUES (1);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                SELECT Key, Value FROM `/Root/SetDefaultIntTest` ORDER BY Key;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [[1];[42]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(AlterTableDropDefault) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSetDropDefaultValue(true);

        TKikimrRunner kikimr(TKikimrSettings(appConfig)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/DropDefaultTest` (
                    Key Int32,
                    Value String DEFAULT "original"u,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                UPSERT INTO `/Root/DropDefaultTest` (Key) VALUES (1);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/DropDefaultTest` ALTER COLUMN Value DROP DEFAULT;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                UPSERT INTO `/Root/DropDefaultTest` (Key) VALUES (2);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                SELECT Key, Value FROM `/Root/DropDefaultTest` ORDER BY Key;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [[1];["original"]];
                [[2];#]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(AlterTableSetDefaultNull) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSetDropDefaultValue(true);

        TKikimrRunner kikimr(TKikimrSettings(appConfig)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/SetDefaultNullTest` (
                    Key Int32,
                    Value String DEFAULT "original"u,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                UPSERT INTO `/Root/SetDefaultNullTest` (Key) VALUES (1);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/SetDefaultNullTest` ALTER COLUMN Value SET DEFAULT NULL;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                UPSERT INTO `/Root/SetDefaultNullTest` (Key) VALUES (2);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                SELECT Key, Value FROM `/Root/SetDefaultNullTest` ORDER BY Key;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [[1];["original"]];
                [[2];#]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(AlterTableSetDefaultNonExistentColumn) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSetDropDefaultValue(true);

        TKikimrRunner kikimr(TKikimrSettings(appConfig)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/SetDefaultNonExistent` (
                    Key Int32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/SetDefaultNonExistent` ALTER COLUMN NonExistent SET DEFAULT "val"u;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "does not exist");
        }
    }

    Y_UNIT_TEST(AlterTableDropDefaultNoExistingDefault) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSetDropDefaultValue(true);

        TKikimrRunner kikimr(TKikimrSettings(appConfig)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/DropDefaultNoDefault` (
                    Key Int32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/DropDefaultNoDefault` ALTER COLUMN Value DROP DEFAULT;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "has no default to drop");
        }
    }

    Y_UNIT_TEST(AlterTableDropDefaultOnSequenceColumn) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSetDropDefaultValue(true);

        TKikimrRunner kikimr(TKikimrSettings(appConfig)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/DropDefaultSerial` (
                    Key Serial,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/DropDefaultSerial` ALTER COLUMN Key DROP DEFAULT;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Cannot drop default for a serial/sequence column");
        }
    }

    Y_UNIT_TEST(AlterTableDropDefaultOnNotNullColumn) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSetDropDefaultValue(true);

        TKikimrRunner kikimr(TKikimrSettings(appConfig)
            .SetEnableAddColumsWithDefaults(true)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/DropDefaultNotNull` (
                    Key Int32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/DropDefaultNotNull` ADD COLUMN Extra Int32 NOT NULL DEFAULT 0;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/DropDefaultNotNull` ALTER COLUMN Extra DROP DEFAULT;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Cannot drop default for a NOT NULL column");
        }
    }

    Y_UNIT_TEST(ShowCreateTableAfterSetDefault) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSetDropDefaultValue(true);

        TKikimrRunner kikimr(TKikimrSettings(appConfig)
            .SetEnableAddColumsWithDefaults(true)
            .SetEnableShowCreate(true)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/ShowCreateSetDefault` (
                    Key Int32,
                    Value Utf8,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(R"(
                SHOW CREATE TABLE `/Root/ShowCreateSetDefault`;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto yson = FormatResultSetYson(result.GetResultSet(0));
            UNIT_ASSERT_STRING_CONTAINS_C(yson, "Key", yson);
            UNIT_ASSERT(!yson.Contains("DEFAULT"));
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/ShowCreateSetDefault` ALTER COLUMN Value SET DEFAULT "hello"u;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(R"(
                SHOW CREATE TABLE `/Root/ShowCreateSetDefault`;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto yson = FormatResultSetYson(result.GetResultSet(0));
            UNIT_ASSERT_STRING_CONTAINS_C(yson, "DEFAULT", yson);
        }
    }

    Y_UNIT_TEST(AlterTableSetDropDefaultMultipleColumns) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSetDropDefaultValue(true);

        TKikimrRunner kikimr(TKikimrSettings(appConfig)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/MultiColDefault` (
                    Key Int32,
                    ColA Utf8 DEFAULT "old_a"u,
                    ColB Int32,
                    ColC Utf8 DEFAULT "old_c"u,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                UPSERT INTO `/Root/MultiColDefault` (Key) VALUES (1);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/MultiColDefault`
                    ALTER COLUMN ColA SET DEFAULT "new_a"u,
                    ALTER COLUMN ColB SET DEFAULT 99,
                    ALTER COLUMN ColC DROP DEFAULT;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                UPSERT INTO `/Root/MultiColDefault` (Key) VALUES (2);
            )";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                SELECT Key, ColA, ColB, ColC FROM `/Root/MultiColDefault` ORDER BY Key;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([
                [[1];["old_a"];#;["old_c"]];
                [[2];["new_a"];[99];#]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ShowCreateTableAfterDropDefault) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSetDropDefaultValue(true);

        TKikimrRunner kikimr(TKikimrSettings(appConfig)
            .SetEnableAddColumsWithDefaults(true)
            .SetEnableShowCreate(true)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/ShowCreateDropDefault` (
                    Key Int32,
                    Value Utf8 DEFAULT "initial"u,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(R"(
                SHOW CREATE TABLE `/Root/ShowCreateDropDefault`;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto yson = FormatResultSetYson(result.GetResultSet(0));
            UNIT_ASSERT_STRING_CONTAINS_C(yson, "DEFAULT", yson);
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/ShowCreateDropDefault` ALTER COLUMN Value DROP DEFAULT;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(R"(
                SHOW CREATE TABLE `/Root/ShowCreateDropDefault`;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto yson = FormatResultSetYson(result.GetResultSet(0));
            UNIT_ASSERT_STRING_CONTAINS_C(yson, "Key", yson);
            UNIT_ASSERT(!yson.Contains("DEFAULT"));
        }
    }

    Y_UNIT_TEST(AlterTableSetDefaultOnColumnTable) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSetDropDefaultValue(true);

        TKikimrRunner kikimr(TKikimrSettings(appConfig)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/OlapSetDefault` (
                    Key Uint32 NOT NULL,
                    Value Uint32,
                    PRIMARY KEY (Key)
                ) WITH (
                    STORE = COLUMN
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/OlapSetDefault` ALTER COLUMN Value SET DEFAULT 42;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Default values are not supported in column tables");
        }
    }

    Y_UNIT_TEST(AlterTableDropDefaultOnColumnTable) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableSetDropDefaultValue(true);

        TKikimrRunner kikimr(TKikimrSettings(appConfig)
            .SetWithSampleTables(false));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto query = R"(
                CREATE TABLE `/Root/OlapDropDefault` (
                    Key Uint32 NOT NULL,
                    Value Uint32,
                    PRIMARY KEY (Key)
                ) WITH (
                    STORE = COLUMN
                );
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto query = R"(
                ALTER TABLE `/Root/OlapDropDefault` ALTER COLUMN Value DROP DEFAULT;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Default values are not supported in column tables");
        }
    }
}

} // namespace NKikimr::NKqp
