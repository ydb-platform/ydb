#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/tx/datashard/datashard_impl.h>

namespace NKikimr::NKqp {

using namespace NYdb;

Y_UNIT_TEST_SUITE(KqpNamedExpressions) {
    Y_UNIT_TEST_TWIN(NamedExpressionSimple, UseSink) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
        TKikimrRunner kikimr(settings);

        const TString query = R"(
            $t = SELECT * FROM KeyValue;

            SELECT * FROM $t;
            
            UPSERT INTO KeyValue (Key, Value) VALUES (3u, "test");

            SELECT * FROM $t;

            UPSERT INTO KeyValue SELECT Key + 10u AS Key, Value FROM $t;

            SELECT * FROM $t;
        )";

        auto client = kikimr.GetQueryClient();
        auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[1u];["One"]];[[2u];["Two"]]])", FormatResultSetYson(result.GetResultSet(0)));
        Cerr << FormatResultSetYson(result.GetResultSet(1)) << Endl;
        Cerr << FormatResultSetYson(result.GetResultSet(2)) << Endl;
        CompareYson(R"([[[1u];["One"]];[[2u];["Two"]]])", FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([[[1u];["One"]];[[2u];["Two"]]])", FormatResultSetYson(result.GetResultSet(2)));
    }

    Y_UNIT_TEST_TWIN(NamedExpressionChanged, UseSink) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
        TKikimrRunner kikimr(settings);

        const TString query = R"(
            $t = (
                SELECT 
                    Key As Key,
                    Value As OldValue,
                    "test" As NewValue
                FROM KeyValue
                WHERE Value != "test"
            );

            UPSERT INTO KeyValue2 (
                SELECT
                    CAST(Key AS String) AS Key,
                    NewValue AS Value
                From $t
            );

            UPDATE KeyValue ON (
                SELECT
                    Key AS Key,
                    NewValue AS Value
                From $t
            );

            SELECT
                COUNT(*)
            FROM $t;

            SELECT * FROM KeyValue2;
        )";

        auto client = kikimr.GetQueryClient();
        auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[2u]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[["1"];["test"]];[["2"];["test"]]])", FormatResultSetYson(result.GetResultSet(1)));
    }

    Y_UNIT_TEST_TWIN(NamedExpressionRandomChanged, UseSink) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
        TKikimrRunner kikimr(settings);

        {
            const TString query = R"(
                $t = (
                    SELECT 
                        Key As Key,
                        CAST(RandomUuid(Key) AS String) As NewValue
                    FROM KeyValue
                    WHERE LENGTH(Value) < 10
                    LIMIT 10
                );

                UPSERT INTO KeyValue2 (
                    SELECT
                        CAST(Key AS String) AS Key,
                        NewValue AS Value
                    From $t
                );

                UPDATE KeyValue ON (
                    SELECT
                        Key AS Key,
                        NewValue AS Value
                    From $t
                );

                SELECT
                    True
                FROM $t
                LIMIT 1;
            )";

            auto client = kikimr.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;

            CompareYson(R"([[%true]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            const TString query = R"(
                SELECT Value FROM KeyValue ORDER BY Value;
                SELECT Value FROM KeyValue2 ORDER BY Value;
            )";

            auto client = kikimr.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            Cerr << FormatResultSetYson(result.GetResultSet(1)) << Endl;

            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)));
        }
    }

    Y_UNIT_TEST_TWIN(NamedExpressionRandomChanged2, UseSink) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
        TKikimrRunner kikimr(settings);

        const TString query = R"(
            $t = (
                SELECT 
                    Key As Key,
                    CAST(RandomUuid(Key) AS String) As Value
                FROM KeyValue
                WHERE LENGTH(Value) < 10
            );

            UPDATE KeyValue ON (
                SELECT
                    Key AS Key,
                    Value AS Value
                From $t
            );

            UPSERT INTO KeyValue2 (
                SELECT
                    CAST(Key AS String) AS Key,
                    Value AS Value
                From $t
            );

            SELECT Value FROM KeyValue ORDER BY Value;
            SELECT Value FROM KeyValue2 ORDER BY Value;
        )";

        auto client = kikimr.GetQueryClient();
        auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;
        Cerr << FormatResultSetYson(result.GetResultSet(1)) << Endl;

        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)));
    }

    Y_UNIT_TEST_TWIN(NamedExpressionRandom, UseSink) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);

        const std::vector<std::pair<std::string, std::string>> operations = {
            {"UPSERT INTO", ""},
            {"REPLACE INTO", ""},
            {"UPDATE", "ON"},
        };

        for (const auto& [operation, operationPart] : operations) {
            TKikimrRunner kikimr(settings);
            auto client = kikimr.GetQueryClient();

            const TString query = std::format(R"(
                $t = (
                    SELECT 
                        Key As Key,
                        CAST(RandomUuid(Key) AS String) As Value
                    FROM KeyValue
                );

                {0} KeyValue2 {1} (
                    SELECT
                        CAST(Key AS String) AS Key,
                        Value AS Value
                    From $t
                );

                {0} KeyValue {1} (
                    SELECT
                        Key AS Key,
                        Value AS Value
                    From $t
                );

                SELECT Value FROM KeyValue ORDER BY Value;
                SELECT Value FROM KeyValue2 ORDER BY Value;
                SELECT Value FROM $t ORDER BY Value;
            )", operation, operationPart);

            auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            Cerr << FormatResultSetYson(result.GetResultSet(1)) << Endl;
            Cerr << FormatResultSetYson(result.GetResultSet(2)) << Endl;
            
            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)));
            UNIT_ASSERT(FormatResultSetYson(result.GetResultSet(0)) != FormatResultSetYson(result.GetResultSet(2)));
        }
    }

    Y_UNIT_TEST_TWIN(NamedExpressionRandomInsert, UseSink) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);

        const std::vector<std::pair<std::string, std::string>> operations = {
            {"UPSERT INTO", ""},
            {"REPLACE INTO", ""},
            {"INSERT INTO", ""},
        };

        for (const auto& [operation, operationPart] : operations) {
            TKikimrRunner kikimr(settings);
            auto client = kikimr.GetQueryClient();

            const TString query = std::format(R"(
                $t = (
                    SELECT 
                        Key As Key,
                        CAST(RandomUuid(Key) AS String) As Value
                    FROM KeyValue
                );

                DELETE FROM KeyValue2;

                {0} KeyValue2 {1} (
                    SELECT
                        CAST(Key AS String) AS Key,
                        Value AS Value
                    From $t
                );

                {0} KeyValue2 {1} (
                    SELECT
                        CAST(Key + 10u AS String) AS Key,
                        Value AS Value
                    From $t
                );

                SELECT Value FROM KeyValue2 WHERE CAST(Key AS Uint64) < 10u ORDER BY Value;
                SELECT Value FROM KeyValue2 WHERE CAST(Key AS Uint64) > 10u ORDER BY Value;
                SELECT Value FROM $t ORDER BY Value;
            )", operation, operationPart);

            auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            
            Cerr << operation << Endl;
            Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            Cerr << FormatResultSetYson(result.GetResultSet(1)) << Endl;
            Cerr << FormatResultSetYson(result.GetResultSet(2)) << Endl;
            
            if (!operation.contains("INSERT")) {
                UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)));
            } else {
                UNIT_ASSERT(FormatResultSetYson(result.GetResultSet(0)) != FormatResultSetYson(result.GetResultSet(1)));
            }
            UNIT_ASSERT(FormatResultSetYson(result.GetResultSet(0)) != FormatResultSetYson(result.GetResultSet(2)));
        }
    }

    Y_UNIT_TEST_TWIN(NamedExpressionRandomDataQuery, UseSink) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);

        const std::vector<std::pair<std::string, std::string>> operations = {
            {"UPSERT INTO", ""},
            {"REPLACE INTO", ""},
            {"UPDATE", "ON"},
        };

        for (const auto& [operation, operationPart] : operations) {
            TKikimrRunner kikimr(settings);
            auto client = kikimr.GetTableClient();

            {
                const TString query = std::format(R"(
                    $t = (
                        SELECT 
                            Key As Key,
                            CAST(RandomUuid(Key) AS String) As Value
                        FROM KeyValue
                        WHERE LENGTH(Value) < 10u
                    );

                    {0} KeyValue2 {1} (
                        SELECT
                            CAST(Key AS String) AS Key,
                            Value AS Value
                        From $t
                    );

                    {0} KeyValue {1} (
                        SELECT
                            Key AS Key,
                            Value AS Value
                        From $t
                    );

                    SELECT Value FROM $t ORDER BY Value;
                )", operation, operationPart);

                auto session = client.CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;
                UNIT_ASSERT_VALUES_EQUAL(2, result.GetResultSet(0).RowsCount());
            }
            {
                const TString query = std::format(R"(
                    SELECT Value FROM KeyValue ORDER BY Value;
                    SELECT Value FROM KeyValue2 ORDER BY Value;
                )", operation, operationPart);

                auto session = client.CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;
                Cerr << FormatResultSetYson(result.GetResultSet(1)) << Endl;

                UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)));
            }
        }
    }

    Y_UNIT_TEST_TWIN(NamedExpressionRandomInsertDataQuery, UseSink) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);

        const std::vector<std::pair<std::string, std::string>> operations = {
            {"UPSERT INTO", ""},
            {"REPLACE INTO", ""},
            {"INSERT INTO", ""},
        };

        for (const auto& [operation, operationPart] : operations) {
            TKikimrRunner kikimr(settings);
            auto client = kikimr.GetTableClient();

            {
                const TString query = std::format(R"(
                    $t = (
                        SELECT 
                            Key As Key,
                            CAST(RandomUuid(Key) AS String) As Value
                        FROM KeyValue
                    );

                    DELETE FROM KeyValue2;

                    {0} KeyValue2 {1} (
                        SELECT
                            CAST(Key AS String) AS Key,
                            Value AS Value
                        From $t
                    );

                    {0} KeyValue2 {1} (
                        SELECT
                            CAST(Key + 10u AS String) AS Key,
                            Value AS Value
                        From $t
                    );

                    SELECT Value FROM $t ORDER BY Value;
                )", operation, operationPart);

                auto session = client.CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;
                UNIT_ASSERT_VALUES_EQUAL(2, result.GetResultSet(0).RowsCount());
            }
            {
                const TString query = std::format(R"(
                    SELECT Value FROM KeyValue2 WHERE CAST(Key AS Uint64) < 10u ORDER BY Value;
                    SELECT Value FROM KeyValue2 WHERE CAST(Key AS Uint64) > 10u ORDER BY Value;
                )", operation, operationPart);

                auto session = client.CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;
                Cerr << FormatResultSetYson(result.GetResultSet(1)) << Endl;

                if (!operation.contains("INSERT")) {
                    UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)));
                } else {
                    UNIT_ASSERT(FormatResultSetYson(result.GetResultSet(0)) != FormatResultSetYson(result.GetResultSet(1)));
                }
            }
        }
    }

    Y_UNIT_TEST_QUAD(NamedExpressionRandomUpsertIndex, UseSink, UseDataQuery) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);

        const std::vector<std::pair<std::string, std::string>> tests = {
            {"", ""},
            {"INDEX i GLOBAL ON (Key2),", ""},
            {"", "INDEX i GLOBAL ON (Key2),"},
            {"INDEX i GLOBAL ON (Key2),", "INDEX i GLOBAL ON (Key2),"},
        };

        for (const auto& [index1, index2] : tests) {
            TKikimrRunner kikimr(settings);
            {
                const TString query = std::format(R"(
                    CREATE TABLE Source (
                        Key String,
                        Key2 String,
                        Value String,
                        PRIMARY KEY (Key)
                    );
                    
                    CREATE TABLE Dest1 (
                        Key String,
                        Key2 String,
                        Value String,
                        {0}
                        PRIMARY KEY (Key)
                    );

                    CREATE TABLE Dest2 (
                        Key String,
                        Key2 String,
                        Value String,
                        {1}
                        PRIMARY KEY (Key)
                    );
                )", index1, index2);

                auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            }

            {
                const TString query = R"(
                    INSERT INTO Source (Key, Key2, Value) VALUES
                        ("1", "test", "");
                )";

                auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            }

            {
                const TString query = R"(
                    $t = (
                        SELECT 
                            Key AS Key,
                            Key AS Key2,
                            CAST(RandomUuid(Key) AS String) As Value
                        FROM Source
                    );

                    UPSERT INTO Dest1 (
                        SELECT
                            Key AS Key,
                            CAST(RandomUuid(Key) AS String)  AS Key2,
                            Value AS Value
                        From $t
                    );

                    UPSERT INTO Dest2 (
                        SELECT
                            Key AS Key,
                            CAST(RandomUuid(Key) AS String)  AS Key2,
                            Value AS Value
                        From $t
                    );
                )";

                if (UseDataQuery) {
                    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                    auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
                } else {
                    auto result = kikimr.GetQueryClient().ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
                }
            }
            {
                const TString query = R"(
                    SELECT Value FROM Dest1 ORDER BY Value;
                    SELECT Value FROM Dest2 ORDER BY Value;
                )";

                auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;
                Cerr << FormatResultSetYson(result.GetResultSet(1)) << Endl;

                const bool onlyOneIndex = (index1.empty() != index2.empty());

                if (onlyOneIndex && UseSink) {
                    // "with index" uses literal executer (precompute), while "without index" uses compute actor.
                    // TODO:
                    UNIT_ASSERT(FormatResultSetYson(result.GetResultSet(0)) != FormatResultSetYson(result.GetResultSet(1)));
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)));
                }
            }
        }
    }

    Y_UNIT_TEST_QUAD(NamedExpressionRandomUpsertReturning, UseSink, UseDataQuery) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);

        const std::vector<std::pair<std::string, std::string>> tests = {
            {"", ""},
            {"RETURNING Value", ""},
            {"", "RETURNING Value"},
            {"RETURNING Value", "RETURNING Value"},
        };

        for (const auto& [ret1, ret2] : tests) {
            TKikimrRunner kikimr(settings);
            {
                const TString query = R"(
                    CREATE TABLE Source (
                        Key String,
                        Key2 String,
                        Value String,
                        PRIMARY KEY (Key)
                    );
                    
                    CREATE TABLE Dest1 (
                        Key String,
                        Key2 String,
                        Value String,
                        PRIMARY KEY (Key)
                    );

                    CREATE TABLE Dest2 (
                        Key String,
                        Key2 String,
                        Value String,
                        PRIMARY KEY (Key)
                    );
                )";

                auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            }

            {
                const TString query = R"(
                    INSERT INTO Source (Key, Key2, Value) VALUES
                        ("1", "test", "");
                )";

                auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            }

            {
                const TString query = std::format(R"(
                    $t = (
                        SELECT 
                            Key AS Key,
                            Key AS Key2,
                            CAST(RandomUuid(Key) AS String) As Value
                        FROM Source
                    );

                    UPSERT INTO Dest1 (
                        SELECT
                            Key AS Key,
                            CAST(RandomUuid(Key) AS String)  AS Key2,
                            Value AS Value
                        From $t
                    )
                    {0};

                    UPSERT INTO Dest2 (
                        SELECT
                            Key AS Key,
                            CAST(RandomUuid(Key) AS String) AS Key2,
                            Value AS Value
                        From $t
                    )
                    {1};
                )", ret1, ret2);

                if (UseDataQuery) {
                    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                    auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
                } else {
                    auto result = kikimr.GetQueryClient().ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
                }
            }
            {
                const TString query = R"(
                    SELECT Value FROM Dest1 ORDER BY Value;
                    SELECT Value FROM Dest2 ORDER BY Value;
                )";

                auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;
                Cerr << FormatResultSetYson(result.GetResultSet(1)) << Endl;

                UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)));
            }
        }
    }

    Y_UNIT_TEST_QUAD(NamedExpressionRandomUpsertRevert, UseSink, UseDataQuery) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);

        const std::vector<std::string> ops = {"UPSERT", "INSERT", "INSERT OR REVERT"};

        for (const auto& op1 : ops) {
            for (const auto& op2 : ops) {
                TKikimrRunner kikimr(settings);
                {
                    const TString query = R"(
                        CREATE TABLE Source (
                            Key String,
                            Key2 String,
                            Value String,
                            PRIMARY KEY (Key)
                        );
                        
                        CREATE TABLE Dest1 (
                            Key String,
                            Key2 String,
                            Value String,
                            PRIMARY KEY (Key)
                        );

                        CREATE TABLE Dest2 (
                            Key String,
                            Key2 String,
                            Value String,
                            PRIMARY KEY (Key)
                        );
                    )";

                    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                    auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
                    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
                }

                {
                    const TString query = R"(
                        INSERT INTO Source (Key, Key2, Value) VALUES
                            ("1", "test", "");
                    )";

                    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                    auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
                }

                {
                    const TString query = std::format(R"(
                        $t = (
                            SELECT 
                                Key AS Key,
                                Key AS Key2,
                                CAST(RandomUuid(Key) AS String) As Value
                            FROM Source
                        );

                        {0} INTO Dest1 (
                            SELECT
                                Key AS Key,
                                CAST(RandomUuid(Key) AS String)  AS Key2,
                                Value AS Value
                            From $t
                        );

                        {1} INTO Dest2 (
                            SELECT
                                Key AS Key,
                                CAST(RandomUuid(Key) AS String) AS Key2,
                                Value AS Value
                            From $t
                        );
                    )", op1, op2);

                    if (UseDataQuery) {
                        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                        auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
                    } else {
                        auto result = kikimr.GetQueryClient().ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
                    }
                }
                {
                    const TString query = R"(
                        SELECT Value FROM Dest1 ORDER BY Value;
                        SELECT Value FROM Dest2 ORDER BY Value;
                    )";

                    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
                    auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                    UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                    Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;
                    Cerr << FormatResultSetYson(result.GetResultSet(1)) << Endl;

                    const bool onlyOneRevert = (op1 == "INSERT OR REVERT") != (op2 == "INSERT OR REVERT");

                    if (onlyOneRevert && UseSink) {
                        // TODO:
                        UNIT_ASSERT(FormatResultSetYson(result.GetResultSet(0)) != FormatResultSetYson(result.GetResultSet(1)));
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)));
                    }
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(NamedExpressionRandomSelect, UseSink) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);

        TKikimrRunner kikimr(settings);
        {
            const TString query = R"(
                CREATE TABLE Source (
                    Key String,
                    Key2 String,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )";

            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                INSERT INTO Source (Key, Key2, Value) VALUES
                    ("1", "test", "");
            )";

            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query = R"(
                $t = (
                    SELECT 
                        Key AS Key,
                        Key AS Key2,
                        CAST(RandomUuid(Key) AS String) As Value
                    FROM Source
                );

               SELECT COUNT(DISTINCT Value) FROM (SELECT * FROM $t UNION ALL SELECT * FROM $t);
            )";

            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;

            UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), "[[2u]]");
        }
       
    }
}

} // namespace NKikimr::NKqp
