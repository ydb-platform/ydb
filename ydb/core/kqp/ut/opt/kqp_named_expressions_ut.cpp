#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/tx/datashard/datashard_impl.h>

namespace NKikimr::NKqp {

using namespace NYdb;

namespace {

void CreateNamedExpressionIndexedTables(TKikimrRunner& kikimr) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

    const auto schemeResult = session.ExecuteSchemeQuery(R"(
        CREATE TABLE Source (
            Key Uint64,
            Tag String,
            Value String,
            INDEX ByTag GLOBAL ON (Tag),
            PRIMARY KEY (Key)
        );

        CREATE TABLE Dest (
            Key Uint64,
            Tag String,
            Value String,
            INDEX ByTag GLOBAL ON (Tag),
            PRIMARY KEY (Key)
        );
    )").ExtractValueSync();
    UNIT_ASSERT_C(schemeResult.GetStatus() == EStatus::SUCCESS, schemeResult.GetIssues().ToString());

    const auto dataResult = session.ExecuteDataQuery(R"(
        UPSERT INTO Source (Key, Tag, Value) VALUES
            (1u, "old", "one"),
            (2u, "old", "two");
    )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_C(dataResult.GetStatus() == EStatus::SUCCESS, dataResult.GetIssues().ToString());
}

TKikimrSettings NamedExpressionIndexedSettings(bool enableIndexStreamWrite) {
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableIndexStreamWrite(enableIndexStreamWrite);
    return TKikimrSettings(app).SetWithSampleTables(false);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpNamedExpressions) {
    Y_UNIT_TEST(NamedExpressionSimple) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
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

    Y_UNIT_TEST(NamedExpressionChanged) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
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

    Y_UNIT_TEST_TWIN(NamedExpressionIndexedResultOnly, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(NamedExpressionIndexedSettings(EnableIndexStreamWrite));
        CreateNamedExpressionIndexedTables(kikimr);

        const TString query = R"(
            $rows = SELECT Key, Tag, Value FROM Source ORDER BY Key;

            SELECT * FROM $rows;

            UPSERT INTO Source (Key, Tag, Value) VALUES (1u, "new", "updated");

            SELECT * FROM $rows;
            SELECT Key, Tag, Value FROM Source ORDER BY Key;
        )";

        const auto result = kikimr.GetQueryClient().ExecuteQuery(
            query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

        const TString originalRows = R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]]])";
        CompareYson(originalRows, FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(originalRows, FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([[[1u];["new"];["updated"]];[[2u];["old"];["two"]]])",
            FormatResultSetYson(result.GetResultSet(2)));
    }

    Y_UNIT_TEST_TWIN(NamedExpressionIndexedSinksReturningAndRead, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(NamedExpressionIndexedSettings(EnableIndexStreamWrite));
        CreateNamedExpressionIndexedTables(kikimr);

        const TString query = R"(
            $rows = SELECT Key, Tag, Value FROM Source ORDER BY Key;

            UPSERT INTO Dest SELECT * FROM $rows RETURNING Key, Tag, Value;

            UPSERT INTO Source
                SELECT Key, "new" AS Tag, Value FROM $rows
                RETURNING Key, Tag, Value;

            SELECT * FROM $rows;
            SELECT Key, Tag, Value FROM Source ORDER BY Key;
            SELECT Key, Tag, Value FROM Dest ORDER BY Key;
        )";

        const auto result = kikimr.GetQueryClient().ExecuteQuery(
            query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

        const TString originalRows = R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]]])";
        const TString updatedRows = R"([[[1u];["new"];["one"]];[[2u];["new"];["two"]]])";
        CompareYsonUnordered(originalRows, FormatResultSetYson(result.GetResultSet(0)));
        CompareYsonUnordered(updatedRows, FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(originalRows, FormatResultSetYson(result.GetResultSet(2)));
        CompareYson(updatedRows, FormatResultSetYson(result.GetResultSet(3)));
        CompareYson(originalRows, FormatResultSetYson(result.GetResultSet(4)));
    }

    Y_UNIT_TEST_TWIN(NamedExpressionSecondaryIndexResultOnly, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(NamedExpressionIndexedSettings(EnableIndexStreamWrite));
        CreateNamedExpressionIndexedTables(kikimr);

        const TString query = R"(
            $rows = (
                SELECT Key, Tag, Value
                FROM Source VIEW ByTag
                WHERE Tag = "old"
                ORDER BY Key
            );

            SELECT * FROM $rows;

            UPSERT INTO Source (Key, Tag, Value) VALUES (1u, "new", "updated");

            SELECT * FROM $rows;
            SELECT Key, Tag, Value FROM Source VIEW ByTag WHERE Tag = "old" ORDER BY Key;
            SELECT Key, Tag, Value FROM Source VIEW ByTag WHERE Tag = "new" ORDER BY Key;
        )";

        const auto result = kikimr.GetQueryClient().ExecuteQuery(
            query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

        const TString originalRows = R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]]])";
        CompareYson(originalRows, FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(originalRows, FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([[[2u];["old"];["two"]]])", FormatResultSetYson(result.GetResultSet(2)));
        CompareYson(R"([[[1u];["new"];["updated"]]])", FormatResultSetYson(result.GetResultSet(3)));
    }

    Y_UNIT_TEST_TWIN(NamedExpressionIndexedConflictCheckingWrites, EnableIndexStreamWrite) {
        const std::vector<std::string> operations = {
            "INSERT",
            "INSERT OR REVERT",
        };

        for (const auto& operation : operations) {
            TKikimrRunner kikimr(NamedExpressionIndexedSettings(EnableIndexStreamWrite));
            CreateNamedExpressionIndexedTables(kikimr);

            const TString query = std::format(R"(
                $rows = SELECT Key, Tag, Value FROM Source ORDER BY Key;

                {0} INTO Dest (
                    SELECT * FROM $rows
                );
                {0} INTO Dest (
                    SELECT Key + 10u AS Key, Tag, Value FROM $rows
                );

                SELECT Key, Tag, Value FROM $rows;
                SELECT Key, Tag, Value FROM Dest ORDER BY Key;
            )", operation);

            const auto result = kikimr.GetQueryClient().ExecuteQuery(
                query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS,
                operation << ": " << result.GetIssues().ToString());

            CompareYson(R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]]])",
                FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]];[[11u];["old"];["one"]];[[12u];["old"];["two"]]])",
                FormatResultSetYson(result.GetResultSet(1)));
        }
    }

    Y_UNIT_TEST_TWIN(NamedExpressionPartialIndexedUpdate, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(NamedExpressionIndexedSettings(EnableIndexStreamWrite));
        CreateNamedExpressionIndexedTables(kikimr);

        const TString query = R"(
            $rows = SELECT Key, Tag, Value FROM Source ORDER BY Key;

            UPSERT INTO Source (Key, Tag)
                SELECT Key, "new" AS Tag FROM $rows;

            SELECT * FROM $rows;
            SELECT Key, Tag, Value FROM Source ORDER BY Key;
        )";

        const auto result = kikimr.GetQueryClient().ExecuteQuery(
            query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]]])",
            FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[1u];["new"];["one"]];[[2u];["new"];["two"]]])",
            FormatResultSetYson(result.GetResultSet(1)));
    }

    Y_UNIT_TEST_TWIN(NamedExpressionIndexedDeleteUpdateReuse, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(NamedExpressionIndexedSettings(EnableIndexStreamWrite));
        CreateNamedExpressionIndexedTables(kikimr);

        const TString query = R"(
            $rows = SELECT Key, Tag, Value FROM Source ORDER BY Key;

            SELECT * FROM $rows;

            DELETE FROM Source ON (
                SELECT Key FROM $rows WHERE Key = 1u
            );

            UPDATE Source ON (
                SELECT Key, "new" AS Tag, "updated" AS Value
                FROM $rows
                WHERE Key = 2u
            );

            SELECT * FROM $rows;
            SELECT Key, Tag, Value FROM Source ORDER BY Key;
        )";

        const auto result = kikimr.GetQueryClient().ExecuteQuery(
            query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

        const TString originalRows = R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]]])";
        CompareYson(originalRows, FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[2u];["old"];["two"]]])", FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([[[2u];["new"];["updated"]]])", FormatResultSetYson(result.GetResultSet(2)));
    }

    Y_UNIT_TEST_TWIN(NamedExpressionIndexedDependencyChain, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(NamedExpressionIndexedSettings(EnableIndexStreamWrite));
        CreateNamedExpressionIndexedTables(kikimr);

        const TString query = R"(
            $base = SELECT Key, Tag, Value FROM Source ORDER BY Key;
            $derived = (
                SELECT Key + 10u AS Key, Tag, Value
                FROM $base
                WHERE Key = 1u
            );

            UPSERT INTO Dest SELECT * FROM $derived;
            UPSERT INTO Source (Key, Tag, Value) VALUES (1u, "new", "updated");

            SELECT * FROM $base;
            SELECT * FROM $derived;
            SELECT Key, Tag, Value FROM Source ORDER BY Key;
            SELECT Key, Tag, Value FROM Dest ORDER BY Key;
        )";

        const auto result = kikimr.GetQueryClient().ExecuteQuery(
            query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]]])",
            FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[11u];["old"];["one"]]])", FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([[[1u];["new"];["updated"]];[[2u];["old"];["two"]]])",
            FormatResultSetYson(result.GetResultSet(2)));
        CompareYson(R"([[[11u];["old"];["one"]]])", FormatResultSetYson(result.GetResultSet(3)));
    }

    Y_UNIT_TEST_TWIN(NamedExpressionMultipleIndexedWrites, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(NamedExpressionIndexedSettings(EnableIndexStreamWrite));
        CreateNamedExpressionIndexedTables(kikimr);

        const TString query = R"(
            $rows = SELECT Key, Tag, Value FROM Source ORDER BY Key;

            UPSERT INTO Dest SELECT * FROM $rows;
            UPSERT INTO Source SELECT Key + 100u AS Key, Tag, Value FROM $rows;

            SELECT * FROM $rows;
            SELECT Key, Tag, Value FROM Dest ORDER BY Key;
            SELECT Key, Tag, Value FROM Source ORDER BY Key;
        )";

        const auto result = kikimr.GetQueryClient().ExecuteQuery(
            query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

        const TString originalRows = R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]]])";
        CompareYson(originalRows, FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(originalRows, FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]];[[101u];["old"];["one"]];[[102u];["old"];["two"]]])",
            FormatResultSetYson(result.GetResultSet(2)));
    }

    Y_UNIT_TEST_TWIN(NamedExpressionPureNondeterministicWriteAndResult, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(NamedExpressionIndexedSettings(EnableIndexStreamWrite));
        CreateNamedExpressionIndexedTables(kikimr);

        const TString query = R"(
            $val = CAST(RandomUuid(1u) AS String);

            UPSERT INTO Dest (Key, Tag, Value) VALUES (1u, "rand", $val);
            SELECT $val AS Value;
        )";

        const auto result = kikimr.GetQueryClient().ExecuteQuery(
            query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

        const auto selectValue = FormatResultSetYson(result.GetResultSet(0));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto readResult = session.ExecuteDataQuery(R"(
            SELECT Value FROM Dest WHERE Key = 1u;
        )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(readResult.GetStatus() == EStatus::SUCCESS, readResult.GetIssues().ToString());

        const auto destValue = FormatResultSetYson(readResult.GetResultSet(0));
        UNIT_ASSERT(selectValue != destValue);
    }

    Y_UNIT_TEST_TWIN(NamedExpressionWriteThenReuseForWrite, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(NamedExpressionIndexedSettings(EnableIndexStreamWrite));
        CreateNamedExpressionIndexedTables(kikimr);

        const TString query = R"(
            $rows = SELECT Key, Tag, Value FROM Source ORDER BY Key;

            UPSERT INTO Dest SELECT * FROM $rows;

            UPSERT INTO Source (Key, Tag, Value) VALUES (3u, "old", "three");

            UPSERT INTO Dest SELECT Key + 10u AS Key, Tag, Value FROM $rows;

            SELECT * FROM $rows;
            SELECT Key, Tag, Value FROM Dest ORDER BY Key;
            SELECT Key, Tag, Value FROM Source ORDER BY Key;
        )";

        const auto result = kikimr.GetQueryClient().ExecuteQuery(
            query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

        const TString allRows = R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]];[[3u];["old"];["three"]]])";
        CompareYson(allRows, FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]];[[11u];["old"];["one"]];[[12u];["old"];["two"]];[[13u];["old"];["three"]]])",
            FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(allRows, FormatResultSetYson(result.GetResultSet(2)));
    }

    Y_UNIT_TEST_TWIN(NamedExpressionClonedStages, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(NamedExpressionIndexedSettings(EnableIndexStreamWrite));
        CreateNamedExpressionIndexedTables(kikimr);

        const TString query = R"(
            $rows = SELECT Key, Tag, Value FROM Source ORDER BY Key;

            UPSERT INTO Dest (Key, Tag, Value)
                SELECT Key, Tag, Value FROM $rows WHERE Key = 1u;

            UPSERT INTO Source (Key, Tag, Value) VALUES (3u, "old", "three");

            UPSERT INTO Dest (Key, Tag, Value)
                SELECT Key + 10u, Tag, Value FROM $rows WHERE Key = 2u;

            SELECT * FROM $rows;
            SELECT Key, Tag, Value FROM Dest ORDER BY Key;
        )";

        const auto result = kikimr.GetQueryClient().ExecuteQuery(
            query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

        const TString originalRows = R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]];[[3u];["old"];["three"]]])";
        CompareYson(originalRows, FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[[1u];["old"];["one"]];[[12u];["old"];["two"]]])",
            FormatResultSetYson(result.GetResultSet(1)));
    }

    Y_UNIT_TEST_TWIN(NamedExpressionResultOnlyNoSink, EnableIndexStreamWrite) {
        TKikimrRunner kikimr(NamedExpressionIndexedSettings(EnableIndexStreamWrite));
        CreateNamedExpressionIndexedTables(kikimr);

        const TString query = R"(
            $rows = SELECT Key, Tag, Value FROM Source ORDER BY Key;

            SELECT * FROM $rows;
            SELECT * FROM $rows;

            UPSERT INTO Source (Key, Tag, Value) VALUES (1u, "new", "updated");

            SELECT * FROM $rows;
            SELECT Key, Tag, Value FROM Source ORDER BY Key;
        )";

        const auto result = kikimr.GetQueryClient().ExecuteQuery(
            query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

        const TString originalRows = R"([[[1u];["old"];["one"]];[[2u];["old"];["two"]]])";
        CompareYson(originalRows, FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(originalRows, FormatResultSetYson(result.GetResultSet(1)));
        CompareYson(originalRows, FormatResultSetYson(result.GetResultSet(2)));
        CompareYson(R"([[[1u];["new"];["updated"]];[[2u];["old"];["two"]]])",
            FormatResultSetYson(result.GetResultSet(3)));
    }

    Y_UNIT_TEST(NamedExpressionRandomChanged) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
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

    Y_UNIT_TEST(NamedExpressionRandomChanged2) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
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

    Y_UNIT_TEST(NamedExpressionRandom) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);

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

    Y_UNIT_TEST(NamedExpressionRandomInsert) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);

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

    Y_UNIT_TEST(NamedExpressionRandomDataQuery) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);

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

    Y_UNIT_TEST(NamedExpressionRandomInsertDataQuery) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);

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

    Y_UNIT_TEST_TWIN(NamedExpressionRandomUpsertIndex, UseDataQuery) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);

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

                UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)));
            }
        }
    }

    Y_UNIT_TEST_QUAD(NamedExpressionRandomUpsertReturning, UseDataQuery, EnableIndexStreamWrite) {
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableIndexStreamWrite(EnableIndexStreamWrite);
        auto settings = TKikimrSettings(app).SetWithSampleTables(true);

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

    Y_UNIT_TEST_QUAD(NamedExpressionRandomUpsertRevert, UseDataQuery, EnableIndexStreamWrite) {
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableIndexStreamWrite(EnableIndexStreamWrite);
        auto settings = TKikimrSettings(app).SetWithSampleTables(true);

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

                    UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), FormatResultSetYson(result.GetResultSet(1)));
                }
            }
        }
    }

    Y_UNIT_TEST(NamedExpressionRandomSelect) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
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
