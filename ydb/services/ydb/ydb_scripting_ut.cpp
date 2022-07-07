#include "ydb_common_ut.h"

#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

using namespace NYdb;

Y_UNIT_TEST_SUITE(YdbScripting) {
    void DoBasicTest(bool v1) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto client = NYdb::NScripting::TScriptingClient(connection);

        const TString v1Prefix = R"(
            --!syntax_v1
        )";

        const TString sql = R"(
            CREATE TABLE `/Root/TestTable` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            COMMIT;

            REPLACE INTO `/Root/TestTable` (Key, Value) VALUES
                (1, "One"),
                (2, "Two");
            COMMIT;

            SELECT * FROM `/Root/TestTable`;
        )";

        auto result = client.ExecuteYqlScript((v1 ? v1Prefix : TString()) + sql).GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(result.IsSuccess());

        TVector<TResultSet> resultSets = result.GetResultSets();
        UNIT_ASSERT_EQUAL(resultSets.size(), 1);

        TResultSetParser rsParser(resultSets[0]);
        UNIT_ASSERT(rsParser.TryNextRow());
        UNIT_ASSERT_EQUAL(rsParser.ColumnParser(0).GetOptionalUint64(), 1ul);
        UNIT_ASSERT_EQUAL(rsParser.ColumnParser(1).GetOptionalString(), "One");
        UNIT_ASSERT(rsParser.TryNextRow());
        UNIT_ASSERT_EQUAL(rsParser.ColumnParser(0).GetOptionalUint64(), 2ul);
        UNIT_ASSERT_EQUAL(rsParser.ColumnParser(1).GetOptionalString(), "Two");
    }

    Y_UNIT_TEST(BasicV0) {
        DoBasicTest(false);
    }

    Y_UNIT_TEST(BasicV1) {
        DoBasicTest(true);
    }

    Y_UNIT_TEST(MultiResults) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto client = NYdb::NScripting::TScriptingClient(connection);

        auto result = client.ExecuteYqlScript(R"(
            PRAGMA kikimr.ScanQuery = "false";

            CREATE TABLE `/Root/TestTable` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            COMMIT;

            REPLACE INTO `/Root/TestTable` (Key, Value) VALUES
                (1, "One"),
                (2, "Two");
            COMMIT;

            SELECT Key, Value FROM `/Root/TestTable`;
            COMMIT;

            REPLACE INTO `/Root/TestTable` (Key, Value) VALUES
                (1, "OneNew");
            COMMIT;

            SELECT Value, Key FROM `/Root/TestTable` WHERE Key = 1;

            SELECT * FROM `/Root/TestTable`;
        )").GetValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(result.IsSuccess());

        TVector<TResultSet> resultSets = result.GetResultSets();
        UNIT_ASSERT_EQUAL(resultSets.size(), 3);

        UNIT_ASSERT_EQUAL(resultSets[0].RowsCount(), 2);
        TResultSetParser rs0(resultSets[0]);
        UNIT_ASSERT(rs0.TryNextRow());
        UNIT_ASSERT_EQUAL(rs0.ColumnParser(0).GetOptionalUint64(), 1ul);
        UNIT_ASSERT_EQUAL(rs0.ColumnParser(1).GetOptionalString(), "One");
        UNIT_ASSERT(rs0.TryNextRow());
        UNIT_ASSERT_EQUAL(rs0.ColumnParser(0).GetOptionalUint64(), 2ul);
        UNIT_ASSERT_EQUAL(rs0.ColumnParser(1).GetOptionalString(), "Two");

        UNIT_ASSERT_EQUAL(resultSets[1].RowsCount(), 1);
        TResultSetParser rs1(resultSets[1]);
        UNIT_ASSERT(rs1.TryNextRow());
        UNIT_ASSERT_EQUAL(rs1.ColumnParser(0).GetOptionalString(), "OneNew");
        UNIT_ASSERT_EQUAL(rs1.ColumnParser(1).GetOptionalUint64(), 1ul);

        UNIT_ASSERT_EQUAL(resultSets[2].RowsCount(), 2);
        TResultSetParser rs2(resultSets[2]);
        UNIT_ASSERT(rs2.TryNextRow());
        UNIT_ASSERT_EQUAL(rs2.ColumnParser(0).GetOptionalUint64(), 1ul);
        UNIT_ASSERT_EQUAL(rs2.ColumnParser(1).GetOptionalString(), "OneNew");
        UNIT_ASSERT(rs2.TryNextRow());
        UNIT_ASSERT_EQUAL(rs2.ColumnParser(0).GetOptionalUint64(), 2ul);
        UNIT_ASSERT_EQUAL(rs2.ColumnParser(1).GetOptionalString(), "Two");
    }

    Y_UNIT_TEST(Params) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto client = NYdb::NScripting::TScriptingClient(connection);

        auto params = client.GetParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").OptionalUint64(10)
                        .AddMember("Value").OptionalString("Ten")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("Key").OptionalUint64(20)
                        .AddMember("Value").OptionalString("Twenty")
                    .EndStruct()
                .EndList()
                .Build()
            .Build();

        auto result = client.ExecuteYqlScript(R"(
            DECLARE $rows AS
                List<Struct<
                    Key: Uint64?,
                    Value: String?>>;

            CREATE TABLE `/Root/TestTable` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            COMMIT;

            REPLACE INTO `/Root/TestTable`
            SELECT * FROM AS_TABLE($rows);
            COMMIT;

            SELECT * FROM `/Root/TestTable`;
        )", params).GetValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_EQUAL(result.GetResultSets().size(), 1);
        UNIT_ASSERT_EQUAL(result.GetResultSets()[0].RowsCount(), 2);
        TResultSetParser rs0(result.GetResultSets()[0]);
        UNIT_ASSERT(rs0.TryNextRow());
        UNIT_ASSERT_EQUAL(rs0.ColumnParser(1).GetOptionalString(), "Ten");
        UNIT_ASSERT(rs0.TryNextRow());
        UNIT_ASSERT_EQUAL(rs0.ColumnParser(1).GetOptionalString(), "Twenty");
    }
}
