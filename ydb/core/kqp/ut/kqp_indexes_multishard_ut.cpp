#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/json/json_reader.h>

#include <util/string/printf.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScripting;

namespace {

NYdb::NTable::TDataQueryResult ExecuteDataQuery(TSession& session, const TString& query) {
    const auto txSettings = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
    return session.ExecuteDataQuery(query, txSettings).ExtractValueSync();
}

void CreateTableWithMultishardIndex(Tests::TClient& client) {
    const TString scheme =  R"(Name: "MultiShardIndexed"
        Columns { Name: "key"    Type: "Uint64" }
        Columns { Name: "fk"    Type: "Uint32" }
        Columns { Name: "value"  Type: "Utf8" }
        KeyColumnNames: ["key"]
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 3 } } } }
        SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
    )";

    NKikimrSchemeOp::TTableDescription desc;
    bool parseOk = ::google::protobuf::TextFormat::ParseFromString(scheme, &desc);
    UNIT_ASSERT(parseOk);

    auto status = client.TClient::CreateTableWithUniformShardedIndex("/Root", desc, "index", {"fk"});
    UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
}

template<bool UseNewEngine>
void FillTable(NYdb::NTable::TSession& session) {
    const TString query(Q_(R"(
        UPSERT INTO `/Root/MultiShardIndexed` (key, fk, value) VALUES
        (1, 1000000000, "v1"),
        (2, 2000000000, "v2"),
        (3, 3000000000, "v3"),
        (4, 4294967295, "v4");
    )"));

    auto result = ExecuteDataQuery(session, query);
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

}

Y_UNIT_TEST_SUITE(KqpMultishardIndex) {
    Y_UNIT_TEST_NEW_ENGINE(SortedRangeReadDesc) {
        TKikimrRunner kikimr(SyntaxV1Settings());
        CreateTableWithMultishardIndex(kikimr.GetTestClient());
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            const TString query(Q_(R"(
                UPSERT INTO `/Root/MultiShardIndexed` (key, fk, value) VALUES
                (1, 1000000000, "v1"),
                (2, 2000000000, "v2"),
                (3, 3000000000, "v3"),
                (4, 4294967295, "v4");
            )"));

            auto result = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                    .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const TString query(Q_(R"(
                SELECT * FROM `/Root/MultiShardIndexed` VIEW index ORDER BY fk DESC LIMIT 1;
            )"));

            auto result = session.ExecuteDataQuery(
                                     query,
                                     TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                              .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[[4294967295u];[4u];[\"v4\"]]]");
        }
    }

    Y_UNIT_TEST_NEW_ENGINE(YqWorksFineAfterAlterIndexTableDirectly) {
        TKikimrRunner kikimr(SyntaxV1Settings());
        CreateTableWithMultishardIndex(kikimr.GetTestClient());
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            const TString query(Q_(R"(
                UPSERT INTO `/Root/MultiShardIndexed` (key, fk, value) VALUES
                (1, 1000000000, "v1"),
                (2, 2000000000, "v2"),
                (3, 3000000000, "v3"),
                (4, 4294967295, "v4");
            )"));

            auto result = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                    .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        kikimr.GetTestServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.push_back("root@builtin");

        { // without token request is forbidded
            Tests::TClient& client = kikimr.GetTestClient();
            const TString scheme =  R"(
                Name: "indexImplTable"
                PartitionConfig {
                    PartitioningPolicy {
                        MinPartitionsCount: 1
                        SizeToSplit: 100500
                        FastSplitSettings {
                            SizeThreshold: 100500
                            RowCountThreshold: 100500
                        }
                    }
                }
            )";
            auto result = client.AlterTable("/Root/MultiShardIndexed/index", scheme, "user@builtin");
            UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_ERROR, "User must not be able to alter index impl table");
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetErrorReason(), "Administrative access denied");
        }

        { // with root token request is accepted
            Tests::TClient& client = kikimr.GetTestClient();
            const TString scheme =  R"(
                Name: "indexImplTable"
                PartitionConfig {
                    PartitioningPolicy {
                        MinPartitionsCount: 1
                        SizeToSplit: 100500
                        FastSplitSettings {
                            SizeThreshold: 100500
                            RowCountThreshold: 100500
                        }
                    }
                }
            )";
            auto result = client.AlterTable("/Root/MultiShardIndexed/index", scheme, "root@builtin");
            UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK, "Super user must be able to alter partition config");
        }

        { // after alter yql works fine
            const TString query(Q_(R"(
                SELECT * FROM `/Root/MultiShardIndexed` VIEW index ORDER BY fk DESC LIMIT 1;
            )"));

            auto result = session.ExecuteDataQuery(
                                     query,
                                     TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                              .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[[4294967295u];[4u];[\"v4\"]]]");
        }

        { // write request works well too
            const TString query(Q_(R"(
                UPSERT INTO `/Root/MultiShardIndexed` (key, fk, value) VALUES
                (1, 1000000000, "v1"),
                (2, 2000000000, "v2"),
                (3, 3000000000, "v3"),
                (4, 4294967295, "v4");
            )"));

            auto result = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                    .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        { // just for sure, public api got error when alter index
            auto settings = NYdb::NTable::TAlterTableSettings()
                .BeginAlterPartitioningSettings()
                    .SetPartitionSizeMb(50)
                    .SetMinPartitionsCount(4)
                    .SetMaxPartitionsCount(5)
                .EndAlterPartitioningSettings();

            auto result = session.AlterTable("/Root/MultiShardIndexed/index/indexImplTable", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        }

        { // however public api is able to perform alter index if user has AlterSchema right and user is a member of the list AdministrationAllowedSIDs
            auto clSettings = NYdb::NTable::TClientSettings().AuthToken("root@builtin").UseQueryCache(false);
            auto client =  NYdb::NTable::TTableClient(kikimr.GetDriver(), clSettings);
            auto session = client.CreateSession().GetValueSync().GetSession();

            auto settings = NYdb::NTable::TAlterTableSettings()
                .BeginAlterPartitioningSettings()
                    .SetPartitionSizeMb(50)
                    .SetMinPartitionsCount(4)
                    .SetMaxPartitionsCount(5)
                .EndAlterPartitioningSettings();

            auto result = session.AlterTable("/Root/MultiShardIndexed/index/indexImplTable", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

    }

    Y_UNIT_TEST_QUAD(SortByPk, WithMvcc, UseNewEngine) {
        auto serverSettings = TKikimrSettings()
            .SetEnableMvcc(WithMvcc)
            .SetEnableMvccSnapshotReads(WithMvcc);
        TKikimrRunner kikimr(serverSettings);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        CreateTableWithMultishardIndex(kikimr.GetTestClient());
        FillTable<UseNewEngine>(session);

        AssertSuccessResult(session.ExecuteDataQuery(Q1_(R"(
            UPSERT INTO `/Root/MultiShardIndexed` (key, fk, value) VALUES
                (10u, 1000, "NewValue1"),
                (11u, 1001, "NewValue2"),
                (12u, 1002, "NewValue3"),
                (13u, 1003, "NewValue4"),
                (14u, 1004, "NewValue5"),
                (15u, 1005, "NewValue6"),
                (101u, 1011, "NewValue7");
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync());

        auto query = Q1_(R"(
            SELECT * FROM MultiShardIndexed VIEW index
            WHERE fk > 100
            ORDER BY fk, key
            LIMIT 100;
        )");

        auto explainResult = session.ExplainDataQuery(query).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

        // Cerr << explainResult.GetPlan() << Endl;

        if (UseNewEngine) {
            NJson::TJsonValue plan;
            NJson::ReadJsonTree(explainResult.GetPlan(), &plan, true);
            auto node = FindPlanNodeByKv(plan, "Name", "TopSort");
            UNIT_ASSERT(node.IsDefined());
        }

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[1000u];[10u];["NewValue1"]];
            [[1001u];[11u];["NewValue2"]];
            [[1002u];[12u];["NewValue3"]];
            [[1003u];[13u];["NewValue4"]];
            [[1004u];[14u];["NewValue5"]];
            [[1005u];[15u];["NewValue6"]];
            [[1011u];[101u];["NewValue7"]];
            [[1000000000u];[1u];["v1"]];
            [[2000000000u];[2u];["v2"]];
            [[3000000000u];[3u];["v3"]];
            [[4294967295u];[4u];["v4"]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }
}

}
}
