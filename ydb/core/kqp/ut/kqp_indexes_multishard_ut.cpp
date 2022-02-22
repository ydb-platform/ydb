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

void CreateTableWithMultishardIndex(Tests::TClient& client) {
    const TString scheme =  R"(Name: "MultiShardIndexed"
        Columns { Name: "key"    Type: "Uint64" }
        Columns { Name: "fk"    Type: "Uint32" }
        Columns { Name: "value"  Type: "Utf8" }
        KeyColumnNames: ["key"])";

    NKikimrSchemeOp::TTableDescription desc;
    bool parseOk = ::google::protobuf::TextFormat::ParseFromString(scheme, &desc);
    UNIT_ASSERT(parseOk);

    auto status = client.TClient::CreateTableWithUniformShardedIndex("/Root", desc, "index", {"fk"});
    UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
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
}

}
}
