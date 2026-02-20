// we define this to allow using sdk build info.
#define INCLUDE_YDB_INTERNAL_H
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <util/system/getpid.h>
#include <ydb/core/sys_view/service/query_history.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/grpc_connections/grpc_connections.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace {
    ui64 SelectCompileCacheCount(TTableClient& tableClient) {
        auto session = tableClient.CreateSession().GetValueSync();
        UNIT_ASSERT_C(session.IsSuccess(), session.GetIssues().ToString());

        const TString query = R"(SELECT COUNT(*) AS cnt FROM `/Root/.sys/compile_cache_queries`;)";

        auto result = session.GetSession().ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetStatus()); //result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        NYdb::TResultSetParser parser(resultSet);
        UNIT_ASSERT(parser.TryNextRow());
        auto value = parser.ColumnParser("cnt").GetUint64();
        return value;
    }


} // namespace
Y_UNIT_TEST_SUITE(KqpSystemView) {

    Y_UNIT_TEST(Join) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();

        while (true) {
            auto it = client.StreamExecuteScanQuery(
                "select NodeId from `/Root/.sys/partition_stats` where Path = '/Root/KeyValue' limit 1"
            ).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            if (StreamResultToYson(it) == "[[#]]") {
                ::Sleep(TDuration::Seconds(1));
                Cerr << "waiting..." << Endl;
                Cerr.Flush();
            } else {
                break;
            }
        }

        auto it = client.StreamExecuteScanQuery(R"(
            --!syntax_v1
            select n.Host, ps.Path, ps.RowCount
            from `/Root/.sys/partition_stats` as ps
            join `/Root/.sys/nodes` as n
            on ps.NodeId = n.NodeId
            where ps.Path = '/Root/KeyValue'
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(R"([[["::1"];["/Root/KeyValue"];[2u]]])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(Sessions) {
        TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        settings.SetAuthToken("root@builtin");  // root@builtin becomes cluster admin
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();
        auto tableClient = kikimr.GetTableClient();
        const size_t sessionsCount = 50;
        std::vector<NYdb::NQuery::TSession> sessionsSet;
        for(ui32 i = 0; i < sessionsCount; i++) {
            sessionsSet.emplace_back(std::move(client.GetSession().GetValueSync().GetSession()));
        }

        Cerr << kikimr.GetTestServer().GetRuntime()->GetNodeId() << Endl;

        ui32 nodeId = kikimr.GetTestServer().GetRuntime()->GetNodeId();

        std::sort(sessionsSet.begin(), sessionsSet.end(), [](const NYdb::NQuery::TSession& a, const NYdb::NQuery::TSession& b){
            return a.GetId() < b.GetId();
        });

        std::vector<TString> stringParts;
        for(ui32 i = 0; i < sessionsCount - 1; i++) {
            stringParts.push_back(Sprintf("[[\"%s\"];[\"IDLE\"];[\"<empty>\"];[%du];[\"\"]];", sessionsSet[i].GetId().data(), nodeId));
        }

        {
            auto result = sessionsSet.front().ExecuteQuery(R"(--!syntax_v1
select 1;)", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TString otherSessions = JoinSeq("\n", stringParts);

        Cerr << NYdb::CreateSDKBuildInfo() << Endl;

        {
            auto result = sessionsSet.back().ExecuteQuery(R"(--!syntax_v1
select SessionId, State, ApplicationName, NodeId, Query from `/Root/.sys/query_sessions` order by SessionId;)", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(Sprintf(R"([
                %s
                [["%s"];["EXECUTING"];["<empty>"];[%du];["--!syntax_v1\nselect SessionId, State, ApplicationName, NodeId, Query from `/Root/.sys/query_sessions` order by SessionId;"]]
            ])", otherSessions.data(), sessionsSet.back().GetId().data(), nodeId), FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto result = sessionsSet.back().ExecuteQuery(Sprintf(R"(--!syntax_v1
select SessionId, State, ApplicationName, NodeId, Query from `/Root/.sys/query_sessions` WHERE StartsWith(SessionId, "ydb://session/3?node_id=%d");)", nodeId), NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            CompareYson(Sprintf(R"([
                %s
                [["%s"];["EXECUTING"];["<empty>"];[%du];["--!syntax_v1\nselect SessionId, State, ApplicationName, NodeId, Query from `/Root/.sys/query_sessions` WHERE StartsWith(SessionId, \"ydb://session/3?node_id=%d\");"]]
            ])", otherSessions.data(), sessionsSet.back().GetId().data(), nodeId, nodeId), FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto result = sessionsSet.back().ExecuteQuery(R"(--!syntax_v1
select ClientSdkBuildInfo, Count(SessionId) from `/Root/.sys/query_sessions` group by ClientSdkBuildInfo;)", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(Sprintf(R"([
                [["%s"];%du]
            ])", NYdb::CreateSDKBuildInfo().data(), (ui32)sessionsSet.size()), FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto result = sessionsSet.back().ExecuteQuery(R"(--!syntax_v1
select ClientPID, Count(SessionId) from `/Root/.sys/query_sessions` group by ClientPID;)", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(Sprintf(R"([
                [["%d"];%du]
            ])", (int)GetPID(), (ui32)sessionsSet.size()), FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto result = sessionsSet.back().ExecuteQuery(R"(--!syntax_v1
select UserSID, Count(SessionId) from `/Root/.sys/query_sessions` group by UserSID;)", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(Sprintf(R"([
                [["root@builtin"];%du]
            ])", (ui32)sessionsSet.size()), FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto result = sessionsSet.back().ExecuteQuery(R"(--!syntax_v1
select QueryCount, Count(SessionId) from `/Root/.sys/query_sessions` group by QueryCount order by QueryCount;)", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(Sprintf(R"([
                [[0u];%du];
                [[1u];1u];
                [[6u];1u]
            ])", (ui32)sessionsSet.size() - 2), FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto result = sessionsSet.back().ExecuteQuery(R"(--!syntax_v1
select Count(SessionId) from `/Root/.sys/query_sessions` where ClientUserAgent LIKE 'grpc%';)", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(Sprintf(R"([
                [%du]
            ])", (ui32)sessionsSet.size()), FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto result = sessionsSet.back().ExecuteQuery(R"(--!syntax_v1
select Count(SessionId) from `/Root/.sys/query_sessions` where ClientAddress LIKE '%:%';)", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(Sprintf(R"([
                [%du]
            ])", (ui32)sessionsSet.size()), FormatResultSetYson(result.GetResultSet(0)));
        }

        {

            auto result = sessionsSet.back().ExecuteQuery(Sprintf(R"(--!syntax_v1
$date_format = DateTime::Format("%s");
select SessionId from `/Root/.sys/query_sessions`
where
SessionId LIKE Utf8("%s") and
StartsWith($date_format(SessionStartAt), cast(DateTime::GetYear(CurrentUtcTimestamp()) as utf8)) and
StartsWith($date_format(StateChangeAt), cast(DateTime::GetYear(CurrentUtcTimestamp()) as utf8)) and
StartsWith($date_format(QueryStartAt), cast(DateTime::GetYear(CurrentUtcTimestamp()) as utf8))
order by SessionId;)", "%Y-%m-%d %H:%M:%S %Z", sessionsSet.back().GetId().data()), NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(Sprintf(R"([
                [["%s"]]
            ])", sessionsSet.back().GetId().data()), FormatResultSetYson(result.GetResultSet(0)));
        }

        {

            auto result = sessionsSet.back().ExecuteQuery(Sprintf(R"(--!syntax_v1
$date_format = DateTime::Format("%s");
select SessionId, QueryStartAt from `/Root/.sys/query_sessions`
where
SessionId LIKE Utf8("%s") and
StartsWith($date_format(SessionStartAt), cast(DateTime::GetYear(CurrentUtcTimestamp()) as utf8)) and
StartsWith($date_format(StateChangeAt), cast(DateTime::GetYear(CurrentUtcTimestamp()) as utf8))
order by SessionId;)", "%Y-%m-%d %H:%M:%S %Z", sessionsSet.front().GetId().data()), NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(Sprintf(R"([
                [["%s"];#]
            ])", sessionsSet.front().GetId().data()), FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto result = sessionsSet.back().ExecuteQuery(Sprintf(R"(
                --!syntax_v1
                select SessionId
                from `/Root/.sys/query_sessions`
                where SessionId="%s"
            )", sessionsSet.back().GetId().data()), NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

            CompareYson(Sprintf(R"([
                [["%s"]]
            ])", sessionsSet.back().GetId().data()), FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(Sprintf(R"(
                --!syntax_v1
                select SessionId
                from `/Root/.sys/query_sessions`
                where SessionId="%s"
            )", sessionsSet.back().GetId().data())).GetValueSync();

            CompareYson(Sprintf(R"([
                [["%s"]]
            ])", sessionsSet.back().GetId().data()), StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(PartitionStatsSimple) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();

        const auto describeResult = kikimr.GetTestClient().Describe(
            kikimr.GetTestServer().GetRuntime(), "/Root/TwoShard");
        const auto startPathId = describeResult.GetPathId();

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT OwnerId, PartIdx, Path, PathId
            FROM `/Root/.sys/partition_stats`
            ORDER BY PathId, PartIdx;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        TStringBuilder expectedYson;
        expectedYson << "[" << Endl;

        for (size_t i = 0; i < 2; ++i) {
            expectedYson << Sprintf(R"([
                [72057594046644480u];[%luu];["/Root/TwoShard"];[%luu]
            ];)", i, startPathId)  << Endl;
        }

        for (size_t i = 0; i < 8; ++i) {
            expectedYson << Sprintf(R"([
                [72057594046644480u];[%luu];["/Root/EightShard"];[%luu]
            ];)", i, startPathId + 1)  << Endl;
        }

        for (size_t i = 0; i < 3; ++i) {
            expectedYson << Sprintf(R"([
                [72057594046644480u];[%luu];["/Root/Logs"];[%luu]
            ];)", i, startPathId + 2)  << Endl;
        }

        for (size_t i = 0; i < 10; ++i) {
            expectedYson << Sprintf(R"([
                [72057594046644480u];[%luu];["/Root/BatchUpload"];[%luu]
            ];)", i, startPathId + 3)  << Endl;
        }

        expectedYson << Sprintf(R"(
            [[72057594046644480u];[0u];["/Root/KeyValue"];[%luu]];
            [[72057594046644480u];[0u];["/Root/KeyValue2"];[%luu]];
            [[72057594046644480u];[0u];["/Root/KeyValueLargePartition"];[%luu]];
            [[72057594046644480u];[0u];["/Root/Test"];[%luu]];
        )", startPathId + 4, startPathId + 5, startPathId + 6, startPathId + 7)  << Endl;

        for (size_t i = 0; i < 2; ++i) {
            expectedYson << Sprintf(R"([
                [72057594046644480u];[%luu];["/Root/Join1"];[%luu]
            ];)", i, startPathId + 8)  << Endl;
        }

        for (size_t i = 0; i < 2; ++i) {
            expectedYson << Sprintf(R"([
                [72057594046644480u];[%luu];["/Root/Join2"];[%luu]
            ];)", i, startPathId + 9)  << Endl;
        }

        for (size_t i = 0; i < 3; ++i) {
            expectedYson << Sprintf(R"([
                [72057594046644480u];[%luu];["/Root/ReorderKey"];[%luu]
            ];)", i, startPathId + 10)  << Endl;
        }

        for (size_t i = 0; i < 5; ++i) {
            expectedYson << Sprintf(R"([
                [72057594046644480u];[%luu];["/Root/ReorderOptionalKey"];[%luu]
            ];)", i, startPathId + 11)  << Endl;
        }

        expectedYson << "]";

        CompareYson(expectedYson, StreamResultToYson(it));
    }

    Y_UNIT_TEST(PartitionStatsRanges) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();
        const auto describeResult = kikimr.GetTestClient().Describe(
            kikimr.GetTestServer().GetRuntime(), "Root/BatchUpload");
        const auto tablePathId = describeResult.GetPathId();

        auto it = client.StreamExecuteScanQuery(Sprintf(R"(
            SELECT OwnerId, PartIdx, Path, PathId
            FROM `/Root/.sys/partition_stats`
            WHERE
            OwnerId = 72057594046644480ul
            AND PathId = %luu
            AND (PartIdx BETWEEN 0 AND 2 OR PartIdx BETWEEN 6 AND 9)
            ORDER BY PathId, PartIdx;
        )", tablePathId)).GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        TStringBuilder expectedYson;
        expectedYson << "[" << Endl;
        for (size_t i : {0, 1, 2, 6, 7, 8, 9}) {
            expectedYson << Sprintf(R"([
                [72057594046644480u];[%luu];["/Root/BatchUpload"];[%luu]
            ];)", i, tablePathId)  << Endl;
        }
        expectedYson << "]";

        CompareYson(expectedYson, StreamResultToYson(it));
    }

    Y_UNIT_TEST(PartitionStatsParametricRanges) {
        TKikimrRunner kikimr;
        const auto describeResult = kikimr.GetTestClient().Describe(
            kikimr.GetTestServer().GetRuntime(), "Root/BatchUpload");
        const auto tablePathId = describeResult.GetPathId();

        auto client = kikimr.GetTableClient();

        auto paramsBuilder = client.GetParamsBuilder();
        auto params = paramsBuilder
            .AddParam("$l1").Int32(0).Build()
            .AddParam("$r1").Int32(2).Build()
            .AddParam("$l2").Int32(6).Build()
            .AddParam("$r2").Int32(9).Build().Build();

        auto it = client.StreamExecuteScanQuery(Sprintf(R"(
            DECLARE $l1 AS Int32;
            DECLARE $r1 AS Int32;

            DECLARE $l2 AS Int32;
            DECLARE $r2 AS Int32;

            SELECT OwnerId, PartIdx, Path, PathId
            FROM `/Root/.sys/partition_stats`
            WHERE
            OwnerId = 72057594046644480ul
            AND PathId = %luu
            AND (PartIdx BETWEEN $l1 AND $r1 OR PartIdx BETWEEN $l2 AND $r2)
            ORDER BY PathId, PartIdx;
        )", tablePathId), params).GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        TStringBuilder expectedYson;
        expectedYson << "[" << Endl;
        for (size_t i : {0, 1, 2, 6, 7, 8, 9}) {
            expectedYson << Sprintf(R"([
                [72057594046644480u];[%luu];["/Root/BatchUpload"];[%luu]
            ];)", i, tablePathId)  << Endl;
        }
        expectedYson << "]";

        CompareYson(expectedYson, StreamResultToYson(it));
    }

    Y_UNIT_TEST(PartitionStatsRange1) {
        TKikimrRunner kikimr;
        const auto describeResult = kikimr.GetTestClient().Describe(
            kikimr.GetTestServer().GetRuntime(), "Root/BatchUpload");
        const auto startPathId = describeResult.GetPathId();

        auto client = kikimr.GetTableClient();

        TString query = Sprintf(R"(
            SELECT OwnerId, PathId, PartIdx, Path
            FROM `/Root/.sys/partition_stats`
            WHERE OwnerId = 72057594046644480ul AND PathId > %luu AND PathId <= %luu
            ORDER BY PathId, PartIdx;
        )", startPathId, startPathId + 5);


        TString expectedYson = Sprintf(R"([
            [[72057594046644480u];[%luu];[0u];["/Root/KeyValue"]];
            [[72057594046644480u];[%luu];[0u];["/Root/KeyValue2"]];
            [[72057594046644480u];[%luu];[0u];["/Root/KeyValueLargePartition"]];
            [[72057594046644480u];[%luu];[0u];["/Root/Test"]];
            [[72057594046644480u];[%luu];[0u];["/Root/Join1"]];
            [[72057594046644480u];[%luu];[1u];["/Root/Join1"]]
        ])", startPathId + 1, startPathId + 2, startPathId + 3, startPathId + 4, startPathId + 5, startPathId + 5);

        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(expectedYson, StreamResultToYson(it));
    }

    Y_UNIT_TEST(PartitionStatsRange2) {
        TKikimrRunner kikimr;
        const auto describeResult = kikimr.GetTestClient().Describe(
            kikimr.GetTestServer().GetRuntime(), "Root/KeyValue");
        const auto startPathId = describeResult.GetPathId();

        auto client = kikimr.GetTableClient();
        TString query = Sprintf(R"(
            SELECT OwnerId, PathId, PartIdx, Path
            FROM `/Root/.sys/partition_stats`
            WHERE OwnerId = 72057594046644480ul AND PathId >= %luu AND PathId < %luu
            ORDER BY PathId, PartIdx;
        )", startPathId, startPathId + 3);

        TString expectedYson = Sprintf(R"([
            [[72057594046644480u];[%luu];[0u];["/Root/KeyValue"]];
            [[72057594046644480u];[%luu];[0u];["/Root/KeyValue2"]];
            [[72057594046644480u];[%luu];[0u];["/Root/KeyValueLargePartition"]]
        ])", startPathId, startPathId + 1,  startPathId + 2);

        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(expectedYson, StreamResultToYson(it));
    }

    Y_UNIT_TEST(PartitionStatsRange3) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();
        const auto describeResult = kikimr.GetTestClient().Describe(
            kikimr.GetTestServer().GetRuntime(), "Root/BatchUpload");
        const auto tablePathId = describeResult.GetPathId();

        auto it = client.StreamExecuteScanQuery(Sprintf(R"(
            SELECT OwnerId, PathId, PartIdx, Path
            FROM `/Root/.sys/partition_stats`
            WHERE OwnerId = 72057594046644480ul AND PathId = %luu AND PartIdx > 1u AND PartIdx < 7u
            ORDER BY PathId, PartIdx;
        )", tablePathId)).GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        TStringBuilder expectedYson;
        expectedYson << "[" << Endl;
        for (size_t i = 2; i < 7; ++i) {
            expectedYson << Sprintf(R"([
                [72057594046644480u];[%luu];[%luu];["/Root/BatchUpload"]
            ];)", tablePathId, i)  << Endl;
        }
        expectedYson << "]";

        CompareYson(expectedYson, StreamResultToYson(it));
    }

    Y_UNIT_TEST(NodesSimple) {
        TKikimrRunner kikimr("", KikimrDefaultUtDomainRoot, 3);
        auto client = kikimr.GetTableClient();

        ui32 offset = kikimr.GetTestServer().GetRuntime()->GetNodeId(0);

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT NodeId, Host
            FROM `/Root/.sys/nodes`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto expected = Sprintf(R"([
            [[%du];["::1"]];
            [[%du];["::1"]];
            [[%du];["::1"]]
        ])", offset, offset + 1, offset + 2);

        CompareYson(expected, StreamResultToYson(it));
    }

    Y_UNIT_TEST(NodesRange1) {
        TKikimrRunner kikimr("", KikimrDefaultUtDomainRoot, 5);
        auto client = kikimr.GetTableClient();

        ui32 offset = kikimr.GetTestServer().GetRuntime()->GetNodeId(0);

        auto query = Sprintf(R"(
            SELECT NodeId, Host
            FROM `/Root/.sys/nodes`
            WHERE NodeId >= %du AND NodeId <= %du
        )", offset + 1, offset + 3);

        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto expected = Sprintf(R"([
            [[%du];["::1"]];
            [[%du];["::1"]];
            [[%du];["::1"]]
        ])", offset + 1, offset + 2, offset + 3);

        CompareYson(expected, StreamResultToYson(it));
    }

    Y_UNIT_TEST(NodesRange2) {
        TKikimrRunner kikimr("", KikimrDefaultUtDomainRoot, 5);
        auto client = kikimr.GetTableClient();

        ui32 offset = kikimr.GetTestServer().GetRuntime()->GetNodeId(0);

        auto query = Sprintf(R"(
            SELECT NodeId, Host
            FROM `/Root/.sys/nodes`
            WHERE NodeId > %du AND NodeId < %du
        )", offset + 1, offset + 3);

        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto expected = Sprintf(R"([
            [[%du];["::1"]];
        ])", offset + 2);

        CompareYson(expected, StreamResultToYson(it));
    }

    Y_UNIT_TEST(NodesOrderByDesc) {
        // Test to reproduce issue #12585: ORDER BY DESC doesn't work for sys views
        // The sys view actors ignore the direction flag and don't guarantee order
        TKikimrRunner kikimr("", KikimrDefaultUtDomainRoot, 5);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        ui32 offset = kikimr.GetTestServer().GetRuntime()->GetNodeId(0);

        auto result = session.ExecuteQuery(R"(--!syntax_v1
            SELECT NodeId, Host
            FROM `/Root/.sys/nodes`
            ORDER BY NodeId DESC
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // Collect all results
        TVector<ui32> nodeIds;
        auto resultSet = result.GetResultSet(0);
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto nodeId = parser.ColumnParser("NodeId").GetOptionalUint32().value();
            nodeIds.push_back(nodeId);
        }

        // Verify we got all 5 nodes
        UNIT_ASSERT_VALUES_EQUAL(nodeIds.size(), 5);

        // Verify results are in descending order (this should fail if the bug exists)
        // According to issue #12585, sys view actors ignore the direction flag
        // and don't guarantee order, so this assertion should fail
        for (size_t i = 1; i < nodeIds.size(); ++i) {
            UNIT_ASSERT_C(nodeIds[i - 1] >= nodeIds[i],
                TStringBuilder() << "Results not in descending order: "
                << "nodeIds[" << (i - 1) << "] = " << nodeIds[i - 1]
                << " < nodeIds[" << i << "] = " << nodeIds[i]
                << ". ORDER BY DESC is being ignored by sys view actors.");
        }

        // Verify exact expected order: offset+4, offset+3, offset+2, offset+1, offset
        UNIT_ASSERT_VALUES_EQUAL(nodeIds[0], offset + 4);
        UNIT_ASSERT_VALUES_EQUAL(nodeIds[1], offset + 3);
        UNIT_ASSERT_VALUES_EQUAL(nodeIds[2], offset + 2);
        UNIT_ASSERT_VALUES_EQUAL(nodeIds[3], offset + 1);
        UNIT_ASSERT_VALUES_EQUAL(nodeIds[4], offset);
    }

    Y_UNIT_TEST(PartitionStatsOrderByDesc) {
        // Test ORDER BY DESC for partition_stats sys view
        // Primary key: OwnerId, PathId, PartIdx, FollowerId
        TKikimrRunner kikimr;
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        auto result = session.ExecuteQuery(R"(--!syntax_v1
            SELECT OwnerId, PathId, PartIdx, FollowerId, Path
            FROM `/Root/.sys/partition_stats`
            ORDER BY OwnerId DESC, PathId DESC, PartIdx DESC, FollowerId DESC
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // Collect all results
        struct TPartitionKey {
            ui64 OwnerId;
            ui64 PathId;
            ui64 PartIdx;
            ui32 FollowerId;
        };
        TVector<TPartitionKey> partitionKeys;
        auto resultSet = result.GetResultSet(0);
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto ownerId = parser.ColumnParser("OwnerId").GetOptionalUint64().value();
            auto pathId = parser.ColumnParser("PathId").GetOptionalUint64().value();
            auto partIdx = parser.ColumnParser("PartIdx").GetOptionalUint64().value();
            auto followerId = parser.ColumnParser("FollowerId").GetOptionalUint32().value();
            partitionKeys.push_back({ownerId, pathId, partIdx, followerId});
        }

        // Verify we got some results
        UNIT_ASSERT_C(partitionKeys.size() > 0, "Expected at least one partition");

        // Verify results are in descending order by OwnerId, PathId, PartIdx, FollowerId
        for (size_t i = 1; i < partitionKeys.size(); ++i) {
            const auto& prev = partitionKeys[i - 1];
            const auto& curr = partitionKeys[i];

            auto prevKey = std::tie(prev.OwnerId, prev.PathId, prev.PartIdx, prev.FollowerId);
            auto currKey = std::tie(curr.OwnerId, curr.PathId, curr.PartIdx, curr.FollowerId);

            UNIT_ASSERT_C(prevKey >= currKey,
                TStringBuilder() << "Results not in descending order: "
                << "partitionKeys[" << (i - 1) << "] = (" << prev.OwnerId << ", " << prev.PathId << ", " << prev.PartIdx << ", " << prev.FollowerId << ")"
                << " < partitionKeys[" << i << "] = (" << curr.OwnerId << ", " << curr.PathId << ", " << curr.PartIdx << ", " << curr.FollowerId << ")"
                << ". ORDER BY DESC is being ignored by sys view actors.");
        }
    }

    Y_UNIT_TEST(QuerySessionsOrderByDesc) {
        // Test ORDER BY DESC for query_sessions sys view
        TKikimrSettings settings;
        settings.SetWithSampleTables(false);
        settings.SetAuthToken("root@builtin");
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        // Create some sessions to have data
        const size_t sessionsCount = 5;
        std::vector<NYdb::NQuery::TSession> sessionsSet;
        for(ui32 i = 0; i < sessionsCount; i++) {
            sessionsSet.emplace_back(std::move(client.GetSession().GetValueSync().GetSession()));
        }

        // Wait a bit for sessions to be registered
        ::Sleep(TDuration::MilliSeconds(100));

        auto result = session.ExecuteQuery(R"(--!syntax_v1
            SELECT SessionId, State, NodeId
            FROM `/Root/.sys/query_sessions`
            ORDER BY SessionId DESC
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // Collect all results
        TVector<std::string> sessionIds;
        auto resultSet = result.GetResultSet(0);
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto sessionId = parser.ColumnParser("SessionId").GetOptionalUtf8().value();
            sessionIds.push_back(sessionId);
        }

        // Verify we got some results
        UNIT_ASSERT_C(sessionIds.size() > 0, "Expected at least one session");

        // Verify results are in descending order (lexicographically)
        for (size_t i = 1; i < sessionIds.size(); ++i) {
            UNIT_ASSERT_C(sessionIds[i - 1] >= sessionIds[i],
                TStringBuilder() << "Results not in descending order: "
                << "sessionIds[" << (i - 1) << "] = \"" << sessionIds[i - 1] << "\""
                << " < sessionIds[" << i << "] = \"" << sessionIds[i] << "\""
                << ". ORDER BY DESC is being ignored by sys view actors.");
        }
    }

    Y_UNIT_TEST(CompileCacheQueriesOrderByDesc) {
        // Test ORDER BY DESC for compile_cache_queries sys view
        // Primary key: NodeId, QueryId
        auto serverSettings = TKikimrSettings().SetKqpSettings({ NKikimrKqp::TKqpSetting() });
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableCompileCacheView(true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        // Execute some queries to populate compile cache
        auto tableClient = kikimr.GetTableClient();
        for (ui32 i = 0; i < 3; ++i) {
            auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
            auto paramsBuilder = TParamsBuilder();
            paramsBuilder.AddParam("$k").Uint64(i).Build();
            auto executedResult = tableSession.ExecuteDataQuery(
                R"(DECLARE $k AS Uint64;
                SELECT COUNT(*) FROM `/Root/EightShard` WHERE Key = $k;)",
                TTxControl::BeginTx().CommitTx(),
                paramsBuilder.Build()
            ).GetValueSync();
            UNIT_ASSERT_C(executedResult.IsSuccess(), executedResult.GetIssues().ToString());
        }

        // Wait a bit for compile cache to be updated
        ::Sleep(TDuration::MilliSeconds(100));

        auto result = session.ExecuteQuery(R"(--!syntax_v1
            SELECT NodeId, QueryId, Query, CompilationDurationMs
            FROM `/Root/.sys/compile_cache_queries`
            ORDER BY NodeId DESC, QueryId DESC
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // Collect all results
        struct TCompileCacheKey {
            ui32 NodeId;
            std::string QueryId;
        };
        TVector<TCompileCacheKey> compileCacheKeys;
        auto resultSet = result.GetResultSet(0);
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto nodeId = parser.ColumnParser("NodeId").GetOptionalUint32().value();
            auto queryId = parser.ColumnParser("QueryId").GetOptionalUtf8().value();
            compileCacheKeys.push_back({nodeId, queryId});
        }

        // Verify we got some results
        if (compileCacheKeys.size() > 0) {
            // Verify results are in descending order by NodeId, then QueryId
            for (size_t i = 1; i < compileCacheKeys.size(); ++i) {
                const auto& prev = compileCacheKeys[i - 1];
                const auto& curr = compileCacheKeys[i];

                auto prevKey = std::tie(prev.NodeId, prev.QueryId);
                auto currKey = std::tie(curr.NodeId, curr.QueryId);

                UNIT_ASSERT_C(prevKey >= currKey,
                    TStringBuilder() << "Results not in descending order: "
                    << "compileCacheKeys[" << (i - 1) << "] = (" << prev.NodeId << ", \"" << prev.QueryId << "\")"
                    << " < compileCacheKeys[" << i << "] = (" << curr.NodeId << ", \"" << curr.QueryId << "\")"
                    << ". ORDER BY DESC is being ignored by sys view actors.");
            }
        }
    }

    Y_UNIT_TEST(TopQueriesOrderByDesc) {
        // Test ORDER BY DESC for top_queries sys views
        TKikimrRunner kikimr("", KikimrDefaultUtDomainRoot, 3);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();

        // Execute some queries to populate stats
        auto tableClient = kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        {
            auto result = tableSession.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/TwoShard`
            )"), TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto result = tableSession.ExecuteDataQuery(Q_(R"(
                SELECT * FROM `/Root/EightShard`
            )"), TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        // Wait a bit for stats to be collected
        ::Sleep(TDuration::MilliSeconds(500));

        // Test top_queries_by_read_bytes_one_minute
        auto result = session.ExecuteQuery(R"(--!syntax_v1
            SELECT IntervalEnd, Rank, ReadBytes
            FROM `/Root/.sys/top_queries_by_read_bytes_one_minute`
            ORDER BY IntervalEnd DESC, Rank DESC
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // Collect all results
        TVector<std::pair<TInstant, ui32>> intervalEndRank;
        auto resultSet = result.GetResultSet(0);
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto intervalEnd = parser.ColumnParser("IntervalEnd").GetOptionalTimestamp().value();
            auto rank = parser.ColumnParser("Rank").GetOptionalUint32().value();
            intervalEndRank.push_back({intervalEnd, rank});
        }

        // Verify we got some results (may be empty if no stats collected yet)
        if (intervalEndRank.size() > 0) {
            // Verify results are in descending order by IntervalEnd, then Rank
            for (size_t i = 1; i < intervalEndRank.size(); ++i) {
                const auto& prev = intervalEndRank[i - 1];
                const auto& curr = intervalEndRank[i];

                auto prevKey = std::tie(prev.first, prev.second);
                auto currKey = std::tie(curr.first, curr.second);

                UNIT_ASSERT_C(prevKey >= currKey,
                    TStringBuilder() << "Results not in descending order: "
                    << "intervalEndRank[" << (i - 1) << "] = (" << prev.first.ToString() << ", " << prev.second << ")"
                    << " < intervalEndRank[" << i << "] = (" << curr.first.ToString() << ", " << curr.second << ")"
                    << ". ORDER BY DESC is being ignored by sys view actors.");
            }
        }
    }

    Y_UNIT_TEST(QueryStatsSimple) {
        auto checkTable = [&] (const TStringBuf tableName) {
            TKikimrRunner kikimr("", KikimrDefaultUtDomainRoot, 3);

            auto client = kikimr.GetTableClient();
            auto session = client.CreateSession().GetValueSync().GetSession();
            {
                auto result = session.ExecuteDataQuery("SELECT 1;",
                    TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            }
            {
                auto result = session.ExecuteDataQuery(Q_(R"(
                    SELECT * FROM `/Root/TwoShard`
                )"), TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            }
            {
                auto result = session.ExecuteDataQuery(Q_(R"(
                    SELECT * FROM `/Root/EightShard`
                )"), TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            }

            TStringStream request;
            request << "SELECT ReadBytes FROM " << tableName << " ORDER BY ReadBytes";

            auto it = client.StreamExecuteScanQuery(request.Str()).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TSet<ui64> readBytesSet;
            for (;;) {
                auto streamPart = it.ReadNext().GetValueSync();
                if (!streamPart.IsSuccess()) {
                    UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                    break;
                }

                if (streamPart.HasResultSet()) {
                    auto resultSet = streamPart.ExtractResultSet();

                    NYdb::TResultSetParser parser(resultSet);
                    while (parser.TryNextRow()) {
                        auto value = parser.ColumnParser("ReadBytes").GetOptionalUint64();
                        UNIT_ASSERT(value);
                        readBytesSet.emplace(*value);
                    }
                }
            }

            UNIT_ASSERT(readBytesSet.contains(0)); // Pure
            UNIT_ASSERT(readBytesSet.contains(79)); // TwoShard
            UNIT_ASSERT(readBytesSet.contains(432)); // EightShard
        };

        checkTable("`/Root/.sys/top_queries_by_read_bytes_one_minute`");
        checkTable("`/Root/.sys/top_queries_by_read_bytes_one_hour`");
        checkTable("`/Root/.sys/top_queries_by_duration_one_minute`");
        checkTable("`/Root/.sys/top_queries_by_duration_one_hour`");
        checkTable("`/Root/.sys/top_queries_by_cpu_time_one_minute`");
        checkTable("`/Root/.sys/top_queries_by_cpu_time_one_hour`");
    }

    Y_UNIT_TEST(QueryStatsScan) {
        auto checkTable = [&] (const TStringBuf tableName) {
            auto kikimr = DefaultKikimrRunner();
            auto client = kikimr.GetTableClient();

            {
                auto it = kikimr.GetTableClient().StreamExecuteScanQuery(R"(
                    SELECT COUNT(*) FROM `/Root/EightShard`
                )").GetValueSync();

                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                CompareYson(R"([[24u]])", StreamResultToYson(it));
            }

            TStringStream request;
            request << "SELECT ReadBytes FROM " << tableName << " ORDER BY ReadBytes";

            auto it = client.StreamExecuteScanQuery(request.Str()).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TSet<ui64> readBytesSet;
            for (;;) {
                auto streamPart = it.ReadNext().GetValueSync();
                if (!streamPart.IsSuccess()) {
                    UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                    break;
                }

                if (streamPart.HasResultSet()) {
                    auto resultSet = streamPart.ExtractResultSet();

                    NYdb::TResultSetParser parser(resultSet);
                    while (parser.TryNextRow()) {
                        auto value = parser.ColumnParser("ReadBytes").GetOptionalUint64();
                        UNIT_ASSERT(value);
                        readBytesSet.emplace(*value);
                    }
                }
            }

            UNIT_ASSERT(readBytesSet.contains(192)); // EightShard
        };

        checkTable("`/Root/.sys/top_queries_by_read_bytes_one_minute`");
    }

    Y_UNIT_TEST(FailNavigate) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient(NYdb::NTable::TClientSettings().AuthToken("user0@builtin"));

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT PathId FROM `/Root/.sys/partition_stats`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto streamPart = it.ReadNext().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(FailResolve) {
        TKikimrRunner kikimr;
        {
            TPermissions permissions("user0@builtin",
                {"ydb.deprecated.describe_schema"}
            );
            auto schemeClient = kikimr.GetSchemeClient();
            auto result = schemeClient.ModifyPermissions("/Root",
                TModifyPermissionsSettings().AddGrantPermissions(permissions)
            ).ExtractValueSync();
            AssertSuccessResult(result);
        }

        auto driverConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken("user0@builtin");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);
        auto it = client.StreamExecuteScanQuery(R"(
            SELECT PathId FROM `/Root/.sys/partition_stats`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto streamPart = it.ReadNext().GetValueSync();
        // TODO: Should be UNAUTHORIZED
        UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::ABORTED);
        driver.Stop(true);
    }

    Y_UNIT_TEST(ReadSuccess) {
        TKikimrRunner kikimr;
        {
            TPermissions permissions("user0@builtin",
                {"ydb.deprecated.describe_schema", "ydb.deprecated.select_row"}
            );
            auto schemeClient = kikimr.GetSchemeClient();
            auto result = schemeClient.ModifyPermissions("/Root",
                TModifyPermissionsSettings().AddGrantPermissions(permissions)
            ).ExtractValueSync();
            AssertSuccessResult(result);
        }

        auto driverConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken("user0@builtin");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);
        auto it = client.StreamExecuteScanQuery(R"(
            SELECT PathId FROM `/Root/.sys/partition_stats`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto streamPart = it.ReadNext().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SUCCESS);
        driver.Stop(true);
    }

    Y_UNIT_TEST(PartitionStatsFollower) {
        auto settings = TKikimrSettings()
            .SetEnableForceFollowers(true)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NLog::PRI_TRACE);

        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();

        AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            CREATE TABLE Followers (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                READ_REPLICAS_SETTINGS = "ANY_AZ:3"
            );
        )").GetValueSync());

        const auto describeResult = kikimr.GetTestClient().Describe(&runtime, "/Root/Followers");
        const auto tablePathId = describeResult.GetPathId();

        Cerr << "... UPSERT" << Endl;
        AssertSuccessResult(session.ExecuteDataQuery(R"(
            --!syntax_v1
            UPSERT INTO Followers (Key, Value) VALUES
                (1u, "One"),
                (11u, "Two"),
                (21u, "Three"),
                (31u, "Four");
        )", TTxControl::BeginTx().CommitTx()).GetValueSync());

        Cerr << "... SELECT from leader" << Endl;
        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1
                SELECT * FROM Followers WHERE Key = 11;
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            AssertSuccessResult(result);

            TString actual = FormatResultSetYson(result.GetResultSet(0));
            CompareYson(R"([
                [[11u];["Two"]]
            ])", actual);
        }

        // from master - should read
        CheckTableReads(session, "/Root/Followers", false, true);
        // from followers - should NOT read yet
        CheckTableReads(session, "/Root/Followers", true, false);

        Cerr << "... SELECT from follower" << Endl;
        {
            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1
                SELECT * FROM Followers WHERE Key >= 21;
            )", TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx()).ExtractValueSync();
            AssertSuccessResult(result);

            TString actual = FormatResultSetYson(result.GetResultSet(0));
            CompareYson(R"([
                [[21u];["Three"]];
                [[31u];["Four"]]
            ])", actual);
        }

        // from master - should read
        CheckTableReads(session, "/Root/Followers", false, true);
        // from followers - should read
        CheckTableReads(session, "/Root/Followers", true, true);

        for (size_t attempt = 0; attempt < 30; ++attempt)
        {
            Cerr << "... SELECT from partition_stats, attempt " << attempt << Endl;
            auto result = session.ExecuteDataQuery(R"(
                SELECT OwnerId, PartIdx, Path, PathId, TabletId,
                    RowCount, RowUpdates, RowReads, RangeReadRows,
                    IF(FollowerId = 0, 'L', 'F') AS LeaderFollower
                FROM `/Root/.sys/partition_stats`
                WHERE RowCount != 0
                ORDER BY PathId, PartIdx, LeaderFollower;
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            AssertSuccessResult(result);

            auto rs = result.GetResultSet(0);
            if (rs.RowsCount() != 2) {
                Sleep(TDuration::Seconds(5));
                continue;
            }

            // Leader and follower have different row stats
            TString actual = FormatResultSetYson(rs);
            CompareYson(Sprintf(R"([
                [[72057594046644480u];[0u];["/Root/Followers"];[%luu];[72075186224037888u];[4u];[0u];[0u];[2u];"F"];
                [[72057594046644480u];[0u];["/Root/Followers"];[%luu];[72075186224037888u];[4u];[4u];[1u];[0u];"L"]
            ])", tablePathId, tablePathId), actual);
            return;
        }

        Y_FAIL("Timeout waiting for from partition_stats");
    }

    Y_UNIT_TEST_TWIN(CompileCacheBasic, EnableCompileCacheView) {
        auto serverSettings = TKikimrSettings().SetKqpSettings({ NKikimrKqp::TKqpSetting() });
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableCompileCacheView(EnableCompileCacheView);
        auto tableClient = kikimr.GetTableClient();
        ui64 initial = SelectCompileCacheCount(tableClient);
        UNIT_ASSERT_EQUAL_C(initial, 0, "Compile cache is not empty at the beginning");
        {
            auto query = R"(DECLARE $k AS Uint64;
            SELECT COUNT(*) FROM `/Root/EightShard` WHERE Key = $k;
            )";

            for (ui32 i = 0; i < 3; ++i) {
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto paramsBuilder = TParamsBuilder();
                paramsBuilder.AddParam("$k").Uint64(i).Build();
                auto preparedResult = session.PrepareDataQuery(query).GetValueSync();
                auto executedResult = session.ExecuteDataQuery(
                    query,
                    TTxControl::BeginTx().CommitTx(),
                    paramsBuilder.Build()
                ).GetValueSync();
                UNIT_ASSERT_C(executedResult.IsSuccess(), executedResult.GetIssues().ToString());
            }
            ui64 afterQuery = SelectCompileCacheCount(tableClient);
            UNIT_ASSERT_EQUAL_C(afterQuery, 2, "Got " << afterQuery << " instead"); // first for sys_view call, second for sum
        }
        TString query = R"(
                SELECT * FROM `/Root/.sys/compile_cache_queries`;
            )";
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), TExecDataQuerySettings().KeepInQueryCache(true)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 3);

        NYdb::TResultSetParser parser(resultSet);
        while(parser.TryNextRow()) {
            auto maybeDuration = parser.ColumnParser("CompilationDurationMs").GetOptionalUint64();
            UNIT_ASSERT(maybeDuration);
            auto duration = maybeDuration.value();
            UNIT_ASSERT_C(duration > 0, "duration is " << duration);
        }
    }

    Y_UNIT_TEST_TWIN(CompileCacheCheckWarnings, EnableCompileCacheView) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableCompileCacheView(EnableCompileCacheView);
        auto client = kikimr.GetQueryClient();
        auto db = kikimr.GetTableClient();
        {
            auto session = db.CreateSession().GetValueSync().GetSession();

            auto createResult = session.ExecuteSchemeQuery(R"(
                CREATE TABLE TestTable (
                    Key Int32?,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(createResult.GetStatus(), EStatus::SUCCESS, createResult.GetIssues());

            auto insertResult = session.ExecuteDataQuery(R"(
                INSERT INTO TestTable (Key, Value) VALUES
                (42, "val"), (NULL, "val1");
                )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());
        }


        auto session = db.CreateSession().GetValueSync().GetSession();
        TString query = R"(
            SELECT * FROM TestTable WHERE Key in [42, NULL];
        )";

        auto compileResult = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), TExecDataQuerySettings().KeepInQueryCache(true)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(compileResult.GetStatus(), EStatus::SUCCESS);

        TString sysViewQuery = R"(SELECT * FROM `/Root/.sys/compile_cache_queries`;)";
        auto result = session.ExecuteDataQuery(sysViewQuery,TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        auto resultSet = result.GetResultSet(0);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 11);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);

        NYdb::TResultSetParser parser(resultSet);
        UNIT_ASSERT(parser.TryNextRow());
        auto value = parser.ColumnParser("Warnings").GetOptionalUtf8();
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL_C(value, compileResult.GetIssues().ToOneLineString(), "one the one side we have: " << value << " on the other " << compileResult.GetIssues().ToOneLineString());

    }

    Y_UNIT_TEST(CompileCacheTenantMismatchReturnsEmpty) {
        auto serverSettings = TKikimrSettings().SetKqpSettings({ NKikimrKqp::TKqpSetting() });
        TKikimrRunner kikimr(serverSettings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        kikimr.GetTestServer().GetRuntime()->GetAppData().FeatureFlags.SetEnableCompileCacheView(true);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto prepareResult = session.PrepareDataQuery(
            R"(DECLARE $k AS Uint64; SELECT COUNT(*) FROM `/Root/EightShard` WHERE Key = $k;)"
        ).GetValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());

        ui64 count = SelectCompileCacheCount(tableClient);
        UNIT_ASSERT_C(count > 0, "Compile cache must not be empty after preparing a query");

        auto edgeActor = runtime.AllocateEdgeActor(0);
        auto compileServiceId = NKqp::MakeKqpCompileServiceID(runtime.GetNodeId(0));

        auto req = std::make_unique<NKikimr::NKqp::TEvKqp::TEvListQueryCacheQueriesRequest>();
        req->Record.SetTenantName("/Root/some-db");
        req->Record.SetFreeSpace(1024 * 1024);

        runtime.Send(new IEventHandle(compileServiceId, edgeActor, req.release()), 0);

        auto response = runtime.GrabEdgeEvent<NKikimr::NKqp::TEvKqp::TEvListQueryCacheQueriesResponse>(
            edgeActor, TDuration::Seconds(5));

        UNIT_ASSERT_C(response, "Compile service did not respond in time");
        UNIT_ASSERT_C(response->Get()->Record.GetFinished(),
            "Response for a serverless database must be marked Finished immediately");
        UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetCacheCacheQueries().size(), 0,
            "Tenant mismatch must produce an empty compile cache response, got "
            << response->Get()->Record.GetCacheCacheQueries().size() << " entries");
    }

    Y_UNIT_TEST(CompileCacheUserIsolation) {
        TKikimrSettings settings;
        settings.SetAuthToken("root@builtin");
        TKikimrRunner kikimr(settings);

        // Grant user1 and user2 permissions to access tables and sys views
        {
            auto schemeClient = kikimr.GetSchemeClient();
            for (const auto& user : {"user1@builtin", "user2@builtin"}) {
                TPermissions permissions(user,
                    {"ydb.deprecated.describe_schema", "ydb.deprecated.select_row"}
                );
                auto result = schemeClient.ModifyPermissions("/Root",
                    TModifyPermissionsSettings().AddGrantPermissions(permissions)
                ).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
        }

        // User1 compiles a query -> cache entry with UserSid="user1@builtin"
        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user1@builtin");
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);
            auto session = client.CreateSession().GetValueSync().GetSession();
            auto result = session.PrepareDataQuery(
                R"(DECLARE $k AS Uint64; SELECT COUNT(*) FROM `/Root/EightShard` WHERE Key = $k;)"
            ).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            driver.Stop(true);
        }

        // User2 compiles the same query -> separate cache entry with UserSid="user2@builtin"
        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user2@builtin");
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);
            auto session = client.CreateSession().GetValueSync().GetSession();
            auto result = session.PrepareDataQuery(
                R"(DECLARE $k AS Uint64; SELECT COUNT(*) FROM `/Root/EightShard` WHERE Key = $k;)"
            ).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            driver.Stop(true);
        }

        // User1 queries compile_cache -> should see only their own entries
        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user1@builtin");
            auto driver = TDriver(driverConfig);
            TTableClient client(driver);
            auto session = client.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(
                R"(SELECT UserSID FROM `/Root/.sys/compile_cache_queries`;)",
                TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto resultSet = result.GetResultSet(0);
            NYdb::TResultSetParser parser(resultSet);
            while (parser.TryNextRow()) {
                auto userSid = parser.ColumnParser("UserSID").GetOptionalUtf8();
                UNIT_ASSERT_C(userSid && *userSid == "user1@builtin",
                    "user1 must see only their own queries, but saw UserSID=" << (userSid ? *userSid : "null"));
            }
            driver.Stop(true);
        }

        // Admin queries compile_cache -> should see entries from both users
        {
            auto tableClient = kikimr.GetTableClient();
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(
                R"(SELECT UserSID FROM `/Root/.sys/compile_cache_queries`;)",
                TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto resultSet = result.GetResultSet(0);
            std::unordered_set<std::string> seenUsers;
            NYdb::TResultSetParser parser(resultSet);
            while (parser.TryNextRow()) {
                auto userSid = parser.ColumnParser("UserSID").GetOptionalUtf8();
                if (userSid) {
                    seenUsers.insert(*userSid);
                }
            }
            UNIT_ASSERT_C(seenUsers.contains("user1@builtin"),
                "Admin must see user1's queries");
            UNIT_ASSERT_C(seenUsers.contains("user2@builtin"),
                "Admin must see user2's queries");
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
