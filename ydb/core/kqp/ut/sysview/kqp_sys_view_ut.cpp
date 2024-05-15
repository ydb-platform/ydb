// we define this to allow using sdk build info.
#define INCLUDE_YDB_INTERNAL_H
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <util/system/getpid.h>
#include <ydb/core/sys_view/service/query_history.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections/grpc_connections.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

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
        TKikimrRunner kikimr("root@builtin");
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

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT OwnerId, PartIdx, Path, PathId
            FROM `/Root/.sys/partition_stats`
            ORDER BY PathId, PartIdx;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [[72057594046644480u];[0u];["/Root/TwoShard"];[2u]];
            [[72057594046644480u];[1u];["/Root/TwoShard"];[2u]];
            [[72057594046644480u];[0u];["/Root/EightShard"];[3u]];
            [[72057594046644480u];[1u];["/Root/EightShard"];[3u]];
            [[72057594046644480u];[2u];["/Root/EightShard"];[3u]];
            [[72057594046644480u];[3u];["/Root/EightShard"];[3u]];
            [[72057594046644480u];[4u];["/Root/EightShard"];[3u]];
            [[72057594046644480u];[5u];["/Root/EightShard"];[3u]];
            [[72057594046644480u];[6u];["/Root/EightShard"];[3u]];
            [[72057594046644480u];[7u];["/Root/EightShard"];[3u]];
            [[72057594046644480u];[0u];["/Root/Logs"];[4u]];
            [[72057594046644480u];[1u];["/Root/Logs"];[4u]];
            [[72057594046644480u];[2u];["/Root/Logs"];[4u]];
            [[72057594046644480u];[0u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[1u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[2u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[3u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[4u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[5u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[6u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[7u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[8u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[9u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[0u];["/Root/KeyValue"];[6u]];
            [[72057594046644480u];[0u];["/Root/KeyValue2"];[7u]];
            [[72057594046644480u];[0u];["/Root/KeyValueLargePartition"];[8u]];
            [[72057594046644480u];[0u];["/Root/Test"];[9u]];
            [[72057594046644480u];[0u];["/Root/Join1"];[10u]];
            [[72057594046644480u];[1u];["/Root/Join1"];[10u]];
            [[72057594046644480u];[0u];["/Root/Join2"];[11u]];
            [[72057594046644480u];[1u];["/Root/Join2"];[11u]]
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(PartitionStatsRanges) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT OwnerId, PartIdx, Path, PathId
            FROM `/Root/.sys/partition_stats`
            WHERE
            OwnerId = 72057594046644480ul
            AND PathId = 5u
            AND (PartIdx BETWEEN 0 AND 2 OR PartIdx BETWEEN 6 AND 9)
            ORDER BY PathId, PartIdx;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [[72057594046644480u];[0u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[1u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[2u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[6u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[7u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[8u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[9u];["/Root/BatchUpload"];[5u]];
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(PartitionStatsParametricRanges) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();

        auto paramsBuilder = client.GetParamsBuilder();
        auto params = paramsBuilder
            .AddParam("$l1").Int32(0).Build()
            .AddParam("$r1").Int32(2).Build()
            .AddParam("$l2").Int32(6).Build()
            .AddParam("$r2").Int32(9).Build().Build();

        auto it = client.StreamExecuteScanQuery(R"(
            DECLARE $l1 AS Int32;
            DECLARE $r1 AS Int32;

            DECLARE $l2 AS Int32;
            DECLARE $r2 AS Int32;

            SELECT OwnerId, PartIdx, Path, PathId
            FROM `/Root/.sys/partition_stats`
            WHERE
            OwnerId = 72057594046644480ul
            AND PathId = 5u
            AND (PartIdx BETWEEN $l1 AND $r1 OR PartIdx BETWEEN $l2 AND $r2)
            ORDER BY PathId, PartIdx;
        )", params).GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [[72057594046644480u];[0u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[1u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[2u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[6u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[7u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[8u];["/Root/BatchUpload"];[5u]];
            [[72057594046644480u];[9u];["/Root/BatchUpload"];[5u]];
        ])", StreamResultToYson(it));
    }

    Y_UNIT_TEST(PartitionStatsRange1) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();

        TString query = R"(
            SELECT OwnerId, PathId, PartIdx, Path
            FROM `/Root/.sys/partition_stats`
            WHERE OwnerId = 72057594046644480ul AND PathId > 5u AND PathId <= 10u
            ORDER BY PathId, PartIdx;
        )";

        TString expectedYson = R"([
            [[72057594046644480u];[6u];[0u];["/Root/KeyValue"]];
            [[72057594046644480u];[7u];[0u];["/Root/KeyValue2"]];
            [[72057594046644480u];[8u];[0u];["/Root/KeyValueLargePartition"]];
            [[72057594046644480u];[9u];[0u];["/Root/Test"]];
            [[72057594046644480u];[10u];[0u];["/Root/Join1"]];
            [[72057594046644480u];[10u];[1u];["/Root/Join1"]]
        ])";

        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(expectedYson, StreamResultToYson(it));
    }

    Y_UNIT_TEST(PartitionStatsRange2) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();
        TString query = R"(
            SELECT OwnerId, PathId, PartIdx, Path
            FROM `/Root/.sys/partition_stats`
            WHERE OwnerId = 72057594046644480ul AND PathId >= 6u AND PathId < 9u
            ORDER BY PathId, PartIdx;
        )";

        TString expectedYson = R"([
            [[72057594046644480u];[6u];[0u];["/Root/KeyValue"]];
            [[72057594046644480u];[7u];[0u];["/Root/KeyValue2"]];
            [[72057594046644480u];[8u];[0u];["/Root/KeyValueLargePartition"]]
        ])";

        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(expectedYson, StreamResultToYson(it));
    }

    Y_UNIT_TEST(PartitionStatsRange3) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetTableClient();

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT OwnerId, PathId, PartIdx, Path
            FROM `/Root/.sys/partition_stats`
            WHERE OwnerId = 72057594046644480ul AND PathId = 5u AND PartIdx > 1u AND PartIdx < 7u
            ORDER BY PathId, PartIdx;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        CompareYson(R"([
            [[72057594046644480u];[5u];[2u];["/Root/BatchUpload"]];
            [[72057594046644480u];[5u];[3u];["/Root/BatchUpload"]];
            [[72057594046644480u];[5u];[4u];["/Root/BatchUpload"]];
            [[72057594046644480u];[5u];[5u];["/Root/BatchUpload"]];
            [[72057594046644480u];[5u];[6u];["/Root/BatchUpload"]];
        ])", StreamResultToYson(it));
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
        TKikimrRunner kikimr("user0@builtin");
        auto client = kikimr.GetTableClient();

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
}

} // namspace NKqp
} // namespace NKikimr
