#include "ut_common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/library/testlib/common/test_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <library/cpp/yson/node/node_io.h>

namespace NKikimr {
namespace NSysView {

using namespace NYdb;
using namespace NYdb::NDump;
using namespace NYdb::NScheme;
using namespace NYdb::NTable;
using namespace NTestUtils;

namespace {

void FillRootTable(TTestEnv& env, ui16 tableNum = 0) {
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();
    NKqp::AssertSuccessResult(session.ExecuteDataQuery(Sprintf(R"(
        REPLACE INTO `/Root/Table%u` (Key, Value) VALUES
            (0u, "X"),
            (1u, "Y"),
            (2u, "Z");
    )", tableNum), TTxControl::BeginTx().CommitTx()).GetValueSync());
}

void CreateRootTable(TTestEnv& env, ui64 partitionCount = 1, bool fillTable = false, ui16 tableNum = 0) {
    env.GetClient().CreateTable("/Root", Sprintf(R"(
        Name: "Table%u"
        Columns { Name: "Key", Type: "Uint64" }
        Columns { Name: "Value", Type: "String" }
        KeyColumnNames: ["Key"]
        UniformPartitionsCount: %lu
    )", tableNum, partitionCount));

    if (fillTable)
        FillRootTable(env, tableNum);
}

void CreateRootColumnTable(TTestEnv& env, ui64 partitionCount = 1, bool fillTable = false, ui16 tableNum = 0) {
    NQuery::TQueryClient client(env.GetDriver());
    auto result = client.ExecuteQuery(Sprintf(R"(
        CREATE TABLE `/Root/Table%u` (
            Key Int32 NOT NULL,
            Value Utf8,
            PRIMARY KEY(Key)
        ) WITH (STORE=COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %lu);
    )", tableNum, partitionCount), NQuery::TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    if (fillTable)
        FillRootTable(env, tableNum);
}

void BreakLock(TSession& session, const TString& tableName) {
    std::optional<TTransaction> tx1;

    {  // tx0: write test data
        auto result = session.ExecuteDataQuery(TStringBuilder() <<
            "UPSERT INTO `" << tableName << "` (Key, Value) VALUES (55u, \"Fifty five\")",
        TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {  // tx0: read all data
        auto result = session.ExecuteDataQuery(TStringBuilder() <<
            "SELECT * FROM `" << tableName << "`",
        TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    while (!tx1) {
    // tx1: start reading
        auto result = session.ExecuteDataQuery(TStringBuilder() <<
            "SELECT * FROM `" << tableName << "` WHERE Key = 55u",
        TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        TString yson = FormatResultSetYson(result.GetResultSet(0));
        if (yson == "[]") {
            continue;
        }

        NKqp::CompareYson(R"([
            [[55u];["Fifty five"]];
        ])", yson);
        tx1 = result.GetTransaction();
        UNIT_ASSERT(tx1);
    }

    {  // tx2: write + commit
        auto result = session.ExecuteDataQuery(TStringBuilder() <<
            "UPSERT INTO `" << tableName << "` (Key, Value) VALUES (55u, \"NewValue1\")",
        TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {  // tx1: try to commit
        auto result = tx1->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

size_t GetRowCount(TTableClient& client, const TString& tableName, const TString& condition = {}) {
    TStringBuilder query;
    query << "SELECT * FROM `" << tableName << "`";
    if (!condition.empty())
        query << " WHERE " << condition;
    auto it = client.StreamExecuteScanQuery(query).GetValueSync();
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    auto ysonString = NKqp::StreamResultToYson(it);
    auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
    UNIT_ASSERT(node.IsList());
    return node.AsList().size();
}

ui64 GetIntervalEnd(TTableClient& client, const TString& name) {
    TStringBuilder query;
    query << "SELECT MAX(IntervalEnd) FROM `" << name << "`";
    auto it = client.StreamExecuteScanQuery(query).GetValueSync();
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    auto ysonString = NKqp::StreamResultToYson(it);
    auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
    UNIT_ASSERT(node.IsList());
    UNIT_ASSERT(node.AsList().size() == 1);
    auto row = node.AsList()[0];
    UNIT_ASSERT(row.IsList());
    UNIT_ASSERT(row.AsList().size() == 1);
    auto value = row.AsList()[0];
    UNIT_ASSERT(value.IsList());
    UNIT_ASSERT(value.AsList().size() == 1);
    return value.AsList()[0].AsUint64();
}

void WaitForStats(TTableClient& client, const TString& tableName, const TString& condition = {}) {
    size_t rowCount = 0;
    for (size_t iter = 0; iter < 30; ++iter) {
        if (rowCount = GetRowCount(client, tableName, condition))
            break;
        Sleep(TDuration::Seconds(5));
    }
    UNIT_ASSERT_GE(rowCount, 0);
}
class TYsonFieldChecker {
    NYT::TNode Root;
    NYT::TNode::TListType::const_iterator RowIterator;

private:
    const NYT::TNode& ExtractOptional(const NYT::TNode& opt) {
        UNIT_ASSERT(opt.IsList());
        UNIT_ASSERT_VALUES_EQUAL(opt.AsList().size(), 1);
        return opt.AsList().front();
    };

public:
    TYsonFieldChecker(const TString& ysonString, size_t fieldCount) {
        Root = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
        UNIT_ASSERT(Root.IsList());
        UNIT_ASSERT_VALUES_EQUAL(Root.AsList().size(), 1);

        const auto& rowNode = Root.AsList().front();
        UNIT_ASSERT(rowNode.IsList());

        const auto& row = rowNode.AsList();
        UNIT_ASSERT_VALUES_EQUAL(row.size(), fieldCount);

        RowIterator = row.begin();
    }

    bool SkipNull() {
        if (RowIterator->IsNull()) {
            ++RowIterator;
            return true;
        } else {
            return false;
        }
    }

    void Null() {
        const auto& value = *RowIterator++;
        UNIT_ASSERT(value.IsNull());
    }

    void Bool(bool expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsBool());
        UNIT_ASSERT_VALUES_EQUAL(value.AsBool(), expected);
    }

    void Uint64(ui64 expected, bool orNull = false) {
        if (!orNull || !SkipNull()) {
            const auto& value = ExtractOptional(*RowIterator++);
            UNIT_ASSERT(value.IsUint64());
            UNIT_ASSERT_VALUES_EQUAL(value.AsUint64(), expected);
        }
    }

    void Uint64Greater(ui64 expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsUint64());
        UNIT_ASSERT_GT(value.AsUint64(), expected);
    }

    void Uint64GreaterOrEquals(ui64 expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsUint64());
        UNIT_ASSERT_GE(value.AsUint64(), expected);
    }

    void Uint64LessOrEquals(ui64 expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsUint64());
        UNIT_ASSERT_LE(value.AsUint64(), expected);
    }

    void Int64(i64 expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsInt64());
        UNIT_ASSERT_VALUES_EQUAL(value.AsInt64(), expected);
    }

    void Int64Greater(i64 expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsInt64());
        UNIT_ASSERT_GT(value.AsInt64(), expected);
    }

    void Int64GreaterOrEquals(i64 expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsInt64());
        UNIT_ASSERT_GE(value.AsInt64(), expected);
    }

    void Double(double expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsDouble());
        UNIT_ASSERT_VALUES_EQUAL(value.AsDouble(), expected);
    }

    void DoubleGreaterOrEquals(double expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsDouble());
        UNIT_ASSERT_GE(value.AsDouble(), expected);
    }

    void String(const TString& expected) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsString());
        UNIT_ASSERT_STRINGS_EQUAL(value.AsString(), expected);
    }

    void StringContains(const TString& substr) {
        const auto& value = ExtractOptional(*RowIterator++);
        UNIT_ASSERT(value.IsString());
        UNIT_ASSERT(value.AsString().Contains(substr));
    }
};

} // namespace

Y_UNIT_TEST_SUITE(SystemView) {

    Y_UNIT_TEST(PartitionStatsOneSchemeShard) {
        TTestEnv env;
        CreateTenantsAndTables(env, true);
        auto describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root/Table0");
        const auto table0PathId = describeResult.GetPathId();

        describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root/Tenant1/Table1");
        const auto table1PathId = describeResult.GetPathId();

        describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root/Tenant2/Table2");
        const auto table2PathId = describeResult.GetPathId();

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off);
        auto driver = TDriver(driverConfig);

        {
            TTableClient client(driver, TClientSettings().Database("/Root"));
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PathId, PartIdx, Path FROM `/Root/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            NKqp::CompareYson(Sprintf(R"([
                [[%luu];[0u];["/Root/Table0"]]
            ])", table0PathId), NKqp::StreamResultToYson(it));
        }
        {
            TTableClient client(driver, TClientSettings().Database("/Root/Tenant1"));
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PathId, PartIdx, Path FROM `/Root/Tenant1/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            NKqp::CompareYson(Sprintf(R"([
                [[%luu];[0u];["/Root/Tenant1/Table1"]]
            ])", table1PathId), NKqp::StreamResultToYson(it));
        }
        {
            TTableClient client(driver, TClientSettings().Database("/Root/Tenant2"));
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PathId, PartIdx, Path FROM `/Root/Tenant2/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            NKqp::CompareYson(Sprintf(R"([
                [[%luu];[0u];["/Root/Tenant2/Table2"]]
            ])", table2PathId), NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(PartitionStatsOneSchemeShardDataQuery) {
        TTestEnv env;
        CreateTenantsAndTables(env, true);
        auto describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root/Table0");
        const auto table0PathId = describeResult.GetPathId();

        describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root/Tenant1/Table1");
        const auto table1PathId = describeResult.GetPathId();

        describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root/Tenant2/Table2");
        const auto table2PathId = describeResult.GetPathId();

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off);
        auto driver = TDriver(driverConfig);

        {
            TTableClient client(driver, TClientSettings().Database("/Root"));
            auto session = client.CreateSession().GetValueSync().GetSession();

            auto result = session.ExecuteDataQuery(R"(
                SELECT PathId, PartIdx, Path FROM `/Root/.sys/partition_stats`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            NKqp::CompareYson(Sprintf(R"([
                [[%luu];[0u];["/Root/Table0"]]
            ])", table0PathId), FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            TTableClient client(driver, TClientSettings().Database("/Root/Tenant1"));
            auto session = client.CreateSession().GetValueSync().GetSession();

            auto result = session.ExecuteDataQuery(R"(
                SELECT PathId, PartIdx, Path FROM `/Root/Tenant1/.sys/partition_stats`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT(result.IsSuccess());
            NKqp::CompareYson(Sprintf(R"([
                [[%luu];[0u];["/Root/Tenant1/Table1"]]
            ])", table1PathId), FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            TTableClient client(driver, TClientSettings().Database("/Root/Tenant2"));
            auto session = client.CreateSession().GetValueSync().GetSession();

            auto result = session.ExecuteDataQuery(R"(
                SELECT PathId, PartIdx, Path FROM `/Root/Tenant2/.sys/partition_stats`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT(result.IsSuccess());
            NKqp::CompareYson(Sprintf(R"([
                [[%luu];[0u];["/Root/Tenant2/Table2"]]
            ])", table2PathId), FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(PgTablesOneSchemeShardDataQuery) {
        TTestEnv env;
        CreateRootTable(env, 1, false, 0);
        CreateRootTable(env, 2, false, 1);

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT schemaname, tablename, tableowner, tablespace, hasindexes, hasrules, hastriggers, rowsecurity FROM `/Root/.sys/pg_tables` WHERE tablename = PgName("Table0") OR tablename = PgName("Table1") ORDER BY tablename;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            NKqp::CompareYson(R"([
                ["public";"Table0";"root@builtin";#;"t";"f";"f";"f"];
                ["public";"Table1";"root@builtin";#;"t";"f";"f";"f"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(Nodes) {
        TTestEnv env;
        CreateTenantsAndTables(env, false);

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off);
        auto driver = TDriver(driverConfig);

        {
            TTableClient client(driver, TClientSettings().Database("/Root/Tenant1"));
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Host, NodeId
                FROM `/Root/Tenant1/.sys/nodes`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            ui32 offset = env.GetServer().GetRuntime()->GetNodeId(0);
            auto expected = Sprintf(R"([
                [["::1"];[%du]];
                [["::1"];[%du]];
            ])", offset + 3, offset + 4);

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
        {
            TTableClient client(driver, TClientSettings().Database("/Root/Tenant2"));
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Host, NodeId
                FROM `/Root/Tenant2/.sys/nodes`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            ui32 offset = env.GetServer().GetRuntime()->GetNodeId(0);
            auto expected = Sprintf(R"([
                [["::1"];[%du]];
                [["::1"];[%du]];
            ])", offset + 1, offset + 2);

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
        {
            TTableClient client(driver, TClientSettings().Database("/Root"));
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Host, NodeId
                FROM `/Root/.sys/nodes`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            ui32 offset = env.GetServer().GetRuntime()->GetNodeId(0);
            auto expected = Sprintf(R"([
                [["::1"];[%du]];
            ])", offset);

            NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(QueryStats) {
        TTestEnv env;
        CreateTenants(env);

        auto* runtime = env.GetServer().GetRuntime();
        runtime ->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_DEBUG);

        auto oneMinute = TDuration::Minutes(1);
        auto oneMinuteUs = oneMinute.MicroSeconds();

        auto instant = TAppData::TimeProvider->Now() + oneMinute;
        auto instantRounded = instant.MicroSeconds() / oneMinuteUs * oneMinuteUs;

        std::vector<ui64> buckets;
        for (size_t i : xrange(4)) {
            Y_UNUSED(i);
            buckets.push_back(instantRounded);
            instantRounded += oneMinuteUs;
        }

        auto& tenant1Nodes = env.GetTenants().List("/Root/Tenant1");
        auto& tenant2Nodes = env.GetTenants().List("/Root/Tenant2");

        UNIT_ASSERT_EQUAL(tenant1Nodes.size(), 2);
        UNIT_ASSERT_EQUAL(tenant2Nodes.size(), 2);

        auto tenant1Node0 = tenant1Nodes[0];
        auto tenant1Node1 = tenant1Nodes[1];
        auto tenant2Node = tenant2Nodes.front();
        ui32 staticNode = 0;

        auto makeQueryEvent = [&runtime] (ui32 nodeIdx, ui64 endTimeUs, const TString& queryText, ui64 readBytes) {
            auto stats = MakeHolder<NSysView::TEvSysView::TEvCollectQueryStats>();
            stats->QueryStats.MutableStats()->SetReadBytes(readBytes);
            stats->QueryStats.SetQueryText(queryText);
            stats->QueryStats.SetQueryTextHash(MurmurHash<ui64>(queryText.data(), queryText.size()));
            stats->QueryStats.SetDurationMs(1);
            stats->QueryStats.SetEndTimeMs(endTimeUs / 1000);

            auto serviceId = MakeSysViewServiceID(runtime->GetNodeId(nodeIdx));
            runtime->Send(new IEventHandle(serviceId, TActorId(), stats.Release()), nodeIdx);
        };

        makeQueryEvent(tenant1Node0, buckets[0], "a", 100);
        makeQueryEvent(tenant1Node1, buckets[1], "b", 200);
        makeQueryEvent(tenant1Node0, buckets[1], "c", 300);
        makeQueryEvent(tenant1Node0, buckets[1], "d", 400);
        makeQueryEvent(tenant1Node1, buckets[2], "e", 500);

        makeQueryEvent(tenant2Node, buckets[0], "f", 600);

        makeQueryEvent(staticNode, buckets[0], "g", 700);

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off);
        auto driver = TDriver(driverConfig);

        {
            TTableClient client(driver, TClientSettings().Database("/Root"));
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT IntervalEnd, QueryText, Rank, ReadBytes
                FROM `/Root/.sys/top_queries_by_read_bytes_one_minute`;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[1] << "u];[\"g\"];[1u];[700u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TTableClient client(driver, TClientSettings().Database("/Root/Tenant1"));
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT IntervalEnd, QueryText, Rank, ReadBytes
                FROM `/Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute`;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[1] << "u];[\"a\"];[1u];[100u]];";
            result << "[[" << buckets[2] << "u];[\"d\"];[1u];[400u]];";
            result << "[[" << buckets[2] << "u];[\"c\"];[2u];[300u]];";
            result << "[[" << buckets[2] << "u];[\"b\"];[3u];[200u]];";
            result << "[[" << buckets[3] << "u];[\"e\"];[1u];[500u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TTableClient client(driver, TClientSettings().Database("/Root/Tenant2"));
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT IntervalEnd, QueryText, Rank, ReadBytes
                FROM `/Root/Tenant2/.sys/top_queries_by_read_bytes_one_minute`;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[1] << "u];[\"f\"];[1u];[600u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, QueryText, Rank, ReadBytes ";
            query << "FROM `/Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute` ";
            query << "WHERE IntervalEnd >= CAST(" << buckets[1] << "ul as Timestamp) ";
            query << "AND IntervalEnd < CAST(" << buckets[3] << "ul as Timestamp);";

            TTableClient client(driver, TClientSettings().Database("/Root/Tenant1"));
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[1] << "u];[\"a\"];[1u];[100u]];";
            result << "[[" << buckets[2] << "u];[\"d\"];[1u];[400u]];";
            result << "[[" << buckets[2] << "u];[\"c\"];[2u];[300u]];";
            result << "[[" << buckets[2] << "u];[\"b\"];[3u];[200u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, QueryText, Rank, ReadBytes ";
            query << "FROM `/Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute` ";
            query << "WHERE IntervalEnd > CAST(" << buckets[1] << "ul as Timestamp) ";
            query << "AND IntervalEnd <= CAST(" << buckets[3] << "ul as Timestamp);";

            TTableClient client(driver, TClientSettings().Database("/Root/Tenant1"));
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[2] << "u];[\"d\"];[1u];[400u]];";
            result << "[[" << buckets[2] << "u];[\"c\"];[2u];[300u]];";
            result << "[[" << buckets[2] << "u];[\"b\"];[3u];[200u]];";
            result << "[[" << buckets[3] << "u];[\"e\"];[1u];[500u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, QueryText, Rank, ReadBytes ";
            query << "FROM `/Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << buckets[2] << "ul as Timestamp) ";
            query << "AND Rank >= 1u AND Rank < 3u";

            TTableClient client(driver, TClientSettings().Database("/Root/Tenant1"));
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[2] << "u];[\"d\"];[1u];[400u]];";
            result << "[[" << buckets[2] << "u];[\"c\"];[2u];[300u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, QueryText, Rank, ReadBytes ";
            query << "FROM `/Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << buckets[2] << "ul as Timestamp) ";
            query << "AND Rank > 1u AND Rank <= 3u";

            TTableClient client(driver, TClientSettings().Database("/Root/Tenant1"));
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[2] << "u];[\"c\"];[2u];[300u]];";
            result << "[[" << buckets[2] << "u];[\"b\"];[3u];[200u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(QueryStatsFields) {
        TTestEnv env;
        CreateRootTable(env, 3);

        auto nowUs = TInstant::Now().MicroSeconds();

        TString queryText("SELECT * FROM `/Root/Table0`");

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            queryText, TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT
                CPUTime,
                CompileCPUTime,
                CompileDuration,
                ComputeNodesCount,
                DeleteBytes,
                DeleteRows,
                Duration,
                EndTime,
                FromQueryCache,
                IntervalEnd,
                MaxComputeCPUTime,
                MaxShardCPUTime,
                MinComputeCPUTime,
                MinShardCPUTime,
                ParametersSize,
                Partitions,
                ProcessCPUTime,
                QueryText,
                Rank,
                ReadBytes,
                ReadRows,
                RequestUnits,
                ShardCount,
                SumComputeCPUTime,
                SumShardCPUTime,
                Type,
                UpdateBytes,
                UpdateRows,
                UserSID
            FROM `/Root/.sys/top_queries_by_read_bytes_one_minute`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);

        TYsonFieldChecker check(ysonString, 29);

        check.Uint64GreaterOrEquals(0); // CPUTime
        check.Uint64GreaterOrEquals(0); // CompileCPUTime
        check.Int64GreaterOrEquals(0); // CompileDuration
        check.Uint64(2); // ComputeNodesCount
        check.Uint64(0); // DeleteBytes
        check.Uint64(0); // DeleteRows
        check.Int64Greater(0); // Duration
        check.Uint64Greater(nowUs); // EndTime
        check.Bool(false); // FromQueryCache
        check.Uint64Greater(nowUs); // IntervalEnd
        check.Uint64GreaterOrEquals(0); // MaxComputeCPUTime
        check.Uint64GreaterOrEquals(0); // MaxShardCPUTime
        check.Uint64GreaterOrEquals(0); // MinComputeCPUTime
        check.Uint64GreaterOrEquals(0); // MinShardCPUTime
        check.Uint64(0); // ParametersSize
        check.Uint64(3); // Partitions
        check.Uint64GreaterOrEquals(0); // ProcessCPUTime
        check.String(queryText); // QueryText
        check.Uint64(1); // Rank
        check.Uint64(0); // ReadBytes
        check.Uint64(0); // ReadRows
        check.Uint64Greater(0); // RequestUnits

        // https://a.yandex-team.ru/arcadia/ydb/core/sys_view/query_stats/query_stats.cpp?rev=r9637451#L356
        check.Uint64(0); // ShardCount
        check.Uint64GreaterOrEquals(0); // SumComputeCPUTime
        check.Uint64GreaterOrEquals(0); // SumShardCPUTime
        check.String("data"); // Type
        check.Uint64(0); // UpdateBytes
        check.Uint64(0); // UpdateRows
        check.Null(); // UserSID
    }

    Y_UNIT_TEST(PartitionStatsTtlFields) {
        TTestEnv env;
        env.GetClient().CreateTable("/Root", R"(
            Name: "Table0"
            Columns { Name: "Key", Type: "Uint64" }
            Columns { Name: "CreatedAt", Type: "Timestamp" }
            KeyColumnNames: ["Key"]
            TTLSettings {
              Enabled {
                ColumnName: "CreatedAt"
              }
            }
        )");

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            "REPLACE INTO `/Root/Table0` (Key, CreatedAt) VALUES (0u, CAST(0 AS Timestamp));",
            TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        // wait for conditional erase
        for (size_t iter = 0; iter < 70; ++iter) {
            auto result = session.ExecuteDataQuery(
                "SELECT * FROM `/Root/Table0`;", TTxControl::BeginTx().CommitTx()
            ).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto ysonString = FormatResultSetYson(result.GetResultSet(0));
            if (ysonString == "[[#]]") {
                break;
            }

            Sleep(TDuration::Seconds(1));
        }

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT
                LastTtlRunTime,
                LastTtlRowsProcessed,
                LastTtlRowsErased
            FROM `/Root/.sys/partition_stats`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);

        TYsonFieldChecker check(ysonString, 3);

        check.Uint64LessOrEquals(TInstant::Now().MicroSeconds()); // LastTtlRunTime
        check.Uint64(1u); // LastTtlRowsProcessed
        check.Uint64(1u); // LastTtlRowsErased
    }

    Y_UNIT_TEST_TWIN(PartitionStatsAfterDropTable, UseColumnTable) {
        TTestEnv env({.DataShardStatsReportIntervalSeconds = 0});
        if (UseColumnTable)
            CreateRootColumnTable(env);
        else
            CreateRootTable(env);

        TTableClient client(env.GetDriver());

        WaitForStats(client, "/Root/.sys/partition_stats", "Path = '/Root/Table0'");

        auto session = client.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(R"(
            DROP TABLE `/Root/Table0`;
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // Verify Table0 is removed from partition_stats
        auto table0Count = GetRowCount(client, "/Root/.sys/partition_stats", "Path = '/Root/Table0'");
        UNIT_ASSERT_VALUES_EQUAL(table0Count, 0);
    }

    Y_UNIT_TEST(PartitionStatsLocksFields) {
        TTestEnv env({.DataShardStatsReportIntervalSeconds = 0});
        CreateRootTable(env, /* partitionCount */ 1, /* fillTable */ true);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        BreakLock(session, "/Root/Table0");

        WaitForStats(client, "/Root/.sys/partition_stats", "LocksBroken != 0");

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT
                LocksAcquired,
                LocksWholeShard,
                LocksBroken
            FROM `/Root/.sys/partition_stats`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);
        TYsonFieldChecker check(ysonString, 3);

        check.Uint64(1); // LocksAcquired
        check.Uint64(0); // LocksWholeShard
        check.Uint64(1); // LocksBroken
    }

    Y_UNIT_TEST_TWIN(PartitionStatsAfterRenameTable, UseColumnTable) {
        TTestEnv env({.DataShardStatsReportIntervalSeconds = 0});
        if (UseColumnTable)
            CreateRootColumnTable(env);
        else
            CreateRootTable(env);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        WaitForStats(client, "/Root/.sys/partition_stats", "Path = '/Root/Table0'");

        auto result = session.ExecuteSchemeQuery(R"(
            ALTER TABLE `/Root/Table0` RENAME TO `/Root/Table1`;
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        WaitForStats(client, "/Root/.sys/partition_stats", "Path = '/Root/Table1'");

        // Verify Table0 is no longer in partition_stats
        auto table0Count = GetRowCount(client, "/Root/.sys/partition_stats", "Path = '/Root/Table0'");
        UNIT_ASSERT_VALUES_EQUAL(table0Count, 0);

        // Verify Table1 exists in partition_stats
        auto table1Count = GetRowCount(client, "/Root/.sys/partition_stats", "Path = '/Root/Table1'");
        UNIT_ASSERT_VALUES_EQUAL(table1Count, 1);
    }

    Y_UNIT_TEST(PartitionStatsFields) {
        auto nowUs = TInstant::Now().MicroSeconds();

        TTestEnv env({.DataShardStatsReportIntervalSeconds = 0});
        CreateRootTable(env);
        const auto describeResult = env.GetClient().Describe(env.GetServer().GetRuntime(), "/Root/Table0");
        const auto tablePathId = describeResult.GetPathId();

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            "REPLACE INTO `/Root/Table0` (Key, Value) VALUES (0u, \"A\");",
            TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        // wait for stats
        for (size_t iter = 0; iter < 30; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT AccessTime FROM `/Root/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto ysonString = NKqp::StreamResultToYson(it);
            if (ysonString != "[[#]]") {
                break;
            }

            Sleep(TDuration::Seconds(1));
        }

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT
                AccessTime,
                CPUCores,
                CoordinatedTxCompleted,
                DataSize,
                ImmediateTxCompleted,
                IndexSize,
                InFlightTxCount,
                NodeId,
                OwnerId,
                PartIdx,
                Path,
                PathId,
                RangeReadRows,
                RangeReads,
                RowCount,
                RowDeletes,
                RowReads,
                RowUpdates,
                StartTime,
                TabletId,
                TxRejectedByOutOfStorage,
                TxRejectedByOverload,
                TxCompleteLag,
                FollowerId,
                LocksAcquired,
                LocksWholeShard,
                LocksBroken,
                UpdateTime
            FROM `/Root/.sys/partition_stats`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);
        TYsonFieldChecker check(ysonString, 28);

        check.Uint64GreaterOrEquals(nowUs); // AccessTime
        check.DoubleGreaterOrEquals(0.0); // CPUCores
        check.Uint64(1u); // CoordinatedTxCompleted
        check.Uint64(584u); // DataSize
        check.Uint64(1u); // ImmediateTxCompleted
        check.Uint64(0u); // IndexSize
        check.Uint64(0u); // InFlightTxCount
        check.Uint64Greater(0u); // NodeId
        check.Uint64(72057594046644480ull); // OwnerId
        check.Uint64(0u); // PartIdx
        check.String("/Root/Table0"); // Path
        check.Uint64(tablePathId); // PathId
        check.Uint64(0u); // RangeReadRows
        check.Uint64(0u); // RangeReads
        check.Uint64(1u); // RowCount
        check.Uint64(0u); // RowDeletes
        check.Uint64(0u); // RowReads
        check.Uint64(1u); // RowUpdates
        check.Uint64GreaterOrEquals(nowUs); // StartTime
        check.Uint64Greater(0u); // TabletId
        check.Uint64(0u); // TxRejectedByOutOfStorage
        check.Uint64(0u); // TxRejectedByOverload
        check.Int64(0); // TxCompleteLag
        check.Uint64(0u); // FollowerId
        check.Uint64(0u); // LocksAcquired
        check.Uint64(0u); // LocksWholeShard
        check.Uint64(0u); // LocksBroken
        check.Uint64GreaterOrEquals(nowUs); // UpdateTime
    }

    Y_UNIT_TEST(QueryStatsAllTables) {
        auto check = [&] (const TString& queryText) {
            TTestEnv env;
            CreateRootTable(env);

            TTableClient client(env.GetDriver());
            auto session = client.CreateSession().GetValueSync().GetSession();
            NKqp::AssertSuccessResult(session.ExecuteDataQuery(
                "SELECT * FROM `/Root/Table0`", TTxControl::BeginTx().CommitTx()
            ).GetValueSync());

            auto it = client.StreamExecuteScanQuery(queryText).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::CompareYson(R"([
                [[0u]]
            ])", NKqp::StreamResultToYson(it));
        };

        check("SELECT ReadBytes FROM `/Root/.sys/top_queries_by_read_bytes_one_minute`");
        check("SELECT ReadBytes FROM `/Root/.sys/top_queries_by_read_bytes_one_hour`");
        check("SELECT ReadBytes FROM `/Root/.sys/top_queries_by_duration_one_minute`");
        check("SELECT ReadBytes FROM `/Root/.sys/top_queries_by_duration_one_hour`");
        check("SELECT ReadBytes FROM `/Root/.sys/top_queries_by_cpu_time_one_minute`");
        check("SELECT ReadBytes FROM `/Root/.sys/top_queries_by_cpu_time_one_hour`");
        check("SELECT ReadBytes FROM `/Root/.sys/top_queries_by_request_units_one_minute`");
        check("SELECT ReadBytes FROM `/Root/.sys/top_queries_by_request_units_one_hour`");
    }

    Y_UNIT_TEST(SysViewScanBackPressure) {
        NKikimrConfig::TTableServiceConfig tableServiceConfig;
        tableServiceConfig.MutableResourceManager()->SetChannelBufferSize(1_KB);

        TTestEnv env({
            .EnableSVP = true,
            .ShowCreateTable = true,
            .TableServiceConfig = tableServiceConfig,
        });
        CreateTenant(env, "Tenant1", true, /* nodesCount */ 1);

        TDriver driver(TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/Tenant1"));
        NQuery::TQueryClient queryClient(driver);

        // PAYLOAD_SIZE * NUMBER_OF_QUERIES should be greater than BatchSizeLimit in TSysViewProcessor (4 MB)
        constexpr ui64 PAYLOAD_SIZE = 100_KB;
        constexpr ui64 NUMBER_OF_QUERIES = 100;

        auto& actorSystem = *env.GetServer().GetRuntime()->GetActorSystem(env.GetTenants().List("/Root/Tenant1")[0]);
        ui64 amountSize = 0;
        const std::string payload(PAYLOAD_SIZE, 'X');
        for (ui64 i = 0; i < NUMBER_OF_QUERIES; ++i) {
            const auto queryText = TStringBuilder() << "SELECT * FROM `/Root/Tenant1/Table0` /* " << i << " = " << payload << " */";
            amountSize += queryText.size();

            auto collectEv = std::make_unique<NSysView::TEvSysView::TEvCollectQueryStats>();
            collectEv->Database = "/Root/Tenant1";
            collectEv->QueryStats.SetQueryTextHash(i);
            collectEv->QueryStats.SetQueryText(queryText);
            collectEv->QueryStats.SetEndTimeMs(TInstant::Now().MilliSeconds());
            actorSystem.Send(NSysView::MakeSysViewServiceID(actorSystem.NodeId), collectEv.release());
        }

        WaitFor(TDuration::Minutes(1), "statistics delivery", [&](TString& error) {
            const auto result = queryClient.ExecuteQuery(
                "SELECT SUM(Len(QueryText)) FROM `/Root/Tenant1/.sys/query_metrics_one_minute`",
                NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ClientTimeout(TDuration::Minutes(1))
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());

            auto resultSet = result.GetResultSetParser(0);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
            UNIT_ASSERT(resultSet.TryNextRow());

            const auto currentSum = resultSet.ColumnParser(0).GetOptionalUint64().value_or(0);
            error = TStringBuilder() << "currentSum = " << currentSum << ", expectedSum = " << amountSize;
            return currentSum >= amountSize;
        });
    }

    Y_UNIT_TEST(QueryStatsRetries) {
        TTestEnv env;
        CreateRootTable(env);

        TString queryText("SELECT * FROM `/Root/Table0`");

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            queryText, TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        auto serviceToKill = MakeSysViewServiceID(env.GetServer().GetRuntime()->GetNodeId(2));
        env.GetServer().GetRuntime()->Send(new IEventHandle(serviceToKill, TActorId(), new TEvents::TEvPoison()));

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT
                ReadBytes
            FROM `/Root/.sys/top_queries_by_read_bytes_one_minute`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        NKqp::CompareYson(R"([
            [[0u]]
        ])", NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(ConcurrentScans) {
        TTestEnv env;
        CreateRootTable(env);
        TTableClient client(env.GetDriver());

        TVector<TAsyncScanQueryPartIterator> futures;
        for (size_t i = 0; i < 20; ++i) {
            auto future0 = client.StreamExecuteScanQuery(R"(
                SELECT
                    ReadBytes
                FROM `/Root/.sys/top_queries_by_read_bytes_one_minute`;
            )");
            futures.push_back(future0);
        }

        for (auto& future0 : futures) {
            auto it = future0.GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto streamPart = it.ReadNext().GetValueSync();
            if (streamPart.IsSuccess()) {
                UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SUCCESS);
                Cerr << "SUCCESS" << Endl;
            } else {
                UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::OVERLOADED);
                Cerr << "FAIL " << streamPart.GetIssues().ToString() << Endl;
            }
        }
    }

    Y_UNIT_TEST(PDisksFields) {
        TTestEnv env(1, 0);

        TTableClient client(env.GetDriver());
        size_t rowCount = 0;
        TString ysonString;

        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT
                    AvailableSize,
                    BoxId,
                    DecommitStatus,
                    ExpectedSlotCount,
                    Guid,
                    Kind,
                    NodeId,
                    NumActiveSlots,
                    Path,
                    PDiskId,
                    ReadCentric,
                    SharedWithOS,
                    SlotSizeInUnits,
                    State,
                    Status,
                    StatusChangeTimestamp,
                    TotalSize,
                    Type
                FROM `/Root/.sys/ds_pdisks`
                WHERE BoxId IS NOT NULL;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            ysonString = NKqp::StreamResultToYson(it);

            auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
            UNIT_ASSERT(node.IsList());
            rowCount = node.AsList().size();

            if (!rowCount) {
                Sleep(TDuration::Seconds(1));
            }
        }

        TYsonFieldChecker check(ysonString, 18);

        check.Uint64(0u); // AvailableSize
        check.Uint64(999u); // BoxId
        check.String("DECOMMIT_NONE"); // DecommitStatus
        check.Uint64(16); // ExpectedSlotCount
        check.Uint64(123u); // Guid
        check.Uint64(0u); // Kind
        check.Uint64(env.GetServer().GetRuntime()->GetNodeId(0)); // NodeId
        check.Uint64(2); // NumActiveSlots
        check.StringContains("pdisk_1.dat"); // Path
        check.Uint64(1u); // PDiskId
        check.Bool(false); // ReadCentric
        check.Bool(false); // SharedWithOS
        check.Uint64(0u); // SlotSizeInUnits
        check.String("Initial"); // State
        check.String("ACTIVE"); // Status
        check.Null(); // StatusChangeTimestamp
        check.Uint64(0u); // TotalSize
        check.String("ROT"); // Type
    }

    Y_UNIT_TEST(VSlotsFields) {
        TTestEnv env(1, 0);

        TTableClient client(env.GetDriver());
        size_t rowCount = 0;
        TString ysonString;

        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT
                    AllocatedSize,
                    AvailableSize,
                    DiskSpace,
                    FailDomain,
                    FailRealm,
                    GroupGeneration,
                    GroupId,
                    Kind,
                    NodeId,
                    PDiskId,
                    Replicated,
                    State,
                    Status,
                    VDisk,
                    VSlotId
                FROM `/Root/.sys/ds_vslots` WHERE GroupId >= 0x80000000;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            ysonString = NKqp::StreamResultToYson(it);

            auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
            UNIT_ASSERT(node.IsList());
            rowCount = node.AsList().size();

            if (!rowCount) {
                Sleep(TDuration::Seconds(1));
            }
        }

        TYsonFieldChecker check(ysonString, 15);

        check.Uint64(0u, true); // AllocatedSize
        check.Uint64(0u, true); // AvailableSize
        check.Null(); // DiskSpace
        check.Uint64(0u); // FailDomain
        check.Uint64(0u); // FailRealm
        check.Uint64(1u); // GroupGeneration
        check.Uint64(2181038080u); // GroupId
        check.String("Default"); // Kind
        check.Uint64(env.GetServer().GetRuntime()->GetNodeId(0)); // NodeId
        check.Uint64(1u); // PDiskId
        check.Null(); // Replicated
        check.Null(); // State
        check.Null(); // Status
        check.Uint64(0u); // VDisk
        check.Uint64(1000u); // VSlotId
    }

    Y_UNIT_TEST(GroupsFields) {
        TTestEnv env;

        TTableClient client(env.GetDriver());
        size_t rowCount = 0;
        TString ysonString;

        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT
                    AllocatedSize,
                    AvailableSize,
                    BoxId,
                    EncryptionMode,
                    ErasureSpecies,
                    Generation,
                    GetFastLatency,
                    GroupId,
                    GroupSizeInUnits,
                    LifeCyclePhase,
                    PutTabletLogLatency,
                    PutUserDataLatency,
                    StoragePoolId,
                    LayoutCorrect,
                    OperatingStatus,
                    ExpectedStatus
                FROM `/Root/.sys/ds_groups` WHERE GroupId >= 0x80000000;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            ysonString = NKqp::StreamResultToYson(it);

            auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
            UNIT_ASSERT(node.IsList());
            rowCount = node.AsList().size();

            if (!rowCount) {
                Sleep(TDuration::Seconds(1));
            }
        }

        TYsonFieldChecker check(ysonString, 16);

        check.Uint64(0u); // AllocatedSize
        check.Uint64GreaterOrEquals(0u); // AvailableSize
        check.Uint64(999u); // BoxId
        check.Uint64(0u); // EncryptionMode
        check.String("none"); // ErasureSpecies
        check.Uint64(1u); // Generation
        check.Null(); // GetFastLatency
        check.Uint64(2181038080u); // GroupId
        check.Uint64(0u); // GroupSizeInUnits
        check.Uint64(0u); // LifeCyclePhase
        check.Null(); // PutTabletLogLatency
        check.Null(); // PutUserDataLatency
        check.Uint64(2u); // StoragePoolId
        check.Bool(true); // LayoutCorrect
        check.String("DISINTEGRATED"); // OperatingStatus
        check.String("DISINTEGRATED"); // ExpectedStatus
    }

    Y_UNIT_TEST(StoragePoolsFields) {
        TTestEnv env(1, 0);

        TTableClient client(env.GetDriver());
        size_t rowCount = 0;
        TString ysonString;

        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT
                    BoxId,
                    DefaultGroupSizeInUnits,
                    EncryptionMode,
                    ErasureSpecies,
                    Generation,
                    Kind,
                    Name,
                    NumGroups,
                    PathId,
                    SchemeshardId,
                    StoragePoolId,
                    VDiskKind
                FROM `/Root/.sys/ds_storage_pools`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            ysonString = NKqp::StreamResultToYson(it);

            auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
            UNIT_ASSERT(node.IsList());
            rowCount = node.AsList().size();

            if (!rowCount) {
                Sleep(TDuration::Seconds(1));
            }
        }

        TYsonFieldChecker check(ysonString, 12);

        check.Uint64(999u); // BoxId
        check.Uint64(0u); // DefaultGroupSizeInUnits
        check.Uint64(0u); // EncryptionMode
        check.String("none"); // ErasureSpecies
        check.Uint64(1u); // Generation
        check.String("test"); // Kind
        check.String("/Root:test"); // Name
        check.Uint64(1u); // NumGroups
        check.Null(); // PathId
        check.Null(); // SchemeshardId
        check.Uint64(2u); // StoragePoolId
        check.String("Default"); // VDiskKind
    }

    Y_UNIT_TEST(StoragePoolsRanges) {
        TTestEnv env(1, 0, {.StoragePools = 3});

        TTableClient client(env.GetDriver());
        size_t rowCount = 0;
        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT BoxId FROM `/Root/.sys/ds_storage_pools`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto ysonString = NKqp::StreamResultToYson(it);
            auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
            UNIT_ASSERT(node.IsList());
            rowCount = node.AsList().size();

            if (!rowCount) {
                Sleep(TDuration::Seconds(1));
            }
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT BoxId, StoragePoolId
                FROM `/Root/.sys/ds_storage_pools`
                WHERE BoxId = 999u AND StoragePoolId > 3u;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::CompareYson(R"([
                [[999u];[4u]];
                [[999u];[5u]];
            ])", NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT BoxId, StoragePoolId
                FROM `/Root/.sys/ds_storage_pools`
                WHERE BoxId = 999u AND StoragePoolId >= 3u;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::CompareYson(R"([
                [[999u];[3u]];
                [[999u];[4u]];
                [[999u];[5u]];
            ])", NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT BoxId, StoragePoolId
                FROM `/Root/.sys/ds_storage_pools`
                WHERE BoxId = 999u AND StoragePoolId < 4u;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::CompareYson(R"([
                [[999u];[2u]];
                [[999u];[3u]];
            ])", NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT BoxId, StoragePoolId
                FROM `/Root/.sys/ds_storage_pools`
                WHERE BoxId = 999u AND StoragePoolId <= 4u;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::CompareYson(R"([
                [[999u];[2u]];
                [[999u];[3u]];
                [[999u];[4u]];
            ])", NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(TopPartitionsByCpuFields) {
        auto nowUs = TInstant::Now().MicroSeconds();

        TTestEnv env(1, 4, {.EnableSVP = true, .DataShardStatsReportIntervalSeconds = 0});
        CreateTenantsAndTables(env);

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/Tenant1");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);
        size_t rowCount = 0;
        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            rowCount = GetRowCount(client, "/Root/Tenant1/.sys/top_partitions_one_minute");
            if (!rowCount) {
                Sleep(TDuration::Seconds(1));
            }
        }
        ui64 intervalEnd = GetIntervalEnd(client, "/Root/Tenant1/.sys/top_partitions_one_minute");

        TStringBuilder query;
        query << R"(
            SELECT
                IntervalEnd,
                Rank,
                TabletId,
                Path,
                PeakTime,
                CPUCores,
                NodeId,
                DataSize,
                RowCount,
                IndexSize,
                InFlightTxCount
            FROM `/Root/Tenant1/.sys/top_partitions_one_minute`)"
            << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp)";
        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);

        TYsonFieldChecker check(ysonString, 11);
        check.Uint64(intervalEnd); // IntervalEnd
        check.Uint64(1); // Rank
        check.Uint64Greater(0); // TabletId
        check.String("/Root/Tenant1/Table1"); // Path
        check.Uint64GreaterOrEquals(nowUs); // PeakTime
        check.DoubleGreaterOrEquals(0.); // CPUCores
        check.Uint64Greater(0); // NodeId
        check.Uint64Greater(0); // DataSize
        check.Uint64(3); // RowCount
        check.Uint64(0); // IndexSize
        check.Uint64(0); // InFlightTxCount
    }

    Y_UNIT_TEST(TopPartitionsByCpuTables) {
        constexpr ui64 partitionCount = 5;

        TTestEnv env(1, 4, {.EnableSVP = true, .DataShardStatsReportIntervalSeconds = 0});
        CreateTenantsAndTables(env, true, partitionCount);

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/Tenant1");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);
        size_t rowCount = 0;
        for (size_t iter = 0; iter < 30 && rowCount < partitionCount; ++iter) {
            rowCount = GetRowCount(client, "/Root/Tenant1/.sys/top_partitions_one_minute");
            if (rowCount < partitionCount) {
                Sleep(TDuration::Seconds(1));
            }
        }
        auto check = [&] (const TString& name) {
            ui64 intervalEnd = GetIntervalEnd(client, name);
            TStringBuilder query;
            query << "SELECT Rank ";
            query << "FROM `" << name << "` ";
            query << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) ";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::CompareYson("[[[1u]];[[2u]];[[3u]];[[4u]];[[5u]]]", NKqp::StreamResultToYson(it));
        };
        check("/Root/Tenant1/.sys/top_partitions_one_minute");
        check("/Root/Tenant1/.sys/top_partitions_one_hour");
    }

    Y_UNIT_TEST(TopPartitionsByCpuRanges) {
        constexpr ui64 partitionCount = 5;

        TTestEnv env(1, 4, {.EnableSVP = true, .DataShardStatsReportIntervalSeconds = 0});
        CreateTenantsAndTables(env, true, partitionCount);

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/Tenant1");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);
        size_t rowCount = 0;
        for (size_t iter = 0; iter < 30 && rowCount < partitionCount; ++iter) {
            rowCount = GetRowCount(client, "/Root/Tenant1/.sys/top_partitions_one_minute");
            if (rowCount < partitionCount) {
                Sleep(TDuration::Seconds(5));
            }
        }
        ui64 intervalEnd = GetIntervalEnd(client, "/Root/Tenant1/.sys/top_partitions_one_minute");
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, Rank ";
            query << "FROM `/Root/Tenant1/.sys/top_partitions_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) ";
            query << "AND Rank > 3u";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TStringBuilder result;
            result << "[";
            result << "[[" << intervalEnd << "u];[4u]];";
            result << "[[" << intervalEnd << "u];[5u]];";
            result << "]";
            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, Rank ";
            query << "FROM `/Root/Tenant1/.sys/top_partitions_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) ";
            query << "AND Rank >= 3u";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TStringBuilder result;
            result << "[";
            result << "[[" << intervalEnd << "u];[3u]];";
            result << "[[" << intervalEnd << "u];[4u]];";
            result << "[[" << intervalEnd << "u];[5u]];";
            result << "]";
            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, Rank ";
            query << "FROM `/Root/Tenant1/.sys/top_partitions_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) ";
            query << "AND Rank < 3u";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TStringBuilder result;
            result << "[";
            result << "[[" << intervalEnd << "u];[1u]];";
            result << "[[" << intervalEnd << "u];[2u]];";
            result << "]";
            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            TStringBuilder query;
            query << "SELECT IntervalEnd, Rank ";
            query << "FROM `/Root/Tenant1/.sys/top_partitions_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) ";
            query << "AND Rank <= 3u";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TStringBuilder result;
            result << "[";
            result << "[[" << intervalEnd << "u];[1u]];";
            result << "[[" << intervalEnd << "u];[2u]];";
            result << "[[" << intervalEnd << "u];[3u]];";
            result << "]";
            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(TopPartitionsByCpuFollowers) {
        auto nowUs = TInstant::Now().MicroSeconds();

        TTestEnv env(1, 4, {
            .EnableSVP = true,
            .EnableForceFollowers = true,
            .DataShardStatsReportIntervalSeconds = 0,
        });

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NLog::PRI_TRACE);

        CreateTenant(env, "Tenant1", true);

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/Tenant1");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);
        auto session = client.CreateSession().GetValueSync().GetSession();

        auto desc = TTableBuilder()
            .AddNullableColumn("Key", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("Key")
            .Build();

        auto settings = TCreateTableSettings()
            .ReplicationPolicy(TReplicationPolicy().ReplicasCount(3));

        auto result = session.CreateTable("/Root/Tenant1/Table1",
            std::move(desc), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        Cerr << "... UPSERT" << Endl;
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/Tenant1/Table1` (Key) VALUES (1u), (2u), (3u);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync());

        Cerr << "... SELECT from leader" << Endl;
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1;
            )", TTxControl::BeginTx().CommitTx()).GetValueSync();
            NKqp::AssertSuccessResult(result);

            TString actual = FormatResultSetYson(result.GetResultSet(0));
            NKqp::CompareYson(R"([
                [[1u]]
            ])", actual);
        }

        Cerr << "... SELECT from follower" << Endl;
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 2;
            )", TTxControl::BeginTx(TTxSettings::StaleRO()).CommitTx()).ExtractValueSync();
            NKqp::AssertSuccessResult(result);

            TString actual = FormatResultSetYson(result.GetResultSet(0));
            NKqp::CompareYson(R"([
                [[2u]]
            ])", actual);
        }

        size_t rowCount = 0;
        for (size_t iter = 0; iter < 30; ++iter) {
            if (rowCount = GetRowCount(client, "/Root/Tenant1/.sys/top_partitions_one_minute", "FollowerId != 0"))
                break;
            Sleep(TDuration::Seconds(5));
        }
        UNIT_ASSERT_GE(rowCount, 0);

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT
                    IntervalEnd,
                    Rank,
                    TabletId,
                    Path,
                    PeakTime,
                    CPUCores,
                    NodeId,
                    DataSize,
                    RowCount,
                    IndexSize,
                    InFlightTxCount,
                    FollowerId,
                    IF(FollowerId = 0, 'L', 'F') AS LeaderFollower
                FROM `/Root/Tenant1/.sys/top_partitions_one_minute`
                ORDER BY IntervalEnd, Rank;
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            NKqp::AssertSuccessResult(result);

            auto rs = result.GetResultSet(0);

            TString actual = FormatResultSetYson(rs);
            Cerr << "\n\n\n\n\n\n\n\n" << actual << "\n\n\n\n\n\n\n\n" << Endl;
        }

        ui64 intervalEnd = GetIntervalEnd(client, "/Root/Tenant1/.sys/top_partitions_one_minute");



        Cerr << "... SELECT leader from .sys/top_partitions_one_minute" << Endl;
        {
            TStringBuilder query;
            query << R"(
                SELECT
                    IntervalEnd,
                    Rank,
                    TabletId,
                    Path,
                    PeakTime,
                    CPUCores,
                    NodeId,
                    DataSize,
                    RowCount,
                    IndexSize,
                    InFlightTxCount,
                    FollowerId
                FROM `/Root/Tenant1/.sys/top_partitions_one_minute`)"
                << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) AND FollowerId = 0";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto ysonString = NKqp::StreamResultToYson(it);

            TYsonFieldChecker check(ysonString, 12);
            check.Uint64(intervalEnd); // IntervalEnd
            check.Uint64(1); // Rank
            check.Uint64Greater(0); // TabletId
            check.String("/Root/Tenant1/Table1"); // Path
            check.Uint64GreaterOrEquals(nowUs); // PeakTime
            check.DoubleGreaterOrEquals(0.); // CPUCores
            check.Uint64Greater(0); // NodeId
            check.Uint64Greater(0); // DataSize
            check.Uint64(3); // RowCount
            check.Uint64(0); // IndexSize
            check.Uint64(0); // InFlightTxCount
            check.Uint64(0); // FollowerId
        }

        Cerr << "... SELECT follower from .sys/top_partitions_one_minute" << Endl;
        {
            TStringBuilder query;
            query << R"(
                SELECT
                    IntervalEnd,
                    Rank,
                    TabletId,
                    Path,
                    PeakTime,
                    CPUCores,
                    NodeId,
                    DataSize,
                    RowCount,
                    IndexSize,
                    InFlightTxCount,
                    FollowerId
                FROM `/Root/Tenant1/.sys/top_partitions_one_minute`)"
                << "WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp) AND FollowerId != 0";
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto ysonString = NKqp::StreamResultToYson(it);

            TYsonFieldChecker check(ysonString, 12);
            check.Uint64(intervalEnd); // IntervalEnd
            check.Uint64(2); // Rank
            check.Uint64Greater(0); // TabletId
            check.String("/Root/Tenant1/Table1"); // Path
            check.Uint64GreaterOrEquals(nowUs); // PeakTime
            check.DoubleGreaterOrEquals(0.); // CPUCores
            check.Uint64Greater(0); // NodeId
            check.Uint64Greater(0); // DataSize
            check.Uint64(3); // RowCount
            check.Uint64(0); // IndexSize
            check.Uint64(0); // InFlightTxCount
            check.Uint64Greater(0); // FollowerId
        }
    }

    Y_UNIT_TEST(TopPartitionsByTliFields) {
        TTestEnv env(1, 4, {.EnableSVP = true, .DataShardStatsReportIntervalSeconds = 0});
        CreateTenantsAndTables(env);

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/Tenant1");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);
        auto session = client.CreateSession().GetValueSync().GetSession();

        const TString tableName = "/Root/Tenant1/Table1";
        const TString viewName = "/Root/Tenant1/.sys/top_partitions_by_tli_one_minute";

        BreakLock(session, tableName);

        WaitForStats(client, viewName, "LocksAcquired != 0");

        ui64 intervalEnd = GetIntervalEnd(client, viewName);

        TStringBuilder query;
        query << R"(
            SELECT
                IntervalEnd,
                Rank,
                TabletId,
                Path,
                LocksAcquired,
                LocksWholeShard,
                LocksBroken,
                NodeId,
                DataSize,
                RowCount,
                IndexSize)"
            << " FROM `" << viewName << "`"
            << " WHERE IntervalEnd = CAST(" << intervalEnd << "ul as Timestamp)"
            << " AND Path=\"" << tableName << "\"";
        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);
        TYsonFieldChecker check(ysonString, 11);
        check.Uint64(intervalEnd); // IntervalEnd
        check.Uint64(1); // Rank
        check.Uint64Greater(0); // TabletId
        check.String(tableName); // Path
        check.Uint64GreaterOrEquals(1); // LocksAcquired
        check.Uint64(0); // LocksWholeShard
        check.Uint64GreaterOrEquals(1); // LocksBroken
        check.Uint64Greater(0); // NodeId
        check.Uint64Greater(0); // DataSize
        check.Uint64(4); // RowCount
        check.Uint64(0); // IndexSize
    }

    Y_UNIT_TEST(Describe) {
        TTestEnv env;
        CreateRootTable(env);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            auto result = session.DescribeSystemView("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            const auto& systemView = result.GetSystemViewDescription();

            UNIT_ASSERT_VALUES_EQUAL(systemView.GetSysViewId(), 1);
            UNIT_ASSERT_VALUES_EQUAL(systemView.GetSysViewName(), "partition_stats");

            const auto& columns = systemView.GetTableColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 31);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0].Name, "OwnerId");
            UNIT_ASSERT_STRINGS_EQUAL(FormatType(columns[0].Type), "Uint64?");

            const auto& keyColumns = systemView.GetPrimaryKeyColumns();
            UNIT_ASSERT_VALUES_EQUAL(keyColumns.size(), 4);
            UNIT_ASSERT_STRINGS_EQUAL(keyColumns[0], "OwnerId");
        }

        TSchemeClient schemeClient(env.GetDriver());
        {
            auto result = schemeClient.DescribePath("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "partition_stats");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::SysView);
        }
        {
            auto result = schemeClient.ListDirectory("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "partition_stats");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::SysView);
        }
    }

    Y_UNIT_TEST(SystemViewFailOps) {
        TTestEnv env;
        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);

        // Make AdministrationAllowedSIDs non-empty to deny any user cluster admin privilege.
        // That can cause side effects, especially when dealing with system reserved names.
        // Using an authorized non-admin user helps avoid these side effects.
        env.GetServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.push_back("root@builtin");

        TTableClient adminClient(env.GetDriver(), TClientSettings().AuthToken("root@builtin"));
        auto adminSession = adminClient.CreateSession().GetValueSync().GetSession();

        {
            auto query = TStringBuilder() << R"(
                --!syntax_v1
                GRANT 'ydb.generic.full' ON `/Root` TO `user@builtin`;
                )";
            auto result = adminSession.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        TTableClient userClient(env.GetDriver(), TClientSettings().AuthToken("user@builtin"));
        auto userSession = userClient.CreateSession().GetValueSync().GetSession();

        {
            auto desc = TTableBuilder()
                .AddNullableColumn("Column1", EPrimitiveType::Uint64)
                .SetPrimaryKeyColumn("Column1")
                .Build();

            auto result = userSession.CreateTable("/Root/.sys/partition_stats", std::move(desc)).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSession.CopyTable("/Root/.sys/partition_stats", "/Root/Table0").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto settings = TAlterTableSettings()
                .AppendDropColumns("OwnerId");

            auto result = userSession.AlterTable("/Root/.sys/partition_stats", settings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSession.DropTable("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSession.ExecuteSchemeQuery(R"(
                DROP TABLE `/Root/.sys/partition_stats`;
            )").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSession.ReadTable("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            TReadTableResultPart streamPart = result.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SCHEME_ERROR);
            streamPart.GetIssues().PrintTo(Cerr);
        }
        {
            TValueBuilder rows;
            rows.BeginList().EndList();
            auto result = userClient.BulkUpsert("/Root/.sys/partition_stats", rows.Build()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }

        auto driverConfig = env.GetDriver().GetConfig();
        driverConfig.SetAuthToken("user@builtin");
        const auto driver = TDriver(driverConfig);
        auto userSchemeClient = TSchemeClient(driver);
        {
            auto result = userSchemeClient.MakeDirectory("/Root/.sys").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "path exist", result.GetIssues().ToString());
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSchemeClient.MakeDirectory("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSchemeClient.RemoveDirectory("/Root/.sys").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = userSchemeClient.RemoveDirectory("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            TModifyPermissionsSettings settings;
            auto result = userSchemeClient.ModifyPermissions("/Root/.sys/partition_stats", settings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            result.GetIssues().PrintTo(Cerr);
        }
    }

    Y_UNIT_TEST(DescribeSystemFolder) {
        TTestEnv env;
        CreateTenantsAndTables(env, true);

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off);
        auto driver = TDriver(driverConfig);

        {
            TSchemeClient schemeClient(driver, TCommonClientSettings().Database("/Root"));
            auto result = schemeClient.ListDirectory("/Root").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_STRINGS_EQUAL(entry.Name, "Root");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Directory);

            auto children = result.GetChildren();
            SortBy(children, [](const auto& entry) { return entry.Name; });
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 5);
            UNIT_ASSERT_STRINGS_EQUAL(children[0].Name, ".metadata");
            UNIT_ASSERT_STRINGS_EQUAL(children[1].Name, ".sys");
            UNIT_ASSERT_STRINGS_EQUAL(children[2].Name, "Table0");
            UNIT_ASSERT_STRINGS_EQUAL(children[3].Name, "Tenant1");
            UNIT_ASSERT_STRINGS_EQUAL(children[4].Name, "Tenant2");
        }
        {
            TSchemeClient schemeClient(driver, TCommonClientSettings().Database("/Root/Tenant1"));
            auto result = schemeClient.ListDirectory("/Root/Tenant1").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "Root/Tenant1");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::SubDomain);

            auto children = result.GetChildren();
            SortBy(children, [](const auto& entry) { return entry.Name; });
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(children[0].Name, ".metadata");
            UNIT_ASSERT_STRINGS_EQUAL(children[1].Name, ".sys");
            UNIT_ASSERT_STRINGS_EQUAL(children[2].Name, "Table1");
        }
        {
            TSchemeClient schemeClient(driver, TCommonClientSettings().Database("/Root"));
            auto result = schemeClient.ListDirectory("/Root/.sys").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, ".sys");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Directory);

            auto children = result.GetChildren();
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 35);

            THashSet<TString> names;
            for (const auto& child : children) {
                names.insert(TString{child.Name});
                UNIT_ASSERT_VALUES_EQUAL(child.Type, ESchemeEntryType::SysView);
            }
            UNIT_ASSERT(names.contains("partition_stats"));
        }
        {
            TSchemeClient schemeClient(driver, TCommonClientSettings().Database("/Root/Tenant1"));
            auto result = schemeClient.ListDirectory("/Root/Tenant1/.sys").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, ".sys");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Directory);

            auto children = result.GetChildren();

            UNIT_ASSERT_VALUES_EQUAL(children.size(), 29);

            THashSet<TString> names;
            for (const auto& child : children) {
                names.insert(TString{child.Name});
                UNIT_ASSERT_VALUES_EQUAL(child.Type, ESchemeEntryType::SysView);
            }
            UNIT_ASSERT(names.contains("partition_stats"));
        }
        {
            TSchemeClient schemeClient(driver, TCommonClientSettings().Database("/Root/Tenant1"));
            auto result = schemeClient.ListDirectory("/Root/Tenant1/Table1/.sys").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
    }

    Y_UNIT_TEST(DescribeAccessDenied) {
        TTestEnv env;
        CreateTenantsAndTables(env, false);

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetAuthToken("user0@builtin");
        auto driver = TDriver(driverConfig);

        {
            TSchemeClient schemeClient(driver, TCommonClientSettings().Database("/Root"));

            {
                auto result = schemeClient.ListDirectory("/Root").GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
                result.GetIssues().PrintTo(Cerr);
            }

            {
                auto result = schemeClient.ListDirectory("/Root/.sys").GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
                result.GetIssues().PrintTo(Cerr);
            }

            {
                auto result = schemeClient.DescribePath("/Root/.sys/partition_stats").GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
                result.GetIssues().PrintTo(Cerr);
            }
        }
        {
            TSchemeClient schemeClient(driver, TCommonClientSettings().Database("/Root/Tenant1"));

            {
                auto result = schemeClient.ListDirectory("/Root/Tenant1").GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
                result.GetIssues().PrintTo(Cerr);
            }
            {
                auto result = schemeClient.ListDirectory("/Root/Tenant1/.sys").GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
                result.GetIssues().PrintTo(Cerr);
            }
            {
                auto result = schemeClient.DescribePath("/Root/Tenant1/.sys/partition_stats").GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
                result.GetIssues().PrintTo(Cerr);
            }
        }
    }

    Y_UNIT_TEST(TabletsFields) {
        TTestEnv env(1, 0);
        CreateRootTable(env);

        TTableClient client(env.GetDriver());
        auto it = client.StreamExecuteScanQuery(R"(
            SELECT
                BootState,
                CPU,
                Generation,
                Memory,
                Network,
                NodeId,
                FollowerId,
                State,
                TabletId,
                Type,
                VolatileState
            FROM `/Root/.sys/hive_tablets`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);

        TYsonFieldChecker check(ysonString, 11);

        check.String("Running"); // BootState
        check.DoubleGreaterOrEquals(0.0); // CPU
        check.Uint64(1u); // Generation
        check.Uint64GreaterOrEquals(0u); // Memory
        check.Uint64(0u); // Network
        check.Uint64(env.GetServer().GetRuntime()->GetNodeId(0)); // NodeId
        check.Uint64(0u); // FollowerId
        check.String("ReadyToWork"); // State
        check.Uint64(72075186224037888ul); // TabletId
        check.String("DataShard"); // Type
        check.String("Running"); // VolatileState
    }

    Y_UNIT_TEST(TabletsShards) {
        TTestEnv env(1, 0);
        CreateRootTable(env, 3);

        TTableClient client(env.GetDriver());
        auto it = client.StreamExecuteScanQuery(R"(
            SELECT FollowerId, TabletId, Type
            FROM `/Root/.sys/hive_tablets`;
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [[0u];[72075186224037888u];["DataShard"]];
            [[0u];[72075186224037889u];["DataShard"]];
            [[0u];[72075186224037890u];["DataShard"]];
        ])", NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(TabletsFollowers) {
        TTestEnv env(1, 0);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        auto desc = TTableBuilder()
            .AddNullableColumn("Column1", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("Column1")
            .Build();

        auto settings = TCreateTableSettings()
            .ReplicationPolicy(TReplicationPolicy().ReplicasCount(3));

        auto result = session.CreateTable("/Root/Table0",
            std::move(desc), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto it = client.StreamExecuteScanQuery(R"(
            SELECT FollowerId, TabletId, Type
            FROM `/Root/.sys/hive_tablets`;
        )").GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        NKqp::CompareYson(R"([
            [[0u];[72075186224037888u];["DataShard"]];
            [[1u];[72075186224037888u];["DataShard"]];
            [[2u];[72075186224037888u];["DataShard"]];
            [[3u];[72075186224037888u];["DataShard"]];
        ])", NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(TabletsRanges) {
        TTestEnv env(1, 0);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        auto desc = TTableBuilder()
            .AddNullableColumn("Column1", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("Column1")
            .Build();

        auto settings = TCreateTableSettings()
            .ReplicationPolicy(TReplicationPolicy().ReplicasCount(3))
            .PartitioningPolicy(TPartitioningPolicy().UniformPartitions(3));

        auto result = session.CreateTable("/Root/Table0",
            std::move(desc), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        std::vector<std::pair<TString, TString>> testData = {
            {
                "TabletId = 72075186224037888ul AND FollowerId > 1u",
                R"([
                    [[2u];[72075186224037888u]];
                    [[3u];[72075186224037888u]];
                ])"
            },
            {
                "TabletId = 72075186224037888ul AND FollowerId >= 1u",
                R"([
                    [[1u];[72075186224037888u]];
                    [[2u];[72075186224037888u]];
                    [[3u];[72075186224037888u]];
                ])"
            },
            {
                "TabletId = 72075186224037888ul AND FollowerId < 2u",
                R"([
                    [[0u];[72075186224037888u]];
                    [[1u];[72075186224037888u]];
                ])"
            },
            {
                "TabletId = 72075186224037888ul AND FollowerId <= 2u",
                R"([
                    [[0u];[72075186224037888u]];
                    [[1u];[72075186224037888u]];
                    [[2u];[72075186224037888u]];
                ])"
            },
            {
                "TabletId > 72075186224037888ul AND TabletId < 72075186224037890ul",
                R"([
                    [[0u];[72075186224037889u]];
                    [[1u];[72075186224037889u]];
                    [[2u];[72075186224037889u]];
                    [[3u];[72075186224037889u]];
                ])"
            }
        };

        TString enablePredicateExtractor = R"(
            PRAGMA Kikimr.OptEnablePredicateExtract = "true";
        )";

        for (auto& data: testData) {
            TString query = R"(
                SELECT FollowerId, TabletId
                FROM `/Root/.sys/hive_tablets`
                WHERE <PREDICATE>;
            )";

            SubstGlobal(query, "<PREDICATE>", data.first);

            auto it = client.StreamExecuteScanQuery(enablePredicateExtractor + query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto streamed = NKqp::StreamResultToYson(it);

            it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto expected = NKqp::StreamResultToYson(it);

            // Compare two ways of execution
            NKqp::CompareYson(expected, streamed);
            // And check with expected result from test description
            NKqp::CompareYson(data.second, streamed);
        }
    }

    Y_UNIT_TEST(TabletsRangesPredicateExtractDisabled) {
        TTestEnv env(1, 0);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        auto desc = TTableBuilder()
            .AddNullableColumn("Column1", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("Column1")
            .Build();

        auto settings = TCreateTableSettings()
            .ReplicationPolicy(TReplicationPolicy().ReplicasCount(3))
            .PartitioningPolicy(TPartitioningPolicy().UniformPartitions(3));

        auto result = session.CreateTable("/Root/Table0",
            std::move(desc), settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        TString query = R"(
            SELECT FollowerId, TabletId
            FROM `/Root/.sys/hive_tablets`
            WHERE TabletId <= 72075186224037888ul OR TabletId >= 72075186224037890ul
            ORDER BY TabletId, FollowerId
        )";

        TString expected = R"([
            [[0u];[72075186224037888u]];
            [[1u];[72075186224037888u]];
            [[2u];[72075186224037888u]];
            [[3u];[72075186224037888u]];
            [[0u];[72075186224037890u]];
            [[1u];[72075186224037890u]];
            [[2u];[72075186224037890u]];
            [[3u];[72075186224037890u]];
        ])";

        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        // System view dows not support multiple ranges, thus here will be an error if
        // predicate extraction occurs.
        NKqp::CompareYson(expected, NKqp::StreamResultToYson(it));
    }

    void TestQueryType(
        std::function<void(const TTestEnv&, const TString&)> execQuery,
        const TString& type)
    {
        TTestEnv env(1, 0);
        CreateRootTable(env, 1, /* fillTable */ true);

        TString query("SELECT * FROM `/Root/Table0`");
        execQuery(env, query);

        TTableClient client(env.GetDriver());
        auto it = client.StreamExecuteScanQuery(R"(
            SELECT QueryText, Type, ReadRows
            FROM `/Root/.sys/top_queries_by_read_bytes_one_minute`
            ORDER BY ReadRows DESC
            LIMIT 1
            ;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        NKqp::CompareYson(
            Sprintf("[[[\"%s\"];[\"%s\"];[3u]]]", query.c_str(), type.c_str()),
            NKqp::StreamResultToYson(it));
    }

    Y_UNIT_TEST(CollectPreparedQueries) {
        TestQueryType([](const TTestEnv& env, const TString& query) {
            TTableClient client(env.GetDriver());
            auto session = client.CreateSession().GetValueSync().GetSession();
            auto prepareResult = session.PrepareDataQuery(query).GetValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
            auto prepared = prepareResult.GetQuery();
            auto result = prepared.Execute(TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }, "data");
    }

    Y_UNIT_TEST(CollectScanQueries) {
        TestQueryType([](const TTestEnv& env, const TString& query) {
            TTableClient client(env.GetDriver());
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::StreamResultToYson(it);
        }, "scan");
    }

    Y_UNIT_TEST(CollectScriptingQueries) {
        TestQueryType([](const TTestEnv& env, const TString& query) {
            auto scriptingClient = NYdb::NScripting::TScriptingClient(env.GetDriver());
            auto result = scriptingClient.ExecuteYqlScript(query).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }, "script");
    }

    Y_UNIT_TEST(QueryMetricsSimple) {
        TTestEnv env(1, 2, {.EnableSVP = true});
        CreateTenant(env, "Tenant1", true);

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDiscoveryMode(EDiscoveryMode::Off)
            .SetDatabase("/Root/Tenant1");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);
        auto session = client.CreateSession().GetValueSync().GetSession();

        NKqp::AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Tenant1/Table1` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync());

        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            "SELECT * FROM `/Root/Tenant1/Table1`", TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        size_t rowCount = 0;
        TString ysonString;

        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT SumReadBytes
                FROM `/Root/Tenant1/.sys/query_metrics_one_minute`
                WHERE QueryText = 'SELECT * FROM `/Root/Tenant1/Table1`';
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            ysonString = NKqp::StreamResultToYson(it);

            auto node = NYT::NodeFromYsonString(ysonString, ::NYson::EYsonType::Node);
            UNIT_ASSERT(node.IsList());
            rowCount = node.AsList().size();

            if (!rowCount) {
                Sleep(TDuration::Seconds(5));
            }
        }

        UNIT_ASSERT_GE(rowCount, 0);
        NKqp::CompareYson(R"([
            [[0u]];
        ])", ysonString);
    }
}
Y_UNIT_TEST_SUITE(ViewQuerySplit) {

Y_UNIT_TEST(Basic) {
    NYql::TIssues issues;
    TViewQuerySplit split;
    UNIT_ASSERT_C(SplitViewQuery("select 1", split, issues), issues.ToString());
    UNIT_ASSERT_STRINGS_EQUAL(split.ContextRecreation, "");
    UNIT_ASSERT_STRINGS_EQUAL(split.Select, "select 1");
}

Y_UNIT_TEST(WithPragmaTablePathPrefix) {
    NYql::TIssues issues;
    TViewQuerySplit split;
    UNIT_ASSERT_C(SplitViewQuery(
        "pragma tablepathprefix = \"/foo/bar\";\n"
        "select 1",
        split, issues
    ), issues.ToString());
    UNIT_ASSERT_STRINGS_EQUAL(split.ContextRecreation, "pragma tablepathprefix = \"/foo/bar\";\n");
    UNIT_ASSERT_STRINGS_EQUAL(split.Select, "select 1");
}

Y_UNIT_TEST(WithPairedPragmaTablePathPrefix) {
    NYql::TIssues issues;
    TViewQuerySplit split;
    UNIT_ASSERT_C(SplitViewQuery(
        "pragma tablepathprefix (\"foo\", \"/bar/baz\");\n"
        "select 1",
        split, issues
    ), issues.ToString());
    UNIT_ASSERT_STRINGS_EQUAL(split.ContextRecreation, "pragma tablepathprefix (\"foo\", \"/bar/baz\");\n");
    UNIT_ASSERT_STRINGS_EQUAL(split.Select, "select 1");
}

Y_UNIT_TEST(WithComments) {
    NYql::TIssues issues;
    TViewQuerySplit split;
    UNIT_ASSERT_C(SplitViewQuery(
        "-- what does the fox say?\n"
        "pragma tablepathprefix = \"/foo/bar\";\n"
        "select * from t",
        split, issues
    ), issues.ToString());
    UNIT_ASSERT_STRINGS_EQUAL(split.ContextRecreation,
        "-- what does the fox say?\n"
        "pragma tablepathprefix = \"/foo/bar\";\n"
    );
    UNIT_ASSERT_STRINGS_EQUAL(split.Select, "select * from t");
}

Y_UNIT_TEST(Joins) {
    NYql::TIssues issues;
    TViewQuerySplit split;
    UNIT_ASSERT_C(SplitViewQuery(
        "$x = \"/t\";\n"
        "$y = \"/tt\";\n"
        "select * from $x as x join $y as y on x.key == y.key",
        split, issues
    ), issues.ToString());
    UNIT_ASSERT_STRINGS_EQUAL(split.ContextRecreation,
        "$x = \"/t\";\n"
        "$y = \"/tt\";\n"
    );
    UNIT_ASSERT_STRINGS_EQUAL(split.Select, "select * from $x as x join $y as y on x.key == y.key");
}

}

} // NSysView
} // NKikimr
