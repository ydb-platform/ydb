#include "ut_common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>

#include <library/cpp/yson/node/node_io.h>

namespace NKikimr {
namespace NSysView {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace {

void CreateTenant(TTestEnv& env, const TString& tenantName, bool extSchemeShard = true) {
    auto subdomain = GetSubDomainDeclareSettings(tenantName);
    if (extSchemeShard) {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().CreateExtSubdomain("/Root", subdomain));
    } else {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().CreateSubdomain("/Root", subdomain));
    }

    env.GetTenants().Run("/Root/" + tenantName, 2);

    auto subdomainSettings = GetSubDomainDefaultSettings(tenantName, env.GetPools());
    subdomainSettings.SetExternalSysViewProcessor(true);

    if (extSchemeShard) {
        subdomainSettings.SetExternalSchemeShard(true);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().AlterExtSubdomain("/Root", subdomainSettings));
    } else {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().AlterSubdomain("/Root", subdomainSettings));
    }
}

void CreateTenants(TTestEnv& env, bool extSchemeShard = true) {
    CreateTenant(env, "Tenant1", extSchemeShard);
    CreateTenant(env, "Tenant2", extSchemeShard);
}

void CreateTable(auto& session, const TString& name, ui64 partitionCount = 1) {
    auto desc = TTableBuilder()
        .AddNullableColumn("Key", EPrimitiveType::Uint64)
        .AddNullableColumn("Value", EPrimitiveType::String)
        .SetPrimaryKeyColumns({"Key"})
        .Build();

    auto settings = TCreateTableSettings();
    settings.PartitioningPolicy(TPartitioningPolicy().UniformPartitions(partitionCount));

    session.CreateTable(name, std::move(desc), std::move(settings)).GetValueSync();
}

void CreateTables(TTestEnv& env, ui64 partitionCount = 1) {
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    CreateTable(session, "Root/Table0", partitionCount);
    NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
        REPLACE INTO `Root/Table0` (Key, Value) VALUES
            (0u, "Z");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync());

    CreateTable(session, "Root/Tenant1/Table1", partitionCount);
    NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
        REPLACE INTO `Root/Tenant1/Table1` (Key, Value) VALUES
            (1u, "A"),
            (2u, "B"),
            (3u, "C");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync());

    CreateTable(session, "Root/Tenant2/Table2", partitionCount);
    NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
        REPLACE INTO `Root/Tenant2/Table2` (Key, Value) VALUES
            (4u, "D"),
            (5u, "E");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync());
}

void CreateTenantsAndTables(TTestEnv& env, bool extSchemeShard = true, ui64 partitionCount = 1) {
    CreateTenants(env, extSchemeShard);
    CreateTables(env, partitionCount);
}

void CreateRootTable(TTestEnv& env, ui64 partitionCount = 1, bool fillTable = false, ui16 tableNum = 0) {
    env.GetClient().CreateTable("/Root", Sprintf(R"(
        Name: "Table%u"
        Columns { Name: "Key", Type: "Uint64" }
        Columns { Name: "Value", Type: "String" }
        KeyColumnNames: ["Key"]
        UniformPartitionsCount: %lu
    )", tableNum, partitionCount));

    if (fillTable) {
        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
            REPLACE INTO `Root/Table0` (Key, Value) VALUES
                (0u, "X"),
                (1u, "Y"),
                (2u, "Z");
        )", TTxControl::BeginTx().CommitTx()).GetValueSync());
    }
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
        CreateTenantsAndTables(env, false);
        TTableClient client(env.GetDriver());
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            NKqp::CompareYson(R"([
                [[4u];[0u];["/Root/Table0"]]
            ])", NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/Tenant1/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            NKqp::CompareYson(R"([
                [[5u];[0u];["/Root/Tenant1/Table1"]]
            ])", NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/Tenant2/.sys/partition_stats`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            NKqp::CompareYson(R"([
                [[6u];[0u];["/Root/Tenant2/Table2"]]
            ])", NKqp::StreamResultToYson(it));
        }
    }

    Y_UNIT_TEST(PartitionStatsOneSchemeShardDataQuery) {
        TTestEnv env;
        CreateTenantsAndTables(env, false);

        env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/.sys/partition_stats`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            NKqp::CompareYson(R"([
                [[4u];[0u];["/Root/Table0"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/Tenant1/.sys/partition_stats`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT(result.IsSuccess());
            NKqp::CompareYson(R"([
                [[5u];[0u];["/Root/Tenant1/Table1"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT PathId, PartIdx, Path FROM `Root/Tenant2/.sys/partition_stats`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT(result.IsSuccess());
            NKqp::CompareYson(R"([
                [[6u];[0u];["/Root/Tenant2/Table2"]]
            ])", FormatResultSetYson(result.GetResultSet(0)));
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
                SELECT schemaname, tablename, tableowner, tablespace, hasindexes, hasrules, hastriggers, rowsecurity FROM `Root/.sys/pg_tables` WHERE tablename = PgName("Table0") OR tablename = PgName("Table1") ORDER BY tablename;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            NKqp::CompareYson(R"([
                ["public";"Table0";"root@builtin";#;"t";"f";"f";"f"];
                ["public";"Table1";"root@builtin";#;"t";"f";"f";"f"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(Nodes) {
        return; // table is currenty switched off

        TTestEnv env;
        CreateTenantsAndTables(env, false);
        TTableClient client(env.GetDriver());
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Host, NodeId
                FROM `Root/Tenant1/.sys/nodes`;
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
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Host, NodeId
                FROM `Root/Tenant2/.sys/nodes`;
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
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT Host, NodeId
                FROM `Root/.sys/nodes`;
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            ui32 offset = env.GetServer().GetRuntime()->GetNodeId(0);
            auto expected = Sprintf(R"([
                [["::1"];[%du]];
                [["::1"];[%du]];
                [["::1"];[%du]];
                [["::1"];[%du]];
                [["::1"];[%du]];
            ])", offset, offset + 1, offset + 2, offset + 3, offset + 4);

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

        TTableClient client(env.GetDriver());
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT IntervalEnd, QueryText, Rank, ReadBytes
                FROM `Root/.sys/top_queries_by_read_bytes_one_minute`;
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            TStringBuilder result;
            result << "[";
            result << "[[" << buckets[1] << "u];[\"g\"];[1u];[700u]];";
            result << "]";

            NKqp::CompareYson(result, NKqp::StreamResultToYson(it));
        }
        {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT IntervalEnd, QueryText, Rank, ReadBytes
                FROM `Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute`;
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
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT IntervalEnd, QueryText, Rank, ReadBytes
                FROM `Root/Tenant2/.sys/top_queries_by_read_bytes_one_minute`;
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
            query << "FROM `Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute` ";
            query << "WHERE IntervalEnd >= CAST(" << buckets[1] << "ul as Timestamp) ";
            query << "AND IntervalEnd < CAST(" << buckets[3] << "ul as Timestamp);";

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
            query << "FROM `Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute` ";
            query << "WHERE IntervalEnd > CAST(" << buckets[1] << "ul as Timestamp) ";
            query << "AND IntervalEnd <= CAST(" << buckets[3] << "ul as Timestamp);";

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
            query << "FROM `Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << buckets[2] << "ul as Timestamp) ";
            query << "AND Rank >= 1u AND Rank < 3u";

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
            query << "FROM `Root/Tenant1/.sys/top_queries_by_read_bytes_one_minute` ";
            query << "WHERE IntervalEnd = CAST(" << buckets[2] << "ul as Timestamp) ";
            query << "AND Rank > 1u AND Rank <= 3u";

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

        TString queryText("SELECT * FROM `Root/Table0`");

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
            FROM `Root/.sys/top_queries_by_read_bytes_one_minute`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);

        TYsonFieldChecker check(ysonString, 29);

        bool iterators = env.GetSettings()->AppConfig->GetTableServiceConfig().GetEnableKqpDataQuerySourceRead();

        check.Uint64GreaterOrEquals(0); // CPUTime
        check.Uint64GreaterOrEquals(0); // CompileCPUTime
        check.Int64GreaterOrEquals(0); // CompileDuration
        check.Uint64(iterators ? 2 : 1); // ComputeNodesCount
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
        check.Uint64(iterators ? 0 : 3); // ShardCount
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
            "REPLACE INTO `Root/Table0` (Key, CreatedAt) VALUES (0u, CAST(0 AS Timestamp));",
            TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        // wait for conditional erase
        for (size_t iter = 0; iter < 70; ++iter) {
            auto result = session.ExecuteDataQuery(
                "SELECT * FROM `Root/Table0`;", TTxControl::BeginTx().CommitTx()
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

    Y_UNIT_TEST(PartitionStatsFields) {
        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);

        auto nowUs = TInstant::Now().MicroSeconds();

        TTestEnv env;
        CreateRootTable(env);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            "REPLACE INTO `Root/Table0` (Key, Value) VALUES (0u, \"A\");",
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
                UpdateTime
            FROM `/Root/.sys/partition_stats`;
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto ysonString = NKqp::StreamResultToYson(it);

        TYsonFieldChecker check(ysonString, 23);

        check.Uint64GreaterOrEquals(nowUs); // AccessTime
        check.DoubleGreaterOrEquals(0.0); // CPUCores
        check.Uint64(1u); // CoordinatedTxCompleted
        check.Uint64(576u); // DataSize
        check.Uint64(1u); // ImmediateTxCompleted
        check.Uint64(0u); // IndexSize
        check.Uint64(0u); // InFlightTxCount
        check.Uint64Greater(0u); // NodeId
        check.Uint64(72057594046644480ull); // OwnerId
        check.Uint64(0u); // PartIdx
        check.String("/Root/Table0"); // Path
        check.Uint64(2u); // PathId
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
        check.Uint64GreaterOrEquals(nowUs); // UpdateTime
    }

    Y_UNIT_TEST(QueryStatsAllTables) {
        auto check = [&] (const TString& queryText) {
            TTestEnv env;
            CreateRootTable(env);

            TTableClient client(env.GetDriver());
            auto session = client.CreateSession().GetValueSync().GetSession();
            NKqp::AssertSuccessResult(session.ExecuteDataQuery(
                "SELECT * FROM `Root/Table0`", TTxControl::BeginTx().CommitTx()
            ).GetValueSync());

            auto it = client.StreamExecuteScanQuery(queryText).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NKqp::CompareYson(R"([
                [[0u]]
            ])", NKqp::StreamResultToYson(it));
        };

        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_read_bytes_one_minute`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_read_bytes_one_hour`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_duration_one_minute`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_duration_one_hour`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_cpu_time_one_minute`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_cpu_time_one_hour`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_request_units_one_minute`");
        check("SELECT ReadBytes FROM `Root/.sys/top_queries_by_request_units_one_hour`");
    }

    Y_UNIT_TEST(QueryStatsRetries) {
        TTestEnv env;
        CreateRootTable(env);

        TString queryText("SELECT * FROM `Root/Table0`");

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
            FROM `Root/.sys/top_queries_by_read_bytes_one_minute`;
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
                FROM `Root/.sys/top_queries_by_read_bytes_one_minute`;
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
                    Guid,
                    Kind,
                    NodeId,
                    PDiskId,
                    Path,
                    ReadCentric,
                    SharedWithOS,
                    Status,
                    StatusChangeTimestamp,
                    TotalSize,
                    Type,
                    ExpectedSlotCount,
                    NumActiveSlots,
                    DecommitStatus
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

        TYsonFieldChecker check(ysonString, 16);

        check.Uint64(0u); // AvailableSize
        check.Uint64(999u); // BoxId
        check.Uint64(123u); // Guid
        check.Uint64(0u); // Kind
        check.Uint64(env.GetServer().GetRuntime()->GetNodeId(0)); // NodeId
        check.Uint64(1u); // PDiskId
        check.StringContains("pdisk_1.dat"); // Path
        check.Bool(false); // ReadCentric
        check.Bool(false); // SharedWithOS
        check.String("ACTIVE"); // Status
        check.Null(); // StatusChangeTimestamp
        check.Uint64(0u); // TotalSize
        check.String("ROT"); // Type
        check.Uint64(16); // ExpectedSlotCount
        check.Uint64(2); // NumActiveSlots
        check.String("DECOMMIT_NONE"); // DecommitStatus
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
                    FailDomain,
                    FailRealm,
                    GroupGeneration,
                    GroupId,
                    Kind,
                    NodeId,
                    PDiskId,
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

        TYsonFieldChecker check(ysonString, 12);

        check.Uint64(0u, true); // AllocatedSize
        check.Uint64(0u, true); // AvailableSize
        check.Uint64(0u); // FailDomain
        check.Uint64(0u); // FailRealm
        check.Uint64(1u); // GroupGeneration
        check.Uint64(2181038080u); // GroupId
        check.String("Default"); // Kind
        check.Uint64(env.GetServer().GetRuntime()->GetNodeId(0)); // NodeId
        check.Uint64(1u); // PDiskId
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
                    LifeCyclePhase,
                    PutTabletLogLatency,
                    PutUserDataLatency,
                    StoragePoolId
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

        TYsonFieldChecker check(ysonString, 12);

        check.Uint64(0u); // AllocatedSize
        check.Uint64GreaterOrEquals(0u); // AvailableSize
        check.Uint64(999u); // BoxId
        check.Uint64(0u); // EncryptionMode
        check.String("none"); // ErasureSpecies
        check.Uint64(1u); // Generation
        check.Null(); // GetFastLatency
        check.Uint64(2181038080u); // GroupId
        check.Uint64(0u); // LifeCyclePhase
        check.Null(); // PutTabletLogLatency
        check.Null(); // PutUserDataLatency
        check.Uint64(2u); // StoragePoolId
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

        TYsonFieldChecker check(ysonString, 11);

        check.Uint64(999u); // BoxId
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
        TTestEnv env(1, 0, 3);

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

    size_t GetRowCount(TTableClient& client, const TString& name) {
        TStringBuilder query;
        query << "SELECT * FROM `" << name << "`";
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

    Y_UNIT_TEST(TopPartitionsFields) {
        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);

        auto nowUs = TInstant::Now().MicroSeconds();

        TTestEnv env(1, 4, 0, 0, true);
        CreateTenantsAndTables(env);

        TTableClient client(env.GetDriver());
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

    Y_UNIT_TEST(TopPartitionsTables) {
        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);

        constexpr ui64 partitionCount = 5;

        TTestEnv env(1, 4, 0, 0, true);
        CreateTenantsAndTables(env, true, partitionCount);

        TTableClient client(env.GetDriver());
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

    Y_UNIT_TEST(TopPartitionsRanges) {
        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);

        constexpr ui64 partitionCount = 5;

        TTestEnv env(1, 4, 0, 0, true);
        CreateTenantsAndTables(env, true, partitionCount);

        TTableClient client(env.GetDriver());
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

    Y_UNIT_TEST(Describe) {
        TTestEnv env;
        CreateRootTable(env);

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();
        {
            auto settings = TDescribeTableSettings()
                .WithKeyShardBoundary(true)
                .WithTableStatistics(true)
                .WithPartitionStatistics(true);

            auto result = session.DescribeTable("/Root/.sys/partition_stats", settings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            const auto& table = result.GetTableDescription();
            const auto& columns = table.GetTableColumns();
            const auto& keyColumns = table.GetPrimaryKeyColumns();

            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 26);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0].Name, "OwnerId");
            UNIT_ASSERT_STRINGS_EQUAL(FormatType(columns[0].Type), "Uint64?");

            UNIT_ASSERT_VALUES_EQUAL(keyColumns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(keyColumns[0], "OwnerId");

            UNIT_ASSERT_VALUES_EQUAL(table.GetPartitionStats().size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(table.GetPartitionsCount(), 0);
        }

        TSchemeClient schemeClient(env.GetDriver());

        {
            auto result = schemeClient.DescribePath("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "partition_stats");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Table);
        }
        {
            auto result = schemeClient.ListDirectory("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "partition_stats");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Table);
        }
    }

    Y_UNIT_TEST(SystemViewFailOps) {
        TTestEnv env;

        TTableClient client(env.GetDriver());
        auto session = client.CreateSession().GetValueSync().GetSession();

        {
            auto desc = TTableBuilder()
                .AddNullableColumn("Column1", EPrimitiveType::Uint64)
                .SetPrimaryKeyColumn("Column1")
                .Build();

            auto result = session.CreateTable("/Root/.sys/partition_stats", std::move(desc)).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = session.CopyTable("/Root/.sys/partition_stats", "/Root/Table0").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto settings = TAlterTableSettings()
                .AppendDropColumns("OwnerId");

            auto result = session.AlterTable("/Root/.sys/partition_stats", settings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = session.DropTable("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = session.ExecuteSchemeQuery(R"(
                DROP TABLE `/Root/.sys/partition_stats`;
            )").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = session.ReadTable("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            TReadTableResultPart streamPart = result.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SCHEME_ERROR);
            streamPart.GetIssues().PrintTo(Cerr);
        }
        {
            TValueBuilder rows;
            rows.BeginList().EndList();
            auto result = client.BulkUpsert("/Root/.sys/partition_stats", rows.Build()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }

        TSchemeClient schemeClient(env.GetDriver());

        {
            auto result = schemeClient.MakeDirectory("/Root/.sys").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.MakeDirectory("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.RemoveDirectory("/Root/.sys").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.RemoveDirectory("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            TModifyPermissionsSettings settings;
            auto result = schemeClient.ModifyPermissions("/Root/.sys/partition_stats", settings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
            result.GetIssues().PrintTo(Cerr);
        }
    }

    Y_UNIT_TEST(DescribeSystemFolder) {
        TTestEnv env;
        CreateTenantsAndTables(env, false);

        TSchemeClient schemeClient(env.GetDriver());
        {
            auto result = schemeClient.ListDirectory("/Root").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_STRINGS_EQUAL(entry.Name, "Root");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Directory);

            auto children = result.GetChildren();
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 4);
            UNIT_ASSERT_STRINGS_EQUAL(children[0].Name, "Table0");
            UNIT_ASSERT_STRINGS_EQUAL(children[1].Name, "Tenant1");
            UNIT_ASSERT_STRINGS_EQUAL(children[2].Name, "Tenant2");
            UNIT_ASSERT_STRINGS_EQUAL(children[3].Name, ".sys");
        }
        {
            auto result = schemeClient.ListDirectory("/Root/Tenant1").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "Tenant1");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::SubDomain);

            auto children = result.GetChildren();
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 2);
            UNIT_ASSERT_STRINGS_EQUAL(children[0].Name, "Table1");
            UNIT_ASSERT_STRINGS_EQUAL(children[1].Name, ".sys");
        }
        {
            auto result = schemeClient.ListDirectory("/Root/.sys").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, ".sys");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Directory);

            auto children = result.GetChildren();
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 21);

            THashSet<TString> names;
            for (const auto& child : children) {
                names.insert(child.Name);
                UNIT_ASSERT_VALUES_EQUAL(child.Type, ESchemeEntryType::Table);
            }
            UNIT_ASSERT(names.contains("partition_stats"));
        }
        {
            auto result = schemeClient.ListDirectory("/Root/Tenant1/.sys").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto entry = result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, ".sys");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, ESchemeEntryType::Directory);

            auto children = result.GetChildren();
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 15);

            THashSet<TString> names;
            for (const auto& child : children) {
                names.insert(child.Name);
                UNIT_ASSERT_VALUES_EQUAL(child.Type, ESchemeEntryType::Table);
            }
            UNIT_ASSERT(names.contains("partition_stats"));
        }
        {
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
            .SetAuthToken("user0@builtin");
        auto driver = TDriver(driverConfig);

        TSchemeClient schemeClient(driver);
        {
            auto result = schemeClient.ListDirectory("/Root").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.ListDirectory("/Root/Tenant1").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.ListDirectory("/Root/.sys").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.ListDirectory("/Root/Tenant1/.sys").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.DescribePath("/Root/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            result.GetIssues().PrintTo(Cerr);
        }
        {
            auto result = schemeClient.DescribePath("/Root/Tenant1/.sys/partition_stats").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            result.GetIssues().PrintTo(Cerr);
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
            WHERE TabletId <= 72075186224037888ul OR TabletId >= 72075186224037890ul;
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

        TString query("SELECT * FROM `Root/Table0`");
        execQuery(env, query);

        TTableClient client(env.GetDriver());
        auto it = client.StreamExecuteScanQuery(R"(
            SELECT QueryText, Type, ReadRows
            FROM `Root/.sys/top_queries_by_read_bytes_one_minute`
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

    // TODO: make a test when tenant support is provided
    void QueryMetricsSimple() {
        TTestEnv env(1, 2);
        CreateTenant(env, "Tenant1", true);
        {
            TTableClient client(env.GetDriver());
            auto session = client.CreateSession().GetValueSync().GetSession();

            NKqp::AssertSuccessResult(session.ExecuteSchemeQuery(R"(
                CREATE TABLE `Root/Tenant1/Table1` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )").GetValueSync());
        }

        auto driverConfig = TDriverConfig()
            .SetEndpoint(env.GetEndpoint())
            .SetDatabase("/Root/Tenant1");
        auto driver = TDriver(driverConfig);

        TTableClient client(driver);
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            "SELECT * FROM `Root/Tenant1/Table1`", TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        size_t rowCount = 0;
        TString ysonString;

        for (size_t iter = 0; iter < 30 && !rowCount; ++iter) {
            auto it = client.StreamExecuteScanQuery(R"(
                SELECT SumReadBytes FROM `Root/Tenant1/.sys/query_metrics`;
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

        NKqp::CompareYson(R"([
            [[0u]];
        ])", ysonString);
    }
}

} // NSysView
} // NKikimr
