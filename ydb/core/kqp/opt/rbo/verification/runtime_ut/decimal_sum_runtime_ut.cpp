#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/sharding/hash_sharding.h>

#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>

#include <algorithm>
#include <limits>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;

using TQuerySession = NYdb::NQuery::TSession;
using TTableSession = NYdb::NTable::TSession;

const TString MaxDecimal = "99999999999999999999999999999999999";
const TString NegativeMaxDecimal = "-99999999999999999999999999999999999";

struct TRow {
    ui64 Id;
    TString Amount;
};

// With two consistency-hash partitions, Ids 1 and 5 route together and Id 4
// routes separately. The test verifies that assumption before executing SQL.
const TVector<TRow> WitnessRows = {
    {1, MaxDecimal},
    {4, NegativeMaxDecimal},
    {5, MaxDecimal},
};

enum class EOptimizer {
    NewRbo,
    Legacy,
};

struct TObservation {
    TString OnePartitionSum;
    TString TwoPartitionSum;
    ui32 DqPhyHashCombineNodeCount = 0;
};

ui32 ShardForId(ui64 id, ui32 shardCount) {
    const ui64 hash = NSharding::THashShardingImpl::CalcHash(id);
    const ui64 bucketWidth = std::numeric_limits<ui64>::max() / shardCount;
    return std::min<ui64>(hash / bucketWidth, shardCount - 1);
}

void CreateTable(TTableSession& session, const TString& path, ui32 partitionCount) {
    const auto result = session.ExecuteSchemeQuery(Sprintf(R"(
        CREATE TABLE `%s` (
            Id Uint64 NOT NULL,
            Amount Decimal(35, 0) NOT NULL,
            PRIMARY KEY (Id)
        )
        PARTITION BY HASH(Id)
        WITH (
            STORE = COLUMN,
            PARTITION_COUNT = %u,
            PARTITION_BY_HASH_FUNCTION = "consistency_hash_64"
        );
    )", path.c_str(), partitionCount)).GetValueSync();

    UNIT_ASSERT_C(
        result.IsSuccess(),
        "HARNESS_ASSUMPTION_FAILED: cannot create " << path << ": "
            << result.GetIssues().ToString());
}

TValue InputRows(const TVector<TRow>& input) {
    TValueBuilder rows;
    rows.BeginList();
    for (const auto& row : input) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("Id").Uint64(row.Id)
            .AddMember("Amount").Decimal(TDecimalValue(row.Amount, 35, 0))
            .EndStruct();
    }
    rows.EndList();
    return rows.Build();
}

void InsertRows(
    NYdb::NTable::TTableClient& client,
    const TString& path,
    const TVector<TRow>& rows)
{
    const auto result = client.BulkUpsert(path, InputRows(rows)).GetValueSync();
    UNIT_ASSERT_C(
        result.IsSuccess(),
        "HARNESS_ASSUMPTION_FAILED: cannot populate " << path << ": "
            << result.GetIssues().ToString());
}

void AssertPartitionCount(TTableSession& session, const TString& path, ui32 expected) {
    const auto result = session.DescribeTable(
        path,
        NYdb::NTable::TDescribeTableSettings().WithTableStatistics(true)).GetValueSync();
    UNIT_ASSERT_C(
        result.IsSuccess(),
        "HARNESS_ASSUMPTION_FAILED: cannot describe " << path << ": "
            << result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL_C(
        result.GetTableDescription().GetPartitionsCount(),
        expected,
        "HARNESS_ASSUMPTION_FAILED: unexpected physical partition count for " << path);
}

NYdb::NQuery::TExecuteQueryResult ExecuteSelect(TQuerySession& session, const TString& sql) {
    auto result = session.ExecuteQuery(
        sql,
        NYdb::NQuery::TTxControl::NoTx(),
        NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
    UNIT_ASSERT_C(
        result.IsSuccess(),
        "HARNESS_ASSUMPTION_FAILED: query failed: "
            << result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL_C(
        result.GetResultSets().size(),
        1,
        "HARNESS_ASSUMPTION_FAILED: query returned an unexpected number of result sets");
    return result;
}

TVector<TRow> ReadRows(TQuerySession& session, const TString& path) {
    auto result = ExecuteSelect(session, Sprintf(R"(
        PRAGMA YqlSelect = 'force';
        SELECT Id, Amount
        FROM `%s`
        ORDER BY Id;
    )", path.c_str()));

    TResultSetParser parser(result.GetResultSet(0));
    TVector<TRow> rows;
    while (parser.TryNextRow()) {
        rows.push_back({
            parser.ColumnParser("Id").GetUint64(),
            TString(parser.ColumnParser("Amount").GetDecimal().ToString()),
        });
    }
    return rows;
}

void AssertInputRows(
    TQuerySession& session,
    const TString& path,
    const TVector<TRow>& expected)
{
    const auto actual = ReadRows(session, path);

    UNIT_ASSERT_VALUES_EQUAL_C(
        actual.size(),
        expected.size(),
        "HARNESS_ASSUMPTION_FAILED: unexpected row count in " << path);
    for (size_t i = 0; i < expected.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            actual[i].Id,
            expected[i].Id,
            "HARNESS_ASSUMPTION_FAILED: unexpected key in " << path);
        UNIT_ASSERT_VALUES_EQUAL_C(
            actual[i].Amount,
            expected[i].Amount,
            "HARNESS_ASSUMPTION_FAILED: unexpected Decimal value in " << path);
    }
}

TString SumQuery(const TString& path) {
    return Sprintf(R"(
        PRAGMA YqlSelect = 'force';
        SELECT SUM(Amount) AS Total
        FROM `%s`;
    )", path.c_str());
}

TString ReadSum(TQuerySession& session, const TString& path) {
    auto result = ExecuteSelect(session, SumQuery(path));

    TResultSetParser parser(result.GetResultSet(0));
    UNIT_ASSERT_C(
        parser.TryNextRow(),
        "HARNESS_ASSUMPTION_FAILED: SUM returned no row for " << path);
    const auto total = parser.ColumnParser("Total").GetOptionalDecimal();
    UNIT_ASSERT_C(
        total,
        "HARNESS_ASSUMPTION_FAILED: SUM returned NULL for populated table " << path);
    UNIT_ASSERT_C(
        !parser.TryNextRow(),
        "HARNESS_ASSUMPTION_FAILED: SUM returned more than one row for " << path);
    return TString(total->ToString());
}

ui32 ExplainDqPhyHashCombineNodeCount(TQuerySession& session, const TString& path) {
    auto result = session.ExecuteQuery(
        SumQuery(path),
        NYdb::NQuery::TTxControl::NoTx(),
        NYdb::NQuery::TExecuteQuerySettings().ExecMode(
            NYdb::NQuery::EExecMode::Explain)).ExtractValueSync();
    UNIT_ASSERT_C(
        result.IsSuccess(),
        "HARNESS_ASSUMPTION_FAILED: cannot explain SUM: "
            << result.GetIssues().ToString());
    UNIT_ASSERT_C(
        result.GetStats() && result.GetStats()->GetAst(),
        "HARNESS_ASSUMPTION_FAILED: SUM Explain returned no physical AST");

    const TString ast = *result.GetStats()->GetAst();
    ui32 count = 0;
    size_t position = 0;
    while ((position = ast.find("DqPhyHashCombine", position)) != TString::npos) {
        ++count;
        position += TStringBuf("DqPhyHashCombine").size();
    }
    return count;
}

TObservation Observe(EOptimizer optimizer) {
    NKikimrConfig::TAppConfig appConfig;
    auto* tableConfig = appConfig.MutableTableServiceConfig();
    tableConfig->SetEnableNewRBO(optimizer == EOptimizer::NewRbo);
    tableConfig->SetEnableFallbackToYqlOptimizer(false);
    tableConfig->SetAllowOlapDataQuery(true);
    tableConfig->SetDefaultLangVer(NYql::GetMaxLangVersion());
    tableConfig->SetBackportMode(
        NKikimrConfig::TTableServiceConfig_EBackportMode_All);

    TKikimrRunner kikimr(
        TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto tableClient = kikimr.GetTableClient();
    auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

    const TString mode = optimizer == EOptimizer::NewRbo ? "NewRbo" : "Legacy";
    const TString onePartition = "/Root/" + mode + "OnePartition";
    const TString twoPartitions = "/Root/" + mode + "TwoPartitions";

    CreateTable(tableSession, onePartition, 1);
    CreateTable(tableSession, twoPartitions, 2);
    InsertRows(tableClient, onePartition, WitnessRows);
    InsertRows(tableClient, twoPartitions, WitnessRows);

    AssertPartitionCount(tableSession, onePartition, 1);
    AssertPartitionCount(tableSession, twoPartitions, 2);

    auto queryClient = kikimr.GetQueryClient();
    auto querySession = queryClient.GetSession().GetValueSync().GetSession();
    AssertInputRows(querySession, onePartition, WitnessRows);
    AssertInputRows(querySession, twoPartitions, WitnessRows);

    return {
        .OnePartitionSum = ReadSum(querySession, onePartition),
        .TwoPartitionSum = ReadSum(querySession, twoPartitions),
        .DqPhyHashCombineNodeCount = ExplainDqPhyHashCombineNodeCount(
            querySession,
            twoPartitions),
    };
}

Y_UNIT_TEST_SUITE(DecimalSumRuntimeDiagnostic) {
    Y_UNIT_TEST(PartitionInvariantAcrossOptimizerModes) {
        const ui32 shardForPositiveOne = ShardForId(1, 2);
        const ui32 shardForNegative = ShardForId(4, 2);
        const ui32 shardForPositiveTwo = ShardForId(5, 2);
        UNIT_ASSERT_VALUES_EQUAL_C(
            shardForPositiveOne,
            shardForPositiveTwo,
            "HARNESS_ASSUMPTION_FAILED: Ids 1 and 5 no longer share a hash partition");
        UNIT_ASSERT_C(
            shardForPositiveOne != shardForNegative,
            "HARNESS_ASSUMPTION_FAILED: Id 4 no longer routes apart from Ids 1 and 5");

        const auto newRbo = Observe(EOptimizer::NewRbo);
        const auto legacy = Observe(EOptimizer::Legacy);

        UNIT_ASSERT_C(
            newRbo.OnePartitionSum == newRbo.TwoPartitionSum &&
                legacy.OnePartitionSum == legacy.TwoPartitionSum,
            "CONFIRMED_MISMATCH: identical Decimal(35,0) rows produce different "
                << "SUMs after only the physical partition count changes, after "
                << "all partition, data, and routing checks passed; "
                << "new RBO {one=" << newRbo.OnePartitionSum
                << ", two=" << newRbo.TwoPartitionSum
                << ", DqPhyHashCombine=" << newRbo.DqPhyHashCombineNodeCount << "}; "
                << "legacy {one=" << legacy.OnePartitionSum
                << ", two=" << legacy.TwoPartitionSum
                << ", DqPhyHashCombine=" << legacy.DqPhyHashCombineNodeCount << "}");
    }
}

} // namespace
} // namespace NKikimr::NKqp
