#include "kqp_analyzed_benchmarks.h"

#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/algorithm.h>
#include <util/stream/file.h>
#include <util/system/shellcommand.h>

#include <cmath>

namespace NKikimr::NKqp {
namespace {

// These snapshots were collected once from the scale-0.1 datasets. Validate
// their metadata before starting the server: renaming a file must not allow it
// to silently supply statistics for another scale, workload or storage type.
NJson::TJsonValue ReadBaseStatistics(const TString& workload, double scale, bool columnStore) {
    const TString storage = columnStore ? "column" : "row";
    const TString path = SRC_("../data/" + workload + "_sf0.1_" + storage + ".json");
    NJson::TJsonValue snapshot;
    NJson::ReadJsonTree(TIFStream(path).ReadAll(), &snapshot, true);
    const auto& metadata = snapshot.GetMapSafe();
    const double capturedScale = metadata.at("scale").GetDoubleSafe();
    Y_ENSURE(capturedScale == 0.1 && scale == capturedScale,
        "Captured benchmark statistics require scale 0.1; requested " << scale << ": " << path);
    Y_ENSURE(metadata.at("workload").GetStringSafe() == workload, "Statistics workload mismatch: " << path);
    Y_ENSURE(metadata.at("storage").GetStringSafe() == storage, "Statistics storage mismatch: " << path);
    const auto& tables = metadata.at("tables");
    for (const auto& [table, stats] : tables.GetMapSafe()) {
        // Deliberately exclude column statistics: those must come from ANALYZE.
        const auto& fields = stats.GetMapSafe();
        Y_ENSURE(fields.size() == 2 && fields.contains("n_rows") && fields.contains("byte_size"),
            "Expected only n_rows and byte_size for " << table);
        Y_ENSURE(fields.at("n_rows").GetDoubleSafe() >= 0 && fields.at("byte_size").GetDoubleSafe() >= 0,
            "Negative base statistics for " << table);
    }
    return tables;
}

void ConfigureBenchmark(Tests::TServerSettings& settings, const NJson::TJsonValue& baseStatistics) {
    auto* config = settings.AppConfig->MutableTableServiceConfig();
    config->SetEnableNewRBO(true);
    config->SetEnableFallbackToYqlOptimizer(false);
    config->SetEnableFallbackOnMultipleStatements(false);
    config->SetEnableKqpDataQueryStreamIdxLookupJoin(false);
    config->SetEnableOrderOptimizaionFSM(true);
    config->SetEnableQueryServiceSpilling(true);
    config->SetAllowOlapDataQuery(true);
    config->SetDefaultCostBasedOptimizationLevel(4);
    config->SetCompileTimeoutMs(TDuration::Minutes(1).MilliSeconds());
    config->SetDefaultLangVer(NYql::GetMaxLangVersion());
    config->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    settings.FeatureFlags.SetEnableSeparationComputeActorsFromRead(true);
    // Shared subqueries need multi-output stages with channel spilling.
    NKikimrKqp::TKqpSetting spilling;
    spilling.SetName("_KqpEnableSpilling");
    spilling.SetValue("true");
    settings.KqpSettings.push_back(spilling);
    NKikimrKqp::TKqpSetting statistics;
    statistics.SetName("OptOverrideStatistics");
    statistics.SetValue(NJson::WriteJson(baseStatistics));
    settings.KqpSettings.push_back(statistics);
}

void RunWorkloadCli(const TString& endpoint, const TString& database,
                    const TString& workload, const TVector<TString>& arguments) {
    TShellCommand command(BinaryPath("ydb/apps/ydb/ydb"),
        TShellCommandOptions().SetUseShell(false).SetCloseAllFdsOnExec(true));
    // The statistics fixture exposes a root endpoint, without tenant discovery endpoints.
    command << "--endpoint" << "grpc://" + endpoint << "--database" << database << "--no-discovery"
        << "workload" << workload << "--path" << workload;
    for (const auto& argument : arguments) {
        command << argument;
    }
    Cerr << "Running " << command.GetQuotedCommand() << Endl;
    command.Run().Wait();
    UNIT_ASSERT_C(command.GetExitCode() == 0,
        command.GetQuotedCommand() << "\nstdout:\n" << command.GetOutput() << "\nstderr:\n" << command.GetError());
}

NYdb::NQuery::TSession CreateSession(NYdb::NQuery::TQueryClient& client) {
    auto result = client.GetSession().GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result.GetSession();
}

} // namespace

TAnalyzedBenchmark::TAnalyzedBenchmark(const TString& workload, double scale, bool columnStore)
    : BaseStatistics(ReadBaseStatistics(workload, scale, columnStore))
    , Env(1, 1, true, [this](Tests::TServerSettings& settings) {
        ConfigureBenchmark(settings, BaseStatistics);
    })
    , Database(NStat::CreateDatabase(Env, "Database"))
    , TablesPath(Database + "/" + workload)
    , Driver(NYdb::TDriverConfig().SetEndpoint(Env.GetEndpoint()).SetDatabase(Database)
        .SetDiscoveryMode(NYdb::EDiscoveryMode::Off))
    , QueryClient(Driver)
    , Session(CreateSession(QueryClient))
{
    UNIT_ASSERT_C(std::isfinite(scale) && scale > 0, "Invalid benchmark scale: " << scale);
    UNIT_ASSERT(workload == "tpch" || workload == "tpcds");
    GetRuntime().SetLogPriority(NKikimrServices::STATISTICS, NActors::NLog::PRI_WARN);
    const TString scaleOption = "--scale=" + ToString(scale);
    RunWorkloadCli(Env.GetEndpoint(), Database, workload,
        {"init", columnStore ? "--store=column" : "--store=row", "--float-mode=decimal", scaleOption});
    RunWorkloadCli(Env.GetEndpoint(), Database, workload,
        {"import", "--upload-threads=1", "--max-in-flight=1", "generator", scaleOption});

    NYdb::NScheme::TSchemeClient schemeClient(Driver);
    const auto directory = schemeClient.ListDirectory(TablesPath).GetValueSync();
    UNIT_ASSERT_C(directory.IsSuccess(), directory.GetIssues().ToString());
    TVector<TString> tables;
    for (const auto& entry : directory.GetChildren()) {
        UNIT_ASSERT_C(entry.Type == NYdb::NScheme::ESchemeEntryType::Table
            || entry.Type == NYdb::NScheme::ESchemeEntryType::ColumnTable, entry.Name);
        tables.emplace_back(entry.Name);
    }
    UNIT_ASSERT_VALUES_EQUAL(tables.size(), workload == "tpch" ? 8 : 24);
    Sort(tables);

    UNIT_ASSERT_VALUES_EQUAL(BaseStatistics.GetMapSafe().size(), tables.size());

    NYdb::NTable::TTableClient tableClient(Driver);
    auto sessionResult = tableClient.CreateSession().GetValueSync();
    UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
    auto session = sessionResult.GetSession();
    for (const auto& table : tables) {
        const TString path = GetTablePath(table);
        Cerr << "Analyzing " << path << Endl;
        const auto analyzed = session.ExecuteSchemeQuery("ANALYZE `" + path + "`;").GetValueSync();
        UNIT_ASSERT_C(analyzed.IsSuccess(), path << ": " << analyzed.GetIssues().ToString());
        const auto pathId = NStat::ResolvePathId(GetRuntime(), path);
        const auto summary = NStat::GetStatistics(GetRuntime(), pathId,
            NStat::EStatType::TABLE_SUMMARY, {std::nullopt}, 0);
        UNIT_ASSERT_VALUES_EQUAL(summary.size(), 1);
        UNIT_ASSERT_C(summary[0].Success && summary[0].TableSummary.Data,
            "Missing analyzed table summary: " << path);
        RowCounts[table] = summary[0].TableSummary.Data->GetRowCount();
        // Reject a stale snapshot even when its scale metadata matches. Column
        // statistics remain those just computed from the generated dataset.
        UNIT_ASSERT_VALUES_EQUAL_C(BaseStatistics.GetMapSafe().at(path).GetMapSafe().at("n_rows").GetDoubleSafe(),
            GetRowCount(table), path);
    }
}

void TAnalyzedBenchmark::SetEnableNewRbo(bool enabled) {
    auto& runtime = GetRuntime();
    const auto edge = runtime.AllocateEdgeActor();
    auto config = Env.GetSettings()->AppConfig->GetTableServiceConfig();
    config.SetEnableNewRBO(enabled);
    // EnableNewRBO is marked InvalidateCompileCache in TableServiceConfig.
    // Wait for each compiler to apply the update before submitting a query,
    // so the second execution cannot reuse the other optimizer's cached plan.
    for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
        auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        *request->Record.MutableConfig()->MutableTableServiceConfig() = config;
        runtime.Send(MakeKqpCompileServiceID(runtime.GetNodeId(node)), edge, request.Release());
        UNIT_ASSERT_C(runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(
            edge, TDuration::Seconds(30)), "Compiler did not acknowledge EnableNewRBO update");
    }
}

std::unique_ptr<TAnalyzedBenchmark> LoadTpchAndAnalyze(double scale, bool columnStore) {
    return std::make_unique<TAnalyzedBenchmark>("tpch", scale, columnStore);
}

std::unique_ptr<TAnalyzedBenchmark> LoadTpcdsAndAnalyze(double scale, bool columnStore) {
    return std::make_unique<TAnalyzedBenchmark>("tpcds", scale, columnStore);
}

} // namespace NKikimr::NKqp
