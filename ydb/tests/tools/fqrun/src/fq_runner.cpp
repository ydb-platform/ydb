#include "fq_runner.h"
#include "fq_setup.h"

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/json/json_prettifier.h>

#include <regex>

using namespace NKikimrRun;

namespace NFqRun {

namespace {

TString CanonizeEndpoints(TString str, const NFq::NConfig::TGatewaysConfig& gatewaysConfig) {
    // Replace endpoints.
    for (const auto& pqCluster : gatewaysConfig.GetPq().GetClusterMapping()) {
        TStringBuilder clusterEndpointName;
        clusterEndpointName << "<pq_" << pqCluster.GetName() << "_endpoint>";
        SubstGlobal(str, pqCluster.GetEndpoint(), clusterEndpointName);
    }
    for (const auto& solomonCluster : gatewaysConfig.GetSolomon().GetClusterMapping()) {
        TStringBuilder clusterEndpointName;
        clusterEndpointName << "<solomon_" << solomonCluster.GetName() << "_endpoint>";
        SubstGlobal(str, solomonCluster.GetCluster(), clusterEndpointName);
    }
    return str;
}

TString CanonizeAstLogicalId(TString ast) {
    // '('('"_logical_id" '171840)))) ---------> '('('"_logical_id" '0))))
    std::regex re(R"foo(("_logical_id" ')(\d+))foo");
    return std::regex_replace(ast.c_str(), re, "$010").c_str();
}

}  // anonymous namespace

class TFqRunner::TImpl {
    using EVerbose = TFqSetupSettings::EVerbose;

public:
    explicit TImpl(const TRunnerOptions& options)
        : Options(options)
        , VerboseLevel(options.FqSettings.VerboseLevel)
        , FqSetup(options.FqSettings)
        , CerrColors(NColorizer::AutoColors(Cerr))
        , CoutColors(NColorizer::AutoColors(Cout))
    {}

    bool ExecuteQuery(const TRequestOptions& query) {
        StartTraceOpt(query.QueryId);

        if (VerboseLevel >= EVerbose::QueriesText) {
            Cout << CoutColors.Cyan() << "Starting " << FederatedQuery::QueryContent::QueryType_Name(query.Type) << " request:\n" << CoutColors.Default() << query.Query << Endl;
        }

        CurrentOptions = query.FqOptions;
        const TRequestResult status = FqSetup.QueryRequest(query, QueryId);

        if (!status.IsSuccess()) {
            Cerr << CerrColors.Red() << "Failed to start stream request execution, reason:" << CerrColors.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        return WaitQuery(query.QueryId);
    }

    bool FetchQueryResults() {
        ResultSets.clear();
        ResultSets.resize(ExecutionMeta.ResultSetSizes.size());
        for (i32 resultSetId = 0; resultSetId < static_cast<i32>(ExecutionMeta.ResultSetSizes.size()); ++resultSetId) {
            const auto rowsCount = ExecutionMeta.ResultSetSizes[resultSetId];
            if (rowsCount > MAX_RESULT_SET_ROWS) {
                Cerr << CerrColors.Red() << "Result set with id " << resultSetId << " have " << rowsCount << " rows, it is larger than allowed limit " << MAX_RESULT_SET_ROWS << ", results will be truncated" << CerrColors.Default() << Endl;
            }

            const TRequestResult status = FqSetup.FetchQueryResults(QueryId, resultSetId, CurrentOptions, ResultSets[resultSetId]);
            if (!status.IsSuccess()) {
                Cerr << CerrColors.Red() << "Failed to fetch result set with id " << resultSetId << ", reason:" << CerrColors.Default() << Endl << status.ToString() << Endl;
                return false;
            }
        }

        return true;
    }

    void PrintQueryResults() {
        if (Options.ResultOutput) {
            Cout << CoutColors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Writing query results..." << CoutColors.Default() << Endl;
            for (size_t i = 0; i < ResultSets.size(); ++i) {
                if (ResultSets.size() > 1 && VerboseLevel >= EVerbose::Info) {
                    *Options.ResultOutput << CoutColors.Cyan() << "Result set " << i + 1 << ":" << CoutColors.Default() << Endl;
                }
                PrintResultSet(Options.ResultOutputFormat, *Options.ResultOutput, ResultSets[i]);
            }
        }
    }

    bool CreateConnections(const std::vector<FederatedQuery::ConnectionContent>& connections, const TFqOptions& options) {
        for (const auto& connection : connections) {
            if (VerboseLevel >= EVerbose::QueriesText) {
                Cout << CoutColors.Cyan() << "Creating connection:\n" << CoutColors.Default() << Endl << connection.DebugString() << Endl;
            }

            TString connectionId;
            const TRequestResult status = FqSetup.CreateConnection(connection, options, connectionId);

            if (!status.IsSuccess()) {
                Cerr << CerrColors.Red() << "Failed to create connection '" << connection.name() << "', reason:" << CerrColors.Default() << Endl << status.ToString() << Endl;
                return false;
            }

            if (!ConnectionNameToId.emplace(std::make_pair(options.Scope, connection.name()), connectionId).second) {
                Cerr << CerrColors.Red() << "Got duplicated connection name '" << connection.name() << "' in scope " << options.Scope << CerrColors.Default() << Endl;
                return false;
            }
        }

        return true;
    }

    bool CreateBindings(const std::vector<FederatedQuery::BindingContent>& bindings, const TFqOptions& options) const {
        for (auto binding : bindings) {
            if (VerboseLevel >= EVerbose::QueriesText) {
                Cout << CoutColors.Cyan() << "Creating binding:\n" << CoutColors.Default() << Endl << binding.DebugString() << Endl;
            }

            const auto it = ConnectionNameToId.find(std::make_pair(options.Scope, binding.connection_id()));
            if (it == ConnectionNameToId.end()) {
                Cerr << CerrColors.Red() << "Failed to create binding '" << binding.name() << "', connection with name '" << binding.connection_id() << "' not found in scope " << options.Scope << CerrColors.Default() << Endl;
                return false;
            }

            binding.set_connection_id(it->second);
            const TRequestResult status = FqSetup.CreateBinding(binding, options);

            if (!status.IsSuccess()) {
                Cerr << CerrColors.Red() << "Failed to create binding '" << binding.name() << "', reason:" << CerrColors.Default() << Endl << status.ToString() << Endl;
                return false;
            }
        }

        return true;
    }

    void ExecuteQueryAsync(const TRequestOptions& query) const {
        StartTraceOpt(query.QueryId);

        if (VerboseLevel >= EVerbose::QueriesText) {
            Cout << CoutColors.Cyan() << "Starting async " << FederatedQuery::QueryContent::QueryType_Name(query.Type) << " request:\n" << CoutColors.Default() << query.Query << Endl;
        }

        FqSetup.QueryRequestAsync(query, Options.PingPeriod);
    }

    void FinalizeRunner() const {
        FqSetup.WaitAsyncQueries();
    }

private:
    bool WaitQuery(size_t queryId) {
        StartTime = TInstant::Now();
        Y_DEFER {
            TFqSetup::StopTraceOpt();
        };

        auto printStats = [this, queryId, astPrinter = GetAstPrinter(queryId), planPrinter = GetPlanPrinter(queryId)](const TExecutionMeta& meta, bool allowEmpty = false) mutable {
            if (astPrinter) {
                astPrinter->Print(meta.Ast, allowEmpty);
            }
            if (planPrinter) {
                planPrinter->Print(meta.Plan, allowEmpty);
            }
            PrintStatistics(queryId, meta.Statistics);
        };

        TString previousIssues;
        while (true) {
            TExecutionMeta meta;
            const TRequestResult status = FqSetup.DescribeQuery(QueryId, CurrentOptions, meta);

            if (const auto newIssues = meta.TransientIssues.ToString(); newIssues && previousIssues != newIssues && VerboseLevel >= EVerbose::Info) {
                previousIssues = newIssues;
                Cerr << CerrColors.Red() << "Query transient issues updated:" << CerrColors.Default() << Endl << newIssues << Endl;
            }
            ExecutionMeta = meta;
            printStats(ExecutionMeta);

            if (IsFinalStatus(ExecutionMeta.Status)) {
                break;
            }

            if (!status.IsSuccess()) {
                Cerr << CerrColors.Red() << "Failed to describe query, reason:" << CerrColors.Default() << Endl << status.ToString() << Endl;
                return false;
            }

            Sleep(Options.PingPeriod);
        }

        printStats(ExecutionMeta, true);
        if (VerboseLevel >= EVerbose::Info) {
            Cout << CoutColors.Cyan() << "Query finished. Duration: " << TInstant::Now() - StartTime << CoutColors.Default() << Endl;
        }

        if (ExecutionMeta.Status != FederatedQuery::QueryMeta::COMPLETED) {
            Cerr << CerrColors.Red() << "Failed to execute query, invalid final status " << FederatedQuery::QueryMeta::ComputeStatus_Name(ExecutionMeta.Status) << ", issues:" << CerrColors.Default() << Endl << ExecutionMeta.Issues.ToString() << Endl;
            return false;
        }

        if (ExecutionMeta.Issues) {
            Cerr << CerrColors.Red() << "Query finished with issues:" << CerrColors.Default() << Endl << ExecutionMeta.Issues.ToString() << Endl;
        }

        return true;
    }

    void StartTraceOpt(size_t queryId) const {
        if (Options.TraceOptAll || Options.TraceOptIds.contains(queryId)) {
            FqSetup.StartTraceOpt();
        }
    }

    std::optional<TCachedPrinter> GetAstPrinter(size_t queryId) const {
        const auto& astOutput = GetValue<TString>(queryId, Options.AstOutputs, {});
        if (!astOutput) {
            return std::nullopt;
        }

        return TCachedPrinter(astOutput, [this](TString ast, IOutputStream& output) {
            if (VerboseLevel >= EVerbose::Info) {
                Cout << CoutColors.Cyan() << "Writing query ast" << CoutColors.Default() << Endl;
            }
            if (Options.CanonicalOutput) {
                ast = CanonizeEndpoints(ast, Options.FqSettings.AppConfig.GetFederatedQueryConfig().GetGateways());
                ast = CanonizeAstLogicalId(ast);
            }
            output.Write(ast);
        });
    }

    std::optional<TCachedPrinter> GetPlanPrinter(size_t queryId) const {
        const auto& planOutput = GetValue<TString>(queryId, Options.PlanOutputs, {});
        if (!planOutput) {
            return std::nullopt;
        }

        return TCachedPrinter(planOutput, [this](TString plan, IOutputStream& output) {
            if (VerboseLevel >= EVerbose::Info) {
                Cout << CoutColors.Cyan() << "Writing query plan" << CoutColors.Default() << Endl;
            }
            if (!plan) {
                return;
            }

            plan = NJson::PrettifyJson(plan, false);
            if (Options.CanonicalOutput) {
                plan = CanonizeEndpoints(plan, Options.FqSettings.AppConfig.GetFederatedQueryConfig().GetGateways());
            }

            output.Write(plan);
            if (!Options.CanonicalOutput) {
                output.Write('\n');
            }
        });
    }

    void PrintStatistics(size_t queryId, const TString& statistics) const {
        if (!statistics) {
            return;
        }

        if (const auto& statsFile = GetValue<TString>(queryId, Options.StatsOutputs, {})) {
            TFileOutput output(statsFile);
            output.Write(NJson::PrettifyJson(statistics, false));
            output.Finish();
        }
    }

private:
    const TRunnerOptions Options;
    const EVerbose VerboseLevel;
    const TFqSetup FqSetup;
    const NColorizer::TColors CerrColors;
    const NColorizer::TColors CoutColors;

    TString QueryId;
    TInstant StartTime;
    TFqOptions CurrentOptions;
    TExecutionMeta ExecutionMeta;
    std::vector<Ydb::ResultSet> ResultSets;
    THashMap<std::pair<TString, TString>, TString> ConnectionNameToId;
};

TFqRunner::TFqRunner(const TRunnerOptions& options)
    : Impl(new TImpl(options))
{}

bool TFqRunner::ExecuteQuery(const TRequestOptions& query) const {
    return Impl->ExecuteQuery(query);
}

bool TFqRunner::FetchQueryResults() const {
    return Impl->FetchQueryResults();
}

void TFqRunner::PrintQueryResults() const {
    Impl->PrintQueryResults();
}

bool TFqRunner::CreateConnections(const std::vector<FederatedQuery::ConnectionContent>& connections, const TFqOptions& options) const {
    return Impl->CreateConnections(connections, options);
}

bool TFqRunner::CreateBindings(const std::vector<FederatedQuery::BindingContent>& bindings, const TFqOptions& options) const {
    return Impl->CreateBindings(bindings, options);
}

void TFqRunner::ExecuteQueryAsync(const TRequestOptions& query) const {
    Impl->ExecuteQueryAsync(query);
}

void TFqRunner::FinalizeRunner() const {
    Impl->FinalizeRunner();
}

}  // namespace NFqRun
