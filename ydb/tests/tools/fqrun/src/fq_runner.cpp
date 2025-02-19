#include "fq_runner.h"
#include "fq_setup.h"

#include <library/cpp/colorizer/colors.h>

using namespace NKikimrRun;

namespace NFqRun {

class TFqRunner::TImpl {
    using EVerbose = TFqSetupSettings::EVerbose;

    static constexpr TDuration REFRESH_PERIOD = TDuration::Seconds(1);

public:
    explicit TImpl(const TRunnerOptions& options)
        : Options(options)
        , VerboseLevel(options.FqSettings.VerboseLevel)
        , FqSetup(options.FqSettings)
        , CerrColors(NColorizer::AutoColors(Cerr))
        , CoutColors(NColorizer::AutoColors(Cout))
    {}

    bool ExecuteStreamQuery(const TRequestOptions& query) {
        if (VerboseLevel >= EVerbose::QueriesText) {
            Cout << CoutColors.Cyan() << "Starting stream request:\n" << CoutColors.Default() << query.Query << Endl;
        }

        const TRequestResult status = FqSetup.StreamRequest(query, StreamQueryId);

        if (!status.IsSuccess()) {
            Cerr << CerrColors.Red() << "Failed to start stream request execution, reason:" << CerrColors.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        return WaitStreamQuery();
    }

    bool FetchQueryResults() {
        ResultSets.clear();
        ResultSets.resize(ExecutionMeta.ResultSetSizes.size());
        for (i32 resultSetId = 0; resultSetId < static_cast<i32>(ExecutionMeta.ResultSetSizes.size()); ++resultSetId) {
            const auto rowsCount = ExecutionMeta.ResultSetSizes[resultSetId];
            if (rowsCount > MAX_RESULT_SET_ROWS) {
                Cerr << CerrColors.Red() << "Result set with id " << resultSetId << " have " << rowsCount << " rows, it is larger than allowed limit " << MAX_RESULT_SET_ROWS << ", results will be truncated" << CerrColors.Default() << Endl;
            }

            const TRequestResult status = FqSetup.FetchQueryResults(StreamQueryId, resultSetId, ResultSets[resultSetId]);
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

    bool CreateConnections(const std::vector<FederatedQuery::ConnectionContent>& connections) {
        for (const auto& connection : connections) {
            if (VerboseLevel >= EVerbose::QueriesText) {
                Cout << CoutColors.Cyan() << "Creating connection:\n" << CoutColors.Default() << Endl << connection.DebugString() << Endl;
            }

            TString connectionId;
            const TRequestResult status = FqSetup.CreateConnection(connection, connectionId);

            if (!status.IsSuccess()) {
                Cerr << CerrColors.Red() << "Failed to create connection '" << connection.name() << "', reason:" << CerrColors.Default() << Endl << status.ToString() << Endl;
                return false;
            }

            if (!ConnectionNameToId.emplace(connection.name(), connectionId).second) {
                Cerr << CerrColors.Red() << "Got duplicated connection name '" << connection.name() << "'" << CerrColors.Default() << Endl;
                return false;
            }
        }

        return true;
    }

    bool CreateBindings(const std::vector<FederatedQuery::BindingContent>& bindings) const {
        for (auto binding : bindings) {
            if (VerboseLevel >= EVerbose::QueriesText) {
                Cout << CoutColors.Cyan() << "Creating binding:\n" << CoutColors.Default() << Endl << binding.DebugString() << Endl;
            }

            const auto it = ConnectionNameToId.find(binding.connection_id());
            if (it == ConnectionNameToId.end()) {
                Cerr << CerrColors.Red() << "Failed to create binding '" << binding.name() << "', connection with name '" << binding.connection_id() << "' not found" << CerrColors.Default() << Endl;
                return false;
            }

            binding.set_connection_id(it->second);
            const TRequestResult status = FqSetup.CreateBinding(binding);

            if (!status.IsSuccess()) {
                Cerr << CerrColors.Red() << "Failed to create binding '" << binding.name() << "', reason:" << CerrColors.Default() << Endl << status.ToString() << Endl;
                return false;
            }
        }

        return true;
    }

private:
    static bool IsFinalStatus(FederatedQuery::QueryMeta::ComputeStatus status) {
        using EStatus = FederatedQuery::QueryMeta;
        return IsIn({EStatus::FAILED, EStatus::COMPLETED, EStatus::ABORTED_BY_USER, EStatus::ABORTED_BY_SYSTEM}, status);
    }

    bool WaitStreamQuery() {
        StartTime = TInstant::Now();

        while (true) {
            TExecutionMeta meta;
            const TRequestResult status = FqSetup.DescribeQuery(StreamQueryId, meta);

            if (meta.TransientIssues.Size() != ExecutionMeta.TransientIssues.Size() && VerboseLevel >= EVerbose::Info) {
                Cerr << CerrColors.Red() << "Query transient issues updated:" << CerrColors.Default() << Endl << meta.TransientIssues.ToString() << Endl;
            }
            ExecutionMeta = meta;

            if (IsFinalStatus(ExecutionMeta.Status)) {
                break;
            }

            if (!status.IsSuccess()) {
                Cerr << CerrColors.Red() << "Failed to describe query, reason:" << CerrColors.Default() << Endl << status.ToString() << Endl;
                return false;
            }

            Sleep(REFRESH_PERIOD);
        }

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

private:
    const TRunnerOptions Options;
    const EVerbose VerboseLevel;
    const TFqSetup FqSetup;
    const NColorizer::TColors CerrColors;
    const NColorizer::TColors CoutColors;

    TString StreamQueryId;
    TInstant StartTime;
    TExecutionMeta ExecutionMeta;
    std::vector<Ydb::ResultSet> ResultSets;
    std::unordered_map<TString, TString> ConnectionNameToId;
};

TFqRunner::TFqRunner(const TRunnerOptions& options)
    : Impl(new TImpl(options))
{}

bool TFqRunner::ExecuteStreamQuery(const TRequestOptions& query) const {
    return Impl->ExecuteStreamQuery(query);
}

bool TFqRunner::FetchQueryResults() const {
    return Impl->FetchQueryResults();
}

void TFqRunner::PrintQueryResults() const {
    Impl->PrintQueryResults();
}

bool TFqRunner::CreateConnections(const std::vector<FederatedQuery::ConnectionContent>& connections) const {
    return Impl->CreateConnections(connections);
}

bool TFqRunner::CreateBindings(const std::vector<FederatedQuery::BindingContent>& bindings) const {
    return Impl->CreateBindings(bindings);
}

}  // namespace NFqRun
