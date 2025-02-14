#include "fq_runner.h"
#include "fq_setup.h"

#include <library/cpp/colorizer/colors.h>

using namespace NKikimrRun;

namespace NFqRun {

class TFqRunner::TImpl {
    static constexpr TDuration REFRESH_PERIOD = TDuration::Seconds(1);

public:
    explicit TImpl(const TRunnerOptions& options)
        : Options(options)
        , FqSetup(options.FqSettings)
        , CerrColors(NColorizer::AutoColors(Cerr))
        , CoutColors(NColorizer::AutoColors(Cout))
    {}

    bool ExecuteStreamQuery(const TRequestOptions& query) {
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
                if (ResultSets.size() > 1) {
                    *Options.ResultOutput << CoutColors.Cyan() << "Result set " << i + 1 << ":" << CoutColors.Default() << Endl;
                }
                PrintResultSet(Options.ResultOutputFormat, *Options.ResultOutput, ResultSets[i]);
            }
        }
    }

private:
    static bool IsFinalStatus(FederatedQuery::QueryMeta::ComputeStatus status) {
        using EStatus = FederatedQuery::QueryMeta;
        return IsIn({EStatus::FAILED, EStatus::COMPLETED, EStatus::ABORTED_BY_USER, EStatus::ABORTED_BY_SYSTEM}, status);
    }

    bool WaitStreamQuery() {
        StartTime = TInstant::Now();

        while (true) {
            const TRequestResult status = FqSetup.DescribeQuery(StreamQueryId, ExecutionMeta);

            if (IsFinalStatus(ExecutionMeta.Status)) {
                break;
            }

            if (!status.IsSuccess()) {
                Cerr << CerrColors.Red() << "Failed to describe query, reason:" << CerrColors.Default() << Endl << status.ToString() << Endl;
                return false;
            }

            Sleep(REFRESH_PERIOD);
        }

        Cout << CoutColors.Cyan() << "Query finished. Duration: " << TInstant::Now() - StartTime << CoutColors.Default() << Endl;

        if (ExecutionMeta.Status != FederatedQuery::QueryMeta::COMPLETED) {
            Cerr << CerrColors.Red() << "Failed to execute query, invalid final status " << FederatedQuery::QueryMeta::ComputeStatus_Name(ExecutionMeta.Status) << ", issues:" << CerrColors.Default() << Endl << ExecutionMeta.Issues.ToString() << Endl;
            if (ExecutionMeta.TransientIssues) {
                Cerr << CerrColors.Red() << "Transient issues:" << CerrColors.Default() << Endl << ExecutionMeta.TransientIssues.ToString() << Endl;
            }
            return false;
        }

        if (ExecutionMeta.Issues) {
            Cerr << CerrColors.Red() << "Query finished with issues:" << CerrColors.Default() << Endl << ExecutionMeta.Issues.ToString() << Endl;
        }

        if (ExecutionMeta.TransientIssues) {
            Cerr << CerrColors.Red() << "Query finished with transient issues:" << CerrColors.Default() << Endl << ExecutionMeta.TransientIssues.ToString() << Endl;
        }

        return true;
    }

private:
    const TRunnerOptions Options;
    const TFqSetup FqSetup;
    const NColorizer::TColors CerrColors;
    const NColorizer::TColors CoutColors;

    TString StreamQueryId;
    TInstant StartTime;
    TExecutionMeta ExecutionMeta;
    std::vector<Ydb::ResultSet> ResultSets;
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

}  // namespace NFqRun
