#include "kqp_runner.h"
#include "ydb_setup.h"

#include <library/cpp/colorizer/colors.h>

using namespace NKikimrRun;

namespace NKqpRun {

//// TKqpRunner::TImpl

class TKqpRunner::TImpl {
    using EVerbose = TYdbSetupSettings::EVerbose;

public:
    enum class EQueryType {
        ScriptQuery,
        YqlScriptQuery,
        AsyncQuery
    };

    explicit TImpl(const TRunnerOptions& options)
        : Options_(options)
        , VerboseLevel_(Options_.YdbSettings.VerboseLevel)
        , YdbSetup_(options.YdbSettings)
        , StatsPrinter_(Options_.PlanOutputFormat)
        , CerrColors_(NColorizer::AutoColors(Cerr))
        , CoutColors_(NColorizer::AutoColors(Cout))
    {}

    bool ExecuteSchemeQuery(const TRequestOptions& query) const {
        StartSchemeTraceOpt();

        if (VerboseLevel_ >= EVerbose::QueriesText) {
            Cout << CoutColors_.Cyan() << "Starting scheme request:\n" << CoutColors_.Default() << query.Query << Endl;
        }

        TSchemeMeta meta;
        TRequestResult status = YdbSetup_.SchemeQueryRequest(query, meta);
        TYdbSetup::StopTraceOpt();

        PrintSchemeQueryAst(meta.Ast);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to execute scheme query, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        return true;
    }

    bool ExecuteScript(const TRequestOptions& script) {
        StartScriptTraceOpt(script.QueryId);

        if (VerboseLevel_ >= EVerbose::QueriesText) {
            Cout << CoutColors_.Cyan() << "Starting script request:\n" << CoutColors_.Default() << script.Query << Endl;
        }

        TRequestResult status = YdbSetup_.ScriptRequest(script, ExecutionOperation_);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to start script execution, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        ExecutionMeta_ = TExecutionMeta();
        ExecutionMeta_.Database = script.Database;

        return WaitScriptExecutionOperation(script.QueryId);
    }

    bool ExecuteQuery(const TRequestOptions& query, EQueryType queryType) {
        StartScriptTraceOpt(query.QueryId);
        StartTime_ = TInstant::Now();

        if (VerboseLevel_ >= EVerbose::QueriesText) {
            Cout << CoutColors_.Cyan() << "Starting query request:\n" << CoutColors_.Default() << query.Query << Endl;
        }

        TString queryTypeStr;
        TQueryMeta meta;
        TRequestResult status;
        switch (queryType) {
        case EQueryType::ScriptQuery:
            queryTypeStr = "Generic";
            status = YdbSetup_.QueryRequest(query, meta, ResultSets_, GetProgressCallback());
            break;

        case EQueryType::YqlScriptQuery:
            queryTypeStr = "Yql script";
            status = YdbSetup_.YqlScriptRequest(query, meta, ResultSets_);
            break;

        case EQueryType::AsyncQuery:
            YdbSetup_.QueryRequestAsync(query);
            return true;
        }

        TYdbSetup::StopTraceOpt();

        if (!meta.Plan) {
            meta.Plan = ExecutionMeta_.Plan;
        }

        PrintScriptAst(query.QueryId, meta.Ast);
        PrintScriptProgress(query.QueryId, meta.Plan);
        PrintScriptPlan(query.QueryId, meta.Plan);
        PrintScriptFinish(meta, queryTypeStr);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to execute query, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        if (!status.Issues.Empty()) {
            Cerr << CerrColors_.Red() << "Request finished with issues:" << CerrColors_.Default() << Endl << status.Issues.ToString() << Endl;
        }

        return true;
    }

    void FinalizeRunner() const {
        YdbSetup_.WaitAsyncQueries();
        YdbSetup_.CloseSessions();
    }

    bool FetchScriptResults() {
        TYdbSetup::StopTraceOpt();

        ResultSets_.clear();
        ResultSets_.resize(ExecutionMeta_.ResultSetsCount);
        for (i32 resultSetId = 0; resultSetId < ExecutionMeta_.ResultSetsCount; ++resultSetId) {
            TRequestResult status = YdbSetup_.FetchScriptExecutionResultsRequest(ExecutionMeta_.Database, ExecutionOperation_, resultSetId, ResultSets_[resultSetId]);

            if (!status.IsSuccess()) {
                Cerr << CerrColors_.Red() << "Failed to fetch result set with id " << resultSetId << ", reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
                return false;
            }
        }

        return true;
    }

    bool ForgetExecutionOperation() {
        TYdbSetup::StopTraceOpt();

        TRequestResult status = YdbSetup_.ForgetScriptExecutionOperationRequest(ExecutionMeta_.Database, ExecutionOperation_);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to forget script execution operation, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        if (!status.Issues.Empty()) {
            Cerr << CerrColors_.Red() << "Forget operation finished with issues:" << CerrColors_.Default() << Endl << status.Issues.ToString() << Endl;
        }

        return true;
    }

    void PrintScriptResults() const {
        if (Options_.ResultOutput) {
            Cout << CoutColors_.Yellow() << TInstant::Now().ToIsoStringLocal() << " Writing script query results..." << CoutColors_.Default() << Endl;
            for (size_t i = 0; i < ResultSets_.size(); ++i) {
                if (ResultSets_.size() > 1 && VerboseLevel_ >= EVerbose::Info) {
                    *Options_.ResultOutput << CoutColors_.Cyan() << "Result set " << i + 1 << ":" << CoutColors_.Default() << Endl;
                }
                PrintResultSet(Options_.ResultOutputFormat, *Options_.ResultOutput, ResultSets_[i]);
            }
        }
    }

private:
    bool WaitScriptExecutionOperation(ui64 queryId) {
        StartTime_ = TInstant::Now();
        Y_DEFER {
            TYdbSetup::StopTraceOpt();
        };

        TDuration getOperationPeriod = TDuration::Seconds(1);
        if (auto progressStatsPeriodMs = Options_.YdbSettings.AppConfig.GetQueryServiceConfig().GetProgressStatsPeriodMs()) {
            getOperationPeriod = TDuration::MilliSeconds(progressStatsPeriodMs);
        }

        TRequestResult status;
        while (true) {
            status = YdbSetup_.GetScriptExecutionOperationRequest(ExecutionMeta_.Database, ExecutionOperation_, ExecutionMeta_);
            PrintScriptProgress(queryId, ExecutionMeta_.Plan);

            if (ExecutionMeta_.Ready) {
                break;
            }

            if (!status.IsSuccess()) {
                Cerr << CerrColors_.Red() << "Failed to get script execution operation, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
                return false;
            }

            if (Options_.ScriptCancelAfter && TInstant::Now() - StartTime_ > Options_.ScriptCancelAfter) {
                Cout << CoutColors_.Yellow() << TInstant::Now().ToIsoStringLocal() << " Cancelling script execution..." << CoutColors_.Default() << Endl;
                TRequestResult cancelStatus = YdbSetup_.CancelScriptExecutionOperationRequest(ExecutionMeta_.Database, ExecutionOperation_);
                if (!cancelStatus.IsSuccess()) {
                    Cerr << CerrColors_.Red() << "Failed to cancel script execution operation, reason:" << CerrColors_.Default() << Endl << cancelStatus.ToString() << Endl;
                    return false;
                }
            }

            Sleep(getOperationPeriod);
        }

        PrintScriptAst(queryId, ExecutionMeta_.Ast);
        PrintScriptProgress(queryId, ExecutionMeta_.Plan);
        PrintScriptPlan(queryId, ExecutionMeta_.Plan);
        PrintScriptFinish(ExecutionMeta_, "Script");

        if (!status.IsSuccess() || ExecutionMeta_.ExecutionStatus != NYdb::NQuery::EExecStatus::Completed) {
            Cerr << CerrColors_.Red() << "Failed to execute script, invalid final status, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        if (!status.Issues.Empty()) {
            Cerr << CerrColors_.Red() << "Request finished with issues:" << CerrColors_.Default() << Endl << status.Issues.ToString() << Endl;
        }

        return true;
    }

    void StartSchemeTraceOpt() const {
        if (Options_.TraceOptType == TRunnerOptions::ETraceOptType::All || Options_.TraceOptType == TRunnerOptions::ETraceOptType::Scheme) {
            YdbSetup_.StartTraceOpt();
        }
    }

    void StartScriptTraceOpt(size_t queryId) const {
        bool startTraceOpt = Options_.TraceOptType == TRunnerOptions::ETraceOptType::All;

        if (Options_.TraceOptType == TRunnerOptions::ETraceOptType::Script) {
            startTraceOpt |= !Options_.TraceOptScriptId || *Options_.TraceOptScriptId == queryId;
        }

        if (startTraceOpt) {
            YdbSetup_.StartTraceOpt();
        }
    }

    void PrintSchemeQueryAst(const TString& ast) const {
        if (Options_.SchemeQueryAstOutput) {
            if (VerboseLevel_ >= EVerbose::Info) {
                Cout << CoutColors_.Cyan() << "Writing scheme query ast" << CoutColors_.Default() << Endl;
            }
            Options_.SchemeQueryAstOutput->Write(ast);
        }
    }

    void PrintScriptAst(size_t queryId, const TString& ast) const {
        if (const auto output = GetValue<IOutputStream*>(queryId, Options_.ScriptQueryAstOutputs, nullptr)) {
            if (VerboseLevel_ >= EVerbose::Info) {
                Cout << CoutColors_.Cyan() << "Writing script query ast" << CoutColors_.Default() << Endl;
            }
            output->Write(ast);
        }
    }

    void PrintScriptPlan(size_t queryId, const TString& plan) const {
        if (const auto output = GetValue<IOutputStream*>(queryId, Options_.ScriptQueryPlanOutputs, nullptr)) {
            if (VerboseLevel_ >= EVerbose::Info) {
                Cout << CoutColors_.Cyan() << "Writing script query plan" << CoutColors_.Default() << Endl;
            }
            StatsPrinter_.PrintPlan(plan, *output);
        }
    }

    void PrintScriptProgress(size_t queryId, const TString& plan) const {
        if (const auto& output = GetValue<TString>(queryId, Options_.InProgressStatisticsOutputFiles, {})) {
            TFileOutput outputStream(output);
            StatsPrinter_.PrintInProgressStatistics(plan, outputStream);
            outputStream.Finish();
        }
        if (const auto& output = GetValue<TString>(queryId, Options_.ScriptQueryTimelineFiles, {})) {
            TFileOutput outputStream(output);
            StatsPrinter_.PrintTimeline(plan, outputStream);
            outputStream.Finish();
        }
    }

    TProgressCallback GetProgressCallback() {
        return [this](ui64 queryId, const NKikimrKqp::TEvExecuterProgress& executerProgress) mutable {
            const TString& plan = executerProgress.GetQueryPlan();
            ExecutionMeta_.Plan = plan;
            PrintScriptProgress(queryId, plan);
        };
    }

    void PrintScriptFinish(const TQueryMeta& meta, const TString& queryType) const {
        if (Options_.YdbSettings.VerboseLevel < EVerbose::Info) {
            return;
        }
        Cout << CoutColors_.Cyan() << queryType << " request finished.";
        if (meta.TotalDuration) {
            Cout << " Total duration: " << meta.TotalDuration;
        } else {
            Cout << " Estimated duration: " << TInstant::Now() - StartTime_;
        }
        Cout << CoutColors_.Default() << Endl;
    }

private:
    TRunnerOptions Options_;
    EVerbose VerboseLevel_;

    TYdbSetup YdbSetup_;
    TStatsPrinter StatsPrinter_;
    NColorizer::TColors CerrColors_;
    NColorizer::TColors CoutColors_;

    TString ExecutionOperation_;
    TExecutionMeta ExecutionMeta_;
    std::vector<Ydb::ResultSet> ResultSets_;
    TInstant StartTime_;
};


//// TKqpRunner

TKqpRunner::TKqpRunner(const TRunnerOptions& options)
    : Impl_(new TImpl(options))
{}

bool TKqpRunner::ExecuteSchemeQuery(const TRequestOptions& query) const {
    return Impl_->ExecuteSchemeQuery(query);
}

bool TKqpRunner::ExecuteScript(const TRequestOptions& script) const {
    return Impl_->ExecuteScript(script);
}

bool TKqpRunner::ExecuteQuery(const TRequestOptions& query) const {
    return Impl_->ExecuteQuery(query, TImpl::EQueryType::ScriptQuery);
}

bool TKqpRunner::ExecuteYqlScript(const TRequestOptions& query) const {
    return Impl_->ExecuteQuery(query, TImpl::EQueryType::YqlScriptQuery);
}

void TKqpRunner::ExecuteQueryAsync(const TRequestOptions& query) const {
    Impl_->ExecuteQuery(query, TImpl::EQueryType::AsyncQuery);
}

void TKqpRunner::FinalizeRunner() const {
    Impl_->FinalizeRunner();
}

bool TKqpRunner::FetchScriptResults() {
    return Impl_->FetchScriptResults();
}

bool TKqpRunner::ForgetExecutionOperation() {
    return Impl_->ForgetExecutionOperation();
}

void TKqpRunner::PrintScriptResults() const {
    Impl_->PrintScriptResults();
}

}  // namespace NKqpRun
