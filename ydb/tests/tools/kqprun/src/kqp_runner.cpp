#include "kqp_runner.h"
#include "ydb_setup.h"

#include <library/cpp/colorizer/colors.h>

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/format.h>


namespace NKqpRun {

//// TKqpRunner::TImpl

class TKqpRunner::TImpl {
public:
    explicit TImpl(const TRunnerOptions& options)
        : Options_(options)
        , YdbSetup_(options.YdbSettings)
        , CerrColors_(NColorizer::AutoColors(Cerr))
        , CoutColors_(NColorizer::AutoColors(Cout))
    {}

    bool ExecuteSchemeQuery(const TString& query) const {
        StartSchemeTraceOpt();

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

    bool ExecuteScript(const TString& script, NKikimrKqp::EQueryAction action, const TString& traceId) {
        StartScriptTraceOpt();

        TRequestResult status = YdbSetup_.ScriptRequest(script, action, traceId, ExecutionOperation_);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to start script execution, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        return WaitScriptExecutionOperation();
    }

    bool ExecuteQuery(const TString& query, NKikimrKqp::EQueryAction action, const TString& traceId) {
        StartScriptTraceOpt();

        TQueryMeta meta;
        TRequestResult status = YdbSetup_.QueryRequest(query, action, traceId, meta, ResultSets_);
        TYdbSetup::StopTraceOpt();

        PrintScriptAst(meta.Ast);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to execute query, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        PrintScriptPlan(meta.Plan);

        return true;
    }

    bool FetchScriptResults() {
        TYdbSetup::StopTraceOpt();

        ResultSets_.resize(ExecutionMeta_.ResultSetsCount);
        for (i32 resultSetId = 0; resultSetId < ExecutionMeta_.ResultSetsCount; ++resultSetId) {
            TRequestResult status = YdbSetup_.FetchScriptExecutionResultsRequest(ExecutionOperation_, resultSetId, ResultSets_[resultSetId]);

            if (!status.IsSuccess()) {
                Cerr << CerrColors_.Red() << "Failed to fetch result set with id " << resultSetId << ", reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
                return false;
            }
        }

        return true;
    }

    void PrintScriptResults() const {
        Cout << CoutColors_.Cyan() << "Writing script query results" << CoutColors_.Default() << Endl;
        for (size_t i = 0; i < ResultSets_.size(); ++i) {
            if (ResultSets_.size() > 1) {
                *Options_.ResultOutput << CoutColors_.Cyan() << "Result set " << i + 1 << ":" << CoutColors_.Default() << Endl;
            }
            PrintScriptResult(ResultSets_[i]);
        }
    }

private:
    bool WaitScriptExecutionOperation() {
        TRequestResult status;
        while (true) {
            status = YdbSetup_.GetScriptExecutionOperationRequest(ExecutionOperation_, ExecutionMeta_);

            if (ExecutionMeta_.Ready) {
                break;
            }

            if (!status.IsSuccess()) {
                Cerr << CerrColors_.Red() << "Failed to get script execution operation, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
                return false;
            }

            Sleep(TDuration::Seconds(1));
        }

        PrintScriptAst(ExecutionMeta_.Ast);

        if (!status.IsSuccess() || ExecutionMeta_.ExecutionStatus != NYdb::NQuery::EExecStatus::Completed) {
            Cerr << CerrColors_.Red() << "Failed to execute script, invalid final status, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        PrintScriptPlan(ExecutionMeta_.Plan);

        return true;
    }

    void StartSchemeTraceOpt() const {
        if (Options_.TraceOptType == TRunnerOptions::ETraceOptType::All || Options_.TraceOptType == TRunnerOptions::ETraceOptType::Scheme) {
            YdbSetup_.StartTraceOpt();
        }
    }

    void StartScriptTraceOpt() const {
        if (Options_.TraceOptType == TRunnerOptions::ETraceOptType::All || Options_.TraceOptType == TRunnerOptions::ETraceOptType::Script) {
            YdbSetup_.StartTraceOpt();
        }
    }

    void PrintSchemeQueryAst(const TString& ast) const {
        if (Options_.SchemeQueryAstOutput) {
            Cout << CoutColors_.Cyan() << "Writing scheme query ast" << CoutColors_.Default() << Endl;
            Options_.SchemeQueryAstOutput->Write(ast);
        }
    }

    void PrintScriptAst(const TString& ast) const {
        if (Options_.ScriptQueryAstOutput) {
            Cout << CoutColors_.Cyan() << "Writing script query ast" << CoutColors_.Default() << Endl;
            Options_.ScriptQueryAstOutput->Write(ast);
        }
    }

    void PrintScriptPlan(const TString& plan) const {
        if (Options_.ScriptQueryPlanOutput) {
            Cout << CoutColors_.Cyan() << "Writing script query plan" << CoutColors_.Default() << Endl;

            NYdb::NConsoleClient::TQueryPlanPrinter printer(Options_.PlanOutputFormat, true, *Options_.ScriptQueryPlanOutput);
            printer.Print(plan);
        }
    }

    void PrintScriptResult(const Ydb::ResultSet& resultSet) const {
        switch (Options_.ResultOutputFormat) {
        case TRunnerOptions::EResultOutputFormat::RowsJson: {
            NYdb::TResultSet result(resultSet);
            NYdb::TResultSetParser parser(result);
            while (parser.TryNextRow()) {
                NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, Options_.ResultOutput);
                writer.SetWriteNanAsString(true);
                NYdb::FormatResultRowJson(parser, result.GetColumnsMeta(), writer, NYdb::EBinaryStringEncoding::Unicode);
                *Options_.ResultOutput << Endl;
            }
            break;
        }

        case TRunnerOptions::EResultOutputFormat::FullJson:
            resultSet.PrintJSON(*Options_.ResultOutput);
            break;
        }
    }

private:
    TRunnerOptions Options_;

    TYdbSetup YdbSetup_;
    NColorizer::TColors CerrColors_;
    NColorizer::TColors CoutColors_;

    TString ExecutionOperation_;
    TExecutionMeta ExecutionMeta_;
    std::vector<Ydb::ResultSet> ResultSets_;
};


//// TKqpRunner

TKqpRunner::TKqpRunner(const TRunnerOptions& options)
    : Impl_(new TImpl(options))
{}

bool TKqpRunner::ExecuteSchemeQuery(const TString& query) const {
    return Impl_->ExecuteSchemeQuery(query);
}

bool TKqpRunner::ExecuteScript(const TString& script, NKikimrKqp::EQueryAction action, const TString& traceId) const {
    return Impl_->ExecuteScript(script, action, traceId);
}

bool TKqpRunner::ExecuteQuery(const TString& query, NKikimrKqp::EQueryAction action, const TString& traceId) const {
    return Impl_->ExecuteQuery(query, action, traceId);
}

bool TKqpRunner::FetchScriptResults() {
    return Impl_->FetchScriptResults();
}

void TKqpRunner::PrintScriptResults() const {
    Impl_->PrintScriptResults();
}

}  // namespace NKqpRun
