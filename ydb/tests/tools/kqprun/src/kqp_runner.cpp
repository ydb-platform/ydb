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
        TSchemeMeta meta;
        TRequestResult status = YdbSetup_.SchemeQueryRequest(query, meta);

        PrintSchemeQueryAst(meta.Ast);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to execute scheme query, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        return true;
    }

    bool ExecuteScript(const TString& script, NKikimrKqp::EQueryAction action, const TString& traceId) {
        TRequestResult status = YdbSetup_.ScriptRequest(script, action, traceId, ExecutionOperation_);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to start script execution, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        return WaitScriptExecutionOperation();
    }

    bool ExecuteQuery(const TString& query, NKikimrKqp::EQueryAction action, const TString& traceId) const {
        TQueryMeta meta;
        TRequestResult status = YdbSetup_.QueryRequest(query, action, traceId, meta);

        PrintScriptAst(meta.Ast);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to execute query, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        PrintScriptPlan(meta.Plan);

        return true;
    }

    bool WriteScriptResults() const {
        for (i32 resultSetId = 0; resultSetId < ExecutionMeta_.ResultSetsCount; ++resultSetId) {
            Ydb::ResultSet resultSet;
            TRequestResult status = YdbSetup_.FetchScriptExecutionResultsRequest(ExecutionOperation_, resultSetId, Options_.ResultsRowsLimit, resultSet);

            if (!status.IsSuccess()) {
                Cerr << CerrColors_.Red() << "Failed to fetch result set with id " << resultSetId << ", reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
                return false;
            }

            PrintScriptResult(resultSet);
        }

        return true;
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
        if (Options_.ScriptQueryAstOutput) {
            Cout << CoutColors_.Cyan() << "Writing script query plan" << CoutColors_.Default() << Endl;

            NYdb::NConsoleClient::TQueryPlanPrinter printer(Options_.PlanOutputFormat, true, *Options_.ScriptQueryPlanOutput);
            printer.Print(plan);
        }
    }

    void PrintScriptResult(const Ydb::ResultSet& resultSet) const {
        switch (Options_.ResultOutputFormat) {
        case TRunnerOptions::EResultOutputFormat::RowsJson:
            Options_.ResultOutput->Write(NYdb::FormatResultSetJson(resultSet, NYdb::EBinaryStringEncoding::Unicode));
            break;

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

bool TKqpRunner::WriteScriptResults() const {
    return Impl_->WriteScriptResults();
}

}  // namespace NKqpRun
