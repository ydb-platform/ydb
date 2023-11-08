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

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to execute scheme query, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        if (Options_.SchemeQueryAstOutput) {
            Cout << CoutColors_.Cyan() << "Writing scheme query ast" << CoutColors_.Default() << Endl;
            Options_.SchemeQueryAstOutput->Write(meta.Ast);
        }

        return true;
    }

    bool ExecuteScript(const TString& script, NKikimrKqp::EQueryAction action) {
        TRequestResult status = YdbSetup_.ScriptQueryRequest(script, action, ExecutionOperation_);

        if (!status.IsSuccess()) {
            Cerr << CerrColors_.Red() << "Failed to start script execution, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        return WaitScriptExecutionOperation();
    }

    bool WriteScriptResults() const {
        for (i32 resultSetId = 0; resultSetId < ExecutionMeta_.ResultSetsCount; ++resultSetId) {
            Ydb::ResultSet resultSet;
            TRequestResult status = YdbSetup_.FetchScriptExecutionResultsRequest(ExecutionOperation_, resultSetId, Options_.ResultsRowsLimit, resultSet);

            if (!status.IsSuccess()) {
                Cerr << CerrColors_.Red() << "Failed to fetch result set with id " << resultSetId << ", reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
                return false;
            }

            Options_.ResultOutput->Write(NYdb::FormatResultSetJson(resultSet, NYdb::EBinaryStringEncoding::Unicode));
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

        if (Options_.ScriptQueryAstOutput) {
            Cout << CoutColors_.Cyan() << "Writing script query ast" << CoutColors_.Default() << Endl;
            Options_.ScriptQueryAstOutput->Write(ExecutionMeta_.Ast);
        }

        if (!status.IsSuccess() || ExecutionMeta_.ExecutionStatus != NYdb::NQuery::EExecStatus::Completed) {
            Cerr << CerrColors_.Red() << "Failed to execute script, invalid final status, reason:" << CerrColors_.Default() << Endl << status.ToString() << Endl;
            return false;
        }

        if (Options_.ScriptQueryPlanOutput) {
            Cout << CoutColors_.Cyan() << "Writing script query plan" << CoutColors_.Default() << Endl;

            NYdb::NConsoleClient::TQueryPlanPrinter printer(Options_.PlanOutputFormat, true, *Options_.ScriptQueryPlanOutput);
            printer.Print(ExecutionMeta_.Plan);
        }

        return true;
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

bool TKqpRunner::ExecuteScript(const TString& query, NKikimrKqp::EQueryAction action) const {
    return Impl_->ExecuteScript(query, action);
}

bool TKqpRunner::WriteScriptResults() const {
    return Impl_->WriteScriptResults();
}

}  // namespace NKqpRun
