#include "query.h"

namespace NYdb::NQuery {

const TVector<TResultSet>& TExecuteQueryResult::GetResultSets() const {
    return ResultSets_;
}

TResultSet TExecuteQueryResult::GetResultSet(size_t resultIndex) const {
    if (resultIndex >= ResultSets_.size()) {
        RaiseError(TString("Requested index out of range\n"));
    }

    return ResultSets_[resultIndex];
}

TScriptExecutionOperation::TScriptExecutionOperation(TStatus&& status, Ydb::Operations::Operation&& operation)
    : TOperation(std::move(status), std::move(operation))
{
    Ydb::Query::ExecuteScriptMetadata metadata;
    GetProto().metadata().UnpackTo(&metadata);

    Metadata_.ExecutionId = metadata.execution_id();
    Metadata_.ExecMode = static_cast<EExecMode>(metadata.exec_mode());
    Metadata_.ExecStatus = static_cast<EExecStatus>(metadata.exec_status());
    Metadata_.ExecStats = metadata.exec_stats();
    Metadata_.ResultSetsMeta.insert(Metadata_.ResultSetsMeta.end(), metadata.result_sets_meta().begin(), metadata.result_sets_meta().end());

    if (metadata.has_script_content()) {
        Metadata_.ScriptContent.Syntax = static_cast<ESyntax>(metadata.script_content().syntax());
        Metadata_.ScriptContent.Text = metadata.script_content().text();
    }
}

} // namespace NYdb::NQuery
