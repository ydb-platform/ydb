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

TExecuteScriptResult::TExecuteScriptResult(TStatus&& status, Ydb::Operations::Operation&& operation)
    : TOperation(std::move(status), std::move(operation))
{
    Ydb::Query::ExecuteScriptMetadata metadata;
    GetProto().metadata().UnpackTo(&metadata);

    Metadata_.ExecutionId = metadata.execution_id();
}

} // namespace NYdb::NQuery
