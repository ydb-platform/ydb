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

} // namespace NYdb::NQuery
