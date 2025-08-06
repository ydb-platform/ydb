#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/arrow/accessor.h>

namespace NYdb::inline Dev {

void TArrowAccessor::SetResultArrow(TResultSet& resultSet, TResultArrow&& resultArrow) {
    resultSet.SetArrowResult(std::move(resultArrow));
}

TResultSet::EFormat TArrowAccessor::Format(const TResultSet& resultSet) {
    return resultSet.Format();
}

const TResultArrow& TArrowAccessor::GetResultArrow(const TResultSet& resultSet) {
    return resultSet.GetArrowResult();
}

} // namespace NYdb
