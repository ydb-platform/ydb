#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/arrow/accessor.h>

namespace NYdb::inline Dev {

    void TArrowAccessor::SetCollectedArrowResult(TResultSet& resultSet, TCollectedArrowResult&& resultArrow) {
        resultSet.SetCollectedArrowResult(std::move(resultArrow));
    }

TResultSet::EFormat TArrowAccessor::Format(const TResultSet& resultSet) {
    return resultSet.Format();
}

const TCollectedArrowResult& TArrowAccessor::GetCollectedArrowResult(const TResultSet& resultSet) {
    return resultSet.GetCollectedArrowResult();
}

const std::string& TArrowAccessor::GetArrowSchema(const TResultSet& resultSet) {
    return resultSet.GetArrowSchema();
}

const std::string& TArrowAccessor::GetArrowData(const TResultSet& resultSet) {
    return resultSet.GetArrowData();
}

} // namespace NYdb
