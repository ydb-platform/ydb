#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/arrow/accessor.h>

namespace NYdb::inline Dev {

TResultSet::EFormat TArrowAccessor::Format(const TResultSet& resultSet) {
    return resultSet.Format();
}

const std::string& TArrowAccessor::GetArrowSchema(const TResultSet& resultSet) {
    return resultSet.GetArrowSchema();
}

const std::vector<std::string>& TArrowAccessor::GetArrowBatches(const TResultSet& resultSet) {
    return resultSet.GetBytesData();
}

} // namespace NYdb
