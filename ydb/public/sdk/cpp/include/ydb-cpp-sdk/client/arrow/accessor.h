#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

namespace NYdb::inline Dev {

struct TCollectedArrowResult {
    std::string Schema;
    std::vector<std::string> Data;
};

//! Provides access to Arrow batches of result set. It is not recommended to use this
//! class in client applications as it is an experimental feature.
class TArrowAccessor {
public:
    static TResultSet::EFormat Format(const TResultSet& resultSet);
    static void SetCollectedArrowResult(TResultSet& resultSet, TCollectedArrowResult&& resultArrow);
    static const TCollectedArrowResult& GetCollectedArrowResult(const TResultSet& resultSet);
    static const std::string& GetArrowSchema(const TResultSet& resultSet);
    static const std::string& GetArrowData(const TResultSet& resultSet);
};

} // namespace NYdb
