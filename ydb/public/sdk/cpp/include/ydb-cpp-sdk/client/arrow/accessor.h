#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

namespace NYdb::inline Dev {

struct TResultArrow {
    std::string Schema;
    std::vector<std::string> Data;
};

//! Provides access to Arrow batches of result set. It is not recommended to use this
//! class in client applications as it is an experimental feature.
class TArrowAccessor {
public:
    static TResultSet::EFormat Format(const TResultSet& resultSet);
    static void SetResultArrow(TResultSet& resultSet, TResultArrow&& resultArrow);
    static const TResultArrow& GetResultArrow(const TResultSet& resultSet);
};

} // namespace NYdb
