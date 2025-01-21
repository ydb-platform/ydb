#include "result.h"

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NYdb::inline V2 {

const Ydb::ResultSet& TProtoAccessor::GetProto(const TResultSet& resultSet) {
    return resultSet.GetProto();
}

} // namespace NYdb
