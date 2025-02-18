#include <ydb-cpp-sdk/client/result/result.h>

#include <ydb-cpp-sdk/client/proto/accessor.h>

namespace NYdb::inline V3 {

const NYdbProtos::ResultSet& TProtoAccessor::GetProto(const TResultSet& resultSet) {
    return resultSet.GetProto();
}

} // namespace NYdb
