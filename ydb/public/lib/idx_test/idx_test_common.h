#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace NYdb {
namespace NTable {
    class TTableClient;
    class TTableDescription;
}
}

namespace NIdxTest {

class TYdbErrorException : public yexception {
public:
    TYdbErrorException(const NYdb::TStatus& status)
        : Status(status) {}

    NYdb::TStatus Status;
};

inline void ThrowOnError(const NYdb::TStatus& status) {
    if (!status.IsSuccess()) {
        throw TYdbErrorException(status) << status;
    }
}

TMaybe<NYdb::NTable::TTableDescription> DescribeTable(const TString& tableName, NYdb::NTable::TTableClient client);

}
