#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYdb {
namespace NTable {
    class TTableClient;
    class TTableDescription;
}
}

namespace NIdxTest {

TMaybe<NYdb::NTable::TTableDescription> DescribeTable(const TString& tableName, NYdb::NTable::TTableClient client);

}
