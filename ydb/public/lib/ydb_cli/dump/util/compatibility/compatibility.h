#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

namespace Ydb::Table {
class DescribeSystemViewResult;
}

namespace NYdb::NDump {

TStatus CheckSysViewCompatibility(
    const Ydb::Table::DescribeSystemViewResult& dumpedProto,
    const Ydb::Table::DescribeSystemViewResult& actualProto);

} // namespace NYdb::NDump
