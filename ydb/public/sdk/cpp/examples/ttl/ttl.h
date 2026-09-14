#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

NYdb::TParams GetTablesDataParams();

bool Run(const NYdb::TDriver& driver, const std::string& path);
