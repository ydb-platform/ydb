#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/table/table.h>

NYdb::TParams GetTablesDataParams();

bool Run(const NYdb::TDriver& driver, const std::string& path);
