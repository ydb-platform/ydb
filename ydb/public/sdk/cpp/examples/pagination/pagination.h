#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

NYdb::TParams GetTablesDataParams();

bool Run(const NYdb::TDriver& driver, const TString& path);
