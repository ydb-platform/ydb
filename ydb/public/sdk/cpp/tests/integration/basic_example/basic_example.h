#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <vector>

using namespace NYdb::NTable;

struct TRunArgs {
    NYdb::TDriver driver;
    std::string path;
};

NYdb::TParams GetTablesDataParams();

void CreateTables(TTableClient client, const std::string& path);
TRunArgs GetRunArgs();
NYdb::TStatus FillTableDataTransaction(TSession session, const std::string& path);
NYdb::TResultSet SelectSimple(TTableClient client, const std::string& path);
void UpsertSimple(TTableClient client, const std::string& path);
NYdb::TResultSet SelectWithParams(TTableClient client, const std::string& path);
NYdb::TResultSet PreparedSelect(TTableClient client, const std::string& path, ui32 seriesId, ui32 seasonId, ui32 episodeId);
NYdb::TResultSet MultiStep(TTableClient client, const std::string& path);
void ExplicitTcl(TTableClient client, const std::string& path);
NYdb::TResultSet PreparedSelect(TTableClient client, const std::string& path, ui32 seriesId, ui32 seasonId, ui32 episodeId);
std::vector<NYdb::TResultSet> ScanQuerySelect(TTableClient client, const std::string& path);
