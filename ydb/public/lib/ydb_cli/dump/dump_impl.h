#pragma once

#include "dump.h"

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NYdb {
namespace NDump {

class TDumpClient {
public:
    explicit TDumpClient(NScheme::TSchemeClient& schemeClient, NTable::TTableClient& tableClient);

    TDumpResult Dump(const TString& dbPath, const TString& fsPath, const TDumpSettings& settings = {});

private:
    NScheme::TSchemeClient& SchemeClient;
    NTable::TTableClient& TableClient;

}; // TDumpClient

} // NDump
} // NYdb
