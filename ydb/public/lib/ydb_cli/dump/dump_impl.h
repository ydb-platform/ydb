#pragma once

#include "dump.h"

namespace NYdb::NDump {

class TDumpClient {
public:
    explicit TDumpClient(const TDriver& driver, const std::shared_ptr<TLog>& log);

    TDumpResult Dump(const TString& dbPath, const TString& fsPath, const TDumpSettings& settings = {});

    TDumpResult DumpCluster(const TString& fsPath);

    TDumpResult DumpDatabase(const TString& database, const TString& fsPath);

private:
    const TDriver& Driver;
    std::shared_ptr<TLog> Log;

}; // TDumpClient

} // NYdb::NDump
