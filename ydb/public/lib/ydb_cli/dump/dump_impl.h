#pragma once

#include "dump.h"

namespace NYdb {
namespace NDump {

class TDumpClient {
public:
    explicit TDumpClient(const TDriver& driver, const std::shared_ptr<TLog>& log);

    TDumpResult Dump(const TString& dbPath, const TString& fsPath, const TDumpSettings& settings = {});

private:
    const TDriver& Driver;
    std::shared_ptr<TLog> Log;

}; // TDumpClient

} // NDump
} // NYdb
