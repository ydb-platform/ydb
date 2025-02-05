#pragma once

#include <string>

namespace NYdb::inline V3 {

struct TConnectionInfo {
    std::string Endpoint = "";
    std::string Database = "";
    bool EnableSsl = false;
};

TConnectionInfo ParseConnectionString(const std::string& connectionString);

} // namespace NYdb

