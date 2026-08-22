#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <string>

namespace NYdb::inline Dev {

struct TConnectionInfo {
    std::string Endpoint = "";
    std::string Database = "";
    bool EnableSsl = false;
};

TConnectionInfo ParseConnectionString(const std::string& connectionString);

} // namespace NYdb
