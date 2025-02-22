#include "parser.h"

#include <ydb-cpp-sdk/client/types/exceptions/exceptions.h>

namespace NYdb::inline V3 {

TConnectionInfo ParseConnectionString(const std::string& connectionString) {
    if (connectionString.length() == 0) {
        ythrow TContractViolation("Empty connection string");
    }

    const std::string databaseFlag = "/?database=";
    const std::string grpcProtocol = "grpc://";
    const std::string grpcsProtocol = "grpcs://";
    const std::string localhostDomain = "localhost:";

    TConnectionInfo connectionInfo;
    std::string endpoint;

    size_t pathIndex = connectionString.find(databaseFlag);
    if (pathIndex == std::string::npos){
        pathIndex = connectionString.length();
    }
    if (pathIndex != connectionString.length()) {
        connectionInfo.Database = connectionString.substr(pathIndex + databaseFlag.length());
        endpoint = connectionString.substr(0, pathIndex);
    } else {
        endpoint = connectionString;
    }

    if (!std::string_view{endpoint}.starts_with(grpcProtocol) && !std::string_view{endpoint}.starts_with(grpcsProtocol) &&
        !std::string_view{endpoint}.starts_with(localhostDomain))
    {
        connectionInfo.Endpoint = endpoint;
        connectionInfo.EnableSsl = true;
    } else if (std::string_view{endpoint}.starts_with(grpcProtocol)) {
        connectionInfo.Endpoint = endpoint.substr(grpcProtocol.length());
        connectionInfo.EnableSsl = false;
    } else if (std::string_view{endpoint}.starts_with(grpcsProtocol)) {
        connectionInfo.Endpoint = endpoint.substr(grpcsProtocol.length());
        connectionInfo.EnableSsl = true;
    } else {
        connectionInfo.Endpoint = endpoint;
        connectionInfo.EnableSsl = false;
    }

    return connectionInfo;
}

} // namespace NYdb

