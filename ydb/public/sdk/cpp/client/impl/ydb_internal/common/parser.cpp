#include "parser.h"

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/string_helpers.h>
#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>

namespace NYdb {

TConnectionInfo ParseConnectionString(const TString& connectionString) {
    if (connectionString.length() == 0) {
        ythrow TContractViolation("Empty connection string");
    }

    const TStringType databaseFlag = "/?database=";
    const TStringType grpcProtocol = "grpc://";
    const TStringType grpcsProtocol = "grpcs://";
    const TStringType localhostDomain = "localhost:";

    TConnectionInfo connectionInfo;
    TStringType endpoint;

    size_t pathIndex = connectionString.find(databaseFlag);
    if (pathIndex == TStringType::npos){
        pathIndex = connectionString.length();
    }
    if (pathIndex != connectionString.length()) {
        connectionInfo.Database = connectionString.substr(pathIndex + databaseFlag.length());
        endpoint = connectionString.substr(0, pathIndex);
    } else {
        endpoint = connectionString;
    }

    if (!StringStartsWith(endpoint, grpcProtocol) && !StringStartsWith(endpoint, grpcsProtocol) &&
        !StringStartsWith(endpoint, localhostDomain))
    {
        connectionInfo.Endpoint = endpoint;
        connectionInfo.EnableSsl = true;
    } else if (StringStartsWith(endpoint, grpcProtocol)) {
        connectionInfo.Endpoint = endpoint.substr(grpcProtocol.length());
        connectionInfo.EnableSsl = false;
    } else if (StringStartsWith(endpoint, grpcsProtocol)) {
        connectionInfo.Endpoint = endpoint.substr(grpcsProtocol.length());
        connectionInfo.EnableSsl = true;
    } else {
        connectionInfo.Endpoint = endpoint;
        connectionInfo.EnableSsl = false;
    }

    return connectionInfo;
}

} // namespace NYdb

