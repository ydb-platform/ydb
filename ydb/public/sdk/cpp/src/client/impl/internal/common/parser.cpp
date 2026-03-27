#define INCLUDE_YDB_INTERNAL_H
#include "parser.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#include <library/cpp/uri/uri.h>
#include <library/cpp/cgiparam/cgiparam.h>

#include <util/string/builder.h>


namespace NYdb::inline Dev {

namespace {
    void ThrowContractViolation(const std::string& connectionString, const std::string& message) {
        ythrow TContractViolation("Failed to parse connection string: \"" + connectionString + "\", error: " + message + "\n");
    }
}

TConnectionInfo ParseConnectionString(const std::string& connectionString) {
    if (connectionString.empty()) {
        ThrowContractViolation(connectionString, "empty connection string");
    }

    std::string connectionStringWithScheme = connectionString;

    if (connectionString.find("://") == std::string::npos) {
        if (connectionString.starts_with("localhost:")) {
            connectionStringWithScheme = "grpc://" + connectionString;
        } else {
            connectionStringWithScheme = "grpcs://" + connectionString;
        }
    }

    TConnectionInfo connectionInfo;

    NUri::TUri uri;
    NUri::TUri::TState::EParsed parseStatus = uri.Parse(
        connectionStringWithScheme, 
        NUri::TFeature::FeaturesDefault | NUri::TFeature::FeatureSchemeFlexible
    );

    if (parseStatus != NUri::TUri::TState::EParsed::ParsedOK) {
        ThrowContractViolation(connectionString, "failure during URI parsing with status: " + std::string(NUri::ParsedStateToString(parseStatus)));
    }

    std::string_view host = uri.GetHost();
    if (host.empty()) {
        ThrowContractViolation(connectionString, "connection string must contain a host");
    }

    // Validate and extract scheme
    std::string_view scheme = uri.GetField(NUri::TUri::FieldScheme);
    if (scheme == "grpc") {
        connectionInfo.EnableSsl = false;
    } else if (scheme == "grpcs") {
        connectionInfo.EnableSsl = true;
    } else {
        ThrowContractViolation(connectionString, "invalid scheme in connection string: only 'grpc' and 'grpcs' are allowed");
    }

    std::uint16_t port = uri.GetPort();
    if (port == 0) {
        connectionInfo.Endpoint = std::string(host);
    } else {
        connectionInfo.Endpoint = std::string(host) + ":" + std::to_string(port);
    }

    // Extract database from path or query parameter
    std::string_view path = uri.GetField(NUri::TUri::FieldPath);
    std::string_view query = uri.GetField(NUri::TUri::FieldQuery);

    bool hasQueryDatabase = false;
    if (!query.empty()) {
        TCgiParameters queryParams(query);
        if (queryParams.Has("database")) {
            connectionInfo.Database = queryParams.Get("database");
            hasQueryDatabase = true;
        }
    }

    if (!path.empty() && path != "/") {
        if (hasQueryDatabase) {
            ThrowContractViolation(connectionString, "database cannot be specified in both path and query parameter");
        }
        connectionInfo.Database = path;
    }

    return connectionInfo;
}

} // namespace NYdb
