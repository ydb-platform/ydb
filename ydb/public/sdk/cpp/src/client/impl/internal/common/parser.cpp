#include "parser.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>
#include <library/cpp/uri/uri.h>
#include <library/cpp/cgiparam/cgiparam.h>
#include <util/string/builder.h>

namespace NYdb::inline Dev {

TConnectionInfo ParseConnectionString(const std::string& connectionString) {
    if (connectionString.length() == 0) {
        ythrow TContractViolation("Empty connection string");
    }

    TConnectionInfo connectionInfo;

    // Parse the URI with flexible scheme support
    NUri::TUri uri;
    NUri::TUri::TState::EParsed parseStatus = uri.Parse(
        connectionString, 
        NUri::TFeature::FeaturesDefault | NUri::TFeature::FeatureSchemeFlexible
    );
    
    if (parseStatus != NUri::TUri::TState::EParsed::ParsedOK) {
        ythrow TContractViolation(TStringBuilder() 
            << "Failed to parse connection string: " 
            << NUri::ParsedStateToString(parseStatus));
    }

    // Validate and extract scheme
    TStringBuf scheme = uri.GetField(NUri::TUri::FieldScheme);
    if (!scheme.empty()) {
        if (scheme != "grpc" && scheme != "grpcs") {
            ythrow TContractViolation("Invalid scheme in connection string: only 'grpc' and 'grpcs' are allowed");
        }
        connectionInfo.EnableSsl = (scheme == "grpcs");
    } else {
        // No scheme provided - enable SSL if host is not localhost
        TStringBuf host = uri.GetHost();
        connectionInfo.EnableSsl = (host != "localhost");
    }

    // Extract host and port
    TStringBuf host = uri.GetHost();
    if (host.empty()) {
        ythrow TContractViolation("Connection string must contain a host");
    }
    
    ui16 port = uri.GetPort();
    if (port == 0) {
        // No port specified
        connectionInfo.Endpoint = std::string(host);
    } else {
        connectionInfo.Endpoint = std::string(host) + ":" + std::to_string(port);
    }

    // Extract database from path or query parameter
    TStringBuf path = uri.GetField(NUri::TUri::FieldPath);
    TStringBuf query = uri.GetField(NUri::TUri::FieldQuery);

    bool hasQueryDatabase = false;
    if (!query.empty()) {
        TCgiParameters queryParams(query);
        if (queryParams.Has("database")) {
            std::string dbValue = queryParams.Get("database");
            // Ensure database starts with '/'
            if (!dbValue.empty() && dbValue[0] != '/') {
                connectionInfo.Database = "/" + dbValue;
            } else {
                connectionInfo.Database = dbValue;
            }
            hasQueryDatabase = true;
        }
    }

    // Database cannot be in both query params and path
    if (hasQueryDatabase && !path.empty()) {
        ythrow TContractViolation("Database cannot be specified in both path and query parameter");
    }

    // If database not found in query and path exists, use path as database
    if (!hasQueryDatabase && !path.empty()) {
        connectionInfo.Database = std::string(path);
    }

    return connectionInfo;
}

} // namespace NYdb

