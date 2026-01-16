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
        NUri::TFeature::FeaturesAll | NUri::TFeature::FeatureSchemeFlexible
    );
    
    if (parseStatus != NUri::TUri::TState::EParsed::ParsedOK) {
        ythrow TContractViolation("Failed to parse connection string: invalid URI format");
    }

    // Validate and extract scheme
    TStringBuf scheme = uri.GetField(NUri::TUri::FieldScheme);
    if (!scheme.empty()) {
        if (scheme != "grpc" && scheme != "grpcs") {
            ythrow TContractViolation("Invalid scheme in connection string: only 'grpc' and 'grpcs' are allowed");
        }
        connectionInfo.EnableSsl = (scheme == "grpcs");
    } else {
        // No scheme provided, assume localhost without SSL
        connectionInfo.EnableSsl = false;
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
        connectionInfo.Endpoint = TStringBuilder() << host << ":" << port;
    }

    // Extract database from path or query parameter
    TStringBuf path = uri.GetField(NUri::TUri::FieldPath);
    TStringBuf query = uri.GetField(NUri::TUri::FieldQuery);

    // First, try to get database from query parameter
    if (!query.empty()) {
        TCgiParameters queryParams(query);
        if (queryParams.Has("database")) {
            connectionInfo.Database = queryParams.Get("database");
        }
    }

    // If database not found in query and path exists, use path as database
    // Note: NUri already includes the leading '/' in the path
    if (connectionInfo.Database.empty() && !path.empty()) {
        connectionInfo.Database = std::string(path);
    }

    return connectionInfo;
}

} // namespace NYdb

