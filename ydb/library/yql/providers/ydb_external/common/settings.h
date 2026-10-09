#pragma once

#include <util/generic/string.h>
#include <util/string/cast.h>

namespace NYql::NYdbExternal {

// Shared by DDL validation and provider compilation; errors never echo settings.
inline TString ValidateConnectionSettings(const TString& endpoint, const TString& database, TString useTls) {
    const auto hasControlOrSpace = [](const TString& value) {
        for (const unsigned char c : value) {
            if (c <= 32 || c == 127) {
                return true;
            }
        }
        return false;
    };
    if (!database.StartsWith('/') || hasControlOrSpace(database) || database.Contains("//") ||
        database.Contains("/../") || database.EndsWith("/..") ||
        database.Contains("/./") || database.EndsWith("/.")) {
        return "YdbExternal requires an absolute DATABASE_NAME without empty, '.' or '..' path components";
    }
    const auto portSeparator = endpoint.rfind(':');
    ui32 port = 0;
    if (!(portSeparator != TString::npos && portSeparator != 0 &&
        !endpoint.Contains('/') && !endpoint.Contains('@') &&
        !endpoint.Contains('?') && !endpoint.Contains('#') && !hasControlOrSpace(endpoint) &&
        TryFromString(TStringBuf(endpoint).SubStr(portSeparator + 1), port) && port > 0 && port <= 65535)) {
        return "YdbExternal requires LOCATION in host:port format";
    }
    useTls.to_lower();
    if (useTls != "true" && useTls != "false") {
        return "YdbExternal USE_TLS must be true or false";
    }
    return {};
}

} // namespace NYql::NYdbExternal
