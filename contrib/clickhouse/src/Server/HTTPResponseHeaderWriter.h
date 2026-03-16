#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <base/types.h>
#include <DBPoco/Net/HTTPResponse.h>
#include <DBPoco/Util/AbstractConfiguration.h>

namespace DB
{

using HTTPResponseHeaderSetup = std::optional<std::unordered_map<String, String>>;

HTTPResponseHeaderSetup parseHTTPResponseHeaders(const DBPoco::Util::AbstractConfiguration & config, const std::string & config_prefix);

std::unordered_map<String, String> parseHTTPResponseHeaders(
    const DBPoco::Util::AbstractConfiguration & config, const std::string & config_prefix, const std::string & default_content_type);

std::unordered_map<String, String> parseHTTPResponseHeaders(const std::string & default_content_type);

void applyHTTPResponseHeaders(DBPoco::Net::HTTPResponse & response, const HTTPResponseHeaderSetup & setup);

void applyHTTPResponseHeaders(DBPoco::Net::HTTPResponse & response, const std::unordered_map<String, String> & setup);
}
