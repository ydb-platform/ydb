#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <base/types.h>
#include <CHDBPoco/Net/HTTPResponse.h>
#include <CHDBPoco/Util/AbstractConfiguration.h>

namespace DB_CHDB
{

using HTTPResponseHeaderSetup = std::optional<std::unordered_map<String, String>>;

HTTPResponseHeaderSetup parseHTTPResponseHeaders(const CHDBPoco::Util::AbstractConfiguration & config, const std::string & config_prefix);

std::unordered_map<String, String> parseHTTPResponseHeaders(
    const CHDBPoco::Util::AbstractConfiguration & config, const std::string & config_prefix, const std::string & default_content_type);

std::unordered_map<String, String> parseHTTPResponseHeaders(const std::string & default_content_type);

void applyHTTPResponseHeaders(CHDBPoco::Net::HTTPResponse & response, const HTTPResponseHeaderSetup & setup);

void applyHTTPResponseHeaders(CHDBPoco::Net::HTTPResponse & response, const std::unordered_map<String, String> & setup);
}
