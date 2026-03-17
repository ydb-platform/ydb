#pragma once

#include <string>

namespace DB_CHDB
{

std::string base64Encode(const std::string & decoded, bool url_encoding = false);

std::string base64Decode(const std::string & encoded, bool url_encoding = false);

}
