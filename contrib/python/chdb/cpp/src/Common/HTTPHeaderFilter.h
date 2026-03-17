#pragma once

#include <IO/HTTPHeaderEntries.h>
#include <CHDBPoco/Util/AbstractConfiguration.h>
#include <vector>
#include <unordered_set>
#include <mutex>


namespace DB_CHDB
{

class HTTPHeaderFilter
{
public:

    void setValuesFromConfig(const CHDBPoco::Util::AbstractConfiguration & config);
    void checkHeaders(const HTTPHeaderEntries & entries) const;

private:
    std::unordered_set<std::string> forbidden_headers;
    std::vector<std::string> forbidden_headers_regexp;

    mutable std::mutex mutex;
};

}
