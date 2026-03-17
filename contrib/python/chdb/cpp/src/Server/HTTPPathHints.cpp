#include <Server/HTTPPathHints.h>

namespace DB_CHDB
{

void HTTPPathHints::add(const String & http_path)
{
    http_paths.push_back(http_path);
}

std::vector<String> HTTPPathHints::getAllRegisteredNames() const
{
    return http_paths;
}

}
