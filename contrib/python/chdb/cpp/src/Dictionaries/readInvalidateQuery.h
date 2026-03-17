#pragma once

#include <string>

namespace DB_CHDB
{

class QueryPipeline;

/// Using in MySQLDictionarySource and XDBCDictionarySource after processing invalidate_query.
std::string readInvalidateQuery(QueryPipeline pipeline);

}
