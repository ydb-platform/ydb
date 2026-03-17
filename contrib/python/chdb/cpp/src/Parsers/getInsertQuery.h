#pragma once
#include <Core/ColumnsWithTypeAndName.h>
#include <Parsers/IdentifierQuotingStyle.h>

namespace DB_CHDB
{
std::string getInsertQuery(const std::string & db_name, const std::string & table_name, const ColumnsWithTypeAndName & columns, IdentifierQuotingStyle quoting);
}
