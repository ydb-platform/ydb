#pragma once
#include <Parsers/IAST_fwd.h>
#include <Core/Types.h>

namespace DB_CHDB
{
ASTPtr tryGetTableOverride(const String & mapped_database, const String & table);
}
