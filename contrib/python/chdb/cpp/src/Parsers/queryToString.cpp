#include <Parsers/queryToString.h>
#include <Parsers/formatAST.h>

namespace DB_CHDB
{
    String queryToString(const ASTPtr & query)
    {
        return queryToString(*query);
    }

    String queryToString(const IAST & query)
    {
        return serializeAST(query);
    }
}
