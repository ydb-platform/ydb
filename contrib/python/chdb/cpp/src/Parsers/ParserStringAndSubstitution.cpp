#include <Parsers/ParserStringAndSubstitution.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB_CHDB
{

bool ParserStringAndSubstitution::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserStringLiteral{}.parse(pos, node, expected) || ParserSubstitution{}.parse(pos, node, expected);
}

}
