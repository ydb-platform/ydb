#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserWatchQuery.h>
#include <Parsers/ExpressionElementParsers.h>


namespace NDB
{

bool ParserWatchQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_watch("WATCH");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;
    ParserKeyword s_events("EVENTS");
    ParserKeyword s_limit("LIMIT");

    ASTPtr database;
    ASTPtr table;
    auto query = std::make_shared<ASTWatchQuery>();

    if (!s_watch.ignore(pos, expected))
    {
        return false;
    }

    if (!name_p.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return false;
    }

    /// EVENTS
    if (s_events.ignore(pos, expected))
    {
        query->is_watch_events = true;
    }

    /// LIMIT length
    if (s_limit.ignore(pos, expected))
    {
        ParserNumber num;

        if (!num.parse(pos, query->limit_length, expected))
            return false;
    }

    if (database)
        query->database = getIdentifierName(database);

    if (table)
        query->table = getIdentifierName(table);

    node = query;

    return true;
}


}
