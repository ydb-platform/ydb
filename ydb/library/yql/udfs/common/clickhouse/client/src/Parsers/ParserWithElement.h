#pragma once

#include <Parsers/IParserBase.h>


namespace NDB
{
/** WITH (scalar query) AS identifier
  *  or WITH identifier AS (subquery)
  */
class ParserWithElement : public IParserBase
{
protected:
    const char * getName() const override { return "WITH element"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
