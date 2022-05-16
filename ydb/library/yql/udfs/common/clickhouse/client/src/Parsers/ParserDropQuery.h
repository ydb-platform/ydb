#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace NDB
{

/** Query like this:
  * DROP|DETACH|TRUNCATE TABLE [IF EXISTS] [db.]name [PERMANENTLY]
  *
  * Or:
  * DROP DATABASE [IF EXISTS] db
  *
  * Or:
  * DROP DICTIONARY [IF EXISTS] [db.]name
  */
class ParserDropQuery : public IParserBase
{
protected:
    const char * getName() const  override{ return "DROP query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
