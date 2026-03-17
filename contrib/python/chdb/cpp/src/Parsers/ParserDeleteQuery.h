#pragma once

#include <Parsers/IParserBase.h>

namespace DB_CHDB
{

/** Query like this:
  * DELETE FROM [db.]name WHERE ...
  */

class ParserDeleteQuery : public IParserBase
{
protected:
    const char * getName() const  override{ return "Delete query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
