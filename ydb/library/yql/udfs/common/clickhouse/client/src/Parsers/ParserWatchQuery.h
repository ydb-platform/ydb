#pragma once

#include <Parsers/IParserBase.h>


namespace NDB
{

/** Query like this:
  * WATCH [db.]table EVENTS
  */
class ParserWatchQuery : public IParserBase
{
protected:
    const char * getName() const override { return "WATCH query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
