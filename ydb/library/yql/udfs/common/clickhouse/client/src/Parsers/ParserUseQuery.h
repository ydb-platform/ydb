#pragma once

#include <Parsers/IParserBase.h>


namespace NDB
{

/** Query USE db
  */
class ParserUseQuery : public IParserBase
{
protected:
    const char * getName() const  override{ return "USE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
