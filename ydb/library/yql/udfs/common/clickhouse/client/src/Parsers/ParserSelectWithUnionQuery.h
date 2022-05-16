#pragma once

#include <Parsers/IParserBase.h>

namespace NDB
{


class ParserSelectWithUnionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SELECT query, possibly with UNION"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
