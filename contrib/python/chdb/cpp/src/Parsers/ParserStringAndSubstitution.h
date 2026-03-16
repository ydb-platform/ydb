#pragma once

#include <Parsers/IParserBase.h>

namespace DB_CHDB
{

class ParserStringAndSubstitution : public IParserBase
{
private:
    const char * getName() const override { return "ParserStringAndSubstitution"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserStringAndSubstitution() = default;
};

}
