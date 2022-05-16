#pragma once

#include <Parsers/IParserBase.h>

namespace NDB
{


class ParserExplainQuery : public IParserBase
{
protected:
    const char * end;

    const char * getName() const override { return "EXPLAIN"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    ParserExplainQuery(const char* end_) : end(end_) {}
};

}
