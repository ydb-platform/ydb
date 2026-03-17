#pragma once

#include <Parsers/IParserBase.h>

namespace DB_CHDB
{

/// Parser for ASTRefreshStrategy
class ParserRefreshStrategy : public IParserBase
{
protected:
    const char * getName() const override { return "refresh strategy"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
