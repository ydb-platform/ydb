#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>

namespace DB_CHDB
{

class ParserKQLSummarize : public ParserKQLBase
{
protected:
    const char * getName() const override { return "KQL summarize"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
