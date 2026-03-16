#pragma once

#include <Parsers/IParserBase.h>


namespace DB_CHDB
{

/** KILL QUERY WHERE <logical expression upon system.processes fields> [SYNC|ASYNC|TEST]
  */
class ParserKillQueryQuery : public IParserBase
{
protected:
    const char * getName() const override { return "KILL QUERY query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

