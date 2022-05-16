#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserBackupQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserSetRoleQuery.h>
#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ParserUseQuery.h>
#include <Parsers/ParserExternalDDLQuery.h>


namespace NDB
{


bool ParserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserInsertQuery insert_p(end);
    ParserUseQuery use_p;
    ParserSetQuery set_p;
    ParserSystemQuery system_p;
    ParserSetRoleQuery set_role_p;
    ParserExternalDDLQuery external_ddl_p;
    ParserBackupQuery backup_p;

    bool res = insert_p.parse(pos, node, expected)
        || use_p.parse(pos, node, expected)
        || set_role_p.parse(pos, node, expected)
        || set_p.parse(pos, node, expected)
        || system_p.parse(pos, node, expected)
        || external_ddl_p.parse(pos, node, expected)
        || backup_p.parse(pos, node, expected);

    return res;
}

}
