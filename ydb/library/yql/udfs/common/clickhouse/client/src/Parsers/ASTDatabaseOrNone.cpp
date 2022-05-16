#include <Parsers/ASTDatabaseOrNone.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace NDB
{
void ASTDatabaseOrNone::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (none)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "NONE" << (settings.hilite ? IAST::hilite_none : "");
        return;
    }
    settings.ostr << backQuoteIfNeed(database_name);
}

}
