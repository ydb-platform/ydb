#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>

namespace NDB
{

class ASTWatchQuery : public ASTQueryWithTableAndOutput
{

public:
    ASTPtr limit_length;
    bool is_watch_events;

    ASTWatchQuery() = default;
    String getID(char) const override { return "WatchQuery_" + database + "_" + table; }

    ASTPtr clone() const override
    {
        std::shared_ptr<ASTWatchQuery> res = std::make_shared<ASTWatchQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override
    {
        std::string indent_str = s.one_line ? "" : std::string(4 * frame.indent, ' ');

        s.ostr << (s.hilite ? hilite_keyword : "") << "WATCH " << (s.hilite ? hilite_none : "")
            << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);

        if (is_watch_events)
        {
            s.ostr << " " << (s.hilite ? hilite_keyword : "") << "EVENTS" << (s.hilite ? hilite_none : "");
        }

        if (limit_length)
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "LIMIT " << (s.hilite ? hilite_none : "");
            limit_length->formatImpl(s, state, frame);
        }
    }
};

}
