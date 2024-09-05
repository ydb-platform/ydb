#include "query_builder.h"

#include <yt/yt/core/misc/error.h>

#include <util/string/join.h>
#include <util/stream/str.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

static std::vector<TString> Parenthesize(std::vector<TString> strings)
{
    for (auto& string : strings) {
        string.prepend('(').append(')');
    }
    return strings;
}

void TQueryBuilder::SetSource(TString source)
{
    Source_ = std::move(source);
}

void TQueryBuilder::SetSource(TString source, TString alias)
{
    Source_ = std::move(source);
    SourceAlias_ = std::move(alias);
}

int TQueryBuilder::AddSelectExpression(TString expression)
{
    SelectEntries_.push_back(TEntryWithAlias{
        std::move(expression),
        std::nullopt,
    });
    return SelectEntries_.size() - 1;
}

int TQueryBuilder::AddSelectExpression(TString expression, TString alias)
{
    SelectEntries_.push_back(TEntryWithAlias{
        std::move(expression),
        std::move(alias),
    });
    return SelectEntries_.size() - 1;
}

void TQueryBuilder::AddWhereConjunct(TString expression)
{
    WhereConjuncts_.push_back(std::move(expression));
}

void TQueryBuilder::AddGroupByExpression(TString expression)
{
    GroupByEntries_.push_back(TEntryWithAlias{
        std::move(expression),
        std::nullopt
    });
}

void TQueryBuilder::AddGroupByExpression(TString expression, TString alias)
{
    GroupByEntries_.push_back(TEntryWithAlias{
        std::move(expression),
        std::move(alias),
    });
}

void TQueryBuilder::AddHavingConjunct(TString expression)
{
    HavingConjuncts_.push_back(std::move(expression));
}

void TQueryBuilder::AddOrderByExpression(TString expression)
{
    OrderByEntries_.push_back(TOrderByEntry{
        std::move(expression),
        std::nullopt,
    });
}

void TQueryBuilder::AddOrderByExpression(TString expression, std::optional<EOrderByDirection> direction)
{
    OrderByEntries_.push_back(TOrderByEntry{
        std::move(expression),
        direction,
    });
}

void TQueryBuilder::AddOrderByAscendingExpression(TString expression)
{
    AddOrderByExpression(std::move(expression), EOrderByDirection::Ascending);
}

void TQueryBuilder::AddOrderByDescendingExpression(TString expression)
{
    AddOrderByExpression(std::move(expression), EOrderByDirection::Descending);
}

void TQueryBuilder::SetLimit(i64 limit)
{
    Limit_ = limit;
}

void TQueryBuilder::AddJoinExpression(
    TString table,
    TString alias,
    TString onExpression,
    ETableJoinType type)
{
    JoinEntries_.push_back(TJoinEntry{
        std::move(table),
        std::move(alias),
        std::move(onExpression),
        type,
    });
}

TString TQueryBuilder::Build()
{
    std::vector<TString> parts;
    parts.reserve(8);

    if (SelectEntries_.empty()) {
        THROW_ERROR_EXCEPTION("Query must have at least one SELECT expression");
    }
    parts.push_back(JoinSeq(", ", SelectEntries_));

    if (!Source_) {
        THROW_ERROR_EXCEPTION("Source must be specified in query");
    }
    if (!SourceAlias_) {
        parts.push_back(Format("FROM [%v]", *Source_));
    } else {
        parts.push_back(Format("FROM [%v] AS %v", *Source_, *SourceAlias_));
    }

    for (const auto& join : JoinEntries_) {
        TStringBuf joinType = join.Type == ETableJoinType::Inner ? "JOIN" : "LEFT JOIN";
        parts.push_back(Format("%v [%v] AS [%v] ON %v", joinType, join.Table, join.Alias, join.OnExpression));
    }

    if (!WhereConjuncts_.empty()) {
        parts.push_back("WHERE");
        parts.push_back(JoinSeq(" AND ", Parenthesize(WhereConjuncts_)));
    }

    if (!GroupByEntries_.empty()) {
        parts.push_back("GROUP BY");
        parts.push_back(JoinSeq(", ", GroupByEntries_));
    }

    if (!HavingConjuncts_.empty()) {
        if (GroupByEntries_.empty()) {
            THROW_ERROR_EXCEPTION("Having without group by is not valid");
        }
        parts.push_back("HAVING");
        parts.push_back(JoinSeq(" AND ", Parenthesize(HavingConjuncts_)));
    }

    if (!OrderByEntries_.empty()) {
        parts.push_back("ORDER BY");
        parts.push_back(JoinSeq(", ", OrderByEntries_));
    }

    if (Limit_) {
        parts.push_back(Format("LIMIT %v", *Limit_));
    }

    return JoinSeq(" ", parts);
}

void AppendToString(TString& dst, const TQueryBuilder::TEntryWithAlias& entry)
{
    TStringOutput output(dst);
    if (entry.Expression == "*") {
        output << "*";
        return;
    }
    output << '(' << entry.Expression << ')';
    if (entry.Alias) {
        output << " AS " << *entry.Alias;
    }
}

void AppendToString(TString& dst, const TQueryBuilder::TOrderByEntry& entry)
{
    TStringOutput output(dst);
    output << '(' << entry.Expression << ')';
    if (entry.Direction) {
        TStringBuf directionString = (*entry.Direction == EOrderByDirection::Ascending)
            ? "ASC"
            : "DESC";
        output << ' ' << directionString;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
