#include "query_builder.h"

#include <yt/yt/core/misc/error.h>

#include <util/string/join.h>
#include <util/stream/str.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

static void Parenthesize(TStringBuilderBase* builder, const std::string& str)
{
    builder->AppendChar('(');
    builder->AppendString(str);
    builder->AppendChar(')');
}

void TQueryBuilder::SetSource(std::string source, int syntax, bool subquerySource)
{
    Source_ = std::move(source);
    SyntaxVersion_ = syntax;
    SourceIsQuery_ = subquerySource;
}

void TQueryBuilder::SetSource(std::string source, std::string alias, int syntax, bool subquerySource)
{
    Source_ = std::move(source);
    SourceAlias_ = std::move(alias);
    SyntaxVersion_ = syntax;
    SourceIsQuery_ = subquerySource;
}

int TQueryBuilder::AddSelectExpression(std::string expression)
{
    SelectEntries_.push_back(TEntryWithAlias{
        std::move(expression),
        std::nullopt,
    });
    return SelectEntries_.size() - 1;
}

int TQueryBuilder::AddSelectExpression(std::string expression, std::string alias)
{
    SelectEntries_.push_back(TEntryWithAlias{
        std::move(expression),
        std::move(alias),
    });
    return SelectEntries_.size() - 1;
}

void TQueryBuilder::AddWhereConjunct(std::string expression)
{
    WhereConjuncts_.push_back(std::move(expression));
}

void TQueryBuilder::AddGroupByExpression(std::string expression)
{
    GroupByEntries_.push_back(TEntryWithAlias{
        std::move(expression),
        std::nullopt
    });
}

void TQueryBuilder::AddGroupByExpression(std::string expression, std::string alias)
{
    GroupByEntries_.push_back(TEntryWithAlias{
        std::move(expression),
        std::move(alias),
    });
}

void TQueryBuilder::SetWithTotals(EWithTotalsMode withTotalsMode)
{
    WithTotalsMode_ = withTotalsMode;
}

void TQueryBuilder::AddHavingConjunct(std::string expression)
{
    HavingConjuncts_.push_back(std::move(expression));
}

void TQueryBuilder::AddOrderByExpression(std::string expression)
{
    OrderByEntries_.push_back(TOrderByEntry{
        std::move(expression),
        std::nullopt,
    });
}

void TQueryBuilder::AddOrderByExpression(std::string expression, std::optional<EOrderByDirection> direction)
{
    OrderByEntries_.push_back(TOrderByEntry{
        std::move(expression),
        direction,
    });
}

void TQueryBuilder::AddOrderByAscendingExpression(std::string expression)
{
    AddOrderByExpression(std::move(expression), EOrderByDirection::Ascending);
}

void TQueryBuilder::AddOrderByDescendingExpression(std::string expression)
{
    AddOrderByExpression(std::move(expression), EOrderByDirection::Descending);
}

void TQueryBuilder::SetOffset(i64 offset)
{
    Offset_ = offset;
}

void TQueryBuilder::SetLimit(i64 limit)
{
    Limit_ = limit;
}

void TQueryBuilder::AddArrayJoinExpression(const std::vector<std::string>& expressions,  const std::vector<std::string>& aliases, ETableJoinType type) {
    TJoinEntry entry;
    entry.Type = type;
    entry.ArrayJoinFields.reserve(expressions.size());
    for (size_t ind = 0; ind < expressions.size(); ++ind) {
        entry.ArrayJoinFields.push_back({expressions[ind], aliases[ind]});
    }
    JoinEntries_.push_back(std::move(entry));
}

void TQueryBuilder::AddJoinExpression(
    std::string table,
    std::string alias,
    std::string onExpression,
    ETableJoinType type)
{
    JoinEntries_.push_back(TJoinEntry{
        std::move(table),
        std::move(alias),
        std::move(onExpression),
        type,
        std::vector<TEntryWithAlias>(),
    });
}

std::string TQueryBuilder::WrapTableName(const std::string& table) {
    if (SyntaxVersion_ == 1) {
        return "[" + table + "]";
    } else if (SyntaxVersion_ == 2) {
        return "`" + table + "`";
    } else {
        THROW_ERROR_EXCEPTION("Only syntax versions 1 and 2 are supported");
    }
}

std::string TQueryBuilder::Build()
{
    TStringBuilder builder;
    TDelimitedStringBuilderWrapper wrapper(&builder, " ");

    if (SelectEntries_.empty()) {
        THROW_ERROR_EXCEPTION("Query must have at least one SELECT expression");
    }
    JoinToString(&wrapper, SelectEntries_.begin(), SelectEntries_.end(), &FormatEntryWithAlias);

    if (!Source_) {
        THROW_ERROR_EXCEPTION("Source must be specified in query");
    }
    if(!SourceIsQuery_) {
        if (!SourceAlias_) {
            wrapper->AppendFormat("FROM %v", WrapTableName(*Source_));
        } else {
            wrapper->AppendFormat("FROM %v AS %v", WrapTableName(*Source_), *SourceAlias_);
        }
    } else {
        if (!SourceAlias_) {
            wrapper->AppendFormat("FROM (%v)", *Source_);
        } else {
            wrapper->AppendFormat("FROM (%v) AS %v", *Source_, *SourceAlias_);
        }
    }

    for (const auto& join : JoinEntries_) {
        if (join.Type == ETableJoinType::Inner || join.Type == ETableJoinType::Left) {
            TStringBuf joinType = join.Type == ETableJoinType::Inner ? "JOIN" : "LEFT JOIN";
            wrapper->AppendFormat("%v %v AS %v ON %v", joinType, WrapTableName(join.Table), WrapTableName(join.Alias), join.OnExpression);
        } else {
            TStringBuf joinType = join.Type == ETableJoinType::ArrayInner ? "ARRAY JOIN" : "LEFT ARRAY JOIN";
            wrapper->AppendFormat(" %v ", joinType);
            JoinToString(&wrapper, join.ArrayJoinFields.begin(), join.ArrayJoinFields.end(), &FormatEntryWithAlias);
        }
    }


    if (!WhereConjuncts_.empty()) {
        wrapper->AppendFormat("WHERE");
        JoinToString(&wrapper, WhereConjuncts_.begin(), WhereConjuncts_.end(), &Parenthesize, " AND ");
    }

    if (!GroupByEntries_.empty()) {
        wrapper->AppendString("GROUP BY");
        JoinToString(&wrapper, GroupByEntries_.begin(), GroupByEntries_.end(), &FormatEntryWithAlias);
    }

    if (WithTotalsMode_ == EWithTotalsMode::BeforeHaving) {
        wrapper->AppendString("WITH TOTALS");
    }

    if (!HavingConjuncts_.empty()) {
        if (GroupByEntries_.empty()) {
            THROW_ERROR_EXCEPTION("Having without group by is not valid");
        }
        wrapper->AppendString("HAVING");
        JoinToString(&wrapper, HavingConjuncts_.begin(), HavingConjuncts_.end(), &Parenthesize, " AND ");
    }

    if (WithTotalsMode_ == EWithTotalsMode::AfterHaving) {
        wrapper->AppendString("WITH TOTALS");
    }

    if (!OrderByEntries_.empty()) {
        wrapper->AppendString("ORDER BY");
        JoinToString(&wrapper, OrderByEntries_.begin(), OrderByEntries_.end(), &FormatOrderByEntry);
    }

    if (Offset_) {
        wrapper->AppendFormat("OFFSET %v", *Offset_);
    }

    if (Limit_) {
        wrapper->AppendFormat("LIMIT %v", *Limit_);
    }

    return builder.Flush();
}

void TQueryBuilder::FormatEntryWithAlias(TStringBuilderBase* builder, const TQueryBuilder::TEntryWithAlias& entry)
{
    if (entry.Expression == "*") {
        builder->AppendChar('*');
        return;
    }
    builder->AppendChar('(');
    builder->AppendString(entry.Expression);
    builder->AppendChar(')');
    if (entry.Alias) {
        builder->AppendString(" AS ");
        builder->AppendString(*entry.Alias);
    }
}

void TQueryBuilder::FormatOrderByEntry(TStringBuilderBase* builder, const TQueryBuilder::TOrderByEntry& entry)
{
    builder->AppendChar('(');
    builder->AppendString(entry.Expression);
    builder->AppendChar(')');
    if (entry.Direction) {
        TStringBuf directionString = (*entry.Direction == EOrderByDirection::Ascending)
            ? "ASC"
            : "DESC";
        builder->AppendChar(' ');
        builder->AppendString(directionString);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
