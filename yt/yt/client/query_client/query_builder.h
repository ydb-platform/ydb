#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOrderByDirection,
    (Ascending)
    (Descending)
);

DEFINE_ENUM(ETableJoinType,
    (Inner)
    (Left)
    (ArrayInner)
    (ArrayLeft)
);

DEFINE_ENUM(EWithTotalsMode,
    (None)
    (BeforeHaving)
    (AfterHaving)
);

////////////////////////////////////////////////////////////////////////////////

class TQueryBuilder
{
public:

    void SetSource(std::string source, int syntaxVersion = 1, bool subquerySource = false);
    void SetSource(std::string source, std::string alias, int syntaxVersion = 1, bool subquerySource = false);

    int AddSelectExpression(std::string expression);
    int AddSelectExpression(std::string expression, std::string alias);

    void AddWhereConjunct(std::string expression);

    void AddGroupByExpression(std::string expression);
    void AddGroupByExpression(std::string expression, std::string alias);

    void SetWithTotals(EWithTotalsMode withTotalsMode);

    void AddHavingConjunct(std::string expression);

    void AddOrderByExpression(std::string expression);
    void AddOrderByExpression(std::string expression, std::optional<EOrderByDirection> direction);

    void AddOrderByAscendingExpression(std::string expression);
    void AddOrderByDescendingExpression(std::string expression);

    void AddJoinExpression(std::string table, std::string alias, std::string onExpression, ETableJoinType type);
    void AddArrayJoinExpression(const std::vector<std::string>& expressions,  const std::vector<std::string>& aliases, ETableJoinType type);

    void SetOffset(i64 offset);
    void SetLimit(i64 limit);

    std::string Build();

private:
    struct TEntryWithAlias
    {
        std::string Expression;
        std::optional<std::string> Alias;
    };

    struct TOrderByEntry
    {
        std::string Expression;
        std::optional<EOrderByDirection> Direction;
    };

    struct TJoinEntry
    {
        std::string Table;
        std::string Alias;
        std::string OnExpression;
        ETableJoinType Type;
        std::vector<TEntryWithAlias> ArrayJoinFields;
    };

private:
    std::optional<std::string> Source_;
    bool SourceIsQuery_ = false;
    int SyntaxVersion_ = 1;
    std::optional<std::string> SourceAlias_;
    std::vector<TEntryWithAlias> SelectEntries_;
    std::vector<std::string> WhereConjuncts_;
    std::vector<TOrderByEntry> OrderByEntries_;
    std::vector<TEntryWithAlias> GroupByEntries_;
    EWithTotalsMode WithTotalsMode_ = EWithTotalsMode::None;
    std::vector<std::string> HavingConjuncts_;
    std::vector<TJoinEntry> JoinEntries_;
    std::optional<i64> Offset_;
    std::optional<i64> Limit_;

    std::string WrapTableName(const std::string& source);

    static void FormatEntryWithAlias(TStringBuilderBase* builder, const TEntryWithAlias& entry);
    static void FormatOrderByEntry(TStringBuilderBase* builder, const TOrderByEntry& entry);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
