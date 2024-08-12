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
);

////////////////////////////////////////////////////////////////////////////////

class TQueryBuilder
{
public:
    void SetSource(TString source);
    void SetSource(TString source, TString alias);

    int AddSelectExpression(TString expression);
    int AddSelectExpression(TString expression, TString alias);

    void AddWhereConjunct(TString expression);

    void AddGroupByExpression(TString expression);
    void AddGroupByExpression(TString expression, TString alias);

    void AddHavingConjunct(TString expression);

    void AddOrderByExpression(TString expression);
    void AddOrderByExpression(TString expression, std::optional<EOrderByDirection> direction);

    void AddOrderByAscendingExpression(TString expression);
    void AddOrderByDescendingExpression(TString expression);

    void AddJoinExpression(TString table, TString alias, TString onExpression, ETableJoinType type);

    void SetLimit(i64 limit);

    TString Build();

private:
    struct TEntryWithAlias
    {
        TString Expression;
        std::optional<TString> Alias;
    };

    struct TOrderByEntry
    {
        TString Expression;
        std::optional<EOrderByDirection> Direction;
    };

    struct TJoinEntry
    {
        TString Table;
        TString Alias;
        TString OnExpression;
        ETableJoinType Type;
    };

private:
    std::optional<TString> Source_;
    std::optional<TString> SourceAlias_;
    std::vector<TEntryWithAlias> SelectEntries_;
    std::vector<TString> WhereConjuncts_;
    std::vector<TOrderByEntry> OrderByEntries_;
    std::vector<TEntryWithAlias> GroupByEntries_;
    std::vector<TString> HavingConjuncts_;
    std::vector<TJoinEntry> JoinEntries_;
    std::optional<i64> Limit_;

private:
    // We overload this functions to allow the corresponding JoinSeq().
    friend void AppendToString(TString& dst, const TEntryWithAlias& entry);
    friend void AppendToString(TString& dst, const TOrderByEntry& entry);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
