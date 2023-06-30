#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOrderByDirection,
    (Ascending)
    (Descending)
);

////////////////////////////////////////////////////////////////////////////////

class TQueryBuilder
{
public:
    void SetSource(TString source);

    int AddSelectExpression(TString expression);
    int AddSelectExpression(TString expression, TString alias);

    void AddWhereConjunct(TString expression);

    void AddGroupByExpression(TString expression);
    void AddGroupByExpression(TString expression, TString alias);

    void AddOrderByExpression(TString expression);
    void AddOrderByExpression(TString expression, std::optional<EOrderByDirection> direction);

    void AddOrderByAscendingExpression(TString expression);
    void AddOrderByDescendingExpression(TString expression);

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

private:
    std::optional<TString> Source_;
    std::vector<TEntryWithAlias> SelectEntries_;
    std::vector<TString> WhereConjuncts_;
    std::vector<TOrderByEntry> OrderByEntries_;
    std::vector<TEntryWithAlias> GroupByEntries_;
    std::optional<i64> Limit_;

private:
    // We overload this functions to allow the corresponding JoinSeq().
    friend void AppendToString(TString& dst, const TEntryWithAlias& entry);
    friend void AppendToString(TString& dst, const TOrderByEntry& entry);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
