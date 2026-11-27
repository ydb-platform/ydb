#include "hierarchical_join_rewriter.h"
#include "query.h"
#include "query_visitors.h"
#include "query_helpers.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NQueryClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TSubqueryDiscoveryResult
{
    std::vector<TConstSubqueryExpressionPtr> InWhereClause;
    std::vector<TConstSubqueryExpressionPtr> InProjectClause;
    std::vector<TConstSubqueryExpressionPtr> InHavingClause;
    std::vector<TConstSubqueryExpressionPtr> InOrderClause;
    std::vector<TConstSubqueryExpressionPtr> InGroupItems;
    std::vector<TConstSubqueryExpressionPtr> InAggregateArguments;
    std::vector<TConstSubqueryExpressionPtr> InJoinClauses;
};

void ThrowOnUnimplementedSubqueryType(const TSubqueryDiscoveryResult& subqueries)
{
    THROW_ERROR_EXCEPTION_IF(
        !subqueries.InHavingClause.empty(),
        "Subquery with JOIN in HAVING clause is not supported");

    THROW_ERROR_EXCEPTION_IF(
        !subqueries.InOrderClause.empty(),
        "Subquery with JOIN in ORDER BY clause is not supported");

    THROW_ERROR_EXCEPTION_IF(
        !subqueries.InJoinClauses.empty(),
        "Subquery with JOIN in JOIN clause is not supported");
}

bool IsEmpty(const TSubqueryDiscoveryResult& subqueries)
{
    return subqueries.InWhereClause.empty() &&
        subqueries.InProjectClause.empty() &&
        subqueries.InHavingClause.empty() &&
        subqueries.InOrderClause.empty() &&
        subqueries.InGroupItems.empty() &&
        subqueries.InAggregateArguments.empty() &&
        subqueries.InJoinClauses.empty();
}

class TSubqueryWithJoinsCollectorVisitor
    : public TVisitor<TSubqueryWithJoinsCollectorVisitor>
{
public:
    using TBase = TVisitor<TSubqueryWithJoinsCollectorVisitor>;

    explicit TSubqueryWithJoinsCollectorVisitor(std::vector<TConstSubqueryExpressionPtr>* subqueries)
        : Subqueries_(subqueries)
    { }

    void OnSubquery(const TSubqueryExpression* subqueryExpression)
    {
        THROW_ERROR_EXCEPTION_IF(
            InsideJoinSubquery_,
            "Subquery nested inside a subquery with JOIN is not supported");

        bool hasJoin = !subqueryExpression->JoinClauses.empty();

        if (hasJoin) {
            THROW_ERROR_EXCEPTION_IF(
                Depth_ > 0,
                "Subquery with JOIN nested inside another subquery is not supported");
            Subqueries_->push_back(subqueryExpression);
        }

        int savedDepth = Depth_;
        bool savedInsideJoinSubquery = InsideJoinSubquery_;
        Depth_++;
        InsideJoinSubquery_ = hasJoin;
        TBase::OnSubquery(subqueryExpression);
        Depth_ = savedDepth;
        InsideJoinSubquery_ = savedInsideJoinSubquery;
    }

private:
    std::vector<TConstSubqueryExpressionPtr>* const Subqueries_;
    int Depth_ = 0;
    bool InsideJoinSubquery_ = false;
};

TSubqueryDiscoveryResult DiscoverSubqueries(const TQueryPtr& query)
{
    TSubqueryDiscoveryResult result;

    auto collect = [] (const TConstExpressionPtr& expression, std::vector<TConstSubqueryExpressionPtr>* subqueries) {
        TSubqueryWithJoinsCollectorVisitor visitor(subqueries);
        visitor.Visit(expression);
    };

    if (query->WhereClause) {
        collect(query->WhereClause, &result.InWhereClause);
    }

    if (query->ProjectClause) {
        for (const auto& projection : query->ProjectClause->Projections) {
            collect(projection.Expression, &result.InProjectClause);
        }
    }

    if (query->HavingClause) {
        collect(query->HavingClause, &result.InHavingClause);
    }

    if (query->OrderClause) {
        for (const auto& orderItem : query->OrderClause->OrderItems) {
            collect(orderItem.Expression, &result.InOrderClause);
        }
    }

    if (query->GroupClause) {
        for (const auto& groupItem : query->GroupClause->GroupItems) {
            collect(groupItem.Expression, &result.InGroupItems);
        }

        for (const auto& aggregateItem : query->GroupClause->AggregateItems) {
            for (const auto& argument : aggregateItem.Arguments) {
                collect(argument, &result.InAggregateArguments);
            }
        }
    }

    for (const auto& joinClause : query->JoinClauses) {
        if (joinClause->Predicate) {
            collect(joinClause->Predicate, &result.InJoinClauses);
        }

        for (const auto& equation : joinClause->ForeignEquations) {
            collect(equation, &result.InJoinClauses);
        }

        for (const auto& equation : joinClause->SelfEquations) {
            collect(equation, &result.InJoinClauses);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

struct TSplitSubqueryResult
{
    TConstSubqueryExpressionPtr SelfSideJoinKeys;
    TConstSubqueryExpressionPtr JoiningSubquery;
};

TTableSchemaPtr BuildSchemaFromNamedItems(const TNamedItemList& namedItems)
{
    std::vector<NTableClient::TColumnSchema> columns;
    columns.reserve(namedItems.size());
    for (const auto& [expression, name] : namedItems) {
        columns.emplace_back(name, expression->LogicalType);
    }

    return New<NTableClient::TTableSchema>(std::move(columns));
}

TSplitSubqueryResult SplitSubquery(const TConstSubqueryExpressionPtr& originalSubquery)
{
    THROW_ERROR_EXCEPTION_IF(
        originalSubquery->JoinClauses.empty(),
        "Subquery must have at least one join clause");

    THROW_ERROR_EXCEPTION_IF(
        originalSubquery->JoinClauses.size() > 1,
        "Subquery with more than one join clause is not supported");

    const auto& originalJoinClause = originalSubquery->JoinClauses[0];

    auto selfSideJoinKeys = New<TSubqueryExpression>(originalSubquery->LogicalType);
    {
        selfSideJoinKeys->FromExpressions = originalSubquery->FromExpressions;

        if (originalSubquery->WhereClause) {
            auto selfSchema = BuildSchemaFromNamedItems(originalSubquery->FromExpressions);
            auto [selfFilter, _] = SplitPredicateByColumnSubset(originalSubquery->WhereClause, *selfSchema);
            selfSideJoinKeys->WhereClause = selfFilter;
        }

        auto selfProjectClause = New<TProjectClause>();
        for (const auto& expression : originalJoinClause->SelfEquations) {
            selfProjectClause->AddProjection(expression, InferName(expression));
        }

        selfSideJoinKeys->ProjectClause = selfProjectClause;
    }

    auto joiningSubquery = New<TSubqueryExpression>(originalSubquery->LogicalType);
    {
        joiningSubquery->FromExpressions = originalSubquery->FromExpressions;
        joiningSubquery->GroupClause = originalSubquery->GroupClause;
        joiningSubquery->ProjectClause = originalSubquery->ProjectClause;
        joiningSubquery->OrderClause = originalSubquery->OrderClause;
        joiningSubquery->Offset = originalSubquery->Offset;
        joiningSubquery->Limit = originalSubquery->Limit;

        auto joinClause = New<TJoinClause>(*originalJoinClause);

        if (originalSubquery->WhereClause && !joinClause->IsLeft) {
            // Inner join: it is safe to move foreign-only WHERE predicates into
            // JoinClause->Predicate so GetJoinSubquery() applies them at fetch time and
            // the compiled JoiningSubquery needs no redundant post-join FilterOp.
            auto foreignSchema = originalJoinClause->Schema.GetRenamedSchema();
            auto [foreignFilter, nonForeignFilter] = SplitPredicateByColumnSubset(
                originalSubquery->WhereClause, *foreignSchema);
            if (!IsTrue(foreignFilter)) {
                joinClause->Predicate = MakeAndExpression(originalJoinClause->Predicate, foreignFilter);
            }
            joiningSubquery->WhereClause = IsTrue(nonForeignFilter) ? nullptr : nonForeignFilter;
        } else {
            // Left join: retain WHERE after the join. Pushing it into the foreign
            // fetch can remove every real match and create a NULL-padded row
            // that satisfies the predicate.
            joiningSubquery->WhereClause = originalSubquery->WhereClause;
        }

        joiningSubquery->JoinClauses = {joinClause};
    }

    return {selfSideJoinKeys, joiningSubquery};
}

THierarchicalJoinClausePtr BuildHierarchicalJoinFromSubquery(
    const TConstSubqueryExpressionPtr& subquery,
    const std::string& columnName,
    const NLogging::TLogger& Logger)
{
    auto hierarchicalJoin = New<THierarchicalJoinClause>();

    auto split = SplitSubquery(subquery);
    hierarchicalJoin->SelfSideJoinKeys = split.SelfSideJoinKeys;
    hierarchicalJoin->JoiningSubquery = split.JoiningSubquery;

    hierarchicalJoin->ResultColumnName = columnName;

    const auto& joinClause = subquery->JoinClauses[0];
    hierarchicalJoin->ForeignTableSchema = joinClause->Schema;
    hierarchicalJoin->ForeignEquations = joinClause->ForeignEquations;
    hierarchicalJoin->SelfEquations = joinClause->SelfEquations;
    hierarchicalJoin->IsLeft = joinClause->IsLeft;
    hierarchicalJoin->ForeignObjectId = joinClause->ForeignObjectId;
    hierarchicalJoin->ForeignCellId = joinClause->ForeignCellId;
    hierarchicalJoin->ForeignJoinedColumns = joinClause->ForeignJoinedColumns;

    YT_TLOG_DEBUG("Built hierarchical join")
        .With("ColumnName", columnName);

    return hierarchicalJoin;
}

////////////////////////////////////////////////////////////////////////////////

class TReplaceSubqueryWithReferenceRewriter
    : public TRewriter<TReplaceSubqueryWithReferenceRewriter>
{
public:
    TReplaceSubqueryWithReferenceRewriter(
        const TConstSubqueryExpressionPtr& targetSubquery,
        const TConstExpressionPtr& replacement)
        : TargetSubquery_(targetSubquery)
        , Replacement_(replacement)
    { }

    TConstExpressionPtr OnSubquery(const TSubqueryExpression* subquery)
    {
        if (subquery == TargetSubquery_.Get()) {
            return Replacement_;
        } else {
            return subquery;
        }
    }

private:
    const TConstSubqueryExpressionPtr TargetSubquery_;
    const TConstExpressionPtr Replacement_;
};

TConstExpressionPtr ReplaceSubqueryWithReference(
    const TConstExpressionPtr& expression,
    const TConstSubqueryExpressionPtr& subquery,
    const std::string& columnName)
{
    auto reference = New<TReferenceExpression>(subquery->LogicalType, columnName);
    TReplaceSubqueryWithReferenceRewriter rewriter(subquery, reference);
    return rewriter.Visit(expression);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TQueryPtr InsertHierarchicalJoins(
    const TQueryPtr& query,
    const NLogging::TLogger& Logger)
{
    YT_TLOG_DEBUG("Rewriting hierarchical joins");

    auto subqueriesToRewrite = DiscoverSubqueries(query);

    if (IsEmpty(subqueriesToRewrite)) {
        return query;
    }

    THROW_ERROR_EXCEPTION_IF(
        query->GroupClause &&
            query->GroupClause->TotalsMode != ETotalsMode::None &&
            !subqueriesToRewrite.InProjectClause.empty(),
        "Subquery with JOIN in projection after GROUP BY WITH TOTALS is not supported");

    ThrowOnUnimplementedSubqueryType(subqueriesToRewrite);

    i64 hierarchicalJoinIndex = 0;
    for (const auto& subquery : subqueriesToRewrite.InProjectClause) {
        auto columnName = Format("hierarchical_join_result_%v", hierarchicalJoinIndex++);
        auto hierarchicalJoin = BuildHierarchicalJoinFromSubquery(subquery, columnName, Logger);

        if (query->GroupClause) {
            query->HierarchicalJoinsAfterGroupBy.push_back(hierarchicalJoin);
        } else {
            query->HierarchicalJoinsBeforeGroupBy.push_back(hierarchicalJoin);
        }

        auto newProjectClause = New<TProjectClause>();
        for (const auto& projection : query->ProjectClause->Projections) {
            auto newExpression = ReplaceSubqueryWithReference(projection.Expression, subquery, columnName);
            newProjectClause->AddProjection(newExpression, projection.Name);
        }

        query->ProjectClause = newProjectClause;
    }

    for (const auto& subquery : subqueriesToRewrite.InGroupItems) {
        auto columnName = Format("hierarchical_join_result_%v", hierarchicalJoinIndex++);
        auto hierarchicalJoin = BuildHierarchicalJoinFromSubquery(subquery, columnName, Logger);

        query->HierarchicalJoinsBeforeGroupBy.push_back(hierarchicalJoin);

        auto newGroupClause = New<TGroupClause>(*query->GroupClause);
        for (auto& groupItem : newGroupClause->GroupItems) {
            groupItem.Expression = ReplaceSubqueryWithReference(groupItem.Expression, subquery, columnName);
        }
        query->GroupClause = newGroupClause;
    }

    for (const auto& subquery : subqueriesToRewrite.InAggregateArguments) {
        auto columnName = Format("hierarchical_join_result_%v", hierarchicalJoinIndex++);
        auto hierarchicalJoin = BuildHierarchicalJoinFromSubquery(subquery, columnName, Logger);

        query->HierarchicalJoinsBeforeGroupBy.push_back(hierarchicalJoin);

        auto newGroupClause = New<TGroupClause>(*query->GroupClause);
        for (auto& aggregateItem : newGroupClause->AggregateItems) {
            for (auto& argument : aggregateItem.Arguments) {
                argument = ReplaceSubqueryWithReference(argument, subquery, columnName);
            }
        }
        query->GroupClause = newGroupClause;
    }

    for (const auto& subquery : subqueriesToRewrite.InWhereClause) {
        auto columnName = Format("hierarchical_join_result_%v", hierarchicalJoinIndex++);
        auto hierarchicalJoin = BuildHierarchicalJoinFromSubquery(subquery, columnName, Logger);

        query->HierarchicalJoinsInWhereClause.push_back(hierarchicalJoin);

        query->WhereClause = ReplaceSubqueryWithReference(query->WhereClause, subquery, columnName);
    }

    return query;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
