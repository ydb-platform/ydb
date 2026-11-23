#include "push_down_group_by.h"
#include "ast_visitors.h"
#include "query_visitors.h"

namespace NYT::NQueryClient {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

struct TSubqueryDetector
    : public NAst::TAstVisitor<TSubqueryDetector>
{
    bool Found = false;

    void OnAlias(const NAst::TAliasExpressionPtr expression)
    {
        Visit(expression->Expression);
    }

    void OnReference(const NAst::TReferenceExpressionPtr expression)
    {
        Visit(expression->Reference.CompositeTypeAccessor.DictOrListItemAccessor);
    }

    void OnQuery(const NAst::TQueryExpressionPtr /*expression*/)
    {
        Found = true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPurityChecker
    : public TVisitor<TPurityChecker>
{
public:
    explicit TPurityChecker(const TTableSchema& schema)
        : Schema_(schema)
    { }

    bool Check(const TConstExpressionPtr& expression)
    {
        IsPure_ = true;
        Visit(expression);
        return IsPure_;
    }

    void OnReference(const TReferenceExpression* referenceExpr)
    {
        // This eliminates call FindColumn if IsPure_ is already false.
        IsPure_ = IsPure_ && Schema_.FindColumn(referenceExpr->ColumnName);
    }

private:
    const TTableSchema& Schema_;
    bool IsPure_ = true;
};

////////////////////////////////////////////////////////////////////////////////

THashSet<std::string> FilterWithRespectToSchema(
    const THashSet<std::string>& columns,
    const TTableSchema& schema)
{
    THashSet<std::string> filteredColumns;

    for (const auto& columnName : columns) {
        if (schema.FindColumn(columnName)) {
            filteredColumns.insert(columnName);
        }
    }

    return filteredColumns;
}

std::pair<TAggregateItemList, TAggregateItemList> GetPureAndMixedAggregateItems(
    const TGroupClause& groupByClause,
    const TTableSchema& joinRenamedSchema,
    TReferenceHarvester* harvester)
{
    TAggregateItemList pureAggregateItems;
    TAggregateItemList mixedAggregateItems;

    auto purityChecker = TPurityChecker(joinRenamedSchema);

    auto isAggregateFunctionGood = [] (TStringBuf function) {
        return function == "first" || function == "min" || function == "max" || function == "sum";
    };

    for (const auto& aggregate : groupByClause.AggregateItems) {
        THROW_ERROR_EXCEPTION_IF(!isAggregateFunctionGood(aggregate.AggregateFunction),
            "Aggregate function %Qv is not supported for Group By Push Down",
            aggregate.AggregateFunction);

        YT_VERIFY(aggregate.Arguments.size() == 1);
        const auto& argument = aggregate.Arguments[0];

        if (purityChecker.Check(argument)) {
            pureAggregateItems.push_back(TAggregateItem(
                {argument},
                aggregate.AggregateFunction,
                aggregate.Name,
                aggregate.ResultType,
                aggregate.ResultType));
        } else {
            THROW_ERROR_EXCEPTION_IF(aggregate.AggregateFunction == "sum",
                "Aggregate function %Qv is neither idempotent nor can be pushed behind join clause",
                aggregate.AggregateFunction);

            harvester->Visit(argument);
            mixedAggregateItems.push_back(aggregate);
        }
    }

    return {std::move(pureAggregateItems), std::move(mixedAggregateItems)};
}

bool IsGroupKeyTooSelective(const TNamedItemList& groupKey, const TJoinClause& joinClause)
{
    const auto& originalSchema = joinClause.Schema.Original;

    THashMap<std::string, int> nameToIndex;
    for (auto& descriptor : joinClause.Schema.GetOrderedSchemaMapping()) {
        InsertOrCrash(nameToIndex, std::pair{std::move(descriptor.Name), descriptor.Index});
    }

    auto keyPresenceMap = std::vector<bool>(originalSchema->GetKeyColumnCount(), false);
    for (const auto& item : groupKey) {
        if (const auto* reference = item.Expression->As<TReferenceExpression>()) {
            auto index = GetOrCrash(nameToIndex, reference->ColumnName);
            if (index < originalSchema->GetKeyColumnCount()) {
                keyPresenceMap[index] = true;
            }
        }
    }

    for (int index = 0; index < originalSchema->GetKeyColumnCount(); ++index) {
        if (originalSchema->Columns()[index].Expression()) {
            continue;
        }
        if (!keyPresenceMap[index]) {
            return false;
        }
    }

    return true;
}

std::pair<TJoinClausePtr, TGroupClausePtr> MakeGroupAndJoinClauses(
    const TGroupClause& groupByClause,
    const TJoinClause& joinClause,
    const TTableSchema& joinRenamedSchema)
{
    auto newJoinClause = New<TJoinClause>(joinClause);
    auto newGroupClause = New<TGroupClause>(groupByClause);
    auto pushedGroupClause = New<TGroupClause>();
    newJoinClause->GroupClause = pushedGroupClause;

    TAggregateItemList aggregateItems;

    auto commonPrefixWithPrimaryKey = joinClause.ForeignKeyPrefix;

    auto foreignJoinedColumns = THashSet<std::string>();
    auto referenceHarvester = TReferenceHarvester(&foreignJoinedColumns);

    for (const auto& groupKeyPart : groupByClause.GroupItems) {
        referenceHarvester.Visit(groupKeyPart.Expression);
    }

    auto [pureAggregates, mixedAggregates] = GetPureAndMixedAggregateItems(
        groupByClause,
        joinRenamedSchema,
        &referenceHarvester);

    foreignJoinedColumns = FilterWithRespectToSchema(foreignJoinedColumns, joinRenamedSchema);
    newJoinClause->ForeignJoinedColumns = foreignJoinedColumns;

    TNamedItemList groupItems;
    for (const auto& eqRhs : joinClause.ForeignEquations) {
        groupItems.push_back(TNamedItem(eqRhs, InferName(eqRhs)));
        if (const auto* reference = eqRhs->As<TReferenceExpression>()) {
            foreignJoinedColumns.erase(reference->ColumnName);
        }
    }

    for (const auto& columnName : foreignJoinedColumns) {
        const auto& column = joinRenamedSchema.GetColumn(columnName);
        groupItems.push_back(TNamedItem(
            New<TReferenceExpression>(column.LogicalType(), columnName),
            columnName));
    }

    if (IsGroupKeyTooSelective(groupItems, joinClause)) {
        return {};
    }

    newGroupClause->AggregateItems = std::move(mixedAggregates);
    for (const auto& aggregate : pureAggregates) {
        newGroupClause->AggregateItems.push_back(TAggregateItem(
            {
                New<TReferenceExpression>(
                    aggregate.ResultType,
                    aggregate.Name),
            },
            aggregate.AggregateFunction,
            aggregate.Name,
            aggregate.ResultType,
            aggregate.ResultType));
    }

    auto emptySchema = TTableSchema();
    TPurityChecker isConstant(emptySchema);

    while (groupItems.size() > 1) {
        auto it = std::find_if(groupItems.begin(), groupItems.end(), [&] (const TNamedItem& item) {
            return isConstant.Check(item.Expression);
        });

        if (it == groupItems.end()) {
            break;
        }

        groupItems.erase(it);
    }

    pushedGroupClause->GroupItems = std::move(groupItems);
    pushedGroupClause->AggregateItems = std::move(pureAggregates);
    pushedGroupClause->TotalsMode = ETotalsMode::None;
    pushedGroupClause->CommonPrefixWithPrimaryKey = commonPrefixWithPrimaryKey;

    return {std::move(newJoinClause), std::move(newGroupClause)};
}

////////////////////////////////////////////////////////////////////////////////

std::string MakeDebugString(const TGroupClause& groupClause)
{
    return Format("(GroupKey: %v, Aggregates: %v, TotalsMode: %Qlv, CommonPrefixWithPrimaryKey: %v)",
        MakeFormattableView(groupClause.GroupItems, [] (TStringBuilderBase* builder, const TNamedItem& item) {
            builder->AppendFormat("%v AS %v", InferName(item.Expression), item.Name);
        }),
        MakeFormattableView(groupClause.AggregateItems, [] (TStringBuilderBase* builder, const TAggregateItem& item) {
            builder->AppendString(item.AggregateFunction);
            builder->AppendFormat(
                "%v AS %v",
                MakeFormattableView(
                    item.Arguments,
                    [] (TStringBuilderBase* builder, const TConstExpressionPtr& expr) {
                        builder->AppendString(InferName(expr));
                    }),
                item.Name);
        }),
        groupClause.TotalsMode,
        groupClause.CommonPrefixWithPrimaryKey);
}

std::string MakeDebugString(const TJoinClause& joinClause)
{
    YT_VERIFY(joinClause.GroupClause);

    return Format(
        "(Schema: %v, Mapping: %v, SelfJoinedColumns: %v, ForeignJoinedColumns: %v, "
        "Predicate: %v, SelfEquations: %v, ForeignEquations: %v, CommonKeyPrefix: %v, "
        "ForeignKeyPrefix: %v, IsLeft: %v, ForeignObjectId: %v, ForeignCellId: %v, "
        "GroupClause: %v)",
        joinClause.Schema.Original,
        MakeFormattableView(joinClause.Schema.Mapping, [] (TStringBuilderBase* builder, const TColumnDescriptor& column) {
            builder->AppendFormat("[%v] as %v", column.Index, column.Name);
        }),
        joinClause.SelfJoinedColumns,
        joinClause.ForeignJoinedColumns,
        InferName(joinClause.Predicate),
        MakeFormattableView(joinClause.SelfEquations, [] (TStringBuilderBase* builder, const TConstExpressionPtr& lhs) {
            builder->AppendString(InferName(lhs));
        }),
        MakeFormattableView(joinClause.ForeignEquations, [] (TStringBuilderBase* builder, const TConstExpressionPtr& rhs) {
            builder->AppendString(InferName(rhs));
        }),
        joinClause.CommonKeyPrefix,
        joinClause.ForeignKeyPrefix,
        joinClause.IsLeft,
        joinClause.ForeignObjectId,
        joinClause.ForeignCellId,
        MakeDebugString(*joinClause.GroupClause));
}

////////////////////////////////////////////////////////////////////////////////

int GetPushDownJoinCount(const NAst::TQuery& ast)
{
    int count = 0;

    for (int index = 0; index < std::ssize(ast.Joins); ++index) {
        if (auto* tableJoin = std::get_if<NAst::TJoin>(&ast.Joins[index]);
            tableJoin && tableJoin->Table.Hint.PushDownGroupBy)
        {
            count++;
        }
    }
    return count;
}

void TryPushDownGroupBy(const TQueryPtr& query, const NAst::TQuery& ast, const TLogger& Logger)
{
    YT_VERIFY(query->JoinClauses.size() == ast.Joins.size());

    if (ast.Joins.empty()) {
        return;
    }

    auto pushDownGroupByJoinCount = GetPushDownJoinCount(ast);
    if (pushDownGroupByJoinCount == 0) {
        return;
    }

    THROW_ERROR_EXCEPTION_IF(query->JoinClauses.size() > 1,
        "\"push_down_group_by\" with multiple join clauses is not supported at the current moment");

    if (!query->GroupClause) {
        THROW_ERROR_EXCEPTION("Found \"push_down_group_by\" hint, but no group clause");
    }

    auto subqueryDetector = TSubqueryDetector();
    subqueryDetector.Visit(ast.SelectExprs);
    subqueryDetector.Visit(ast.WherePredicate);
    subqueryDetector.Visit(ast.GroupExprs);
    subqueryDetector.Visit(ast.HavingPredicate);
    for (const auto& orderExpression : ast.OrderExpressions) {
        subqueryDetector.Visit(orderExpression.Expressions);
    }

    const auto& tableJoin = std::get<NAst::TJoin>(ast.Joins[0]);
    subqueryDetector.Visit(tableJoin.Lhs);
    subqueryDetector.Visit(tableJoin.Rhs);
    subqueryDetector.Visit(tableJoin.Predicate);
    if (subqueryDetector.Found) {
        YT_TLOG_DEBUG("\"push_down_group_by\" disabled for scalar subqueries");
        return;
    }

    const auto& joinClause = query->JoinClauses[0];

    auto joinRenamedSchema = joinClause->Schema.GetRenamedSchema();

    auto [newJoinClause, newGroupClause] = MakeGroupAndJoinClauses(
        *query->GroupClause,
        *joinClause,
        *joinRenamedSchema);

    if (!newJoinClause) {
        YT_TLOG_DEBUG("\"push_down_group_by\" considered ineffective");
        return;
    }

    YT_TLOG_DEBUG("\"push_down_group_by\" complete")
        .With("JoinClause", MakeDebugString(*newJoinClause))
        .With("GroupByClause", MakeDebugString(*newGroupClause));

    query->JoinClauses[0] = newJoinClause;

    query->GroupClause = newGroupClause;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
