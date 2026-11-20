#include "query_preparer.h"

#include "ast_visitors.h"
#include "callbacks.h"
#include "functions.h"
#include "helpers.h"
#include "hierarchical_join_rewriter.h"
#include "lexer.h"
#include "private.h"
#include "push_down_group_by.h"
#include "query_helpers.h"
#include "query_visitors.h"
#include "expr_builder_base.h"

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/misc/variant.h>

#include <unordered_set>

namespace NYT::NQueryClient {

using namespace NCodegen;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxJoinNumber = 200;
constexpr int MaxMultiJoinGroupNumber = 15;

EValueType ComparableTypes[] = {
    EValueType::Boolean,
    EValueType::Int64,
    EValueType::Uint64,
    EValueType::Double,
    EValueType::String,
    EValueType::Any,
    EValueType::Composite,
};

////////////////////////////////////////////////////////////////////////////////

namespace {

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<std::string>* functions);

void ExtractFunctionNames(
    const NAst::TWhenThenExpressionList& exprs,
    std::vector<std::string>* functions);

void ExtractFunctionNames(
    const NAst::TExpressionPtr& expr,
    std::vector<std::string>* functions)
{
    if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
        functions->push_back(ToLower(functionExpr->FunctionName));
        ExtractFunctionNames(functionExpr->Arguments, functions);
    } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
        ExtractFunctionNames(unaryExpr->Operand, functions);
    } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
        ExtractFunctionNames(binaryExpr->Lhs, functions);
        ExtractFunctionNames(binaryExpr->Rhs, functions);
    } else if (auto inExpr = expr->As<NAst::TInExpression>()) {
        ExtractFunctionNames(inExpr->Expr, functions);
    } else if (auto betweenExpr = expr->As<NAst::TBetweenExpression>()) {
        ExtractFunctionNames(betweenExpr->Expr, functions);
    } else if (auto transformExpr = expr->As<NAst::TTransformExpression>()) {
        ExtractFunctionNames(transformExpr->Expr, functions);
        ExtractFunctionNames(transformExpr->DefaultExpr, functions);
    } else if (auto caseExpr = expr->As<NAst::TCaseExpression>()) {
        ExtractFunctionNames(caseExpr->OptionalOperand, functions);
        ExtractFunctionNames(caseExpr->WhenThenExpressions, functions);
        ExtractFunctionNames(caseExpr->DefaultExpression, functions);
    } else if (auto likeExpr = expr->As<NAst::TLikeExpression>()) {
        ExtractFunctionNames(likeExpr->Text, functions);
        ExtractFunctionNames(likeExpr->Pattern, functions);
        ExtractFunctionNames(likeExpr->EscapeCharacter, functions);
    } else if (expr->As<NAst::TLiteralExpression>()) {
    } else if (expr->As<NAst::TReferenceExpression>()) {
    } else if (auto aliasExpr = expr->As<NAst::TAliasExpression>()) {
        ExtractFunctionNames(aliasExpr->Expression, functions);
    } else if (auto queryExpr = expr->As<NAst::TQueryExpression>()) {
        ExtractFunctionNames(queryExpr->Query.WherePredicate, functions);
        ExtractFunctionNames(queryExpr->Query.SelectExprs, functions);
    } else {
        YT_ABORT();
    }
}

void ExtractFunctionNames(
    const NAst::TNullableExpressionList& exprs,
    std::vector<std::string>* functions)
{
    if (!exprs) {
        return;
    }

    CheckStackDepth();

    for (const auto& expr : *exprs) {
        ExtractFunctionNames(expr, functions);
    }
}

void ExtractFunctionNames(
    const NAst::TWhenThenExpressionList& whenThenExpressions,
    std::vector<std::string>* functions)
{
    CheckStackDepth();

    for (const auto& [condition, result] : whenThenExpressions) {
        ExtractFunctionNames(condition, functions);
        ExtractFunctionNames(result, functions);
    }
}

std::vector<std::string> ExtractFunctionNames(
    const NAst::TQuery& query,
    const NAst::TAliasMap& aliasMap)
{
    std::vector<std::string> functions;

    ExtractFunctionNames(query.WherePredicate, &functions);
    ExtractFunctionNames(query.HavingPredicate, &functions);
    ExtractFunctionNames(query.SelectExprs, &functions);

    if (query.GroupExprs) {
        for (const auto& expr : *query.GroupExprs) {
            ExtractFunctionNames(expr, &functions);
        }
    }

    for (const auto& join : query.Joins) {
        Visit(join,
            [&] (const NAst::TJoin& tableJoin) {
                ExtractFunctionNames(tableJoin.Lhs, &functions);
                ExtractFunctionNames(tableJoin.Rhs, &functions);
            },
            [&] (const NAst::TArrayJoin& arrayJoin) {
                ExtractFunctionNames(arrayJoin.Columns, &functions);
            });
    }

    for (const auto& orderExpression : query.OrderExpressions) {
        for (const auto& expr : orderExpression.Expressions) {
            ExtractFunctionNames(expr, &functions);
        }
    }

    for (const auto& aliasedExpression : aliasMap) {
        ExtractFunctionNames(aliasedExpression.second, &functions);
    }

    Visit(query.FromClause,
        [&] (const NAst::TTableDescriptor& /*table*/) { },
        [&] (const NAst::TQueryAstHeadPtr& subquery) {
            auto extracted = ExtractFunctionNames(subquery->Ast, aliasMap);
            functions.insert(
                functions.end(),
                std::make_move_iterator(extracted.begin()),
                std::make_move_iterator(extracted.end()));
        },
        [&] (const NAst::TExpressionList& expressions) {
            ExtractFunctionNames(expressions, &functions);
        },
        [&] (const NAst::TUnionAllClause& unionAll) {
            for (const auto* subquery : unionAll.Subqueries) {
                auto extracted = ExtractFunctionNames(subquery->Ast, subquery->AliasMap);
                functions.insert(
                    functions.end(),
                    std::make_move_iterator(extracted.begin()),
                    std::make_move_iterator(extracted.end()));
            }
        });


    std::sort(functions.begin(), functions.end());
    functions.erase(
        std::unique(functions.begin(), functions.end()),
        functions.end());

    return functions;
}

////////////////////////////////////////////////////////////////////////////////

TConstExpressionPtr BuildPredicate(
    const NAst::TExpressionList& expressionAst,
    TExpressionBuilder* builder,
    TStringBuf name)
{
    if (expressionAst.size() != 1) {
        THROW_ERROR_EXCEPTION("Expecting scalar expression")
            .With("source", FormatExpression(expressionAst));
    }

    // TODO(lukyan): BuildTypedExpression(expressionAst.front(), {EValueType::Boolean}) ?
    auto typedPredicate = builder->BuildTypedExpression(expressionAst.front());

    auto actualType = typedPredicate->GetWireType();
    EValueType expectedType(EValueType::Boolean);
    if (actualType != expectedType) {
        THROW_ERROR_EXCEPTION("%v is not a boolean expression", name)
            .With("source", expressionAst.front()->GetSource(builder->GetSource()))
            .With("actual_type", actualType)
            .With("expected_type", expectedType);
    }

    return ApplyRewriters(typedPredicate);
}

TGroupClausePtr BuildGroupClause(
    const NAst::TExpressionList& expressionsAst,
    ETotalsMode totalsMode,
    TExpressionBuilder* builder)
{
    auto groupClause = New<TGroupClause>();
    groupClause->TotalsMode = totalsMode;

    for (const auto& expressionAst : expressionsAst) {
        auto typedExpr = builder->BuildTypedExpression(expressionAst, ComparableTypes);
        groupClause->AddGroupItem(ApplyRewriters(typedExpr), builder->InferGroupItemName(typedExpr, *expressionAst));
    }

    builder->SetGroupData(
        &groupClause->GroupItems,
        &groupClause->AggregateItems,
        totalsMode);

    return groupClause;
}

void DropLimitClauseWhenGroupByOne(const TQueryPtr& query)
{
    if (!query->OrderClause &&
        query->GroupClause &&
        std::ssize(query->GroupClause->GroupItems) == 1 &&
        query->GroupClause->GroupItems[0].Expression->As<TLiteralExpression>() &&
        query->Limit &&
        query->Offset == 0 &&
        *query->Limit != 0)
    {
        query->Limit = std::nullopt;
        query->ScanOrder = EScanOrder::Unordered;
    }
}

void PrepareQuery(
    const TQueryPtr& query,
    const NAst::TQuery& ast,
    TExpressionBuilder* builder,
    bool mergeVersionedRows = true)
{
    if (ast.WherePredicate) {
        auto wherePredicate = BuildPredicate(*ast.WherePredicate, builder, "WHERE-clause");
        query->WhereClause = IsTrue(wherePredicate) ? nullptr : wherePredicate;
    }

    if (ast.GroupExprs) {
        auto groupClause = BuildGroupClause(*ast.GroupExprs, ast.TotalsMode, builder);

        auto keyColumns = query->GetKeyColumns();

        TNamedItemList groupItems = std::move(groupClause->GroupItems);

        std::vector<int> touchedKeyColumns(keyColumns.size(), -1);
        for (int index = 0; index < std::ssize(groupItems); ++index) {
            const auto& item = groupItems[index];
            if (auto referenceExpr = item.Expression->As<TReferenceExpression>()) {
                int keyPartIndex = ColumnNameToKeyPartIndex(keyColumns, referenceExpr->ColumnName);
                if (keyPartIndex >= 0) {
                    touchedKeyColumns[keyPartIndex] = index;
                }
            }
        }

        size_t keyPrefix = 0;
        for (; keyPrefix < touchedKeyColumns.size(); ++keyPrefix) {
            if (touchedKeyColumns[keyPrefix] >= 0) {
                continue;
            }

            const auto& expression = query->Schema.Original->Columns()[keyPrefix].Expression();

            if (!expression) {
                break;
            }

            // Call PrepareExpression to extract references only.
            THashSet<std::string> references;
            PrepareExpression(*expression, *query->Schema.Original, builder->Functions(), &references);

            auto canEvaluate = true;
            for (const auto& reference : references) {
                int referenceIndex = query->Schema.Original->GetColumnIndexOrThrow(reference);
                if (touchedKeyColumns[referenceIndex] < 0) {
                    canEvaluate = false;
                }
            }

            if (!canEvaluate) {
                break;
            }
        }

        if (!mergeVersionedRows) {
            keyPrefix = 0;
        }

        touchedKeyColumns.resize(keyPrefix);
        for (int index : touchedKeyColumns) {
            if (index >= 0) {
                groupClause->GroupItems.push_back(std::move(groupItems[index]));
            }
        }

        groupClause->CommonPrefixWithPrimaryKey = groupClause->GroupItems.size();

        for (auto& item : groupItems) {
            if (item.Expression) {
                groupClause->GroupItems.push_back(std::move(item));
            }
        }

        query->GroupClause = groupClause;

        // not prefix, because of equal prefixes near borders
        bool containsPrimaryKey = keyPrefix == query->GetKeyColumns().size();
        // COMPAT(lukyan)
        query->UseDisjointGroupBy = containsPrimaryKey && !keyColumns.empty();
    }

    if (ast.HavingPredicate) {
        if (!query->GroupClause) {
            THROW_ERROR_EXCEPTION("Expected GROUP BY before HAVING");
        }
        query->HavingClause = BuildPredicate(
            *ast.HavingPredicate,
            builder,
            "HAVING-clause");
    }

    if (!ast.OrderExpressions.empty()) {
        auto orderClause = New<TOrderClause>();

        for (const auto& orderExpr : ast.OrderExpressions) {
            for (const auto& expressionAst : orderExpr.Expressions) {
                auto typedExpr = builder->BuildTypedExpression(
                    expressionAst,
                    ComparableTypes);

                orderClause->OrderItems.push_back({ApplyRewriters(typedExpr), orderExpr.Descending});
            }
        }

        bool canOmitOrderBy = CanOmitOrderBy(0, orderClause->OrderItems, query->GetKeyColumns());
        if (!canOmitOrderBy) {
            query->OrderClause = std::move(orderClause);
            query->ScanOrder = EScanOrder::Unordered;
        }

        bool canReverseScanForOrderBy = false;
        if (!canOmitOrderBy &&
            builder->GetContext().Options.AllowReverseScanForOrderBy)
        {
            canReverseScanForOrderBy = CanReverseScanForOrderBy(
                /*fixedKeyPrefix*/ 0,
                query->OrderClause->OrderItems,
                query->GetKeyColumns(),
                query->GroupClause);
        }

        if (canReverseScanForOrderBy) {
            query->ScanOrder = EScanOrder::Reversed;
        }
    } else if (!ast.Limit) {
        // COMPAT(sabdenovch): For queries with LIMIT the scan order is resolved
        // in PreparePlanFragmentImpl, after the group clause (including aggregate
        // items), the limit clause and the group-by push-down are final.
        query->ScanOrder = EScanOrder::Unordered;
    }

    if (ast.SelectExprs) {
        auto projectClause = New<TProjectClause>();
        for (const auto& expressionAst : *ast.SelectExprs) {
            auto typedExpr = builder->BuildTypedExpression(expressionAst);

            projectClause->AddProjection(ApplyRewriters(typedExpr), InferColumnName(*expressionAst));
        }

        query->ProjectClause = projectClause;
    } else {
        // Select all columns.
        builder->PopulateAllColumns();
    }

    builder->Finish();
}

////////////////////////////////////////////////////////////////////////////////

class TYsonToQueryExpressionConvertVisitor
    : public TYsonConsumerBase
{
public:
    explicit TYsonToQueryExpressionConvertVisitor(TStringBuilder* builder)
        : Builder_(builder)
    { }

    void OnStringScalar(TStringBuf value) override
    {
        Builder_->AppendChar('"');
        Builder_->AppendString(EscapeC(value));
        Builder_->AppendChar('"');
    }

    void OnInt64Scalar(i64 value) override
    {
        Builder_->AppendFormat("%v", value);
    }

    void OnUint64Scalar(ui64 value) override
    {
        Builder_->AppendFormat("%vu", value);
    }

    void OnDoubleScalar(double value) override
    {
        Builder_->AppendString(NAst::FormatLiteralValue(value));
    }

    void OnBooleanScalar(bool value) override
    {
        Builder_->AppendFormat("%lv", value);
    }

    void OnEntity() override
    {
        Builder_->AppendString("null");
    }

    void OnBeginList() override
    {
        Builder_->AppendChar('(');
        InListBeginning_ = true;
    }

    void OnListItem() override
    {
        if (!InListBeginning_) {
            Builder_->AppendString(", ");
        }
        InListBeginning_ = false;
    }

    void OnEndList() override
    {
        Builder_->AppendChar(')');
    }

    void OnBeginMap() override
    {
        THROW_ERROR_EXCEPTION("Maps inside YSON placeholder are not allowed");
    }

    void OnKeyedItem(TStringBuf) override
    {
        THROW_ERROR_EXCEPTION("Maps inside YSON placeholder are not allowed");
    }

    void OnEndMap() override
    {
        THROW_ERROR_EXCEPTION("Maps inside YSON placeholder are not allowed");
    }

    void OnBeginAttributes() override
    {
        THROW_ERROR_EXCEPTION("Attributes inside YSON placeholder are not allowed");
    }

    void OnEndAttributes() override
    {
        THROW_ERROR_EXCEPTION("Attributes inside YSON placeholder are not allowed");
    }

private:
    TStringBuilder* Builder_;
    bool InListBeginning_;
};

void YsonParseError(TStringBuf message, TYsonStringBuf source)
{
    THROW_ERROR_EXCEPTION("%v", message)
        .With("context", Format("%v", source.AsStringBuf()));
}

THashMap<std::string, std::string> ConvertYsonPlaceholdersToQueryLiterals(TYsonStringBuf placeholders)
{
    TMemoryInput input{placeholders.AsStringBuf()};
    TYsonPullParser ysonParser{&input, EYsonType::Node};
    TYsonPullParserCursor ysonCursor{&ysonParser};

    if (ysonCursor->GetType() != EYsonItemType::BeginMap) {
        YsonParseError("Incorrect placeholder argument: YSON map expected", placeholders);
    }

    ysonCursor.Next();

    THashMap<std::string, std::string> queryLiterals;
    while (ysonCursor->GetType() != EYsonItemType::EndMap) {
        if (ysonCursor->GetType() != EYsonItemType::StringValue) {
            YsonParseError("Incorrect YSON map placeholder: keys should be strings", placeholders);
        }
        auto key = std::string(ysonCursor->UncheckedAsString());

        ysonCursor.Next();
        switch (ysonCursor->GetType()) {
            case EYsonItemType::EntityValue:
            case EYsonItemType::BooleanValue:
            case EYsonItemType::Int64Value:
            case EYsonItemType::Uint64Value:
            case EYsonItemType::DoubleValue:
            case EYsonItemType::StringValue:
            case EYsonItemType::BeginList: {
                TStringBuilder valueBuilder;
                TYsonToQueryExpressionConvertVisitor ysonValueTransferrer{&valueBuilder};
                ysonCursor.TransferComplexValue(&ysonValueTransferrer);
                queryLiterals.emplace(std::move(key), valueBuilder.Flush());
                break;
            }
            default:
                YsonParseError("Incorrect placeholder map: values should be plain types or lists", placeholders);
        }
    }

    return queryLiterals;
}

NAst::TAstHead ParseQueryString(
    TStringBuf source,
    NAst::TParser::token::yytokentype strayToken,
    TYsonStringBuf placeholderValues = {},
    int syntaxVersion = 1)
{
    auto head = NAst::TAstHead();

    THashMap<std::string, std::string> queryLiterals;
    if (placeholderValues) {
        queryLiterals = ConvertYsonPlaceholdersToQueryLiterals(placeholderValues);
    }

    NAst::TLexer lexer(source, strayToken, std::move(queryLiterals), syntaxVersion);
    std::stack<NAst::TAliasMap> aliasMapStack;
    aliasMapStack.push({});
    NAst::TParser parser(lexer, &head, source, std::move(aliasMapStack));

    int result = parser.parse();

    if (result != 0) {
        THROW_ERROR_EXCEPTION("Parse failure")
            .With("source", source);
    }

    return head;
}

////////////////////////////////////////////////////////////////////////////////

NAst::TParser::token::yytokentype GetStrayToken(EParseMode mode)
{
    switch (mode) {
        case EParseMode::Query:      return NAst::TParser::token::StrayWillParseQuery;
        case EParseMode::JobQuery:   return NAst::TParser::token::StrayWillParseJobQuery;
        case EParseMode::Expression: return NAst::TParser::token::StrayWillParseExpression;
        default:                     YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

void EliminateRedundantProjections(const TQueryPtr& innerSubquery, const TTableSchema& outerQueryReadSchema)
{
    auto projectClause = New<TProjectClause>();

    if (!innerSubquery->ProjectClause) {
        for (const auto& column : outerQueryReadSchema.Columns()) {
            projectClause->Projections.push_back(TNamedItem(
                New<TReferenceExpression>(column.LogicalType(), column.Name()),
                column.Name()));
        }
    } else {
        TColumnSet readColumns;
        for (const auto& column : outerQueryReadSchema.Columns()) {
            readColumns.insert(column.Name());
        }

        TNamedItemList filteredProjections;
        for (const auto& projection : innerSubquery->ProjectClause->Projections) {
            if (readColumns.contains(projection.Name)) {
                filteredProjections.push_back(projection);
            }
        }

        projectClause->Projections = std::move(filteredProjections);
    }

    innerSubquery->ProjectClause = std::move(projectClause);
}

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr MakeColumnTypesOptional(const TTableSchemaPtr& schema)
{
    std::vector<TColumnSchema> columns = schema->Columns();

    for (auto& column : columns) {
        if (!column.LogicalType()->IsNullable()) {
            column.SetLogicalType(OptionalLogicalType(column.LogicalType()));
        }
    }

    return New<TTableSchema>(columns,
        schema->IsStrict(),
        schema->IsUniqueKeys(),
        schema->GetSchemaModification(),
        schema->DeletedColumns());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void DefaultFetchFunctions(
    TRange<std::string> /*names*/,
    const TTypeInferrerMapPtr& typeInferrers,
    EExecutionBackend /*executionBackend*/)
{
    MergeFrom(typeInferrers.Get(), *GetBuiltinTypeInferrers());
}

////////////////////////////////////////////////////////////////////////////////

TParsedSource::TParsedSource(TStringBuf source, NAst::TAstHead astHead)
    : Source(source)
    , AstHead(std::move(astHead))
{ }

std::unique_ptr<TParsedSource> ParseSource(
    TStringBuf source,
    EParseMode mode,
    TYsonStringBuf placeholderValues,
    int syntaxVersion)
{
    return std::make_unique<TParsedSource>(
        source,
        ParseQueryString(
            source,
            GetStrayToken(mode),
            placeholderValues,
            syntaxVersion));
}

////////////////////////////////////////////////////////////////////////////////

THashMap<std::string, int> BuildReferenceToIndexMap(const std::vector<TConstExpressionPtr>& equations)
{
    THashMap<std::string, int> map;

    for (int index = 0; index < std::ssize(equations); ++index) {
        auto& expression = equations[index];
        if (auto* ref = expression->As<TReferenceExpression>()) {
            auto [_, inserted] = map.insert(std::pair(ref->ColumnName, index));
            THROW_ERROR_EXCEPTION_IF(!inserted, "Foreign key column %Qv occurs more than once in a join clause",
                ref->ColumnName);
        }
    }

    return map;
}

std::vector<std::pair<TConstExpressionPtr, int>> MakeExpressionsFromComputedColumns(
    const TTableSchemaPtr& schema,
    const TConstTypeInferrerMapPtr& functions,
    const std::optional<std::string>& alias)
{
    std::vector<std::pair<TConstExpressionPtr, int>> expressionsAndColumnIndices;

    for (int index = 0; index < schema->GetColumnCount(); ++index) {
        const auto& column = schema->Columns()[index];
        if (!column.Expression()) {
            continue;
        }

        auto evaluatedColumnExpression = PrepareExpression(
            *column.Expression(),
            *schema,
            functions);

        if (alias) {
            TAddAliasRewriter addAliasRewriter{.Alias = alias};
            evaluatedColumnExpression = addAliasRewriter.Visit(evaluatedColumnExpression);
        }

        expressionsAndColumnIndices.push_back({std::move(evaluatedColumnExpression), index});
    }

    return expressionsAndColumnIndices;
}

std::unique_ptr<TExpressionBuilder> CreateExpressionBuilder(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap,
    const TPreparePlanFragmentContext& context,
    int expressionBuilderVersion)
{
    return (expressionBuilderVersion == 1
        ? &CreateExpressionBuilderV1
        : &CreateExpressionBuilderV2)(source, functions, aliasMap, context);
}

TJoinClausePtr BuildJoinClause(
    const TDataSplit& foreignDataSplit,
    const NAst::TJoin& tableJoin,
    TStringBuf source,
    const NAst::TAliasMap& aliasMap,
    const TConstTypeInferrerMapPtr& functions,
    size_t* globalCommonKeyPrefix,
    const TTableSchemaPtr& tableSchema,
    const std::optional<std::string>& tableAlias,
    TExpressionBuilder* builder,
    const TPreparePlanFragmentContext& context)
{
    const auto& Logger = context.Logger;
    const auto& options = context.Options;

    auto foreignTableSchema = foreignDataSplit.TableSchema;

    auto joinClause = New<TJoinClause>();
    joinClause->Schema.Original = foreignTableSchema;
    joinClause->ForeignObjectId = foreignDataSplit.ObjectId;
    joinClause->IsLeft = tableJoin.IsLeft;

    if (!tableJoin.Table.Hint.RequireSyncReplica && options.AllowJoinWithAsyncLastCommittedTimestampIfRequireSyncReplicaIsFalse) {
        joinClause->RequireSyncReplica = false;
    }

    // BuildPredicate and BuildTypedExpression are used with foreignBuilder.
    auto foreignBuilder = CreateExpressionBuilder(source, functions, aliasMap, context, options.BuilderVersion);

    foreignBuilder->AddTable({
        .Schema = *foreignTableSchema,
        .Alias = tableJoin.Table.Alias,
        .Mapping = &joinClause->Schema.Mapping,
    });

    std::vector<TConstExpressionPtr> selfEquations;
    selfEquations.reserve(tableJoin.Fields.size() + tableJoin.Lhs.size());
    std::vector<TConstExpressionPtr> foreignEquations;
    foreignEquations.reserve(tableJoin.Fields.size() + tableJoin.Rhs.size());

    THashSet<std::string> commonColumnNames;
    // Merge columns.
    for (const auto& referenceExpr : tableJoin.Fields) {
        auto columnName = InferColumnName(referenceExpr->Reference);
        commonColumnNames.insert(columnName);

        auto selfColumnType = builder->ResolveColumn(referenceExpr->Reference);
        auto foreignColumnType = foreignBuilder->ResolveColumn(referenceExpr->Reference);

        if (!selfColumnType || !foreignColumnType) {
            THROW_ERROR_EXCEPTION("Column %Qv not found",
                columnName);
        }

        if (!NTableClient::IsV1Type(selfColumnType) || !NTableClient::IsV1Type(foreignColumnType)) {
            THROW_ERROR_EXCEPTION("Cannot join column %Qv of nonsimple type",
                columnName)
                .With("self_type", selfColumnType)
                .With("foreign_type", foreignColumnType);
        }

        // N.B. When we try join optional<int32> and int16 columns it must work.
        if (NTableClient::GetWireType(selfColumnType) != NTableClient::GetWireType(foreignColumnType)) {
            THROW_ERROR_EXCEPTION("Column %Qv type mismatch in join",
                columnName)
                .With("self_type", selfColumnType)
                .With("foreign_type", foreignColumnType);
        }

        selfEquations.push_back(New<TReferenceExpression>(selfColumnType, columnName));
        foreignEquations.push_back(New<TReferenceExpression>(foreignColumnType, columnName));
    }

    for (const auto& argument : tableJoin.Lhs) {
        selfEquations.push_back(ApplyRewriters(builder->BuildTypedExpression(argument, ComparableTypes)));
    }
    for (const auto& argument : tableJoin.Rhs) {
        foreignEquations.push_back(ApplyRewriters(foreignBuilder->BuildTypedExpression(argument, ComparableTypes)));
    }

    if (selfEquations.size() != foreignEquations.size()) {
        THROW_ERROR_EXCEPTION("Tuples of same size are expected but got %v vs %v",
            selfEquations.size(),
            foreignEquations.size())
            .With("lhs_source", FormatExpression(tableJoin.Lhs))
            .With("rhs_source", FormatExpression(tableJoin.Rhs));
    }

    for (int index = 0; index < std::ssize(selfEquations); ++index) {
        if (selfEquations[index]->GetWireType() != foreignEquations[index]->GetWireType()) {
            THROW_ERROR_EXCEPTION("Types mismatch in join equation \"%v = %v\"",
                InferName(selfEquations[index]),
                InferName(foreignEquations[index]))
                .With("self_type", selfEquations[index]->LogicalType)
                .With("foreign_type", foreignEquations[index]->LogicalType);
        }
    }

    // If possible, use ranges, rearrange equations according to foreign key columns, enriching with evaluated columns
    size_t commonKeyPrefix = 0;
    size_t foreignKeyPrefix = 0;
    std::vector<TConstExpressionPtr> keySelfEquations;
    std::vector<TConstExpressionPtr> keyForeignEquations;
    THashSet<int> usedForKeyPrefixEquations;

    auto foreignReferenceToIndexMap = BuildReferenceToIndexMap(foreignEquations);
    auto selfComputedColumnsExpressions = MakeExpressionsFromComputedColumns(tableSchema, functions, tableAlias);

    for (const auto& foreignKeyColumn : foreignTableSchema->Columns()) {
        if (!foreignKeyColumn.SortOrder()) {
            break;
        }

        auto foreignKeyColumnReference = NAst::TReference(foreignKeyColumn.Name(), tableJoin.Table.Alias);
        auto aliasedForeignKeyColumnName = NAst::InferColumnName(foreignKeyColumnReference);
        auto it = foreignReferenceToIndexMap.find(aliasedForeignKeyColumnName);

        if (it != foreignReferenceToIndexMap.end()) {
            keySelfEquations.push_back(selfEquations[it->second]);
            keyForeignEquations.push_back(foreignEquations[it->second]);
            usedForKeyPrefixEquations.insert(it->second);
        } else if (foreignKeyColumn.Expression()) {
            auto evaluatedColumnExpression = PrepareExpression(
                *foreignKeyColumn.Expression(),
                *foreignTableSchema,
                functions);

            if (auto& alias = tableJoin.Table.Alias) {
                TAddAliasRewriter addAliasRewriter{.Alias = *alias};
                evaluatedColumnExpression = addAliasRewriter.Visit(evaluatedColumnExpression);
            }
            TSelfifyRewriter selfifyRewriter{
                .SelfEquations = selfEquations,
                .ForeignReferenceToIndexMap = foreignReferenceToIndexMap,
            };

            auto matchingSelfExpression = selfifyRewriter.Visit(evaluatedColumnExpression);
            if (!selfifyRewriter.Success) {
                break;
            }

            // The computedSelfEquation might already be among the computed key columns of the self schema.
            // e.g. JOIN equation (A.a, A.b, B.f) = (C.c, C.e, C.q) and table schemas are as follows:
            // A: [hash(a, b), a, b] and C: [hash(c, e), c, d, e]
            // In this case
            for (const auto& [expr, columnIndex] : selfComputedColumnsExpressions) {
                if (Compare(expr, matchingSelfExpression)) {
                    const auto& column = tableSchema->Columns()[columnIndex];
                    auto aliasedReference = NAst::TReference(column.Name(), tableAlias);

                    // Register self evaluated column in the effective schema.
                    builder->ResolveColumn(aliasedReference);

                    matchingSelfExpression = New<TReferenceExpression>(
                        column.LogicalType(),
                        NAst::InferColumnName(aliasedReference));
                    break;
                }
            }

            // Register foreign evaluated column in the effective schema.
            foreignBuilder->ResolveColumn(foreignKeyColumnReference);

            keySelfEquations.push_back(std::move(matchingSelfExpression));
            keyForeignEquations.push_back(New<TReferenceExpression>(
                foreignKeyColumn.LogicalType(),
                aliasedForeignKeyColumnName));
        } else {
            break;
        }

        if (commonKeyPrefix == foreignKeyPrefix &&
            static_cast<int>(commonKeyPrefix) < tableSchema->GetKeyColumnCount())
        {
            if (auto* reference = keySelfEquations.back()->As<TReferenceExpression>()) {
                auto aliasedName = NAst::InferColumnName(NAst::TReference(
                    tableSchema->Columns()[commonKeyPrefix].Name(),
                    tableAlias));
                if (reference->ColumnName == aliasedName) {
                    commonKeyPrefix++;
                }
            }
        }

        foreignKeyPrefix++;
    }

    joinClause->SelfEquations = std::move(keySelfEquations);
    joinClause->ForeignEquations = std::move(keyForeignEquations);
    for (int index = 0; index < std::ssize(selfEquations); ++index) {
        if (!usedForKeyPrefixEquations.contains(index)) {
            joinClause->SelfEquations.push_back(selfEquations[index]);
            joinClause->ForeignEquations.push_back(foreignEquations[index]);
        }
    }
    *globalCommonKeyPrefix = std::min(*globalCommonKeyPrefix, commonKeyPrefix);
    joinClause->ForeignKeyPrefix = foreignKeyPrefix;
    joinClause->CommonKeyPrefix = *globalCommonKeyPrefix;

    YT_TLOG_DEBUG("Creating join")
        .With("CommonKeyPrefix", joinClause->CommonKeyPrefix)
        .With("ForeignKeyPrefix", joinClause->ForeignKeyPrefix);

    if (tableJoin.Predicate) {
        joinClause->Predicate = BuildPredicate(
            *tableJoin.Predicate,
            foreignBuilder.get(),
            "JOIN-PREDICATE-clause");
    }

    auto schemaAfterJoin = tableJoin.IsLeft ? MakeColumnTypesOptional(foreignTableSchema) : foreignTableSchema;

    builder->AddTable({
        .Schema = *schemaAfterJoin,
        .Alias = tableJoin.Table.Alias,
        .Mapping = &joinClause->Schema.Mapping,
        .SelfJoinedColumns = &joinClause->SelfJoinedColumns,
        .ForeignJoinedColumns = &joinClause->ForeignJoinedColumns,
        .SharedColumns = commonColumnNames,
        .ModifiedSchemaHolder = std::move(schemaAfterJoin),
    });

    return joinClause;
}

TJoinClausePtr BuildArrayJoinClause(
    const NAst::TArrayJoin& arrayJoin,
    TStringBuf source,
    const NAst::TAliasMap& aliasMap,
    const TConstTypeInferrerMapPtr& functions,
    TExpressionBuilder* builder,
    int builderVersion)
{
    auto arrayJoinClause = New<TJoinClause>();
    arrayJoinClause->IsLeft = arrayJoin.IsLeft;

    int arrayCount = std::ssize(arrayJoin.Columns);

    TSchemaColumns nestedColumns(arrayCount);
    arrayJoinClause->ArrayExpressions.resize(arrayCount);
    for (int index = 0; index < arrayCount; ++index) {
        const auto& expr = arrayJoin.Columns[index];
        const auto* aliasExpression = expr->As<NAst::TAliasExpression>();
        YT_ASSERT(aliasExpression);

        const auto& typedExpression =
            arrayJoinClause->ArrayExpressions[index] =
                ApplyRewriters(builder->BuildTypedExpression(
                    aliasExpression->Expression,
                    {builderVersion == 1 ? EValueType::Any : EValueType::Composite}));

        auto logicalType = typedExpression->LogicalType;
        auto metatype = logicalType->GetMetatype();
        if (metatype == ELogicalMetatype::Optional) {
            logicalType = logicalType->UncheckedAsOptionalTypeRef().GetElement();
            metatype = logicalType->GetMetatype();
        }

        THROW_ERROR_EXCEPTION_IF(metatype != ELogicalMetatype::List,
            "Expected a list-like type expression in the ARRAY JOIN operator, got %v",
            *typedExpression->LogicalType);

        auto flattenedType = logicalType->UncheckedAsListTypeRef().GetElement();
        if ((arrayJoin.IsLeft || arrayCount > 1) && !flattenedType->IsNullable()) {
            flattenedType = OptionalLogicalType(std::move(flattenedType));
        }
        nestedColumns[index] = TColumnSchema(aliasExpression->Name, std::move(flattenedType));
    }

    arrayJoinClause->Schema.Original = New<TTableSchema>(std::move(nestedColumns));

    ValidateColumnUniqueness(*arrayJoinClause->Schema.Original);

    auto arrayBuilder = CreateExpressionBuilder(
        source,
        functions,
        aliasMap,
        builder->GetContext(),
        builderVersion);

    arrayBuilder->AddTable({
        .Schema = *arrayJoinClause->Schema.Original,
        .Alias = std::nullopt,
        .Mapping = &arrayJoinClause->Schema.Mapping,
    });

    for (const auto& nestedTableColumn : arrayJoinClause->Schema.Original->Columns()) {
        auto type = arrayBuilder->ResolveColumn(NAst::TReference(nestedTableColumn.Name()));
        YT_ASSERT(type);
    }

    if (arrayJoin.Predicate) {
        arrayJoinClause->Predicate = BuildPredicate(
            *arrayJoin.Predicate,
            arrayBuilder.get(),
            "JOIN-PREDICATE-clause");
    }

    builder->AddTable({
        .Schema = *arrayJoinClause->Schema.Original,
        .Alias = std::nullopt,
        .Mapping = &arrayJoinClause->Schema.Mapping,
        .SelfJoinedColumns = &arrayJoinClause->SelfJoinedColumns,
        .ForeignJoinedColumns = &arrayJoinClause->ForeignJoinedColumns,
    });

    return arrayJoinClause;
}

////////////////////////////////////////////////////////////////////////////////

// Default name for columns that don't have explicit name in SQL SELECT expressions for syntaxVersion>=3

std::string MakeColumnNameByIndex(int index)
{
    return Format("$projection_%v", index);
}

std::optional<i64> TryGetIntegerValue(NAst::TExpressionPtr expr)
{
    if (auto* literal = expr->As<NAst::TLiteralExpression>()) {
        if (std::holds_alternative<i64>(literal->Value)) {
            return std::get<i64>(literal->Value);
        }
    }

    if (auto* unary = expr->As<NAst::TUnaryOpExpression>()) {
        if (unary->Opcode == EUnaryOp::Minus) {
            if (auto* innerLiteral = unary->Operand[0]->As<NAst::TLiteralExpression>()) {
                if (std::holds_alternative<i64>(innerLiteral->Value)) {
                    return -std::get<i64>(innerLiteral->Value);
                }
            }
        }
    }

    return std::nullopt;
}

std::optional<i64> TryGetProjectionIndex(NAst::TExpressionPtr expr)
{
    auto index = TryGetIntegerValue(expr);
    if (!index || *index == 0) {
        return std::nullopt;
    }

    return index;
}

class TCardinalityIntoHyperLogLogWithPrecisionRewriter
    : public NAst::TRewriter<TCardinalityIntoHyperLogLogWithPrecisionRewriter>
{
public:
    TCardinalityIntoHyperLogLogWithPrecisionRewriter(
        TObjectsHolder* head,
        int precision)
        : NAst::TRewriter<TCardinalityIntoHyperLogLogWithPrecisionRewriter>(head)
        , Precision_(precision)
    { }

    NAst::TExpressionPtr OnFunction(NAst::TFunctionExpressionPtr functionExpr)
    {
        auto rewritten = NAst::TRewriter<TCardinalityIntoHyperLogLogWithPrecisionRewriter>::OnFunction(functionExpr)->As<NAst::TFunctionExpression>();
        auto name = rewritten->FunctionName;

        if (name == "cardinality") {
            return Head->New<NAst::TFunctionExpression>(
                NullSourceLocation,
                Format("hll_%v", Precision_),
                rewritten->Arguments);
        } else if (name == "cardinality_state") {
            return Head->New<NAst::TFunctionExpression>(
                NullSourceLocation,
                Format("hll_%v_state", Precision_),
                rewritten->Arguments);
        } else if (name == "cardinality_merge") {
            return Head->New<NAst::TFunctionExpression>(
                NullSourceLocation,
                Format("hll_%v_merge", Precision_),
                rewritten->Arguments);
        } else if (name == "cardinality_merge_state") {
            return Head->New<NAst::TFunctionExpression>(
                NullSourceLocation,
                Format("hll_%v_merge_state", Precision_),
                rewritten->Arguments);
        } else {
            return rewritten;
        }
    }

private:
    const int Precision_;
};

void RewriteCardinalityIntoHyperLogLogWithPrecision(
    NAst::TQuery* ast,
    NAst::TAliasMap* aliasMap,
    TObjectsHolder* holder,
    int hyperLogLogPrecision)
{
    auto rewriter = TCardinalityIntoHyperLogLogWithPrecisionRewriter(holder, hyperLogLogPrecision);

    if (auto* subquery = std::get_if<NAst::TQueryAstHeadPtr>(&ast->FromClause); subquery) {
        RewriteCardinalityIntoHyperLogLogWithPrecision(
            &(*subquery)->Ast,
            &(*subquery)->AliasMap,
            holder,
            hyperLogLogPrecision);
    }

    if (auto* unionAll = std::get_if<NAst::TUnionAllClause>(&ast->FromClause); unionAll) {
        for (auto* subquery : unionAll->Subqueries) {
            RewriteCardinalityIntoHyperLogLogWithPrecision(
                &subquery->Ast,
                &subquery->AliasMap,
                holder,
                hyperLogLogPrecision);
        }
    }

    if (auto* expressions = std::get_if<NAst::TExpressionList>(&ast->FromClause); expressions) {
        for (auto& item : *expressions) {
            item = rewriter.Visit(item);
        }
    }

    for (auto& [key, value] : *aliasMap) {
        value = rewriter.Visit(value);
    }

    if (ast->SelectExprs) {
        ast->SelectExprs = rewriter.Visit(ast->SelectExprs);
    }

    if (ast->WherePredicate) {
        ast->WherePredicate = rewriter.Visit(ast->WherePredicate);
    }

    if (ast->GroupExprs) {
        ast->GroupExprs = rewriter.Visit(ast->GroupExprs);
    }

    if (ast->HavingPredicate) {
        ast->HavingPredicate = rewriter.Visit(ast->HavingPredicate);
    }

    for (auto& item : ast->OrderExpressions) {
        item.Expressions = rewriter.Visit(item.Expressions);
    }
}

namespace {

void RewriteIntegerIndicesToReferencesInGroupByAndOrderByIfNeededImpl(
    NAst::TQuery* ast,
    NAst::TAliasMap* aliasMap,
    NAst::TAstHead* head,
    const THashSet<std::string>& reservedNames)
{
    if (!ast->GroupExprs && ast->OrderExpressions.empty()) {
        return;
    }

    bool hasIndexReference = false;

    auto& orderExpressionList = ast->OrderExpressions;
    auto& projections = ast->SelectExprs;
    int projectionCount = projections ? std::ssize(*projections) : 0;

    auto isIndexReference = [&] (NAst::TExpressionPtr expr) {
        if (auto projectionIndex = TryGetProjectionIndex(expr)) {
            if (*projectionIndex < 0 || *projectionIndex > projectionCount) {
                THROW_ERROR_EXCEPTION("Reference expression index is out of bounds")
                    .With("index", *projectionIndex);
            }

            return true;
        }

        return false;
    };

    if (ast->GroupExprs) {
        for (auto* expr : ast->GroupExprs.value()) {
            hasIndexReference |= isIndexReference(expr);
        }
    }

    THROW_ERROR_EXCEPTION_IF(hasIndexReference && !projections.has_value(),
        "Projections are not specified, "
        "but projection indices are used in GROUP BY key");

    for (auto& orderExpr : orderExpressionList) {
        for (auto* expr : orderExpr.Expressions) {
            hasIndexReference |= isIndexReference(expr);
        }
    }

    THROW_ERROR_EXCEPTION_IF(hasIndexReference && !projections.has_value(),
        "Projections are not specified, "
        "but projection indices are used in ORDER BY key");

    if (!hasIndexReference) {
        return;
    }

    THashMap<i64, NAst::TReference> indexToReference;

    for (i64 projectionIndex = 0; projectionIndex < std::ssize(*projections); ++projectionIndex) {
        auto& expr = (*projections)[projectionIndex];
        auto aliasExpressionAst = expr->As<NAst::TAliasExpression>();

        // NB: Here the SQL standard uses one-based indexing.
        i64 adjustedProjectionIndex = projectionIndex + 1;

        if (aliasExpressionAst) {
            indexToReference.emplace(adjustedProjectionIndex, NAst::TReference(aliasExpressionAst->Name));
            continue;
        }

        auto referenceExpressionAst = expr->As<NAst::TReferenceExpression>();
        if (referenceExpressionAst) {
            indexToReference.emplace(adjustedProjectionIndex, referenceExpressionAst->Reference);
            continue;
        }

        std::string newAlias;
        for (int index = projectionIndex + 1; ; ++index) {
            newAlias = MakeColumnNameByIndex(index);
            if (!reservedNames.contains(newAlias) && !aliasMap->contains(newAlias)) {
                break;
            }
        }
        indexToReference.emplace(adjustedProjectionIndex, NAst::TReference(newAlias));
        aliasMap->emplace(newAlias, expr);

        auto* newExpr = head->New<NAst::TAliasExpression>(expr->SourceLocation, expr, newAlias);
        (*projections)[projectionIndex] = newExpr;
    }

    if (ast->GroupExprs) {
        NAst::TExpressionList& groupExpressionList = *ast->GroupExprs;

        for (i64 index = 0; index < std::ssize(groupExpressionList); ++index) {
            auto& expr = groupExpressionList[index];
            if (auto projectionIndex = TryGetProjectionIndex(expr)) {
                auto referenceIt = indexToReference.find(*projectionIndex);
                if (referenceIt == indexToReference.end()) {
                    THROW_ERROR_EXCEPTION("Reference expression index is out of bounds")
                        .With("index", *projectionIndex);
                }

                auto* newExpr = head->New<NAst::TReferenceExpression>(
                    expr->SourceLocation,
                    referenceIt->second);
                groupExpressionList[index] = newExpr;
            }
        }
    }

    for (auto& orderExpr : orderExpressionList) {
        for (i64 index = 0; index < std::ssize(orderExpr.Expressions); ++index) {
            auto& expr = orderExpr.Expressions[index];
            if (auto projectionIndex = TryGetProjectionIndex(expr)) {
                auto referenceIt = indexToReference.find(*projectionIndex);
                if (referenceIt == indexToReference.end()) {
                    THROW_ERROR_EXCEPTION("Reference expression index is out of bounds")
                        .With("index", *projectionIndex);
                }

                auto* newExpr = head->New<NAst::TReferenceExpression>(
                    expr->SourceLocation,
                    referenceIt->second);
                orderExpr.Expressions[index] = newExpr;
            }
        }
    }

    return;
}

using TReservedNamesByQuery = THashMap<const NAst::TQuery*, THashSet<std::string>>;

class TProjectionIndicesRewriter
    : public NAst::TAstVisitor<TProjectionIndicesRewriter>
{
public:
    TProjectionIndicesRewriter(
        NAst::TAstHead* head,
        TReservedNamesByQuery* reservedNamesByQuery)
        : Head_(head)
        , ReservedNamesByQuery_(reservedNamesByQuery)
    { }

    void Rewrite(NAst::TQuery* query, NAst::TAliasMap* aliasMap)
    {
        CheckStackDepth();

        if (ReservedNamesByQuery_->contains(query)) {
            return;
        }

        THashSet<std::string> reservedNames;
        auto* previousReservedNames = std::exchange(ReservedNames_, &reservedNames);
        auto guard = Finally([&] {
            ReservedNames_ = previousReservedNames;
        });

        NYT::Visit(
            query->FromClause,
            [] (const NAst::TTableDescriptor& /*table*/) { },
            [&] (const NAst::TQueryAstHeadPtr& subquery) {
                Rewrite(&subquery->Ast, &subquery->AliasMap);
            },
            [&] (const NAst::TExpressionList& expressions) {
                Visit(expressions);
            },
            [&] (const NAst::TUnionAllClause& unionAllClause) {
                for (const auto& subquery : unionAllClause.Subqueries) {
                    Rewrite(&subquery->Ast, &subquery->AliasMap);
                }
            });

        for (const auto& [name, expression] : *aliasMap) {
            reservedNames.insert(name);
            Visit(expression);
        }

        for (const auto& join : query->Joins) {
            NYT::Visit(
                join,
                [&] (const NAst::TJoin& tableJoin) {
                    Visit(tableJoin.Lhs);
                    Visit(tableJoin.Rhs);
                    Visit(tableJoin.Predicate);
                },
                [&] (const NAst::TArrayJoin& arrayJoin) {
                    Visit(arrayJoin.Columns);
                    Visit(arrayJoin.Predicate);
                });
        }

        Visit(query->SelectExprs);
        Visit(query->WherePredicate);
        Visit(query->GroupExprs);
        Visit(query->HavingPredicate);
        for (const auto& orderExpression : query->OrderExpressions) {
            Visit(orderExpression.Expressions);
        }

        RewriteIntegerIndicesToReferencesInGroupByAndOrderByIfNeededImpl(query, aliasMap, Head_, reservedNames);

        ReservedNamesByQuery_->emplace(query, std::move(reservedNames));
    }

    void OnQuery(NAst::TQueryExpressionPtr queryExpr)
    {
        Rewrite(&queryExpr->Query, &queryExpr->AliasMap);
        auto it = ReservedNamesByQuery_->find(&queryExpr->Query);
        if (it == ReservedNamesByQuery_->end()) {
            THROW_ERROR_EXCEPTION("Reserved names are missing for subquery");
        }

        // A generated alias must not capture a correlated reference in a nested scalar query.
        ReservedNames_->insert(it->second.begin(), it->second.end());
    }

    void OnAlias(NAst::TAliasExpressionPtr aliasExpr)
    {
        ReservedNames_->insert(aliasExpr->Name);
        Visit(aliasExpr->Expression);
    }

    void OnReference(NAst::TReferenceExpressionPtr referenceExpr)
    {
        const auto& reference = referenceExpr->Reference;
        ReservedNames_->insert(reference.ColumnName);
        if (reference.TableName) {
            ReservedNames_->insert(*reference.TableName);
        }
        Visit(reference.CompositeTypeAccessor.DictOrListItemAccessor);
    }

private:
    NAst::TAstHead* const Head_;
    TReservedNamesByQuery* const ReservedNamesByQuery_;

    THashSet<std::string>* ReservedNames_ = nullptr;
};

void RewriteIntegerIndicesToReferencesInGroupByAndOrderByIfNeeded(
    NAst::TQuery* query,
    NAst::TAliasMap* aliasMap,
    NAst::TAstHead* head)
{
    TReservedNamesByQuery reservedNamesByQuery;
    auto rewriter = TProjectionIndicesRewriter(head, &reservedNamesByQuery);
    rewriter.Rewrite(query, aliasMap);
}

// NB: This visitor does NOT traverse into subqueries in the FROM clause (e.g., `SELECT FROM (SELECT ...)`).
// Such subqueries are handled separately via recursive calls to PreparePlanFragmentImpl.
class TPathCollector
    : public NAst::TAstVisitor<TPathCollector>
{
public:
    explicit TPathCollector(std::function<void(const NYPath::TYPath&)> onPath)
        : OnPath_(std::move(onPath))
    { }

    void OnQuery(const NAst::TQueryExpressionPtr queryExpr)
    {
        if (const auto* table = std::get_if<NAst::TTableDescriptor>(&queryExpr->Query.FromClause)) {
            OnPath_(table->Path);
        }

        for (const auto& join : queryExpr->Query.Joins) {
            NYT::Visit(join,
                [&] (const NAst::TJoin& tableJoin) {
                    OnPath_(tableJoin.Table.Path);
                },
                [&] (const NAst::TArrayJoin& /*arrayJoin*/) { });
        }

        for (const auto& [_, alias] : queryExpr->AliasMap) {
            Visit(alias);
        }

        Visit(queryExpr->Query.SelectExprs);
        Visit(queryExpr->Query.WherePredicate);
        Visit(queryExpr->Query.GroupExprs);
        Visit(queryExpr->Query.HavingPredicate);
        for (const auto& [expr, _] : queryExpr->Query.OrderExpressions) {
            Visit(expr);
        }
    }

private:
    std::function<void(const NYPath::TYPath&)> OnPath_;
};

const TDataSplit& GetDataSplitOrThrow(
    const THashMap<NYPath::TYPath, TDataSplit>& dataSplits,
    const NYPath::TYPath& path)
{
    auto it = dataSplits.find(path);
    if (it == dataSplits.end()) {
        THROW_ERROR_EXCEPTION("Data split not found for table path %v", path);
    }
    return it->second;
}

THashMap<NYPath::TYPath, TDataSplit> GetDataSplits(
    IPrepareCallbacks* callbacks,
    const NAst::TQuery& queryAst,
    const NAst::TAliasMap& aliasMap,
    int depth,
    const NLogging::TLogger& Logger)
{
    {
        const auto* table = std::get_if<NAst::TTableDescriptor>(&queryAst.FromClause);

        YT_TLOG_DEBUG("Getting initial data splits")
            .With("PrimaryPath", table ? table->Path : "inapplicable")
            .With("ForeignPaths", MakeFormattableView(
                queryAst.Joins,
                [] (TStringBuilderBase* builder, const std::variant<NAst::TJoin, NAst::TArrayJoin>& join) {
                    if (auto* tableJoin = std::get_if<NAst::TJoin>(&join)) {
                        FormatValue(builder, tableJoin->Table.Path, TStringBuf());
                    }
            }))
            .With("SubqueryDepth", depth);
    }

    THashMap<NYPath::TYPath, TFuture<TDataSplit>> asyncDataSplits;

    auto pathCollector = TPathCollector([&] (const NYPath::TYPath& path) -> void {
        auto [it, inserted] = asyncDataSplits.try_emplace(path);
        if (inserted) {
            it->second = callbacks->GetInitialSplit(path);
        }
    });
    auto query = NAst::TQueryExpression({}, queryAst, aliasMap);
    pathCollector.Visit(&query);

    std::vector<TFuture<std::pair<NYPath::TYPath, TDataSplit>>> futures;
    futures.reserve(asyncDataSplits.size());
    for (const auto& [path, future] : asyncDataSplits) {
        futures.push_back(future.Apply(BIND([path] (const TDataSplit& dataSplit) {
            return std::pair{path, dataSplit};
        })));
    }

    auto dataSplitPairs = WaitForFast(AllSucceeded(futures))
        .ValueOrThrow();

    THashMap<NYPath::TYPath, TDataSplit> result;
    result.reserve(dataSplitPairs.size());
    for (auto& pair : dataSplitPairs) {
        result.insert(std::move(pair));
    }

    YT_TLOG_DEBUG("Initial data splits received");
    return result;
}

} // namespace

void ValidateOrderByOffsetAndLimit(const NAst::TQuery& queryAst)
{
    if (!queryAst.OrderExpressions.empty() && !queryAst.Limit) {
        THROW_ERROR_EXCEPTION("ORDER BY used without LIMIT");
    }

    if (queryAst.Offset && !queryAst.Limit) {
        THROW_ERROR_EXCEPTION("OFFSET used without LIMIT");
    }

    if (queryAst.Limit && *queryAst.Limit > MaxQueryLimit) {
        THROW_ERROR_EXCEPTION("Maximum LIMIT exceeded")
            .With("limit", *queryAst.Limit)
            .With("max_limit", MaxQueryLimit);
    }

    i64 offset = queryAst.Offset.value_or(0);
    if (offset < 0) {
        THROW_ERROR_EXCEPTION("Negative OFFSET is forbidden")
            .With("offset", offset);
    }

    if (queryAst.Limit && *queryAst.Limit < 0) {
        THROW_ERROR_EXCEPTION("Negative LIMIT is forbidden")
            .With("limit", *queryAst.Limit);
    }

    THROW_ERROR_EXCEPTION_IF(
        queryAst.Limit && offset > std::numeric_limits<i64>::max() - *queryAst.Limit,
        "Sum of offset %v and limit %v overflows i64",
        offset,
        *queryAst.Limit);
}

void ValidateHaving(const NAst::TQuery& queryAst, bool hasOrderBy)
{
    if (!queryAst.HavingPredicate || hasOrderBy) {
        return;
    }

    if (queryAst.Limit) {
        THROW_ERROR_EXCEPTION("HAVING with LIMIT is not allowed");
    }

    if (queryAst.Offset) {
        THROW_ERROR_EXCEPTION("HAVING with OFFSET is not allowed");
    }
}

TPlanFragmentPtr PreparePlanFragmentImpl(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    const NAst::TQuery& queryAst,
    const NAst::TAliasMap& aliasMap,
    const TPreparePlanFragmentOptions& options,
    IMemoryUsageTrackerPtr memoryTracker,
    int depth)
{
    if (depth > MaxSubqueryDepth) {
        THROW_ERROR_EXCEPTION("Maximum subquery depth exceeded")
            .With("max_subquery_depth", MaxSubqueryDepth);
    }

    if (queryAst.SelectExprs &&
        options.MaxProjectionCount &&
        std::ssize(*queryAst.SelectExprs) > *options.MaxProjectionCount)
    {
        THROW_ERROR_EXCEPTION("Maximum projection count exceeded")
            .With("projection_count", std::ssize(*queryAst.SelectExprs))
            .With("max_projection_count", *options.MaxProjectionCount);
    }

    auto query = New<TQuery>(TGuid::Create());

    auto Logger = MakeQueryLogger(query);

    auto fragment = New<TPlanFragment>();

    Visit(queryAst.FromClause,
        [&] (const NAst::TQueryAstHeadPtr& subquery) {
            fragment->SubqueryFragment = PreparePlanFragmentImpl(
                callbacks,
                source,
                subquery->Ast,
                subquery->AliasMap,
                options,
                memoryTracker,
                depth + 1);
        },
        [&] (const NAst::TTableDescriptor&) { },
        [&] (const NAst::TExpressionList&) {
            THROW_ERROR_EXCEPTION("Unexpected expression in from clause");
        },
        [&] (const NAst::TUnionAllClause& unionAll) {
            auto unionAllQuery = New<TUnionAllQuery>();
            // TODO(sabdenovch): Consider type extension to largest accommodating type per column.
            TTableSchemaPtr commonSchema;

            for (const auto* subqueryHead : unionAll.Subqueries) {
                auto subFragment = PreparePlanFragmentImpl(
                    callbacks,
                    source,
                    subqueryHead->Ast,
                    subqueryHead->AliasMap,
                    options,
                    memoryTracker,
                    depth + 1);

                auto subSchema = subFragment->Query->GetTableSchema();

                if (!commonSchema) {
                    commonSchema = subSchema;
                } else {
                    if (subSchema->GetColumnCount() != commonSchema->GetColumnCount()) {
                        THROW_ERROR_EXCEPTION(
                            "UNION ALL subqueries have different column counts %v and %v",
                            commonSchema->GetColumnCount(),
                            subSchema->GetColumnCount());
                    }
                    for (int i = 0; i < subSchema->GetColumnCount(); ++i) {
                        const auto& expected = commonSchema->Columns()[i];
                        const auto& actual = subSchema->Columns()[i];
                        if (actual.Name() != expected.Name()) {
                            THROW_ERROR_EXCEPTION(
                                "UNION ALL subqueries name column %v differently: %Qv and %Qv",
                                i,
                                expected.Name(),
                                actual.Name());
                        }
                        if (*actual.LogicalType() != *expected.LogicalType()) {
                            THROW_ERROR_EXCEPTION(
                                "UNION ALL subqueries have incompatible types for column %Qv, %v and %v",
                                expected.Name(),
                                *expected.LogicalType(),
                                *actual.LogicalType());
                        }
                    }
                }
                unionAllQuery->SubqueryFragments.push_back(std::move(subFragment));
            }

            unionAllQuery->Schema = std::move(commonSchema);
            fragment->UnionAllQuery = std::move(unionAllQuery);
        });

    auto functions = New<TTypeInferrerMap>();
    callbacks->FetchFunctions(ExtractFunctionNames(queryAst, aliasMap), functions, options.ExecutionBackend);

    auto dataSplits = GetDataSplits(callbacks, queryAst, aliasMap, depth, Logger);

    const auto* table = std::get_if<NAst::TTableDescriptor>(&queryAst.FromClause);

    if (table) {
        const auto& dataSplit = GetDataSplitOrThrow(dataSplits, table->Path);
        fragment->DataSource.ObjectId = dataSplit.ObjectId;
        query->Schema.Original = dataSplit.TableSchema;
    } else if (fragment->UnionAllQuery) {
        query->Schema.Original = fragment->UnionAllQuery->Schema;
    } else {
        query->Schema.Original = fragment->SubqueryFragment->Query->GetTableSchema();
    }

    auto alias = Visit(queryAst.FromClause,
        [&] (const NAst::TTableDescriptor& tableDescriptor) {
            return tableDescriptor.Alias;
        },
        [&] (const NAst::TQueryAstHeadPtr& subquery) {
            return subquery->Alias;
        },
        [&] (const NAst::TExpressionList&) -> std::optional<std::string> {
            return std::nullopt;
        },
        [&] (const NAst::TUnionAllClause& unionAll) {
            return unionAll.Alias;
        });

    TPreparePlanFragmentContext context{Logger, dataSplits, options};

    auto builder = CreateExpressionBuilder(
        source,
        functions,
        aliasMap,
        context,
        options.BuilderVersion);

    builder->AddTable({
        .Schema = *query->Schema.Original,
        .Alias = alias,
        .Mapping = &query->Schema.Mapping,
    });

    std::vector<TJoinClausePtr> joinClauses;
    size_t commonKeyPrefix = options.MergeVersionedRows
        ? std::numeric_limits<size_t>::max()
        : 0;
    for (const auto& join : queryAst.Joins) {
        Visit(join,
            [&] (const NAst::TJoin& tableJoin) {
                joinClauses.push_back(BuildJoinClause(
                    GetDataSplitOrThrow(dataSplits, tableJoin.Table.Path),
                    tableJoin,
                    source,
                    aliasMap,
                    functions,
                    &commonKeyPrefix,
                    query->Schema.Original,
                    table ? table->Alias : std::nullopt,
                    builder.get(),
                    context));
            },
            [&] (const NAst::TArrayJoin& arrayJoin) {
                joinClauses.push_back(BuildArrayJoinClause(
                    arrayJoin,
                    source,
                    aliasMap,
                    functions,
                    builder.get(),
                    options.BuilderVersion));
            });
    }

    PrepareQuery(query, queryAst, builder.get(), options.MergeVersionedRows);

    // Why after PrepareQuery? GetTableSchema is called inside PrepareQuery?
    query->JoinClauses.assign(joinClauses.begin(), joinClauses.end());

    if (std::ssize(query->JoinClauses) > MaxJoinNumber) {
        THROW_ERROR_EXCEPTION("The number of joins exceeds the allowed maximum. Consider rewriting the query.")
            .With("join_number", std::ssize(query->JoinClauses))
            .With("max_join_number", MaxMultiJoinGroupNumber);
    }

    auto joinGroups = GetJoinGroups(query->JoinClauses, query->GetRenamedSchema());
    if (std::ssize(joinGroups) > MaxMultiJoinGroupNumber) {
        THROW_ERROR_EXCEPTION("The number of multi-join groups exceeds the allowed maximum. Consider rewriting the query.")
            .With("multi_join_group_number", std::ssize(joinGroups))
            .With("max_multi_join_group_number", MaxMultiJoinGroupNumber);
    }

    ValidateOrderByOffsetAndLimit(queryAst);
    ValidateHaving(queryAst, /*hasOrderBy*/ query->OrderClause != nullptr);

    if (queryAst.Limit) {
        query->Limit = queryAst.Limit;
    }

    if (queryAst.Offset) {
        query->Offset = *queryAst.Offset;
    }

    DropLimitClauseWhenGroupByOne(query);

    TryPushDownGroupBy(query, queryAst, Logger);

    // COMPAT(sabdenovch): Group by with limit may be processed in unordered fashion
    // only if the group clause cannot be processed in order. Resolve it here, where
    // the group clause (including aggregate items), the limit clause and the
    // group-by push-down are final.
    if (!query->OrderClause && query->Limit &&
        options.AllowUnorderedGroupByWithLimit &&
        query->GroupClause &&
        !query->GroupClause->AllAggregatesAreFirst() &&
        query->GroupClause->CommonPrefixWithPrimaryKey == 0)
    {
        query->ScanOrder = EScanOrder::Unordered;
    }

    auto readSchema = query->GetReadSchema();

    if (fragment->SubqueryFragment) {
        EliminateRedundantProjections(fragment->SubqueryFragment->Query, *readSchema);
    } else if (fragment->UnionAllQuery) {
        for (const auto& subqueryFragment : fragment->UnionAllQuery->SubqueryFragments) {
            EliminateRedundantProjections(subqueryFragment->Query, *readSchema);
        }
    }

    auto queryFingerprint = InferName(query, {.OmitValues = true});
    YT_TLOG_DEBUG("Prepared query")
        .With("Fingerprint", queryFingerprint)
        .With("ReadSchema", *readSchema)
        .With("ResultSchema", *query->GetTableSchema());

    auto rowBuffer = New<TRowBuffer>(
        TQueryPreparerBufferTag(),
        TChunkedMemoryPool::DefaultStartChunkSize,
        memoryTracker);

    query = InsertHierarchicalJoins(query, Logger);

    fragment->Query = query;

    return fragment;
}

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    const TPreparePlanFragmentOptions& options,
    NYson::TYsonStringBuf placeholderValues,
    IMemoryUsageTrackerPtr memoryTracker)
{
    auto parsedSource = ParseSource(source, EParseMode::Query, placeholderValues, options.SyntaxVersion);

    return PreparePlanFragment(
        callbacks,
        source,
        std::get<NAst::TQuery>(parsedSource->AstHead.Ast),
        parsedSource->AstHead,
        options,
        std::move(memoryTracker));
}

void ApplyAstRewriters(
    NAst::TQuery& queryAst,
    NAst::TAliasMap& aliasMap,
    NAst::TAstHead& astHead,
    const TPreparePlanFragmentOptions& options)
{
    if (options.SyntaxVersion >= 3) {
        RewriteIntegerIndicesToReferencesInGroupByAndOrderByIfNeeded(&queryAst, &aliasMap, &astHead);
    }

    if (options.ShouldRewriteCardinalityIntoHyperLogLog) {
        RewriteCardinalityIntoHyperLogLogWithPrecision(&queryAst, &aliasMap, &astHead, options.HyperLogLogPrecision);
    }
}

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    NAst::TQuery& queryAst,
    NAst::TAstHead& astHead,
    const TPreparePlanFragmentOptions& options,
    IMemoryUsageTrackerPtr memoryTracker)
{
    auto aliasMap = astHead.AliasMap;

    ApplyAstRewriters(queryAst, aliasMap, astHead, options);

    return PreparePlanFragmentImpl(
        callbacks,
        source,
        queryAst,
        aliasMap,
        options,
        std::move(memoryTracker),
        /*depth*/ 0);
}

TQueryPtr PrepareJobQuery(
    TStringBuf source,
    const TTableSchemaPtr& tableSchema,
    const TFunctionsFetcher& functionsFetcher)
{
    auto astHead = ParseQueryString(source, NAst::TParser::token::StrayWillParseJobQuery);
    const auto& ast = std::get<NAst::TQuery>(astHead.Ast);
    const auto& aliasMap = astHead.AliasMap;

    if (ast.Offset) {
        THROW_ERROR_EXCEPTION("OFFSET is not supported in map-reduce queries");
    }

    if (ast.Limit) {
        THROW_ERROR_EXCEPTION("LIMIT is not supported in map-reduce queries");
    }

    if (ast.GroupExprs) {
        THROW_ERROR_EXCEPTION("GROUP BY is not supported in map-reduce queries");
    }

    auto query = New<TQuery>(TGuid::Create());
    query->Schema.Original = tableSchema;

    auto functionNames = ExtractFunctionNames(ast, aliasMap);

    auto functions = New<TTypeInferrerMap>();
    functionsFetcher(functionNames, functions, EExecutionBackend::Native);

    auto Logger = MakeQueryLogger(query);
    THashMap<NYPath::TYPath, TDataSplit> emptyDataSplits;
    TPreparePlanFragmentOptions defaultOptions;
    TPreparePlanFragmentContext context{Logger, emptyDataSplits, defaultOptions};
    auto builder = CreateExpressionBuilder(source, functions, aliasMap, context, 1);

    builder->AddTable({
        .Schema = *tableSchema,
        .Alias = std::nullopt,
        .Mapping = &query->Schema.Mapping,
    });

    PrepareQuery(
        query,
        ast,
        builder.get());

    return query;
}

TConstExpressionPtr PrepareExpression(
    TStringBuf source,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions,
    THashSet<std::string>* references)
{
    return PrepareExpression(
        *ParseSource(source, EParseMode::Expression),
        tableSchema,
        1,
        functions,
        references);
}

TConstExpressionPtr PrepareExpression(
    const TParsedSource& parsedSource,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions)
{
    return PrepareExpression(parsedSource, tableSchema, 1, functions);
}

TConstExpressionPtr PrepareExpression(
    const TParsedSource& parsedSource,
    const TTableSchema& tableSchema,
    int builderVersion,
    const TConstTypeInferrerMapPtr& functions,
    THashSet<std::string>* references)
{
    auto expr = std::get<NAst::TExpressionPtr>(parsedSource.AstHead.Ast);
    const auto& aliasMap = parsedSource.AstHead.AliasMap;

    std::vector<TColumnDescriptor> mapping;

    auto Logger = MakeQueryLogger(TGuid::Create());
    THashMap<NYPath::TYPath, TDataSplit> emptyDataSplits;
    TPreparePlanFragmentOptions defaultOptions;
    TPreparePlanFragmentContext context{Logger, emptyDataSplits, defaultOptions};
    auto builder = CreateExpressionBuilder(parsedSource.Source, functions, aliasMap, context, builderVersion);

    builder->AddTable({
        .Schema = tableSchema,
        .Alias = std::nullopt,
        .Mapping = &mapping,
    });

    auto result = ApplyRewriters(builder->BuildTypedExpression(expr));

    if (references) {
        for (const auto& item : mapping) {
            references->insert(item.Name);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
