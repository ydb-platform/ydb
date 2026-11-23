#include "expr_builder_base.h"
#include "functions.h"
#include "helpers.h"
#include "private.h"

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TTypeSet GetTypes(const NAst::TLiteralValue& literalValue)
{
    return Visit(literalValue,
        [] (const NAst::TNullLiteralValue&) {
            return TTypeSet({
                EValueType::Null,
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
                EValueType::Boolean,
                EValueType::String,
                EValueType::Any,
            });
        },
        [] (i64) {
            return TTypeSet({
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
            });
        },
        [] (ui64) {
            return TTypeSet({
                EValueType::Uint64,
                EValueType::Double,
            });
        },
        [] (double) {
            return TTypeSet({
                EValueType::Double,
            });
        },
        [] (bool) {
            return TTypeSet({
                EValueType::Boolean,
            });
        },
        [] (const std::string&) {
            return TTypeSet({
                EValueType::String,
            });
        });
}

////////////////////////////////////////////////////////////////////////////////

bool Unify(TTypeSet* genericAssignments, const TTypeSet& types)
{
    auto intersection = *genericAssignments & types;

    if (intersection.IsEmpty()) {
        return false;
    } else {
        *genericAssignments = intersection;
        return true;
    }
}

EValueType GetFrontWithCheck(const TTypeSet& typeSet, TStringBuf /*source*/)
{
    return typeSet.GetFront();
}

TTypeSet InferFunctionTypes(
    const ITypeInferrerPtr& inferrer,
    const std::vector<TTypeSet>& effectiveTypes,
    std::vector<TTypeSet>* genericAssignments,
    TStringBuf functionName,
    TStringBuf source)
{
    auto constraints = inferrer->GetNormalizedConstraints(functionName);

    *genericAssignments = std::move(constraints.TypeConstraints);

    int argIndex = 1;
    auto arg = effectiveTypes.begin();
    auto formalArg = constraints.FormalArguments.begin();
    for (;
        formalArg != constraints.FormalArguments.end() && arg != effectiveTypes.end();
        arg++, formalArg++, argIndex++)
    {
        auto& constraints = (*genericAssignments)[*formalArg];
        if (!Unify(&constraints, *arg)) {
            THROW_ERROR_EXCEPTION("No matching function %Qv", functionName)
                .With("argument_index", argIndex)
                .With("expected_type", ToString(constraints))
                .With("actual_type", ToString(*arg))
                .With("expression", source);
        }
    }

    bool hasNoRepeatedArgument = !constraints.RepeatedType.operator bool();

    if (formalArg != constraints.FormalArguments.end() ||
        (arg != effectiveTypes.end() && hasNoRepeatedArgument))
    {
        THROW_ERROR_EXCEPTION("No matching function %Qv", functionName)
            .With("expected_arguments", constraints.FormalArguments.size())
            .With("actual_arguments", effectiveTypes.size())
            .With("expression", source);
    }

    for (; arg != effectiveTypes.end(); arg++) {
        int constraintIndex = constraints.RepeatedType->first;
        if (constraints.RepeatedType->second) {
            constraintIndex = genericAssignments->size();
            genericAssignments->push_back((*genericAssignments)[constraints.RepeatedType->first]);
        }
        auto& constraints = (*genericAssignments)[constraintIndex];
        if (!Unify(&constraints, *arg)) {
            THROW_ERROR_EXCEPTION(
                "Wrong type for repeated argument to function %Qv: expected %Qv, got %Qv",
                functionName,
                constraints,
                *arg)
                .With("expression", source);
        }
    }

    return (*genericAssignments)[constraints.ReturnType];
}

std::vector<EValueType> RefineFunctionTypes(
    const ITypeInferrerPtr& inferrer,
    EValueType resultType,
    int argumentCount,
    std::vector<TTypeSet>* genericAssignments,
    TStringBuf functionName,
    TStringBuf source)
{
    auto constraints = inferrer->GetNormalizedConstraints(functionName);

    (*genericAssignments)[constraints.ReturnType] = TTypeSet({resultType});

    std::vector<EValueType> genericAssignmentsMin;
    for (auto& constraint : *genericAssignments) {
        genericAssignmentsMin.push_back(GetFrontWithCheck(constraint, source));
    }

    std::vector<EValueType> effectiveTypes;
    effectiveTypes.reserve(argumentCount);
    int argIndex = 0;

    for (auto formalArg = constraints.FormalArguments.begin();
        formalArg != constraints.FormalArguments.end() && argIndex < argumentCount;
        ++formalArg, ++argIndex)
    {
        effectiveTypes.push_back(genericAssignmentsMin[*formalArg]);
    }

    for (; argIndex < argumentCount; ++argIndex) {
        int constraintIndex = constraints.RepeatedType->first;
        if (constraints.RepeatedType->second) {
            constraintIndex = genericAssignments->size() - (argumentCount - argIndex);
        }

        effectiveTypes.push_back(genericAssignmentsMin[constraintIndex]);
    }

    return effectiveTypes;
}

// 1. Init generic assignments with constraints
//    Intersect generic assignments with argument types and save them
//    Infer feasible result types
// 2. Apply result types and restrict generic assignments and argument types

void IntersectGenericsWithArgumentTypes(
    const std::vector<TTypeSet>& effectiveTypes,
    std::vector<TTypeSet>* genericAssignments,
    const std::vector<int>& formalArguments,
    const std::optional<std::pair<int, bool>>& repeatedType,
    TStringBuf functionName,
    TStringBuf source)
{
    if (!repeatedType && formalArguments.size() != effectiveTypes.size() || repeatedType && effectiveTypes.size() < formalArguments.size()) {
        THROW_ERROR_EXCEPTION("No matching function %Qv", functionName)
            .With("expected_arguments", formalArguments.size())
            .With("actual_arguments", effectiveTypes.size())
            .With("expression", source);
    }

    for (int argIndex = 0; argIndex < std::ssize(formalArguments); ++argIndex) {
        auto& constraints = (*genericAssignments)[formalArguments[argIndex]];
        if (!Unify(&constraints, effectiveTypes[argIndex])) {
            THROW_ERROR_EXCEPTION("No matching function %Qv", functionName)
                .With("argument_index", argIndex + 1)
                .With("expected_type", ToString(constraints))
                .With("actual_type", ToString(effectiveTypes[argIndex]))
                .With("expression", source);
        }
    }

    for (int argIndex = std::ssize(formalArguments); argIndex < std::ssize(effectiveTypes); ++argIndex) {
        int constraintIndex = repeatedType->first;
        if (repeatedType->second) {
            constraintIndex = genericAssignments->size();
            genericAssignments->push_back((*genericAssignments)[repeatedType->first]);
        }
        auto& constraints = (*genericAssignments)[constraintIndex];
        if (!Unify(&constraints, effectiveTypes[argIndex])) {
            THROW_ERROR_EXCEPTION("No matching function %Qv", functionName)
                .With("argument_index", argIndex + 1)
                .With("expected_type", ToString(constraints))
                .With("actual_type", ToString(effectiveTypes[argIndex]))
                .With("expression", source);
        }
    }
}

std::vector<EValueType> RefineFunctionTypes(
    EValueType resultType,
    int argumentCount,
    TNormalizedContraints* constraints,
    EValueType* stateType,
    TStringBuf source)
{
    constraints->TypeConstraints[constraints->ReturnType] = TTypeSet({resultType});

    std::vector<EValueType> genericAssignmentsMin;
    genericAssignmentsMin.reserve(constraints->TypeConstraints.size());
    for (const auto& constraint : constraints->TypeConstraints) {
        genericAssignmentsMin.push_back(GetFrontWithCheck(constraint, source));
    }

    *stateType = genericAssignmentsMin[constraints->StateType];

    std::vector<EValueType> effectiveTypes;
    effectiveTypes.reserve(argumentCount);
    int argIndex = 0;

    for (int formalArgConstraint : constraints->FormalArguments) {
        effectiveTypes.push_back(genericAssignmentsMin[formalArgConstraint]);
        argIndex++;
    }

    // TODO(sabdenovch): Get rid of the contract where union repeated types are dumped at the
    // end of the TypeConstraints by changing TNormalizedConstraints::FormalArguments ->
    // -> TNormalizedContraints::Arguments. Change GetNormalizedConstraints api, consider
    // argument types immediately. Unify RefineFunctionTypes(...) into single function,
    // unify IntersectGenericsWithArgumentTypes and InferFunctionTypes, move their logic
    // into GetNormalizedConstraints.
    for (; argIndex < argumentCount; ++argIndex) {
        int constraintIndex = constraints->RepeatedType->first;
        if (constraints->RepeatedType->second) {
            constraintIndex = genericAssignmentsMin.size() - (argumentCount - argIndex);
        }
        effectiveTypes.push_back(genericAssignmentsMin[constraintIndex]);
    }

    return effectiveTypes;
}

struct TOperatorTyper
{
    TTypeSet Constraint;
    std::optional<EValueType> ResultType;
};

TEnumIndexedArray<EBinaryOp, TOperatorTyper> BuildBinaryOperatorTypers()
{
    TEnumIndexedArray<EBinaryOp, TOperatorTyper> result;

    for (auto op : {
        EBinaryOp::Plus,
        EBinaryOp::Minus,
        EBinaryOp::Multiply,
        EBinaryOp::Divide})
    {
        result[op] = {
            TTypeSet({EValueType::Int64, EValueType::Uint64, EValueType::Double}),
            std::nullopt
        };
    }

    for (auto op : {
        EBinaryOp::Modulo,
        EBinaryOp::LeftShift,
        EBinaryOp::RightShift,
        EBinaryOp::BitOr,
        EBinaryOp::BitAnd})
    {
        result[op] = {
            TTypeSet({EValueType::Int64, EValueType::Uint64}),
            std::nullopt
        };
    }

    for (auto op : {
        EBinaryOp::And,
        EBinaryOp::Or})
    {
        result[op] = {
            TTypeSet({EValueType::Boolean}),
            EValueType::Boolean
        };
    }

    for (auto op : {
        EBinaryOp::Equal,
        EBinaryOp::NotEqual,
        EBinaryOp::Less,
        EBinaryOp::Greater,
        EBinaryOp::LessOrEqual,
        EBinaryOp::GreaterOrEqual})
    {
        result[op] = {
            TTypeSet({
                EValueType::Int64,
                EValueType::Uint64,
                EValueType::Double,
                EValueType::Boolean,
                EValueType::String,
                EValueType::Any}),
            EValueType::Boolean
        };
    }

    for (auto op : {EBinaryOp::Concatenate}) {
        result[op] = {
            TTypeSet({ EValueType::String, }),
            EValueType::String
        };
    }

    return result;
}

const TEnumIndexedArray<EBinaryOp, TOperatorTyper>& GetBinaryOperatorTypers()
{
    static auto result = BuildBinaryOperatorTypers();
    return result;
}

TEnumIndexedArray<EUnaryOp, TOperatorTyper> BuildUnaryOperatorTypers()
{
    TEnumIndexedArray<EUnaryOp, TOperatorTyper> result;

    for (auto op : {
        EUnaryOp::Plus,
        EUnaryOp::Minus})
    {
        result[op] = {
            TTypeSet({EValueType::Int64, EValueType::Uint64, EValueType::Double}),
            std::nullopt
        };
    }

    result[EUnaryOp::BitNot] = {
        TTypeSet({EValueType::Int64, EValueType::Uint64}),
        std::nullopt
    };

    result[EUnaryOp::Not] = {
        TTypeSet({EValueType::Boolean}),
        std::nullopt
    };

    return result;
}

const TEnumIndexedArray<EUnaryOp, TOperatorTyper>& GetUnaryOperatorTypers()
{
    static auto result = BuildUnaryOperatorTypers();
    return result;
}

TTypeSet InferBinaryExprTypes(
    EBinaryOp opCode,
    const TTypeSet& lhsTypes,
    const TTypeSet& rhsTypes,
    TTypeSet* genericAssignments,
    TStringBuf lhsSource,
    TStringBuf rhsSource)
{
    if (IsRelationalBinaryOp(opCode) && (lhsTypes & rhsTypes).IsEmpty()) {
        return TTypeSet{EValueType::Boolean};
    }

    const auto& binaryOperators = GetBinaryOperatorTypers();

    *genericAssignments = binaryOperators[opCode].Constraint;

    if (!Unify(genericAssignments, lhsTypes)) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv: expected %Qv, got %Qv",
            opCode,
            *genericAssignments,
            lhsTypes)
            .With("lhs_source", lhsSource)
            .With("rhs_source", rhsSource);
    }

    if (!Unify(genericAssignments, rhsTypes)) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv: expected %Qv, got %Qv",
            opCode,
            *genericAssignments,
            rhsTypes)
            .With("lhs_source", lhsSource)
            .With("rhs_source", rhsSource);
    }

    TTypeSet resultTypes;
    if (binaryOperators[opCode].ResultType) {
        resultTypes = TTypeSet({*binaryOperators[opCode].ResultType});
    } else {
        resultTypes = *genericAssignments;
    }

    return resultTypes;
}

std::pair<EValueType, EValueType> RefineBinaryExprTypes(
    EBinaryOp opCode,
    EValueType resultType,
    const TTypeSet& lhsTypes,
    const TTypeSet& rhsTypes,
    TTypeSet* genericAssignments,
    TStringBuf lhsSource,
    TStringBuf rhsSource,
    TStringBuf source)
{
    if (IsRelationalBinaryOp(opCode) && (lhsTypes & rhsTypes).IsEmpty()) {
        // Empty intersection (Any, alpha) || (alpha, Any), where alpha = {bool, int, uint, double, string}
        if (lhsTypes.Get(EValueType::Any)) {
            return std::pair(EValueType::Any, GetFrontWithCheck(rhsTypes, rhsSource));
        }

        if (rhsTypes.Get(EValueType::Any)) {
            return std::pair(GetFrontWithCheck(lhsTypes, lhsSource), EValueType::Any);
        }

        THROW_ERROR_EXCEPTION("Type mismatch in expression")
            .With("lhs_source", lhsSource)
            .With("rhs_source", rhsSource);
    }

    const auto& binaryOperators = GetBinaryOperatorTypers();

    EValueType argType;
    if (binaryOperators[opCode].ResultType) {
        argType = GetFrontWithCheck(*genericAssignments, source);
    } else {
        YT_VERIFY(genericAssignments->Get(resultType));
        argType = resultType;
    }

    return std::pair(argType, argType);
}

TTypeSet InferUnaryExprTypes(
    EUnaryOp opCode,
    const TTypeSet& argTypes,
    TTypeSet* genericAssignments,
    TStringBuf opSource)
{
    const auto& unaryOperators = GetUnaryOperatorTypers();

    *genericAssignments = unaryOperators[opCode].Constraint;

    if (!Unify(genericAssignments, argTypes)) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv: expected %Qv, got %Qv",
            opCode,
            *genericAssignments,
            argTypes)
            .With("op_source", opSource);
    }

    TTypeSet resultTypes;
    if (unaryOperators[opCode].ResultType) {
        resultTypes = TTypeSet({*unaryOperators[opCode].ResultType});
    } else {
        resultTypes = *genericAssignments;
    }

    return resultTypes;
}

EValueType RefineUnaryExprTypes(
    EUnaryOp opCode,
    EValueType resultType,
    TTypeSet* genericAssignments,
    TStringBuf opSource)
{
    const auto& unaryOperators = GetUnaryOperatorTypers();

    EValueType argType;
    if (unaryOperators[opCode].ResultType) {
        argType = GetFrontWithCheck(*genericAssignments, opSource);
    } else {
        YT_VERIFY(genericAssignments->Get(resultType));
        argType = resultType;
    }

    return argType;
}

////////////////////////////////////////////////////////////////////////////////

using TExpressionGenerator = std::function<TConstExpressionPtr(EValueType)>;

struct TUntypedExpression
{
    TTypeSet FeasibleTypes;
    TExpressionGenerator Generator;
    bool IsConstant;
};

class TExpressionBuilderV1
    : public TExpressionBuilder
{
public:
    TExpressionBuilderV1(
        TStringBuf source,
        const TConstTypeInferrerMapPtr& functions,
        const NAst::TAliasMap& aliasMap,
        const TPreparePlanFragmentContext& context)
        : TExpressionBuilder(source, functions, context)
        , AliasMap_(aliasMap)
    { }

    TUntypedExpression OnExpression(
        const NAst::TExpression* expr);

    TConstExpressionPtr BuildTypedExpression(const NAst::TExpression* expr, TRange<EValueType> resultTypes) override;

    void AddTable(TNameSource nameSource) override
    {
        ColumnResolver_.AddTable(std::move(nameSource));
    }

    TLogicalTypePtr ResolveColumn(const NAst::TColumnReference& reference) override
    {
        return ColumnResolver_.Resolve(reference);
    }

    void PopulateAllColumns() override
    {
        ColumnResolver_.PopulateAllColumns();
    }

    void Finish() override
    {
        ColumnResolver_.Finish();
    }

    void SetGroupData(const TNamedItemList* groupItems, TAggregateItemList* aggregateItems, ETotalsMode totalsMode) override
    {
        YT_VERIFY(!GroupItems_ && !AggregateItems_);

        GroupItems_ = groupItems;
        AggregateItems_ = aggregateItems;
        AfterGroupBy_ = true;
        TotalsMode_ = totalsMode;
    }

    std::string InferGroupItemName(
        const TConstExpressionPtr& /*typedExpression*/,
        const NAst::TExpression& expressionsAst) override
    {
        return InferColumnName(expressionsAst);
    }

private:
    struct ResolveNestedTypesResult
    {
        TCompositeMemberAccessorPath NestedStructOrTupleItemAccessor;
        TLogicalTypePtr IntermediateType;
        TLogicalTypePtr ResultType;
    };

    const NAst::TAliasMap& AliasMap_;
    std::set<std::string> UsedAliases_;
    int Depth_ = 0;

    THashMap<std::pair<std::string, EValueType>, TConstExpressionPtr> AggregateLookup_;

    TReferenceResolver ColumnResolver_;

    const TNamedItemList* GroupItems_ = nullptr;
    // TODO(lukyan): Enrich TMappedSchema with alias and keep here pointers to TMappedSchema.

    TAggregateItemList* AggregateItems_ = nullptr;

    bool AfterGroupBy_ = false;
    ETotalsMode TotalsMode_ = ETotalsMode::None;

    ResolveNestedTypesResult ResolveNestedTypes(
        const TLogicalTypePtr& type,
        const NAst::TReference& reference);

    TConstExpressionPtr UnwrapListOrDictItemAccessor(
        const NAst::TReference& reference,
        ELogicalMetatype metaType);

    TUntypedExpression UnwrapCompositeMemberAccessor(
        const NAst::TReference& reference,
        const TLogicalTypePtr& type);

    TUntypedExpression OnReference(
        const NAst::TReference& reference);

    TUntypedExpression OnFunction(
        const NAst::TFunctionExpression* functionExpr);

    TUntypedExpression OnUnaryOp(
        const NAst::TUnaryOpExpression* unaryExpr);

    TUntypedExpression MakeBinaryExpr(
        const NAst::TBinaryOpExpression* binaryExpr,
        EBinaryOp op,
        TUntypedExpression lhs,
        TUntypedExpression rhs,
        std::optional<size_t> offset);

    friend struct TBinaryOpGenerator;

    TUntypedExpression OnBinaryOp(
        const NAst::TBinaryOpExpression* binaryExpr);

    void InferArgumentTypes(
        std::vector<TConstExpressionPtr>* typedArguments,
        std::vector<EValueType>* argTypes,
        const NAst::TExpressionList& expressions,
        TStringBuf operatorName,
        TStringBuf source);

    TUntypedExpression OnInOp(
        const NAst::TInExpression* inExpr);

    TUntypedExpression OnBetweenOp(
        const NAst::TBetweenExpression* betweenExpr);

    TUntypedExpression OnTransformOp(
        const NAst::TTransformExpression* transformExpr);

    TUntypedExpression OnCaseOp(
        const NAst::TCaseExpression* caseExpr);

    TUntypedExpression OnLikeOp(
        const NAst::TLikeExpression* likeExpr);
};

std::unique_ptr<TExpressionBuilder> CreateExpressionBuilderV1(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap,
    const TPreparePlanFragmentContext& context)
{
    return std::make_unique<TExpressionBuilderV1>(source, functions, aliasMap, context);
}

TUntypedExpression TExpressionBuilderV1::OnExpression(
    const NAst::TExpression* expr)
{
    CheckStackDepth();

    ++Depth_;
    auto depthGuard = Finally([&] {
        --Depth_;
    });

    if (Depth_ > MaxExpressionDepth) {
        THROW_ERROR_EXCEPTION("Maximum expression depth exceeded")
            .With("max_expression_depth", MaxExpressionDepth);
    }

    if (auto literalExpr = expr->As<NAst::TLiteralExpression>()) {
        const auto& literalValue = literalExpr->Value;

        auto resultTypes = GetTypes(literalValue);
        TExpressionGenerator generator = [literalValue] (EValueType type) {
            return New<TLiteralExpression>(
                type,
                CastValueWithCheck(GetValue(literalValue), type));
        };
        return TUntypedExpression{.FeasibleTypes = resultTypes, .Generator = std::move(generator), .IsConstant = true};
    } else if (auto aliasExpr = expr->As<NAst::TAliasExpression>()) {
        return OnReference(NAst::TReference(aliasExpr->Name));
    } else if (auto referenceExpr = expr->As<NAst::TReferenceExpression>()) {
        return OnReference(referenceExpr->Reference);
    } else if (auto functionExpr = expr->As<NAst::TFunctionExpression>()) {
        return OnFunction(functionExpr);
    } else if (auto unaryExpr = expr->As<NAst::TUnaryOpExpression>()) {
        return OnUnaryOp(unaryExpr);
    } else if (auto binaryExpr = expr->As<NAst::TBinaryOpExpression>()) {
        return OnBinaryOp(binaryExpr);
    } else if (auto inExpr = expr->As<NAst::TInExpression>()) {
        return OnInOp(inExpr);
    } else if (auto betweenExpr = expr->As<NAst::TBetweenExpression>()) {
        return OnBetweenOp(betweenExpr);
    } else if (auto transformExpr = expr->As<NAst::TTransformExpression>()) {
        return OnTransformOp(transformExpr);
    } else if (auto caseExpr = expr->As<NAst::TCaseExpression>()) {
        return OnCaseOp(caseExpr);
    } else if (auto likeExpr = expr->As<NAst::TLikeExpression>()) {
        return OnLikeOp(likeExpr);
    } else if (expr->As<NAst::TQueryExpression>()) {
        THROW_ERROR_EXCEPTION("Subqueries in expressions are not supported in expression builder v1");
    }

    YT_ABORT();
}

TConstExpressionPtr TExpressionBuilderV1::BuildTypedExpression(
    const NAst::TExpression* expr,
    TRange<EValueType> resultTypes)
{
    TTypeSet feasibleTypes(resultTypes);

    auto expressionTyper = OnExpression(expr);
    YT_VERIFY(!expressionTyper.FeasibleTypes.IsEmpty());

    if (!Unify(&feasibleTypes, expressionTyper.FeasibleTypes)) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression: expected %Qv, got %Qv",
            feasibleTypes,
            expressionTyper.FeasibleTypes)
            .With("source", expr->GetSource(Source_));
    }

    auto result = expressionTyper.Generator(
        GetFrontWithCheck(feasibleTypes, expr->GetSource(Source_)));

    result = ApplyRewriters(result);

    return result;
}

TExpressionBuilderV1::ResolveNestedTypesResult TExpressionBuilderV1::ResolveNestedTypes(
    const TLogicalTypePtr& type,
    const NAst::TReference& reference)
{
    TCompositeMemberAccessorPath nestedStructOrTupleItemAccessor;
    nestedStructOrTupleItemAccessor.Reserve(std::ssize(reference.CompositeTypeAccessor.NestedStructOrTupleItemAccessor));

    TLogicalTypePtr current = type;
    bool optional = false;

    for (const auto& item : reference.CompositeTypeAccessor.NestedStructOrTupleItemAccessor) {
        Visit(item,
            [&] (const TStructMemberAccessor& structMember) {
                if (current->GetMetatype() == ELogicalMetatype::Optional) {
                    current = current->GetElement();
                    optional = true;
                }

                if (current->GetMetatype() != ELogicalMetatype::Struct) {
                    THROW_ERROR_EXCEPTION("Member %Qv is not found", structMember)
                        .With("source", NAst::FormatReference(reference));
                }

                const auto& fields = current->AsStructTypeRef().GetFields();
                for (int index = 0; index < std::ssize(fields); ++index) {
                    if (fields[index].Name == structMember) {
                        current = fields[index].Type;
                        nestedStructOrTupleItemAccessor.AppendStructMember(structMember, index);
                        return;
                    }
                }

                THROW_ERROR_EXCEPTION("Member %Qv is not found", structMember)
                    .With("source", NAst::FormatReference(reference));
            },
            [&] (const TTupleItemIndexAccessor& itemIndex) {
                if (current->GetMetatype() == ELogicalMetatype::Optional) {
                    current = current->GetElement();
                    optional = true;
                }

                if (current->GetMetatype() != ELogicalMetatype::Tuple) {
                    THROW_ERROR_EXCEPTION("Member %Qv is not found", itemIndex)
                        .With("source", NAst::FormatReference(reference));
                }

                const auto& tupleElements = current->AsTupleTypeRef().GetElements();

                if (itemIndex < 0 || itemIndex >= std::ssize(tupleElements)) {
                    THROW_ERROR_EXCEPTION("Member %Qv is not found", itemIndex)
                        .With("source", NAst::FormatReference(reference));
                }

                current = tupleElements[itemIndex];
                nestedStructOrTupleItemAccessor.AppendTupleItem(itemIndex);
            });
    }

    auto resultType = current;

    if (reference.CompositeTypeAccessor.DictOrListItemAccessor) {
        if (current->GetMetatype() == ELogicalMetatype::Optional) {
            current = current->GetElement();
            optional = true;
        }

        if (current->GetMetatype() == ELogicalMetatype::List) {
            resultType = current->GetElement();
        } else if (current->GetMetatype() == ELogicalMetatype::Dict) {
            auto keyType = GetWireType(current->AsDictTypeRef().GetKey());
            if (keyType != EValueType::String) {
                THROW_ERROR_EXCEPTION("Expected string key type, but got %Qlv",
                    keyType)
                    .With("source", NAst::FormatReference(reference));
            }
            resultType = current->AsDictTypeRef().GetValue();
        } else {
            THROW_ERROR_EXCEPTION("Incorrect nested item accessor")
                .With("source", NAst::FormatReference(reference));
        }
    }

    if (optional) {
        resultType = MakeOptionalIfNot(std::move(resultType));
    }

    return {std::move(nestedStructOrTupleItemAccessor), std::move(current), std::move(resultType)};
}

TConstExpressionPtr TExpressionBuilderV1::UnwrapListOrDictItemAccessor(
    const NAst::TReference& reference,
    ELogicalMetatype metaType)
{
    if (!reference.CompositeTypeAccessor.DictOrListItemAccessor.has_value()) {
        return {};
    }

    auto itemIndex = *reference.CompositeTypeAccessor.DictOrListItemAccessor;

    if (std::ssize(itemIndex) != 1) {
        THROW_ERROR_EXCEPTION("Expression inside of the list or dict item accessor should be scalar")
            .With("source", NAst::FormatReference(reference));
    }

    auto resultTypes = TTypeSet{};
    if (metaType == ELogicalMetatype::List) {
        resultTypes = TTypeSet{EValueType::Int64};
    } else if (metaType == ELogicalMetatype::Dict) {
        resultTypes = TTypeSet{EValueType::String};
    } else {
        YT_ABORT();
    }

    auto untypedExpression = OnExpression(itemIndex.front());
    if (!Unify(&resultTypes, untypedExpression.FeasibleTypes)) {
        THROW_ERROR_EXCEPTION("Incorrect type inside of the list or dict item accessor")
            .With("source", NAst::FormatReference(reference))
            .With("actual_type", ToString(untypedExpression.FeasibleTypes))
            .With("expected_type", ToString(resultTypes));
    }

    if (metaType == ELogicalMetatype::List) {
        return untypedExpression.Generator(EValueType::Int64);
    } else if (metaType == ELogicalMetatype::Dict) {
        return untypedExpression.Generator(EValueType::String);
    } else {
        YT_ABORT();
    }
}

TUntypedExpression TExpressionBuilderV1::UnwrapCompositeMemberAccessor(
    const NAst::TReference& reference,
    const TLogicalTypePtr& type)
{
    auto columnReference = New<TReferenceExpression>(type, InferColumnName(reference));

    if (reference.CompositeTypeAccessor.IsEmpty()) {
        auto generator = [columnReference] (EValueType /*type*/) {
            return columnReference;
        };

        auto wireType = GetWireType(type);
        if (wireType == EValueType::Composite) {
            wireType = EValueType::Any;
        }

        return {TTypeSet({wireType}), std::move(generator), /*IsConstant*/ false};
    }

    auto resolved = ResolveNestedTypes(type, reference);
    auto listOrDictItemAccessor = UnwrapListOrDictItemAccessor(reference, resolved.IntermediateType->GetMetatype());

    auto memberAccessor = New<TCompositeMemberAccessorExpression>(
        MakeOptionalIfNot(resolved.ResultType),
        columnReference,
        std::move(resolved.NestedStructOrTupleItemAccessor),
        listOrDictItemAccessor);

    auto generator = [memberAccessor] (EValueType /*type*/) {
        return memberAccessor;
    };

    auto wireType = GetWireType(resolved.ResultType);
    if (wireType == EValueType::Composite) {
        wireType = EValueType::Any;
    }

    return {TTypeSet({wireType}), std::move(generator), /*IsConstant*/ false};
}

TUntypedExpression TExpressionBuilderV1::OnReference(const NAst::TReference& reference)
{
    if (AfterGroupBy_) {
        TLogicalTypePtr type;

        // Search other way after group by.
        if (!reference.TableName) {
            for (int index = 0; index < std::ssize(*GroupItems_); ++index) {
                const auto& item = (*GroupItems_)[index];
                if (item.Name == reference.ColumnName) {
                    type = item.Expression->LogicalType;
                    break;
                }
            }
        }

        if (type) {
            if (TotalsMode_ != ETotalsMode::None) {
                type = MakeOptionalIfNot(type);
            }
            return UnwrapCompositeMemberAccessor(reference, type);
        }
    }

    if (!reference.TableName) {
        const auto& columnName = reference.ColumnName;
        auto found = AliasMap_.find(columnName);

        if (found != AliasMap_.end()) {
            // try InferName(found, expand aliases = true)

            if (UsedAliases_.insert(columnName).second) {
                auto aliasExpr = OnExpression(found->second);
                UsedAliases_.erase(columnName);
                return aliasExpr;
            }
        }
    }

    // Do not resolve this way after group by.
    if (!AfterGroupBy_) {
        if (auto type = ResolveColumn(reference)) {
            return UnwrapCompositeMemberAccessor(reference, type);
        }

        if (auto reinterpreted = TryReinterpretAsMemberAccess(reference);
            reinterpreted && ResolveColumn(*reinterpreted))
        {
            return OnReference(*reinterpreted);
        }
    }

    THROW_ERROR_EXCEPTION("Undefined reference %Qv",
        InferColumnName(reference));
}

TUntypedExpression TExpressionBuilderV1::OnFunction(const NAst::TFunctionExpression* functionExpr)
{
    auto functionName = ToLower(functionExpr->FunctionName);

    const auto& descriptor = Functions_->GetFunction(functionName);

    if (descriptor->IsAggregate()) {
        auto subexpressionName = InferColumnName(*functionExpr);

        std::vector<TTypeSet> argTypes;
        argTypes.reserve(functionExpr->Arguments.size());
        std::vector<TExpressionGenerator> operandTypers;
        operandTypers.reserve(functionExpr->Arguments.size());

        if (!AfterGroupBy_) {
            THROW_ERROR_EXCEPTION("Misuse of aggregate function %Qv", functionName);
        }

        AfterGroupBy_ = false;
        for (const auto& argument : functionExpr->Arguments) {
            auto untypedArgument = OnExpression(argument);
            argTypes.push_back(untypedArgument.FeasibleTypes);
            operandTypers.push_back(untypedArgument.Generator);
        }
        AfterGroupBy_ = true;

        // TODO(lukyan): Move following code into GetAggregateColumnPtr or remove GetAggregateColumnPtr function.

        auto constraints = descriptor->GetNormalizedConstraints(functionName);
        IntersectGenericsWithArgumentTypes(
            argTypes,
            &constraints.TypeConstraints,
            constraints.FormalArguments,
            constraints.RepeatedType,
            functionName,
            functionExpr->GetSource(Source_));

        auto resultTypes = constraints.TypeConstraints[constraints.ReturnType];

        TExpressionGenerator generator = [
            this,
            constraints = std::move(constraints),
            functionName = std::move(functionName),
            subexpressionName = std::move(subexpressionName),
            operandTypers = std::move(operandTypers),
            source = functionExpr->GetSource(Source_)
        ] (EValueType type) mutable -> TConstExpressionPtr {
            auto key = std::pair(subexpressionName, type);
            auto foundCached = AggregateLookup_.find(key);
            if (foundCached != AggregateLookup_.end()) {
                return foundCached->second;
            }

            EValueType stateType;
            auto effectiveTypes = RefineFunctionTypes(
                type,
                operandTypers.size(),
                &constraints,
                &stateType,
                source);

            std::vector<TConstExpressionPtr> typedOperands;
            for (int index = 0; index < std::ssize(effectiveTypes); ++index) {
                typedOperands.push_back(operandTypers[index](effectiveTypes[index]));

                typedOperands.back() = ApplyRewriters(typedOperands.back());
            }

            subexpressionName = InferName(New<TFunctionExpression>(type, functionName, typedOperands));

            AggregateItems_->emplace_back(
                typedOperands,
                functionName,
                subexpressionName,
                MakeLogicalType(GetLogicalType(stateType), /*required*/ false),
                MakeLogicalType(GetLogicalType(type), /*required*/ false));

            auto expr = New<TReferenceExpression>(
                MakeLogicalType(GetLogicalType(type), /*required*/ false),
                subexpressionName);
            AggregateLookup_.emplace(key, expr);

            return expr;
        };

        return TUntypedExpression{resultTypes, std::move(generator), /*IsConstant*/ false};
    } else {
        std::vector<TTypeSet> argTypes;
        std::vector<TExpressionGenerator> operandTypers;
        argTypes.reserve(functionExpr->Arguments.size());
        operandTypers.reserve(functionExpr->Arguments.size());
        for (const auto& argument : functionExpr->Arguments) {
            auto untypedArgument = OnExpression(argument);
            argTypes.push_back(untypedArgument.FeasibleTypes);
            operandTypers.push_back(untypedArgument.Generator);
        }

        std::vector<TTypeSet> genericAssignments;
        auto resultTypes = InferFunctionTypes(
            descriptor,
            argTypes,
            &genericAssignments,
            functionName,
            functionExpr->GetSource(Source_));

        TExpressionGenerator generator = [
            functionName,
            descriptor,
            operandTypers,
            genericAssignments,
            source = functionExpr->GetSource(Source_)
        ] (EValueType type) mutable {
            auto effectiveTypes = RefineFunctionTypes(
                descriptor,
                type,
                operandTypers.size(),
                &genericAssignments,
                functionName,
                source);

            std::vector<TConstExpressionPtr> typedOperands;
            typedOperands.reserve(std::ssize(effectiveTypes));
            for (int index = 0; index < std::ssize(effectiveTypes); ++index) {
                typedOperands.push_back(operandTypers[index](effectiveTypes[index]));
            }

            return New<TFunctionExpression>(type, functionName, std::move(typedOperands));
        };

        return TUntypedExpression{.FeasibleTypes = resultTypes, .Generator = std::move(generator), .IsConstant = false};
    }
}

TUntypedExpression TExpressionBuilderV1::OnUnaryOp(const NAst::TUnaryOpExpression* unaryExpr)
{
    if (unaryExpr->Operand.size() != 1) {
        THROW_ERROR_EXCEPTION(
            "Unary operator %Qv must have exactly one argument",
            unaryExpr->Opcode);
    }

    auto untypedOperand = OnExpression(unaryExpr->Operand.front());

    TTypeSet genericAssignments;
    auto resultTypes = InferUnaryExprTypes(
        unaryExpr->Opcode,
        untypedOperand.FeasibleTypes,
        &genericAssignments,
        unaryExpr->Operand.front()->GetSource(Source_));

    if (untypedOperand.IsConstant) {
        auto value = untypedOperand.Generator(untypedOperand.FeasibleTypes.GetFront());
        if (auto foldedExpr = FoldConstants(unaryExpr->Opcode, value)) {
            TExpressionGenerator generator = [foldedExpr] (EValueType type) {
                return New<TLiteralExpression>(
                    type,
                    CastValueWithCheck(*foldedExpr, type));
            };
            return TUntypedExpression{.FeasibleTypes = resultTypes, .Generator = std::move(generator), .IsConstant = true};
        }
    }

    TExpressionGenerator generator = [
        op = unaryExpr->Opcode,
        untypedOperand,
        genericAssignments,
        opSource = unaryExpr->Operand.front()->GetSource(Source_)
    ] (EValueType type) mutable {
        auto argType = RefineUnaryExprTypes(
            op,
            type,
            &genericAssignments,
            opSource);
        return New<TUnaryOpExpression>(type, op, untypedOperand.Generator(argType));
    };
    return TUntypedExpression{.FeasibleTypes = resultTypes, .Generator = std::move(generator), .IsConstant = false};
}

void ThrowOnCompositeScalarMismatch(
    EBinaryOp op,
    const TConstExpressionPtr& lhsExpr,
    const TConstExpressionPtr& rhsExpr,
    TStringBuf lhsSource,
    TStringBuf rhsSource)
{
    if (IsRelationalBinaryOp(op) &&
        (IsV3Composite(lhsExpr->LogicalType) ^ IsV3Composite(rhsExpr->LogicalType)))
    {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv", op)
            .With("lhs_source", lhsSource)
            .With("rhs_source", rhsSource);
    }
}

TUntypedExpression TExpressionBuilderV1::MakeBinaryExpr(
    const NAst::TBinaryOpExpression* binaryExpr,
    EBinaryOp op,
    TUntypedExpression lhs,
    TUntypedExpression rhs,
    std::optional<size_t> offset)
{
    TTypeSet genericAssignments;

    auto lhsSource = offset ? binaryExpr->Lhs[*offset]->GetSource(Source_) : "";
    auto rhsSource = offset ? binaryExpr->Rhs[*offset]->GetSource(Source_) : "";

    auto resultTypes = InferBinaryExprTypes(
        op,
        lhs.FeasibleTypes,
        rhs.FeasibleTypes,
        &genericAssignments,
        lhsSource,
        rhsSource);

    if (lhs.IsConstant && rhs.IsConstant) {
        auto lhsValue = lhs.Generator(lhs.FeasibleTypes.GetFront());
        auto rhsValue = rhs.Generator(rhs.FeasibleTypes.GetFront());
        if (auto foldedExpr = FoldConstants(op, lhsValue, rhsValue)) {
            TExpressionGenerator generator = [foldedExpr] (EValueType type) {
                return New<TLiteralExpression>(
                    type,
                    CastValueWithCheck(*foldedExpr, type));
            };
            return TUntypedExpression{.FeasibleTypes = resultTypes, .Generator = std::move(generator), .IsConstant = true};
        }
    }

    TExpressionGenerator generator = [
        op,
        lhs,
        rhs,
        genericAssignments,
        lhsSource,
        rhsSource,
        source = binaryExpr->GetSource(Source_)
    ] (EValueType type) mutable {
        auto argTypes = RefineBinaryExprTypes(
            op,
            type,
            lhs.FeasibleTypes,
            rhs.FeasibleTypes,
            &genericAssignments,
            lhsSource,
            rhsSource,
            source);

        auto lhsExpr = lhs.Generator(argTypes.first);
        auto rhsExpr = rhs.Generator(argTypes.second);

        ThrowOnCompositeScalarMismatch(op, lhsExpr, rhsExpr, lhsSource, rhsSource);

        return New<TBinaryOpExpression>(type, op, lhsExpr, rhsExpr);
    };
    return TUntypedExpression{resultTypes, std::move(generator), /*IsConstant*/ false};
}

struct TBinaryOpGenerator
{
    TExpressionBuilderV1& Builder;
    const NAst::TBinaryOpExpression* BinaryExpr;

    TUntypedExpression Do(size_t keySize, EBinaryOp op)
    {
        YT_VERIFY(keySize > 0);
        size_t offset = keySize - 1;

        auto untypedLhs = Builder.OnExpression(BinaryExpr->Lhs[offset]);
        auto untypedRhs = Builder.OnExpression(BinaryExpr->Rhs[offset]);

        auto result = Builder.MakeBinaryExpr(BinaryExpr, op, std::move(untypedLhs), std::move(untypedRhs), offset);

        while (offset > 0) {
            --offset;
            auto untypedLhs = Builder.OnExpression(BinaryExpr->Lhs[offset]);
            auto untypedRhs = Builder.OnExpression(BinaryExpr->Rhs[offset]);

            auto eq = Builder.MakeBinaryExpr(
                BinaryExpr,
                op == EBinaryOp::NotEqual ? EBinaryOp::Or : EBinaryOp::And,
                Builder.MakeBinaryExpr(
                    BinaryExpr,
                    op == EBinaryOp::NotEqual ? EBinaryOp::NotEqual : EBinaryOp::Equal,
                    untypedLhs,
                    untypedRhs,
                    offset),
                std::move(result),
                std::nullopt);

            if (op == EBinaryOp::Equal || op == EBinaryOp::NotEqual) {
                result = eq;
                continue;
            }

            EBinaryOp strongOp = op;
            if (op == EBinaryOp::LessOrEqual) {
                strongOp = EBinaryOp::Less;
            } else if (op == EBinaryOp::GreaterOrEqual)  {
                strongOp = EBinaryOp::Greater;
            }

            result = Builder.MakeBinaryExpr(
                BinaryExpr,
                EBinaryOp::Or,
                Builder.MakeBinaryExpr(
                    BinaryExpr,
                    strongOp,
                    std::move(untypedLhs),
                    std::move(untypedRhs),
                    offset),
                std::move(eq),
                std::nullopt);
        }

        return result;
    }
};

TUntypedExpression TExpressionBuilderV1::OnBinaryOp(
    const NAst::TBinaryOpExpression* binaryExpr)
{
    if (IsRelationalBinaryOp(binaryExpr->Opcode)) {
        if (binaryExpr->Lhs.size() != binaryExpr->Rhs.size()) {
            THROW_ERROR_EXCEPTION("Tuples of same size are expected but got %v vs %v",
                binaryExpr->Lhs.size(),
                binaryExpr->Rhs.size())
                .With("source", binaryExpr->GetSource(Source_));
        }

        int keySize = binaryExpr->Lhs.size();
        return TBinaryOpGenerator{*this, binaryExpr}.Do(keySize, binaryExpr->Opcode);
    } else {
        if (binaryExpr->Lhs.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                .With("source", FormatExpression(binaryExpr->Lhs));
        }

        if (binaryExpr->Rhs.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                .With("source", FormatExpression(binaryExpr->Rhs));
        }

        auto untypedLhs = OnExpression(binaryExpr->Lhs.front());
        auto untypedRhs = OnExpression(binaryExpr->Rhs.front());

        return MakeBinaryExpr(binaryExpr, binaryExpr->Opcode, std::move(untypedLhs), std::move(untypedRhs), 0);
    }
}

void TExpressionBuilderV1::InferArgumentTypes(
    std::vector<TConstExpressionPtr>* typedArguments,
    std::vector<EValueType>* argTypes,
    const NAst::TExpressionList& expressions,
    TStringBuf operatorName,
    TStringBuf source)
{
    std::unordered_set<std::string> columnNames;

    for (const auto& argument : expressions) {
        auto untypedArgument = OnExpression(argument);

        auto argType = GetFrontWithCheck(untypedArgument.FeasibleTypes, argument->GetSource(Source_));
        auto typedArgument = untypedArgument.Generator(argType);

        typedArguments->push_back(typedArgument);
        argTypes->push_back(argType);
        if (auto reference = typedArgument->As<TReferenceExpression>()) {
            if (!columnNames.insert(reference->ColumnName).second) {
                THROW_ERROR_EXCEPTION("%v operator has multiple references to column %Qv",
                    operatorName,
                    reference->ColumnName)
                    .With("source", source);
            }
        }
    }
}

TUntypedExpression TExpressionBuilderV1::OnInOp(
    const NAst::TInExpression* inExpr)
{
    std::vector<TConstExpressionPtr> typedArguments;
    std::vector<EValueType> argTypes;

    auto source = inExpr->GetSource(Source_);

    InferArgumentTypes(
        &typedArguments,
        &argTypes,
        inExpr->Expr,
        "IN",
        inExpr->GetSource(Source_));

    for (auto type : argTypes) {
        if (IsAnyOrComposite(type)) {
            THROW_ERROR_EXCEPTION("Cannot use expression of type %Qlv with IN operator", type)
                .With("source", source);
        }
    }

    auto capturedRows = LiteralTupleListToRows(inExpr->Values, argTypes, source);
    auto result = New<TInExpression>(std::move(typedArguments), std::move(capturedRows));

    TTypeSet resultTypes({EValueType::Boolean});
    TExpressionGenerator generator = [result] (EValueType /*type*/) mutable {
        return result;
    };
    return TUntypedExpression{resultTypes, std::move(generator), /*IsConstant*/ false};
}

TUntypedExpression TExpressionBuilderV1::OnBetweenOp(
    const NAst::TBetweenExpression* betweenExpr)
{
    std::vector<TConstExpressionPtr> typedArguments;
    std::vector<EValueType> argTypes;

    auto source = betweenExpr->GetSource(Source_);

    InferArgumentTypes(
        &typedArguments,
        &argTypes,
        betweenExpr->Expr,
        "BETWEEN",
        source);

    auto capturedRows = LiteralRangesListToRows(betweenExpr->Values, argTypes, source);
    auto result = New<TBetweenExpression>(std::move(typedArguments), std::move(capturedRows));

    TTypeSet resultTypes({EValueType::Boolean});
    TExpressionGenerator generator = [result] (EValueType /*type*/) mutable {
        return result;
    };
    return TUntypedExpression{resultTypes, std::move(generator), /*IsConstant*/ false};
}

TUntypedExpression TExpressionBuilderV1::OnTransformOp(
    const NAst::TTransformExpression* transformExpr)
{
    std::vector<TConstExpressionPtr> typedArguments;
    std::vector<EValueType> argTypes;

    auto source = transformExpr->GetSource(Source_);

    InferArgumentTypes(
        &typedArguments,
        &argTypes,
        transformExpr->Expr,
        "TRANSFORM",
        source);

    if (transformExpr->From.size() != transformExpr->To.size()) {
        THROW_ERROR_EXCEPTION("Size mismatch for source and result arrays in TRANSFORM operator")
            .With("source", source);
    }

    TTypeSet resultTypes({
        EValueType::Null,
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
        EValueType::Boolean,
        EValueType::String,
        EValueType::Any});

    for (const auto& tuple : transformExpr->To) {
        if (tuple.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                .With("source", source);
        }

        auto valueTypes = GetTypes(tuple.front());

        if (!Unify(&resultTypes, valueTypes)) {
            THROW_ERROR_EXCEPTION("Types mismatch in tuple")
                .With("source", source)
                .With("actual_type", ToString(valueTypes))
                .With("expected_type", ToString(resultTypes));
        }
    }

    const auto& defaultExpr = transformExpr->DefaultExpr;

    TConstExpressionPtr defaultTypedExpr;

    EValueType resultType;
    if (defaultExpr) {
        if (defaultExpr->size() != 1) {
            THROW_ERROR_EXCEPTION("Default expression must scalar")
                .With("source", source);
        }

        auto untypedArgument = OnExpression(defaultExpr->front());

        if (!Unify(&resultTypes, untypedArgument.FeasibleTypes)) {
            THROW_ERROR_EXCEPTION("Type mismatch in default expression: expected %Qlv, got %Qlv",
                resultTypes,
                untypedArgument.FeasibleTypes)
                .With("source", source);
        }

        resultType = GetFrontWithCheck(resultTypes, source);

        defaultTypedExpr = untypedArgument.Generator(resultType);
    } else {
        resultType = GetFrontWithCheck(resultTypes, source);
    }

    auto rowBuffer = New<TRowBuffer>(TQueryPreparerBufferTag());
    TUnversionedRowBuilder rowBuilder;
    std::vector<TRow> rows;
    rows.reserve(std::ssize(transformExpr->From));

    for (int index = 0; index < std::ssize(transformExpr->From); ++index) {
        const auto& sourceTuple = transformExpr->From[index];
        if (sourceTuple.size() != argTypes.size()) {
            THROW_ERROR_EXCEPTION("Arguments size mismatch in tuple")
                .With("source", source);
        }
        for (int i = 0; i < std::ssize(sourceTuple); ++i) {
            auto valueType = GetType(sourceTuple[i]);
            auto value = GetValue(sourceTuple[i]);

            if (valueType == EValueType::Null) {
                value = MakeUnversionedSentinelValue(EValueType::Null);
            } else if (valueType != argTypes[i]) {
                if (IsArithmeticType(valueType) && IsArithmeticType(argTypes[i])) {
                    value = CastValueWithCheck(value, argTypes[i]);
                } else {
                    THROW_ERROR_EXCEPTION("Types mismatch in tuple")
                        .With("source", source)
                        .With("actual_type", valueType)
                        .With("expected_type", argTypes[i]);
                }
            }
            rowBuilder.AddValue(value);
        }

        const auto& resultTuple = transformExpr->To[index];

        YT_VERIFY(resultTuple.size() == 1);
        auto value = CastValueWithCheck(GetValue(resultTuple.front()), resultType);
        rowBuilder.AddValue(value);

        rows.push_back(rowBuffer->CaptureRow(rowBuilder.GetRow()));
        rowBuilder.Reset();
    }

    std::sort(rows.begin(), rows.end(), [argCount = argTypes.size()] (TRow lhs, TRow rhs) {
        return CompareRows(lhs, rhs, argCount) < 0;
    });

    auto capturedRows = MakeSharedRange(std::move(rows), std::move(rowBuffer));
    auto result = New<TTransformExpression>(
        resultType,
        std::move(typedArguments),
        std::move(capturedRows),
        std::move(defaultTypedExpr));

    TExpressionGenerator generator = [result] (EValueType /*type*/) mutable {
        return result;
    };
    return TUntypedExpression{TTypeSet({resultType}), std::move(generator), /*IsConstant*/ false};
}

TUntypedExpression TExpressionBuilderV1::OnCaseOp(const NAst::TCaseExpression* caseExpr)
{
    if (std::ssize(caseExpr->WhenThenExpressions) > MaxWhenThenExpressionCount) {
        THROW_ERROR_EXCEPTION("Maximum WHEN/THEN expression count exceeded")
            .With("when_then_expression_count", std::ssize(caseExpr->WhenThenExpressions))
            .With("max_when_then_expression_count", MaxWhenThenExpressionCount);
    }

    auto source = caseExpr->GetSource(Source_);

    TUntypedExpression untypedOperand;
    TTypeSet operandTypes;
    bool hasOptionalOperand = false;
    {
        if (caseExpr->OptionalOperand) {
            if (caseExpr->OptionalOperand->size() != 1) {
                THROW_ERROR_EXCEPTION("Expression inside CASE should be scalar")
                    .With("source", source);
            }

            untypedOperand = OnExpression(caseExpr->OptionalOperand->front());
            operandTypes = untypedOperand.FeasibleTypes;
            hasOptionalOperand = true;
        }
    }

    std::vector<TUntypedExpression> untypedConditions;
    untypedConditions.reserve(caseExpr->WhenThenExpressions.size());
    EValueType conditionType{};
    std::optional<EValueType> operandType;
    {
        TTypeSet conditionTypes;
        if (hasOptionalOperand) {
            conditionTypes = operandTypes;
        } else {
            conditionTypes = TTypeSet({EValueType::Boolean});
        }

        for (auto& [condition, _] : caseExpr->WhenThenExpressions) {
            if (condition.size() != 1) {
                THROW_ERROR_EXCEPTION("Expression inside CASE WHEN should be scalar")
                    .With("source", source);
            }

            auto untypedExpression = OnExpression(condition.front());
            if (!Unify(&conditionTypes, untypedExpression.FeasibleTypes)) {
                THROW_ERROR_EXCEPTION("Types mismatch in CASE WHEN expression")
                    .With("source", source)
                    .With("actual_type", ToString(untypedExpression.FeasibleTypes))
                    .With("expected_type", ToString(conditionTypes));
            }

            untypedConditions.push_back(std::move(untypedExpression));
        }

        conditionType = GetFrontWithCheck(conditionTypes, source);

        if (hasOptionalOperand) {
            operandType = conditionType;
        }
    }

    std::vector<TUntypedExpression> untypedResults;
    untypedResults.reserve(caseExpr->WhenThenExpressions.size());
    TUntypedExpression untypedDefault{};
    EValueType resultType{};
    {
        TTypeSet resultTypes({
            EValueType::Null,
            EValueType::Int64,
            EValueType::Uint64,
            EValueType::Double,
            EValueType::Boolean,
            EValueType::String,
            EValueType::Any});

        for (auto& [_, result] : caseExpr->WhenThenExpressions) {
            if (result.size() != 1) {
                THROW_ERROR_EXCEPTION("Expression inside CASE THEN should be scalar")
                    .With("source", source);
            }

            auto untypedExpression = OnExpression(result.front());
            if (!Unify(&resultTypes, untypedExpression.FeasibleTypes)) {
                THROW_ERROR_EXCEPTION("Types mismatch in CASE THEN/ELSE expression")
                    .With("source", source)
                    .With("actual_type", ToString(untypedExpression.FeasibleTypes))
                    .With("expected_type", ToString(resultTypes));
            }

            untypedResults.push_back(std::move(untypedExpression));
        }

        if (caseExpr->DefaultExpression) {
            if (caseExpr->DefaultExpression->size() != 1) {
                THROW_ERROR_EXCEPTION("Expression inside CASE ELSE should be scalar")
                    .With("source", source);
            }

            untypedDefault = OnExpression(caseExpr->DefaultExpression->front());
            if (!Unify(&resultTypes, untypedDefault.FeasibleTypes)) {
                THROW_ERROR_EXCEPTION("Types mismatch in CASE THEN/ELSE expression")
                    .With("source", source)
                    .With("actual_type", ToString(untypedDefault.FeasibleTypes))
                    .With("expected_type", ToString(resultTypes));
            }
        }

        resultType = GetFrontWithCheck(resultTypes, source);
    }

    TConstExpressionPtr typedOptionalOperand;
    std::vector<TWhenThenExpressionPtr> typedWhenThenExpressions;
    typedWhenThenExpressions.reserve(caseExpr->WhenThenExpressions.size());
    TConstExpressionPtr typedDefaultExpression;
    {
        if (hasOptionalOperand) {
            typedOptionalOperand = untypedOperand.Generator(*operandType);
        }

        for (size_t index = 0; index < untypedConditions.size(); ++index) {
            typedWhenThenExpressions.push_back(New<TWhenThenExpression>(
                untypedConditions[index].Generator(conditionType),
                untypedResults[index].Generator(resultType)));
        }

        if (caseExpr->DefaultExpression) {
            typedDefaultExpression = untypedDefault.Generator(resultType);
        }
    }

    auto result = New<TCaseExpression>(
        resultType,
        std::move(typedOptionalOperand),
        std::move(typedWhenThenExpressions),
        std::move(typedDefaultExpression));

    TExpressionGenerator generator = [result] (EValueType /*type*/) mutable {
        return result;
    };

    return TUntypedExpression{TTypeSet({resultType}), std::move(generator), /*IsConstant*/ false};
}

TUntypedExpression TExpressionBuilderV1::OnLikeOp(const NAst::TLikeExpression* likeExpr)
{
    auto source = likeExpr->GetSource(Source_);

    if (likeExpr->Opcode == EStringMatchOp::Regex && likeExpr->EscapeCharacter) {
        THROW_ERROR_EXCEPTION("ESCAPE should not be used together with REGEXP (RLIKE)")
            .With("source", source);
    }

    auto makeTypedStringExpression = [this] (
        const NAst::TExpressionList& expression,
        TStringBuf name,
        TStringBuf source)
    {
        auto stringTypes = TTypeSet({EValueType::String,});

        if (expression.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar %Qv expression",
                name)
                .With("source", source);
        }

        auto untypedExpression = OnExpression(expression.front());
        if (!Unify(&stringTypes, untypedExpression.FeasibleTypes)) {
            THROW_ERROR_EXCEPTION("Types mismatch in %v", name)
                .With("source", source)
                .With("actual_type", ToString(untypedExpression.FeasibleTypes))
                .With("expected_type", ToString(stringTypes));
        }

        return untypedExpression.Generator(EValueType::String);
    };

    auto typedText = makeTypedStringExpression(likeExpr->Text, "LIKE matched value", source);
    auto typedPattern = makeTypedStringExpression(likeExpr->Pattern, "LIKE pattern", source);

    TConstExpressionPtr typedEscapeCharacter;
    if (likeExpr->EscapeCharacter) {
        typedEscapeCharacter = makeTypedStringExpression(likeExpr->EscapeCharacter.value(), "escape character", source);
    }

    auto result = New<TLikeExpression>(
        std::move(typedText),
        likeExpr->Opcode,
        std::move(typedPattern),
        std::move(typedEscapeCharacter));

    TExpressionGenerator generator = [result] (EValueType /*type*/) {
        return result;
    };

    return TUntypedExpression{TTypeSet({EValueType::Boolean}), std::move(generator), /*IsConstant*/ false};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
