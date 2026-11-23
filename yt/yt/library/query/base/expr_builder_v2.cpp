#include "expr_builder_base.h"
#include "functions.h"
#include "helpers.h"
#include "private.h"
#include "query_helpers.h"
#include "query_preparer.h"

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

// Types (t1, t2) unified to t2 if there is cast t1 -> t2.
// Types (t1, t2, t3) are not unified to t3 with casts t1 -> t2 and t2 -> t3 because no direct cast t1 -> t3.
// Types (t1, t2, t3) unified to t2 with t2 -> t3 and t1 -> t3.

TTypeId UnifyTypes(TStringBuf name, TTypingCtx* typingCtx, TRange<TTypeId> types)
{
    // One generic parameter.
    std::vector<TTypeId> signature(std::ssize(types), -1);

    return typingCtx->InferFunctionType(name, {TTypingCtx::TFunctionSignature(signature)}, types, 0)[0];
}

////////////////////////////////////////////////////////////////////////////////

TConstExpressionPtr CreateCoercion(EValueType type, TConstExpressionPtr operand)
{
    if (const auto* literalExpr = operand->As<TLiteralExpression>()) {
        TValue value = literalExpr->Value;

        return New<TLiteralExpression>(
            type,
            CastValueWithCheck(value, type));
    }

    if (GetWireType(operand->LogicalType) == type) {
        return operand;
    }

    if (GetWireType(operand->LogicalType) == EValueType::Composite && type == EValueType::Any) {
        return operand;
    }

    const char* castName;
    if (type == EValueType::Int64) {
        castName = "int64";
    } else if (type == EValueType::Uint64) {
        castName = "uint64";
    } else if (type == EValueType::Double) {
        castName = "double";
    } else if (type == EValueType::Boolean) {
        castName = "boolean";
    } else if (type == EValueType::String) {
        castName = "string";
    } else if (type == EValueType::Any) {
        castName = "to_any";
    } else {
        THROW_ERROR_EXCEPTION("No cast from %v to %Qlv", *operand->LogicalType, type);
    }

    return New<TFunctionExpression>(
        type,
        castName,
        std::vector<TConstExpressionPtr>{operand});
}

////////////////////////////////////////////////////////////////////////////////

struct TAliasResolver
{
    const NAst::TAliasMap& AliasMap;
    std::set<std::string> UsedAliases;

    TReferenceResolver ColumnResolver;

    bool AfterGroupBy = false;
    bool NeedSubstitute = false;
    ETotalsMode TotalsMode = ETotalsMode::None;

    const TNamedItemList* GroupItems = nullptr;
    TAggregateItemList* AggregateItems = nullptr;

    THashMap<std::string, TConstExpressionPtr> AggregateLookup;

    explicit TAliasResolver(const NAst::TAliasMap& aliasMap)
        : AliasMap(aliasMap)
    { }
};

class TExpressionBuilderV2
    : public TExpressionBuilder
{
public:
    TExpressionBuilderV2(
        TStringBuf source,
        const TConstTypeInferrerMapPtr& functions,
        const NAst::TAliasMap& aliasMap,
        const TPreparePlanFragmentContext& context)
        : TExpressionBuilder(source, functions, context)
    {
        PushAliasResolver(aliasMap);
    }

    void PushAliasResolver(const NAst::TAliasMap& aliasMap)
    {
        AliasResolvers_.push_back(std::make_unique<TAliasResolver>(aliasMap));
    }

    void PopAliasResolver()
    {
        AliasResolvers_.pop_back();
    }

    void AddTable(TNameSource nameSource) override
    {
        AliasResolvers_.back()->ColumnResolver.AddTable(std::move(nameSource));
    }

    TLogicalTypePtr ResolveColumn(const NAst::TColumnReference& reference) override
    {
        return AliasResolvers_.back()->ColumnResolver.Resolve(reference);
    }

    void PopulateAllColumns() override
    {
        AliasResolvers_.back()->ColumnResolver.PopulateAllColumns();
    }

    TConstExpressionPtr BuildTypedExpression(
        const NAst::TExpression* expr, TRange<EValueType> /*resultTypes*/) override
    {
        auto result = OnExpression(expr);

        for (const auto& aliasResolver : AliasResolvers_) {
            if (aliasResolver->NeedSubstitute) {
                THROW_ERROR_EXCEPTION("Expression or its parts are not in GROUP BY keys")
                    .With("expression", InferName(result));
            }
        }

        return result;
    }

    void Finish() override
    {
        AliasResolvers_.back()->ColumnResolver.Finish();
    }

    std::string InferGroupItemName(
        const TConstExpressionPtr& /*typedExpression*/,
        const NAst::TExpression& expressionsAst) override
    {
        return InferColumnName(expressionsAst);
    }

    void SetGroupData(const TNamedItemList* groupItems, TAggregateItemList* aggregateItems, ETotalsMode totalsMode) override
    {
        auto* aliasResolver = AliasResolvers_.back().get();

        YT_VERIFY(!aliasResolver->GroupItems && !aliasResolver->AggregateItems);

        aliasResolver->GroupItems = groupItems;
        aliasResolver->AggregateItems = aggregateItems;
        aliasResolver->AfterGroupBy = true;
        aliasResolver->TotalsMode = totalsMode;
    }

private:
    struct ResolveNestedTypesResult
    {
        TCompositeMemberAccessorPath NestedStructOrTupleItemAccessor;
        TLogicalTypePtr IntermediateType;
        TLogicalTypePtr ResultType;
    };

    TTypingCtx TypingCtx_;

    std::vector<std::unique_ptr<TAliasResolver>> AliasResolvers_;

    int Depth_ = 0;

    TConstExpressionPtr DoOnExpression(
        const NAst::TExpression* expr);

    TConstExpressionPtr OnExpression(
        const NAst::TExpression* expr);

    ResolveNestedTypesResult ResolveNestedTypes(
        const TLogicalTypePtr& type,
        const NAst::TReference& reference);

    TConstExpressionPtr UnwrapListOrDictItemAccessor(
        const NAst::TReference& reference,
        ELogicalMetatype metaType);

    TConstExpressionPtr OnColumnReference(
        const NAst::TColumnReference& reference);

    TConstExpressionPtr OnReference(
        const NAst::TReference& reference);

    TConstExpressionPtr OnFunction(
        const NAst::TFunctionExpression* functionExpr);

    TConstExpressionPtr OnUnaryOp(
        const NAst::TUnaryOpExpression* unaryExpr);

    TConstExpressionPtr MakeBinaryExpr(
        const NAst::TBinaryOpExpression* binaryExpr,
        EBinaryOp op,
        TConstExpressionPtr lhs,
        TConstExpressionPtr rhs,
        std::optional<size_t> offset);

    friend struct TBinaryOpGeneratorV2;

    TConstExpressionPtr OnBinaryOp(
        const NAst::TBinaryOpExpression* binaryExpr);

    void InferArgumentTypes(
        std::vector<TConstExpressionPtr>* typedArguments,
        std::vector<EValueType>* argTypes,
        const NAst::TExpressionList& expressions,
        TStringBuf operatorName,
        TStringBuf source);

    TConstExpressionPtr OnInOp(
        const NAst::TInExpression* inExpr);

    TConstExpressionPtr OnBetweenOp(
        const NAst::TBetweenExpression* betweenExpr);

    TConstExpressionPtr OnTransformOp(
        const NAst::TTransformExpression* transformExpr);

    TConstExpressionPtr OnCaseOp(
        const NAst::TCaseExpression* caseExpr);

    TConstExpressionPtr OnLikeOp(
        const NAst::TLikeExpression* likeExpr);

    TConstExpressionPtr OnQueryOp(
        const NAst::TQueryExpression* queryExpr);
};

std::unique_ptr<TExpressionBuilder> CreateExpressionBuilderV2(
    TStringBuf source,
    const TConstTypeInferrerMapPtr& functions,
    const NAst::TAliasMap& aliasMap,
    const TPreparePlanFragmentContext& context)
{
    return std::make_unique<TExpressionBuilderV2>(source, functions, aliasMap, context);
}

TConstExpressionPtr TExpressionBuilderV2::DoOnExpression(
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

        auto type = GetType(literalValue);

        return New<TLiteralExpression>(
            type,
            CastValueWithCheck(GetValue(literalValue), type));
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
    } else if (auto queryExpr = expr->As<NAst::TQueryExpression>()) {
        return OnQueryOp(queryExpr);
    }

    YT_ABORT();
}

TConstExpressionPtr TExpressionBuilderV2::OnExpression(
    const NAst::TExpression* expr)
{
    auto result = DoOnExpression(expr);

    return result;
}

TExpressionBuilderV2::ResolveNestedTypesResult TExpressionBuilderV2::ResolveNestedTypes(
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

TConstExpressionPtr TExpressionBuilderV2::UnwrapListOrDictItemAccessor(
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

    EValueType expectedType;
    if (metaType == ELogicalMetatype::List) {
        expectedType = EValueType::Int64;
    } else if (metaType == ELogicalMetatype::Dict) {
        expectedType = EValueType::String;
    } else {
        YT_ABORT();
    }

    auto typedExpression = OnExpression(itemIndex.front());
    if (typedExpression->GetWireType() != expectedType) {
        THROW_ERROR_EXCEPTION("Incorrect type inside of the list or dict item accessor")
            .With("source", NAst::FormatReference(reference))
            .With("actual_type", ToString(expectedType))
            .With("expected_type", ToString(typedExpression->GetWireType()));
    }

    return typedExpression;
}

TConstExpressionPtr TExpressionBuilderV2::OnColumnReference(const NAst::TColumnReference& reference)
{
    if (AliasResolvers_.empty()) {
        THROW_ERROR_EXCEPTION("Undefined reference %Qv",
            InferColumnName(reference));
    }

    auto* aliasResolver = AliasResolvers_.back().get();

    if (aliasResolver->AfterGroupBy) {
        auto* groupItems = aliasResolver->GroupItems;
        YT_VERIFY(groupItems);

        if (!reference.TableName) {
            for (const auto& [expression, name] : *groupItems) {
                // TODO(lukyan): InferColumnName(reference) instead of reference.ColumnName?
                if (name == reference.ColumnName) {
                    auto type = expression->LogicalType;
                    if (aliasResolver->TotalsMode != ETotalsMode::None) {
                        type = MakeOptionalIfNot(type);
                    }
                    return New<TReferenceExpression>(type, reference.ColumnName);
                }
            }
        }
    }

    if (!reference.TableName) {
        const auto& columnName = reference.ColumnName;
        auto found = aliasResolver->AliasMap.find(columnName);

        if (found != aliasResolver->AliasMap.end()) {
            // try InferName(found, expand aliases = true)

            if (aliasResolver->UsedAliases.insert(columnName).second) {
                auto aliasExpr = OnExpression(found->second);
                aliasResolver->UsedAliases.erase(columnName);
                return aliasExpr;
            }
        }
    }

    // Need substitute for expressions inside aggregate functions.
    if (auto type = aliasResolver->ColumnResolver.Resolve(reference)) {
        if (aliasResolver->AfterGroupBy) {
            // Cannot increase resolver level.
            for (int index = 0; index + 1 < std::ssize(AliasResolvers_); ++index) {
                if (AliasResolvers_[index]->NeedSubstitute) {
                    THROW_ERROR_EXCEPTION("Misuse of columns under group by")
                        .With("column", InferColumnName(reference));
                }
            }

            aliasResolver->NeedSubstitute = true;
        }

        // OnExpression.
        return New<TReferenceExpression>(type, InferColumnName(reference));
    }

    // Lookup in parent alias resolver.
    auto saveAliasResolver = std::move(AliasResolvers_.back());
    AliasResolvers_.pop_back();
    auto result = OnColumnReference(reference);
    AliasResolvers_.push_back(std::move(saveAliasResolver));

    return result;
}

TConstExpressionPtr TExpressionBuilderV2::OnReference(const NAst::TReference& reference)
{
    if (reference.TableName && !ResolveColumn(reference)) {
        if (auto reinterpreted = TryReinterpretAsMemberAccess(reference);
            reinterpreted && ResolveColumn(*reinterpreted))
        {
            return OnReference(*reinterpreted);
        }
    }

    auto referenceExpr = OnColumnReference(reference);

    if (reference.CompositeTypeAccessor.IsEmpty()) {
        return referenceExpr;
    }

    auto resolved = ResolveNestedTypes(referenceExpr->LogicalType, reference);
    auto listOrDictItemAccessor = UnwrapListOrDictItemAccessor(reference, resolved.IntermediateType->GetMetatype());

    if (listOrDictItemAccessor && listOrDictItemAccessor->LogicalType->IsNullable()) {
        resolved.ResultType = MakeOptionalIfNot(resolved.ResultType);
    }

    auto memberAccessor = New<TCompositeMemberAccessorExpression>(
        resolved.ResultType,
        referenceExpr,
        std::move(resolved.NestedStructOrTupleItemAccessor),
        listOrDictItemAccessor);

    return memberAccessor;
}

TConstExpressionPtr TExpressionBuilderV2::OnFunction(const NAst::TFunctionExpression* functionExpr)
{
    auto functionName = ToLower(functionExpr->FunctionName);

    if (functionName == "cast_operator") {
        THROW_ERROR_EXCEPTION_IF(functionExpr->Arguments.size() != 2,
            "Expected two arguments for %Qv function, got %v",
            functionName,
            functionExpr->Arguments.size());

        auto* literalArgument = functionExpr->Arguments[1]->As<NAst::TLiteralExpression>();

        THROW_ERROR_EXCEPTION_UNLESS(literalArgument && std::holds_alternative<std::string>(literalArgument->Value),
            "Misuse of function %Qv",
            functionName);

        return New<TFunctionExpression>(
            NTableClient::ParseType(std::get<std::string>(literalArgument->Value)),
            functionName,
            std::vector<TConstExpressionPtr>{OnExpression(functionExpr->Arguments[0])});
    }

    const auto& descriptor = Functions_->GetFunction(functionName);

    std::vector<TConstExpressionPtr> typedOperands;
    std::vector<TLogicalTypePtr> argumentTypes;

    auto getArguments = [&] {
        typedOperands.reserve(functionExpr->Arguments.size());
        argumentTypes.reserve(functionExpr->Arguments.size());
        for (const auto& argument : functionExpr->Arguments) {
            auto typedArgument = OnExpression(argument);
            argumentTypes.push_back(typedArgument->LogicalType);
            typedOperands.push_back(std::move(typedArgument));
        }
    };

    // Regular function.
    if (!descriptor->IsAggregate()) {
        getArguments();

        auto functionTypes = descriptor->InferTypes(
            &TypingCtx_,
            argumentTypes,
            functionName);

        for (int i = 0; i < std::ssize(typedOperands); ++i) {
            auto operandType = TypingCtx_.GetTypeId(typedOperands[i]->LogicalType);
            if (operandType != functionTypes[i + 1]) {
                auto type = TypingCtx_.GetWireType(functionTypes[i + 1]);
                typedOperands[i] = CreateCoercion(type, typedOperands[i]);
            }
        }

        return New<TFunctionExpression>(TypingCtx_.GetLogicalType(functionTypes[0]), functionName, typedOperands);
    } else {
        // Aggregate function.

        std::vector<std::unique_ptr<TAliasResolver>> poppedAliasResolvers;

        // Upper aggregation level is determined by nesting level of aggregate functions.
        while (!AliasResolvers_.empty()) {
            auto* aliasResolver = AliasResolvers_.back().get();

            if (aliasResolver->AfterGroupBy) {
                break;
            } else {
                poppedAliasResolvers.push_back(std::move(AliasResolvers_.back()));
                AliasResolvers_.pop_back();
            }
        }

        if (AliasResolvers_.empty()) {
            THROW_ERROR_EXCEPTION("Misuse of aggregate function %Qv", functionName)
                .With("expression", InferColumnName(*functionExpr));
        }

        auto* aliasResolver = AliasResolvers_.back().get();

        aliasResolver->AfterGroupBy = false;
        getArguments();
        aliasResolver->AfterGroupBy = true;

        // Determine aggregation level by substitution level.
        // Example query:
        // sel (sel n.a + sum(o.y), sum(n.b) from array_agg(o.z) as n group n.a) from ... as o group by o.x
        // sum(o.y) is outer aggregate because o.y is not outer group key and cannot be used without aggregation.
        // sum(n.b) is inner aggregate.

        for (const auto& currentAliasResolver : AliasResolvers_) {
            // Find level by substitute flags.
            if (currentAliasResolver->NeedSubstitute) {
                aliasResolver = currentAliasResolver.get();
                break;
            }
        }

        aliasResolver->NeedSubstitute = false;

        while (!poppedAliasResolvers.empty()) {
            AliasResolvers_.push_back(std::move(poppedAliasResolvers.back()));
            poppedAliasResolvers.pop_back();
        }

        std::vector<TTypeId> inferredTypes;

        inferredTypes = descriptor->InferTypes(
            &TypingCtx_,
            argumentTypes,
            functionName);

        for (int i = 0; i < std::ssize(typedOperands); ++i) {
            auto operandType = TypingCtx_.GetTypeId(typedOperands[i]->LogicalType);
            if (operandType != inferredTypes[i + 2]) {
                auto type = TypingCtx_.GetWireType(inferredTypes[i + 2]);
                typedOperands[i] = CreateCoercion(type, typedOperands[i]);
            }
        }

        auto resultType = TypingCtx_.GetLogicalType(inferredTypes[0]);
        auto stateType = TypingCtx_.GetLogicalType(inferredTypes[1]);

        auto subexpressionName = InferName(New<TFunctionExpression>(resultType, functionName, typedOperands));

        auto found = aliasResolver->AggregateLookup.find(subexpressionName);
        if (found != aliasResolver->AggregateLookup.end()) {
            return found->second;
        } else {
            aliasResolver->AggregateItems->emplace_back(
                typedOperands,
                functionName,
                subexpressionName,
                stateType,
                resultType);

            auto expr = New<TReferenceExpression>(
                resultType,
                subexpressionName);
            YT_VERIFY(aliasResolver->AggregateLookup.emplace(subexpressionName, expr).second);
            return expr;
        }
    }
}

TConstExpressionPtr TExpressionBuilderV2::OnUnaryOp(const NAst::TUnaryOpExpression* unaryExpr)
{
    if (unaryExpr->Operand.size() != 1) {
        THROW_ERROR_EXCEPTION(
            "Unary operator %Qv must have exactly one argument",
            unaryExpr->Opcode);
    }

    auto operand = OnExpression(unaryExpr->Operand.front());

    std::vector<TTypeId> types;

    try {
        types = TypingCtx_.InferFunctionType(
            GetUnaryOpcodeLexeme(unaryExpr->Opcode),
            {TypingCtx_.GetTypeId(operand->GetWireType())});
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv",
            GetUnaryOpcodeLexeme(unaryExpr->Opcode))
            .With("source", unaryExpr->Operand.front()->GetSource(Source_))
            .With(ex);;
    }

    YT_VERIFY(std::ssize(types) == 2);
    auto type = TypingCtx_.GetWireType(types[0]);

    if (auto foldedExpr = FoldConstants(unaryExpr->Opcode, operand)) {
        return New<TLiteralExpression>(
            type,
            CastValueWithCheck(*foldedExpr, type));
    }

    if (TypingCtx_.GetTypeId(operand->LogicalType) != types[1]) {
        operand = CreateCoercion(TypingCtx_.GetWireType(types[1]), operand);
    }

    return New<TUnaryOpExpression>(
        type,
        unaryExpr->Opcode,
        operand);
}

TConstExpressionPtr TExpressionBuilderV2::MakeBinaryExpr(
    const NAst::TBinaryOpExpression* binaryExpr,
    EBinaryOp op,
    TConstExpressionPtr typedLhs,
    TConstExpressionPtr typedRhs,
    std::optional<size_t> offset)
{
    auto lhsSource = offset ? binaryExpr->Lhs[*offset]->GetSource(Source_) : "";
    auto rhsSource = offset ? binaryExpr->Rhs[*offset]->GetSource(Source_) : "";

    // TODO(lukyan): Use LogicalType and remove GetWireType.
    // Currently there are issues with defined function signatures.
    // Functions are defined in terms of EValueType. Need signatures for int32, int64...
    // TODO(lukyan): Consider coercions of references in range inference.

    auto lhsTypeId = TypingCtx_.GetTypeId(typedLhs->LogicalType);
    auto rhsTypeId = TypingCtx_.GetTypeId(typedRhs->LogicalType);

    std::vector<TTypeId> types;
    try {
        types = TypingCtx_.InferFunctionType(
            GetBinaryOpcodeLexeme(op),
            {
                lhsTypeId,
                rhsTypeId});
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Type mismatch in expression %Qv",
            GetBinaryOpcodeLexeme(op))
            .With("lhs_source", lhsSource)
            .With("rhs_source", rhsSource)
            .With("lhs_type", TypingCtx_.GetLogicalType(lhsTypeId))
            .With("rhs_type", TypingCtx_.GetLogicalType(rhsTypeId))
            .With(ex);
    }

    YT_VERIFY(std::ssize(types) == 3);
    auto type = TypingCtx_.GetWireType(types[0]);

    if (auto foldedExpr = FoldConstants(op, typedLhs, typedRhs)) {
        return New<TLiteralExpression>(
            type,
            CastValueWithCheck(*foldedExpr, type));
    }

    if (TypingCtx_.GetTypeId(typedLhs->LogicalType) != types[1]) {
        typedLhs = CreateCoercion(TypingCtx_.GetWireType(types[1]), typedLhs);
    }

    if (TypingCtx_.GetTypeId(typedRhs->LogicalType) != types[2]) {
        typedRhs = CreateCoercion(TypingCtx_.GetWireType(types[2]), typedRhs);
    }

    return New<TBinaryOpExpression>(
        type,
        op,
        typedLhs,
        typedRhs);
}

struct TBinaryOpGeneratorV2
{
    TExpressionBuilderV2& Builder;
    const NAst::TBinaryOpExpression* BinaryExpr;

    TConstExpressionPtr Do(size_t keySize, EBinaryOp op)
    {
        YT_VERIFY(keySize > 0);
        size_t offset = keySize - 1;

        auto typedLhs = Builder.OnExpression(BinaryExpr->Lhs[offset]);
        auto typedRhs = Builder.OnExpression(BinaryExpr->Rhs[offset]);

        auto result = Builder.MakeBinaryExpr(BinaryExpr, op, std::move(typedLhs), std::move(typedRhs), offset);

        while (offset > 0) {
            --offset;
            auto typedLhs = Builder.OnExpression(BinaryExpr->Lhs[offset]);
            auto typedRhs = Builder.OnExpression(BinaryExpr->Rhs[offset]);

            auto eq = Builder.MakeBinaryExpr(
                BinaryExpr,
                op == EBinaryOp::NotEqual ? EBinaryOp::Or : EBinaryOp::And,
                Builder.MakeBinaryExpr(
                    BinaryExpr,
                    op == EBinaryOp::NotEqual ? EBinaryOp::NotEqual : EBinaryOp::Equal,
                    typedLhs,
                    typedRhs,
                    offset),
                std::move(result),
                std::nullopt);

            if (op == EBinaryOp::Equal || op == EBinaryOp::NotEqual) {
                result = eq;
                continue;
            }

            EBinaryOp strictOp = op;
            if (op == EBinaryOp::LessOrEqual) {
                strictOp = EBinaryOp::Less;
            } else if (op == EBinaryOp::GreaterOrEqual)  {
                strictOp = EBinaryOp::Greater;
            }

            result = Builder.MakeBinaryExpr(
                BinaryExpr,
                EBinaryOp::Or,
                Builder.MakeBinaryExpr(
                    BinaryExpr,
                    strictOp,
                    std::move(typedLhs),
                    std::move(typedRhs),
                    offset),
                std::move(eq),
                std::nullopt);
        }

        return result;
    }
};

TConstExpressionPtr TExpressionBuilderV2::OnBinaryOp(
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
        return TBinaryOpGeneratorV2{*this, binaryExpr}.Do(keySize, binaryExpr->Opcode);
    } else {
        if (binaryExpr->Lhs.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                .With("source", FormatExpression(binaryExpr->Lhs));
        }

        if (binaryExpr->Rhs.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                .With("source", FormatExpression(binaryExpr->Rhs));
        }

        auto typedLhs = OnExpression(binaryExpr->Lhs.front());
        auto typedRhs = OnExpression(binaryExpr->Rhs.front());

        return MakeBinaryExpr(binaryExpr, binaryExpr->Opcode, std::move(typedLhs), std::move(typedRhs), 0);
    }
}

void TExpressionBuilderV2::InferArgumentTypes(
    std::vector<TConstExpressionPtr>* typedArguments,
    std::vector<EValueType>* argTypes,
    const NAst::TExpressionList& expressions,
    TStringBuf operatorName,
    TStringBuf source)
{
    std::unordered_set<std::string> columnNames;

    for (const auto& argument : expressions) {
        auto typedArgument = OnExpression(argument);

        if (auto reference = typedArgument->As<TReferenceExpression>()) {
            if (!columnNames.insert(reference->ColumnName).second) {
                THROW_ERROR_EXCEPTION("%v operator has multiple references to column %Qv",
                    operatorName,
                    reference->ColumnName)
                    .With("source", source);
            }
        }

        argTypes->push_back(typedArgument->GetWireType());
        typedArguments->push_back(std::move(typedArgument));
    }
}

TConstExpressionPtr TExpressionBuilderV2::OnInOp(
    const NAst::TInExpression* inExpr)
{
    auto source = inExpr->GetSource(Source_);

    std::vector<TConstExpressionPtr> typedArguments;
    std::vector<EValueType> argTypes;

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
    return New<TInExpression>(std::move(typedArguments), std::move(capturedRows));
}

TConstExpressionPtr TExpressionBuilderV2::OnBetweenOp(
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
    return New<TBetweenExpression>(std::move(typedArguments), std::move(capturedRows));
}

TConstExpressionPtr TExpressionBuilderV2::OnTransformOp(
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

    std::vector<TTypeId> resultTypes;

    for (const auto& tuple : transformExpr->To) {
        if (tuple.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar expression")
                .With("source", source);
        }

        resultTypes.push_back(TypingCtx_.GetTypeId(GetType(tuple.front())));
    }

    TConstExpressionPtr defaultTypedExpr;

    if (const auto& defaultExpr = transformExpr->DefaultExpr) {
        if (defaultExpr->size() != 1) {
            THROW_ERROR_EXCEPTION("Default expression must scalar")
                .With("source", source);
        }

        defaultTypedExpr = OnExpression(defaultExpr->front());
        resultTypes.push_back(TypingCtx_.GetTypeId(defaultTypedExpr->GetWireType()));
    }

    std::sort(resultTypes.begin(), resultTypes.end());
    resultTypes.erase(std::unique(resultTypes.begin(), resultTypes.end()), resultTypes.end());

    // TODO(lukyan): On error find first occurrence of value with no coercion.
    auto resultType = TypingCtx_.GetWireType(UnifyTypes("transform", &TypingCtx_, resultTypes));

    if (defaultTypedExpr && TypingCtx_.GetTypeId(resultType) != TypingCtx_.GetTypeId(defaultTypedExpr->GetWireType())) {
        defaultTypedExpr = CreateCoercion(resultType, defaultTypedExpr);
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
    return New<TTransformExpression>(
        resultType,
        std::move(typedArguments),
        std::move(capturedRows),
        std::move(defaultTypedExpr));
}

TConstExpressionPtr TExpressionBuilderV2::OnCaseOp(const NAst::TCaseExpression* caseExpr)
{
    if (std::ssize(caseExpr->WhenThenExpressions) > MaxWhenThenExpressionCount) {
        THROW_ERROR_EXCEPTION("Maximum WHEN/THEN expression count exceeded")
            .With("when_then_expression_count", std::ssize(caseExpr->WhenThenExpressions))
            .With("max_when_then_expression_count", MaxWhenThenExpressionCount);
    }

    auto source = caseExpr->GetSource(Source_);

    TConstExpressionPtr typedOptionalOperand;
    if (caseExpr->OptionalOperand) {
        if (caseExpr->OptionalOperand->size() != 1) {
            THROW_ERROR_EXCEPTION("Expression inside CASE should be scalar")
                .With("source", source);
        }

        typedOptionalOperand = OnExpression(caseExpr->OptionalOperand->front());
    }

    std::vector<TWhenThenExpressionPtr> typedWhenThenExpressions;
    typedWhenThenExpressions.reserve(caseExpr->WhenThenExpressions.size());

    auto expectedConditionType = TypingCtx_.GetTypeId(typedOptionalOperand
        ? typedOptionalOperand->GetWireType()
        : EValueType::Boolean);

    std::vector<TTypeId> resultTypes;

    for (const auto& [when, then] : caseExpr->WhenThenExpressions) {
        if (when.size() != 1) {
            THROW_ERROR_EXCEPTION("Expression inside CASE WHEN should be scalar")
                .With("source", source);
        }

        auto whenExpression = OnExpression(when.front());
        auto actualConditionType = TypingCtx_.GetTypeId(whenExpression->GetWireType());

        try {
            // Ignore result, only check.
            TypingCtx_.InferFunctionType(
                "when",
                {TTypingCtx::TFunctionSignature({expectedConditionType})},
                {actualConditionType},
                0);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Types mismatch in CASE WHEN expression")
                .With(ex);
        }

        if (then.size() != 1) {
            THROW_ERROR_EXCEPTION("Expression inside CASE THEN should be scalar")
                .With("source", source);
        }

        if (actualConditionType != expectedConditionType) {
            whenExpression = CreateCoercion(TypingCtx_.GetWireType(expectedConditionType), whenExpression);
        }

        auto thenExpression = OnExpression(then.front());

        resultTypes.push_back(TypingCtx_.GetTypeId(thenExpression->GetWireType()));

        typedWhenThenExpressions.push_back(
            New<TWhenThenExpression>(std::move(whenExpression), std::move(thenExpression)));
    }

    TConstExpressionPtr typedDefaultExpression;
    if (caseExpr->DefaultExpression) {
        if (caseExpr->DefaultExpression->size() != 1) {
            THROW_ERROR_EXCEPTION("Expression inside CASE ELSE should be scalar")
                .With("source", source);
        }

        typedDefaultExpression = OnExpression(caseExpr->DefaultExpression->front());

        resultTypes.push_back(TypingCtx_.GetTypeId(typedDefaultExpression->GetWireType()));
    }

    std::sort(resultTypes.begin(), resultTypes.end());
    resultTypes.erase(std::unique(resultTypes.begin(), resultTypes.end()), resultTypes.end());

    // TODO(lukyan): On error find first occurrence of value with no coercion.
    TTypeId resultType;
    try {
        resultType = UnifyTypes("case", &TypingCtx_, resultTypes);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Types mismatch in CASE THEN/ELSE expression")
            .With(ex);
    }

    for (auto typedWhenThen : typedWhenThenExpressions) {
        auto thenResultType = TypingCtx_.GetTypeId(typedWhenThen->Result->GetWireType());

        if (resultType != thenResultType) {
            typedWhenThen->Result = CreateCoercion(TypingCtx_.GetWireType(resultType), typedWhenThen->Result);
        }
    }

    if (typedDefaultExpression &&
        resultType != TypingCtx_.GetTypeId(typedDefaultExpression->GetWireType()))
    {
        typedDefaultExpression = CreateCoercion(TypingCtx_.GetWireType(resultType), typedDefaultExpression);
    }

    return New<TCaseExpression>(
        TypingCtx_.GetWireType(resultType),
        std::move(typedOptionalOperand),
        std::move(typedWhenThenExpressions),
        std::move(typedDefaultExpression));
}

TConstExpressionPtr TExpressionBuilderV2::OnLikeOp(const NAst::TLikeExpression* likeExpr)
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
        if (expression.size() != 1) {
            THROW_ERROR_EXCEPTION("Expecting scalar %Qv expression",
                name)
                .With("source", source);
        }

        auto typedExpression = OnExpression(expression.front());

        if (typedExpression->GetWireType() != EValueType::String && typedExpression->GetWireType() != EValueType::Null) {
            THROW_ERROR_EXCEPTION("Types mismatch in %v", name)
                .With("source", source)
                .With("actual_type", typedExpression->GetWireType())
                .With("expected_type", EValueType::String);
        }

        return typedExpression;
    };

    auto typedText = makeTypedStringExpression(likeExpr->Text, "LIKE matched value", source);
    auto typedPattern = makeTypedStringExpression(likeExpr->Pattern, "LIKE pattern", source);

    TConstExpressionPtr typedEscapeCharacter;
    if (likeExpr->EscapeCharacter) {
        typedEscapeCharacter = makeTypedStringExpression(likeExpr->EscapeCharacter.value(), "escape character", source);
    }

    return New<TLikeExpression>(
        std::move(typedText),
        likeExpr->Opcode,
        std::move(typedPattern),
        std::move(typedEscapeCharacter));
}

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
    auto typedPredicate = builder->BuildTypedExpression(expressionAst.front(), {});

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
        auto typedExpr = builder->BuildTypedExpression(expressionAst);

        groupClause->AddGroupItem(ApplyRewriters(typedExpr), builder->InferGroupItemName(typedExpr, *expressionAst));
    }

    builder->SetGroupData(
        &groupClause->GroupItems,
        &groupClause->AggregateItems,
        totalsMode);

    return groupClause;
}

TConstExpressionPtr TExpressionBuilderV2::OnQueryOp(const NAst::TQueryExpression* queryExpr)
{
    // COMPAT(dtorilov): Remove after 26.2.
    if (!Context_.Options.EnableScalarSubqueryOrderByAndLimit &&
        (!queryExpr->Query.OrderExpressions.empty() || queryExpr->Query.Offset || queryExpr->Query.Limit))
    {
        THROW_ERROR_EXCEPTION("ORDER BY, OFFSET and LIMIT are disabled in scalar subqueries");
    }

    NAst::TExpressionList fromExpressions;

    Visit(queryExpr->Query.FromClause,
        [&] (const NAst::TTableDescriptor& /*table*/) {
            THROW_ERROR_EXCEPTION("Subquery from table is not supported");
        },
        [&] (const NAst::TQueryAstHeadPtr& /*subquery*/) {
            THROW_ERROR_EXCEPTION("Subquery from subquery in expression not supported");
        },
        [&] (const NAst::TExpressionList& expressions) {
            fromExpressions = expressions;
        },
        [&] (const NAst::TUnionAllClause& /*unionAll*/) {
            THROW_ERROR_EXCEPTION("UNION ALL in scalar subquery expression is not supported");
        });

    if (queryExpr->Query.WithIndex) {
        THROW_ERROR_EXCEPTION("WITH INDEX clause is not supported in subqueries");
    }

    ValidateOrderByOffsetAndLimit(queryExpr->Query);
    ValidateHaving(queryExpr->Query, /*hasOrderBy*/ !queryExpr->Query.OrderExpressions.empty());

    if (queryExpr->Query.HavingPredicate) {
        THROW_ERROR_EXCEPTION("HAVING clause is not supported in subqueries");
    }

    TNamedItemList typedFromExpressions;

    std::vector<TColumnSchema> columns;

    for (const auto& expressionAst : fromExpressions) {
        auto typedExpr = ApplyRewriters(BuildTypedExpression(expressionAst, {}));
        auto columnName = InferColumnName(*expressionAst);

        typedFromExpressions.emplace_back(typedExpr, columnName);

        auto type = typedExpr->LogicalType;

        if (type->GetMetatype() == ELogicalMetatype::Optional) {
            type = type->GetElement();
        }

        if (type->GetMetatype() != ELogicalMetatype::List) {
            THROW_ERROR_EXCEPTION("Unexpected type instead of list")
                .With("column_name", columnName)
                .With("actual_type", type->GetMetatype());
        }

        columns.emplace_back(columnName, MakeOptionalIfNot(type->GetElement()));
    }

    auto schema = New<TTableSchema>(columns);

    PushAliasResolver(queryExpr->AliasMap);
    AddTable({
        .Schema = *schema,
        .Alias = std::nullopt,
    });

    std::vector<TJoinClausePtr> joinClauses;
    if (!queryExpr->Query.Joins.empty()) {
        size_t commonKeyPrefix = std::numeric_limits<size_t>::max();
        for (const auto& join : queryExpr->Query.Joins) {
            Visit(join,
                [&] (const NAst::TJoin& tableJoin) {
                    auto dataSplitsIt = Context_.DataSplits.find(tableJoin.Table.Path);
                    if (dataSplitsIt == Context_.DataSplits.end()) {
                        THROW_ERROR_EXCEPTION("Data split not found for table path %Qv", tableJoin.Table.Path);
                    }

                    joinClauses.push_back(BuildJoinClause(
                        dataSplitsIt->second,
                        tableJoin,
                        Source_,
                        queryExpr->AliasMap,
                        Functions_,
                        &commonKeyPrefix,
                        schema,
                        /*tableAlias*/ std::nullopt,
                        this,
                        Context_));
                },
                [&] (const NAst::TArrayJoin& /*arrayJoin*/) {
                    THROW_ERROR_EXCEPTION("ARRAY JOIN is not supported in subquery joins");
                });
        }
    }

    TConstExpressionPtr whereClause;

    if (queryExpr->Query.WherePredicate) {
        auto wherePredicate = BuildPredicate(*queryExpr->Query.WherePredicate, this, "WHERE-clause");
        whereClause = IsTrue(wherePredicate) ? nullptr : wherePredicate;
    }

    TGroupClausePtr groupClause;
    if (queryExpr->Query.GroupExprs) {
        groupClause = BuildGroupClause(*queryExpr->Query.GroupExprs, queryExpr->Query.TotalsMode, this);
    }

    TOrderClausePtr orderClause;
    if (!queryExpr->Query.OrderExpressions.empty()) {
        orderClause = New<TOrderClause>();
        for (const auto& orderExpression : queryExpr->Query.OrderExpressions) {
            for (const auto& expressionAst : orderExpression.Expressions) {
                auto typedExpression = ApplyRewriters(BuildTypedExpression(expressionAst, {}));
                orderClause->OrderItems.push_back({typedExpression, orderExpression.Descending});
            }
        }
    }

    TProjectClausePtr projectClause;
    if (queryExpr->Query.SelectExprs) {
        projectClause = New<TProjectClause>();
        for (const auto& expressionAst : *queryExpr->Query.SelectExprs) {
            auto typedExpr = ApplyRewriters(BuildTypedExpression(expressionAst, {}));
            auto name = InferColumnName(*expressionAst);

            projectClause->AddProjection(typedExpr, name);
        }
    }

    Finish();

    PopAliasResolver();

    std::vector<NTableClient::TStructField> resultFields;

    for (const auto& [expression, name] : projectClause->Projections) {
        resultFields.push_back(NTableClient::TStructField{
            .Name = name,
            .StableName = name,
            .Type = expression->LogicalType,
        });
    }

    auto resultType = ListLogicalType(StructLogicalType(resultFields, /*removedFieldStableNames*/ {}));

    auto result = New<TSubqueryExpression>(resultType);

    result->FromExpressions = typedFromExpressions;

    result->JoinClauses = {joinClauses.begin(), joinClauses.end()};

    result->WhereClause = whereClause;
    result->GroupClause = groupClause;
    result->ProjectClause = projectClause;
    result->OrderClause = orderClause;
    result->Offset = queryExpr->Query.Offset.value_or(0);
    result->Limit = queryExpr->Query.Limit;

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
