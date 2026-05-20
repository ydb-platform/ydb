#include "kqp_opt_log_json_index.h"

#include <expected>
#include <functional>

#include <ydb/core/base/json_index.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <yql/essentials/core/sql_types/yql_atom_enums.h>
#include <yql/essentials/core/yql_opt_utils.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NJsonIndex;

namespace {

struct TPredicateCollectResult {
    TString ColumnName;
    TCollectResult Collect;
};

struct TJsonNodeParams {
    TString ColumnName;
    TString JsonPath;
    std::optional<EDataSlot> ReturningType;
    std::unordered_map<TString, TString> Variables;
    std::unordered_map<TString, TString> ParamVariables;
};

TPredicateCollectResult MakeCollectError(TExprContext& ctx, TPositionHandle pos, TStringBuf message) {
    return TPredicateCollectResult{"", TCollectResult(
        TIssue(ctx.GetPosition(pos), TString{message}))};
}

bool IsJsonValueReturningNonIndexable(std::optional<EDataSlot> slot) {
    if (!slot) {
        return false;
    }

    switch (*slot) {
        case EDataSlot::Int8:
        case EDataSlot::Int16:
        case EDataSlot::Int32:
        case EDataSlot::Int64:
        case EDataSlot::Uint8:
        case EDataSlot::Uint16:
        case EDataSlot::Uint32:
        case EDataSlot::Uint64:
        case EDataSlot::Float:
        case EDataSlot::Double:
        case EDataSlot::String:
        case EDataSlot::Utf8:
        case EDataSlot::Bool:
            return false;
        default:
            return true;
    }
}

TExprBase UnwrapPredicate(TExprBase node) {
    while (true) {
        if (const auto just = node.Maybe<TCoJust>()) {
            node = just.Cast().Input();
        } else if (const auto coalesce = node.Maybe<TCoCoalesce>()) {
            node = coalesce.Cast().Predicate();
        } else if (const auto optionalIf = node.Maybe<TCoOptionalIf>()) {
            node = optionalIf.Cast().Predicate();
        } else {
            break;
        }
    }
    return node;
}

TExprBase UnwrapValue(TExprBase node) {
    while (true) {
        if (const auto just = node.Maybe<TCoJust>()) {
            node = just.Cast().Input();
        } else if (const auto unwrap = node.Maybe<TCoUnwrap>()) {
            node = unwrap.Cast().Optional();
        } else if (const auto cast = node.Maybe<TCoSafeCast>()) {
            node = cast.Cast().Value();
        } else {
            break;
        }
    }
    return node;
}

std::optional<TString> EncodeValueToJsonPath(const TExprBase& node, bool negative = false) {
    if (node.Maybe<TCoMinus>()) {
        return EncodeValueToJsonPath(UnwrapValue(node.Cast<TCoMinus>().Arg()), !negative);
    }

    TString value;

    if (node.Maybe<TCoNothing>() || node.Maybe<TCoNull>()) {
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Null);
        return value;
    }

    if (node.Maybe<TCoBool>()) {
        const auto boolValue = FromString<bool>(node.Cast<TCoBool>().Literal().Value());
        AppendJsonIndexLiteral(value, boolValue
            ? NBinaryJson::EEntryType::BoolTrue
            : NBinaryJson::EEntryType::BoolFalse);
        return value;
    }

    if (node.Maybe<TCoString>()) {
        const auto stringValue = node.Cast<TCoString>().Literal().Value();
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::String, stringValue);
        return value;
    }

    if (node.Maybe<TCoUtf8>()) {
        const auto utf8Value = node.Cast<TCoUtf8>().Literal().Value();
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::String, utf8Value);
        return value;
    }

    if (node.Maybe<TCoFloat>()) {
        double literalValue = static_cast<double>(FromString<float>(node.Cast<TCoFloat>().Literal().Value()));
        if (negative) literalValue = -literalValue;
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoDouble>()) {
        double literalValue = FromString<double>(node.Cast<TCoDouble>().Literal().Value());
        if (negative) literalValue = -literalValue;
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoInt8>()) {
        double literalValue = static_cast<double>(FromString<i8>(node.Cast<TCoInt8>().Literal().Value()));
        if (negative) literalValue = -literalValue;
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoInt16>()) {
        double literalValue = static_cast<double>(FromString<i16>(node.Cast<TCoInt16>().Literal().Value()));
        if (negative) literalValue = -literalValue;
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoInt32>()) {
        double literalValue = static_cast<double>(FromString<i32>(node.Cast<TCoInt32>().Literal().Value()));
        if (negative) literalValue = -literalValue;
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoInt64>()) {
        const auto intValue = FromString<i64>(node.Cast<TCoInt64>().Literal().Value());
        if (intValue > MaxSupportedInt || intValue < -MaxSupportedInt) {
            return std::nullopt;
        }
        double literalValue = static_cast<double>(intValue);
        if (negative) literalValue = -literalValue;
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoUint8>()) {
        double literalValue = static_cast<double>(FromString<ui8>(node.Cast<TCoUint8>().Literal().Value()));
        if (negative) literalValue = -literalValue;
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoUint16>()) {
        double literalValue = static_cast<double>(FromString<ui16>(node.Cast<TCoUint16>().Literal().Value()));
        if (negative) literalValue = -literalValue;
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoUint32>()) {
        double literalValue = static_cast<double>(FromString<ui32>(node.Cast<TCoUint32>().Literal().Value()));
        if (negative) literalValue = -literalValue;
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoUint64>()) {
        const auto uintValue = FromString<ui64>(node.Cast<TCoUint64>().Literal().Value());
        if (uintValue > static_cast<ui64>(MaxSupportedInt)) {
            return std::nullopt;
        }
        double literalValue = static_cast<double>(uintValue);
        if (negative) literalValue = -literalValue;
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    return std::nullopt;
}

bool IsSupportedJsonParamType(const TTypeAnnotationNode* type) {
    const auto* innerType = RemoveAllOptionals(type);
    if (!innerType || innerType->GetKind() != ETypeAnnotationKind::Data) {
        return false;
    }

    switch (innerType->Cast<TDataExprType>()->GetSlot()) {
        case EDataSlot::String:
        case EDataSlot::Utf8:
        case EDataSlot::Bool:
        case EDataSlot::Int8:
        case EDataSlot::Int16:
        case EDataSlot::Int32:
        case EDataSlot::Int64:
        case EDataSlot::Uint8:
        case EDataSlot::Uint16:
        case EDataSlot::Uint32:
        case EDataSlot::Uint64:
        case EDataSlot::Float:
        case EDataSlot::Double:
            return true;
        default:
            return false;
    }
}

std::expected<TJsonNodeParams, TString> VisitJsonNode(const TCoJsonQueryBase& jsonNode) {
    if (!jsonNode.Json().Maybe<TCoMember>()) {
        return std::unexpected("JSON source must be a column reference");
    }

    if (!jsonNode.JsonPath().Maybe<TCoUtf8>()) {
        return std::unexpected("Expected JSON path as a string literal");
    }

    const auto& nodeVariables = jsonNode.Variables().Ref();
    std::unordered_map<TString, TString> variables;
    std::unordered_map<TString, TString> paramVariables;

    if (nodeVariables.IsCallable("AsDict")) {
        for (ui32 i = 0; i < nodeVariables.ChildrenSize(); ++i) {
            const auto& pair = *nodeVariables.ChildPtr(i);
            if (pair.ChildrenSize() != 2) {
                return std::unexpected("Expected key/value pair in PASSING clause");
            }

            const auto keyExpr = TExprBase(pair.ChildPtr(0));
            if (!keyExpr.Maybe<TCoUtf8>()) {
                return std::unexpected("Expected Utf8 key in PASSING clause");
            }

            const auto varName = TString(keyExpr.Cast<TCoUtf8>().Literal().Value());

            const auto applyExpr = TExprBase(pair.ChildPtr(1));
            if (!applyExpr.Maybe<TCoApply>()) {
                return std::unexpected(TStringBuilder() << "Variable '" << varName
                    << "' is bound to unsupported expression");
            }

            if (applyExpr.Ref().ChildrenSize() <= 1) {
                return std::unexpected(TStringBuilder() << "Variable '" << varName
                    << "' is bound to malformed expression");
            }

            const auto innerValue = UnwrapValue(TExprBase(applyExpr.Ref().ChildPtr(1)));

            if (innerValue.Maybe<TCoParameter>()) {
                const auto paramName = TString(innerValue.Cast<TCoParameter>().Name().Value());
                const auto paramType = innerValue.Cast<TCoParameter>().Ref().GetTypeAnn();
                if (!IsSupportedJsonParamType(paramType)) {
                    return std::unexpected(TStringBuilder() << "Variable '" << varName
                        << "' is bound to a parameter with unsupported type");
                }
                paramVariables.emplace(varName, paramName);
                continue;
            }

            const auto encoded = EncodeValueToJsonPath(innerValue);
            if (!encoded) {
                return std::unexpected(TStringBuilder() << "Variable '" << varName
                    << "' is bound to unsupported expression");
            }

            variables.emplace(varName, *encoded);
        }
    } else if (!nodeVariables.GetTypeAnn() || nodeVariables.GetTypeAnn()->GetKind() != ETypeAnnotationKind::EmptyDict) {
        return std::unexpected("Expected empty dict as variables");
    }

    if (jsonNode.Maybe<TCoJsonExists>()) {
        const auto jsonExists = jsonNode.Cast<TCoJsonExists>();
        if (jsonExists.OnError() && jsonExists.OnError().Maybe<TCoJust>()) {
            const auto arg = jsonExists.OnError().Cast<TCoJust>().Input();
            if (arg.Maybe<TCoBool>() && FromString<bool>(arg.Cast<TCoBool>().Literal().Value())) {
                return std::unexpected("JSON_EXISTS with ON ERROR TRUE is not supported");
            }
        }
    }

    std::optional<EDataSlot> returningType;
    if (jsonNode.Maybe<TCoJsonValue>()) {
        const auto jsonValue = jsonNode.Cast<TCoJsonValue>();

        const auto onEmptyMode = FromString<EJsonValueHandlerMode>(jsonValue.OnEmptyMode().Ref().Content());
        const auto onEmptyValue = jsonValue.OnEmpty();
        if (onEmptyMode == EJsonValueHandlerMode::DefaultValue && !onEmptyValue.Maybe<TCoNull>()) {
            return std::unexpected("DEFAULT ON EMPTY in JSON_VALUE must be NULL");
        }

        const auto onErrorMode = FromString<EJsonValueHandlerMode>(jsonValue.OnErrorMode().Ref().Content());
        const auto onErrorValue = jsonValue.OnError();
        if (onErrorMode == EJsonValueHandlerMode::DefaultValue && !onErrorValue.Maybe<TCoNull>()) {
            return std::unexpected("DEFAULT ON ERROR in JSON_VALUE must be NULL");
        }

        if (!jsonValue.ReturningType()) {
            return std::unexpected("RETURNING clause is required for JSON_VALUE in JSON index predicates");
        }

        const auto* returningTypeAnn = jsonValue.ReturningType().Ref()
            .GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        returningType = returningTypeAnn->Cast<TDataExprType>()->GetSlot();

        if (IsJsonValueReturningNonIndexable(returningType)) {
            return std::unexpected("Date/time types in RETURNING clause are not supported");
        }
    }

    return TJsonNodeParams{
        .ColumnName = jsonNode.Json().Cast<TCoMember>().Name().StringValue(),
        .JsonPath = jsonNode.JsonPath().Cast<TCoUtf8>().Literal().StringValue(),
        .ReturningType = returningType,
        .Variables = std::move(variables),
        .ParamVariables = std::move(paramVariables)
    };
}

std::optional<TPredicateCollectResult> MergePredicateResults(std::optional<TPredicateCollectResult> left,
    std::optional<TPredicateCollectResult> right, TCollectResult::ETokensMode mode, TExprContext& ctx, TPositionHandle pos)
{
    const bool leftValid  = left.has_value()  && !left->Collect.IsError();
    const bool rightValid = right.has_value() && !right->Collect.IsError();

    if (leftValid && rightValid) {
        if (left->ColumnName != right->ColumnName) {
            return MakeCollectError(ctx, pos, "Cross-column predicates are not supported");
        }

        const auto merged = (mode == TCollectResult::ETokensMode::And)
            ? MergeAnd(std::move(left->Collect), std::move(right->Collect))
            : MergeOr(std::move(left->Collect), std::move(right->Collect));
        return TPredicateCollectResult{std::move(left->ColumnName), std::move(merged)};
    }

    // AND semantics: one of the operands must be valid
    if (mode == TCollectResult::ETokensMode::And) {
        if (leftValid) {
            return left;
        }

        if (rightValid) {
            return right;
        }

        if (left.has_value() && left->Collect.IsError()) {
            return left;
        }

        if (right.has_value() && right->Collect.IsError()) {
            return right;
        }

        return std::nullopt;
    }

    // OR semantics: both operands must be valid
    if (mode == TCollectResult::ETokensMode::Or) {
        if (left.has_value() && left->Collect.IsError()) {
            return left;
        }

        if (right.has_value() && right->Collect.IsError()) {
            return right;
        }

        return std::nullopt;
    }

    Y_UNREACHABLE();
}

TPredicateCollectResult AppendComparisonValue(const TString& columnName, TCollectResult collectResult,
    std::optional<TExprBase> comparisonValue)
{
    YQL_ENSURE(!collectResult.IsError(), "Expected valid collect result");

    auto& tokens = collectResult.GetTokens();
    if (collectResult.CanCollect() && comparisonValue.has_value()) {
        YQL_ENSURE(tokens.size() == 1, "Expected exactly one token");
        auto node = tokens.extract(tokens.begin());

        if (comparisonValue->Maybe<TCoParameter>()) {
            const auto paramName = TString(comparisonValue->Cast<TCoParameter>().Name().Value());
            node.value().ParamName = paramName;
        } else if (const auto encodedValue = EncodeValueToJsonPath(*comparisonValue)) {
            node.value().PathToken += *encodedValue;
        }

        tokens.insert(std::move(node));
        collectResult.StopCollecting();
    }

    return TPredicateCollectResult{columnName, std::move(collectResult)};
}

TPredicateCollectResult ParseAndCollectJson(const TJsonNodeParams& params,
    ECallableType callableType, std::optional<TExprBase> comparisonValue,
    TExprContext& ctx, TPositionHandle pos)
{
    TIssues parseIssues;
    const auto path = NJsonPath::ParseJsonPath(params.JsonPath, parseIssues, 1);
    if (!parseIssues.Empty()) {
        return MakeCollectError(ctx, pos, "Failed to parse JSON path expression: " + parseIssues.ToOneLineString());
    }

    auto collectResult = CollectJsonPath(path, callableType, params.Variables, params.ParamVariables);
    if (collectResult.IsError()) {
        return MakeCollectError(ctx, pos, collectResult.GetError().GetMessage());
    }

    return AppendComparisonValue(params.ColumnName, std::move(collectResult), comparisonValue);
}

std::expected<std::optional<TExprBase>, TString> TryExtractComparisonValue(const TExprBase& value) {
    // Negation (NULL ON ...) -> error
    if (value.Maybe<TCoNull>() || value.Maybe<TCoNothing>()) {
        return std::unexpected("NULL is not supported for literal comparison values");
    }

    // Literal case (append to path)
    if (value.Maybe<TCoDataCtor>() || value.Maybe<TCoMinus>()) {
        return value;
    }

    // Parameter case (append to path)
    if (value.Maybe<TCoParameter>()) {
        const auto paramType = value.Cast<TCoParameter>().Ref().GetTypeAnn();
        if (!IsSupportedJsonParamType(paramType)) {
            return std::unexpected(TString("Parameter with unsupported type"));
        }
        return value;
    }

    // Ignore
    return std::optional<TExprBase>(std::nullopt);
}

std::optional<TPredicateCollectResult> VisitJsonBinaryOperator(const TExprBase& node, TExprBase left, TExprBase right, TExprContext& ctx) {
    left = UnwrapValue(left);
    right = UnwrapValue(right);

    if (!left.Maybe<TCoJsonValue>()) {
        if (!right.Maybe<TCoJsonValue>()) {
            return std::nullopt;
        }
        std::swap(left, right);
    }

    const auto jsonSide = left;
    const auto otherSide = right;

    const auto leftParams = VisitJsonNode(jsonSide.Cast<TCoJsonValue>());
    if (!leftParams.has_value()) {
        return MakeCollectError(ctx, jsonSide.Pos(), leftParams.error());
    }
    if (leftParams->ReturningType.has_value() && *leftParams->ReturningType == EDataSlot::Bool) {
        return MakeCollectError(ctx, jsonSide.Pos(), "Comparison JSON_VALUE with RETURNING Bool is not supported");
    }

    std::optional<TExprBase> comparisonValue;
    if (node.Maybe<TCoCmpEqual>()) {
        auto extracted = TryExtractComparisonValue(otherSide);
        if (!extracted.has_value()) {
            return MakeCollectError(ctx, otherSide.Pos(), extracted.error());
        }
        comparisonValue = std::move(*extracted);
    }

    const auto leftResult = ParseAndCollectJson(*leftParams, ECallableType::JsonValue, comparisonValue, ctx, left.Pos());

    if (otherSide.Maybe<TCoJsonValue>()) {
        const auto rightParams = VisitJsonNode(otherSide.Cast<TCoJsonValue>());
        if (!rightParams.has_value()) {
            return MakeCollectError(ctx, otherSide.Pos(), rightParams.error());
        }
        if (rightParams->ReturningType.has_value() && *rightParams->ReturningType == EDataSlot::Bool) {
            return MakeCollectError(ctx, jsonSide.Pos(), "Comparison JSON_VALUE with RETURNING Bool is not supported");
        }

        const auto rightResult = ParseAndCollectJson(*rightParams, ECallableType::JsonValue, std::nullopt, ctx, otherSide.Pos());
        return MergePredicateResults(std::move(leftResult), std::move(rightResult),
            TCollectResult::ETokensMode::And, ctx, otherSide.Pos());
    }

    return leftResult;
}

std::optional<TPredicateCollectResult> VisitJsonSqlIn(const TCoSqlIn& node, TExprContext& ctx) {
    auto lookup = UnwrapValue(node.Lookup());
    auto collection = UnwrapValue(node.Collection());

    if (!lookup.Maybe<TCoJsonValue>()) {
        return std::nullopt;
    }

    const auto jsonLookup = lookup.Cast<TCoJsonValue>();
    const auto jsonParams = VisitJsonNode(jsonLookup);
    if (!jsonParams.has_value()) {
        return MakeCollectError(ctx, jsonLookup.Pos(), jsonParams.error());
    }

    if (jsonParams->ReturningType.has_value() && *jsonParams->ReturningType == EDataSlot::Bool) {
        return MakeCollectError(ctx, jsonLookup.Pos(), "SQL IN with JSON_VALUE with RETURNING Bool is not supported");
    }

    if (collection.Maybe<TCoParameter>()) {
        const auto& param = collection.Cast<TCoParameter>();
        const auto* paramTypeAnn = RemoveAllOptionals(param.Ref().GetTypeAnn());

        if (!paramTypeAnn || paramTypeAnn->GetKind() != ETypeAnnotationKind::List) {
            return MakeCollectError(ctx, param.Pos(), "Unsupported parameter type in SQL IN");
        }

        const auto* listItemType = paramTypeAnn->Cast<TListExprType>()->GetItemType();
        if (!IsSupportedJsonParamType(listItemType)) {
            return MakeCollectError(ctx, param.Pos(), "List parameter item type is not supported for JSON index");
        }

        auto baseResult = ParseAndCollectJson(*jsonParams, ECallableType::JsonValue,
            std::nullopt, ctx, jsonLookup.Pos());
        if (baseResult.Collect.IsError()) {
            return baseResult;
        }

        const auto paramName = TString(param.Name().Value());
        if (baseResult.Collect.CanCollect()) {
            auto& tokens = baseResult.Collect.GetTokens();
            YQL_ENSURE(tokens.size() == 1);

            auto nodeHandle = tokens.extract(tokens.begin());
            nodeHandle.value().ParamName = std::move(paramName);
            tokens.insert(std::move(nodeHandle));

            baseResult.Collect.StopCollecting();
            baseResult.Collect.SetTokensMode(TCollectResult::ETokensMode::Or);
        }

        return baseResult;
    }

    if (!collection.Maybe<TExprList>()) {
        return std::nullopt;
    }

    auto baseResult = ParseAndCollectJson(*jsonParams, ECallableType::JsonValue, std::nullopt, ctx, jsonLookup.Pos());
    if (baseResult.Collect.IsError()) {
        return baseResult;
    }

    std::optional<TPredicateCollectResult> acc;
    for (const auto& item : collection.Cast<TExprList>()) {
        const auto literal = UnwrapValue(TExprBase(item));
        auto extracted = TryExtractComparisonValue(literal);
        if (!extracted.has_value()) {
            return MakeCollectError(ctx, literal.Pos(), extracted.error());
        }

        auto itemResult = AppendComparisonValue(baseResult.ColumnName, baseResult.Collect, *extracted);
        if (!acc.has_value()) {
            acc = std::move(itemResult);
        } else {
            acc = MergePredicateResults(std::move(acc), std::move(itemResult),
                TCollectResult::ETokensMode::Or, ctx, literal.Pos());
        }
    }

    return acc;
}

std::optional<TPredicateCollectResult> VisitJsonExists(const TExprBase& node, TExprContext& ctx) {
    if (node.Maybe<TCoJsonExists>()) {
        const auto params = VisitJsonNode(node.Cast<TCoJsonExists>());
        if (!params) {
            return MakeCollectError(ctx, node.Pos(), params.error());
        }

        return ParseAndCollectJson(*params, ECallableType::JsonExists, std::nullopt, ctx, node.Pos());
    }

    return std::nullopt;
}

std::optional<TPredicateCollectResult> VisitJsonValue(const TExprBase& node, TExprContext& ctx) {
    if (const auto cmp = node.Maybe<TCoCompare>()) {
        return VisitJsonBinaryOperator(node, cmp.Cast().Left(), cmp.Cast().Right(), ctx);
    }

    if (const auto sqlIn = node.Maybe<TCoSqlIn>()) {
        return VisitJsonSqlIn(sqlIn.Cast(), ctx);
    }

    if (node.Maybe<TCoJsonValue>()) {
        const auto jsonValue = node.Cast<TCoJsonValue>();
        const auto params = VisitJsonNode(jsonValue);
        if (!params) {
            return MakeCollectError(ctx, node.Pos(), params.error());
        }

        std::optional<TExprBase> comparisonValue;
        if (params->ReturningType && *params->ReturningType == EDataSlot::Bool) {
            comparisonValue = TExprBase(Build<TCoBool>(ctx, node.Pos()).Literal().Build(true).Done().Ptr());
        }

        return ParseAndCollectJson(*params, ECallableType::JsonValue, comparisonValue, ctx, node.Pos());
    }

    return std::nullopt;
}

std::optional<TPredicateCollectResult> VisitJsonPredicate(const TExprBase& predicate, TExprContext& ctx) {
    const auto node = UnwrapPredicate(predicate);

    if (const auto maybeAnd = node.Maybe<TCoAnd>()) {
        const auto andNode = maybeAnd.Cast();
        if (andNode.ArgCount() == 0) {
            return std::nullopt;
        }

        auto result = VisitJsonPredicate(andNode.Arg(0), ctx);
        for (size_t i = 1; i < andNode.ArgCount(); ++i) {
            const auto nextNode = andNode.Arg(i);
            const auto nextResult = VisitJsonPredicate(nextNode, ctx);
            result = MergePredicateResults(std::move(result), std::move(nextResult),
                TCollectResult::ETokensMode::And, ctx, nextNode.Pos());
        }

        return result;
    }

    if (const auto maybeOr = node.Maybe<TCoOr>()) {
        const auto orNode = maybeOr.Cast();
        if (orNode.ArgCount() == 0) {
            return std::nullopt;
        }

        auto result = VisitJsonPredicate(orNode.Arg(0), ctx);
        for (size_t i = 1; i < orNode.ArgCount(); ++i) {
            const auto nextNode = orNode.Arg(i);
            const auto nextResult = VisitJsonPredicate(nextNode, ctx);
            result = MergePredicateResults(std::move(result), std::move(nextResult),
                TCollectResult::ETokensMode::Or, ctx, nextNode.Pos());
        }

        return result;
    }

    if (const auto jsonExists = VisitJsonExists(node, ctx)) {
        return jsonExists;
    }

    if (const auto jsonValue = VisitJsonValue(node, ctx)) {
        return jsonValue;
    }

    return std::nullopt;
}

} // namespace

std::optional<TJsonIndexSettings> CollectJsonIndexPredicate(const TExprBase& body, const TExprBase& node, TExprContext& ctx) {
    bool hasJsonQuery = false;

    std::function<void(const TExprNode::TPtr&)> countJsonNodes = [&](const TExprNode::TPtr& expr) {
        if (TExprBase(expr).Maybe<TCoJsonQueryBase>()) {
            hasJsonQuery |= static_cast<bool>(TExprBase(expr).Maybe<TCoJsonQuery>());
            return;
        }
        for (const auto& child : expr->Children()) {
            countJsonNodes(child);
        }
    };

    countJsonNodes(body.Ptr());

    if (hasJsonQuery) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "Failed to extract search terms from predicate: JSON_QUERY is not supported"));
        return std::nullopt;
    }

    const auto result = VisitJsonPredicate(body, ctx);
    if (!result.has_value()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "Failed to extract search terms from predicate: nothing to extract"));
        return std::nullopt;
    }

    const auto& collectResult = result->Collect;
    if (collectResult.IsError()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "Failed to extract search terms from predicate: " << collectResult.GetError().GetMessage()));
        return std::nullopt;
    }

    if (collectResult.GetTokens().empty()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "Failed to extract search terms from predicate: Empty result"));
        return std::nullopt;
    }

    TVector<TExprNode::TPtr> tokenNodes;
    tokenNodes.reserve(collectResult.GetTokens().size());

    for (const auto& tokenPair : collectResult.GetTokens()) {
        auto pair = Build<TExprList>(ctx, node.Pos())
            .Add<TCoString>().Literal().Build(tokenPair.PathToken).Build()
            .Add<TCoString>().Literal().Build(tokenPair.ParamName).Build()
            .Done().Ptr();

        tokenNodes.push_back(std::move(pair));
    }

    TStringBuf defaultOperator = collectResult.GetTokensMode() == TCollectResult::ETokensMode::Or ? "or" : "and";

    TKqpReadTableFullTextIndexSettings settings;
    settings.SetDefaultOperator(Build<TCoString>(ctx, node.Pos()).Literal().Build(defaultOperator).Done().Ptr());
    settings.SetMinimumShouldMatch(Build<TCoString>(ctx, node.Pos()).Literal().Build("").Done().Ptr());
    settings.SetTokens(ctx.NewList(node.Pos(), std::move(tokenNodes)));

    return TJsonIndexSettings{std::move(result->ColumnName), std::move(settings)};
}

} // namespace NKikimr::NKqp::NOpt
