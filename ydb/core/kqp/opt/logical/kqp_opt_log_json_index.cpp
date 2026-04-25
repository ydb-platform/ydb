#include "kqp_opt_log_json_index.h"

#include <ydb/core/base/json_index.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <yql/essentials/core/sql_types/yql_atom_enums.h>

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

TExprBase UnwrapOptionalNodes(TExprBase node) {
    while (true) {
        if (auto just = node.Maybe<TCoJust>()) {
            node = just.Cast().Input();
        } else if (auto coalesce = node.Maybe<TCoCoalesce>()) {
            node = coalesce.Cast().Predicate();
        } else if (auto optionalIf = node.Maybe<TCoOptionalIf>()) {
            node = optionalIf.Cast().Predicate();
        } else {
            break;
        }
    }
    return node;
}

std::expected<TJsonNodeParams, TString> VisitJsonNode(const TCoJsonQueryBase& jsonNode) {
    if (!jsonNode.Json().Maybe<TCoMember>()) {
        if (jsonNode.Json().Maybe<TCoJsonQueryBase>()) {
            return std::unexpected("Nested JSON_* functions are not supported");
        }
        return std::unexpected("JSON source must be a column reference");
    }

    if (!jsonNode.JsonPath().Maybe<TCoUtf8>()) {
        return std::unexpected("Expected JSON path as a string literal");
    }

    const auto& variables = jsonNode.Variables().Ref();
    if (!variables.GetTypeAnn() || variables.GetTypeAnn()->GetKind() != ETypeAnnotationKind::EmptyDict) {
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

        if (jsonValue.ReturningType()) {
            const auto* returningTypeAnn = jsonValue.ReturningType().Ref()
                .GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            returningType = returningTypeAnn->Cast<TDataExprType>()->GetSlot();
        }
    }

    return TJsonNodeParams{
        .ColumnName = jsonNode.Json().Cast<TCoMember>().Name().StringValue(),
        .JsonPath = jsonNode.JsonPath().Cast<TCoUtf8>().Literal().StringValue(),
        .ReturningType = returningType
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

        auto merged = (mode == TCollectResult::ETokensMode::And)
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

std::optional<TString> EncodeValueToJsonPath(const TExprBase& node) {
    TString value;

    if (node.Maybe<TCoNull>()) {
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Null);
        return value;
    }

    if (node.Maybe<TCoBool>()) {
        auto boolValue = FromString<bool>(node.Cast<TCoBool>().Literal().Value());
        AppendJsonIndexLiteral(value, boolValue
            ? NBinaryJson::EEntryType::BoolTrue
            : NBinaryJson::EEntryType::BoolFalse);
        return value;
    }

    if (node.Maybe<TCoString>()) {
        auto stringValue = node.Cast<TCoString>().Literal().Value();
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::String, stringValue);
        return value;
    }

    if (node.Maybe<TCoUtf8>()) {
        auto utf8Value = node.Cast<TCoUtf8>().Literal().Value();
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::String, utf8Value);
        return value;
    }

    if (node.Maybe<TCoFloat>()) {
        double literalValue = static_cast<double>(FromString<float>(node.Cast<TCoFloat>().Literal().Value()));
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoDouble>()) {
        double literalValue = static_cast<double>(FromString<double>(node.Cast<TCoDouble>().Literal().Value()));
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoInt8>()) {
        double literalValue = static_cast<double>(FromString<i8>(node.Cast<TCoInt8>().Literal().Value()));
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoInt16>()) {
        double literalValue = static_cast<double>(FromString<i16>(node.Cast<TCoInt16>().Literal().Value()));
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoInt32>()) {
        double literalValue = static_cast<double>(FromString<i32>(node.Cast<TCoInt32>().Literal().Value()));
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoInt64>()) {
        double literalValue = static_cast<double>(FromString<i64>(node.Cast<TCoInt64>().Literal().Value()));
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoUint8>()) {
        double literalValue = static_cast<double>(FromString<ui8>(node.Cast<TCoUint8>().Literal().Value()));
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoUint16>()) {
        double literalValue = static_cast<double>(FromString<ui16>(node.Cast<TCoUint16>().Literal().Value()));
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoUint32>()) {
        double literalValue = static_cast<double>(FromString<ui32>(node.Cast<TCoUint32>().Literal().Value()));
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    if (node.Maybe<TCoUint64>()) {
        double literalValue = static_cast<double>(FromString<ui64>(node.Cast<TCoUint64>().Literal().Value()));
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
        return value;
    }

    return std::nullopt;
}

TPredicateCollectResult ParseAndCollectJson(const TString& columnName, const TString& jsonPath,
    ECallableType callableType, std::optional<TExprBase> comparisonValue, TExprContext& ctx, TPositionHandle pos)
{
    NYql::TIssues parseIssues;
    const auto path = NYql::NJsonPath::ParseJsonPath(jsonPath, parseIssues, 1);
    if (!parseIssues.Empty()) {
        return MakeCollectError(ctx, pos, "Failed to parse JSON path expression: " + parseIssues.ToOneLineString());
    }

    auto collectResult = CollectJsonPath(path, callableType);
    if (collectResult.IsError()) {
        return TPredicateCollectResult{"", std::move(collectResult)};
    }

    auto& tokens = collectResult.GetTokens();
    if (collectResult.CanCollect() && comparisonValue.has_value()) {
        YQL_ENSURE(tokens.size() == 1, "Expected exactly one token");

        if (auto encodedValue = EncodeValueToJsonPath(*comparisonValue)) {
            auto node = tokens.extract(tokens.begin());
            node.value() += *encodedValue;
            tokens.insert(std::move(node));
            collectResult.StopCollecting();
        }
    }

    return TPredicateCollectResult{columnName, std::move(collectResult)};
}

template<typename TJsonNode>
std::optional<std::pair<TExprBase, TExprBase>> NormalizeBinaryJsonOperands(TExprBase left, TExprBase right) {
    left = UnwrapOptionalNodes(left);
    right = UnwrapOptionalNodes(right);
    if (!left.Maybe<TJsonNode>()) {
        if (!right.Maybe<TJsonNode>()) {
            return std::nullopt;
        }
        std::swap(left, right);
    }
    return std::pair{std::move(left), std::move(right)};
}

std::optional<TPredicateCollectResult> VisitJsonBinaryOperator(const TExprBase& node, TExprBase left, TExprBase right, TExprContext& ctx) {
    auto normalized = NormalizeBinaryJsonOperands<TCoJsonValue>(std::move(left), std::move(right));
    if (!normalized) {
        return std::nullopt;
    }

    auto [jsonSide, otherSide] = *normalized;
    auto leftParams = VisitJsonNode(jsonSide.Cast<TCoJsonValue>());
    if (!leftParams.has_value()) {
        return MakeCollectError(ctx, jsonSide.Pos(), leftParams.error());
    }

    if (IsJsonValueReturningNonIndexable(leftParams->ReturningType)) {
        return MakeCollectError(ctx, jsonSide.Pos(), "Date/time types in RETURNING clause are not supported");
    }

    if (leftParams->ReturningType.has_value() && *leftParams->ReturningType == EDataSlot::Bool) {
        return MakeCollectError(ctx, jsonSide.Pos(), "Comparison JSON_VALUE with RETURNING Bool is not supported");
    }

    std::optional<TExprBase> comparisonValue;
    if (node.Maybe<TCoCmpEqual>() && otherSide.Maybe<TCoDataCtor>()) {
        comparisonValue = otherSide;
    }

    auto leftResult = ParseAndCollectJson(leftParams->ColumnName, leftParams->JsonPath,
        ECallableType::JsonValue, comparisonValue, ctx, left.Pos());

    if (otherSide.Maybe<TCoJsonValue>()) {
        auto rightParams = VisitJsonNode(otherSide.Cast<TCoJsonValue>());
        if (!rightParams.has_value()) {
            return MakeCollectError(ctx, otherSide.Pos(), rightParams.error());
        }

        auto rightResult = ParseAndCollectJson(rightParams->ColumnName, rightParams->JsonPath,
            ECallableType::JsonValue, std::nullopt, ctx, otherSide.Pos());
        return MergePredicateResults(std::move(leftResult), std::move(rightResult),
            TCollectResult::ETokensMode::And, ctx, otherSide.Pos());
    }

    return leftResult;
}

std::optional<TPredicateCollectResult> VisitJsonExists(const TExprBase& node, TExprContext& ctx) {
    if (node.Maybe<TCoJsonExists>()) {
        auto params = VisitJsonNode(node.Cast<TCoJsonExists>());
        if (!params) {
            return MakeCollectError(ctx, node.Pos(), params.error());
        }

        return ParseAndCollectJson(params->ColumnName, params->JsonPath,
            ECallableType::JsonExists, std::nullopt, ctx, node.Pos());
    }

    return std::nullopt;
}

std::optional<TPredicateCollectResult> VisitJsonValue(const TExprBase& node, TExprContext& ctx) {
    if (auto cmp = node.Maybe<TCoCompare>()) {
        return VisitJsonBinaryOperator(node, cmp.Cast().Left(), cmp.Cast().Right(), ctx);
    }

    if (node.Maybe<TCoJsonValue>()) {
        auto jsonValue = node.Cast<TCoJsonValue>();
        auto params = VisitJsonNode(jsonValue);
        if (!params) {
            return MakeCollectError(ctx, node.Pos(), params.error());
        }

        std::optional<TExprBase> comparisonValue;
        if (params->ReturningType && *params->ReturningType == EDataSlot::Bool) {
            comparisonValue = TExprBase(Build<TCoBool>(ctx, node.Pos()).Literal().Build(true).Done().Ptr());
        }

        return ParseAndCollectJson(params->ColumnName, params->JsonPath,
            ECallableType::JsonValue, comparisonValue, ctx, node.Pos());
    }

    return std::nullopt;
}

std::optional<TPredicateCollectResult> VisitJsonPredicate(const TExprBase& node, TExprContext& ctx) {
    if (auto optionalIf = node.Maybe<TCoOptionalIf>()) {
        return VisitJsonPredicate(optionalIf.Cast().Predicate(), ctx);
    }

    if (auto coalesce = node.Maybe<TCoCoalesce>()) {
        return VisitJsonPredicate(coalesce.Cast().Predicate(), ctx);
    }

    if (auto just = node.Maybe<TCoJust>()) {
        return VisitJsonPredicate(just.Cast().Input(), ctx);
    }

    if (auto maybeAnd = node.Maybe<TCoAnd>()) {
        auto andNode = maybeAnd.Cast();
        if (andNode.ArgCount() == 0) {
            return std::nullopt;
        }

        auto result = VisitJsonPredicate(andNode.Arg(0), ctx);
        for (size_t i = 1; i < andNode.ArgCount(); ++i) {
            auto nextNode = andNode.Arg(i);
            auto nextResult = VisitJsonPredicate(nextNode, ctx);

            result = MergePredicateResults(std::move(result), std::move(nextResult),
                TCollectResult::ETokensMode::And, ctx, nextNode.Pos());
        }

        return result;
    }

    if (auto maybeOr = node.Maybe<TCoOr>()) {
        auto orNode = maybeOr.Cast();
        if (orNode.ArgCount() == 0) {
            return std::nullopt;
        }

        auto result = VisitJsonPredicate(orNode.Arg(0), ctx);
        if (!result.has_value()) {
            return MakeCollectError(ctx, orNode.Arg(0).Pos(), "JSON index does not support OR with non-indexable predicates");
        }

        for (size_t i = 1; i < orNode.ArgCount(); ++i) {
            auto nextNode = orNode.Arg(i);
            auto nextResult = VisitJsonPredicate(nextNode, ctx);

            if (!nextResult.has_value()) {
                return MakeCollectError(ctx, nextNode.Pos(), "JSON index does not support OR with non-indexable predicates");
            }

            result = MergePredicateResults(std::move(result), std::move(nextResult),
                TCollectResult::ETokensMode::Or, ctx, nextNode.Pos());
        }

        return result;
    }

    if (auto existsResult = VisitJsonExists(node, ctx)) {
        return existsResult;
    }

    if (auto valueResult = VisitJsonValue(node, ctx)) {
        return valueResult;
    }

    return std::nullopt;
}

} // namespace

std::optional<TJsonIndexSettings> CollectJsonIndexPredicate(const TExprBase& body, const TExprBase& node, TExprContext& ctx) {
    size_t totalJsonNodes = 0;
    bool hasJsonQuery = false;

    std::function<void(const TExprNode::TPtr&)> countJsonNodes = [&](const TExprNode::TPtr& expr) {
        if (TExprBase(expr).Maybe<TCoJsonQueryBase>()) {
            totalJsonNodes++;
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

    if (totalJsonNodes == 0) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "Failed to extract search terms from predicate: no JSON_* functions found"));
        return std::nullopt;
    }

    auto result = VisitJsonPredicate(body, ctx);
    if (!result.has_value()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "Failed to extract search terms from predicate: nothing to extract"));
        return std::nullopt;
    }

    auto& collectResult = result->Collect;
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
    for (const auto& token : collectResult.GetTokens()) {
        tokenNodes.push_back(Build<TCoString>(ctx, node.Pos()).Literal().Build(token).Done().Ptr());
    }

    TStringBuf defaultOperator = collectResult.GetTokensMode() == TCollectResult::ETokensMode::Or ? "or" : "and";

    auto settings = TKqpReadTableFullTextIndexSettings{};
    settings.SetDefaultOperator(Build<TCoString>(ctx, node.Pos()).Literal().Build(defaultOperator).Done().Ptr());
    settings.SetMinimumShouldMatch(Build<TCoString>(ctx, node.Pos()).Literal().Build("").Done().Ptr());
    settings.SetTokens(ctx.NewList(node.Pos(), std::move(tokenNodes)));

    return TJsonIndexSettings{std::move(result->ColumnName), std::move(settings)};
}

} // namespace NKikimr::NKqp::NOpt
