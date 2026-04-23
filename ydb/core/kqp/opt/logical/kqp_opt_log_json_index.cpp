#include "kqp_opt_log_json_index.h"

#include <ydb/core/base/json_index.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NJsonIndex;

namespace {

struct TPredicateCollectResult {
    TString ColumnName;
    TCollectResult Collect;
    size_t ProcessedJsonNodes = 0;
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
        case EDataSlot::Date:
        case EDataSlot::Datetime:
        case EDataSlot::Timestamp:
            return true;
        default:
            return false;
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
        return std::unexpected("Nested JSON_* functions are not supported");
    }

    if (!jsonNode.JsonPath().Maybe<TCoUtf8>()) {
        return std::unexpected("Expected JSON path as a string literal");
    }

    const auto& variables = jsonNode.Variables().Ref();
    if (!variables.GetTypeAnn() || variables.GetTypeAnn()->GetKind() != ETypeAnnotationKind::EmptyDict) {
        return std::unexpected("Expected empty dict as variables");
    }

    std::optional<EDataSlot> returningType;
    if (jsonNode.Maybe<TCoJsonValue>()) {
        auto jsonValue = jsonNode.Cast<TCoJsonValue>();
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
    if ((left.has_value() && left->Collect.IsError()) || !right.has_value()) {
        return left;
    }

    if ((right.has_value() && right->Collect.IsError()) || !left.has_value()) {
        return right;
    }

    if (left->ColumnName != right->ColumnName) {
        auto error = TCollectResult(TIssue(ctx.GetPosition(pos),
            TStringBuilder() << "Cross-column predicates are not supported"));
        return TPredicateCollectResult{"", std::move(error)};
    }

    size_t totalProcessed = left->ProcessedJsonNodes + right->ProcessedJsonNodes;
    auto merged = (mode == TCollectResult::ETokensMode::And)
        ? MergeAnd(std::move(left->Collect), std::move(right->Collect))
        : MergeOr(std::move(left->Collect), std::move(right->Collect));
    return TPredicateCollectResult{std::move(left->ColumnName), std::move(merged), totalProcessed};
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
        auto error = TCollectResult(TIssue(ctx.GetPosition(pos), TStringBuilder()
            << "Failed to parse JSON path expression: " << parseIssues.ToOneLineString()));
        return TPredicateCollectResult{"", std::move(error)};
    }

    auto collectResult = CollectJsonPath(path, callableType);
    if (collectResult.IsError()) {
        return TPredicateCollectResult{"", std::move(collectResult)};
    }

    auto& tokens = collectResult.GetTokens();
    if (collectResult.CanCollect() && comparisonValue.has_value()) {
        YQL_ENSURE(tokens.size() == 1, "Expected exactly one token");

        if (auto encodedValue = EncodeValueToJsonPath(*comparisonValue)) {
            tokens.front() += *encodedValue;
            collectResult.StopCollecting();
        }
    }

    return TPredicateCollectResult{columnName, std::move(collectResult), 1};
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
        return MakeCollectError(ctx, jsonSide.Pos(),
            "JSON_VALUE with Date/DateTime/Timestamp RETURNING is not supported by JSON index");
    }

    if (leftParams->ReturningType.has_value() && *leftParams->ReturningType == EDataSlot::Bool) {
        return MakeCollectError(ctx, otherSide.Pos(),
            "Comparison with JSON_VALUE RETURNING Bool is not supported by JSON index");
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

            if (result.has_value() && result->Collect.IsError()) {
                return result;
            }
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

            if (result.has_value() && result->Collect.IsError()) {
                return result;
            }
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
    auto result = VisitJsonPredicate(body, ctx);
    if (!result) {
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
            << "Failed to extract search terms from predicate: JSON_QUERY is not supported by JSON index"));
        return std::nullopt;
    }

    if (result->ProcessedJsonNodes != totalJsonNodes) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "Failed to extract search terms from predicate: not all JSON_* functions could be used for index lookup"));
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
