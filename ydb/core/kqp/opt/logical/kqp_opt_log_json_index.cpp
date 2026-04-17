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
};

struct TExtractJsonResult {
    TString ColumnName;
    TString JsonPath;
    std::optional<EDataSlot> ReturningType;
};

ECallableType JsonCallableType(const TExprBase& node) {
    if (node.Maybe<TCoJsonValue>()) {
        return ECallableType::JsonValue;
    } else if (node.Maybe<TCoJsonExists>()) {
        return ECallableType::JsonExists;
    } else if (node.Maybe<TCoJsonQuery>()) {
        return ECallableType::JsonQuery;
    }

    YQL_ENSURE(false, "Unsupported JSON_* function: " << node.Ref().Content());
    return ECallableType::JsonExists;
}

std::optional<TExtractJsonResult> ExtractJsonQueryBase(const TExprBase& node) {
    if (!node.Maybe<TCoJsonQueryBase>()) {
        return std::nullopt;
    }

    auto jsonNode = node.Cast<TCoJsonQueryBase>();
    if (!jsonNode.Json().template Maybe<TCoMember>()) {
        return std::nullopt;
    }
    if (!jsonNode.JsonPath().template Maybe<TCoUtf8>()) {
        return std::nullopt;
    }

    const auto& variables = jsonNode.Variables().Ref();
    if (!variables.GetTypeAnn() || variables.GetTypeAnn()->GetKind() != ETypeAnnotationKind::EmptyDict) {
        return std::nullopt;
    }

    std::optional<EDataSlot> returningType;
    if (node.Maybe<TCoJsonValue>()) {
        auto jsonValue = node.Cast<TCoJsonValue>();
        if (jsonValue.ReturningType()) {
            const auto* returningTypeAnn = jsonValue.ReturningType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            returningType = returningTypeAnn->Cast<TDataExprType>()->GetSlot();
        }
    }

    return TExtractJsonResult{
        .ColumnName = jsonNode.Json().template Cast<TCoMember>().Name().StringValue(),
        .JsonPath = jsonNode.JsonPath().template Cast<TCoUtf8>().Literal().StringValue(),
        .ReturningType = returningType
    };
}

template <typename TLiteral, typename TNode>
void AppendNumberToJsonPath(const TExprBase& node, TString& value) {
    double literalValue = static_cast<double>(FromString<TLiteral>(node.Cast<TNode>().Literal().Value()));
    AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &literalValue);
}

std::optional<TString> EncodeValueToJsonPath(const TExprBase& node) {
    TString value;

    if (node.Maybe<TCoNull>()) {
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Null);
        return value;
    }

    if (node.Maybe<TCoBool>()) {
        auto boolValue = FromString<bool>(node.Cast<TCoBool>().Literal().Value());
        AppendJsonIndexLiteral(value, boolValue ? NBinaryJson::EEntryType::BoolTrue : NBinaryJson::EEntryType::BoolFalse);
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

std::optional<TPredicateCollectResult> CollectFromJsonCallable(const TString& columnName, const TString& jsonPath,
    ECallableType callableType, std::optional<TExprBase> comparisonValue, TExprContext& ctx, TPositionHandle pos)
{
    NYql::TIssues parseIssues;
    const auto path = NYql::NJsonPath::ParseJsonPath(jsonPath, parseIssues, 1);
    if (!parseIssues.Empty()) {
        auto error = TCollectResult(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Failed to parse jsonpath expression: " << parseIssues.ToOneLineString()));
        return TPredicateCollectResult{"", std::move(error)};
    }

    auto collectResult = CollectJsonPath(path, callableType);
    if (collectResult.IsError()) {
        return TPredicateCollectResult{"", std::move(collectResult)};
    }

    auto& tokens = collectResult.GetTokens();
    if (comparisonValue && !collectResult.IsFinished() && tokens.size() == 1) {
        if (auto encodedValue = EncodeValueToJsonPath(*comparisonValue)) {
            tokens.front() += *encodedValue;
        }
    }

    return TPredicateCollectResult{columnName, std::move(collectResult)};
}

std::optional<TPredicateCollectResult> MergePredicateResults(std::optional<TPredicateCollectResult> left,
    std::optional<TPredicateCollectResult> right, TCollectResult::ETokensMode mode)
{
    if (left && left->Collect.IsError()) {
        return left;
    }
    if (right && right->Collect.IsError()) {
        return right;
    }

    if (!left || !right || left->ColumnName != right->ColumnName) {
        return std::nullopt;
    }

    auto merged = (mode == TCollectResult::ETokensMode::And)
        ? MergeAnd(std::move(left->Collect), std::move(right->Collect))
        : MergeOr(std::move(left->Collect), std::move(right->Collect));
    return TPredicateCollectResult{std::move(left->ColumnName), std::move(merged)};
}

std::optional<TPredicateCollectResult> VisitJsonPredicate(const TExprBase& node, TExprContext& ctx, TPositionHandle pos) {
    if (auto just = node.Maybe<TCoJust>()) {
        return VisitJsonPredicate(just.Cast().Input(), ctx, pos);
    }
    if (auto optionalIf = node.Maybe<TCoOptionalIf>()) {
        return VisitJsonPredicate(optionalIf.Cast().Predicate(), ctx, pos);
    }
    if (auto coalesce = node.Maybe<TCoCoalesce>()) {
        return VisitJsonPredicate(coalesce.Cast().Predicate(), ctx, pos);
    }

    auto createError = [&](const TString& message) -> TPredicateCollectResult {
        return TPredicateCollectResult{"", TCollectResult(TIssue(ctx.GetPosition(pos), message))};
    };

    if (auto maybeAnd = node.Maybe<TCoAnd>()) {
        auto andNode = maybeAnd.Cast();
        if (andNode.ArgCount() == 0) {
            return std::nullopt;
        }
        auto result = VisitJsonPredicate(andNode.Arg(0), ctx, pos);
        for (size_t i = 1; i < andNode.ArgCount(); ++i) {
            auto next = VisitJsonPredicate(andNode.Arg(i), ctx, pos);
            result = MergePredicateResults(std::move(result), std::move(next), TCollectResult::ETokensMode::And);
        }
        return result;
    }

    if (auto maybeOr = node.Maybe<TCoOr>()) {
        auto orNode = maybeOr.Cast();
        if (orNode.ArgCount() == 0) {
            return std::nullopt;
        }
        auto result = VisitJsonPredicate(orNode.Arg(0), ctx, pos);
        for (size_t i = 1; i < orNode.ArgCount(); ++i) {
            auto next = VisitJsonPredicate(orNode.Arg(i), ctx, pos);
            result = MergePredicateResults(std::move(result), std::move(next), TCollectResult::ETokensMode::Or);
        }
        return result;
    }

    if (auto maybeNot = node.Maybe<TCoNot>()) {
        auto notNode = maybeNot.Cast();
        if (auto maybeCoalesce = notNode.Value().Maybe<TCoCoalesce>()) {
            auto coalesceNode = maybeCoalesce.Cast();
            if (!coalesceNode.Predicate().Maybe<TCoJsonValue>()) {
                return std::nullopt;
            }

            auto extracted = ExtractJsonQueryBase(coalesceNode.Predicate().Cast<TCoJsonValue>());
            if (!extracted) {
                return createError("Failed to extract JSON_VALUE from predicate");
            }

            std::optional<TExprBase> comparisonValue;
            if (extracted->ReturningType && extracted->ReturningType == EDataSlot::Bool) {
                comparisonValue = TExprBase(Build<TCoBool>(ctx, pos).Literal().Build(false).Done().Ptr());
            }

            return CollectFromJsonCallable(extracted->ColumnName, extracted->JsonPath,
                ECallableType::JsonValue, comparisonValue, ctx, pos);
        }

        return std::nullopt;
    }

    if (auto maybeExists = node.Maybe<TCoExists>()) {
        return VisitJsonPredicate(maybeExists.Cast().Optional(), ctx, pos);
    }

    if (auto maybeCmp = node.Maybe<TCoCompare>()) {
        auto cmp = maybeCmp.Cast();
        TExprBase jsonSide = cmp.Left();
        TExprBase otherSide = cmp.Right();

        if (!jsonSide.Maybe<TCoJsonQueryBase>()) {
            if (!otherSide.Maybe<TCoJsonQueryBase>()) {
                return std::nullopt;
            }
            std::swap(jsonSide, otherSide);
        }

        if (!jsonSide.Maybe<TCoJsonValue>()) {
            return createError("Expected JSON_VALUE on the left side of comparison");
        }
        if (!otherSide.Maybe<TCoDataCtor>()) {
            return createError("Expected a literal in comparison");
        }

        auto extracted = ExtractJsonQueryBase(jsonSide.Cast<TCoJsonValue>());
        if (!extracted) {
            return createError("Failed to extract JSON_VALUE from comparison");
        }

        auto comparisonValue = (node.Maybe<TCoCmpEqual>()) ? std::make_optional(otherSide) : std::nullopt;
        return CollectFromJsonCallable(extracted->ColumnName, extracted->JsonPath,
            ECallableType::JsonValue, comparisonValue, ctx, pos);
    }

    if (auto maybeJson = node.Maybe<TCoJsonQueryBase>()) {
        auto extracted = ExtractJsonQueryBase(maybeJson.Cast());
        if (!extracted) {
            return createError("Failed to extract JSON_* function");
        }

        std::optional<TExprBase> comparisonValue;
        if (extracted->ReturningType && extracted->ReturningType == EDataSlot::Bool) {
            comparisonValue = TExprBase(Build<TCoBool>(ctx, pos).Literal().Build(true).Done().Ptr());
        }

        return CollectFromJsonCallable(extracted->ColumnName, extracted->JsonPath,
            JsonCallableType(node), comparisonValue, ctx, pos);
    }

    return createError("Unsupported predicate node: " + TString(node.Ref().Content()));
}

} // namespace

std::optional<TJsonIndexSettings> CollectJsonIndexPredicate(const TExprBase& body, const TExprBase& node, TExprContext& ctx) {
    Cout << NYql::NCommon::ExprToPrettyString(ctx, body.Ref()) << Endl;

    auto result = VisitJsonPredicate(body, ctx, node.Pos());
    if (!result) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
            << "Failed to extract search terms from predicate: Predicate is not supported"));
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
