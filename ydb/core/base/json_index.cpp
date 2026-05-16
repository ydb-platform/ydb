#include "json_index.h"

#include <library/cpp/json/json_writer.h>
#include <util/string/join.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/binary_json/format.h>

#include <ranges>

namespace NKikimr::NJsonIndex {

using NYql::TIssue;
using NYql::TIssues;
using namespace NYql::NJsonPath;
namespace {

NBinaryJson::EEntryType GetEntryType(const TJsonPathItem& item) {
    switch (item.Type) {
        case EJsonPathItemType::StringLiteral:
            return NBinaryJson::EEntryType::String;
        case EJsonPathItemType::NumberLiteral:
            return NBinaryJson::EEntryType::Number;
        case EJsonPathItemType::BooleanLiteral:
            if (item.GetBoolean()) {
                return NBinaryJson::EEntryType::BoolTrue;
            } else {
                return NBinaryJson::EEntryType::BoolFalse;
            }
        case EJsonPathItemType::NullLiteral:
            return NBinaryJson::EEntryType::Null;
        default:
            Y_ENSURE(false, "Unexpected item type");
    }
    return NBinaryJson::EEntryType::Null;
}

// Keys may contain binary data (e.g. a zero byte), so we prefix each key with its
// varint-encoded length (LEB128). This allows unambiguous prefix-scan queries.
// Key lengths are encoded as (actual_length + 1) so that the encoded length byte is never
// zero. This reserves \x00 exclusively for the literal separator used in AppendJsonIndexLiteral,
// preventing ambiguity between an empty-key path component and the start of a literal suffix
void AppendKey(TString& prefix, TStringBuf key) {
    size_t size = key.size() + 1;
    do {
        if (size < 0x80) {
            prefix.push_back((ui8)size);
        } else {
            prefix.push_back(0x80 | (ui8)(size & 0x7F));
        }
        size >>= 7;
    } while (size > 0);
    prefix += key;
}

bool IsSuffixType(EJsonPathItemType type) {
    switch (type) {
        case EJsonPathItemType::StringLiteral:
        case EJsonPathItemType::NumberLiteral:
        case EJsonPathItemType::BooleanLiteral:
        case EJsonPathItemType::NullLiteral:
        case EJsonPathItemType::Variable:
            return true;
        default:
            return false;
    }
}

bool IsPredicateType(EJsonPathItemType type) {
    switch (type) {
        case EJsonPathItemType::UnaryNot:
        case EJsonPathItemType::BinaryAnd:
        case EJsonPathItemType::BinaryOr:
        case EJsonPathItemType::BinaryLess:
        case EJsonPathItemType::BinaryLessEqual:
        case EJsonPathItemType::BinaryGreater:
        case EJsonPathItemType::BinaryGreaterEqual:
        case EJsonPathItemType::BinaryEqual:
        case EJsonPathItemType::BinaryNotEqual:
        case EJsonPathItemType::StartsWithPredicate:
        case EJsonPathItemType::LikeRegexPredicate:
        case EJsonPathItemType::IsUnknownPredicate:
        case EJsonPathItemType::ExistsPredicate:
            return true;
        default:
            return false;
    }
}

// Remove redundant tokens based on prefix (ancestor/descendant) relationships
//
// OR  -> keep roots (minimal tokens): if token A is a prefix of token B, B is redundant
//        because any match for B also matches A (ancestor covers descendant)
//
// AND -> keep leaves (maximal tokens): if token A is a prefix of token B, A is redundant
//        because requiring both A and B is satisfied by B alone (descendant implies ancestor)
//
// String prefix check is unambiguous because:
//   1. AppendKey encodes key lengths as (actual_length + 1), so no key-length byte is ever \x00
//   2. AppendJsonIndexLiteral always starts the literal suffix with \x00
// Therefore \x00 can only appear at the start of a literal suffix, never inside the path portion
// A token B whose content after position A.size() starts with \x00 has the SAME path as A but
// carries a value constraint - still a valid ancestor/descendant relationship for pruning.
void PruneRedundantTokens(TTokens& tokens, TCollectResult::ETokensMode mode) {
    if (tokens.size() <= 1 || mode == TCollectResult::ETokensMode::NotSet) {
        return;
    }

    TTokens result;
    std::optional<TToken> lastKept;

    if (mode == TCollectResult::ETokensMode::Or) {
        // OR: keep minimal tokens (roots). Token is redundant if a shorter prefix is already kept
        for (const auto& token : tokens) {
            if (lastKept.has_value() && token.PathToken.StartsWith(lastKept->PathToken)) {
                continue;
            }

            result.insert(token);
            if (token.ParamName.empty()) {
                lastKept = token;
            }
        }
    } else {
        // AND: keep maximal tokens (leaves). Token is redundant if a longer token that starts with it is already kept
        for (const auto& token : std::ranges::reverse_view(tokens)) {
             if (token.ParamName.empty() && lastKept.has_value() && lastKept->PathToken.StartsWith(token.PathToken)) {
                continue;
            }

            result.insert(token);
            lastKept = token;
        }
    }

    tokens = std::move(result);
}

TCollectResult MergeBooleanOperands(TCollectResult left, TCollectResult right,
    TCollectResult::ETokensMode incompatibleMode, TCollectResult::ETokensMode combinedMode)
{
    if (left.IsError()) {
        return left;
    }
    if (right.IsError()) {
        return right;
    }

    auto& leftTokens = left.GetTokens();
    auto& rightTokens = right.GetTokens();

    bool hasMix = false;
    if (!leftTokens.empty() && !rightTokens.empty()) {
        if (left.GetTokensMode() == incompatibleMode || right.GetTokensMode() == incompatibleMode) {
            hasMix = true;
        }
    }

    leftTokens.insert(rightTokens.begin(), rightTokens.end());
    if (leftTokens.size() > 1) {
        auto finalMode = hasMix ? TCollectResult::ETokensMode::Or : combinedMode;
        left.SetTokensMode(finalMode);
        PruneRedundantTokens(leftTokens, finalMode);
    }
    return left;
}

TCollectResult MergeComparisonPathResults(TCollectResult left, TCollectResult right) {
    if (left.IsError()) {
        return left;
    }
    if (right.IsError()) {
        return right;
    }

    auto& leftTokens = left.GetTokens();
    auto& rightTokens = right.GetTokens();

    bool hasMix = false;
    if (!leftTokens.empty() && !rightTokens.empty()) {
        if (left.GetTokensMode() == TCollectResult::ETokensMode::Or ||
            right.GetTokensMode() == TCollectResult::ETokensMode::Or) {
            hasMix = true;
        }
    }

    leftTokens.insert(rightTokens.begin(), rightTokens.end());
    if (leftTokens.size() > 1) {
        left.SetTokensMode(hasMix ? TCollectResult::ETokensMode::Or : TCollectResult::ETokensMode::And);
    }
    left.StopCollecting();
    return left;
}

class TQueryCollector {
    enum class EMode {
        // Main path from the context item ($): accumulate index tokens along the query
        // Predicate operators are disallowed when the callable is JSON_EXISTS (existence vs. boolean result)
        Context = 0,

        // Body of a filter (? ...): the current node is @
        // Predicates are allowed here, including for JSON_EXISTS
        Filter = 1,

        // Subexpression under a predicate (exists(...) argument or path side of a comparison)
        // Nested predicate syntax (AND/OR, further comparisons, etc.) is rejected
        Predicate = 2,

        // Comparison RHS: scalar literals (string, number, bool, null)
        // Also supports unary arithmetic operations (+, -) for numbers and variables
        Literal = 3
    };

public:
    TQueryCollector(
        const TJsonPathPtr path, ECallableType callableType,
        const std::unordered_map<TString, TString>& variables,
        const std::unordered_map<TString, TString>& paramVariables
    )
        : Reader(path)
        , CallableType(callableType)
        , Variables(variables)
        , ParamVariables(paramVariables.begin(), paramVariables.end())
    {
    }

    TCollectResult Collect() {
        return Collect(Reader.ReadFirst(), EMode::Context);
    }

private:
    TCollectResult Collect(const TJsonPathItem& item, EMode mode);

    TCollectResult CollectEqualOperands(const TJsonPathItem& leftItem, const TJsonPathItem& rightItem);
    TCollectResult CollectArithmeticOperand(const TJsonPathItem& item, EMode mode);

    TCollectResult ContextObject(EMode mode);

    TCollectResult MemberAccess(const TJsonPathItem& item, EMode mode);
    TCollectResult WildcardMemberAccess(const TJsonPathItem& item, EMode mode);

    TCollectResult ArrayAccess(const TJsonPathItem& item, EMode mode);

    TCollectResult UnaryArithmeticOp(const TJsonPathItem& item, EMode mode);
    TCollectResult BinaryArithmeticOp(const TJsonPathItem& item, EMode mode);

    TCollectResult BinaryAnd(const TJsonPathItem& item, EMode mode);
    TCollectResult BinaryOr(const TJsonPathItem& item, EMode mode);
    TCollectResult BinaryEqual(const TJsonPathItem& item, EMode mode);
    TCollectResult BinaryComparisonOp(const TJsonPathItem& item, EMode mode);

    TCollectResult Methods(const TJsonPathItem& item, EMode mode);
    TCollectResult Predicates(const TJsonPathItem& item, EMode mode);

    TCollectResult FilterObject(const TJsonPathItem& item, EMode mode);
    TCollectResult FilterPredicate(const TJsonPathItem& item, EMode mode);

    TCollectResult Literal(const TJsonPathItem& item, EMode mode);
    TCollectResult Variable(const TJsonPathItem& item, EMode mode);

    std::optional<double> EvaluteNumericLiteral(const TJsonPathItem& item);
    bool ArePredicatesAllowed(EMode mode) const;

private:
    TJsonPathReader Reader;
    ECallableType CallableType;
    std::unordered_map<TString, TString> Variables;
    std::unordered_map<TString, TString> ParamVariables;
    TVector<TString> FilterObjectPrefixes;
};

TCollectResult TQueryCollector::Collect(const TJsonPathItem& item, EMode mode) {
    const bool isUnaryOp = item.Type == EJsonPathItemType::UnaryMinus || item.Type == EJsonPathItemType::UnaryPlus;
    if (mode == EMode::Literal && !(IsSuffixType(item.Type) || isUnaryOp)) {
        return TCollectResult(TIssue("Expected a literal expression"));
    }

    if (mode == EMode::Predicate && IsPredicateType(item.Type)) {
        return TCollectResult(TIssue("Predicates are not allowed in this context"));
    }

    switch (item.Type) {
        case EJsonPathItemType::MemberAccess:
            return MemberAccess(item, mode);
        case EJsonPathItemType::WildcardMemberAccess:
            return WildcardMemberAccess(item, mode);
        case EJsonPathItemType::ContextObject:
            return ContextObject(mode);
        case EJsonPathItemType::Variable:
            return Variable(item, mode);
        case EJsonPathItemType::ArrayAccess:
        case EJsonPathItemType::WildcardArrayAccess:
        case EJsonPathItemType::LastArrayIndex:
            return ArrayAccess(item, mode);
        case EJsonPathItemType::UnaryMinus:
        case EJsonPathItemType::UnaryPlus:
            return UnaryArithmeticOp(item, mode);
        case EJsonPathItemType::BinaryAdd:
        case EJsonPathItemType::BinarySubstract:
        case EJsonPathItemType::BinaryMultiply:
        case EJsonPathItemType::BinaryDivide:
        case EJsonPathItemType::BinaryModulo:
            return BinaryArithmeticOp(item, mode);
        case EJsonPathItemType::BinaryAnd:
            return BinaryAnd(item, mode);
        case EJsonPathItemType::BinaryOr:
            return BinaryOr(item, mode);
        case EJsonPathItemType::NumberLiteral:
        case EJsonPathItemType::BooleanLiteral:
        case EJsonPathItemType::NullLiteral:
        case EJsonPathItemType::StringLiteral:
            return Literal(item, mode);
        case EJsonPathItemType::FilterObject:
            return FilterObject(item, mode);
        case EJsonPathItemType::FilterPredicate:
            return FilterPredicate(item, mode);
        case EJsonPathItemType::BinaryEqual:
            return BinaryEqual(item, mode);
        case EJsonPathItemType::BinaryLess:
        case EJsonPathItemType::BinaryLessEqual:
        case EJsonPathItemType::BinaryGreater:
        case EJsonPathItemType::BinaryGreaterEqual:
        case EJsonPathItemType::BinaryNotEqual:
            return BinaryComparisonOp(item, mode);
        case EJsonPathItemType::AbsMethod:
        case EJsonPathItemType::FloorMethod:
        case EJsonPathItemType::CeilingMethod:
        case EJsonPathItemType::DoubleMethod:
        case EJsonPathItemType::TypeMethod:
        case EJsonPathItemType::SizeMethod:
        case EJsonPathItemType::KeyValueMethod:
            return Methods(item, mode);
        case EJsonPathItemType::UnaryNot:
        case EJsonPathItemType::StartsWithPredicate:
        case EJsonPathItemType::LikeRegexPredicate:
        case EJsonPathItemType::IsUnknownPredicate:
        case EJsonPathItemType::ExistsPredicate:
            return Predicates(item, mode);
    }
}

TCollectResult TQueryCollector::Literal(const TJsonPathItem& item, EMode mode) {
    if (mode != EMode::Literal) {
        return TCollectResult(TIssue("Literal expressions are not allowed in this context"));
    }

    TString value;
    switch (item.Type) {
        case EJsonPathItemType::StringLiteral:
            AppendJsonIndexLiteral(value, GetEntryType(item), item.GetString(), nullptr);
            break;
        case EJsonPathItemType::NumberLiteral: {
            double number = item.GetNumber();
            AppendJsonIndexLiteral(value, GetEntryType(item), {}, &number);
            break;
        }
        case EJsonPathItemType::BooleanLiteral:
        case EJsonPathItemType::NullLiteral:
            AppendJsonIndexLiteral(value, GetEntryType(item), {}, nullptr);
            break;
        default:
            return TCollectResult(TIssue("Expected a literal expression"));
    }

    return TCollectResult(std::move(value));
}

TCollectResult TQueryCollector::ContextObject(EMode mode) {
    Y_UNUSED(mode);
    return TCollectResult("");
}

TCollectResult TQueryCollector::MemberAccess(const TJsonPathItem& item, EMode mode) {
    auto result = Collect(Reader.ReadInput(item), mode);
    if (!result.CanCollect()) {
        return result;
    }

    auto& tokens = result.GetTokens();
    auto node = tokens.extract(tokens.begin());
    AppendKey(node.value().PathToken, item.GetString());
    tokens.insert(std::move(node));
    return result;
}

TCollectResult TQueryCollector::WildcardMemberAccess(const TJsonPathItem& item, EMode mode) {
    auto result = Collect(Reader.ReadInput(item), mode);
    result.StopCollecting();
    return result;
}

TCollectResult TQueryCollector::ArrayAccess(const TJsonPathItem& item, EMode mode) {
    return Collect(Reader.ReadInput(item), mode);
}

TCollectResult TQueryCollector::UnaryArithmeticOp(const TJsonPathItem& item, EMode mode) {
    if (mode == EMode::Literal) {
        auto val = EvaluteNumericLiteral(item);
        if (!val) {
            return TCollectResult(TIssue("Expected a numeric literal expression"));
        }

        TString value;
        double number = *val;
        AppendJsonIndexLiteral(value, NBinaryJson::EEntryType::Number, {}, &number);
        return TCollectResult(std::move(value));
    }

    auto result = Collect(Reader.ReadInput(item), mode);
    result.StopCollecting();
    return result;
}

TCollectResult TQueryCollector::BinaryArithmeticOp(const TJsonPathItem& item, EMode mode) {
    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);

    auto leftCollectResult = CollectArithmeticOperand(leftItem, mode);
    auto rightCollectResult = CollectArithmeticOperand(rightItem, mode);
    return MergeComparisonPathResults(std::move(leftCollectResult), std::move(rightCollectResult));
}

TCollectResult TQueryCollector::BinaryAnd(const TJsonPathItem& item, EMode mode) {
    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);
    auto leftCollectResult = Collect(leftItem, mode);
    auto rightCollectResult = Collect(rightItem, mode);
    return MergeBooleanOperands(std::move(leftCollectResult), std::move(rightCollectResult),
        TCollectResult::ETokensMode::Or, TCollectResult::ETokensMode::And);
}

TCollectResult TQueryCollector::BinaryOr(const TJsonPathItem& item, EMode mode) {
    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);
    auto leftCollectResult = Collect(leftItem, mode);
    auto rightCollectResult = Collect(rightItem, mode);
    return MergeBooleanOperands(std::move(leftCollectResult), std::move(rightCollectResult),
        TCollectResult::ETokensMode::And, TCollectResult::ETokensMode::Or);
}

TCollectResult TQueryCollector::BinaryEqual(const TJsonPathItem& item, EMode mode) {
    if (!ArePredicatesAllowed(mode)) {
        return TCollectResult(TIssue("Predicates are not allowed in this context"));
    }

    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);

    const bool leftIsLiteral = IsSuffixType(leftItem.Type) || EvaluteNumericLiteral(leftItem).has_value();
    const bool rightIsLiteral = IsSuffixType(rightItem.Type) || EvaluteNumericLiteral(rightItem).has_value();

    if (!leftIsLiteral && rightIsLiteral) {
        return CollectEqualOperands(leftItem, rightItem);
    }

    if (leftIsLiteral && !rightIsLiteral) {
        return CollectEqualOperands(rightItem, leftItem);
    }

    if (!leftIsLiteral && !rightIsLiteral) {
        auto leftCollectResult = CollectArithmeticOperand(leftItem, EMode::Predicate);
        auto rightCollectResult = CollectArithmeticOperand(rightItem, EMode::Predicate);
        return MergeComparisonPathResults(std::move(leftCollectResult), std::move(rightCollectResult));
    }

    return TCollectResult(TIssue("Comparison is not allowed between literals/variables on both sides"));
}

TCollectResult TQueryCollector::BinaryComparisonOp(const TJsonPathItem& item, EMode mode) {
    if (!ArePredicatesAllowed(mode)) {
        return TCollectResult(TIssue("Predicates are not allowed in this context"));
    }

    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);

    auto leftCollectResult = CollectArithmeticOperand(leftItem, EMode::Predicate);
    auto rightCollectResult = CollectArithmeticOperand(rightItem, EMode::Predicate);
    return MergeComparisonPathResults(std::move(leftCollectResult), std::move(rightCollectResult));
}

TCollectResult TQueryCollector::FilterObject(const TJsonPathItem& item, EMode mode) {
    Y_UNUSED(item);
    Y_UNUSED(mode);
    if (FilterObjectPrefixes.empty()) {
        return TCollectResult(TIssue("'@' is only allowed inside filters"));
    }
    return TCollectResult(TString(FilterObjectPrefixes.back()));
}

TCollectResult TQueryCollector::FilterPredicate(const TJsonPathItem& item, EMode mode) {
    auto inputCollectResult = Collect(Reader.ReadInput(item), mode);
    if (inputCollectResult.IsError()) {
        return inputCollectResult;
    }

    const auto& tokens = inputCollectResult.GetTokens();

    // If input path is already stopped (e.g. $.a.*) or has multiple tokens,
    // we can't apply the filter to narrow down
    if (!inputCollectResult.CanCollect()) {
        inputCollectResult.StopCollecting();
        return inputCollectResult;
    }

    FilterObjectPrefixes.push_back(tokens.begin()->PathToken);
    const auto& predicateItem = Reader.ReadFilterPredicate(item);
    auto predicateResult = Collect(predicateItem, EMode::Filter);
    FilterObjectPrefixes.pop_back();

    if (predicateResult.IsError()) {
        return predicateResult;
    }

    predicateResult.StopCollecting();
    return predicateResult;
}

TCollectResult TQueryCollector::Methods(const TJsonPathItem& item, EMode mode) {
    const auto& input = Reader.ReadInput(item);
    if (IsSuffixType(input.Type) || EvaluteNumericLiteral(input).has_value()) {
        TCollectResult result(TTokens{});
        result.StopCollecting();
        return result;
    }

    auto result = Collect(input, mode);
    result.StopCollecting();
    return result;
}

TCollectResult TQueryCollector::Predicates(const TJsonPathItem& item, EMode mode) {
    if (!ArePredicatesAllowed(mode)) {
        return TCollectResult(TIssue("Predicates are not allowed in this context"));
    }

    auto result = Collect(Reader.ReadInput(item), EMode::Predicate);
    result.StopCollecting();
    return result;
}

TCollectResult TQueryCollector::CollectEqualOperands(const TJsonPathItem& leftItem, const TJsonPathItem& rightItem) {
    auto pathResult = Collect(leftItem, EMode::Predicate);
    if (pathResult.IsError()) {
        return pathResult;
    }

    auto literalResult = Collect(rightItem, EMode::Literal);
    if (literalResult.IsError()) {
        return literalResult;
    }

    auto& pathTokens = pathResult.GetTokens();
    auto& literalTokens = literalResult.GetTokens();
    if (pathResult.CanCollect() && literalTokens.size() == 1) {
        auto node = pathTokens.extract(pathTokens.begin());
        const auto& literalToken = *literalTokens.begin();

        if (!literalToken.ParamName.empty()) {
            // Parametric variable: record the variable name so the caller can append a runtime value
            node.value().ParamName = literalToken.ParamName;
        } else {
            // Regular literal: append the encoded value suffix to the path token
            node.value().PathToken += literalToken.PathToken;
        }

        pathTokens.insert(std::move(node));
    }

    pathResult.StopCollecting();
    return pathResult;
}

TCollectResult TQueryCollector::CollectArithmeticOperand(const TJsonPathItem& item, EMode mode) {
    return IsSuffixType(item.Type) || EvaluteNumericLiteral(item).has_value() ? TCollectResult(TTokens{}) : Collect(item, mode);
}

TCollectResult TQueryCollector::Variable(const TJsonPathItem& item, EMode mode) {
    if (mode != EMode::Literal) {
        return TCollectResult(TIssue("Variables are not allowed in this context"));
    }

    auto name = TString(item.GetString());

    if (auto it = ParamVariables.find(name); it != ParamVariables.end()) {
        TTokens tokens;
        tokens.emplace("", it->second);
        return TCollectResult(std::move(tokens));
    }

    if (auto it = Variables.find(name); it != Variables.end()) {
        return TCollectResult(TString(it->second));
    }

    return TCollectResult(TTokens{});
}

std::optional<double> TQueryCollector::EvaluteNumericLiteral(const TJsonPathItem& item) {
    switch (item.Type) {
        case EJsonPathItemType::NumberLiteral:
            return item.GetNumber();
        case EJsonPathItemType::UnaryMinus: {
            auto val = EvaluteNumericLiteral(Reader.ReadInput(item));
            return val ? std::optional<double>(-*val) : std::nullopt;
        }
        case EJsonPathItemType::UnaryPlus:
            return EvaluteNumericLiteral(Reader.ReadInput(item));
        default:
            return std::nullopt;
    }
}

bool TQueryCollector::ArePredicatesAllowed(EMode mode) const {
    switch (mode) {
        case EMode::Context:
            // JSON_EXISTS returns true for any non-empty result, including a single
            // boolean false from the predicate with a context object.
            // There is a context object if the tokens are not empty
            return CallableType != ECallableType::JsonExists;
        case EMode::Filter:
            return true;
        case EMode::Predicate:
        case EMode::Literal:
            return false;
    }
}

void TokenizeBinaryJson(const NBinaryJson::TContainerCursor& root, const TString& prefix, TVector<TString>& tokens);

void TokenizeBinaryJson(NBinaryJson::TEntryCursor element, const TString& prefix, TVector<TString>& tokens) {
    if (element.GetType() == NBinaryJson::EEntryType::Container) {
        TokenizeBinaryJson(element.GetContainer(), prefix, tokens);
        return;
    }
    TString token = prefix;
    switch (element.GetType()) {
    case NBinaryJson::EEntryType::String:
        AppendJsonIndexLiteral(token, element.GetType(), element.GetString(), nullptr);
        break;
    case NBinaryJson::EEntryType::Number: {
        double number = element.GetNumber();
        AppendJsonIndexLiteral(token, element.GetType(), {}, &number);
        break;
    }
    case NBinaryJson::EEntryType::BoolFalse:
    case NBinaryJson::EEntryType::BoolTrue:
    case NBinaryJson::EEntryType::Null:
        AppendJsonIndexLiteral(token, element.GetType(), {}, nullptr);
        break;
    case NBinaryJson::EEntryType::Container:
        Y_ENSURE(false, "Unreachable");
        break;
    }
    tokens.push_back(token);
}

TString TokenizeJsonNextPrefix(const TString& prefix, TStringBuf key) {
    TString newPrefix = prefix;
    AppendKey(newPrefix, key);
    return newPrefix;
}

void TokenizeBinaryJson(const NBinaryJson::TContainerCursor& root, const TString& prefix, TVector<TString>& tokens) {
    switch (root.GetType()) {
    case NBinaryJson::EContainerType::TopLevelScalar:
        TokenizeBinaryJson(root.GetElement(0), prefix, tokens);
        break;
    case NBinaryJson::EContainerType::Array:
        for (ui32 pos = 0; pos < root.GetSize(); pos++) {
            TokenizeBinaryJson(root.GetElement(pos), prefix, tokens);
        }
        break;
    case NBinaryJson::EContainerType::Object:
        auto it = root.GetObjectIterator();
        while (it.HasNext()) {
            auto kv = it.Next();
            Y_ENSURE(kv.first.GetType() == NBinaryJson::EEntryType::String);
            TString nextPrefix = TokenizeJsonNextPrefix(prefix, kv.first.GetString());
            tokens.push_back(nextPrefix);
            TokenizeBinaryJson(kv.second, nextPrefix, tokens);
        }
        break;
    }
}

}  // namespace

void AppendJsonIndexLiteral(TString& out, NBinaryJson::EEntryType type, TStringBuf stringPayload,
    const double* numberPayload)
{
    out.push_back(0);
    out.push_back(static_cast<char>(type));
    switch (type) {
        case NBinaryJson::EEntryType::String:
            out += stringPayload;
            break;
        case NBinaryJson::EEntryType::Number:
            Y_ENSURE(numberPayload, "Number payload required");
            out += TStringBuf(reinterpret_cast<const char*>(numberPayload), sizeof(double));
            break;
        case NBinaryJson::EEntryType::BoolFalse:
        case NBinaryJson::EEntryType::BoolTrue:
        case NBinaryJson::EEntryType::Null:
            Y_ENSURE(!numberPayload, "No number payload for bool/null literal");
            break;
        case NBinaryJson::EEntryType::Container:
            Y_ENSURE(false, "Container is not a scalar literal");
    }
}

TCollectResult::TCollectResult(TTokens&& tokens)
    : Result(std::move(tokens))
{
}

TCollectResult::TCollectResult(TString&& token) {
    TTokens tokens;
    tokens.emplace(std::move(token), "");
    Result = std::move(tokens);
}

TCollectResult::TCollectResult(TCollectResult::TError&& issue)
    : Result(std::move(issue))
{
}

const TTokens& TCollectResult::GetTokens() const {
    Y_ENSURE(!IsError(), "Result is not a query");
    return std::get<TTokens>(Result);
}

TTokens& TCollectResult::GetTokens() {
    Y_ENSURE(!IsError(), "Result is not a query");
    return std::get<TTokens>(Result);
}

const TCollectResult::TError& TCollectResult::GetError() const {
    Y_ENSURE(IsError(), "Result is not an error");
    return std::get<TCollectResult::TError>(Result);
}

bool TCollectResult::IsError() const {
    return std::holds_alternative<TCollectResult::TError>(Result);
}

void TCollectResult::StopCollecting() {
    Stopped = true;
}

bool TCollectResult::CanCollect() const {
    return !IsError() && !Stopped && GetTokens().size() == 1;
}

TCollectResult::ETokensMode TCollectResult::GetTokensMode() const {
    return TokensMode;
}

void TCollectResult::SetTokensMode(ETokensMode mode) {
    TokensMode = mode;
}

TVector<TString> TokenizeBinaryJson(TStringBuf text) {
    TVector<TString> tokens;
    if (text.empty()) {
        return tokens;
    }
    tokens.emplace_back();
    auto reader = NKikimr::NBinaryJson::TBinaryJsonReader::Make(text);
    TokenizeBinaryJson(reader->GetRootCursor(), "", tokens);
    return tokens;
}

TVector<TString> TokenizeJson(TStringBuf text, TString& error) {
    auto json = NKikimr::NBinaryJson::SerializeToBinaryJson(text);
    if (std::holds_alternative<TString>(json)) {
        error = std::get<TString>(json);
        return TVector<TString>();
    }
    error = "";
    auto buffer = std::get<NKikimr::NBinaryJson::TBinaryJson>(json);
    return TokenizeBinaryJson(TStringBuf(buffer.data(), buffer.size()));
}

TCollectResult CollectJsonPath(const TJsonPathPtr& path, ECallableType callableType,
    const std::unordered_map<TString, TString>& variables, const std::unordered_map<TString, TString>& paramVariables)
{
    auto result = TQueryCollector(path, callableType, variables, paramVariables).Collect();
    if (!result.IsError()) {
        if (result.GetTokens().empty()) {
            result = TCollectResult(TIssue("Cannot collect tokens for the given JSON path"));
        }

        if (callableType != ECallableType::JsonValue) {
            result.StopCollecting();
        }
    }

    return result;
}

TCollectResult MergeAnd(TCollectResult left, TCollectResult right) {
    return MergeBooleanOperands(std::move(left), std::move(right),
        TCollectResult::ETokensMode::Or, TCollectResult::ETokensMode::And);
}

TCollectResult MergeOr(TCollectResult left, TCollectResult right) {
    return MergeBooleanOperands(std::move(left), std::move(right),
        TCollectResult::ETokensMode::And, TCollectResult::ETokensMode::Or);
}

TString FormatJsonIndexToken(const TString& pathToken, const TString& paramName) {
    TStringStream ss;
    NJson::TJsonWriter writer(&ss, /* formatOutput */ false);
    writer.OpenMap();

    size_t nullPos = pathToken.find('\0');
    size_t pathEnd = (nullPos == TString::npos) ? pathToken.size() : nullPos;

    // Decode LEB128-prefixed key segments from the path portion
    if (pathEnd > 0) {
        std::vector<TString> path;
        size_t pos = 0;

        while (pos < pathEnd) {
            size_t encodedLength = 0;
            int shift = 0;
            while (pos < pathEnd) {
                ui8 byte = static_cast<ui8>(pathToken[pos++]);
                encodedLength |= static_cast<size_t>(byte & 0x7F) << shift;
                shift += 7;
                if (!(byte & 0x80)) {
                    break;
                }
            }

            if (encodedLength == 0) {
                break;
            }

            size_t keyLength = encodedLength - 1;
            if (pos + keyLength > pathEnd) {
                break;
            }

            path.push_back(pathToken.substr(pos, keyLength));
            pos += keyLength;
        }

        if (!path.empty()) {
            writer.Write("path", JoinSeq(".", path));
        }
    }

    // Decode literal suffix
    if (nullPos != TString::npos && nullPos + 1 < pathToken.size()) {
        auto typeCode = static_cast<NBinaryJson::EEntryType>(static_cast<ui8>(pathToken[nullPos + 1]));

        switch (typeCode) {
            case NBinaryJson::EEntryType::BoolFalse:
                writer.Write("literal", false);
                break;
            case NBinaryJson::EEntryType::BoolTrue:
                writer.Write("literal", true);
                break;
            case NBinaryJson::EEntryType::Null:
                writer.Write("literal", NJson::TJsonValue(NJson::JSON_NULL));
                break;
            case NBinaryJson::EEntryType::String:
                writer.Write("literal", pathToken.substr(nullPos + 2));
                break;
            case NBinaryJson::EEntryType::Number:
                if (nullPos + 2 + sizeof(double) <= pathToken.size()) {
                    double d;
                    memcpy(&d, pathToken.data() + nullPos + 2, sizeof(double));
                    writer.Write("literal", d);
                }
                break;
            default:
                break;
        }
    }

    // Param name
    if (!paramName.empty()) {
        writer.Write("param", paramName);
    }

    writer.CloseMap();
    writer.Flush();
    return ss.Str();
}

} // namespace NKikimr::NJsonIndex
