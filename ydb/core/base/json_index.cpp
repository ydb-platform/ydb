#include "json_index.h"

#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/binary_json/format.h>

namespace NKikimr {

namespace NJsonIndex {

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
void AppendKey(TString& prefix, TStringBuf key) {
    size_t size = key.size();
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

bool IsLiteralType(EJsonPathItemType type) {
    switch (type) {
        case EJsonPathItemType::StringLiteral:
        case EJsonPathItemType::NumberLiteral:
        case EJsonPathItemType::BooleanLiteral:
        case EJsonPathItemType::NullLiteral:
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
        left.SetTokensMode(hasMix ? TCollectResult::ETokensMode::Or : combinedMode);
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
        // Also supports unary arithmetic operations (+, -) for numbers
        Literal = 3
    };

public:
    TQueryCollector(const TJsonPathPtr path, ECallableType callableType)
        : Reader(path)
        , CallableType(callableType)
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
    TVector<TString> FilterObjectPrefixes;
};

TCollectResult TQueryCollector::Collect(const TJsonPathItem& item, EMode mode) {
    const bool isUnaryOp = item.Type == EJsonPathItemType::UnaryMinus || item.Type == EJsonPathItemType::UnaryPlus;
    if (mode == EMode::Literal && !IsLiteralType(item.Type) && !isUnaryOp) {
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
    AppendKey(node.value(), item.GetString());
    tokens.insert(std::move(node));
    return result;
}

TCollectResult TQueryCollector::WildcardMemberAccess(const TJsonPathItem& item, EMode mode) {
    auto result = Collect(Reader.ReadInput(item), mode);
    result.StopCollecting();
    if (!result.IsError() && result.GetTokens().size() > 1) {
        return TCollectResult(TIssue("Expected at most one result, but got " + std::to_string(result.GetTokens().size())));
    }
    return result;
}

TCollectResult TQueryCollector::ArrayAccess(const TJsonPathItem& item, EMode mode) {
    auto result = Collect(Reader.ReadInput(item), mode);
    if (!result.IsError() && result.GetTokens().size() > 1) {
        return TCollectResult(TIssue("Expected at most one result, but got " + std::to_string(result.GetTokens().size())));
    }
    return result;
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
    if (leftCollectResult.IsError()) {
        return leftCollectResult;
    }

    auto rightCollectResult = CollectArithmeticOperand(rightItem, mode);
    if (rightCollectResult.IsError()) {
        return rightCollectResult;
    }

    auto& leftTokens = leftCollectResult.GetTokens();
    auto& rightTokens = rightCollectResult.GetTokens();

    bool hasMix = false;
    if (!leftTokens.empty() && !rightTokens.empty()) {
        if (leftCollectResult.GetTokensMode() == TCollectResult::ETokensMode::Or ||
            rightCollectResult.GetTokensMode() == TCollectResult::ETokensMode::Or) {
            hasMix = true;
        }
    }

    leftTokens.insert(rightTokens.begin(), rightTokens.end());
    if (leftTokens.size() > 1) {
        leftCollectResult.SetTokensMode(hasMix ? TCollectResult::ETokensMode::Or : TCollectResult::ETokensMode::And);
    }
    leftCollectResult.StopCollecting();
    return leftCollectResult;
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

    const bool leftIsLiteral = IsLiteralType(leftItem.Type) || EvaluteNumericLiteral(leftItem).has_value();
    const bool rightIsLiteral = IsLiteralType(rightItem.Type) || EvaluteNumericLiteral(rightItem).has_value();

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

    return TCollectResult(TIssue("Comparison is not allowed between literals on both sides"));
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

    FilterObjectPrefixes.push_back(*tokens.begin());
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
    auto result = Collect(Reader.ReadInput(item), mode);
    result.StopCollecting();
    if (!result.IsError() && result.GetTokens().size() > 1) {
        return TCollectResult(TIssue("Expected at most one result, but got " + std::to_string(result.GetTokens().size())));
    }
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
        node.value() += *literalTokens.begin();
        pathTokens.insert(std::move(node));
    }

    pathResult.StopCollecting();
    return pathResult;
}

TCollectResult TQueryCollector::CollectArithmeticOperand(const TJsonPathItem& item, EMode mode) {
    if (IsLiteralType(item.Type) || EvaluteNumericLiteral(item).has_value()) {
        return TCollectResult(TCollectResult::TTokens{});
    }
    return Collect(item, mode);
}

TCollectResult TQueryCollector::Variable(const TJsonPathItem& item, EMode mode) {
    Y_UNUSED(item);
    Y_UNUSED(mode);
    return TCollectResult(TIssue("Variables are not supported at the moment"));
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
    tokens.insert(std::move(token));
    Result = std::move(tokens);
}

TCollectResult::TCollectResult(TCollectResult::TError&& issue)
    : Result(std::move(issue))
{
}

const TCollectResult::TTokens& TCollectResult::GetTokens() const {
    Y_ENSURE(!IsError(), "Result is not a query");
    return std::get<TTokens>(Result);
}

TCollectResult::TTokens& TCollectResult::GetTokens() {
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

TVector<TString> TokenizeBinaryJson(const TStringBuf binaryJson) {
    TVector<TString> tokens;
    if (!binaryJson.size()) {
        return tokens;
    }
    tokens.emplace_back();
    auto reader = NKikimr::NBinaryJson::TBinaryJsonReader::Make(binaryJson);
    TokenizeBinaryJson(reader->GetRootCursor(), "", tokens);
    return tokens;
}

TVector<TString> TokenizeJson(const TStringBuf jsonStr, TString& error) {
    auto json = NKikimr::NBinaryJson::SerializeToBinaryJson(jsonStr);
    if (std::holds_alternative<TString>(json)) {
        error = std::get<TString>(json);
        return TVector<TString>();
    }
    error = "";
    auto buffer = std::get<NKikimr::NBinaryJson::TBinaryJson>(json);
    return TokenizeBinaryJson(TStringBuf(buffer.data(), buffer.size()));
}

TCollectResult CollectJsonPath(const TJsonPathPtr path, ECallableType callableType) {
    auto result = TQueryCollector(path, callableType).Collect();
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

}  // namespace NJsonIndex

}  // namespace NKikimr
