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

}  // namespace

TResult::TResult()
    : Result(TTokens{})
{
}

TResult::TResult(TTokens&& tokens)
    : Result(std::move(tokens))
{
}

TResult::TResult(TString&& token)
    : Result(TTokens{std::move(token)})
{
}

TResult::TResult(TResult::TError&& issue)
    : Result(std::move(issue))
{
}

const TResult::TTokens& TResult::GetTokens() const {
    Y_ENSURE(!IsError(), "Result is not a query");
    return std::get<TTokens>(Result);
}

TResult::TTokens& TResult::GetTokens() {
    Y_ENSURE(!IsError(), "Result is not a query");
    return std::get<TTokens>(Result);
}

const TResult::TError& TResult::GetError() const {
    Y_ENSURE(IsError(), "Result is not an error");
    return std::get<TResult::TError>(Result);
}

bool TResult::IsError() const {
    return std::holds_alternative<TResult::TError>(Result);
}

bool TResult::IsFinished() const {
    return Finished;
}

bool TResult::CanCollect() const {
    return !IsError() && !IsFinished();
}

void TResult::Finish() {
    Finished = true;
}

TResult::ETokensMode TResult::GetTokensMode() const {
    return TokensMode;
}

void TResult::SetTokensMode(ETokensMode mode) {
    TokensMode = mode;
}

TQueryCollector::TQueryCollector(const TJsonPathPtr path, ECallableType callableType)
    : Reader(path)
    , CallableType(callableType)
{
}

TResult TQueryCollector::Collect() {
    return Collect(Reader.ReadFirst(), EMode::Context);
}

TResult TQueryCollector::Collect(const TJsonPathItem& item, EMode mode) {
    if (mode == EMode::Literal) {
        switch (item.Type) {
            case EJsonPathItemType::NullLiteral:
            case EJsonPathItemType::BooleanLiteral:
            case EJsonPathItemType::NumberLiteral:
            case EJsonPathItemType::StringLiteral:
                break;
            default:
                return TResult(TIssue("Expected a literal expression"));
        }
    }

    if (mode == EMode::Predicate) {
        switch (item.Type) {
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
                return TResult(TIssue("Predicates are not allowed in this context"));
            default:
                break;
        }
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
            return ArrayAccess(item, mode);
        case EJsonPathItemType::WildcardArrayAccess:
            return WildcardArrayAccess(item, mode);
        case EJsonPathItemType::LastArrayIndex:
            return LastArrayIndex(item, mode);
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
        case EJsonPathItemType::UnaryNot:
            return UnaryNot(item, mode);
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
        case EJsonPathItemType::StartsWithPredicate:
        case EJsonPathItemType::LikeRegexPredicate:
        case EJsonPathItemType::IsUnknownPredicate:
        case EJsonPathItemType::ExistsPredicate:
            return Predicates(item, mode);
    }
}

TResult TQueryCollector::Literal(const TJsonPathItem& item, EMode mode) {
    if (mode != EMode::Literal) {
        return TResult(TIssue("Literal expressions are not allowed in this context"));
    }

    TString value;
    value.push_back(0);
    value.push_back((char)GetEntryType(item));

    switch (item.Type) {
        case EJsonPathItemType::StringLiteral: {
            value += item.GetString();
            break;
        }

        case EJsonPathItemType::NumberLiteral: {
            double number = item.GetNumber();
            value += TStringBuf((char*)&number, sizeof(double));
            break;
        }

        case EJsonPathItemType::BooleanLiteral:
        case EJsonPathItemType::NullLiteral:
            break;
        default:
            return TResult(TIssue("Expected a literal expression"));
    }

    return TResult(std::move(value));
}

TResult TQueryCollector::ContextObject(EMode mode) {
    Y_UNUSED(mode);
    return TResult("");
}

TResult TQueryCollector::MemberAccess(const TJsonPathItem& item, EMode mode) {
    auto result = Collect(Reader.ReadInput(item), mode);
    if (!result.CanCollect()) {
        return result;
    }

    auto& tokens = result.GetTokens();
    Y_ENSURE(tokens.size() <= 1, "Expected at most one result, but got " << tokens.size());

    // There is no context or filter object, so we can't collect any more
    if (tokens.empty()) {
        return {};
    }

    auto& token = tokens.front();
    AppendKey(token, item.GetString());
    return result;
}

TResult TQueryCollector::WildcardMemberAccess(const TJsonPathItem& item, EMode mode) {
    auto result = Collect(Reader.ReadInput(item), mode);
    result.Finish();
    if (!result.IsError() && result.GetTokens().size() > 1) {
        return TResult(TIssue("Expected at most one result, but got " + std::to_string(result.GetTokens().size())));
    }
    return result;
}

TResult TQueryCollector::ArrayAccess(const TJsonPathItem& item, EMode mode) {
    auto result = Collect(Reader.ReadInput(item), mode);
    if (!result.IsError() && result.GetTokens().size() > 1) {
        return TResult(TIssue("Expected at most one result, but got " + std::to_string(result.GetTokens().size())));
    }
    return result;
}

TResult TQueryCollector::WildcardArrayAccess(const TJsonPathItem& item, EMode mode) {
    auto result = Collect(Reader.ReadInput(item), mode);
    if (!result.IsError() && result.GetTokens().size() > 1) {
        return TResult(TIssue("Expected at most one result, but got " + std::to_string(result.GetTokens().size())));
    }
    return result;
}

TResult TQueryCollector::LastArrayIndex(const TJsonPathItem& item, EMode mode) {
    auto result = Collect(Reader.ReadInput(item), mode);
    if (!result.IsError() && result.GetTokens().size() > 1) {
        return TResult(TIssue("Expected at most one result, but got " + std::to_string(result.GetTokens().size())));
    }
    return result;
}

TResult TQueryCollector::UnaryArithmeticOp(const TJsonPathItem& item, EMode mode) {
    auto result = Collect(Reader.ReadInput(item), mode);
    result.Finish();
    return result;
}

TResult TQueryCollector::BinaryArithmeticOp(const TJsonPathItem& item, EMode mode) {
    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);

    auto leftResult = CollectArithmeticOperand(leftItem, mode);
    if (leftResult.IsError()) {
        return leftResult;
    }

    auto rightResult = CollectArithmeticOperand(rightItem, mode);
    if (rightResult.IsError()) {
        return rightResult;
    }

    if (leftResult.GetTokensMode() == TResult::ETokensMode::Or ||
        rightResult.GetTokensMode() == TResult::ETokensMode::Or) {
        return TResult(TIssue("Cannot mix AND and OR operators in jsonpath expression"));
    }

    auto& leftTokens = leftResult.GetTokens();
    auto& rightTokens = rightResult.GetTokens();
    leftTokens.insert(leftTokens.end(), rightTokens.begin(), rightTokens.end());
    if (leftTokens.size() > 1) {
        leftResult.SetTokensMode(TResult::ETokensMode::And);
    }
    leftResult.Finish();
    return leftResult;
}

TResult TQueryCollector::BinaryAnd(const TJsonPathItem& item, EMode mode) {
    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);

    auto leftResult = Collect(leftItem, mode);
    if (leftResult.IsError()) {
        return leftResult;
    }

    auto rightResult = Collect(rightItem, mode);
    if (rightResult.IsError()) {
        return rightResult;
    }

    if (leftResult.GetTokensMode() == TResult::ETokensMode::Or ||
        rightResult.GetTokensMode() == TResult::ETokensMode::Or) {
        return TResult(TIssue("Cannot mix AND and OR operators in jsonpath expression"));
    }

    auto& leftTokens = leftResult.GetTokens();
    auto& rightTokens = rightResult.GetTokens();
    leftTokens.insert(leftTokens.end(), rightTokens.begin(), rightTokens.end());
    if (leftTokens.size() > 1) {
        leftResult.SetTokensMode(TResult::ETokensMode::And);
    }
    return leftResult;
}

TResult TQueryCollector::BinaryOr(const TJsonPathItem& item, EMode mode) {
    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);

    auto leftResult = Collect(leftItem, mode);
    if (leftResult.IsError()) {
        return leftResult;
    }

    auto rightResult = Collect(rightItem, mode);
    if (rightResult.IsError()) {
        return rightResult;
    }

    if (leftResult.GetTokensMode() == TResult::ETokensMode::And ||
        rightResult.GetTokensMode() == TResult::ETokensMode::And) {
        return TResult(TIssue("Cannot mix AND and OR operators in jsonpath expression"));
    }

    auto& leftTokens = leftResult.GetTokens();
    auto& rightTokens = rightResult.GetTokens();
    leftTokens.insert(leftTokens.end(), rightTokens.begin(), rightTokens.end());
    if (leftTokens.size() > 1) {
        leftResult.SetTokensMode(TResult::ETokensMode::Or);
    }
    return leftResult;
}

TResult TQueryCollector::UnaryNot(const TJsonPathItem& item, EMode mode) {
    if (!ArePredicatesAllowed(mode)) {
        return TResult(TIssue("Predicates are not allowed in this context"));
    }

    auto result = Collect(Reader.ReadInput(item), EMode::Predicate);
    result.Finish();
    return result;
}

TResult TQueryCollector::BinaryEqual(const TJsonPathItem& item, EMode mode) {
    if (!ArePredicatesAllowed(mode)) {
        return TResult(TIssue("Predicates are not allowed in this context"));
    }

    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);

    const bool leftIsLiteral = IsLiteralType(leftItem.Type);
    const bool rightIsLiteral = IsLiteralType(rightItem.Type);

    if (!leftIsLiteral && rightIsLiteral) {
        return CollectEqualOperands(leftItem, rightItem);
    }

    if (leftIsLiteral && !rightIsLiteral) {
        return CollectEqualOperands(rightItem, leftItem);
    }

    return TResult(TIssue("Comparison requires exactly one path and one literal operand"));
}

TResult TQueryCollector::BinaryComparisonOp(const TJsonPathItem& item, EMode mode) {
    if (!ArePredicatesAllowed(mode)) {
        return TResult(TIssue("Predicates are not allowed in this context"));
    }

    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);

    auto leftResult = CollectArithmeticOperand(leftItem, EMode::Predicate);
    if (leftResult.IsError()) {
        return leftResult;
    }

    auto rightResult = CollectArithmeticOperand(rightItem, EMode::Predicate);
    if (rightResult.IsError()) {
        return rightResult;
    }

    if (leftResult.GetTokensMode() == TResult::ETokensMode::Or ||
        rightResult.GetTokensMode() == TResult::ETokensMode::Or) {
        return TResult(TIssue("Cannot mix AND and OR operators in jsonpath expression"));
    }

    auto& leftTokens = leftResult.GetTokens();
    auto& rightTokens = rightResult.GetTokens();
    leftTokens.insert(leftTokens.end(), rightTokens.begin(), rightTokens.end());
    if (leftTokens.size() > 1) {
        leftResult.SetTokensMode(TResult::ETokensMode::And);
    }
    leftResult.Finish();
    return leftResult;
}

TResult TQueryCollector::FilterObject(const TJsonPathItem& item, EMode mode) {
    Y_UNUSED(item);
    Y_UNUSED(mode);
    if (FilterObjectPrefixes.empty()) {
        return TResult(TIssue("'@' is only allowed inside filters"));
    }
    return TResult(TString(FilterObjectPrefixes.back()));
}

TResult TQueryCollector::FilterPredicate(const TJsonPathItem& item, EMode mode) {
    auto inputResult = Collect(Reader.ReadInput(item), mode);
    if (inputResult.IsError()) {
        return inputResult;
    }

    const auto& tokens = inputResult.GetTokens();

    // If input path is already finished (e.g. $.a.*) or has multiple tokens,
    // we can't apply the filter to narrow down
    if (inputResult.IsFinished() || tokens.size() != 1) {
        inputResult.Finish();
        return inputResult;
    }

    FilterObjectPrefixes.push_back(tokens.front());
    const auto& predicateItem = Reader.ReadFilterPredicate(item);
    auto predicateResult = Collect(predicateItem, EMode::Filter);
    FilterObjectPrefixes.pop_back();

    if (predicateResult.IsError()) {
        return predicateResult;
    }

    predicateResult.Finish();
    return predicateResult;
}

TResult TQueryCollector::Methods(const TJsonPathItem& item, EMode mode) {
    auto result = Collect(Reader.ReadInput(item), mode);
    result.Finish();
    if (!result.IsError() && result.GetTokens().size() > 1) {
        return TResult(TIssue("Expected at most one result, but got " + std::to_string(result.GetTokens().size())));
    }
    return result;
}

TResult TQueryCollector::Predicates(const TJsonPathItem& item, EMode mode) {
    if (!ArePredicatesAllowed(mode)) {
        return TResult(TIssue("Predicates are not allowed in this context"));
    }

    auto result = Collect(Reader.ReadInput(item), EMode::Predicate);
    result.Finish();
    return result;
}

TResult TQueryCollector::CollectEqualOperands(const TJsonPathItem& leftItem, const TJsonPathItem& rightItem) {
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
    if (!pathResult.IsFinished() && pathTokens.size() == 1 && literalTokens.size() == 1) {
        pathTokens.front() += literalTokens.front();
    }

    pathResult.Finish();
    return pathResult;
}

TResult TQueryCollector::CollectArithmeticOperand(const TJsonPathItem& item, EMode mode) {
    if (IsLiteralType(item.Type)) {
        return TResult(TResult::TTokens{});
    }
    return Collect(item, mode);
}

TResult TQueryCollector::Variable(const TJsonPathItem& item, EMode mode) {
    Y_UNUSED(item);
    Y_UNUSED(mode);
    return TResult(TIssue("Variables are not supported at the moment"));
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

TVector<TString> BuildSearchTerms(const TString& jsonPathStr) {
    TIssues issues;
    const TJsonPathPtr path = ParseJsonPath(jsonPathStr, issues, 1);
    if (!issues.Empty()) {
        return {};
    }

    auto result = TQueryCollector(path, TQueryCollector::ECallableType::JsonExists).Collect();
    if (result.IsError()) {
        return {};
    }

    return result.GetTokens();
}

void TokenizeBinaryJson(const NBinaryJson::TContainerCursor& root, const TString& prefix, TVector<TString>& tokens);

void TokenizeBinaryJson(NBinaryJson::TEntryCursor element, const TString& prefix, TVector<TString>& tokens) {
    if (element.GetType() == NBinaryJson::EEntryType::Container) {
        TokenizeBinaryJson(element.GetContainer(), prefix, tokens);
        return;
    }
    TString token = prefix;
    token.push_back(0);
    // Value always comes last in the token and we may want range queries on value,
    // so we just store element type and data.
    token.push_back((char)element.GetType());
    switch (element.GetType()) {
    case NBinaryJson::EEntryType::String:
        token += element.GetString();
        break;
    case NBinaryJson::EEntryType::Number: {
        double number = element.GetNumber();
        token += TStringBuf((char*)&number, sizeof(double));
        break;
    }
    case NBinaryJson::EEntryType::BoolFalse:
    case NBinaryJson::EEntryType::BoolTrue:
    case NBinaryJson::EEntryType::Null:
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

}  // namespace NJsonIndex

}  // namespace NKikimr
