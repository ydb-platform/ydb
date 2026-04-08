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

class TQueryCollector {
    enum class EMode {
        Context = 0,
        Filter = 1,
        Predicate = 2,
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

    bool ArePredicatesAllowed(EMode mode) const;

private:
    TJsonPathReader Reader;
    ECallableType CallableType;
    TVector<TString> FilterObjectPrefixes;
};

TCollectResult TQueryCollector::Collect(const TJsonPathItem& item, EMode mode) {
    if (mode == EMode::Literal && !IsLiteralType(item.Type)) {
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
    Y_ENSURE(tokens.size() <= 1, "Expected at most one result, but got " << tokens.size());

    // There is no context or filter object, so we can't collect any more
    if (tokens.empty()) {
        return {};
    }

    auto& token = tokens.front();
    AppendKey(token, item.GetString());
    return result;
}

TCollectResult TQueryCollector::WildcardMemberAccess(const TJsonPathItem& item, EMode mode) {
    auto result = Collect(Reader.ReadInput(item), mode);
    result.Finish();
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
    auto result = Collect(Reader.ReadInput(item), mode);
    result.Finish();
    return result;
}

TCollectResult TQueryCollector::BinaryArithmeticOp(const TJsonPathItem& item, EMode mode) {
    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);

    auto lefTCollectResult = CollectArithmeticOperand(leftItem, mode);
    if (lefTCollectResult.IsError()) {
        return lefTCollectResult;
    }

    auto righTCollectResult = CollectArithmeticOperand(rightItem, mode);
    if (righTCollectResult.IsError()) {
        return righTCollectResult;
    }

    if (lefTCollectResult.GetTokensMode() == TCollectResult::ETokensMode::Or ||
        righTCollectResult.GetTokensMode() == TCollectResult::ETokensMode::Or) {
        return TCollectResult(TIssue("Cannot mix AND and OR operators in jsonpath expression"));
    }

    auto& leftTokens = lefTCollectResult.GetTokens();
    auto& rightTokens = righTCollectResult.GetTokens();
    leftTokens.insert(leftTokens.end(), rightTokens.begin(), rightTokens.end());
    if (leftTokens.size() > 1) {
        lefTCollectResult.SetTokensMode(TCollectResult::ETokensMode::And);
    }
    lefTCollectResult.Finish();
    return lefTCollectResult;
}

TCollectResult TQueryCollector::BinaryAnd(const TJsonPathItem& item, EMode mode) {
    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);

    auto lefTCollectResult = Collect(leftItem, mode);
    if (lefTCollectResult.IsError()) {
        return lefTCollectResult;
    }

    auto righTCollectResult = Collect(rightItem, mode);
    if (righTCollectResult.IsError()) {
        return righTCollectResult;
    }

    if (lefTCollectResult.GetTokensMode() == TCollectResult::ETokensMode::Or ||
        righTCollectResult.GetTokensMode() == TCollectResult::ETokensMode::Or) {
        return TCollectResult(TIssue("Cannot mix AND and OR operators in jsonpath expression"));
    }

    auto& leftTokens = lefTCollectResult.GetTokens();
    auto& rightTokens = righTCollectResult.GetTokens();
    leftTokens.insert(leftTokens.end(), rightTokens.begin(), rightTokens.end());
    if (leftTokens.size() > 1) {
        lefTCollectResult.SetTokensMode(TCollectResult::ETokensMode::And);
    }
    return lefTCollectResult;
}

TCollectResult TQueryCollector::BinaryOr(const TJsonPathItem& item, EMode mode) {
    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);

    auto lefTCollectResult = Collect(leftItem, mode);
    if (lefTCollectResult.IsError()) {
        return lefTCollectResult;
    }

    auto righTCollectResult = Collect(rightItem, mode);
    if (righTCollectResult.IsError()) {
        return righTCollectResult;
    }

    if (lefTCollectResult.GetTokensMode() == TCollectResult::ETokensMode::And ||
        righTCollectResult.GetTokensMode() == TCollectResult::ETokensMode::And) {
        return TCollectResult(TIssue("Cannot mix AND and OR operators in jsonpath expression"));
    }

    auto& leftTokens = lefTCollectResult.GetTokens();
    auto& rightTokens = righTCollectResult.GetTokens();
    leftTokens.insert(leftTokens.end(), rightTokens.begin(), rightTokens.end());
    if (leftTokens.size() > 1) {
        lefTCollectResult.SetTokensMode(TCollectResult::ETokensMode::Or);
    }
    return lefTCollectResult;
}

TCollectResult TQueryCollector::BinaryEqual(const TJsonPathItem& item, EMode mode) {
    if (!ArePredicatesAllowed(mode)) {
        return TCollectResult(TIssue("Predicates are not allowed in this context"));
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

    return TCollectResult(TIssue("Comparison requires exactly one path and one literal operand"));
}

TCollectResult TQueryCollector::BinaryComparisonOp(const TJsonPathItem& item, EMode mode) {
    if (!ArePredicatesAllowed(mode)) {
        return TCollectResult(TIssue("Predicates are not allowed in this context"));
    }

    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);

    auto lefTCollectResult = CollectArithmeticOperand(leftItem, EMode::Predicate);
    if (lefTCollectResult.IsError()) {
        return lefTCollectResult;
    }

    auto righTCollectResult = CollectArithmeticOperand(rightItem, EMode::Predicate);
    if (righTCollectResult.IsError()) {
        return righTCollectResult;
    }

    if (lefTCollectResult.GetTokensMode() == TCollectResult::ETokensMode::Or ||
        righTCollectResult.GetTokensMode() == TCollectResult::ETokensMode::Or) {
        return TCollectResult(TIssue("Cannot mix AND and OR operators in jsonpath expression"));
    }

    auto& leftTokens = lefTCollectResult.GetTokens();
    auto& rightTokens = righTCollectResult.GetTokens();
    leftTokens.insert(leftTokens.end(), rightTokens.begin(), rightTokens.end());
    if (leftTokens.size() > 1) {
        lefTCollectResult.SetTokensMode(TCollectResult::ETokensMode::And);
    }
    lefTCollectResult.Finish();
    return lefTCollectResult;
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
    auto inpuTCollectResult = Collect(Reader.ReadInput(item), mode);
    if (inpuTCollectResult.IsError()) {
        return inpuTCollectResult;
    }

    const auto& tokens = inpuTCollectResult.GetTokens();

    // If input path is already finished (e.g. $.a.*) or has multiple tokens,
    // we can't apply the filter to narrow down
    if (inpuTCollectResult.IsFinished() || tokens.size() != 1) {
        inpuTCollectResult.Finish();
        return inpuTCollectResult;
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

TCollectResult TQueryCollector::Methods(const TJsonPathItem& item, EMode mode) {
    auto result = Collect(Reader.ReadInput(item), mode);
    result.Finish();
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
    result.Finish();
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
    if (!pathResult.IsFinished() && pathTokens.size() == 1 && literalTokens.size() == 1) {
        pathTokens.front() += literalTokens.front();
    }

    pathResult.Finish();
    return pathResult;
}

TCollectResult TQueryCollector::CollectArithmeticOperand(const TJsonPathItem& item, EMode mode) {
    if (IsLiteralType(item.Type)) {
        return TCollectResult(TCollectResult::TTokens{});
    }
    return Collect(item, mode);
}

TCollectResult TQueryCollector::Variable(const TJsonPathItem& item, EMode mode) {
    Y_UNUSED(item);
    Y_UNUSED(mode);
    return TCollectResult(TIssue("Variables are not supported at the moment"));
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

}  // namespace

TCollectResult::TCollectResult()
    : Result(TTokens{})
{
}

TCollectResult::TCollectResult(TTokens&& tokens)
    : Result(std::move(tokens))
{
}

TCollectResult::TCollectResult(TString&& token)
    : Result(TTokens{std::move(token)})
{
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

bool TCollectResult::IsFinished() const {
    return Finished;
}

bool TCollectResult::CanCollect() const {
    return !IsError() && !IsFinished();
}

void TCollectResult::Finish() {
    Finished = true;
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
    return TQueryCollector(path, callableType).Collect();
}

TVector<TString> BuildSearchTerms(const TString& jsonPathStr) {
    TIssues issues;
    const TJsonPathPtr path = ParseJsonPath(jsonPathStr, issues, 1);
    if (!issues.Empty()) {
        return {};
    }

    auto result = CollectJsonPath(path, ECallableType::JsonExists);
    if (result.IsError()) {
        return {};
    }

    return result.GetTokens();
}

}  // namespace NJsonIndex

}  // namespace NKikimr
