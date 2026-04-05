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

TQueryCollector::TQueryCollector(const TJsonPathPtr path, ECallableType callableType)
    : Reader(path)
    , CallableType(callableType)
{
}

TResult TQueryCollector::Collect() {
    return Collect(Reader.ReadFirst(), ECollectMode::None);
}

TResult TQueryCollector::Collect(const TJsonPathItem& item, ECollectMode collectMode) {
    switch (item.Type) {
        case EJsonPathItemType::MemberAccess:
            return MemberAccess(item, collectMode);
        case EJsonPathItemType::WildcardMemberAccess:
            return WildcardMemberAccess(item, collectMode);
        case EJsonPathItemType::ContextObject:
            return ContextObject();
        case EJsonPathItemType::Variable:
            return Variable(item);
        case EJsonPathItemType::ArrayAccess:
            return ArrayAccess(item, collectMode);
        case EJsonPathItemType::WildcardArrayAccess:
            return WildcardArrayAccess(item, collectMode);
        case EJsonPathItemType::LastArrayIndex:
            return LastArrayIndex(item, collectMode);
        case EJsonPathItemType::UnaryMinus:
        case EJsonPathItemType::UnaryPlus:
            return UnaryArithmeticOp(item, collectMode);
        case EJsonPathItemType::BinaryAdd:
        case EJsonPathItemType::BinarySubstract:
        case EJsonPathItemType::BinaryMultiply:
        case EJsonPathItemType::BinaryDivide:
        case EJsonPathItemType::BinaryModulo:
            return BinaryArithmeticOp(item, collectMode);
        case EJsonPathItemType::BinaryAnd:
            return BinaryAnd(item);
        case EJsonPathItemType::BinaryOr:
            return BinaryOr(item);
        case EJsonPathItemType::UnaryNot:
            return UnaryNot(item);
        case EJsonPathItemType::NumberLiteral:
        case EJsonPathItemType::BooleanLiteral:
        case EJsonPathItemType::NullLiteral:
        case EJsonPathItemType::StringLiteral:
            return EvaluateLiteral(item, collectMode);
        case EJsonPathItemType::FilterObject:
            return FilterObject(item);
        case EJsonPathItemType::FilterPredicate:
            return FilterPredicate(item);
        case EJsonPathItemType::BinaryLess:
            return BinaryLess(item);
        case EJsonPathItemType::BinaryLessEqual:
            return BinaryLessEqual(item);
        case EJsonPathItemType::BinaryGreater:
            return BinaryGreater(item);
        case EJsonPathItemType::BinaryGreaterEqual:
            return BinaryGreaterEqual(item);
        case EJsonPathItemType::BinaryEqual:
            return BinaryEqual(item, collectMode);
        case EJsonPathItemType::BinaryNotEqual:
            return BinaryNotEqual(item);
        case EJsonPathItemType::AbsMethod:
        case EJsonPathItemType::FloorMethod:
        case EJsonPathItemType::CeilingMethod:
        case EJsonPathItemType::DoubleMethod:
        case EJsonPathItemType::TypeMethod:
        case EJsonPathItemType::SizeMethod:
        case EJsonPathItemType::KeyValueMethod:
            return Methods(item, collectMode);
        case EJsonPathItemType::StartsWithPredicate:
        case EJsonPathItemType::LikeRegexPredicate:
        case EJsonPathItemType::IsUnknownPredicate:
        case EJsonPathItemType::ExistsPredicate:
            return Predicates(item, collectMode);
    }
}

TResult TQueryCollector::EvaluateLiteral(const TJsonPathItem& item, ECollectMode collectMode) {
    if (collectMode == ECollectMode::None) {
        return {};
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
            return TResult(std::move(TIssue("Expected a literal expression")));
    }

    return TResult(std::move(value));
}

TResult TQueryCollector::ContextObject() {
    return TResult("");
}

TResult TQueryCollector::MemberAccess(const TJsonPathItem& item, ECollectMode collectMode) {
    auto results = Collect(Reader.ReadInput(item), collectMode);
    if (!results.CanCollect()) {
        return results;
    }

    auto& tokens = results.GetTokens();
    Y_ENSURE(tokens.size() <= 1, "Expected at most one result, but got " << tokens.size());

    // There is no context or filter object, so we can't collect any more
    if (tokens.empty()) {
        return {};
    }

    auto& token = tokens.front();
    AppendKey(token, item.GetString());
    return results;
}

TResult TQueryCollector::WildcardMemberAccess(const TJsonPathItem& item, ECollectMode collectMode) {
    auto results = Collect(Reader.ReadInput(item), collectMode);
    results.Finish();
    return results;
}

TResult TQueryCollector::ArrayAccess(const TJsonPathItem& item, ECollectMode collectMode) {
    auto results = Collect(Reader.ReadInput(item), collectMode);
    if (!results.IsError()) {
        Y_ENSURE(results.GetTokens().size() <= 1, "Expected at most one result, but got " << results.GetTokens().size());
    }
    return results;
}

TResult TQueryCollector::WildcardArrayAccess(const TJsonPathItem& item, ECollectMode collectMode) {
    auto results = Collect(Reader.ReadInput(item), collectMode);
    if (!results.IsError()) {
        Y_ENSURE(results.GetTokens().size() <= 1, "Expected at most one result, but got " << results.GetTokens().size());
    }
    return results;
}

TResult TQueryCollector::LastArrayIndex(const TJsonPathItem& item, ECollectMode collectMode) {
    auto results = Collect(Reader.ReadInput(item), collectMode);
    if (!results.IsError()) {
        Y_ENSURE(results.GetTokens().size() <= 1, "Expected at most one result, but got " << results.GetTokens().size());
    }
    return results;
}

TResult TQueryCollector::UnaryArithmeticOp(const TJsonPathItem& item, ECollectMode collectMode) {
    auto results = Collect(Reader.ReadInput(item), collectMode);
    results.Finish();
    return results;
}

TResult TQueryCollector::BinaryArithmeticOp(const TJsonPathItem& item, ECollectMode collectMode) {
    auto leftResults = Collect(Reader.ReadLeftOperand(item), collectMode);
    if (leftResults.IsError()) {
        return leftResults;
    }

    auto rightResults = Collect(Reader.ReadRightOperand(item), collectMode);
    if (rightResults.IsError()) {
        return rightResults;
    }

    auto& leftTokens = leftResults.GetTokens();
    auto& rightTokens = rightResults.GetTokens();
    leftTokens.insert(leftTokens.end(), rightTokens.begin(), rightTokens.end());
    leftResults.Finish();
    return leftResults;
}

TResult TQueryCollector::BinaryAnd(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::BinaryOr(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::UnaryNot(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::BinaryEqual(const TJsonPathItem& item, ECollectMode collectMode) {
    const auto& leftItem = Reader.ReadLeftOperand(item);
    const auto& rightItem = Reader.ReadRightOperand(item);

    const bool leftIsLiteral = IsLiteralType(leftItem.Type);
    const bool rightIsLiteral = IsLiteralType(rightItem.Type);

    if (!leftIsLiteral && rightIsLiteral) {
        return CollectEqualOperands(leftItem, rightItem, collectMode);
    }

    if (leftIsLiteral && !rightIsLiteral) {
        return CollectEqualOperands(rightItem, leftItem, collectMode);
    }

    return TResult(TIssue("Comparison requires exactly one path and one literal operand"));
}

TResult TQueryCollector::BinaryLess(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::BinaryLessEqual(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::BinaryGreater(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::BinaryGreaterEqual(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::BinaryNotEqual(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::FilterObject(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::FilterPredicate(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::Methods(const TJsonPathItem& item, ECollectMode collectMode) {
    auto results = Collect(Reader.ReadInput(item), collectMode);
    results.Finish();
    return results;
}

TResult TQueryCollector::Predicates(const TJsonPathItem& item, ECollectMode collectMode) {
    auto result = Collect(Reader.ReadInput(item), collectMode);
    if (result.IsError()) {
        return result;
    }

    // JSON_EXISTS returns true for any non-empty result, including a single
    // boolean false from the predicate with the context object
    auto jsonExistsWithContext = CallableType == ECallableType::JsonExists && collectMode == ECollectMode::None;
    // IsUnknown predicate does not guarantee that the path exists (($.k.v == 0) is unknown != "1k1v|0")
    auto isUnknownPredicate = item.Type == EJsonPathItemType::IsUnknownPredicate;

    if (jsonExistsWithContext || isUnknownPredicate) {
        result.GetTokens().clear();
        result.GetTokens().emplace_back();
    }

    result.Finish();
    return result;
}

TResult TQueryCollector::CollectEqualOperands(const TJsonPathItem& leftItem, const TJsonPathItem& rightItem, ECollectMode collectMode) {
    auto pathResult = Collect(leftItem, collectMode);
    if (pathResult.IsError()) {
        return pathResult;
    }

    auto literalResult = Collect(rightItem, ECollectMode::Context);
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
};

TResult TQueryCollector::Variable(const TJsonPathItem&) {
    return TResult(TIssue("Variables are not supported at the moment"));
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
