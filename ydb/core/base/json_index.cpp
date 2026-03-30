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

}  // namespace

TResult::TResult(const TQueries& queries)
    : Result(queries)
{
}

TResult::TResult(TQueries&& queries)
    : Result(std::move(queries))
{
}

TResult::TResult(TString&& query)
    : Result(TQueries{std::move(query)})
{
}

TResult::TResult(const TString& query)
    : Result(TQueries{query})
{
}

TResult::TResult(TResult::TError&& issue)
    : Result(std::move(issue))
{
}

const TResult::TQueries& TResult::GetQueries() const {
    return std::get<TQueries>(Result);
}

TResult::TQueries& TResult::GetQueries() {
    return std::get<TQueries>(Result);
}

const TResult::TError& TResult::GetError() const {
    return std::get<TResult::TError>(Result);
}

bool TResult::IsError() const {
    return std::holds_alternative<TResult::TError>(Result);
}

bool TResult::IsDone() const {
    return Done;
}

void TResult::MarkDone() {
    Done = true;
}

TQueryCollector::TQueryCollector(const TJsonPathPtr path)
    : Reader(path)
{
}

TResult TQueryCollector::Collect() {
    return Collect(Reader.ReadFirst());
}

TResult TQueryCollector::Collect(const TJsonPathItem& item) {
    switch (item.Type) {
        case EJsonPathItemType::MemberAccess:
            return MemberAccess(item);
        case EJsonPathItemType::WildcardMemberAccess:
            return Finalize(item);
        case EJsonPathItemType::ContextObject:
            return ContextObject();
        case EJsonPathItemType::Variable:
            return Variable(item);
        case EJsonPathItemType::NumberLiteral:
            return NumberLiteral(item);
        case EJsonPathItemType::ArrayAccess:
            return ArrayAccess(item);
        case EJsonPathItemType::WildcardArrayAccess:
            return WildcardArrayAccess(item);
        case EJsonPathItemType::LastArrayIndex:
            return LastArrayIndex(item);
        case EJsonPathItemType::UnaryMinus:
            return UnaryMinus(item);
        case EJsonPathItemType::UnaryPlus:
            return UnaryPlus(item);
        case EJsonPathItemType::BinaryAdd:
            return BinaryAdd(item);
        case EJsonPathItemType::BinarySubstract:
            return BinarySubstract(item);
        case EJsonPathItemType::BinaryMultiply:
            return BinaryMultiply(item);
        case EJsonPathItemType::BinaryDivide:
            return BinaryDivide(item);
        case EJsonPathItemType::BinaryModulo:
            return BinaryModulo(item);
        case EJsonPathItemType::BinaryAnd:
            return BinaryAnd(item);
        case EJsonPathItemType::BinaryOr:
            return BinaryOr(item);
        case EJsonPathItemType::UnaryNot:
            return UnaryNot(item);
        case EJsonPathItemType::BooleanLiteral:
            return BooleanLiteral(item);
        case EJsonPathItemType::NullLiteral:
            return NullLiteral();
        case EJsonPathItemType::StringLiteral:
            return StringLiteral(item);
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
            return BinaryEqual(item);
        case EJsonPathItemType::BinaryNotEqual:
            return BinaryNotEqual(item);
        case EJsonPathItemType::AbsMethod:
        case EJsonPathItemType::FloorMethod:
        case EJsonPathItemType::CeilingMethod:
        case EJsonPathItemType::DoubleMethod:
        case EJsonPathItemType::TypeMethod:
        case EJsonPathItemType::SizeMethod:
        case EJsonPathItemType::KeyValueMethod:
            return Finalize(item);
        case EJsonPathItemType::StartsWithPredicate:
            return StartsWithPredicate(item);
        case EJsonPathItemType::IsUnknownPredicate:
            return IsUnknownPredicate(item);
        case EJsonPathItemType::ExistsPredicate:
            return ExistsPredicate(item);
        case EJsonPathItemType::LikeRegexPredicate:
            return LikeRegexPredicate(item);
    }
}

TResult TQueryCollector::EvaluateLiteral(const TJsonPathItem& item) {
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

    return TResult(value);
}

TResult TQueryCollector::Finalize(const TJsonPathItem& item) {
    auto input = Collect(Reader.ReadInput(item));
    input.MarkDone();
    return input;
}

TResult TQueryCollector::FinalizeEmpty(const TJsonPathItem& item) {
    auto input = Collect(Reader.ReadInput(item));
    if (input.IsError() || input.GetQueries().empty()) {
        return input;
    }

    auto result = TResult("");
    result.MarkDone();
    return result;
}

TResult TQueryCollector::ContextObject() {
    return TResult(TResult::TQueries{TString{}});
}

TResult TQueryCollector::MemberAccess(const TJsonPathItem& item) {
    auto input = Collect(Reader.ReadInput(item));
    if (input.IsError() || input.IsDone() || input.GetQueries().empty()) {
        return input;
    }

    // TODO: Handle multiple queries
    auto& query = input.GetQueries()[0];
    AppendKey(query, item.GetString());
    return input;
}

TResult TQueryCollector::ArrayAccess(const TJsonPathItem& item) {
    return Collect(Reader.ReadInput(item));
}

TResult TQueryCollector::WildcardArrayAccess(const TJsonPathItem& item) {
    return Collect(Reader.ReadInput(item));
}

TResult TQueryCollector::LastArrayIndex(const TJsonPathItem& item) {
    return Collect(Reader.ReadInput(item));
}

TResult TQueryCollector::UnaryMinus(const TJsonPathItem& item) {
    return Finalize(item);
}

TResult TQueryCollector::UnaryPlus(const TJsonPathItem& item) {
    return Finalize(item);
}

TResult TQueryCollector::BinaryAdd(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::BinarySubstract(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::BinaryMultiply(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::BinaryDivide(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::BinaryModulo(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
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

TResult TQueryCollector::BinaryEqual(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::BinaryNotEqual(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::NullLiteral() {
    return TResult(TResult::TQueries{});
}

TResult TQueryCollector::BooleanLiteral(const TJsonPathItem&) {
    return TResult(TResult::TQueries{});
}

TResult TQueryCollector::NumberLiteral(const TJsonPathItem&) {
    return TResult(TResult::TQueries{});
}

TResult TQueryCollector::StringLiteral(const TJsonPathItem&) {
    return TResult(TResult::TQueries{});
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

// TODO: Add support for filters and JSON_VALUE
// Do not emit specific posting keys for the predicates: JsonExists/SqlExists treats any non-empty jsonpath
// result as true, including a single boolean false from the predicate, so index + residual
// JSON_EXISTS would return wrong rows. We just emit an empty query to indicate that the predicate
// is present to read all json documents.
TResult TQueryCollector::StartsWithPredicate(const TJsonPathItem& item) {
    return FinalizeEmpty(item);
}

TResult TQueryCollector::IsUnknownPredicate(const TJsonPathItem& item) {
    return FinalizeEmpty(item);
}

TResult TQueryCollector::ExistsPredicate(const TJsonPathItem& item) {
    return FinalizeEmpty(item);
}

TResult TQueryCollector::LikeRegexPredicate(const TJsonPathItem& item) {
    return FinalizeEmpty(item);
}

TResult TQueryCollector::Variable(const TJsonPathItem&) {
    return TResult(TIssue("Variables are not supported at the moment"));
}

TVector<TString> BuildSearchTerms(const TString& jsonPathStr) {
    TIssues issues;
    const TJsonPathPtr path = ParseJsonPath(jsonPathStr, issues, 1);
    if (!issues.Empty()) {
        return {};
    }

    auto result = TQueryCollector(path).Collect();
    if (result.IsError()) {
        return {};
    }

    return result.GetQueries();
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
