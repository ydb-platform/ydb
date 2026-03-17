#include "json_index.h"

namespace NKikimr {

namespace NJsonIndex {

TResult::TResult(const TQuery& query)
    : Result(query)
{
}

TResult::TResult(TQuery&& query)
    : Result(std::move(query))
{
}

TResult::TResult(std::string&& query)
    : Result(std::move(query))
{
}

TResult::TResult(const std::string& query)
    : Result(query)
{
}

TResult::TResult(TResult::TError&& issue)
    : Result(std::move(issue))
{
}

const TResult::TQuery& TResult::GetQuery() const {
    return std::get<TQuery>(Result);
}

TResult::TQuery& TResult::GetQuery() {
    return std::get<TQuery>(Result);
}

const TResult::TError& TResult::GetError() const {
    return std::get<TResult::TError>(Result);
}

bool TResult::IsError() const {
    return std::holds_alternative<TResult::TError>(Result);
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
            return WildcardMemberAccess(item);
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
            return AbsMethod(item);
        case EJsonPathItemType::FloorMethod:
            return FloorMethod(item);
        case EJsonPathItemType::CeilingMethod:
            return CeilingMethod(item);
        case EJsonPathItemType::DoubleMethod:
            return DoubleMethod(item);
        case EJsonPathItemType::TypeMethod:
            return TypeMethod(item);
        case EJsonPathItemType::SizeMethod:
            return SizeMethod(item);
        case EJsonPathItemType::KeyValueMethod:
            return KeyValueMethod(item);
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

TResult TQueryCollector::ContextObject() {
    return TResult(std::string{});
}

TResult TQueryCollector::MemberAccess(const TJsonPathItem& item) {
    auto input = Collect(Reader.ReadInput(item));
    if (input.IsError()) {
        return input;
    }

    auto& query = input.GetQuery();
    if (!query.has_value()) {
        return input;
    }

    auto member = std::string(item.GetString());
    auto size = member.size();

    do {
        if (size < 0x80) {
            query->push_back((ui8)size);
        } else {
            query->push_back(0x80 | (ui8)(size & 0x7F));
        }
        size >>= 7;
    } while (size > 0);

    *query += std::move(member);
    return input;
}

TResult TQueryCollector::WildcardMemberAccess(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
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
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::UnaryPlus(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
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
    // TODO: Implement
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::BooleanLiteral(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::NumberLiteral(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::StringLiteral(const TJsonPathItem& item) {
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

TResult TQueryCollector::AbsMethod(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::FloorMethod(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::CeilingMethod(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::DoubleMethod(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::TypeMethod(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::SizeMethod(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::KeyValueMethod(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::StartsWithPredicate(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::IsUnknownPredicate(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::ExistsPredicate(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::LikeRegexPredicate(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

TResult TQueryCollector::Variable(const TJsonPathItem& item) {
    // TODO: Implement
    Y_UNUSED(item);
    return TResult(TIssue("Not implemented"));
}

}  // namespace NJsonIndex

}  // namespace NKikimr
