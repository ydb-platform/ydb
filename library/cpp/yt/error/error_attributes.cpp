#include "error_attributes.h"

#include "error.h"
#include "error_code.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

std::vector<TErrorAttributes::TKey> TErrorAttributes::ListKeys() const
{
    std::vector<TKey> keys;
    keys.reserve(Map_.size());
    for (const auto& [key, value] : Map_) {
        keys.push_back(key);
    }
    return keys;
}

std::vector<TErrorAttributes::TKeyValuePair> TErrorAttributes::ListPairs() const
{
    std::vector<TKeyValuePair> pairs;
    pairs.reserve(Map_.size());
    for (const auto& pair : Map_) {
        pairs.push_back(pair);
    }
    return pairs;
}

std::optional<TErrorAttribute::TValue> TErrorAttributes::FindValue(TStringBuf key) const
{
    auto it = Map_.find(key);
    return it == Map_.end()
        ? std::nullopt
        : std::optional(it->second);
}

void TErrorAttributes::SetValue(const TKey& key, const TValue& value)
{
    Map_[key] = value;
}

void TErrorAttributes::SetAttribute(const TErrorAttribute& attribute)
{
    SetValue(attribute.Key, attribute.Value);
}

bool TErrorAttributes::Remove(const TKey& key)
{
    return Map_.erase(key) > 0;
}

TErrorAttributes::TValue TErrorAttributes::GetValue(TStringBuf key) const
{
    auto result = FindValue(key);
    if (!result) {
        ThrowNoSuchAttributeException(key);
    }
    return *result;
}

void TErrorAttributes::Clear()
{
    Map_.clear();
}

bool TErrorAttributes::Contains(TStringBuf key) const
{
    return Map_.contains(key);
}

bool operator == (const TErrorAttributes& lhs, const TErrorAttributes& rhs)
{
    auto lhsPairs = lhs.ListPairs();
    auto rhsPairs = rhs.ListPairs();
    if (lhsPairs.size() != rhsPairs.size()) {
        return false;
    }

    std::sort(lhsPairs.begin(), lhsPairs.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });
    std::sort(rhsPairs.begin(), rhsPairs.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });

    for (auto index = 0; index < std::ssize(lhsPairs); ++index) {
        if (lhsPairs[index].first != rhsPairs[index].first) {
            return false;
        }
    }

    for (auto index = 0; index < std::ssize(lhsPairs); ++index) {
        if (lhsPairs[index].second != rhsPairs[index].second) {
            return false;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsSpecialCharacter(char ch)
{
    return ch == '\\' || ch == '/' || ch == '@' || ch == '*' || ch == '&' || ch == '[' || ch == '{';
}

// AppendYPathLiteral.
void DoFormatAttributeKey(TStringBuilderBase* builder, TStringBuf value)
{
    constexpr char asciiBegin = 32;
    constexpr char asciiEnd = 127;
    builder->Preallocate(value.length() + 16);
    for (unsigned char ch : value) {
        if (IsSpecialCharacter(ch)) {
            builder->AppendChar('\\');
            builder->AppendChar(ch);
        } else if (ch < asciiBegin || ch > asciiEnd) {
            builder->AppendString(TStringBuf("\\x"));
            builder->AppendChar(IntToHexLowercase[ch >> 4]);
            builder->AppendChar(IntToHexLowercase[ch & 0xf]);
        } else {
            builder->AppendChar(ch);
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

[[noreturn]] void TErrorAttributes::ThrowCannotParseAttributeException(TStringBuf key, const std::exception& ex)
{
    THROW_ERROR_EXCEPTION(
        "Error parsing attribute %Qv",
        key)
        << ex;
}

[[noreturn]] void TErrorAttributes::ThrowNoSuchAttributeException(TStringBuf key)
{
    auto formatAttributeKey = [] (auto key) {
        TStringBuilder builder;
        DoFormatAttributeKey(&builder, key);
        return builder.Flush();
    };

    THROW_ERROR_EXCEPTION(
        /*NYTree::EErrorCode::ResolveError*/ TErrorCode{500},
        "Attribute %Qv is not found",
        formatAttributeKey(key));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
