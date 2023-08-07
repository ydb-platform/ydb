#include "token.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/string/guid.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

const TStringBuf ListBeginToken("begin");
const TStringBuf ListEndToken("end");
const TStringBuf ListBeforeToken("before:");
const TStringBuf ListAfterToken("after:");

bool IsSpecialListKey(TStringBuf key)
{
    return
        key == ListBeginToken ||
        key == ListEndToken ||
        key.StartsWith(ListBeforeToken) ||
        key.StartsWith(ListAfterToken);
}

TStringBuf ExtractListIndex(TStringBuf token)
{
    if (token[0] >= '0' && token[0] <= '9') {
        return token;
    } else {
        auto colonIndex = token.find(':');
        if (colonIndex == TStringBuf::npos) {
            return token;
        } else {
            return TStringBuf(token.begin() + colonIndex + 1, token.end());
        }
    }
}

int ParseListIndex(TStringBuf token)
{
    try {
        return FromString<int>(token);
    } catch (const std::exception&) {
        THROW_ERROR_EXCEPTION("Invalid list index %Qv",
            token);
    }
}

std::optional<int> TryAdjustListIndex(int index, int count)
{
    int adjustedIndex = index >= 0 ? index : index + count;
    if (adjustedIndex < 0 || adjustedIndex >= count) {
        return std::nullopt;
    }
    return adjustedIndex;
}

TString ToYPathLiteral(TStringBuf value)
{
    TStringBuilder builder;
    AppendYPathLiteral(&builder, value);
    return builder.Flush();
}

TString ToYPathLiteral(i64 value)
{
    return ToString(value);
}

TString ToYPathLiteral(TGuid value)
{
    return ToString(value);
}

void AppendYPathLiteral(TStringBuilderBase* builder, TStringBuf value)
{
    builder->Preallocate(value.length() + 16);
    for (unsigned char ch : value) {
        if (IsSpecialCharacter(ch)) {
            builder->AppendChar('\\');
            builder->AppendChar(ch);
        } else if (ch < 32 || ch > 127) {
            builder->AppendString(TStringBuf("\\x"));
            builder->AppendChar(IntToHexLowercase[ch >> 4]);
            builder->AppendChar(IntToHexLowercase[ch & 0xf]);
        } else {
            builder->AppendChar(ch);
        }
    }
}

void AppendYPathLiteral(TStringBuilderBase* builder, i64 value)
{
    builder->AppendFormat("%v", value);
}

bool IsSpecialCharacter(char ch)
{
    return ch == '\\' || ch == '/' || ch == '@' || ch == '*' || ch == '&' || ch == '[' || ch == '{';
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
