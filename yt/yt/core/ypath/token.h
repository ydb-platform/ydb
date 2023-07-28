#pragma once

#include "public.h"

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

extern const TStringBuf ListBeginToken;
extern const TStringBuf ListEndToken;
extern const TStringBuf ListBeforeToken;
extern const TStringBuf ListAfterToken;

DEFINE_ENUM(ETokenType,
    (Literal)
    (Slash)
    (Ampersand)
    (At)
    (Asterisk)
    (StartOfStream)
    (EndOfStream)
    (Range)
);

TString ToYPathLiteral(TStringBuf value);
TString ToYPathLiteral(i64 value);
TString ToYPathLiteral(TGuid value);
template <class E>
    requires TEnumTraits<E>::IsEnum
TString ToYPathLiteral(E value);

template <class T, class TTag>
TString ToYPathLiteral(const TStrongTypedef<T, TTag>& value);

void AppendYPathLiteral(TStringBuilderBase* builder, TStringBuf value);
void AppendYPathLiteral(TStringBuilderBase* builder, i64 value);

TStringBuf ExtractListIndex(TStringBuf token);
int ParseListIndex(TStringBuf token);
std::optional<int> TryAdjustListIndex(int index, int count);

bool IsSpecialListKey(TStringBuf key);
bool IsSpecialCharacter(char ch);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath

#define TOKEN_INL_H_
#include "token-inl.h"
#undef TOKEN_INL_H_
