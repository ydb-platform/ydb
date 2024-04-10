#pragma once

#include "token.h"

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

class TTokenizer
{
public:
    explicit TTokenizer(TYPathBuf path = {});

    TTokenizer(const TTokenizer&) = delete;
    TTokenizer& operator=(const TTokenizer&) = delete;

    TTokenizer(TTokenizer&&) = default;
    TTokenizer& operator=(TTokenizer&&) = default;

    void Reset(TYPathBuf path);

    ETokenType Advance();

    ETokenType GetType() const;
    TStringBuf GetToken() const;
    TStringBuf GetPrefix() const;
    TStringBuf GetPrefixPlusToken() const;
    TStringBuf GetSuffix() const;
    TStringBuf GetInput() const;
    TStringBuf GetPath() const;
    const TString& GetLiteralValue() const;

    void Expect(ETokenType expectedType) const;
    void ExpectListIndex() const;
    bool Skip(ETokenType expectedType);
    [[noreturn]] void ThrowUnexpected() const;

private:
    TYPathBuf Path_;

    ETokenType Type_;
    ETokenType PreviousType_;
    TStringBuf Token_;
    TStringBuf Input_;
    TString LiteralValue_;

    void SetType(ETokenType type);
    const char* AdvanceEscaped(const char* current);
    static int ParseHexDigit(char ch, TStringBuf context);
    static void ThrowMalformedEscapeSequence(TStringBuf context);
};

////////////////////////////////////////////////////////////////////////////////

bool HasPrefix(const TYPath& fullPath, const TYPath& prefixPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
