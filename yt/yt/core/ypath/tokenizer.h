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
    ETokenType GetPreviousType() const;
    TStringBuf GetToken() const;
    TYPathBuf GetPrefix() const;
    TYPathBuf GetPrefixPlusToken() const;
    TStringBuf GetSuffix() const;
    TStringBuf GetInput() const;
    TYPathBuf GetPath() const;
    const TString& GetLiteralValue() const;

    void Expect(ETokenType expectedType) const;
    void ExpectListIndex() const;
    bool Skip(ETokenType expectedType);
    [[noreturn]] void ThrowUnexpected() const;

    // For iterations. Restores tokenizer to current state on destruction.
    // Does not restore LiteralValue_.
    class TCheckpoint
    {
    public:
        explicit TCheckpoint(TTokenizer& tokenizer);
        ~TCheckpoint();

    private:
        TTokenizer& Tokenizer_;
        TYPathBuf Path_;
        ETokenType Type_;
        ETokenType PreviousType_;
        TStringBuf Token_;
        TStringBuf Input_;
    };

private:
    friend class TTokenizer::TCheckpoint;

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

bool HasPrefix(TYPathBuf fullPath, TYPathBuf prefixPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
