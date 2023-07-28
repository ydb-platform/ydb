#pragma once

#include "public.h"
#include "lexer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

class TTokenizer
{
public:
    explicit TTokenizer(TStringBuf input);

    bool ParseNext();
    const TToken& CurrentToken() const;
    ETokenType GetCurrentType() const;
    TStringBuf GetCurrentSuffix() const;
    TStringBuf CurrentInput() const;
    size_t GetPosition() const;

private:
    TStringBuf Input_;
    TToken Token_;
    TStatelessLexer Lexer_;
    size_t Parsed_ = 0;
    size_t Position_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
