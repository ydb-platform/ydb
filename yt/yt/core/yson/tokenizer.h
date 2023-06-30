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
    TStringBuf Input;
    TToken Token;
    TStatelessLexer Lexer;
    size_t Parsed;
    size_t Position;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
