#pragma once

#include "public.h"
#include "token.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

class TStatelessYsonLexerImplBase;

class TStatelessLexer
{
public:
    TStatelessLexer();

    ~TStatelessLexer();

    size_t GetToken(TStringBuf data, TToken* token);

private:
    std::unique_ptr<TStatelessYsonLexerImplBase> Impl;

};

////////////////////////////////////////////////////////////////////////////////

size_t GetToken(TStringBuf data, TToken* token);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
