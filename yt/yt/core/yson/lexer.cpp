#include "lexer.h"
#include "lexer_detail.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TStatelessLexer::TStatelessLexer()
    : Impl(std::make_unique<TStatelesYsonLexerImpl<false>>())
{ }

TStatelessLexer::~TStatelessLexer()
{ }

size_t TStatelessLexer::GetToken(TStringBuf data, TToken* token)
{
    return Impl->GetToken(data, token);
}

////////////////////////////////////////////////////////////////////////////////

size_t GetToken(TStringBuf data, TToken* token)
{
    NDetail::TLexer<TStringReader, false> Lexer = NDetail::TLexer<TStringReader, false>(TStringReader());
    Lexer.SetBuffer(data.begin(), data.end());
    Lexer.GetToken(token);
    return Lexer.Current() - data.begin();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
