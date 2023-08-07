#include "lexer.h"
#include "lexer_detail.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

size_t TStatelessLexer::ParseToken(TStringBuf data, TToken* token)
{
    Lexer_.SetBuffer(data.begin(), data.end());
    Lexer_.ParseToken(token);
    return Lexer_.Current() - data.begin();
}

////////////////////////////////////////////////////////////////////////////////

size_t ParseToken(TStringBuf data, TToken* token)
{
    TStatelessLexer lexer;
    return lexer.ParseToken(data, token);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
