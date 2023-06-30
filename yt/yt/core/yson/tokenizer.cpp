#include "tokenizer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TTokenizer::TTokenizer(TStringBuf input)
    : Input(input)
    , Parsed(0)
    , Position(0)
{ }

bool TTokenizer::ParseNext()
{
    Input = Input.Tail(Parsed);
    Token.Reset();
    Parsed = Lexer.GetToken(Input, &Token);
    Position += Parsed;
    return !CurrentToken().IsEmpty();
}

const TToken& TTokenizer::CurrentToken() const
{
    return Token;
}

ETokenType TTokenizer::GetCurrentType() const
{
    return CurrentToken().GetType();
}

TStringBuf TTokenizer::GetCurrentSuffix() const
{
    return Input.Tail(Parsed);
}

TStringBuf TTokenizer::CurrentInput() const
{
    return Input;
}

size_t TTokenizer::GetPosition() const
{
    return Position;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
