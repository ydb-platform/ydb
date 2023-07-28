#include "tokenizer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TTokenizer::TTokenizer(TStringBuf input)
    : Input_(input)
{ }

bool TTokenizer::ParseNext()
{
    Input_ = Input_.Tail(Parsed_);
    Token_.Reset();
    Parsed_ = Lexer_.ParseToken(Input_, &Token_);
    Position_ += Parsed_;
    return !CurrentToken().IsEmpty();
}

const TToken& TTokenizer::CurrentToken() const
{
    return Token_;
}

ETokenType TTokenizer::GetCurrentType() const
{
    return CurrentToken().GetType();
}

TStringBuf TTokenizer::GetCurrentSuffix() const
{
    return Input_.Tail(Parsed_);
}

TStringBuf TTokenizer::CurrentInput() const
{
    return Input_;
}

size_t TTokenizer::GetPosition() const
{
    return Position_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
