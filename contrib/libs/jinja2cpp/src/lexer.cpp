#include "lexer.h"

#include <iostream>

namespace jinja2
{

bool Lexer::Preprocess()
{
    bool result = true;
    while (1)
    {
        lexertk::token token = m_tokenizer();
        if (token.is_error())
        {
            result = false;
            break;
        }

        Token newToken;
        newToken.range.startOffset = token.position;
        newToken.range.endOffset = newToken.range.startOffset + token.length;

        if (token.type == lexertk::token::e_eof)
        {
            newToken.type = Token::Eof;
            m_tokens.push_back(std::move(newToken));
            break;
        }

        switch (token.type)
        {
        case lexertk::token::e_number:
            result = ProcessNumber(token, newToken);
            break;
        case lexertk::token::e_symbol:
            result = ProcessSymbolOrKeyword(token, newToken);
            break;
        case lexertk::token::e_string:
            result = ProcessString(token, newToken);
            break;
        case lexertk::token::e_lte:
            newToken.type = Token::LessEqual;
            break;
        case lexertk::token::e_ne:
            newToken.type = Token::NotEqual;
            break;
        case lexertk::token::e_gte:
            newToken.type = Token::GreaterEqual;
            break;
        case lexertk::token::e_eq:
            newToken.type = Token::Equal;
            break;
        case lexertk::token::e_mulmul:
            newToken.type = Token::MulMul;
            break;
        case lexertk::token::e_divdiv:
            newToken.type = Token::DivDiv;
            break;
        default:
            newToken.type = static_cast<Token::Type>(token.type);
            break;
        }

        if (result)
            m_tokens.push_back(std::move(newToken));
        else
            break;
    }

    return result;
}

bool Lexer::ProcessNumber(const lexertk::token&, Token& newToken)
{
    newToken.type = Token::FloatNum;
    newToken.value = m_helper->GetAsValue(newToken.range, newToken.type);
    return true;
}

bool Lexer::ProcessSymbolOrKeyword(const lexertk::token&, Token& newToken)
{
    Keyword kwType = m_helper->GetKeyword(newToken.range);
    Token::Type tokType = Token::Unknown;
    
    switch (kwType)
    {
    case Keyword::None:
        tokType = Token::None;
        break;
    case Keyword::True:
        tokType = Token::True;
        break;
    case Keyword::False:
        tokType = Token::False;
        break;
    default:
        tokType = Token::Unknown;
        break;
    }
    
    if (tokType == Token::Unknown)
    {
        newToken.type = Token::Identifier;
        auto id = m_helper->GetAsString(newToken.range);
        newToken.value = InternalValue(id);
    }
    else
    {
        newToken.type = tokType;
    }
    return true;
}

bool Lexer::ProcessString(const lexertk::token&, Token& newToken)
{
    newToken.type = Token::String;
    newToken.value = m_helper->GetAsValue(newToken.range, newToken.type);
    return true;
}

} // namespace jinja2
