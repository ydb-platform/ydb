#ifndef JINJA2CPP_SRC_ERROR_HANDLING_H
#define JINJA2CPP_SRC_ERROR_HANDLING_H

#include "lexer.h"
#include <jinja2cpp/error_info.h>
#include <contrib/restricted/expected-lite/include/nonstd/expected.hpp>

#include <initializer_list>
#include <vector>

namespace jinja2
{

struct ParseError
{
    ParseError() = default;
    ParseError(ErrorCode code, Token tok)
        : errorCode(code)
        , errorToken(tok)
    {}

    ParseError(ErrorCode code, Token tok, std::initializer_list<Token> toks)
        : errorCode(code)
        , errorToken(tok)
        , relatedTokens(toks)
    {}
    ParseError(const ParseError&) = default;
    ParseError(ParseError&& other) noexcept(true)
        : errorCode(std::move(other.errorCode))
        , errorToken(std::move(other.errorToken))
        , relatedTokens(std::move(other.relatedTokens))
    {}

    ParseError& operator =(const ParseError&) = default;
    ParseError& operator =(ParseError&& error) noexcept
    {
        if (this == &error)
            return *this;

        std::swap(errorCode, error.errorCode);
        std::swap(errorToken, error.errorToken);
        std::swap(relatedTokens, error.relatedTokens);

        return *this;
    }

    ErrorCode errorCode;
    Token errorToken;
    std::vector<Token> relatedTokens;
};

inline auto MakeParseError(ErrorCode code, Token tok)
{
    return nonstd::make_unexpected(ParseError{code, tok});
}

inline auto MakeParseError(ErrorCode code, Token tok, std::initializer_list<Token> toks)
{
    return nonstd::make_unexpected(ParseError{code, tok, toks});
}

} // namespace jinja2
#endif // JINJA2CPP_SRC_ERROR_HANDLING_H
