#pragma once

#include "ast.h"
#include "parser.h"

namespace NYT::NQueryClient::NAst {

////////////////////////////////////////////////////////////////////////////////

class TBaseLexer
{
public:
    TBaseLexer(
        const TString& source,
        TParser::token_type strayToken);

    TParser::token_type GetNextToken(
        TParser::semantic_type* yyval,
        TParser::location_type* yyloc);

private:
    void Initialize(const char* begin, const char* end);

private:
    TParser::token_type StrayToken_;
    bool InjectedStrayToken_;

    // Ragel state variables.
    // See Ragel User Manual for host interface specification.
    const char* p;
    const char* pe;
    const char* ts;
    const char* te;
    const char* eof;
    int cs;
    int act;

    // Saves embedded chunk boundaries and embedding depth.
    const char* rs;
    const char* re;
    int rd;

    // Saves beginning-of-string boundary to compute locations.
    const char* s;
};

////////////////////////////////////////////////////////////////////////////////

class TLexer
{
public:
    TLexer(
        const TString& source,
        TParser::token_type strayToken,
        THashMap<TString, TString> placeholderValues);

    TParser::token_type GetNextToken(
        TParser::semantic_type* yyval,
        TParser::location_type* yyloc);

private:
    struct TPlaceholderLexerData
    {
        TBaseLexer Lexer;
        TParser::location_type Location;
    };

    TBaseLexer QueryLexer_;
    std::optional<TPlaceholderLexerData> Placeholder_;

    THashMap<TString, TString> PlaceholderValues_;

    std::optional<TParser::token_type> GetNextTokenFromPlaceholder(
        TParser::semantic_type* yyval,
        TParser::location_type* yyloc);

    void SetPlaceholder(
        TParser::semantic_type* yyval,
        TParser::location_type* yyloc);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NAst

