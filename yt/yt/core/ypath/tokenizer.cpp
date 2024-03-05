#include "tokenizer.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/token.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

TTokenizer::TTokenizer(TYPathBuf path)
{
    Reset(path);
}

void TTokenizer::Reset(TYPathBuf path)
{
    Path_ = path;
    Type_ = ETokenType::StartOfStream;
    PreviousType_ = ETokenType::StartOfStream;
    Token_ = TStringBuf();
    Input_ = Path_;

    LiteralValue_.clear();
    LiteralValue_.reserve(Path_.length());
}

ETokenType TTokenizer::Advance()
{
    // Replace Input_ with suffix.
    Input_ = TStringBuf(Input_.begin() + Token_.length(), Input_.end());
    LiteralValue_.clear();

    // Check for EndOfStream.
    const char* current = Input_.begin();
    if (current == Input_.end()) {
        SetType(ETokenType::EndOfStream);
        Token_ = TStringBuf();
        return Type_;
    }

    SetType(ETokenType::Literal);
    bool proceed = true;
    while (proceed && current != Input_.end()) {
        auto token = NYson::CharToTokenType(*current);
        if (token == NYson::ETokenType::LeftBracket ||
            token == NYson::ETokenType::LeftBrace)
        {
            if (current == Input_.begin()) {
                SetType(ETokenType::Range);
                current = Input_.end();
            }
            proceed = false;
            continue;
        }

        switch (*current) {
            case '/':
            case '@':
            case '&':
            case '*':
                if (current == Input_.begin()) {
                    Token_ = TStringBuf(current, current + 1);
                    switch (*current) {
                        case '/': SetType(ETokenType::Slash);     break;
                        case '@': SetType(ETokenType::At);        break;
                        case '&': SetType(ETokenType::Ampersand); break;
                        case '*': SetType(ETokenType::Asterisk);  break;
                        default:  YT_ABORT();
                    }
                    return Type_;
                }
                proceed = false;
                break;

            case '\\':
                current = AdvanceEscaped(current);
                break;

            default:
                LiteralValue_.append(*current);
                ++current;
                break;
        }
    }

    Token_ = TStringBuf(Input_.begin(), current);
    return Type_;
}

void TTokenizer::ThrowMalformedEscapeSequence(TStringBuf context)
{
    THROW_ERROR_EXCEPTION("Malformed escape sequence %Qv in YPath",
        context);
}

void TTokenizer::SetType(ETokenType type)
{
    PreviousType_ = Type_;
    Type_ = type;
}

const char* TTokenizer::AdvanceEscaped(const char* current)
{
    YT_ASSERT(*current == '\\');
    ++current;

    if (current == Input_.end()) {
        THROW_ERROR_EXCEPTION("Unexpected end-of-string in YPath while parsing escape sequence");
    }

    if (IsSpecialCharacter(*current)) {
        LiteralValue_.append(*current);
        ++current;
    } else {
        if (*current == 'x') {
            if (current + 2 >= Input_.end()) {
                ThrowMalformedEscapeSequence(TStringBuf(current - 1, Input_.end()));
            }
            TStringBuf context(current - 1, current + 3);
            int hi = ParseHexDigit(current[1], context);
            int lo = ParseHexDigit(current[2], context);
            LiteralValue_.append((hi << 4) + lo);
            current = context.end();
        } else {
            ThrowMalformedEscapeSequence(TStringBuf(current - 1, current + 1));
        }
    }

    return current;
}

int TTokenizer::ParseHexDigit(char ch, TStringBuf context)
{
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }

    if (ch >= 'a' && ch <= 'f') {
        return ch - 'a' + 10;
    }

    if (ch >= 'A' && ch <= 'F') {
        return ch - 'A' + 10;
    }

    ThrowMalformedEscapeSequence(context);
    YT_ABORT();
}

void TTokenizer::Expect(ETokenType expectedType) const
{
    if (expectedType != Type_) {
        if (Type_ == ETokenType::EndOfStream) {
            if (PreviousType_ == ETokenType::Slash) {
                THROW_ERROR_EXCEPTION("Expected %Qlv in YPath but found end-of-string; please note that YPath cannot "
                    "normally end with \"/\"",
                    expectedType);
            } else {
                THROW_ERROR_EXCEPTION("Expected %Qlv in YPath but found end-of-string",
                    expectedType);
            }
        } else {
            THROW_ERROR_EXCEPTION("Expected %Qlv in YPath but found %Qlv token %Qv",
                expectedType,
                Type_,
                Token_);
        }
    }
}

void TTokenizer::ExpectListIndex() const
{
    Expect(NYPath::ETokenType::Literal);
    i64 index;
    const auto& token = GetLiteralValue();
    if (!IsSpecialListKey(token) && !TryFromString(token, index)) {
        THROW_ERROR_EXCEPTION("Expected special list key or integer for repeated field index, %Qv found", token)
            << TErrorAttribute("ypath", GetPrefixPlusToken());
    }
}

bool TTokenizer::Skip(ETokenType expectedType)
{
    if (Type_ == expectedType) {
        Advance();
        return true;
    }

    return false;
}

void TTokenizer::ThrowUnexpected() const
{
    if (Type_ == ETokenType::EndOfStream) {
        if (PreviousType_ == ETokenType::Slash) {
            THROW_ERROR_EXCEPTION("Unexpected end-of-string in YPath; please note that YPath cannot "
                "normally end with \"/\"");
        } else {
            THROW_ERROR_EXCEPTION("Unexpected end-of-string in YPath");
        }
    } else {
        THROW_ERROR_EXCEPTION("Unexpected %Qlv token %Qv in YPath",
            Type_,
            Token_);
    }
}

ETokenType TTokenizer::GetType() const
{
    return Type_;
}

TStringBuf TTokenizer::GetToken() const
{
    return Token_;
}

TStringBuf TTokenizer::GetPrefix() const
{
    return TStringBuf(Path_.begin(), Input_.begin());
}

TStringBuf TTokenizer::GetPrefixPlusToken() const
{
    return TStringBuf(Path_.begin(), Input_.begin() + Token_.length());
}

TStringBuf TTokenizer::GetSuffix() const
{
    return TStringBuf(Input_.begin() + Token_.length(), Input_.end());
}

TStringBuf TTokenizer::GetInput() const
{
    return Input_;
}

TStringBuf TTokenizer::GetPath() const
{
    return Path_;
}

const TString& TTokenizer::GetLiteralValue() const
{
    return LiteralValue_;
}

////////////////////////////////////////////////////////////////////////////////

bool HasPrefix(const TYPath& fullPath, const TYPath& prefixPath)
{
    TTokenizer fullTokenizer(fullPath);
    TTokenizer prefixTokenizer(prefixPath);

    while (true) {
        if (prefixTokenizer.Advance() == ETokenType::EndOfStream) {
            return true;
        }

        if (fullTokenizer.Advance() == ETokenType::EndOfStream) {
            return false;
        }

        if (prefixTokenizer.GetToken() != fullTokenizer.GetToken()) {
            return false;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
