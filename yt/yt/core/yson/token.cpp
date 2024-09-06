#include "token.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

ETokenType CharToTokenType(char ch)
{
    switch (ch) {
        case ';': return ETokenType::Semicolon;
        case '=': return ETokenType::Equals;
        case '{': return ETokenType::LeftBrace;
        case '}': return ETokenType::RightBrace;
        case '#': return ETokenType::Hash;
        case '[': return ETokenType::LeftBracket;
        case ']': return ETokenType::RightBracket;
        case '<': return ETokenType::LeftAngle;
        case '>': return ETokenType::RightAngle;
        case '(': return ETokenType::LeftParenthesis;
        case ')': return ETokenType::RightParenthesis;
        case '+': return ETokenType::Plus;
        case ':': return ETokenType::Colon;
        case ',': return ETokenType::Comma;
        case '/': return ETokenType::Slash;
        default:  return ETokenType::EndOfStream;
    }
}

char TokenTypeToChar(ETokenType type)
{
    switch (type) {
        case ETokenType::Semicolon:         return ';';
        case ETokenType::Equals:            return '=';
        case ETokenType::Hash:              return '#';
        case ETokenType::LeftBracket:       return '[';
        case ETokenType::RightBracket:      return ']';
        case ETokenType::LeftBrace:         return '{';
        case ETokenType::RightBrace:        return '}';
        case ETokenType::LeftAngle:         return '<';
        case ETokenType::RightAngle:        return '>';
        case ETokenType::LeftParenthesis:   return '(';
        case ETokenType::RightParenthesis:  return ')';
        case ETokenType::Plus:              return '+';
        case ETokenType::Colon:             return ':';
        case ETokenType::Comma:             return ',';
        case ETokenType::Slash:             return '/';
        default:                            YT_ABORT();
    }
}

TString TokenTypeToString(ETokenType type)
{
    return TString(1, TokenTypeToChar(type));
}

////////////////////////////////////////////////////////////////////////////////

const TToken TToken::EndOfStream;

TToken::TToken()
    : Type_(ETokenType::EndOfStream)
{ }

TToken::TToken(ETokenType type)
    : Type_(type)
{
    switch (type) {
        case ETokenType::String:
        case ETokenType::Int64:
        case ETokenType::Uint64:
        case ETokenType::Double:
        case ETokenType::Boolean:
            YT_ABORT();
        default:
            break;
    }
}

TToken::TToken(TStringBuf stringValue, bool binaryString)
    : Type_(ETokenType::String)
    , StringValue_(stringValue)
    , BinaryString_(binaryString)
{ }

TToken::TToken(i64 int64Value)
    : Type_(ETokenType::Int64)
    , Int64Value_(int64Value)
{ }

TToken::TToken(ui64 uint64Value)
    : Type_(ETokenType::Uint64)
    , Uint64Value_(uint64Value)
{ }

TToken::TToken(double doubleValue)
    : Type_(ETokenType::Double)
    , DoubleValue_(doubleValue)
{ }

TToken::TToken(bool booleanValue)
    : Type_(ETokenType::Boolean)
    , BooleanValue_(booleanValue)
{ }

bool TToken::IsEmpty() const
{
    return Type_ == ETokenType::EndOfStream;
}

TStringBuf TToken::GetStringValue() const
{
    ExpectType(ETokenType::String);
    return StringValue_;
}

bool TToken::IsBinaryString() const
{
    ExpectType(ETokenType::String);
    return BinaryString_;
}

i64 TToken::GetInt64Value() const
{
    ExpectType(ETokenType::Int64);
    return Int64Value_;
}

ui64 TToken::GetUint64Value() const
{
    ExpectType(ETokenType::Uint64);
    return Uint64Value_;
}

double TToken::GetDoubleValue() const
{
    ExpectType(ETokenType::Double);
    return DoubleValue_;
}

bool TToken::GetBooleanValue() const
{
    ExpectType(ETokenType::Boolean);
    return BooleanValue_;
}

void TToken::ExpectTypes(const std::vector<ETokenType>& expectedTypes) const
{
    if (expectedTypes.size() == 1) {
        ExpectType(expectedTypes.front());
    } else if (std::find(expectedTypes.begin(), expectedTypes.end(), Type_) == expectedTypes.end()) {
        auto typesString = JoinToString(
            expectedTypes,
            [] (TStringBuilderBase* builder, ETokenType type) {
                builder->AppendFormat("%Qlv", type);
            },
            TStringBuf(" or "));
        if (Type_ == ETokenType::EndOfStream) {
            THROW_ERROR_EXCEPTION("Unexpected end of stream; expected types are %v",
                typesString);
        } else {
            THROW_ERROR_EXCEPTION("Unexpected token %Qv of type %Qlv; expected types are %v",
                *this,
                Type_,
                typesString);
        }
    }
}

void TToken::ExpectType(ETokenType expectedType) const
{
    if (Type_ != expectedType) {
        if (Type_ == ETokenType::EndOfStream) {
            THROW_ERROR_EXCEPTION("Unexpected end of stream; expected type is %Qlv",
                expectedType);
        } else {
            THROW_ERROR_EXCEPTION("Unexpected token %Qv of type %Qlv; expected type is %Qlv",
                *this,
                Type_,
                expectedType);
        }
    }
}


void TToken::ThrowUnexpected() const
{
    if (Type_ == ETokenType::EndOfStream) {
        THROW_ERROR_EXCEPTION("Unexpected end of stream");
    } else {
        THROW_ERROR_EXCEPTION("Unexpected token %Qv of type %Qlv",
            *this,
            Type_);
    }
}

void TToken::Reset()
{
    Type_ = ETokenType::EndOfStream;
    Int64Value_ = 0;
    Uint64Value_ = 0;
    DoubleValue_ = 0.0;
    StringValue_ = TStringBuf();
    BooleanValue_ = false;
}

// TODO(arkady-e1ppa): Consider doing ToString anyway (e.g. with spec = 'v')
// and then applying spec to the string value.
void FormatValue(TStringBuilderBase* builder, const TToken& token, TStringBuf spec)
{
    switch (token.GetType()) {
        case ETokenType::EndOfStream:
            FormatValue(builder, TStringBuf{}, spec);
            break;

        case ETokenType::String:
            FormatValue(builder, token.GetStringValue(), spec);
            break;

        case ETokenType::Int64:
            FormatValue(builder, token.GetInt64Value(), spec);
            break;

        case ETokenType::Uint64:
            FormatValue(builder, token.GetUint64Value(), spec);
            break;

        case ETokenType::Double:
            FormatValue(builder, token.GetDoubleValue(), spec);
            break;

        case ETokenType::Boolean:
            FormatValue(builder, FormatBool(token.GetBooleanValue()), spec);
            break;

        default:
            FormatValue(builder, TokenTypeToString(token.GetType()), spec);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
