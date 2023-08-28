#pragma once

#include "detail.h"
#include "token.h"

namespace NYT::NYson::NDetail {

/*! \internal */
////////////////////////////////////////////////////////////////////////////////
// EReadStartCase tree representation:
// Root                                =     xb
//     BinaryStringOrOtherSpecialToken =    x0b
//         BinaryString                =    00b
//         OtherSpecialToken           =    10b
//     Other                           =    x1b
//         BinaryScalar                =  xx01b
//             BinaryInt64             =  0001b
//             BinaryDouble            =  0101b
//             BinaryFalse             =  1001b
//             BinaryTrue              =  1101b
//         Other                       = xxx11b
//             Quote                   = 00011b
//             DigitOrMinus            = 00111b
//             String                  = 01011b
//             Space                   = 01111b
//             Plus                    = 10011b
//             None                    = 10111b
//             Percent                 = 11011b
DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EReadStartCase, ui8,
    ((BinaryString)                 (0))    // =    00b
    ((OtherSpecialToken)            (2))    // =    10b

    ((BinaryInt64)                  (1))    // =   001b
    ((BinaryDouble)                 (5))    // =   101b
    ((BinaryFalse)                  (9))    // =  1001b
    ((BinaryTrue)                   (13))   // =  1101b
    ((BinaryUint64)                 (17))   // = 10001b

    ((Quote)                        (3))    // = 00011b
    ((DigitOrMinus)                 (7))    // = 00111b
    ((String)                       (11))   // = 01011b
    ((Space)                        (15))   // = 01111b
    ((Plus)                         (19))   // = 10011b
    ((None)                         (23))   // = 10111b
    ((Percent)                      (27))   // = 11011b
);

template <class TBlockStream, bool EnableLinePositionInfo>
class TLexer
    : public TLexerBase<TBlockStream, EnableLinePositionInfo>
{
private:
    using TBase = TLexerBase<TBlockStream, EnableLinePositionInfo>;

    static EReadStartCase GetStartState(char ch)
    {
#define NN EReadStartCase::None
#define BS EReadStartCase::BinaryString
#define BI EReadStartCase::BinaryInt64
#define BD EReadStartCase::BinaryDouble
#define BF EReadStartCase::BinaryFalse
#define BT EReadStartCase::BinaryTrue
#define BU EReadStartCase::BinaryUint64
#define SP NN // EReadStartCase::Space
#define DM EReadStartCase::DigitOrMinus
#define ST EReadStartCase::String
#define PL EReadStartCase::Plus
#define QU EReadStartCase::Quote
#define PC EReadStartCase::Percent
#define TT(name) (EReadStartCase(static_cast<ui8>(ETokenType::name) << 2) | EReadStartCase::OtherSpecialToken)

        static const EReadStartCase lookupTable[] =
        {
            NN,BS,BI,BD,BF,BT,BU,NN,NN,SP,SP,SP,SP,SP,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,

            // 32
            SP, // ' '
            NN, // '!'
            QU, // '"'
            TT(Hash), // '#'
            NN, // '$'
            PC, // '%'
            NN, // '&'
            NN, // "'"
            TT(LeftParenthesis), // '('
            TT(RightParenthesis), // ')'
            NN, // '*'
            PL, // '+'
            TT(Comma), // ','
            DM, // '-'
            NN, // '.'
            TT(Slash), // '/'

            // 48
            DM,DM,DM,DM,DM,DM,DM,DM,DM,DM, // '0' - '9'
            TT(Colon), // ':'
            TT(Semicolon), // ';'
            TT(LeftAngle), // '<'
            TT(Equals), // '='
            TT(RightAngle), // '>'
            NN, // '?'

            // 64
            NN, // '@'
            ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST, // 'A' - 'M'
            ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST, // 'N' - 'Z'
            TT(LeftBracket), // '['
            NN, // '\'
            TT(RightBracket), // ']'
            NN, // '^'
            ST, // '_'

            // 96
            NN, // '`'

            ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST, // 'a' - 'm'
            ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST, // 'n' - 'z'
            TT(LeftBrace), // '{'
            NN, // '|'
            TT(RightBrace), // '}'
            NN, // '~'
            NN, // '^?' non-printable
            // 128
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,

            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN
        };

#undef NN
#undef BS
#undef BI
#undef BD
#undef BF
#undef BT
#undef BU
#undef SP
#undef DM
#undef ST
#undef PL
#undef QU
#undef PC
#undef TT
        return lookupTable[static_cast<ui8>(ch)];
    }

public:
    TLexer(
        const TBlockStream& blockStream,
        i64 memoryLimit = std::numeric_limits<i64>::max())
        : TBase(blockStream, memoryLimit)
    { }

    void ParseToken(TToken* token)
    {
        char ch = TBase::SkipSpaceAndGetChar();
        auto state = GetStartState(ch);
        auto stateBits = static_cast<unsigned>(state);

        if (ch == '\0') {
            *token = TToken::EndOfStream;
            return;
        }

        if (stateBits & 1) { // Other = x1b
            if (stateBits & 1 << 1) { // Other = xxx11b
                if (state == EReadStartCase::Quote) {
                    TBase::Advance(1);
                    auto value = TBase::ReadQuotedString();
                    *token = TToken(value, /*binaryString*/ false);
                } else if (state == EReadStartCase::DigitOrMinus) {
                    ReadNumeric<true>(token);
                } else if (state == EReadStartCase::Plus) {
                    TBase::Advance(1);

                    char ch = TBase::template GetChar<true>();

                    if (!isdigit(ch)) {
                        *token = TToken(ETokenType::Plus);
                    } else {
                        ReadNumeric<true>(token);
                    }
                } else if (state == EReadStartCase::String) {
                    auto value = TBase::template ReadUnquotedString<true>();
                    *token = TToken(value, /*binaryString*/ false);
                } else if (state == EReadStartCase::Percent) {
                    TBase::Advance(1);
                    char ch = TBase::template GetChar<true>();
                    if (ch == 't' || ch == 'f') {
                        *token = TToken(TBase::template ReadBoolean<true>());
                    } else {
                        *token = TToken(TBase::template ReadNanOrInf<true>());
                    }
                } else { // None
                    YT_ASSERT(state == EReadStartCase::None);
                    THROW_ERROR_EXCEPTION("Unexpected %Qv",
                        ch);
                }
            } else { // BinaryScalar = x01b
                TBase::Advance(1);
                if (state == EReadStartCase::BinaryDouble) {
                    double value = TBase::ReadBinaryDouble();
                    *token = TToken(value);
                } else if (state == EReadStartCase::BinaryInt64) {
                    i64 value = TBase::ReadBinaryInt64();
                    *token = TToken(value);
                } else if (state == EReadStartCase::BinaryUint64) {
                    ui64 value = TBase::ReadBinaryUint64();
                    *token = TToken(value);
                } else if (state == EReadStartCase::BinaryFalse) {
                    *token = TToken(false);
                } else if (state == EReadStartCase::BinaryTrue) {
                    *token = TToken(true);
                } else {
                    YT_ABORT();
                }
            }
        } else { // BinaryStringOrOtherSpecialToken = x0b
            TBase::Advance(1);
            if (stateBits & 1 << 1) { // OtherSpecialToken = 10b
                YT_ASSERT((stateBits & 3) == static_cast<unsigned>(EReadStartCase::OtherSpecialToken));
                *token = TToken(ETokenType(stateBits >> 2));
            } else { // BinaryString = 00b
                YT_ASSERT((stateBits & 3) == static_cast<unsigned>(EReadStartCase::BinaryString));
                auto value = TBase::ReadBinaryString();
                *token = TToken(value, /*binaryString*/ true);
            }
        }
    }

    template <bool AllowFinish>
    void ReadNumeric(TToken* token)
    {
        TStringBuf valueBuffer;
        auto numericResult = TBase::template ReadNumeric<AllowFinish>(&valueBuffer);

        if (numericResult == ENumericResult::Double) {
            try {
                *token = TToken(FromString<double>(valueBuffer));
            } catch (const std::exception& ex) {
                THROW_ERROR CreateLiteralError(ETokenType::Double, valueBuffer.begin(), valueBuffer.size())
                    << *this
                    << ex;
            }
        } else if (numericResult == ENumericResult::Int64) {
            try {
                *token = TToken(FromString<i64>(valueBuffer));
            } catch (const std::exception& ex) {
                THROW_ERROR CreateLiteralError(ETokenType::Int64, valueBuffer.begin(), valueBuffer.size())
                    << *this
                    << ex;
            }
        } else if (numericResult == ENumericResult::Uint64) {
            try {
                *token = TToken(FromString<ui64>(valueBuffer.SubStr(0, valueBuffer.size() - 1)));
            } catch (const std::exception& ex) {
                THROW_ERROR CreateLiteralError(ETokenType::Int64, valueBuffer.begin(), valueBuffer.size())
                    << *this
                    << ex;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */

} // namespace NYT::NYson::NDetail
