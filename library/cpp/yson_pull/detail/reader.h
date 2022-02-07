#pragma once

#include "lexer_base.h"
#include "symbols.h"

#include <library/cpp/yson_pull/reader.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NYsonPull {
    namespace NDetail {
        /*! \internal */
        ////////////////////////////////////////////////////////////////////////////////

        enum class special_token : ui8 {
            // Special values:
            // YSON
            semicolon = 0,     // ;
            equals = 1,        // =
            hash = 2,          // #
            left_bracket = 3,  // [
            right_bracket = 4, // ]
            left_brace = 5,    // {
            right_brace = 6,   // }
            left_angle = 7,    // <
            right_angle = 8,   // >
        };

        // char_class tree representation:
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
        enum class char_class : ui8 {
            binary_string = 0, // =    00b

            special_token_mask = 2, // =    10b
            semicolon = 2 + (0 << 2),
            equals = 2 + (1 << 2),
            hash = 2 + (2 << 2),
            left_bracket = 2 + (3 << 2),
            right_bracket = 2 + (4 << 2),
            left_brace = 2 + (5 << 2),
            right_brace = 2 + (6 << 2),
            left_angle = 2 + (7 << 2),
            right_angle = 2 + (8 << 2),

            binary_scalar_mask = 1,
            binary_int64 = 1 + (0 << 2),  // =   001b
            binary_double = 1 + (1 << 2), // =   101b
            binary_false = 1 + (2 << 2),  // =  1001b
            binary_true = 1 + (3 << 2),   // =  1101b
            binary_uint64 = 1 + (4 << 2), // = 10001b

            other_mask = 3,
            quote = 3 + (0 << 2),   // = 00011b
            number = 3 + (1 << 2),  // = 00111b
            string = 3 + (2 << 2),  // = 01011b
            percent = 3 + (6 << 2), // = 11011b
            none = 3 + (5 << 2),    // = 10111b
        };

#define CHAR_SUBCLASS(x) (static_cast<ui8>(x) >> 2)

        inline char_class get_char_class(ui8 ch) {
#define NN char_class::none
#define BS char_class::binary_string
#define BI char_class::binary_int64
#define BD char_class::binary_double
#define BF char_class::binary_false
#define BT char_class::binary_true
#define BU char_class::binary_uint64
#define SP NN // char_class::space
#define NB char_class::number
#define ST char_class::string
#define QU char_class::quote
#define PC char_class::percent
#define TT(name) (static_cast<char_class>( \
    (static_cast<ui8>(special_token::name) << 2) | static_cast<ui8>(char_class::special_token_mask)))

            static constexpr char_class lookup[256] =
                {
                    NN, BS, BI, BD, BF, BT, BU, NN, NN, SP, SP, SP, SP, SP, NN, NN,
                    NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN,

                    // 32
                    SP,       // ' '
                    NN,       // '!'
                    QU,       // '"'
                    TT(hash), // '#'
                    NN,       // '$'
                    PC,       // '%'
                    NN,       // '&'
                    NN,       // "'"
                    NN,       // '('
                    NN,       // ')'
                    NN,       // '*'
                    NB,       // '+'
                    NN,       // ','
                    NB,       // '-'
                    NN,       // '.'
                    NN,       // '/'

                    // 48
                    NB, NB, NB, NB, NB, NB, NB, NB, NB, NB, // '0' - '9'
                    NN,                                     // ':'
                    TT(semicolon),                          // ';'
                    TT(left_angle),                         // '<'
                    TT(equals),                             // '='
                    TT(right_angle),                        // '>'
                    NN,                                     // '?'

                    // 64
                    NN,                                                 // '@'
                    ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, // 'A' - 'M'
                    ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, // 'N' - 'Z'
                    TT(left_bracket),                                   // '['
                    NN,                                                 // '\'
                    TT(right_bracket),                                  // ']'
                    NN,                                                 // '^'
                    ST,                                                 // '_'

                    // 96
                    NN, // '`'

                    ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, // 'a' - 'm'
                    ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, ST, // 'n' - 'z'
                    TT(left_brace),                                     // '{'
                    NN,                                                 // '|'
                    TT(right_brace),                                    // '}'
                    NN,                                                 // '~'
                    NN,                                                 // '^?' non-printable
                    // 128
                    NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN,
                    NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN,
                    NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN,
                    NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN,

                    NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN,
                    NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN,
                    NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN,
                    NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN, NN};

#undef NN
#undef BS
#undef BI
#undef BD
#undef SP
#undef NB
#undef ST
#undef QU
#undef TT
            return lookup[ch];
        }

        template <bool EnableLinePositionInfo>
        class gen_reader_impl {
            enum class state {
                delimiter = 0,    //! expecting ';' or closing-char ('>', ']', '}')
                maybe_value = 1,  //! expecting a value or closing-char
                maybe_key = 2,    //! expecting a key or closing-char
                equals = 3,       //! expecting '=' (followed by value)
                value = 4,        //! expecting a value
                value_noattr = 5, //! expecting a value w/o attrs (after attrs)

                // by design, rare states have numbers starting from first_rare_state
                first_rare_state = 6,
                before_begin = first_rare_state,   //! before started reading the stream
                before_end = first_rare_state + 1, //! Expecting end of stream
                after_end = first_rare_state + 2,  //! after end of stream
            };

            lexer_base<EnableLinePositionInfo> lexer_;
            state state_;
            TEvent event_;
            TVector<EEventType> stack_;
            EStreamType mode_;

        public:
            gen_reader_impl(
                NYsonPull::NInput::IStream& buffer,
                EStreamType mode,
                TMaybe<size_t> memoryLimit = {})
                : lexer_(buffer, memoryLimit)
                , state_{state::before_begin}
                , mode_{mode} {
            }

            const TEvent& last_event() const {
                return event_;
            }

            ATTRIBUTE(hot)
            const TEvent& next_event() {
                if (Y_LIKELY(state_ < state::first_rare_state)) {
                    // 'hot' handler for in-stream events
                    next_event_hot();
                } else {
                    // these events happen no more than once per stream
                    next_event_cold();
                }
                return event_;
            }

        private:
            ATTRIBUTE(hot)
            void next_event_hot() {
                auto ch = lexer_.get_byte();
                auto cls = get_char_class(ch);
                if (Y_UNLIKELY(cls == char_class::none)) {
                    ch = lexer_.skip_space_and_get_byte();
                    if (Y_UNLIKELY(ch == NSymbol::eof)) {
                        handle_eof();
                        return;
                    }
                    cls = get_char_class(ch);
                }

                // states maybe_value/value/value_noattr are distinguished
                // later in state_value_special
                switch (state_) {
                    case state::maybe_value:
                        state_value(ch, cls);
                        break;
                    case state::maybe_key:
                        state_maybe_key(ch, cls);
                        break;
                    case state::equals:
                        state_equals(ch);
                        break;
                    case state::value:
                        state_value(ch, cls);
                        break;
                    case state::value_noattr:
                        state_value(ch, cls);
                        break;
                    case state::delimiter:
                        state_delimiter(ch, cls);
                        break;
                    default:
                        Y_UNREACHABLE();
                }
            }

            ATTRIBUTE(noinline, cold)
            void next_event_cold() {
                switch (state_) {
                    case state::before_begin:
                        state_before_begin();
                        break;
                    case state::after_end:
                        lexer_.fail("Attempted read past stream end");
                    case state::before_end:
                        state_before_end();
                        break;
                    default:
                        Y_UNREACHABLE();
                }
            }

            //! Present a scalar value for caller
            template <typename T>
            void yield(T value) {
                event_ = TEvent{TScalar{value}};
            }

            //! Present a scalar value with non-scalar tag (i.e. key)
            template <typename T>
            void yield(EEventType type, T value) {
                event_ = TEvent{type, TScalar{value}};
            }

            //! Present a value from number variant
            void yield(const number& value) {
                switch (value.type) {
                    case number_type::int64:
                        yield(value.value.as_int64);
                        break;
                    case number_type::uint64:
                        yield(value.value.as_uint64);
                        break;
                    case number_type::float64:
                        yield(value.value.as_float64);
                        break;
                }
            }

            //! Present a value from %-literal variant
            void yield(const percent_scalar& value) {
                switch (value.type) {
                    case percent_scalar_type::boolean:
                        yield(value.value.as_boolean);
                        break;
                    case percent_scalar_type::float64:
                        yield(value.value.as_float64);
                        break;
                }
            }

            //! Present a value-less event
            void yield(EEventType type) {
                event_ = TEvent{type};
            }

            //! Push the opening of a paired event
            void push(EEventType type) {
                stack_.push_back(type);
            }

            //! Close the paired_event, verify that delimiters are well-formed
            void pop(EEventType first, EEventType last) {
                if (Y_UNLIKELY(stack_.empty() || stack_.back() != first)) {
                    pop_fail(first, last);
                    return;
                }
                stack_.pop_back();

                yield(last);
                switch (first) {
                    case EEventType::BeginList:
                        next(state::delimiter);
                        break;

                    case EEventType::BeginMap:
                        next(state::delimiter);
                        break;

                    case EEventType::BeginAttributes:
                        next(state::value_noattr);
                        break;

                    case EEventType::BeginStream:
                        next(state::after_end);
                        break;

                    default:
                        Y_UNREACHABLE();
                }

                if (Y_UNLIKELY(mode_ == EStreamType::Node && stack_.size() == 1 && state_ == state::delimiter)) {
                    next(state::before_end);
                }
            }

            ATTRIBUTE(noinline, cold)
            void pop_fail(EEventType first, EEventType last) {
                if (stack_.empty()) {
                    lexer_.fail("Unpaired events: expected opening '", first, "' for '", last, "', but event stack is empty");
                } else {
                    lexer_.fail("Unpaired events: expected opening '", first, "' for '", last, "', but '", stack_.back(), "' is found.");
                }
            }

            //! Transition to new_state
            void next(state new_state) {
                state_ = new_state;
            }

            bool in_map() {
                return (stack_.back() == EEventType::BeginMap) || (stack_.back() == EEventType::BeginAttributes) || (stack_.back() == EEventType::BeginStream && mode_ == EStreamType::MapFragment);
            }

            ATTRIBUTE(noinline, cold)
            void handle_eof() {
                switch (state_) {
                    case state::maybe_value:
                    case state::maybe_key:
                    case state::delimiter:
                    case state::before_end:
                        pop(EEventType::BeginStream, EEventType::EndStream);
                        return;

                    default:
                        lexer_.fail("Unexpected end of stream");
                }
            }

            ATTRIBUTE(noinline, cold)
            void state_before_begin() {
                push(EEventType::BeginStream);
                yield(EEventType::BeginStream);
                switch (mode_) {
                    case EStreamType::Node:
                        next(state::value);
                        break;
                    case EStreamType::ListFragment:
                        next(state::maybe_value);
                        break;
                    case EStreamType::MapFragment:
                        next(state::maybe_key);
                        break;
                    default:
                        Y_UNREACHABLE();
                }
            }

            ATTRIBUTE(noinline, cold)
            void state_before_end() {
                auto ch = lexer_.skip_space_and_get_byte();
                if (ch == NSymbol::eof) {
                    handle_eof();
                } else {
                    lexer_.fail("Expected stream end, but found ", NCEscape::quote(ch));
                }
            }

            ATTRIBUTE(hot)
            void state_delimiter(ui8 ch, char_class cls) {
                if (Y_LIKELY(ch == NSymbol::item_separator)) {
                    lexer_.advance(1);
                    next(in_map() ? state::maybe_key : state::maybe_value);
                    // immediately read next value
                    next_event_hot();
                    return;
                }
                state_delimiter_fallback(ch, cls);
            }

            ATTRIBUTE(noinline, hot)
            void state_delimiter_fallback(ui8 ch, char_class cls) {
                auto cls_bits = static_cast<ui8>(cls);
                if ((cls_bits & 3) == static_cast<ui8>(char_class::special_token_mask)) {
                    auto token = static_cast<special_token>(cls_bits >> 2);
                    lexer_.advance(1);
                    switch (token) {
                            /* // handled in the fast track
                case special_token::semicolon:
                    next(in_map()? state::maybe_key : state::maybe_value);
                    // immediately read next value
                    return next_event();
                */

                        case special_token::right_bracket:
                            pop(EEventType::BeginList, EEventType::EndList);
                            return;

                        case special_token::right_brace:
                            pop(EEventType::BeginMap, EEventType::EndMap);
                            return;

                        case special_token::right_angle:
                            pop(EEventType::BeginAttributes, EEventType::EndAttributes);
                            return;

                        default:
                            break;
                    }
                }

                COLD_BLOCK_BYVALUE
                lexer_.fail(
                    "Unexpected ", NCEscape::quote(ch), ", expected one of ",
                    NCEscape::quote(NSymbol::item_separator), ", ",
                    NCEscape::quote(NSymbol::end_list), ", ",
                    NCEscape::quote(NSymbol::end_map), ", ",
                    NCEscape::quote(NSymbol::end_attributes));
                COLD_BLOCK_END
            }

            ATTRIBUTE(noinline, hot)
            void state_maybe_key(ui8 ch, char_class cls) {
                auto key = TStringBuf{};
                // Keys are always strings, put binary-string key into fast lane
                if (Y_LIKELY(ch == NSymbol::string_marker)) {
                    lexer_.advance(1);
                    key = lexer_.read_binary_string();
                } else {
                    switch (cls) {
                        case char_class::quote:
                            lexer_.advance(1);
                            key = lexer_.read_quoted_string();
                            break;

                        case char_class::string:
                            key = lexer_.read_unquoted_string();
                            break;

                        case char_class::right_brace:
                            lexer_.advance(1);
                            pop(EEventType::BeginMap, EEventType::EndMap);
                            return;

                        case char_class::right_angle:
                            lexer_.advance(1);
                            pop(EEventType::BeginAttributes, EEventType::EndAttributes);
                            return;

                        default:
                            COLD_BLOCK_BYVALUE
                            lexer_.fail("Unexpected ", NCEscape::quote(ch), ", expected key string");
                            COLD_BLOCK_END
                    }
                }

                yield(EEventType::Key, key);
                next(state::equals);
            }

            ATTRIBUTE(hot)
            void state_equals(ui8 ch) {
                // skip '='
                if (Y_UNLIKELY(ch != NSymbol::key_value_separator)) {
                    COLD_BLOCK_BYVALUE
                    lexer_.fail("Unexpected ", NCEscape::quote(ch), ", expected ", NCEscape::quote(NSymbol::key_value_separator));
                    COLD_BLOCK_END
                }
                lexer_.advance(1);
                next(state::value);
                // immediately read the following value
                // (this symbol yields no result)
                next_event_hot();
            }

            ATTRIBUTE(noinline, hot)
            void state_value(ui8 ch, char_class cls) {
                auto cls_bits = static_cast<ui8>(cls);
                if (cls_bits & 1) {            // Other = x1b
                    if (cls_bits & (1 << 1)) { // Other = xxx11b
                        state_value_text_scalar(cls);
                    } else { // BinaryScalar = x01b
                        state_value_binary_scalar(cls);
                    }
                    next(state::delimiter);
                } else { // BinaryStringOrOtherSpecialToken = x0b
                    lexer_.advance(1);
                    if (cls_bits & 1 << 1) {
                        // special token
                        auto token = static_cast<special_token>(cls_bits >> 2);
                        state_value_special(token, ch);
                    } else {
                        // binary string
                        yield(lexer_.read_binary_string());
                        next(state::delimiter);
                    }
                }
            }

            ATTRIBUTE(noinline)
            void state_value_special(special_token token, ui8 ch) {
                // Value starters are always accepted values
                switch (token) {
                    case special_token::hash:
                        yield(TScalar{});
                        next(state::delimiter);
                        return;

                    case special_token::left_bracket:
                        push(EEventType::BeginList);
                        yield(EEventType::BeginList);
                        next(state::maybe_value);
                        return;

                    case special_token::left_brace:
                        push(EEventType::BeginMap);
                        yield(EEventType::BeginMap);
                        next(state::maybe_key);
                        return;

                    default:
                        break;
                }

                // ...closing-chars are only allowed in maybe_value state
                if (state_ == state::maybe_value) {
                    switch (token) {
                        case special_token::right_bracket:
                            pop(EEventType::BeginList, EEventType::EndList);
                            return;

                        case special_token::right_brace:
                            pop(EEventType::BeginMap, EEventType::EndMap);
                            return;

                            // right_angle is impossible in maybe_value state
                            // (only in delimiter, maybe_key)

                        default:
                            break;
                    }
                }

                // attributes are not allowed after attributes (thus, value_noattr state)
                if (state_ != state::value_noattr && token == special_token::left_angle) {
                    push(EEventType::BeginAttributes);
                    yield(EEventType::BeginAttributes);
                    next(state::maybe_key);
                    return;
                }

                COLD_BLOCK_BYVALUE
                lexer_.fail("Unexpected ", NCEscape::quote(ch));
                COLD_BLOCK_END
            }

            ATTRIBUTE(hot)
            void state_value_binary_scalar(char_class cls) {
                lexer_.advance(1);
                switch (cls) {
                    case char_class::binary_double:
                        yield(lexer_.read_binary_double());
                        break;

                    case char_class::binary_int64:
                        yield(lexer_.read_binary_int64());
                        break;

                    case char_class::binary_uint64:
                        yield(lexer_.read_binary_uint64());
                        break;

                    case char_class::binary_false:
                        yield(false);
                        break;

                    case char_class::binary_true:
                        yield(true);
                        break;

                    default:
                        Y_UNREACHABLE();
                }
            }

            ATTRIBUTE(noinline)
            void state_value_text_scalar(char_class cls) {
                switch (cls) {
                    case char_class::quote:
                        lexer_.advance(1);
                        yield(lexer_.read_quoted_string());
                        break;

                    case char_class::number:
                        yield(lexer_.read_numeric());
                        break;

                    case char_class::string:
                        yield(lexer_.read_unquoted_string());
                        break;

                    case char_class::percent:
                        lexer_.advance(1);
                        yield(lexer_.read_percent_scalar());
                        break;

                    case char_class::none:
                        COLD_BLOCK_BYVALUE
                        lexer_.fail("Invalid yson value.");
                        COLD_BLOCK_END
                        break;

                    default:
                        Y_UNREACHABLE();
                }
            }
        };

        class reader_impl: public gen_reader_impl<false> {
        public:
            using gen_reader_impl<false>::gen_reader_impl;
        };
    }
}
