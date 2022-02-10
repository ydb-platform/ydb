#pragma once

#include "byte_reader.h"
#include "cescape.h"
#include "macros.h"
#include "number.h"
#include "percent_scalar.h"
#include "stream_counter.h"
#include "varint.h"

#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>

namespace NYsonPull {
    namespace NDetail {
        template <bool EnableLinePositionInfo>
        class lexer_base: public byte_reader<stream_counter<EnableLinePositionInfo>> {
            using Base = byte_reader<
                stream_counter<EnableLinePositionInfo>>;

            TVector<ui8> token_buffer_;
            TMaybe<size_t> memory_limit_;

        public:
            lexer_base(
                NYsonPull::NInput::IStream& buffer,
                TMaybe<size_t> memory_limit)
                : Base(buffer)
                , memory_limit_{memory_limit} {
            }

            ATTRIBUTE(noinline, hot)
            ui8 skip_space_and_get_byte() {
                auto& buf = Base::stream().buffer();
                if (Y_LIKELY(!buf.is_empty())) {
                    auto ch = *buf.pos();
                    if (Y_LIKELY(!is_space(ch))) {
                        return ch;
                    }
                }
                return skip_space_and_get_byte_fallback();
            }

            ATTRIBUTE(hot)
            ui8 get_byte() {
                auto& buf = Base::stream().buffer();
                if (Y_LIKELY(!buf.is_empty())) {
                    return *buf.pos();
                }
                return Base::get_byte();
            }

            number read_numeric() {
                token_buffer_.clear();
                auto type = number_type::int64;
                while (true) {
                    auto ch = this->Base::template get_byte<true>();
                    if (isdigit(ch) || ch == '+' || ch == '-') {
                        token_buffer_.push_back(ch);
                    } else if (ch == '.' || ch == 'e' || ch == 'E') {
                        token_buffer_.push_back(ch);
                        type = number_type::float64;
                    } else if (ch == 'u') {
                        token_buffer_.push_back(ch);
                        type = number_type::uint64;
                    } else if (Y_UNLIKELY(isalpha(ch))) {
                        COLD_BLOCK_BYVALUE
                        Base::fail("Unexpected ", NCEscape::quote(ch), " in numeric literal");
                        COLD_BLOCK_END
                    } else {
                        break;
                    }
                    check_memory_limit();
                    Base::advance(1);
                }

                auto str = token_buffer();
                try {
                    switch (type) {
                        case number_type::float64:
                            return FromString<double>(str);
                        case number_type::int64:
                            return FromString<i64>(str);
                        case number_type::uint64:
                            str.Chop(1); // 'u' suffix
                            return FromString<ui64>(str);
                    }
                    Y_UNREACHABLE();
                } catch (const std::exception& err) {
                    Base::fail(err.what());
                }
            }

            TStringBuf read_quoted_string() {
                auto count_trailing_slashes = [](ui8* begin, ui8* end) {
                    auto count = size_t{0};
                    if (begin < end) {
                        for (auto p = end - 1; p >= begin && *p == '\\'; --p) {
                            ++count;
                        }
                    }
                    return count;
                };

                token_buffer_.clear();
                auto& buf = Base::stream().buffer();
                while (true) {
                    this->Base::template fill_buffer<false>();
                    auto* quote = reinterpret_cast<const ui8*>(
                        ::memchr(buf.pos(), '"', buf.available()));
                    if (quote == nullptr) {
                        token_buffer_.insert(
                            token_buffer_.end(),
                            buf.pos(),
                            buf.end());
                        Base::advance(buf.available());
                        continue;
                    }

                    token_buffer_.insert(
                        token_buffer_.end(),
                        buf.pos(),
                        quote);
                    Base::advance(quote - buf.pos() + 1); // +1 for the quote itself

                    // We must count the number of '\' at the end of StringValue
                    // to check if it's not \"
                    int slash_count = count_trailing_slashes(
                        token_buffer_.data(),
                        token_buffer_.data() + token_buffer_.size());
                    if (slash_count % 2 == 0) {
                        break;
                    } else {
                        token_buffer_.push_back('"');
                    }
                    check_memory_limit();
                }

                NCEscape::decode_inplace(token_buffer_);
                return token_buffer();
            }

            TStringBuf read_unquoted_string() {
                token_buffer_.clear();
                while (true) {
                    auto ch = this->Base::template get_byte<true>();
                    if (isalpha(ch) || isdigit(ch) ||
                        ch == '_' || ch == '-' || ch == '%' || ch == '.') {
                        token_buffer_.push_back(ch);
                    } else {
                        break;
                    }
                    check_memory_limit();
                    Base::advance(1);
                }
                return token_buffer();
            }

            ATTRIBUTE(noinline, hot)
            TStringBuf read_binary_string() {
                auto slength = NVarInt::read<i32>(*this);
                if (Y_UNLIKELY(slength < 0)) {
                    COLD_BLOCK_BYVALUE
                    Base::fail("Negative binary string literal length ", slength);
                    COLD_BLOCK_END
                }
                auto length = static_cast<ui32>(slength);

                auto& buf = Base::stream().buffer();
                if (Y_LIKELY(buf.available() >= length)) {
                    auto result = TStringBuf{
                        reinterpret_cast<const char*>(buf.pos()),
                        length};
                    Base::advance(length);
                    return result;
                } else { // reading in Buffer
                    return read_binary_string_fallback(length);
                }
            }

            ATTRIBUTE(noinline)
            TStringBuf read_binary_string_fallback(size_t length) {
                auto& buf = Base::stream().buffer();
                auto needToRead = length;
                token_buffer_.clear();
                while (needToRead) {
                    this->Base::template fill_buffer<false>();
                    auto chunk_size = std::min(needToRead, buf.available());

                    token_buffer_.insert(
                        token_buffer_.end(),
                        buf.pos(),
                        buf.pos() + chunk_size);
                    check_memory_limit();
                    needToRead -= chunk_size;
                    Base::advance(chunk_size);
                }
                return token_buffer();
            }

            percent_scalar read_percent_scalar() {
                auto throw_incorrect_percent_scalar = [&]() {
                    Base::fail("Incorrect %-literal prefix ", NCEscape::quote(token_buffer()));
                };

                auto assert_literal = [&](TStringBuf literal) -> void {
                    for (size_t i = 2; i < literal.size(); ++i) {
                        token_buffer_.push_back(this->Base::template get_byte<false>());
                        Base::advance(1);
                        if (Y_UNLIKELY(token_buffer_.back() != literal[i])) {
                            throw_incorrect_percent_scalar();
                        }
                    }
                };

                token_buffer_.clear();
                token_buffer_.push_back(this->Base::template get_byte<false>());
                Base::advance(1);

                switch (token_buffer_[0]) {
                    case 't':
                        assert_literal(percent_scalar::true_literal);
                        return percent_scalar(true);
                    case 'f':
                        assert_literal(percent_scalar::false_literal);
                        return percent_scalar(false);
                    case 'n':
                        assert_literal(percent_scalar::nan_literal);
                        return percent_scalar(std::numeric_limits<double>::quiet_NaN());
                    case 'i':
                        assert_literal(percent_scalar::positive_inf_literal);
                        return percent_scalar(std::numeric_limits<double>::infinity());
                    case '-':
                        assert_literal(percent_scalar::negative_inf_literal);
                        return percent_scalar(-std::numeric_limits<double>::infinity());
                    default:
                        throw_incorrect_percent_scalar();
                }

                Y_UNREACHABLE();
            }

            i64 read_binary_int64() {
                return NVarInt::read<i64>(*this);
            }

            ui64 read_binary_uint64() {
                return NVarInt::read<ui64>(*this);
            }

            double read_binary_double() {
                union {
                    double as_double;
                    ui8 as_bytes[sizeof(double)];
                } data;
                static_assert(sizeof(data) == sizeof(double), "bad union size");

                auto needToRead = sizeof(double);

                auto& buf = Base::stream().buffer();
                while (needToRead != 0) {
                    Base::fill_buffer();

                    auto chunk_size = std::min(needToRead, buf.available());
                    if (chunk_size == 0) {
                        Base::fail("Error parsing binary double literal");
                    }
                    std::copy(
                        buf.pos(),
                        buf.pos() + chunk_size,
                        data.as_bytes + (sizeof(double) - needToRead));
                    needToRead -= chunk_size;
                    Base::advance(chunk_size);
                }
                return data.as_double;
            }

        private:
            static bool is_space(ui8 ch) {
                static const ui8 lookupTable[] =
                    {
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,

                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,

                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,

                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
                return lookupTable[ch];
            }

            ATTRIBUTE(noinline, cold)
            ui8 skip_space_and_get_byte_fallback() {
                auto& buf = Base::stream().buffer();
                while (true) {
                    // FIXME
                    if (buf.is_empty()) {
                        if (Base::stream().at_end()) {
                            return '\0';
                        }
                        Base::fill_buffer();
                    } else {
                        if (!is_space(*buf.pos())) {
                            break;
                        }
                        Base::advance(1);
                    }
                }
                return Base::get_byte();
            }

            void check_memory_limit() {
                if (Y_UNLIKELY(memory_limit_ && token_buffer_.capacity() > *memory_limit_)) {
                    COLD_BLOCK_BYVALUE
                    Base::fail(
                        "Memory limit exceeded while parsing YSON stream: "
                        "allocated ",
                        token_buffer_.capacity(),
                        ", limit ", *memory_limit_);
                    COLD_BLOCK_END
                }
            }

            TStringBuf token_buffer() const {
                auto* begin = reinterpret_cast<const char*>(token_buffer_.data());
                return {begin, token_buffer_.size()};
            }
        };
    }
}
