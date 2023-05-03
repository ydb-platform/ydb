//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
// Copyright (c) 2022-2023 Alexander Grund
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#include "iconv_codecvt.hpp"
#include <boost/assert.hpp>
#include <array>
#include <cerrno>
#include <limits>
#include <vector>
#ifdef BOOST_LOCALE_WITH_ICONV
#    include "boost/locale/util/iconv.hpp"
#endif

namespace boost { namespace locale {

#ifdef BOOST_LOCALE_WITH_ICONV
    static const char* utf32_encoding()
    {
        union {
            char one;
            uint32_t value;
        } test;
        test.value = 1;
        return (test.one == 1) ? "UTF-32LE" : "UTF-32BE";
    }

    class mb2_iconv_converter : public util::base_converter {
    public:
        mb2_iconv_converter(const std::string& encoding) : encoding_(encoding)
        {
            iconv_handle d = iconv_open(utf32_encoding(), encoding.c_str());
            if(!d)
                throw std::runtime_error("Unsupported encoding" + encoding);

            for(unsigned c = 0; c < first_byte_table_.size(); c++) {
                const char ibuf[2] = {char(c), 0};
                size_t insize = sizeof(ibuf);
                uint32_t obuf[2] = {illegal, illegal};
                size_t outsize = sizeof(obuf);
                // Basic single codepoint conversion
                call_iconv(d, ibuf, &insize, reinterpret_cast<char*>(obuf), &outsize);
                if(insize == 0 && outsize == 0 && obuf[1] == 0)
                    first_byte_table_[c] = obuf[0];
                else {
                    // Test if this is illegal first byte or incomplete
                    insize = 1;
                    outsize = sizeof(obuf);
                    call_iconv(d, nullptr, nullptr, nullptr, nullptr);
                    size_t res = call_iconv(d, ibuf, &insize, reinterpret_cast<char*>(obuf), &outsize);

                    // Now if this single byte starts a sequence we add incomplete
                    // to know to ask that we need two bytes, otherwise it may only be illegal

                    first_byte_table_[c] = (res == size_t(-1) && errno == EINVAL) ? incomplete : illegal;
                }
            }
        }

        mb2_iconv_converter(const mb2_iconv_converter& other) :
            first_byte_table_(other.first_byte_table_), encoding_(other.encoding_)
        {}

        bool is_thread_safe() const override { return false; }

        mb2_iconv_converter* clone() const override { return new mb2_iconv_converter(*this); }

        uint32_t to_unicode(const char*& begin, const char* end) override
        {
            if(begin == end)
                return incomplete;

            const unsigned char seq0 = *begin;

#    if defined(BOOST_GCC_VERSION) && BOOST_GCC_VERSION >= 40600
#        pragma GCC diagnostic push
#        pragma GCC diagnostic ignored "-Wtype-limits"
#    endif
            static_assert(std::numeric_limits<unsigned char>::max()
                            < std::tuple_size<decltype(first_byte_table_)>::value,
                          "Wrong table size");
#    if defined(BOOST_GCC_VERSION) && BOOST_GCC_VERSION >= 40600
#        pragma GCC diagnostic pop
#    endif

            const uint32_t index = first_byte_table_[seq0];
            if(index == illegal)
                return illegal;
            if(index != incomplete) {
                begin++;
                return index;
            } else if(begin + 1 == end)
                return incomplete;

            open(to_utf_, utf32_encoding(), encoding_.c_str());

            // maybe illegal or may be double byte

            const char inseq[3] = {static_cast<char>(seq0), begin[1], 0};
            size_t insize = sizeof(inseq);
            uint32_t result[2] = {illegal, illegal};
            size_t outsize = sizeof(result);
            call_iconv(to_utf_, inseq, &insize, reinterpret_cast<char*>(result), &outsize);
            if(outsize == 0 && insize == 0 && result[1] == 0) {
                begin += 2;
                return result[0];
            }
            return illegal;
        }

        uint32_t from_unicode(uint32_t cp, char* begin, const char* end) override
        {
            if(cp == 0) {
                if(begin != end) {
                    *begin = 0;
                    return 1;
                } else {
                    return incomplete;
                }
            }

            open(from_utf_, encoding_.c_str(), utf32_encoding());

            const uint32_t inbuf[2] = {cp, 0};
            size_t insize = sizeof(inbuf);
            char outseq[3] = {0};
            size_t outsize = 3;

            call_iconv(from_utf_, reinterpret_cast<const char*>(inbuf), &insize, outseq, &outsize);

            if(insize != 0 || outsize > 1)
                return illegal;
            size_t len = 2 - outsize;
            size_t reminder = end - begin;
            if(reminder < len)
                return incomplete;
            for(unsigned i = 0; i < len; i++)
                *begin++ = outseq[i];
            return static_cast<uint32_t>(len);
        }

        int max_len() const override
        {
            return 2;
        }

    private:
        std::array<uint32_t, 256> first_byte_table_;
        std::string encoding_;
        iconv_handle to_utf_, from_utf_;

        static void open(iconv_handle& d, const char* to, const char* from)
        {
            if(!d)
                d = iconv_open(to, from);
            BOOST_ASSERT(d);
        }
    };

    std::unique_ptr<util::base_converter> create_iconv_converter(const std::string& encoding)
    {
        try {
            return std::unique_ptr<util::base_converter>(new mb2_iconv_converter(encoding));
        } catch(const std::exception&) {
            return nullptr;
        }
    }

#else // no iconv
    std::unique_ptr<util::base_converter> create_iconv_converter(const std::string& /*encoding*/)
    {
        return nullptr;
    }
#endif

}} // namespace boost::locale
