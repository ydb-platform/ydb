//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
// Copyright (c) 2022-2023 Alexander Grund
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_LOCALE_UTIL_ENCODING_HPP
#define BOOST_LOCALE_UTIL_ENCODING_HPP

#include <boost/locale/config.hpp>
#include <cstdint>
#include <string>

namespace boost { namespace locale { namespace util {

    /// Get the UTF encoding name of the given \a CharType
    template<typename CharType>
    const char* utf_name()
    {
        constexpr auto CharSize = sizeof(CharType);
        static_assert(CharSize == 1 || CharSize == 2 || CharSize == 4, "Unknown Character Encoding");
        switch(CharSize) {
            case 1: return "UTF-8";
            case 2: {
                const int16_t endianMark = 1;
                const bool isLittleEndian = reinterpret_cast<const char*>(&endianMark)[0] == 1;
                return isLittleEndian ? "UTF-16LE" : "UTF-16BE";
            }
            case 4: {
                const int32_t endianMark = 1;
                const bool isLittleEndian = reinterpret_cast<const char*>(&endianMark)[0] == 1;
                return isLittleEndian ? "UTF-32LE" : "UTF-32BE";
            }
        }
        BOOST_UNREACHABLE_RETURN("Unknown UTF");
    }

    /// Make encoding lowercase and remove all non-alphanumeric characters
    BOOST_LOCALE_DECL std::string normalize_encoding(const std::string& encoding);
    BOOST_LOCALE_DECL std::string normalize_encoding(const char* encoding);
    /// True if the normalized encodings are equal
    inline bool are_encodings_equal(const std::string& l, const std::string& r)
    {
        return normalize_encoding(l) == normalize_encoding(r);
    }

#if BOOST_LOCALE_USE_WIN32_API
    int encoding_to_windows_codepage(const char* encoding);
    int encoding_to_windows_codepage(const std::string& encoding);
#endif

}}} // namespace boost::locale::util

#endif
