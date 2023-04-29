//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
// Copyright (c) 2022-2023 Alexander Grund
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#include "boost/locale/util/encoding.hpp"
#include "boost/locale/util/string.hpp"
#include <boost/assert.hpp>
#if BOOST_LOCALE_USE_WIN32_API
#    include "boost/locale/util/win_codepages.hpp"
#    ifndef NOMINMAX
#        define NOMINMAX
#    endif
#    include <windows.h>
#endif
#include <algorithm>
#include <cstring>

namespace boost { namespace locale { namespace util {
    static std::string do_normalize_encoding(const char* encoding, const size_t len)
    {
        std::string result;
        result.reserve(len);
        for(char c = *encoding; c != 0; c = *(++encoding)) {
            if(is_lower_ascii(c) || is_numeric_ascii(c))
                result += c;
            else if(is_upper_ascii(c))
                result += char(c - 'A' + 'a');
        }
        return result;
    }

    std::string normalize_encoding(const std::string& encoding)
    {
        return do_normalize_encoding(encoding.c_str(), encoding.size());
    }

    std::string normalize_encoding(const char* encoding)
    {
        return do_normalize_encoding(encoding, std::strlen(encoding));
    }

#if BOOST_LOCALE_USE_WIN32_API
    static int normalized_encoding_to_windows_codepage(const std::string& encoding)
    {
        constexpr size_t n = sizeof(all_windows_encodings) / sizeof(all_windows_encodings[0]);
        windows_encoding* begin = all_windows_encodings;
        windows_encoding* end = all_windows_encodings + n;

        windows_encoding* ptr = std::lower_bound(begin, end, encoding.c_str());
        while(ptr != end && ptr->name == encoding) {
            if(ptr->was_tested)
                return ptr->codepage;
            else if(IsValidCodePage(ptr->codepage)) {
                // the thread safety is not an issue, maximum
                // it would be checked more then once
                ptr->was_tested = 1;
                return ptr->codepage;
            } else
                ++ptr;
        }
        return -1;
    }

    int encoding_to_windows_codepage(const char* encoding)
    {
        return normalized_encoding_to_windows_codepage(normalize_encoding(encoding));
    }

    int encoding_to_windows_codepage(const std::string& encoding)
    {
        return normalized_encoding_to_windows_codepage(normalize_encoding(encoding));
    }

#endif
}}} // namespace boost::locale::util
