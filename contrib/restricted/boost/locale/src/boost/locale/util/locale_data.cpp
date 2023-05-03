//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
// Copyright (c) 2022-2023 Alexander Grund
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#include <boost/locale/util/locale_data.hpp>
#include "boost/locale/encoding/conv.hpp"
#include "boost/locale/util/encoding.hpp"
#include "boost/locale/util/string.hpp"
#include <boost/assert.hpp>
#include <algorithm>
#include <stdexcept>
#include <string>

namespace boost { namespace locale { namespace util {
    locale_data::locale_data()
    {
        reset();
    }

    locale_data::locale_data(const std::string& locale_name)
    {
        if(!parse(locale_name))
            throw std::invalid_argument("Failed to parse locale name: " + locale_name);
    }

    void locale_data::reset()
    {
        language_ = "C";
        country_.clear();
        encoding_ = "US-ASCII";
        variant_.clear();
        utf8_ = false;
    }

    std::string locale_data::to_string() const
    {
        std::string result = language_;
        if(!country_.empty())
            (result += '_') += country_;
        if(!encoding_.empty() && !util::are_encodings_equal(encoding_, "US-ASCII"))
            (result += '.') += encoding_;
        if(!variant_.empty())
            (result += '@') += variant_;
        return result;
    }

    bool locale_data::parse(const std::string& locale_name)
    {
        reset();
        return parse_from_lang(locale_name);
    }

    bool locale_data::parse_from_lang(const std::string& input)
    {
        const auto end = input.find_first_of("-_@.");
        std::string tmp = input.substr(0, end);
        if(tmp.empty())
            return false;
        // lowercase ASCII
        for(char& c : tmp) {
            if(is_upper_ascii(c))
                c += 'a' - 'A';
            else if(!is_lower_ascii(c))
                return false;
        }
        if(tmp != "c" && tmp != "posix") // Keep default
            language_ = tmp;

        if(end >= input.size())
            return true;
        else if(input[end] == '-' || input[end] == '_')
            return parse_from_country(input.substr(end + 1));
        else if(input[end] == '.')
            return parse_from_encoding(input.substr(end + 1));
        else {
            BOOST_ASSERT_MSG(input[end] == '@', "Unexpected delimiter");
            return parse_from_variant(input.substr(end + 1));
        }
    }

    bool locale_data::parse_from_country(const std::string& input)
    {
        if(language_ == "C")
            return false;

        const auto end = input.find_first_of("@.");
        std::string tmp = input.substr(0, end);
        if(tmp.empty())
            return false;

        // Make uppercase
        for(char& c : tmp) {
            if(util::is_lower_ascii(c))
                c += 'A' - 'a';
        }
        // If it's ALL uppercase ASCII, assume ISO 3166 country id
        if(std::find_if_not(tmp.begin(), tmp.end(), util::is_upper_ascii) != tmp.end()) {
            // else handle special cases:
            //   - en_US_POSIX is an alias for C
            //   - M49 country code: 3 digits
            if(language_ == "en" && tmp == "US_POSIX") {
                language_ = "C";
                tmp.clear();
            } else if(tmp.size() != 3u || std::find_if_not(tmp.begin(), tmp.end(), util::is_numeric_ascii) != tmp.end())
                return false;
        }

        country_ = tmp;
        if(end >= input.size())
            return true;
        else if(input[end] == '.')
            return parse_from_encoding(input.substr(end + 1));
        else {
            BOOST_ASSERT_MSG(input[end] == '@', "Unexpected delimiter");
            return parse_from_variant(input.substr(end + 1));
        }
    }

    bool locale_data::parse_from_encoding(const std::string& input)
    {
        const auto end = input.find_first_of('@');
        std::string tmp = input.substr(0, end);
        if(tmp.empty())
            return false;
        // No assumptions, but uppercase
        for(char& c : tmp) {
            if(util::is_lower_ascii(c))
                c += 'A' - 'a';
        }
        encoding_ = tmp;

        utf8_ = util::normalize_encoding(encoding_) == "utf8";

        if(end >= input.size())
            return true;
        else {
            BOOST_ASSERT_MSG(input[end] == '@', "Unexpected delimiter");
            return parse_from_variant(input.substr(end + 1));
        }
    }

    bool locale_data::parse_from_variant(const std::string& input)
    {
        if(language_ == "C")
            return false;
        if(input.empty())
            return false;
        variant_ = input;
        // No assumptions, just make it lowercase
        for(char& c : variant_) {
            if(util::is_upper_ascii(c))
                c += 'a' - 'A';
        }
        return true;
    }

}}} // namespace boost::locale::util
